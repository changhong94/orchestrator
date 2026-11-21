package taskartifact

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"syscall"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/reflocation"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
)

const preparedCacheVersion = 1
const maxPreparedCacheBytes = 4 << 20

type preparedFileStamp struct {
	Device, Inode      uint64
	Size, Mtime, Ctime int64
	Mode               uint32
}
type preparedCacheEntry struct {
	Version    int
	Result     Result
	Resolution canonicalResolution
	Files      map[string]preparedFileStamp
}
type preparedCacheAccess struct {
	path string
	aead cipher.AEAD
	aad  []byte
}

// Prepare keeps plaintext metadata in the tenant task. The optional cache is
// encrypted/authenticated by that task's customer key. It never admits a run:
// the exact-run summary handshake and all runtime ownership barriers still run.
func Prepare(ctx context.Context, spec configsock.ArtifactPrepareSpec) (*Result, error) {
	// Only registered static snapshot templates carry a pinned host runtime.
	// Live-fork and ordinary restore requests must not pay cache preparation.
	if spec.SnapshotRuntime == "" {
		return prepareUncached(ctx, spec)
	}
	return prepareCached(ctx, spec)
}
func prepareCached(ctx context.Context, spec configsock.ArtifactPrepareSpec) (*Result, error) {
	started := time.Now()
	access, err := preparedCacheFor(spec)
	if err != nil || access == nil {
		return prepareUncached(ctx, spec)
	}
	if result, err := access.load(ctx, spec); err == nil {
		result.PrepareDuration = time.Since(started)
		result.ConfigReadDuration = result.PrepareDuration
		log.Printf("prepared artifact cache hit duration=%s", result.PrepareDuration)
		return result, nil
	}
	result, err := prepareUncached(ctx, spec)
	if err != nil {
		return nil, err
	}
	// This is preparation-only work. A second validated read bracketed by file
	// stamps prevents publishing metadata from a concurrently replaced carrier.
	before, err := preparedStamps(spec, result)
	if err != nil {
		return result, nil
	}
	verified, err := prepareUncached(ctx, spec)
	if err != nil {
		return result, nil
	}
	after, err := preparedStamps(spec, verified)
	if err != nil || !reflect.DeepEqual(before, after) || !reflect.DeepEqual(result.cacheResolution, verified.cacheResolution) {
		return result, nil
	}
	entry := preparedCacheEntry{Version: preparedCacheVersion, Result: *verified, Resolution: verified.cacheResolution, Files: after}
	if err := access.save(entry); err != nil {
		log.Printf("prepared artifact cache publication failed: %v", err)
	} else {
		log.Printf("prepared artifact cache fill duration=%s", time.Since(started))
	}
	return result, nil
}
func preparedCacheFor(spec configsock.ArtifactPrepareSpec) (*preparedCacheAccess, error) {
	dir := os.Getenv("KUASAR_PREPARED_ARTIFACT_CACHE")
	if dir == "" || !filepath.IsAbs(dir) || spec.RootSourceKind != string(types.ResumeSourceSnapshot) || spec.LaunchMode != string(types.LaunchMemory) || spec.ReadSourceImageConfig || spec.PreflightImageBundle || len(spec.SBXCreateWorkspaces) != 0 || len(spec.SBXCreateMounts) != 0 {
		return nil, nil
	}
	cfg, err := rtconfig.LoadManifestConfig(spec.ManifestConfig)
	if err != nil {
		return nil, err
	}
	key, err := cfg.CustomerKey()
	if err != nil {
		return nil, err
	}
	defer clear(key[:])
	// A named root is independent of the assigned instance's checkpoint directory.
	// For this fast path all dependencies must also be independently qualified.
	if rootRef, e := manifest.ParseRef(spec.RootRef); e == nil && rootRef.Location != "" {
		spec.RelativeDir = ""
	}
	spec.RunID = ""
	spec.AbsoluteDeadlineUnixNano = 0
	normalized, err := json.Marshal(struct {
		Version int
		Spec    configsock.ArtifactPrepareSpec
		Config  *rtconfig.ManifestConfig
	}{preparedCacheVersion, spec, cfg})
	if err != nil {
		return nil, err
	}
	mac := hmac.New(sha256.New, key[:])
	mac.Write([]byte("kuasar prepared artifact cache key v1"))
	derived := mac.Sum(nil)
	defer clear(derived)
	block, err := aes.NewCipher(derived)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	mac.Reset()
	mac.Write(normalized)
	aad := mac.Sum(nil)
	return &preparedCacheAccess{filepath.Join(dir, hex.EncodeToString(aad)+".prepared"), aead, aad}, nil
}
func fileStamp(path string) (preparedFileStamp, error) {
	st, err := os.Stat(path)
	if err != nil {
		return preparedFileStamp{}, err
	}
	if !st.Mode().IsRegular() {
		return preparedFileStamp{}, errors.New("prepared carrier is not regular")
	}
	s := st.Sys().(*syscall.Stat_t)
	return preparedFileStamp{uint64(s.Dev), s.Ino, s.Size, s.Mtim.Nano(), s.Ctim.Nano(), s.Mode}, nil
}
func preparedStamps(spec configsock.ArtifactPrepareSpec, result *Result) (map[string]preparedFileStamp, error) {
	locations := rtconfig.RefLocations{}
	for name, uri := range result.RefLocationURIs {
		current, err := reflocation.Resolve(spec.RefLocationParent, name)
		if err != nil {
			return nil, err
		}
		if current.URI != uri {
			return nil, errors.New("prepared location changed")
		}
		locations[name] = current.Path
	}
	relativeDir := spec.RelativeDir
	if relativeDir == "" {
		path, local, err := rootFilePath(spec.RootRef, "", locations)
		if err != nil || !local {
			return nil, errors.New("local preparation root required")
		}
		relativeDir = filepath.Dir(path)
	}
	refs := append([]string{spec.RootRef, result.Summary.RootSource.SandboxRef}, result.cacheResolution.RequiredRefs...)
	files := map[string]preparedFileStamp{}
	for _, ref := range refs {
		if ref == "" {
			continue
		}
		parsed, parseErr := manifest.ParseRef(ref)
		rootParsed, _ := manifest.ParseRef(spec.RootRef)
		if rootParsed.Location != "" && (parseErr != nil || parsed.Scheme != manifest.RefSchemeFile || (parsed.Location == "" && !filepath.IsAbs(parsed.Path))) {
			return nil, errors.New("preparation dependency must be independently qualified")
		}
		path, local, err := rootFilePath(ref, relativeDir, locations)
		if err != nil {
			return nil, err
		}
		if !local {
			return nil, errors.New("remote preparation cache not supported")
		}
		st, err := fileStamp(path)
		if err != nil {
			return nil, err
		}
		files[path] = st
	}
	if spec.SnapshotRuntime != "" {
		ref, err := manifest.ParseRef(spec.SnapshotRuntime)
		if err != nil || ref.Scheme != manifest.RefSchemeFile {
			return nil, errors.New("runtime binding is not local")
		}
		st, err := fileStamp(ref.Path)
		if err != nil {
			return nil, err
		}
		files[ref.Path] = st
	}
	if len(files) == 0 {
		return nil, errors.New("empty preparation dependencies")
	}
	return files, nil
}
func (a *preparedCacheAccess) readEntry(ctx context.Context) (*preparedCacheEntry, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f, err := os.Open(a.path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	raw, err := io.ReadAll(io.LimitReader(f, maxPreparedCacheBytes+1))
	if err != nil {
		return nil, err
	}
	if len(raw) > maxPreparedCacheBytes || len(raw) < a.aead.NonceSize() {
		return nil, errors.New("invalid prepared cache size")
	}
	plain, err := a.aead.Open(nil, raw[:a.aead.NonceSize()], raw[a.aead.NonceSize():], a.aad)
	if err != nil {
		return nil, err
	}
	defer clear(plain)
	var e preparedCacheEntry
	if err = json.Unmarshal(plain, &e); err != nil {
		return nil, err
	}
	if e.Version != preparedCacheVersion {
		return nil, errors.New("prepared cache version")
	}
	e.Result.cacheResolution = e.Resolution
	return &e, nil
}

func (a *preparedCacheAccess) load(ctx context.Context, spec configsock.ArtifactPrepareSpec) (*Result, error) {
	e, err := a.readEntry(ctx)
	if err != nil {
		return nil, err
	}
	return consumePreparedEntry(ctx, spec, e)
}

func consumePreparedEntry(ctx context.Context, spec configsock.ArtifactPrepareSpec, e *preparedCacheEntry) (*Result, error) {
	e.Result.cacheResolution = e.Resolution
	current, err := preparedStamps(spec, &e.Result)
	if err != nil {
		return nil, err
	}
	if !reflect.DeepEqual(current, e.Files) {
		return nil, errors.New("prepared carrier changed")
	}
	e.Resolution.RunID = spec.RunID
	encoded, err := json.Marshal(e.Resolution)
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(encoded)
	e.Result.Summary.ResolutionDigest = hex.EncodeToString(digest[:])
	e.Result.cacheResolution = e.Resolution
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return &e.Result, nil
}
func (a *preparedCacheAccess) save(e preparedCacheEntry) (retErr error) {
	plain, err := json.Marshal(e)
	if err != nil {
		return err
	}
	defer clear(plain)
	if len(plain) > maxPreparedCacheBytes-64 {
		return errors.New("prepared cache entry too large")
	}
	nonce := make([]byte, a.aead.NonceSize())
	if _, err = rand.Read(nonce); err != nil {
		return err
	}
	raw := a.aead.Seal(nonce, nonce, plain, a.aad)
	dir := filepath.Dir(a.path)
	if err = os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	info, err := os.Stat(dir)
	if err != nil {
		return err
	}
	if info.Mode().Perm()&0077 != 0 {
		return fmt.Errorf("prepared cache directory is not private: %s", dir)
	}
	f, err := os.CreateTemp(dir, ".prepared-")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	_, err = f.Write(raw)
	if err == nil {
		err = f.Sync()
	}
	err = errors.Join(err, f.Close())
	if err != nil {
		return err
	}
	if err = os.Rename(f.Name(), a.path); err != nil {
		return err
	}
	df, err := os.Open(dir)
	if err != nil {
		return err
	}
	return errors.Join(df.Sync(), df.Close())
}
