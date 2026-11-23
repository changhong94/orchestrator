package main

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"sync"
	"syscall"
)

type executableVersion struct {
	Dev, Ino     uint64
	Size         int64
	Mode         uint32
	Mtime, Ctime syscall.Timespec
}
type executableProof struct {
	Version executableVersion
	Digest  string
}

var approvedExecutables struct {
	sync.Mutex
	proofs map[executableProof]struct{}
}

// Only shared hosts cache a proof. Every use opens and stats the current file;
// replacement, in-place changes or a different expected digest miss the cache.
// No mutable VM state or tenant authority is cached here.
func verifyExecutable(path, expected string, shared bool) error {
	if len(expected) != 64 {
		return errors.New("sandbox executable digest missing or invalid")
	}
	f, e := os.Open(path)
	if e != nil {
		return e
	}
	defer f.Close()
	version := func() (executableVersion, error) {
		st, e := f.Stat()
		if e != nil {
			return executableVersion{}, e
		}
		if !st.Mode().IsRegular() {
			return executableVersion{}, errors.New("sandbox executable is not regular")
		}
		x := st.Sys().(*syscall.Stat_t)
		return executableVersion{x.Dev, x.Ino, x.Size, x.Mode, x.Mtim, x.Ctim}, nil
	}
	before, e := version()
	if e != nil {
		return e
	}
	proof := executableProof{before, expected}
	if shared {
		approvedExecutables.Lock()
		defer approvedExecutables.Unlock()
		current, e := version()
		if e != nil {
			return e
		}
		before = current
		proof = executableProof{before, expected}
		if _, ok := approvedExecutables.proofs[proof]; ok {
			return nil
		}
	}
	h := sha256.New()
	if _, e = io.Copy(h, f); e != nil {
		return e
	}
	after, e := version()
	if e != nil {
		return e
	}
	if before != after || hex.EncodeToString(h.Sum(nil)) != expected {
		return errors.New("sandbox executable digest mismatch or changed during verification")
	}
	if shared {
		if len(approvedExecutables.proofs) >= 16 {
			approvedExecutables.proofs = nil
		}
		if approvedExecutables.proofs == nil {
			approvedExecutables.proofs = make(map[executableProof]struct{})
		}
		approvedExecutables.proofs[proof] = struct{}{}
	}
	return nil
}
