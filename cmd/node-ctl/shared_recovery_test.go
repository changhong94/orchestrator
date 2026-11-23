package main

import (
	"crypto/sha256"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestExecutableProofRejectsMutationAndDifferentAuthority(t *testing.T) {
	p := filepath.Join(t.TempDir(), "ctl")
	original := []byte("approved binary")
	if e := os.WriteFile(p, original, 0700); e != nil {
		t.Fatal(e)
	}
	want := fmt.Sprintf("%x", sha256.Sum256(original))
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if e := verifyExecutable(p, want, true); e != nil {
				t.Error(e)
			}
		}()
	}
	wg.Wait()
	if e := verifyExecutable(p, fmt.Sprintf("%x", sha256.Sum256([]byte("wrong"))), true); e == nil {
		t.Fatal("accepted wrong digest")
	}
	st, _ := os.Stat(p)
	if e := os.WriteFile(p, []byte("modified binary"), 0700); e != nil {
		t.Fatal(e)
	}
	os.Chtimes(p, st.ModTime(), st.ModTime())
	if e := verifyExecutable(p, want, true); e == nil {
		t.Fatal("cached proof survived in-place modification")
	}
	if e := os.WriteFile(p+".next", original, 0700); e != nil {
		t.Fatal(e)
	}
	if e := os.Rename(p+".next", p); e != nil {
		t.Fatal(e)
	}
	if e := verifyExecutable(p, want, true); e != nil {
		t.Fatal(e)
	}
}
func TestSharedEndpointRecoversOnlyStaleSocket(t *testing.T) {
	d := t.TempDir()
	os.Chmod(d, 0700)
	p := filepath.Join(d, "runs.sock")
	l, e := net.ListenUnix("unixpacket", &net.UnixAddr{Name: p, Net: "unixpacket"})
	if e != nil {
		t.Fatal(e)
	}
	l.SetUnlinkOnClose(false)
	if _, close, e := listenSharedRuns(p); e == nil {
		close()
		t.Fatal("stole active endpoint")
	}
	l.Close()
	_, close, e := listenSharedRuns(p)
	if e != nil {
		t.Fatal(e)
	}
	if _, c, e := listenSharedRuns(p); e == nil {
		c()
		t.Fatal("duplicate owner")
	}
	close()
	if e = os.WriteFile(p, []byte("preserve"), 0600); e != nil {
		t.Fatal(e)
	}
	if _, c, e := listenSharedRuns(p); e == nil {
		c()
		t.Fatal("removed non-socket")
	}
	b, _ := os.ReadFile(p)
	if string(b) != "preserve" {
		t.Fatal("changed unrelated file")
	}
}
