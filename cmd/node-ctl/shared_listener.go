package main

import (
	"errors"
	"fmt"
	"golang.org/x/sys/unix"
	"net"
	"os"
	"path/filepath"
	"syscall"
)

// Keep an exclusive endpoint lock until the listener is closed. After SIGKILL,
// only the new owner may remove a stale socket in the private host directory.
func listenSharedRuns(path string) (*net.UnixListener, func(), error) {
	if !filepath.IsAbs(path) {
		return nil, nil, errors.New("absolute shared endpoint required")
	}
	dir, e := os.Lstat(filepath.Dir(path))
	if e != nil {
		return nil, nil, e
	}
	if !dir.IsDir() || dir.Mode().Perm()&0077 != 0 || dir.Sys().(*syscall.Stat_t).Uid != uint32(os.Geteuid()) {
		return nil, nil, errors.New("shared endpoint requires private owned directory")
	}
	fd, e := unix.Open(path+".lock", unix.O_CREAT|unix.O_RDWR|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0600)
	if e != nil {
		return nil, nil, e
	}
	unlock := func() { unix.Close(fd) }
	if e = unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB); e != nil {
		unlock()
		return nil, nil, e
	}
	st, e := os.Lstat(path)
	if e == nil {
		if st.Mode()&os.ModeSocket == 0 || st.Sys().(*syscall.Stat_t).Uid != uint32(os.Geteuid()) {
			unlock()
			return nil, nil, errors.New("unexpected shared endpoint file")
		}
		c, err := net.DialUnix("unixpacket", nil, &net.UnixAddr{Name: path, Net: "unixpacket"})
		if err == nil {
			c.Close()
			unlock()
			return nil, nil, errors.New("shared endpoint already active")
		}
		if !errors.Is(err, syscall.ECONNREFUSED) {
			unlock()
			return nil, nil, fmt.Errorf("probe shared endpoint: %w", err)
		}
		if e = os.Remove(path); e != nil {
			unlock()
			return nil, nil, e
		}
	} else if !os.IsNotExist(e) {
		unlock()
		return nil, nil, e
	}
	l, e := net.ListenUnix("unixpacket", &net.UnixAddr{Name: path, Net: "unixpacket"})
	if e != nil {
		unlock()
		return nil, nil, e
	}
	return l, func() { l.Close(); unlock() }, nil
}
