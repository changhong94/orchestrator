package main

// This entry's dependencies deliberately exclude the VM engine and conductor.
// Both full and registration-only launchers use exactly the same PID lock,
// cgroup validation, sealed request and same-PID native proxy handoff.
import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"golang.org/x/sys/unix"
	"os"
	"path/filepath"
	"strconv"
)

type sharedRunRequest struct {
	RunID, Pidfile, Socket string
	Env                    []string
}

// The initial Go launcher execs a native lifetime/credential proxy. PID and
// POSIX pidfile lock survive exec; no Go heap remains in the instance unit.
func registerSharedRun(args []string) error {
	fs := flag.NewFlagSet("register-shared", flag.ContinueOnError)
	pidfile := fs.String("pidfile", "", "")
	socket := fs.String("config-socket", "", "")
	rid := fs.String("run-id", "", "")
	if e := fs.Parse(args); e != nil {
		return e
	}
	envDefault(pidfile, "TASK_PIDFILE")
	envDefault(socket, "TASK_CONFIG_SOCKET")
	envDefault(rid, "TASK_RUN_ID")
	manager := os.Getenv("KUASAR_SHARED_RUN_SOCKET")
	helper := os.Getenv("KUASAR_NATIVE_RUN_PROXY")
	if !filepath.IsAbs(*pidfile) || !filepath.IsAbs(*socket) || !filepath.IsAbs(manager) || !filepath.IsAbs(helper) || *rid == "" {
		return errors.New("absolute shared-run endpoints and run identity required")
	}
	lock, e := openLockedPidfile(*pidfile)
	if e != nil {
		return e
	}
	defer unix.Close(lock)
	cg, e := prepareRunnerCgroup(*rid)
	if e != nil {
		return e
	}
	defer cg.Close()
	raw, e := json.Marshal(sharedRunRequest{RunID: *rid, Pidfile: *pidfile, Socket: *socket, Env: os.Environ()})
	if e != nil {
		return e
	}
	defer clear(raw)
	fd, e := unix.MemfdCreate("shared-run-registration", unix.MFD_CLOEXEC|unix.MFD_ALLOW_SEALING)
	if e != nil {
		return e
	}
	defer unix.Close(fd)
	if _, e = unix.Write(fd, raw); e != nil {
		return e
	}
	if _, e = unix.Seek(fd, 0, 0); e != nil {
		return e
	}
	if _, e = unix.FcntlInt(uintptr(fd), unix.F_ADD_SEALS, unix.F_SEAL_WRITE|unix.F_SEAL_SHRINK|unix.F_SEAL_GROW|unix.F_SEAL_SEAL); e != nil {
		return e
	}
	for _, n := range []int{fd, lock, int(cg.Fd())} {
		if _, e = unix.FcntlInt(uintptr(n), unix.F_SETFD, 0); e != nil {
			return e
		}
	}
	return unix.Exec(helper, []string{helper, manager, *socket, strconv.Itoa(fd), strconv.Itoa(int(cg.Fd())), strconv.Itoa(lock)}, os.Environ())
}

func envDefault(p *string, key string) {
	if *p == "" {
		*p = os.Getenv(key)
	}
}

// lockPidfile opens path, takes a non-blocking exclusive POSIX lock (fails if another
// instance already holds it — the double-start guard), and writes our pid. The
// descriptor stays open in the parent to retain the lock, but is close-on-exec so
// sandbox-ctl children cannot inherit the parent-owned RunID/Build pidfile. It is
// never closed and never unlinked — the launcher does not clean up the pidfile.
func lockPidfile(path string) error { _, err := openLockedPidfile(path); return err }
func openLockedPidfile(path string) (int, error) {
	fd, err := unix.Open(path, unix.O_RDWR|unix.O_CREAT|unix.O_CLOEXEC, 0o600)
	if err != nil {
		return -1, fmt.Errorf("open pidfile %s: %w", path, err)
	}
	lk := unix.Flock_t{Type: unix.F_WRLCK, Whence: 0, Start: 0, Len: 0}
	if err := unix.FcntlFlock(uintptr(fd), unix.F_SETLK, &lk); err != nil {
		unix.Close(fd)
		return -1, fmt.Errorf("pidfile %s locked (task already running?): %w", path, err)
	}
	if err := unix.Ftruncate(fd, 0); err != nil {
		unix.Close(fd)
		return -1, fmt.Errorf("truncate pidfile: %w", err)
	}
	if _, err := unix.Pwrite(fd, []byte(strconv.Itoa(os.Getpid())+"\n"), 0); err != nil {
		unix.Close(fd)
		return -1, fmt.Errorf("write pidfile: %w", err)
	}
	return fd, nil
}
