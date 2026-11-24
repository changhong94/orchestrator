package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/sandboxer/pkg/controllease"
	"github.com/kuasar-sandbox/sandboxer/pkg/instanceenv"
	"github.com/kuasar-sandbox/sandboxer/pkg/restore"
	"golang.org/x/sys/unix"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

var launchSharedPrepared func(context.Context, string, string, []*os.File, []restore.StagedCatalog) (func(), <-chan error, int, int, error)
var launchSharedRun func(context.Context, string, []string, []string, *os.File, *os.File) error

type dialResult struct {
	conn net.Conn
	err  error
}
type runBroker struct {
	conn    *net.UnixConn
	mu      sync.Mutex
	replies chan dialResult
	done    chan struct{}
	cancel  context.CancelFunc
}

func (b *runBroker) read() {
	defer close(b.done)
	defer b.cancel()
	for {
		buf := make([]byte, 32)
		oob := make([]byte, unix.CmsgSpace(4))
		n, on, flags, _, e := b.conn.ReadMsgUnix(buf, oob)
		if e != nil {
			return
		}
		var fds []int
		cms, e := unix.ParseSocketControlMessage(oob[:on])
		if e != nil {
			return
		}
		for _, cm := range cms {
			got, e := unix.ParseUnixRights(&cm)
			if e != nil {
				return
			}
			fds = append(fds, got...)
		}
		for _, fd := range fds {
			unix.CloseOnExec(fd)
		}
		if flags&(unix.MSG_TRUNC|unix.MSG_CTRUNC) != 0 || n != 1 {
			for _, fd := range fds {
				unix.Close(fd)
			}
			return
		}
		if buf[0] == 'C' {
			for _, fd := range fds {
				unix.Close(fd)
			}
			b.cancel()
			continue
		}
		result := dialResult{err: errors.New("native proxy could not connect config socket")}
		if buf[0] == 'R' && len(fds) == 1 {
			f := os.NewFile(uintptr(fds[0]), "authenticated-task-socket")
			result.conn, result.err = net.FileConn(f)
			f.Close()
		} else {
			for _, fd := range fds {
				unix.Close(fd)
			}
		}
		select {
		case b.replies <- result:
		default:
			if result.conn != nil {
				result.conn.Close()
			}
			return
		}
	}
}
func (b *runBroker) dial(ctx context.Context) (net.Conn, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	select {
	case <-b.done:
		return nil, errors.New("native task proxy closed")
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	if _, e := b.conn.Write([]byte("D")); e != nil {
		return nil, e
	}
	select {
	case v := <-b.replies:
		return v.conn, v.err
	case <-b.done:
		return nil, errors.New("native task proxy closed")
	case <-ctx.Done():
		// Close the lease rather than let a late reply be mistaken for another dial.
		b.conn.Close()
		return nil, ctx.Err()
	}
}

func serveSharedRuns(ctx context.Context, path string) error {
	if executable, err := os.Executable(); err == nil && len(os.Getenv("KUASAR_EMBEDDED_SANDBOX_SHA256")) == 64 {
		done := make(chan struct{})
		expected := os.Getenv("KUASAR_EMBEDDED_SANDBOX_SHA256")
		go func() {
			defer close(done)
			_ = verifyExecutable(filepath.Join(filepath.Dir(executable), "sandbox-ctl"), expected, true)
		}()
		defer func() { <-done }()
	}

	l, closeListener, e := listenSharedRuns(path)
	if e != nil {
		return e
	}
	defer closeListener()
	if e = os.Chmod(path, 0600); e != nil {
		return e
	}
	stop := context.AfterFunc(ctx, func() { l.Close() })
	defer stop()
	var wg sync.WaitGroup
	defer wg.Wait()
	for {
		c, e := l.AcceptUnix()
		if e != nil {
			if ctx.Err() != nil {
				return nil
			}
			return e
		}
		wg.Add(1)
		go func() { defer wg.Done(); serveSharedRun(ctx, c) }()
	}
}
func serveSharedRun(parent context.Context, c *net.UnixConn) {
	defer c.Close()
	c.SetReadDeadline(time.Now().Add(30 * time.Second))
	raw, oob := make([]byte, 128<<10), make([]byte, unix.CmsgSpace(4))
	n, on, flags, _, e := c.ReadMsgUnix(raw, oob)
	if e != nil {
		return
	}
	var fds []int
	cms, e := unix.ParseSocketControlMessage(oob[:on])
	if e != nil {
		return
	}
	for _, cm := range cms {
		ns, e := unix.ParseUnixRights(&cm)
		if e != nil {
			return
		}
		fds = append(fds, ns...)
	}
	defer func() {
		for _, fd := range fds {
			unix.Close(fd)
		}
	}()
	if len(fds) != 1 || flags&(unix.MSG_TRUNC|unix.MSG_CTRUNC) != 0 {
		return
	}
	unix.CloseOnExec(fds[0])
	// Dup into an os.File owner; raw receiver FDs are independently closed above.
	owned, e := unix.FcntlInt(uintptr(fds[0]), unix.F_DUPFD_CLOEXEC, 3)
	if e != nil {
		return
	}
	cg := os.NewFile(uintptr(owned), "shared-run-vmm")
	defer cg.Close()
	sr, e := c.SyscallConn()
	if e != nil {
		return
	}
	var peer *unix.Ucred
	ce := sr.Control(func(fd uintptr) { peer, e = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED) })
	if ce != nil || e != nil {
		return
	}
	if e = controllease.AuthorizeRunner(peer, cg); e != nil {
		slog.Error("shared run authorization rejected", "err", e)
		return
	}
	var req sharedRunRequest
	if json.Unmarshal(raw[:n], &req) != nil {
		return
	}
	clear(raw)
	if req.RunID == "" || strings.ContainsAny(req.RunID, "/\\\x00") || !filepath.IsAbs(req.Socket) || !filepath.IsAbs(req.Pidfile) {
		return
	}
	pid, e := os.ReadFile(req.Pidfile)
	if e != nil || strings.TrimSpace(string(pid)) != strconv.Itoa(int(peer.Pid)) {
		return
	}
	link, e := os.Readlink(fmt.Sprintf("/proc/self/fd/%d", cg.Fd()))
	if e != nil || !strings.HasSuffix(link, "@"+req.RunID+".service/vmm") {
		return
	}
	scoped, e := instanceenv.With(parent, req.Env)
	if e != nil {
		return
	}
	scoped = instanceenv.WithShared(scoped)
	ctx, cancel := context.WithCancel(scoped)
	defer cancel()
	broker := &runBroker{conn: c, replies: make(chan dialResult, 1), done: make(chan struct{}), cancel: cancel}
	c.SetReadDeadline(time.Time{})
	go broker.read()
	alias := req.Socket + ".shared-" + req.RunID
	unregister, e := configsock.RegisterTaskDialer(alias, broker.dial)
	if e != nil {
		return
	}
	defer unregister()
	e = runSharedTask(ctx, req, alias, cg)
	code := byte('0')
	if e != nil {
		code = '1'
		slog.Error("shared task exited", "run_id", req.RunID, "err", e)
	}
	c.Write([]byte{'X', code})
	c.Close()
	<-broker.done
	select {
	case v := <-broker.replies:
		if v.conn != nil {
			v.conn.Close()
		}
	default:
	}
}
func runSharedTask(ctx context.Context, req sharedRunRequest, socket string, cg *os.File) error {
	traceLast := time.Now()
	trace := func(stage string) {
		if os.Getenv("KUASAR_POOL_TRACE") == "1" {
			now := time.Now()
			fmt.Fprintf(os.Stderr, "POOL_STAGE kind=shared run=%s stage=%s us=%d end_ns=%d tid=%d\n", req.RunID, stage, now.Sub(traceLast).Microseconds(), now.UnixNano(), unix.Gettid())
			traceLast = now
		}
	}
	trace("authorized")
	// The approved executable is normally beside this shared host. Start its
	// read-only integrity work while the authenticated run session/spec RPCs
	// are in flight. This is only a hint: embeddedLauncher still opens/stats
	// the authoritative path and verifies its expected digest before launch.
	// A changed inode/digest or a nonstandard deployment follows that path.
	if executable, err := os.Executable(); err == nil {
		done := make(chan struct{})
		expected := instanceenv.Get(ctx, "KUASAR_EMBEDDED_SANDBOX_SHA256")
		go func() {
			defer close(done)
			_ = verifyExecutable(filepath.Join(filepath.Dir(executable), "sandbox-ctl"), expected, true)
		}()
		defer func() { <-done }()
	}

	session, e := openRunSessionKeeper(ctx, socket, "sandbox", req.RunID)
	if e != nil {
		return e
	}
	defer session.Close()
	trace("session_open")
	prepared, e := startPreparedSandboxContext(ctx, socket, req.RunID, cg)
	if e != nil {
		return e
	}
	waitCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	if prepared != nil {
		defer prepared.Close()
		go func() {
			select {
			case <-prepared.done:
				cancel()
			case <-waitCtx.Done():
			}
		}()
	}
	trace("prepared_ready")
	var bootstrap *configsock.SandboxTaskSpec
	sid, e := waitSandboxAssignmentWithRetry(waitCtx, socket, req.RunID, func(ctx context.Context, socket, kind, rid string) (string, error) {
		trace("assignment_rpc_begin")
		out, e := configsock.WaitSandboxBootstrap(ctx, socket, rid)
		if e != nil {
			return "", e
		}
		bootstrap = out.Bootstrap
		return out.TaskID, nil
	})
	if e != nil {
		return e
	}
	root := filepath.Dir(filepath.Dir(req.Pidfile))
	ready, e := connectReadinessSocket(configsock.ReadinessSocketPath(root, sid))
	if e != nil {
		return e
	}
	defer ready.Close()
	return launchTaskWithBootstrap(ctx, func() {}, socket, sid, req.RunID, ready, cg, slog.Default().With("run_id", req.RunID), bootstrap, prepared)
}

// Once duplicated into the runtime, the caller's ready writer must close.
// Otherwise readiness EOF waits for VM teardown, stalling cold/native creation.
func ownedRunCapabilities(cg, ready *os.File) (*os.File, *os.File, error) {
	duplicate := func(f *os.File) (*os.File, error) {
		n, e := unix.FcntlInt(f.Fd(), unix.F_DUPFD_CLOEXEC, 3)
		if e != nil {
			return nil, e
		}
		return os.NewFile(uintptr(n), "shared-run-owned"), nil
	}
	dc, e := duplicate(cg)
	if e != nil {
		return nil, nil, e
	}
	dr, e := duplicate(ready)
	if e != nil {
		dc.Close()
		return nil, nil, e
	}
	if e = ready.Close(); e != nil {
		dc.Close()
		dr.Close()
		return nil, nil, e
	}
	return dc, dr, nil
}
