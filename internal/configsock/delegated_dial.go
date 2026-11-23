package configsock

import (
	"context"
	"errors"
	"net"
	"sync"
)

// A delegated connection is connected by the exact per-run native proxy before
// its FD is transferred. SO_PEERCRED still names that proxy, never the manager.
// Keys are private per-run aliases; normal sockets cannot be shadowed.
var delegatedDials sync.Map

func RegisterTaskDialer(alias string, dial func(context.Context) (net.Conn, error)) (func(), error) {
	if dial == nil {
		return nil, errors.New("nil delegated dialer")
	}
	if _, loaded := delegatedDials.LoadOrStore(alias, dial); loaded {
		return nil, errors.New("duplicate delegated task transport")
	}
	return func() { delegatedDials.Delete(alias) }, nil
}
func delegatedDial(ctx context.Context, socket string) (net.Conn, error) {
	if d, ok := delegatedDials.Load(socket); ok {
		return d.(func(context.Context) (net.Conn, error))(ctx)
	}
	var d net.Dialer
	return d.DialContext(ctx, "unix", socket)
}
