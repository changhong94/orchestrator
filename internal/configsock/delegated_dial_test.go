package configsock

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
)

func TestDelegatedTaskDialersRemainIsolated(t *testing.T) {
	for _, id := range []string{"a", "b"} {
		id := id
		remove, e := RegisterTaskDialer("/private/"+id, func(context.Context) (net.Conn, error) {
			c, s := net.Pipe()
			go func() {
				defer s.Close()
				buf := make([]byte, 4096)
				s.Read(buf)
				fmt.Fprintf(s, "HTTP/1.1 200 OK\r\nContent-Length: 1\r\nConnection: close\r\n\r\n%s", id)
			}()
			return c, nil
		})
		if e != nil {
			t.Fatal(e)
		}
		defer remove()
	}
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		for _, id := range []string{"a", "b"} {
			wg.Add(1)
			go func(id string) {
				defer wg.Done()
				r, e := HTTPClient("/private/" + id).Get("http://localhost/check")
				if e != nil {
					t.Error(e)
					return
				}
				defer r.Body.Close()
				b, e := io.ReadAll(r.Body)
				if e != nil || string(b) != id {
					t.Errorf("cross-run response %q: %v", b, e)
				}
			}(id)
		}
	}
	wg.Wait()
	if _, e := RegisterTaskDialer("/private/a", func(context.Context) (net.Conn, error) { return nil, nil }); e == nil {
		t.Fatal("replaced another run's transport")
	}
}
