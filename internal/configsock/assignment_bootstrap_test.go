package configsock

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
)

func TestAssignmentBootstrapRequiresExactTaskPeer(t *testing.T) {
	for _, denied := range []bool{false, true} {
		t.Run(strconv.FormatBool(denied), func(t *testing.T) {
			runPF := filepath.Join(t.TempDir(), "run.pid")
			taskPF := filepath.Join(t.TempDir(), "task.pid")
			mustWrite(t, runPF, strconv.Itoa(os.Getpid()))
			peer := os.Getpid()
			if denied {
				peer++
			}
			mustWrite(t, taskPF, strconv.Itoa(peer))
			var hits atomic.Int32
			sock, _ := startTestServer(t, Deps{Provider: stubProvider{pidFile: runPF, sandboxAuth: SandboxTaskAuth{PidFile: taskPF}, bootstrapHits: &hits}})
			out, err := WaitSandboxBootstrap(context.Background(), sock, "sr-test")
			if denied {
				if err == nil || hits.Load() != 0 {
					t.Fatalf("bootstrap leaked past exact task auth: %v hits=%d", err, hits.Load())
				}
				return
			}
			if err != nil || out.Bootstrap == nil || out.Bootstrap.SandboxID != out.TaskID || hits.Load() != 1 {
				t.Fatalf("bootstrap assignment: %+v %v hits=%d", out, err, hits.Load())
			}
		})
	}
}
