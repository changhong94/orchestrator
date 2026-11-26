package store

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/secretbox"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

// This is a feasibility probe, not a new production activation protocol.
// A durable authorization cannot by itself attest to a later guest ACK.
// Exit the writer without Close so recovery observes committed storage, not
// an orderly shutdown which might accidentally persist the missing fact.
func TestLaunchAuthorizationCrashWindow(t *testing.T) {
	const pathEnv = "KUASAR_TEST_AUTHORIZATION_DB"
	const modeEnv = "KUASAR_TEST_AUTHORIZATION_PHASE"
	box, err := secretbox.NewFromColonHex(strings.Repeat("0", 64))
	if err != nil {
		t.Fatal(err)
	}
	if path := os.Getenv(pathEnv); path != "" {
		s, err := Open(path, box)
		if err != nil {
			t.Fatal(err)
		}
		ctx := context.Background()
		sb := testSandboxWithServiceCredentials("authorization-probe", types.ProfileE2B)
		source := types.ResumeSource{Kind: types.ResumeSourceSnapshot, Ref: "manifest://" + strings.Repeat("a", 64), SandboxRef: "manifest://" + strings.Repeat("b", 64)}
		sb.TemplateID = (types.TemplateID{Profile: types.ProfileE2B, Kind: types.KindSnp, Ref: source.Ref}).String()
		sb.State, sb.LaunchMode, sb.RunID = types.StateStarting, types.LaunchMemory, "exact-probe-run"
		if err := s.InsertSandbox(ctx, sb); err != nil {
			t.Fatal(err)
		}
		if _, err := s.db.Exec(`CREATE TABLE feasibility_authorization(sid TEXT PRIMARY KEY,run_id TEXT,source TEXT,sandbox_source TEXT)`); err != nil {
			t.Fatal(err)
		}
		if _, err := s.db.Exec(`INSERT INTO feasibility_authorization VALUES(?,?,?,?)`, sb.ID, sb.RunID, source.Ref, source.SandboxRef); err != nil {
			t.Fatal(err)
		}
		mode := os.Getenv(modeEnv)
		// guest_ack exists only in the old process, exactly as proposed for a
		// final in-memory confirmation. No guest is launched by this store test.
		guestACK := mode == "authorized_after_ack" || mode == "running_after_ack"
		if mode == "running_before_ack" || mode == "running_after_ack" {
			changed, err := s.CommitPreparedRunning(ctx, sb, source)
			if err != nil || !changed {
				t.Fatalf("commit: %v %v", changed, err)
			}
		}
		if guestACK {
			fmt.Println("simulated-guest-ack")
		}
		os.Exit(0)
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"authorized_before_ack", "authorized_after_ack", "running_before_ack", "running_after_ack"} {
		t.Run(mode, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "node.db")
			cmd := exec.Command(exe, "-test.run=^TestLaunchAuthorizationCrashWindow$")
			cmd.Env = append(os.Environ(), pathEnv+"="+path, modeEnv+"="+mode)
			out, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("writer: %v %s", err, out)
			}
			wantACK := strings.HasSuffix(mode, "after_ack")
			if strings.Contains(string(out), "simulated-guest-ack") != wantACK {
				t.Fatal("writer did not reach selected crash boundary")
			}
			s, err := Open(path, box)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			sb, err := s.Get(context.Background(), "authorization-probe")
			if err != nil || sb == nil {
				t.Fatal(err)
			}
			var sid, run, source, parent string
			if err := s.db.QueryRow(`SELECT sid,run_id,source,sandbox_source FROM feasibility_authorization`).Scan(&sid, &run, &source, &parent); err != nil {
				t.Fatal(err)
			}
			if sid != sb.ID || run != sb.RunID || source == "" || parent == "" {
				t.Fatal("durable authorization lost")
			}
			want := types.StateStarting
			if strings.HasPrefix(mode, "running_") {
				want = types.StateRunning
			}
			if sb.State != want {
				t.Fatalf("state %s want %s", sb.State, want)
			}
			t.Logf("after abrupt writer exit: phase=%s durable_state=%s; authorization survives but cannot prove ACK", mode, sb.State)
		})
	}
}
