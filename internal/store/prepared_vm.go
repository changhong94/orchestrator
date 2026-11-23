package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

// PreparedVMLease is durable host-only ownership, never tenant credentials.
// The exact runner remains responsible until AdoptPreparedVM atomically moves
// its port to the new sandbox row. Size of the reserve is unrelated to capacity.
type PreparedVMLease struct {
	RunID      string          `json:"run_id"`
	Port       string          `json:"port"`
	FloatingIP string          `json:"floating_ip"`
	InnerIP    string          `json:"inner_ip"`
	MAC        string          `json:"mac"`
	Spec       json.RawMessage `json:"spec"`
}

func (s *Store) GetPreparedVM(ctx context.Context, runID string) (*PreparedVMLease, error) {
	var raw string
	err := s.db.QueryRowContext(ctx, "SELECT record FROM prepared_vm_leases WHERE run_id=?", runID).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var lease PreparedVMLease
	if err = json.Unmarshal([]byte(raw), &lease); err != nil {
		return nil, err
	}
	if lease.RunID != runID {
		return nil, errors.New("prepared VM owner mismatch")
	}
	return &lease, nil
}

func (s *Store) SavePreparedVM(ctx context.Context, lease PreparedVMLease) error {
	if lease.RunID == "" || lease.Port == "" || lease.InnerIP == "" || lease.MAC == "" || !json.Valid(lease.Spec) {
		return errors.New("invalid prepared VM lease")
	}
	raw, err := json.Marshal(lease)
	if err != nil {
		return err
	}
	result, err := s.db.ExecContext(ctx, `INSERT INTO prepared_vm_leases(run_id,record)
 SELECT ?,? WHERE EXISTS(SELECT 1 FROM run_pool_ownership WHERE run_id=?)`, lease.RunID, string(raw), lease.RunID)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return errors.New("prepared VM runner ownership lost")
	}
	return nil
}

// DeletePreparedVM follows confirmed runner termination and exact port release.
func (s *Store) DeletePreparedVM(ctx context.Context, runID string) error {
	_, err := s.db.ExecContext(ctx, "DELETE FROM prepared_vm_leases WHERE run_id=?", runID)
	return err
}

// AdoptPreparedVM has one commit point: neither a crash nor concurrent claims
// can leave the port unowned or owned by both a pool entry and a sandbox.
func (s *Store) AdoptPreparedVM(ctx context.Context, sid, runID string) (*PreparedVMLease, error) {
	if sid == "" || sid != runID {
		return nil, errors.New("prepared VM identity cannot be renamed")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	var raw string
	if err = tx.QueryRowContext(ctx, "SELECT record FROM prepared_vm_leases WHERE run_id=?", runID).Scan(&raw); err != nil {
		return nil, err
	}
	var lease PreparedVMLease
	if err = json.Unmarshal([]byte(raw), &lease); err != nil {
		return nil, err
	}
	if lease.RunID != runID {
		return nil, errors.New("prepared VM lease identity mismatch")
	}
	result, err := tx.ExecContext(ctx, `UPDATE sandboxes SET run_id=?,floatingip=?,vswitch_port=?,inner_ip=?,port_mac=?
 WHERE id=? AND state=? AND run_id='' AND vswitch_port=''`, runID, lease.FloatingIP, lease.Port, lease.InnerIP, lease.MAC, sid, types.StateStarting)
	if err != nil {
		return nil, err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return nil, err
	}
	if n != 1 {
		return nil, fmt.Errorf("prepared VM target ownership lost: %s", sid)
	}
	if _, err = tx.ExecContext(ctx, "DELETE FROM prepared_vm_leases WHERE run_id=?", runID); err != nil {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return &lease, nil
}
