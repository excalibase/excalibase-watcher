package postgres

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// slotOwnersTable records which watcher owns which replication slot. Every
// watcher upserts its row at startup, heartbeats it while running and marks
// released_at on graceful shutdown. Slot cleanup reads it to tell a slot whose
// owner is merely reconnecting from one nobody will ever resume.
const slotOwnersTable = "excalibase_cdc_slot_owners"

const slotOwnersDDL = `
CREATE TABLE IF NOT EXISTS ` + slotOwnersTable + ` (
	slot_name    text PRIMARY KEY,
	owner_id     text NOT NULL,
	heartbeat_at timestamptz NOT NULL DEFAULT now(),
	released_at  timestamptz
)`

const registerSQL = `
INSERT INTO ` + slotOwnersTable + ` (slot_name, owner_id, heartbeat_at, released_at)
VALUES ($1, $2, now(), NULL)
ON CONFLICT (slot_name) DO UPDATE
	SET owner_id = EXCLUDED.owner_id, heartbeat_at = now(), released_at = NULL`

const heartbeatSQL = `UPDATE ` + slotOwnersTable + ` SET heartbeat_at = now() WHERE slot_name = $1 AND owner_id = $2`

const releaseSQL = `UPDATE ` + slotOwnersTable + ` SET released_at = now() WHERE slot_name = $1 AND owner_id = $2`

type execer interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

type slotRegistry struct {
	slot  string
	owner string
}

func newSlotRegistry(slot, owner string) *slotRegistry {
	return &slotRegistry{slot: slot, owner: owner}
}

func (r *slotRegistry) ensureTable(ctx context.Context, db execer) error {
	if _, err := db.Exec(ctx, slotOwnersDDL); err != nil {
		return fmt.Errorf("creating %s: %w", slotOwnersTable, err)
	}
	return nil
}

func (r *slotRegistry) register(ctx context.Context, db execer) error {
	if _, err := db.Exec(ctx, registerSQL, r.slot, r.owner); err != nil {
		return fmt.Errorf("registering slot owner: %w", err)
	}
	return nil
}

func (r *slotRegistry) heartbeat(ctx context.Context, db execer) error {
	if _, err := db.Exec(ctx, heartbeatSQL, r.slot, r.owner); err != nil {
		return fmt.Errorf("slot owner heartbeat: %w", err)
	}
	return nil
}

// release marks the row so the next cleanup may drop the slot without waiting
// for the heartbeat to go stale. Called on graceful shutdown (SIGTERM).
func (r *slotRegistry) release(ctx context.Context, db execer) error {
	if _, err := db.Exec(ctx, releaseSQL, r.slot, r.owner); err != nil {
		return fmt.Errorf("releasing slot owner: %w", err)
	}
	return nil
}

func (r *slotRegistry) runHeartbeat(ctx context.Context, db execer, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			beatCtx, cancel := context.WithTimeout(ctx, maintenanceQueryTimeout)
			err := r.heartbeat(beatCtx, db)
			cancel()
			if err != nil && ctx.Err() == nil {
				slog.Warn("slot owner heartbeat failed", "error", err)
			}
		}
	}
}
