package postgres

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/excalibase/watcher-go/internal/metrics"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
)

// slotStatsSQL reads the slot position from pg_replication_slots. On a
// standby pg_current_wal_lsn() raises an error, so the last received WAL
// position is used there instead.
const slotStatsSQL = `
SELECT COALESCE(confirmed_flush_lsn::text, ''),
       COALESCE(restart_lsn::text, ''),
       active,
       CASE WHEN pg_is_in_recovery() THEN pg_last_wal_receive_lsn()::text
            ELSE pg_current_wal_lsn()::text END
FROM pg_replication_slots
WHERE slot_name = $1`

type SlotStats struct {
	ConfirmedFlushLSN uint64
	RestartLSN        uint64
	// WALLSN is pg_current_wal_lsn() on a primary, pg_last_wal_receive_lsn() on a standby.
	WALLSN uint64
	Active bool
}

// RetainedWALBytes is the WAL the slot pins on disk: everything from
// restart_lsn to the current WAL position.
func (s SlotStats) RetainedWALBytes() uint64 {
	if s.RestartLSN == 0 || s.WALLSN < s.RestartLSN {
		return 0
	}
	return s.WALLSN - s.RestartLSN
}

type rowQuerier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

func querySlotStats(ctx context.Context, db rowQuerier, slotName string) (SlotStats, error) {
	var confirmed, restart, wal string
	var stats SlotStats
	err := db.QueryRow(ctx, slotStatsSQL, slotName).Scan(&confirmed, &restart, &stats.Active, &wal)
	if err != nil {
		return SlotStats{}, fmt.Errorf("querying replication slot stats: %w", err)
	}
	if stats.ConfirmedFlushLSN, err = parseLSNText(confirmed); err != nil {
		return SlotStats{}, err
	}
	if stats.RestartLSN, err = parseLSNText(restart); err != nil {
		return SlotStats{}, err
	}
	if stats.WALLSN, err = parseLSNText(wal); err != nil {
		return SlotStats{}, err
	}
	return stats, nil
}

// parseLSNText converts a pg_lsn text value ("0/16B3D80") to its numeric form.
// An empty string (NULL column) is 0.
func parseLSNText(text string) (uint64, error) {
	if text == "" {
		return 0, nil
	}
	lsn, err := pglogrepl.ParseLSN(text)
	if err != nil {
		return 0, fmt.Errorf("parsing pg_lsn %q: %w", text, err)
	}
	return uint64(lsn), nil
}

// slotSampler publishes the slot gauges on a ticker and refreshes the lag
// gauge at the same cadence so it keeps growing while no WAL arrives.
type slotSampler struct {
	interval time.Duration
	lag      *lagTracker
	fetch    func(ctx context.Context) (SlotStats, error)
}

func newSlotSampler(interval time.Duration, lag *lagTracker, fetch func(context.Context) (SlotStats, error)) *slotSampler {
	return &slotSampler{interval: interval, lag: lag, fetch: fetch}
}

func (s *slotSampler) run(ctx context.Context) {
	s.sampleAndLog(ctx)
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.sampleAndLog(ctx)
		}
	}
}

func (s *slotSampler) sampleAndLog(ctx context.Context) {
	if err := s.sampleOnce(ctx); err != nil && ctx.Err() == nil {
		slog.Warn("replication slot stats unavailable", "error", err)
	}
}

func (s *slotSampler) sampleOnce(ctx context.Context) error {
	s.lag.Publish()
	stats, err := s.fetch(ctx)
	if err != nil {
		return err
	}
	metrics.SetSlotStats(stats.ConfirmedFlushLSN, stats.RestartLSN, stats.RetainedWALBytes())
	return nil
}
