package postgres

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"time"

	"github.com/excalibase/watcher-go/internal/metrics"
	"github.com/jackc/pgx/v5"
)

// slotCandidatesSQL lists this database's logical slots joined with the owner
// registry. Standby-safe: see slotStatsSQL for the WAL position expression.
const slotCandidatesSQL = `
SELECT s.slot_name,
       s.active,
       COALESCE(s.restart_lsn::text, ''),
       CASE WHEN pg_is_in_recovery() THEN pg_last_wal_receive_lsn()::text
            ELSE pg_current_wal_lsn()::text END,
       o.owner_id, o.heartbeat_at, o.released_at
FROM pg_replication_slots s
LEFT JOIN ` + slotOwnersTable + ` o ON o.slot_name = s.slot_name
WHERE s.slot_type = 'logical' AND s.database = current_database()`

const dropSlotSQL = `SELECT pg_drop_replication_slot($1)`

const (
	logKeySlot     = "slot"
	logKeyReason   = "reason"
	logKeyRetained = "retained_wal_bytes"
)

type slotOwner struct {
	OwnerID     string
	HeartbeatAt time.Time
	ReleasedAt  *time.Time
}

type slotCandidate struct {
	Name             string
	Active           bool
	RetainedWALBytes uint64
	Owner            *slotOwner // nil when no registry row exists
}

type orphanRules struct {
	ownSlot           string
	pattern           *regexp.Regexp
	staleAfter        time.Duration
	retainedThreshold uint64 // 0 = any amount of retained WAL qualifies
	now               func() time.Time
}

type orphanDecision struct {
	Drop   bool
	Reason string
}

func keep(reason string) orphanDecision { return orphanDecision{Reason: reason} }

func drop(reason string) orphanDecision { return orphanDecision{Drop: true, Reason: reason} }

// decide applies the orphan rules. A slot is dropped only when it matches the
// naming pattern, is not this watcher's own, is inactive, retains more WAL
// than the threshold and either its owner released it or nothing has been
// heard from the owner for staleAfter. For a slot without a registry row the
// time the cleaner first observed it inactive stands in for the heartbeat.
func (r orphanRules) decide(c slotCandidate, firstSeenInactive time.Time) orphanDecision {
	switch {
	case c.Name == r.ownSlot:
		return keep("own slot")
	case !r.pattern.MatchString(c.Name):
		return keep("name does not match pattern")
	case c.Active:
		return keep("active")
	case r.retainedThreshold > 0 && c.RetainedWALBytes <= r.retainedThreshold:
		return keep("retained wal below threshold")
	case c.Owner != nil && c.Owner.ReleasedAt != nil:
		return drop("released by owner")
	}
	lastAlive := firstSeenInactive
	if c.Owner != nil {
		lastAlive = c.Owner.HeartbeatAt
	}
	if r.now().Sub(lastAlive) < r.staleAfter {
		return keep("recent heartbeat")
	}
	return drop("heartbeat stale")
}

func compileSlotPattern(pattern string) (*regexp.Regexp, error) {
	if pattern == "" {
		return nil, errors.New("slot_cleanup.slot_pattern must not be empty")
	}
	return regexp.Compile(pattern)
}

type slotCleaner struct {
	rules    orphanRules
	interval time.Duration
	dryRun   bool
	fetch    func(ctx context.Context) ([]slotCandidate, error)
	drop     func(ctx context.Context, name string) error
	// inactiveSince remembers when a slot without a registry row was first
	// seen inactive, so an older watcher that is only reconnecting is spared.
	inactiveSince map[string]time.Time
}

func newSlotCleaner(rules orphanRules, interval time.Duration, dryRun bool,
	fetch func(context.Context) ([]slotCandidate, error), drop func(context.Context, string) error) *slotCleaner {
	return &slotCleaner{
		rules:         rules,
		interval:      interval,
		dryRun:        dryRun,
		fetch:         fetch,
		drop:          drop,
		inactiveSince: make(map[string]time.Time),
	}
}

func (c *slotCleaner) run(ctx context.Context) {
	c.cleanupAndLog(ctx)
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.cleanupAndLog(ctx)
		}
	}
}

func (c *slotCleaner) cleanupAndLog(ctx context.Context) {
	if err := c.cleanupOnce(ctx); err != nil && ctx.Err() == nil {
		slog.Warn("replication slot cleanup failed", "error", err)
	}
}

func (c *slotCleaner) cleanupOnce(ctx context.Context) error {
	candidates, err := c.fetch(ctx)
	if err != nil {
		return err
	}
	var errs []error
	for _, candidate := range candidates {
		decision := c.rules.decide(candidate, c.observeInactive(candidate))
		if !decision.Drop {
			slog.Debug("replication slot kept", logKeySlot, candidate.Name, logKeyReason, decision.Reason)
			continue
		}
		if err := c.dropSlot(ctx, candidate, decision.Reason); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// observeInactive tracks first-seen-inactive for unregistered slots and
// returns the mark to use for this candidate.
func (c *slotCleaner) observeInactive(candidate slotCandidate) time.Time {
	if candidate.Active || candidate.Owner != nil {
		delete(c.inactiveSince, candidate.Name)
		return time.Time{}
	}
	firstSeen, seen := c.inactiveSince[candidate.Name]
	if !seen {
		firstSeen = c.rules.now()
		c.inactiveSince[candidate.Name] = firstSeen
	}
	return firstSeen
}

func (c *slotCleaner) dropSlot(ctx context.Context, candidate slotCandidate, reason string) error {
	if c.dryRun {
		slog.Info("dry run: would drop orphaned replication slot",
			logKeySlot, candidate.Name, logKeyReason, reason, logKeyRetained, candidate.RetainedWALBytes)
		return nil
	}
	if err := c.drop(ctx, candidate.Name); err != nil {
		return fmt.Errorf("dropping slot %s: %w", candidate.Name, err)
	}
	delete(c.inactiveSince, candidate.Name)
	metrics.IncSlotDropped()
	slog.Info("dropped orphaned replication slot",
		logKeySlot, candidate.Name, logKeyReason, reason, logKeyRetained, candidate.RetainedWALBytes)
	return nil
}

type slotQuerier interface {
	rowQuerier
	execer
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

func fetchSlotCandidates(ctx context.Context, db slotQuerier) ([]slotCandidate, error) {
	rows, err := db.Query(ctx, slotCandidatesSQL)
	if err != nil {
		return nil, fmt.Errorf("listing replication slots: %w", err)
	}
	defer rows.Close()

	var candidates []slotCandidate
	for rows.Next() {
		candidate, err := scanSlotCandidate(rows)
		if err != nil {
			return nil, err
		}
		candidates = append(candidates, candidate)
	}
	return candidates, rows.Err()
}

func scanSlotCandidate(rows pgx.Rows) (slotCandidate, error) {
	var candidate slotCandidate
	var restart, wal string
	var ownerID *string
	var heartbeatAt, releasedAt *time.Time
	if err := rows.Scan(&candidate.Name, &candidate.Active, &restart, &wal, &ownerID, &heartbeatAt, &releasedAt); err != nil {
		return slotCandidate{}, fmt.Errorf("scanning replication slot: %w", err)
	}
	stats := SlotStats{}
	var err error
	if stats.RestartLSN, err = parseLSNText(restart); err != nil {
		return slotCandidate{}, err
	}
	if stats.WALLSN, err = parseLSNText(wal); err != nil {
		return slotCandidate{}, err
	}
	candidate.RetainedWALBytes = stats.RetainedWALBytes()
	if ownerID != nil && heartbeatAt != nil {
		candidate.Owner = &slotOwner{OwnerID: *ownerID, HeartbeatAt: *heartbeatAt, ReleasedAt: releasedAt}
	}
	return candidate, nil
}

func dropReplicationSlot(ctx context.Context, db execer, name string) error {
	_, err := db.Exec(ctx, dropSlotSQL, name)
	return err
}
