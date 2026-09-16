package postgres

import (
	"context"
	"errors"
	"regexp"
	"testing"
	"time"

	"github.com/excalibase/watcher-go/internal/metrics"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func testRules(clock *fakeClock) orphanRules {
	return orphanRules{
		ownSlot:           "cdc_mine",
		pattern:           regexp.MustCompile(`^cdc_`),
		staleAfter:        30 * time.Minute,
		retainedThreshold: 0,
		now:               clock.Now,
	}
}

func TestOrphanDecision(t *testing.T) {
	clock := newFakeClock()
	now := clock.Now()
	released := now.Add(-time.Minute)
	cases := []struct {
		name              string
		candidate         slotCandidate
		firstSeenInactive time.Time
		wantDrop          bool
		wantReason        string
	}{
		{
			name:       "own slot is never dropped even when stale",
			candidate:  slotCandidate{Name: "cdc_mine", Owner: &slotOwner{HeartbeatAt: now.Add(-2 * time.Hour)}},
			wantReason: "own slot",
		},
		{
			name:       "pattern mismatch is never dropped",
			candidate:  slotCandidate{Name: "debezium", Owner: &slotOwner{HeartbeatAt: now.Add(-2 * time.Hour)}},
			wantReason: "name does not match pattern",
		},
		{
			name:       "active slot is kept",
			candidate:  slotCandidate{Name: "cdc_other", Active: true, Owner: &slotOwner{HeartbeatAt: now.Add(-2 * time.Hour)}},
			wantReason: "active",
		},
		{
			name:       "recent heartbeat is kept",
			candidate:  slotCandidate{Name: "cdc_other", Owner: &slotOwner{HeartbeatAt: now.Add(-5 * time.Minute)}},
			wantReason: "recent heartbeat",
		},
		{
			name:       "stale heartbeat is dropped",
			candidate:  slotCandidate{Name: "cdc_other", Owner: &slotOwner{HeartbeatAt: now.Add(-31 * time.Minute)}},
			wantDrop:   true,
			wantReason: "heartbeat stale",
		},
		{
			name:       "released by owner is dropped immediately",
			candidate:  slotCandidate{Name: "cdc_other", Owner: &slotOwner{HeartbeatAt: now, ReleasedAt: &released}},
			wantDrop:   true,
			wantReason: "released by owner",
		},
		{
			name:              "no registry row: kept until observed inactive for staleAfter",
			candidate:         slotCandidate{Name: "cdc_legacy"},
			firstSeenInactive: now.Add(-10 * time.Minute),
			wantReason:        "recent heartbeat",
		},
		{
			name:              "no registry row: dropped after observed inactive for staleAfter",
			candidate:         slotCandidate{Name: "cdc_legacy"},
			firstSeenInactive: now.Add(-31 * time.Minute),
			wantDrop:          true,
			wantReason:        "heartbeat stale",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := testRules(clock).decide(c.candidate, c.firstSeenInactive)
			if got.Drop != c.wantDrop {
				t.Errorf("Drop = %v, want %v (reason %q)", got.Drop, c.wantDrop, got.Reason)
			}
			if got.Reason != c.wantReason {
				t.Errorf("Reason = %q, want %q", got.Reason, c.wantReason)
			}
		})
	}
}

func TestOrphanDecisionRetainedWALThreshold(t *testing.T) {
	clock := newFakeClock()
	rules := testRules(clock)
	rules.retainedThreshold = 1024
	stale := slotOwner{HeartbeatAt: clock.Now().Add(-time.Hour)}

	below := rules.decide(slotCandidate{Name: "cdc_a", RetainedWALBytes: 1024, Owner: &stale}, time.Time{})
	if below.Drop {
		t.Errorf("retained WAL at threshold must be kept, got reason %q", below.Reason)
	}
	above := rules.decide(slotCandidate{Name: "cdc_a", RetainedWALBytes: 1025, Owner: &stale}, time.Time{})
	if !above.Drop {
		t.Errorf("retained WAL above threshold must be dropped, got reason %q", above.Reason)
	}
}

type fakeSlotStore struct {
	candidates []slotCandidate
	fetchErr   error
	dropped    []string
	dropErr    error
}

func (f *fakeSlotStore) fetch(context.Context) ([]slotCandidate, error) {
	return f.candidates, f.fetchErr
}

func (f *fakeSlotStore) drop(_ context.Context, name string) error {
	f.dropped = append(f.dropped, name)
	return f.dropErr
}

func newTestCleaner(clock *fakeClock, store *fakeSlotStore, dryRun bool) *slotCleaner {
	return newSlotCleaner(testRules(clock), time.Hour, dryRun, store.fetch, store.drop)
}

func TestCleanupOnceDropsOnlyOrphans(t *testing.T) {
	clock := newFakeClock()
	stale := &slotOwner{HeartbeatAt: clock.Now().Add(-time.Hour)}
	fresh := &slotOwner{HeartbeatAt: clock.Now()}
	store := &fakeSlotStore{candidates: []slotCandidate{
		{Name: "cdc_mine", Owner: stale},
		{Name: "cdc_orphan", Owner: stale},
		{Name: "cdc_live", Active: true, Owner: stale},
		{Name: "cdc_fresh", Owner: fresh},
		{Name: "pgl_other", Owner: stale},
	}}
	before := testutil.ToFloat64(metrics.SlotsDropped)

	if err := newTestCleaner(clock, store, false).cleanupOnce(context.Background()); err != nil {
		t.Fatalf("cleanupOnce: %v", err)
	}

	if len(store.dropped) != 1 || store.dropped[0] != "cdc_orphan" {
		t.Errorf("dropped = %v, want [cdc_orphan]", store.dropped)
	}
	if got := testutil.ToFloat64(metrics.SlotsDropped) - before; got != 1 {
		t.Errorf("cdc_slots_dropped_total delta = %v, want 1", got)
	}
}

func TestCleanupOnceDryRunDropsNothing(t *testing.T) {
	clock := newFakeClock()
	store := &fakeSlotStore{candidates: []slotCandidate{
		{Name: "cdc_orphan", Owner: &slotOwner{HeartbeatAt: clock.Now().Add(-time.Hour)}},
	}}
	before := testutil.ToFloat64(metrics.SlotsDropped)

	if err := newTestCleaner(clock, store, true).cleanupOnce(context.Background()); err != nil {
		t.Fatalf("cleanupOnce: %v", err)
	}

	if len(store.dropped) != 0 {
		t.Errorf("dry run dropped %v", store.dropped)
	}
	if got := testutil.ToFloat64(metrics.SlotsDropped) - before; got != 0 {
		t.Errorf("dry run must not count drops, delta = %v", got)
	}
}

func TestCleanupOnceTracksFirstSeenInactiveForUnregisteredSlots(t *testing.T) {
	clock := newFakeClock()
	store := &fakeSlotStore{candidates: []slotCandidate{{Name: "cdc_legacy"}}}
	cleaner := newTestCleaner(clock, store, false)

	_ = cleaner.cleanupOnce(context.Background())
	if len(store.dropped) != 0 {
		t.Fatalf("first observation must not drop, got %v", store.dropped)
	}

	clock.Advance(31 * time.Minute)
	_ = cleaner.cleanupOnce(context.Background())
	if len(store.dropped) != 1 || store.dropped[0] != "cdc_legacy" {
		t.Errorf("dropped = %v, want [cdc_legacy] after staleAfter of observed inactivity", store.dropped)
	}
	if _, tracked := cleaner.inactiveSince["cdc_legacy"]; tracked {
		t.Error("dropped slot must be forgotten")
	}
}

func TestCleanupOnceForgetsSlotThatBecameActive(t *testing.T) {
	clock := newFakeClock()
	store := &fakeSlotStore{candidates: []slotCandidate{{Name: "cdc_legacy"}}}
	cleaner := newTestCleaner(clock, store, false)

	_ = cleaner.cleanupOnce(context.Background())
	store.candidates = []slotCandidate{{Name: "cdc_legacy", Active: true}}
	_ = cleaner.cleanupOnce(context.Background())

	if _, tracked := cleaner.inactiveSince["cdc_legacy"]; tracked {
		t.Error("active slot must not keep its inactive-since mark")
	}
}

func TestCleanupOnceReportsFetchAndDropErrors(t *testing.T) {
	clock := newFakeClock()
	store := &fakeSlotStore{fetchErr: errors.New("db hibernated")}
	if err := newTestCleaner(clock, store, false).cleanupOnce(context.Background()); err == nil {
		t.Error("expected fetch error")
	}

	store = &fakeSlotStore{
		candidates: []slotCandidate{{Name: "cdc_orphan", Owner: &slotOwner{HeartbeatAt: clock.Now().Add(-time.Hour)}}},
		dropErr:    errors.New("permission denied"),
	}
	before := testutil.ToFloat64(metrics.SlotsDropped)
	if err := newTestCleaner(clock, store, false).cleanupOnce(context.Background()); err == nil {
		t.Error("expected drop error")
	}
	if got := testutil.ToFloat64(metrics.SlotsDropped) - before; got != 0 {
		t.Errorf("failed drop must not count, delta = %v", got)
	}
}

func TestCleanerRunExecutesAtStartupAndOnTicker(t *testing.T) {
	calls := 0
	fetch := func(context.Context) ([]slotCandidate, error) {
		calls++
		return nil, nil
	}
	cleaner := newSlotCleaner(testRules(newFakeClock()), 10*time.Millisecond, false, fetch, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	cleaner.run(ctx)

	if calls < 3 {
		t.Errorf("cleanup ran %d times, want startup + ticks", calls)
	}
}

func TestCompileSlotPattern(t *testing.T) {
	if _, err := compileSlotPattern("^cdc_"); err != nil {
		t.Errorf("valid pattern rejected: %v", err)
	}
	if _, err := compileSlotPattern("("); err == nil {
		t.Error("invalid pattern accepted")
	}
	if _, err := compileSlotPattern(""); err == nil {
		t.Error("empty pattern must be rejected: it would match every slot")
	}
}
