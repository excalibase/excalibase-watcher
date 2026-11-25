package mysql

import (
	"testing"

	"github.com/excalibase/watcher-go/internal/cdc"
)

const (
	set1 = serverA + ":1-10"
	set2 = serverA + ":1-11"
	set3 = serverA + ":1-12"
)

func rowEvent(sourceID string) cdc.Event {
	return cdc.Event{Type: cdc.Insert, SourceID: sourceID}
}

func confirmed(g *commitGate) string {
	set, _ := g.Confirmable()
	return set
}

// A transaction with three rows, two of them acked: its boundary must not be
// confirmed, so a restart replays the whole transaction.
func TestGateHoldsBoundaryUntilEveryRowOfTheTransactionIsAcked(t *testing.T) {
	g := newCommitGate()
	g.Enable()
	g.Committed(set1)
	for range 3 {
		g.Hand()
	}
	g.Committed(set2)

	g.ObservePublished(rowEvent("mysql:" + serverA + ":11:0"))
	g.ObservePublished(rowEvent("mysql:" + serverA + ":11:1"))
	if got := confirmed(g); got != set1 {
		t.Fatalf("two of three rows acked: confirmed %q, want %q", got, set1)
	}

	g.ObservePublished(rowEvent("mysql:" + serverA + ":11:2"))
	if got := confirmed(g); got != set2 {
		t.Fatalf("all rows acked: confirmed %q, want %q", got, set2)
	}
}

func TestGateIgnoresEventsItDidNotHand(t *testing.T) {
	g := newCommitGate()
	g.Enable()
	g.Hand()
	g.Committed(set1)

	g.ObservePublished(cdc.Event{Type: cdc.Insert, SourceID: "pg:0/16B3748"})
	g.ObservePublished(cdc.Event{Type: cdc.Insert})
	g.ObservePublished(cdc.Event{Type: cdc.Commit, SourceID: "mysql:" + serverA + ":11:0"})
	if _, ok := g.Confirmable(); ok {
		t.Fatal("foreign or unpublishable events must not confirm a MySQL boundary")
	}
}

func TestGateWithoutPublisherConfirmsEveryBoundary(t *testing.T) {
	g := newCommitGate()
	g.Hand()
	g.Committed(set1)
	if got := confirmed(g); got != set1 {
		t.Fatalf("ungated: confirmed %q, want %q", got, set1)
	}
}

// Boundaries with nothing handed between them (filtered tables) collapse
// into one entry carrying the newest set.
func TestGateCoalescesBoundariesWaitingOnTheSameEvents(t *testing.T) {
	g := newCommitGate()
	g.Enable()
	g.Hand()
	g.Committed(set1)
	g.Committed(set2)
	if len(g.commits) != 1 {
		t.Fatalf("pending boundaries = %d, want 1", len(g.commits))
	}
	g.ObservePublished(rowEvent("mysql:" + serverA + ":9:0"))
	if got := confirmed(g); got != set2 {
		t.Fatalf("confirmed %q, want %q", got, set2)
	}
}

func TestGateNeverConfirmsAnEmptySet(t *testing.T) {
	g := newCommitGate()
	g.Committed("")
	if _, ok := g.Confirmable(); ok {
		t.Fatal("an empty set must never become the offset")
	}
	g.Committed(set1)
	g.Hand()
	g.Committed("")
	if got := confirmed(g); got != set1 {
		t.Fatalf("an empty set replaced the offset: %q", got)
	}
}

// The offset is the newest settled boundary, not the newest boundary: a later
// transaction still waiting on acks must not pull it forward.
func TestGateConfirmsOnlyUpToTheLastSettledBoundary(t *testing.T) {
	g := newCommitGate()
	g.Enable()
	g.Hand()
	g.Committed(set1)
	g.Hand()
	g.Committed(set2)
	g.Hand()
	g.Committed(set3)
	g.ObservePublished(rowEvent("mysql:" + serverA + ":10:0"))
	g.ObservePublished(rowEvent("mysql:" + serverA + ":11:0"))
	if got := confirmed(g); got != set2 {
		t.Fatalf("confirmed %q, want %q", got, set2)
	}
}
