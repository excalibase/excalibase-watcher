package mysql

import (
	"testing"

	"github.com/excalibase/watcher-go/internal/cdc"
)

func rowEvent(sourceID string) cdc.Event {
	return cdc.Event{Type: cdc.Insert, SourceID: sourceID}
}

func confirmed(t *testing.T, g *commitGate) BinlogPosition {
	t.Helper()
	pos, _ := g.Confirmable()
	return pos
}

// A rows event with three rows, two of them acked: the transaction's boundary
// must not be confirmed, so a restart replays the whole transaction.
func TestGateHoldsBoundaryUntilEveryRowOfTheEventIsAcked(t *testing.T) {
	g := newCommitGate()
	g.Enable()
	start := BinlogPosition{File: "mysql-bin.000001", Position: 100}
	g.Committed(start)
	for range 3 {
		g.Hand()
	}
	end := BinlogPosition{File: "mysql-bin.000001", Position: 900}
	g.Committed(end)

	g.ObservePublished(rowEvent("mysql:mysql-bin.000001:800:0"))
	g.ObservePublished(rowEvent("mysql:mysql-bin.000001:800:1"))
	if got := confirmed(t, g); got != start {
		t.Fatalf("two of three rows acked: confirmed %v, want %v", got, start)
	}

	g.ObservePublished(rowEvent("mysql:mysql-bin.000001:800:2"))
	if got := confirmed(t, g); got != end {
		t.Fatalf("all rows acked: confirmed %v, want %v", got, end)
	}
}

func TestGateIgnoresEventsItDidNotHand(t *testing.T) {
	g := newCommitGate()
	g.Enable()
	g.Hand()
	g.Committed(BinlogPosition{File: "mysql-bin.000001", Position: 500})

	g.ObservePublished(cdc.Event{Type: cdc.Insert, SourceID: "pg:0/16B3748"})
	g.ObservePublished(cdc.Event{Type: cdc.Insert})
	g.ObservePublished(cdc.Event{Type: cdc.Commit, SourceID: "mysql:mysql-bin.000001:500:0"})
	if _, ok := g.Confirmable(); ok {
		t.Fatal("foreign or unpublishable events must not confirm a MySQL boundary")
	}
}

func TestGateWithoutPublisherConfirmsEveryBoundary(t *testing.T) {
	g := newCommitGate()
	g.Hand()
	pos := BinlogPosition{File: "mysql-bin.000001", Position: 300}
	g.Committed(pos)
	if got := confirmed(t, g); got != pos {
		t.Fatalf("ungated: confirmed %v, want %v", got, pos)
	}
}

// Boundaries with nothing handed between them (idle rotates, filtered
// tables) collapse into one entry carrying the newest position.
func TestGateCoalescesBoundariesWaitingOnTheSameEvents(t *testing.T) {
	g := newCommitGate()
	g.Enable()
	g.Hand()
	g.Committed(BinlogPosition{File: "mysql-bin.000001", Position: 300})
	g.Committed(BinlogPosition{File: "mysql-bin.000002", Position: 4})
	if len(g.commits) != 1 {
		t.Fatalf("pending boundaries = %d, want 1", len(g.commits))
	}
	g.ObservePublished(rowEvent("mysql:mysql-bin.000001:200:0"))
	if got, want := confirmed(t, g), (BinlogPosition{File: "mysql-bin.000002", Position: 4}); got != want {
		t.Fatalf("confirmed %v, want %v", got, want)
	}
}

func TestGateSkipsPositionsReplicationCannotResumeFrom(t *testing.T) {
	g := newCommitGate()
	g.Committed(BinlogPosition{File: "mysql-bin.000001", Position: 0})
	g.Committed(BinlogPosition{Position: 900})
	if _, ok := g.Confirmable(); ok {
		t.Fatal("a zero position or a missing file must never become the offset")
	}
}
