package postgres

import (
	"testing"

	"github.com/excalibase/watcher-go/internal/cdc"
	"github.com/jackc/pglogrepl"
)

// streamTxn runs one decoded transaction through the gate: BEGIN, the
// publishable changes, COMMIT ending at end. It returns the handed events.
func streamTxn(gate *ackGate, changes int, end pglogrepl.LSN) []cdc.Event {
	gate.Begin()
	events := make([]cdc.Event, 0, changes)
	for range changes {
		events = append(events, gate.Hand(cdc.Event{Type: cdc.Insert}))
	}
	gate.Committed(end)
	return events
}

func ackAll(gate *ackGate, events []cdc.Event) {
	for _, event := range events {
		gate.ObservePublished(event)
	}
}

func TestAckGateDisabledConfirmsEachCommit(t *testing.T) {
	gate := newAckGate()
	streamTxn(gate, 2, 200)

	if got := gate.Flush(); got != 200 {
		t.Errorf("Flush without gating = %v, want 200", got)
	}
}

func TestAckGateHoldsCommitUntilAllItsChangesAreAcked(t *testing.T) {
	gate := newAckGate()
	gate.Enable()
	events := streamTxn(gate, 2, 200)

	gate.ObservePublished(events[0])
	if got := gate.Flush(); got != 0 {
		t.Errorf("Flush with one change unacked = %v, want 0", got)
	}
	gate.ObservePublished(events[1])
	if got := gate.Flush(); got != 200 {
		t.Errorf("Flush after every change acked = %v, want 200 (commit end)", got)
	}
}

// Transactions stream in commit order, so a later one can carry changes at
// lower WAL positions than one already acked; only sequence decides.
func TestAckGateHoldsLaterCommitWhoseChangesSitAtLowerPositions(t *testing.T) {
	gate := newAckGate()
	gate.Enable()
	late := streamTxn(gate, 1, 300)
	early := streamTxn(gate, 1, 400)

	ackAll(gate, late)
	if got := gate.Flush(); got != 300 {
		t.Fatalf("Flush = %v, want 300 (only the fully acked commit)", got)
	}
	ackAll(gate, early)
	if got := gate.Flush(); got != 400 {
		t.Errorf("Flush = %v, want 400", got)
	}
}

func TestAckGateCommitWithoutChangesWaitsForEarlierOnes(t *testing.T) {
	gate := newAckGate()
	gate.Enable()
	first := streamTxn(gate, 1, 100)
	streamTxn(gate, 0, 150)

	if got := gate.Flush(); got != 0 {
		t.Fatalf("Flush = %v, want 0 while an earlier change is unacked", got)
	}
	ackAll(gate, first)
	if got := gate.Flush(); got != 150 {
		t.Errorf("Flush = %v, want 150", got)
	}
}

func TestAckGateIdlePositionOnlyWhenNothingIsOutstanding(t *testing.T) {
	gate := newAckGate()
	gate.Enable()
	events := streamTxn(gate, 1, 100)

	gate.Idle(500)
	if got := gate.Flush(); got != 0 {
		t.Fatalf("Flush = %v, want 0: idle position with an unacked change", got)
	}
	ackAll(gate, events)
	gate.Idle(500)
	if got := gate.Flush(); got != 500 {
		t.Errorf("Flush = %v, want 500 once everything is acked", got)
	}
}

func TestAckGateIdlePositionIgnoredInsideTransaction(t *testing.T) {
	gate := newAckGate()
	gate.Enable()
	gate.Begin()

	gate.Idle(500)
	if got := gate.Flush(); got != 0 {
		t.Errorf("Flush = %v, want 0 inside an open transaction", got)
	}
}

func TestAckGateNewSessionDropsTheOpenTransaction(t *testing.T) {
	gate := newAckGate()
	gate.Enable()
	gate.Begin()
	partial := gate.Hand(cdc.Event{Type: cdc.Insert})

	gate.NewSession()
	gate.ObservePublished(partial)
	gate.Idle(700)
	if got := gate.Flush(); got != 700 {
		t.Errorf("Flush = %v, want 700: the interrupted transaction is streamed again", got)
	}
}

func TestAckGateUnhandReusesTheSequence(t *testing.T) {
	gate := newAckGate()
	gate.Enable()
	gate.Begin()
	rejected := gate.Hand(cdc.Event{Type: cdc.Insert})
	gate.Unhand()
	accepted := gate.Hand(cdc.Event{Type: cdc.Insert})
	gate.Committed(100)

	if rejected.Sequence != accepted.Sequence {
		t.Fatalf("sequences %d and %d, want the rejected one reused", rejected.Sequence, accepted.Sequence)
	}
	gate.ObservePublished(accepted)
	if got := gate.Flush(); got != 100 {
		t.Errorf("Flush = %v, want 100: a rejected event never reaches the publisher", got)
	}
}

func TestAckGatePendingTracksUnackedChanges(t *testing.T) {
	gate := newAckGate()
	gate.Enable()
	events := streamTxn(gate, 1, 100)

	if !gate.Pending() {
		t.Fatal("expected pending publish")
	}
	ackAll(gate, events)
	if gate.Pending() {
		t.Error("nothing should be pending after the ack")
	}
}

func TestAckGateIgnoresEventsWithoutSequence(t *testing.T) {
	gate := newAckGate()
	gate.Enable()
	streamTxn(gate, 1, 100)

	gate.ObservePublished(cdc.Event{LSN: "mysql-bin.000001:1234"})
	if !gate.Pending() {
		t.Error("an event this listener did not hand must not satisfy its ack")
	}
}

func TestAckGateNeverMovesBackwards(t *testing.T) {
	gate := newAckGate()
	gate.Enable()
	ackAll(gate, streamTxn(gate, 1, 300))
	gate.NewSession()
	ackAll(gate, streamTxn(gate, 1, 200))

	if got := gate.Flush(); got != 300 {
		t.Errorf("Flush = %v, want 300 after a re-streamed older commit", got)
	}
}

func TestAckGateCommitsWithoutNewChangesShareOneEntry(t *testing.T) {
	gate := newAckGate()
	gate.Enable()
	first := streamTxn(gate, 1, 100)
	for end := pglogrepl.LSN(200); end <= 1000; end += 100 {
		streamTxn(gate, 0, end)
	}

	if len(gate.commits) != 1 {
		t.Fatalf("pending commits = %d, want 1: commits waiting on the same change share an entry", len(gate.commits))
	}
	ackAll(gate, first)
	if got := gate.Flush(); got != 1000 {
		t.Errorf("Flush = %v, want 1000", got)
	}
}
