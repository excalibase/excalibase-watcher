package postgres

import (
	"testing"

	"github.com/excalibase/watcher-go/internal/cdc"
	"github.com/jackc/pglogrepl"
)

func TestAckGateDisabledAcksEverythingReceived(t *testing.T) {
	gate := newAckGate()
	gate.Handed(pglogrepl.LSN(100))

	if got := gate.FlushLSN(pglogrepl.LSN(200)); got != 200 {
		t.Errorf("FlushLSN without gating = %v, want 200 (received)", got)
	}
}

func TestAckGateHoldsFlushAtLastAckedWhilePublishPending(t *testing.T) {
	gate := newAckGate()
	gate.Enable()

	gate.ObservePublished(cdc.Event{LSN: pglogrepl.LSN(50).String()})
	gate.Handed(pglogrepl.LSN(100))

	if !gate.Pending() {
		t.Fatal("expected pending publish")
	}
	if got := gate.FlushLSN(pglogrepl.LSN(200)); got != 50 {
		t.Errorf("FlushLSN with pending publish = %v, want 50 (last acked)", got)
	}
}

func TestAckGateReleasesFlushOnceHandedEventsAreAcked(t *testing.T) {
	gate := newAckGate()
	gate.Enable()

	gate.Handed(pglogrepl.LSN(100))
	gate.ObservePublished(cdc.Event{LSN: pglogrepl.LSN(100).String()})

	if gate.Pending() {
		t.Fatal("nothing should be pending after ack of the last handed LSN")
	}
	if got := gate.FlushLSN(pglogrepl.LSN(200)); got != 200 {
		t.Errorf("FlushLSN after ack = %v, want 200 (received)", got)
	}
}

func TestAckGateNothingHandedYetAcksReceived(t *testing.T) {
	gate := newAckGate()
	gate.Enable()

	if got := gate.FlushLSN(pglogrepl.LSN(300)); got != 300 {
		t.Errorf("FlushLSN with nothing handed = %v, want 300", got)
	}
}

func TestAckGateIgnoresNonPostgresPositions(t *testing.T) {
	gate := newAckGate()
	gate.Enable()

	gate.Handed(pglogrepl.LSN(100))
	gate.ObservePublished(cdc.Event{LSN: "mysql-bin.000001:1234"})
	gate.ObservePublished(cdc.Event{LSN: ""})

	if !gate.Pending() {
		t.Error("MySQL-style or empty positions must not satisfy a Postgres ack")
	}
}

func TestAckGateKeepsMonotonicMaximums(t *testing.T) {
	gate := newAckGate()
	gate.Enable()

	gate.Handed(pglogrepl.LSN(100))
	gate.Handed(pglogrepl.LSN(90))
	gate.ObservePublished(cdc.Event{LSN: pglogrepl.LSN(95).String()})
	gate.ObservePublished(cdc.Event{LSN: pglogrepl.LSN(80).String()})

	if got := gate.FlushLSN(pglogrepl.LSN(200)); got != 95 {
		t.Errorf("FlushLSN = %v, want 95 (max acked, still below max handed 100)", got)
	}
}
