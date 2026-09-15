package postgres

import (
	"sync/atomic"

	"github.com/excalibase/watcher-go/internal/cdc"
	"github.com/jackc/pglogrepl"
)

// ackGate decides which LSN may be confirmed to the primary.
//
// The watcher only confirms (standby status update) WAL whose publishable
// events have been acknowledged by NATS. While an event handed to the event
// bus is still unacknowledged the flush position is held at the last
// acknowledged LSN, so a crash re-streams the unpublished events
// (at-least-once). The gate is inert until Enable is called, which happens
// when a NATS publisher is wired; without a publisher there is no durability
// target and everything received is confirmed.
type ackGate struct {
	gated  atomic.Bool
	handed atomic.Uint64
	acked  atomic.Uint64
}

func newAckGate() *ackGate {
	return &ackGate{}
}

func (g *ackGate) Enable() {
	g.gated.Store(true)
}

// Handed records that a publishable event ending at lsn was passed to the bus.
func (g *ackGate) Handed(lsn pglogrepl.LSN) {
	storeMax(&g.handed, uint64(lsn))
}

// ObservePublished is the publisher's onPublished callback. Positions that are
// not Postgres LSNs (MySQL binlog positions, snapshot events) are ignored.
func (g *ackGate) ObservePublished(event cdc.Event) {
	lsn, err := pglogrepl.ParseLSN(event.LSN)
	if err != nil {
		return
	}
	storeMax(&g.acked, uint64(lsn))
}

// Pending reports whether a handed event still awaits its NATS ack.
func (g *ackGate) Pending() bool {
	return g.gated.Load() && g.handed.Load() > g.acked.Load()
}

// FlushLSN returns the position safe to confirm given everything received so far.
func (g *ackGate) FlushLSN(received pglogrepl.LSN) pglogrepl.LSN {
	if !g.Pending() {
		return received
	}
	return pglogrepl.LSN(g.acked.Load())
}

func storeMax(target *atomic.Uint64, value uint64) {
	for {
		current := target.Load()
		if value <= current || target.CompareAndSwap(current, value) {
			return
		}
	}
}
