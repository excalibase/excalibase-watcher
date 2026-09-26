package postgres

import (
	"sync/atomic"

	"github.com/excalibase/watcher-go/internal/cdc"
	"github.com/jackc/pglogrepl"
)

// ackGate decides which LSN may be confirmed to the primary.
//
// Postgres streams transactions in commit order, so a change's WAL position
// says nothing about whether everything before it was delivered. The gate
// numbers every publishable event in hand-off order (the order the publisher
// acks in) and only confirms a commit's end LSN once every event handed up to
// that commit is acked. A crash then re-streams exactly the unconfirmed
// transactions (at-least-once). Until Enable is called, which happens when a
// NATS publisher is wired, there is no durability target and every handed
// event counts as acked.
//
// All methods except ObservePublished run on the listener goroutine.
type ackGate struct {
	gated atomic.Bool
	acked atomic.Uint64

	handed      uint64
	inTxn       bool
	commits     []pendingCommit
	confirmable pglogrepl.LSN
}

type pendingCommit struct {
	lastSequence uint64
	end          pglogrepl.LSN
}

func newAckGate() *ackGate {
	return &ackGate{}
}

func (g *ackGate) Enable() {
	g.gated.Store(true)
}

func (g *ackGate) Begin() {
	g.inTxn = true
}

// Hand numbers a publishable event about to be passed to the bus.
func (g *ackGate) Hand(event cdc.Event) cdc.Event {
	g.handed++
	event.Sequence = g.handed
	return event
}

// Unhand takes back the last Hand when the bus refused that event, so it
// never waits for an ack that cannot come.
func (g *ackGate) Unhand() {
	g.handed--
}

// Committed closes the open transaction; end is its commit's end LSN.
// Commits that wait on the same last change share one entry, so a stalled
// publisher holds at most one entry per handed event.
func (g *ackGate) Committed(end pglogrepl.LSN) {
	g.inTxn = false
	if last := len(g.commits) - 1; last >= 0 && g.commits[last].lastSequence == g.handed {
		g.commits[last].end = max(g.commits[last].end, end)
		return
	}
	g.commits = append(g.commits, pendingCommit{lastSequence: g.handed, end: end})
}

// NewSession forgets the transaction a dropped connection interrupted:
// Postgres streams it again from its beginning.
func (g *ackGate) NewSession() {
	g.inTxn = false
}

// Idle offers a keepalive position. The walsender has streamed every
// transaction committed before it, so it is safe once all of them are acked.
func (g *ackGate) Idle(position pglogrepl.LSN) {
	g.settle()
	if g.inTxn || len(g.commits) > 0 || g.Pending() {
		return
	}
	g.confirmable = max(g.confirmable, position)
}

// ObservePublished is the publisher's onPublished callback. The publisher
// acks in hand-off order, so the highest sequence acked covers all before it.
// Events this listener did not number are ignored.
func (g *ackGate) ObservePublished(event cdc.Event) {
	if event.Sequence == 0 {
		return
	}
	storeMax(&g.acked, event.Sequence)
}

// Pending reports whether a handed event still awaits its NATS ack.
func (g *ackGate) Pending() bool {
	return g.handed > g.ackedSequence()
}

// Flush returns the position safe to confirm.
func (g *ackGate) Flush() pglogrepl.LSN {
	g.settle()
	return g.confirmable
}

func (g *ackGate) settle() {
	acked := g.ackedSequence()
	settled := 0
	for _, commit := range g.commits {
		if commit.lastSequence > acked {
			break
		}
		g.confirmable = max(g.confirmable, commit.end)
		settled++
	}
	g.commits = g.commits[settled:]
}

func (g *ackGate) ackedSequence() uint64 {
	if !g.gated.Load() {
		return g.handed
	}
	return g.acked.Load()
}

func storeMax(target *atomic.Uint64, value uint64) {
	for {
		current := target.Load()
		if value <= current || target.CompareAndSwap(current, value) {
			return
		}
	}
}
