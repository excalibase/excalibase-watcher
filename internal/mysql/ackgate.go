package mysql

import (
	"strings"
	"sync"
	"sync/atomic"

	"github.com/excalibase/watcher-go/internal/cdc"
)

// sourcePrefix marks the source ids of events this listener hands to the bus.
const sourcePrefix = "mysql:"

// commitGate decides which binlog position may be persisted as the resume
// offset.
//
// Every row of a multi-row binlog event carries the event's end position, so
// persisting the position of the last acked row resumes after rows that were
// never acked. The gate instead persists only transaction boundaries (after an
// XID, a DDL or a rotate), and only once every event handed before that
// boundary is acked. A crash then replays the unconfirmed transactions whole;
// the rows already published carry the same source id, so the stream drops
// them as duplicates. Until Enable is called, which happens when a NATS
// publisher is wired, every handed event counts as acked.
//
// The publisher acks in hand-off order, so counting acked events is enough.
type commitGate struct {
	gated atomic.Bool
	acked atomic.Uint64

	mu          sync.Mutex
	handed      uint64
	commits     []pendingCommit
	confirmable BinlogPosition
}

type pendingCommit struct {
	lastHanded uint64
	pos        BinlogPosition
}

func newCommitGate() *commitGate {
	return &commitGate{}
}

func (g *commitGate) Enable() {
	g.gated.Store(true)
}

// Hand counts an event the bus accepted for publishing.
func (g *commitGate) Hand() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.handed++
}

// Committed records a transaction boundary. Boundaries waiting on the same
// handed events share one entry, so an idle stream or a stalled publisher
// holds at most one entry per handed event.
func (g *commitGate) Committed(pos BinlogPosition) {
	if pos.File == "" || pos.Position < minBinlogPosition {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if last := len(g.commits) - 1; last >= 0 && g.commits[last].lastHanded == g.handed {
		g.commits[last].pos = pos
		return
	}
	g.commits = append(g.commits, pendingCommit{lastHanded: g.handed, pos: pos})
}

// minBinlogPosition is where the first event of a binlog file starts; a
// smaller position (events inside a compressed transaction carry 0) is not a
// place replication can resume from.
const minBinlogPosition = 4

// ObservePublished is the publisher's onPublished callback. Events this
// listener did not hand are ignored.
func (g *commitGate) ObservePublished(event cdc.Event) {
	if ownEvent(event) {
		g.acked.Add(1)
	}
}

// Confirmable returns the newest boundary whose events are all acked.
func (g *commitGate) Confirmable() (BinlogPosition, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	acked := g.handed
	if g.gated.Load() {
		acked = g.acked.Load()
	}
	settled := 0
	for _, commit := range g.commits {
		if commit.lastHanded > acked {
			break
		}
		g.confirmable = commit.pos
		settled++
	}
	g.commits = g.commits[settled:]
	return g.confirmable, g.confirmable.File != ""
}

func ownEvent(event cdc.Event) bool {
	return strings.HasPrefix(event.SourceID, sourcePrefix) && cdc.IsPublishable(event.Type)
}
