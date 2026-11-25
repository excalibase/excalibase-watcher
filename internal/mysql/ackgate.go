package mysql

import (
	"strings"
	"sync"
	"sync/atomic"

	"github.com/excalibase/watcher-go/internal/cdc"
)

// sourcePrefix marks the source ids of events this listener hands to the bus.
const sourcePrefix = "mysql:"

// commitGate decides which GTID set may be persisted as the resume offset.
//
// A GTID set only ever names whole transactions, so it cannot land in the
// middle of one. The gate still persists only transaction boundaries (after an
// XID or a DDL), and only once every event handed before that boundary is
// acked, so the set never names a transaction whose rows were not all
// published. A crash then replays the unconfirmed transactions whole;
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
	confirmable string
}

type pendingCommit struct {
	lastHanded uint64
	gtidSet    string
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

// Committed records a transaction boundary: the GTID set executed once the
// transaction is done. Boundaries waiting on the same
// handed events share one entry, so an idle stream or a stalled publisher
// holds at most one entry per handed event.
func (g *commitGate) Committed(gtidSet string) {
	if gtidSet == "" {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if last := len(g.commits) - 1; last >= 0 && g.commits[last].lastHanded == g.handed {
		g.commits[last].gtidSet = gtidSet
		return
	}
	g.commits = append(g.commits, pendingCommit{lastHanded: g.handed, gtidSet: gtidSet})
}

// ObservePublished is the publisher's onPublished callback. Events this
// listener did not hand are ignored.
func (g *commitGate) ObservePublished(event cdc.Event) {
	if ownEvent(event) {
		g.acked.Add(1)
	}
}

// Confirmable returns the newest boundary whose events are all acked.
func (g *commitGate) Confirmable() (string, bool) {
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
		g.confirmable = commit.gtidSet
		settled++
	}
	g.commits = g.commits[settled:]
	return g.confirmable, g.confirmable != ""
}

func ownEvent(event cdc.Event) bool {
	return strings.HasPrefix(event.SourceID, sourcePrefix) && cdc.IsPublishable(event.Type)
}
