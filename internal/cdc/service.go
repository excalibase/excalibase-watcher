package cdc

import (
	"encoding/json"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultChanBuffer    = 100000            // 100k events buffered per subscriber
	backpressureLogEvery = 10 * time.Second  // log at most once per interval when blocked
)

type subscriber struct {
	ch          chan Event
	lastWarnAt  atomic.Int64 // epoch nanos
}

type Service struct {
	mu         sync.RWMutex
	tableSubs  map[string][]*subscriber
	globalSubs []*subscriber
	running    atomic.Bool
	closed     atomic.Bool
}

func NewService() *Service {
	return &Service{
		tableSubs: make(map[string][]*subscriber),
	}
}

func (s *Service) Subscribe(table string) (<-chan Event, func()) {
	s.mu.Lock()
	defer s.mu.Unlock()

	sub := &subscriber{ch: make(chan Event, defaultChanBuffer)}
	s.tableSubs[table] = append(s.tableSubs[table], sub)

	unsub := func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		subs := s.tableSubs[table]
		for i, existing := range subs {
			if existing == sub {
				s.tableSubs[table] = append(subs[:i], subs[i+1:]...)
				break
			}
		}
		if len(s.tableSubs[table]) == 0 {
			delete(s.tableSubs, table)
		}
	}

	return sub.ch, unsub
}

func (s *Service) SubscribeAll() (<-chan Event, func()) {
	s.mu.Lock()
	defer s.mu.Unlock()

	sub := &subscriber{ch: make(chan Event, defaultChanBuffer)}
	s.globalSubs = append(s.globalSubs, sub)

	unsub := func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		for i, existing := range s.globalSubs {
			if existing == sub {
				s.globalSubs = append(s.globalSubs[:i], s.globalSubs[i+1:]...)
				break
			}
		}
	}

	return sub.ch, unsub
}

func (s *Service) HandleEvent(event Event) {
	if s.closed.Load() {
		return
	}

	// Validate JSON data for DML events only (DDL data is raw SQL, not JSON)
	if isTableEvent(event.Type) && event.Data != "" && !isValidJSON(event.Data) {
		slog.Warn("rejecting event with invalid JSON data",
			"type", event.Type.String(),
			"table", event.Table,
		)
		return
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	// All events go to global subscribers (blocking — CDC must not drop events)
	for _, sub := range s.globalSubs {
		s.deliver(sub, event, "global", "")
	}

	// DML events (INSERT, UPDATE, DELETE, TRUNCATE) also go to per-table subscribers
	if event.Table != "" && isTableEvent(event.Type) {
		for _, sub := range s.tableSubs[event.Table] {
			s.deliver(sub, event, "table", event.Table)
		}
	}
}

// deliver sends an event to a subscriber, blocking if the channel is full.
// Logs a backpressure warning at most once per backpressureLogEvery interval.
func (s *Service) deliver(sub *subscriber, event Event, kind, table string) {
	// Fast path: non-blocking send
	select {
	case sub.ch <- event:
		return
	default:
	}

	// Channel full — log backpressure (rate-limited) and block
	now := time.Now().UnixNano()
	last := sub.lastWarnAt.Load()
	if now-last >= backpressureLogEvery.Nanoseconds() && sub.lastWarnAt.CompareAndSwap(last, now) {
		slog.Warn("subscriber channel full, applying backpressure",
			"kind", kind,
			"table", table,
			"type", event.Type.String(),
			"buffer_size", cap(sub.ch),
		)
	}

	// Blocking send — respect service shutdown
	for {
		select {
		case sub.ch <- event:
			return
		case <-time.After(100 * time.Millisecond):
			if s.closed.Load() {
				return
			}
		}
	}
}

func (s *Service) MarkRunning() {
	s.running.Store(true)
}

func (s *Service) IsRunning() bool {
	return s.running.Load()
}

func (s *Service) SubscriberCount(table string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.tableSubs[table])
}

func (s *Service) HasTableSubscribers(table string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, exists := s.tableSubs[table]
	return exists
}

func (s *Service) TotalSubscriberCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	total := len(s.globalSubs)
	for _, subs := range s.tableSubs {
		total += len(subs)
	}
	return total
}

func (s *Service) Shutdown() {
	if s.closed.Swap(true) {
		return // already closed
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, sub := range s.globalSubs {
		close(sub.ch)
	}
	s.globalSubs = nil

	for table, subs := range s.tableSubs {
		for _, sub := range subs {
			close(sub.ch)
		}
		delete(s.tableSubs, table)
	}
}

func isTableEvent(t EventType) bool {
	switch t {
	case Insert, Update, Delete, Truncate:
		return true
	default:
		return false
	}
}

func isValidJSON(data string) bool {
	return json.Valid([]byte(data))
}
