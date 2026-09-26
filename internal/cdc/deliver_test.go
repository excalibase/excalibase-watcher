package cdc

import (
	"sync"
	"testing"
	"time"
)

// TestDeliverBlocksWhenChannelFull exercises the backpressure path in
// Service.deliver. We bypass Subscribe so we can install a subscriber with a
// tiny channel — the production 100k buffer is too large for a unit test.
func TestDeliverBlocksWhenChannelFull(t *testing.T) {
	svc := NewService()

	small := &subscriber{ch: make(chan Event, 1)}
	svc.mu.Lock()
	svc.tableSubs["t"] = append(svc.tableSubs["t"], small)
	svc.mu.Unlock()

	// Fill the buffer
	svc.deliver(small, Event{Type: Insert, Table: "t"}, "table", "t")

	// Send another event in a goroutine — deliver should block until the
	// buffer has room.
	delivered := make(chan struct{})
	go func() {
		svc.deliver(small, Event{Type: Insert, Table: "t"}, "table", "t")
		close(delivered)
	}()

	// deliver should still be blocked
	select {
	case <-delivered:
		t.Fatal("deliver returned before buffer had room")
	case <-time.After(50 * time.Millisecond):
	}

	// Drain one event, which should unblock deliver
	<-small.ch
	select {
	case <-delivered:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("deliver did not unblock after drain")
	}
}

// TestDeliverReturnsOnShutdown verifies the blocking sender gives up when the
// service is closed, so a subscriber that stops reading doesn't wedge Shutdown.
func TestDeliverReturnsOnShutdown(t *testing.T) {
	svc := NewService()

	small := &subscriber{ch: make(chan Event, 1)}
	svc.mu.Lock()
	svc.tableSubs["t"] = append(svc.tableSubs["t"], small)
	svc.mu.Unlock()

	// Fill buffer
	small.ch <- Event{Type: Insert, Table: "t"}

	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		svc.deliver(small, Event{Type: Insert, Table: "t"}, "table", "t")
		close(done)
	}()

	// Give deliver time to enter the blocking loop
	time.Sleep(50 * time.Millisecond)

	// Close the service — deliver should return instead of waiting forever
	svc.closed.Store(true)

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("deliver did not return after service closed")
	}
	wg.Wait()
}

// A subscriber that stops reading (publisher shutting down while NATS is
// unreachable) must be able to unsubscribe even while a producer is blocked
// delivering to its full buffer; otherwise shutdown deadlocks.
func TestUnsubscribeReleasesAProducerBlockedOnAFullBuffer(t *testing.T) {
	svc := NewService()
	_, unsub := svc.SubscribeAll()
	for i := 0; i < defaultChanBuffer; i++ {
		svc.HandleEvent(NewEvent(Insert, "public", "users", `{"id":1}`, "INSERT", ""))
	}

	produced := make(chan struct{})
	go func() {
		svc.HandleEvent(NewEvent(Insert, "public", "users", `{"id":2}`, "INSERT", ""))
		close(produced)
	}()
	time.Sleep(200 * time.Millisecond)

	unsubscribed := make(chan struct{})
	go func() {
		unsub()
		close(unsubscribed)
	}()
	for name, done := range map[string]chan struct{}{"unsubscribe": unsubscribed, "blocked producer": produced} {
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatalf("%s never returned", name)
		}
	}
	unsub()
}
