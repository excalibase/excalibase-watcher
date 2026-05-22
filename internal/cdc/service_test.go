package cdc

import (
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestSubscribeReturnsChannel(t *testing.T) {
	svc := NewService()
	defer svc.Shutdown()

	ch, unsub := svc.Subscribe("users")
	defer unsub()

	if ch == nil {
		t.Fatal("Subscribe returned nil channel")
	}
}

func TestSubscribeAllReturnsGlobalChannel(t *testing.T) {
	svc := NewService()
	defer svc.Shutdown()

	ch, unsub := svc.SubscribeAll()
	defer unsub()

	if ch == nil {
		t.Fatal("SubscribeAll returned nil channel")
	}
}

func TestIsNotRunningByDefault(t *testing.T) {
	svc := NewService()
	defer svc.Shutdown()

	if svc.IsRunning() {
		t.Error("should not be running by default")
	}
}

func TestIsRunningAfterMarkRunning(t *testing.T) {
	svc := NewService()
	defer svc.Shutdown()

	svc.MarkRunning()
	if !svc.IsRunning() {
		t.Error("should be running after MarkRunning()")
	}
}

func TestRouteEventsToCorrectTableChannels(t *testing.T) {
	svc := NewService()
	defer svc.Shutdown()

	usersCh, unsub1 := svc.Subscribe("users")
	defer unsub1()
	ordersCh, unsub2 := svc.Subscribe("orders")
	defer unsub2()

	usersEvent := NewEvent(Insert, "public", "users", `{"id":1}`, "INSERT", "0/1")
	ordersEvent := NewEvent(Insert, "public", "orders", `{"id":2}`, "INSERT", "0/2")

	svc.HandleEvent(usersEvent)
	svc.HandleEvent(ordersEvent)

	select {
	case e := <-usersCh:
		if e.Table != "users" {
			t.Errorf("users channel got table %q", e.Table)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for users event")
	}

	select {
	case e := <-ordersCh:
		if e.Table != "orders" {
			t.Errorf("orders channel got table %q", e.Table)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for orders event")
	}
}

func TestTrackSubscriberCounts(t *testing.T) {
	svc := NewService()
	defer svc.Shutdown()

	if svc.SubscriberCount("users") != 0 {
		t.Error("should be 0 before any subscriptions")
	}

	_, unsub1 := svc.Subscribe("users")
	if svc.SubscriberCount("users") != 1 {
		t.Errorf("after 1 subscribe, count = %d", svc.SubscriberCount("users"))
	}

	_, unsub2 := svc.Subscribe("users")
	if svc.SubscriberCount("users") != 2 {
		t.Errorf("after 2 subscribes, count = %d", svc.SubscriberCount("users"))
	}

	unsub1()
	if svc.SubscriberCount("users") != 1 {
		t.Errorf("after 1 unsub, count = %d", svc.SubscriberCount("users"))
	}

	unsub2()
	if svc.SubscriberCount("users") != 0 {
		t.Errorf("after all unsub, count = %d", svc.SubscriberCount("users"))
	}
}

func TestGlobalChannelReceivesDDLBeginCommit(t *testing.T) {
	svc := NewService()
	defer svc.Shutdown()

	globalCh, unsub := svc.SubscribeAll()
	defer unsub()

	// DDL, BEGIN, COMMIT should only go to global, not per-table
	tableCh, unsubTable := svc.Subscribe("users")
	defer unsubTable()

	events := []Event{
		NewEvent(Begin, "public", "", ``, "BEGIN", "0/1"),
		NewEvent(DDL, "public", "", `{"query":"ALTER TABLE users ADD COLUMN age INT"}`, "DDL", "0/2"),
		NewEvent(Commit, "public", "", ``, "COMMIT", "0/3"),
	}

	for _, e := range events {
		svc.HandleEvent(e)
	}

	// Should receive all 3 on global
	for i := 0; i < 3; i++ {
		select {
		case <-globalCh:
		case <-time.After(time.Second):
			t.Fatalf("timeout waiting for global event %d", i)
		}
	}

	// Table channel should NOT receive any of these (table is empty for these events)
	select {
	case e := <-tableCh:
		t.Errorf("table channel should not receive %v", e.Type)
	case <-time.After(100 * time.Millisecond):
		// expected
	}
}

func TestCleanupChannelWhenNoSubscribers(t *testing.T) {
	svc := NewService()
	defer svc.Shutdown()

	_, unsub := svc.Subscribe("users")
	if svc.SubscriberCount("users") != 1 {
		t.Fatal("expected 1 subscriber")
	}

	unsub()
	if svc.SubscriberCount("users") != 0 {
		t.Error("expected 0 subscribers after unsub")
	}

	// Internal table entry should be cleaned up
	if svc.HasTableSubscribers("users") {
		t.Error("table entry should be cleaned up")
	}
}

func TestRejectInvalidJsonData(t *testing.T) {
	svc := NewService()
	defer svc.Shutdown()

	ch, unsub := svc.Subscribe("users")
	defer unsub()

	// Invalid JSON data (not starting with { and ending with })
	event := NewEvent(Insert, "public", "users", "not json", "INSERT", "0/1")
	svc.HandleEvent(event)

	select {
	case <-ch:
		t.Error("should not receive event with invalid JSON data")
	case <-time.After(100 * time.Millisecond):
		// expected — event rejected
	}
}

func TestShutdownClosesAllChannels(t *testing.T) {
	svc := NewService()

	ch1, _ := svc.Subscribe("users")
	ch2, _ := svc.SubscribeAll()

	svc.Shutdown()

	// Channels should be closed (reads return zero value + ok=false)
	select {
	case _, ok := <-ch1:
		if ok {
			t.Error("table channel should be closed")
		}
	case <-time.After(time.Second):
		t.Fatal("timeout — table channel not closed")
	}

	select {
	case _, ok := <-ch2:
		if ok {
			t.Error("global channel should be closed")
		}
	case <-time.After(time.Second):
		t.Fatal("timeout — global channel not closed")
	}
}

func TestConcurrentSubscribeAndEmit(t *testing.T) {
	svc := NewService()
	defer svc.Shutdown()

	var wg sync.WaitGroup
	const goroutines = 50
	const eventsPerGoroutine = 100

	// Concurrent subscribers
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ch, unsub := svc.Subscribe("stress")
			defer unsub()
			// Drain events
			for j := 0; j < eventsPerGoroutine; j++ {
				select {
				case <-ch:
				case <-time.After(5 * time.Second):
					return
				}
			}
		}()
	}

	// Concurrent emitters
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < eventsPerGoroutine; j++ {
				svc.HandleEvent(NewEvent(Insert, "public", "stress", `{"x":1}`, "INSERT", "0/1"))
			}
		}()
	}

	wg.Wait()
}

func TestTotalSubscriberCount(t *testing.T) {
	svc := NewService()
	defer svc.Shutdown()

	if svc.TotalSubscriberCount() != 0 {
		t.Error("should be 0 initially")
	}

	_, unsub1 := svc.Subscribe("users")
	_, unsub2 := svc.Subscribe("orders")
	_, unsub3 := svc.SubscribeAll()

	if svc.TotalSubscriberCount() != 3 {
		t.Errorf("expected 3, got %d", svc.TotalSubscriberCount())
	}

	unsub1()
	unsub2()
	unsub3()

	if svc.TotalSubscriberCount() != 0 {
		t.Errorf("expected 0 after all unsub, got %d", svc.TotalSubscriberCount())
	}
}

// TestEventTypeJSONBackwardCompat verifies EventType serializes as string (matching Java).
func TestEventTypeJSONBackwardCompat(t *testing.T) {
	tests := []struct {
		typ    EventType
		expect string
	}{
		{Begin, `"BEGIN"`},
		{Commit, `"COMMIT"`},
		{Insert, `"INSERT"`},
		{Update, `"UPDATE"`},
		{Delete, `"DELETE"`},
		{DDL, `"DDL"`},
		{Truncate, `"TRUNCATE"`},
		{Heartbeat, `"HEARTBEAT"`},
	}

	for _, tt := range tests {
		t.Run(tt.expect, func(t *testing.T) {
			data, err := json.Marshal(tt.typ)
			if err != nil {
				t.Fatal(err)
			}
			if string(data) != tt.expect {
				t.Errorf("Marshal(%v) = %s, want %s", tt.typ, data, tt.expect)
			}

			// Round-trip
			var decoded EventType
			if err := json.Unmarshal(data, &decoded); err != nil {
				t.Fatalf("Unmarshal(%s): %v", data, err)
			}
			if decoded != tt.typ {
				t.Errorf("round-trip: got %v, want %v", decoded, tt.typ)
			}
		})
	}
}

// TestEventJSONFormat verifies the full event JSON matches Java format.
func TestEventJSONFormat(t *testing.T) {
	event := NewEvent(Insert, "public", "users", `{"id":1, "name":"Alice"}`, "INSERT", "0/1234")

	data, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	s := string(data)

	// type must be string "INSERT", not integer 2
	if !strings.Contains(s, `"type":"INSERT"`) {
		t.Errorf("type should be string, got: %s", s)
	}
	// schema present
	if !strings.Contains(s, `"schema":"public"`) {
		t.Errorf("missing schema: %s", s)
	}
	// table present
	if !strings.Contains(s, `"table":"users"`) {
		t.Errorf("missing table: %s", s)
	}
	// data preserved as string (not parsed)
	if !strings.Contains(s, `"data":"{\"id\":1, \"name\":\"Alice\"}"`) {
		t.Errorf("data format wrong: %s", s)
	}
	// timestamps are numbers
	if !strings.Contains(s, `"timestamp":`) {
		t.Errorf("missing timestamp: %s", s)
	}
	if !strings.Contains(s, `"sourceTimestamp":0`) {
		t.Errorf("missing sourceTimestamp: %s", s)
	}
}
