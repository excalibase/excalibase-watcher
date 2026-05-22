package cdc

import (
	"testing"
	"time"
)

func TestNewEventWithSourceTS(t *testing.T) {
	before := time.Now().UnixMilli()
	e := NewEventWithSourceTS(Insert, "public", "users", `{"id":1}`, "INSERT", "0/1", 1700000000000)
	after := time.Now().UnixMilli()

	if e.Type != Insert {
		t.Errorf("type = %v", e.Type)
	}
	if e.Schema != "public" || e.Table != "users" {
		t.Errorf("schema/table = %q/%q", e.Schema, e.Table)
	}
	if e.SourceTimestamp != 1700000000000 {
		t.Errorf("sourceTimestamp = %d", e.SourceTimestamp)
	}
	if e.Timestamp < before || e.Timestamp > after {
		t.Errorf("timestamp %d not in [%d, %d]", e.Timestamp, before, after)
	}
}

func TestEventTypeString(t *testing.T) {
	cases := map[EventType]string{
		Begin:     "BEGIN",
		Commit:    "COMMIT",
		Insert:    "INSERT",
		Update:    "UPDATE",
		Delete:    "DELETE",
		DDL:       "DDL",
		Truncate:  "TRUNCATE",
		Heartbeat: "HEARTBEAT",
	}
	for tp, want := range cases {
		if got := tp.String(); got != want {
			t.Errorf("EventType(%d).String() = %q, want %q", tp, got, want)
		}
	}
}
