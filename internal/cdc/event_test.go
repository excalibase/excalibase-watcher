package cdc

import (
	"encoding/json"
	"strings"
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

// SourceID is the publisher's dedupe key, not part of the consumer payload.
func TestSourceIDIsNotInThePayload(t *testing.T) {
	event := NewEvent(Insert, "public", "users", `{"id":1}`, "INSERT", "0/1")
	event.SourceID = "pg:0/1:0"
	data, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "pg:0/1:0") {
		t.Errorf("payload carries the source id: %s", data)
	}
}
