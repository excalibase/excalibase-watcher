package cdc

import (
	"encoding/json"
	"fmt"
	"time"
)

type EventType int

const (
	Begin EventType = iota
	Commit
	Insert
	Update
	Delete
	DDL
	Truncate
	Heartbeat
)

func (t EventType) String() string {
	switch t {
	case Begin:
		return "BEGIN"
	case Commit:
		return "COMMIT"
	case Insert:
		return "INSERT"
	case Update:
		return "UPDATE"
	case Delete:
		return "DELETE"
	case DDL:
		return "DDL"
	case Truncate:
		return "TRUNCATE"
	case Heartbeat:
		return "HEARTBEAT"
	default:
		return "UNKNOWN"
	}
}

func (t EventType) MarshalJSON() ([]byte, error) {
	return json.Marshal(t.String())
}

func (t *EventType) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	switch s {
	case "BEGIN":
		*t = Begin
	case "COMMIT":
		*t = Commit
	case "INSERT":
		*t = Insert
	case "UPDATE":
		*t = Update
	case "DELETE":
		*t = Delete
	case "DDL":
		*t = DDL
	case "TRUNCATE":
		*t = Truncate
	case "HEARTBEAT":
		*t = Heartbeat
	default:
		return fmt.Errorf("unknown event type: %q", s)
	}
	return nil
}

type Event struct {
	Type            EventType `json:"type"`
	Schema          string    `json:"schema"`
	Table           string    `json:"table,omitempty"`
	Data            string    `json:"data,omitempty"`
	RawMessage      string    `json:"rawMessage,omitempty"`
	LSN             string    `json:"lsn,omitempty"`
	Timestamp       int64     `json:"timestamp"`
	SourceTimestamp int64     `json:"sourceTimestamp"`
}

func NewEvent(typ EventType, schema, table, data, rawMessage, lsn string) Event {
	return Event{
		Type:       typ,
		Schema:     schema,
		Table:      table,
		Data:       data,
		RawMessage: rawMessage,
		LSN:        lsn,
		Timestamp:  time.Now().UnixMilli(),
	}
}

func NewEventWithSourceTS(typ EventType, schema, table, data, rawMessage, lsn string, sourceTimestamp int64) Event {
	e := NewEvent(typ, schema, table, data, rawMessage, lsn)
	e.SourceTimestamp = sourceTimestamp
	return e
}
