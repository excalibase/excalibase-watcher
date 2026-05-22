package postgres

import (
	"encoding/binary"
	"testing"

	"github.com/excalibase/watcher-go/internal/cdc"
)

// Helper to build a null-terminated string in a byte slice
func appendCString(buf []byte, s string) []byte {
	buf = append(buf, []byte(s)...)
	buf = append(buf, 0)
	return buf
}

func appendUint32(buf []byte, v uint32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, v)
	return append(buf, b...)
}

func appendUint16(buf []byte, v uint16) []byte {
	b := make([]byte, 2)
	binary.BigEndian.PutUint16(b, v)
	return append(buf, b...)
}

func appendUint64(buf []byte, v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return append(buf, b...)
}

func appendInt32(buf []byte, v int32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, uint32(v))
	return append(buf, b...)
}

// Build a RELATION message: 'R' + relation_id(4) + namespace(cstring) + name(cstring) + replica_identity(1) + num_columns(2) + columns...
func buildRelationMsg(relID uint32, namespace, name string, cols []testCol) []byte {
	buf := []byte{'R'}
	buf = appendUint32(buf, relID)
	buf = appendCString(buf, namespace)
	buf = appendCString(buf, name)
	buf = append(buf, 0) // replica identity
	buf = appendUint16(buf, uint16(len(cols)))
	for _, c := range cols {
		buf = append(buf, 0) // column flags
		buf = appendCString(buf, c.name)
		buf = appendUint32(buf, uint32(c.typeOID))
		buf = appendInt32(buf, 0) // type modifier
	}
	return buf
}

type testCol struct {
	name    string
	typeOID int
}

// Build an INSERT message: 'I' + relation_id(4) + 'N'(1) + tuple_data
func buildInsertMsg(relID uint32, values []testTupleVal) []byte {
	buf := []byte{'I'}
	buf = appendUint32(buf, relID)
	buf = append(buf, 'N') // tuple type marker
	buf = appendTupleData(buf, values)
	return buf
}

type testTupleVal struct {
	marker byte   // 'n' (null), 't' (text), 'u' (unchanged)
	value  string // only for 't'
}

func appendTupleData(buf []byte, values []testTupleVal) []byte {
	buf = appendUint16(buf, uint16(len(values)))
	for _, v := range values {
		buf = append(buf, v.marker)
		if v.marker == 't' {
			data := []byte(v.value)
			buf = appendInt32(buf, int32(len(data)))
			buf = append(buf, data...)
		}
	}
	return buf
}

// Build a BEGIN message: 'B' + finalLSN(8) + pgMicros(8) + xid(4)
func buildBeginMsg(pgMicros uint64) []byte {
	buf := []byte{'B'}
	buf = appendUint64(buf, 0)         // finalLSN
	buf = appendUint64(buf, pgMicros)  // timestamp in microseconds since 2000-01-01
	buf = appendUint32(buf, 42)        // xid
	return buf
}

// Build a COMMIT message: 'C'
func buildCommitMsg() []byte {
	return []byte{'C'}
}

// Build a DELETE message: 'D' + relation_id(4) + tuple_type(1) + tuple_data
func buildDeleteMsg(relID uint32, values []testTupleVal) []byte {
	buf := []byte{'D'}
	buf = appendUint32(buf, relID)
	buf = append(buf, 'K') // key tuple
	buf = appendTupleData(buf, values)
	return buf
}

// Build an UPDATE message: 'U' + relation_id(4) + ['K'/'O' + old_tuple] + 'N' + new_tuple
func buildUpdateMsg(relID uint32, oldValues, newValues []testTupleVal) []byte {
	buf := []byte{'U'}
	buf = appendUint32(buf, relID)
	if oldValues != nil {
		buf = append(buf, 'O') // old tuple marker
		buf = appendTupleData(buf, oldValues)
	}
	buf = append(buf, 'N') // new tuple marker
	buf = appendTupleData(buf, newValues)
	return buf
}

// Build a TRUNCATE message: 'T' + num_relations(4) + options(1) + relation_ids(4 each)
func buildTruncateMsg(options byte, relIDs ...uint32) []byte {
	buf := []byte{'T'}
	buf = appendUint32(buf, uint32(len(relIDs)))
	buf = append(buf, options)
	for _, id := range relIDs {
		buf = appendUint32(buf, id)
	}
	return buf
}

func TestParseBegin(t *testing.T) {
	// 1,000,000 microseconds = 1 second after PG epoch (2000-01-01 00:00:01 UTC)
	// PG epoch in Unix millis = 946684800000
	// Expected sourceTimestamp = 946684800000 + 1000 = 946684801000
	msg := buildBeginMsg(1_000_000)
	p := NewParser(nil, false, nil)
	event := p.Parse(msg, "0/1")

	if event == nil {
		t.Fatal("Parse returned nil for BEGIN")
	}
	if event.Type != cdc.Begin {
		t.Errorf("type = %v, want BEGIN", event.Type)
	}
	if event.SourceTimestamp != 946684801000 {
		t.Errorf("sourceTimestamp = %d, want 946684801000", event.SourceTimestamp)
	}
	if event.LSN != "0/1" {
		t.Errorf("lsn = %q, want 0/1", event.LSN)
	}
}

func TestParseCommit(t *testing.T) {
	msg := buildCommitMsg()
	p := NewParser(nil, false, nil)
	event := p.Parse(msg, "0/2")

	if event == nil {
		t.Fatal("Parse returned nil for COMMIT")
	}
	if event.Type != cdc.Commit {
		t.Errorf("type = %v, want COMMIT", event.Type)
	}
}

func TestParseRelationAndInsert(t *testing.T) {
	p := NewParser(nil, false, nil)

	// First send RELATION
	relMsg := buildRelationMsg(1, "public", "users", []testCol{
		{"id", 23},    // INT4
		{"name", 25},  // TEXT
		{"active", 16}, // BOOL
	})
	relEvent := p.Parse(relMsg, "0/1")
	if relEvent != nil {
		t.Error("RELATION should return nil event")
	}

	// Then INSERT
	insMsg := buildInsertMsg(1, []testTupleVal{
		{'t', "42"},
		{'t', "Alice"},
		{'t', "t"},
	})
	event := p.Parse(insMsg, "0/2")

	if event == nil {
		t.Fatal("Parse returned nil for INSERT")
	}
	if event.Type != cdc.Insert {
		t.Errorf("type = %v, want INSERT", event.Type)
	}
	if event.Schema != "public" {
		t.Errorf("schema = %q, want public", event.Schema)
	}
	if event.Table != "users" {
		t.Errorf("table = %q, want users", event.Table)
	}

	// Verify JSON data:
	// id should be a number (OID 23 = INT4)
	// name should be a string (OID 25 = TEXT)
	// active should be boolean (OID 16 = BOOL)
	expected := `{"id":42, "name":"Alice", "active":true}`
	if event.Data != expected {
		t.Errorf("data = %q\nwant  %q", event.Data, expected)
	}
}

func TestParseInsertWithNull(t *testing.T) {
	p := NewParser(nil, false, nil)

	relMsg := buildRelationMsg(1, "public", "users", []testCol{
		{"id", 23},
		{"email", 25},
	})
	p.Parse(relMsg, "0/1")

	insMsg := buildInsertMsg(1, []testTupleVal{
		{'t', "1"},
		{'n', ""},
	})
	event := p.Parse(insMsg, "0/2")

	if event == nil {
		t.Fatal("Parse returned nil")
	}
	expected := `{"id":1, "email":null}`
	if event.Data != expected {
		t.Errorf("data = %q\nwant  %q", event.Data, expected)
	}
}

func TestParseInsertWithUnchangedToast(t *testing.T) {
	p := NewParser(nil, false, nil)

	relMsg := buildRelationMsg(1, "public", "docs", []testCol{
		{"id", 23},
		{"content", 25},
	})
	p.Parse(relMsg, "0/1")

	insMsg := buildInsertMsg(1, []testTupleVal{
		{'t', "1"},
		{'u', ""},
	})
	event := p.Parse(insMsg, "0/2")

	expected := `{"id":1, "content":"unchanged"}`
	if event.Data != expected {
		t.Errorf("data = %q\nwant  %q", event.Data, expected)
	}
}

func TestParseUpdate(t *testing.T) {
	p := NewParser(nil, false, nil)

	relMsg := buildRelationMsg(1, "public", "users", []testCol{
		{"id", 23},
		{"name", 25},
	})
	p.Parse(relMsg, "0/1")

	upMsg := buildUpdateMsg(1,
		[]testTupleVal{
			{'t', "1"},
			{'t', "Alice"},
		},
		[]testTupleVal{
			{'t', "1"},
			{'t', "Bob"},
		},
	)
	event := p.Parse(upMsg, "0/3")

	if event == nil {
		t.Fatal("Parse returned nil for UPDATE")
	}
	if event.Type != cdc.Update {
		t.Errorf("type = %v, want UPDATE", event.Type)
	}
	expected := `{"old":{"id":1, "name":"Alice"}, "new":{"id":1, "name":"Bob"}}`
	if event.Data != expected {
		t.Errorf("data = %q\nwant  %q", event.Data, expected)
	}
}

func TestParseUpdateWithoutOldTuple(t *testing.T) {
	p := NewParser(nil, false, nil)

	relMsg := buildRelationMsg(1, "public", "users", []testCol{
		{"id", 23},
		{"name", 25},
	})
	p.Parse(relMsg, "0/1")

	// Update with only new tuple (no old data when replica identity is default)
	upMsg := buildUpdateMsg(1, nil, []testTupleVal{
		{'t', "1"},
		{'t', "Bob"},
	})
	event := p.Parse(upMsg, "0/3")

	expected := `{"new":{"id":1, "name":"Bob"}}`
	if event.Data != expected {
		t.Errorf("data = %q\nwant  %q", event.Data, expected)
	}
}

func TestParseDelete(t *testing.T) {
	p := NewParser(nil, false, nil)

	relMsg := buildRelationMsg(1, "public", "users", []testCol{
		{"id", 23},
		{"name", 25},
	})
	p.Parse(relMsg, "0/1")

	delMsg := buildDeleteMsg(1, []testTupleVal{
		{'t', "1"},
		{'t', "Alice"},
	})
	event := p.Parse(delMsg, "0/4")

	if event == nil {
		t.Fatal("Parse returned nil for DELETE")
	}
	if event.Type != cdc.Delete {
		t.Errorf("type = %v, want DELETE", event.Type)
	}
	expected := `{"id":1, "name":"Alice"}`
	if event.Data != expected {
		t.Errorf("data = %q\nwant  %q", event.Data, expected)
	}
}

func TestParseTruncate(t *testing.T) {
	p := NewParser(nil, false, nil)
	var extraEvents []cdc.Event

	p.SetEventHandler(func(e cdc.Event) {
		extraEvents = append(extraEvents, e)
	})

	relMsg1 := buildRelationMsg(1, "public", "users", []testCol{{"id", 23}})
	relMsg2 := buildRelationMsg(2, "public", "orders", []testCol{{"id", 23}})
	p.Parse(relMsg1, "0/1")
	p.Parse(relMsg2, "0/1")

	truncMsg := buildTruncateMsg(0, 1, 2)
	event := p.Parse(truncMsg, "0/5")

	if event == nil {
		t.Fatal("Parse returned nil for TRUNCATE")
	}
	if event.Type != cdc.Truncate {
		t.Errorf("type = %v, want TRUNCATE", event.Type)
	}
	if event.Table != "users" {
		t.Errorf("first truncate table = %q, want users", event.Table)
	}

	// Second relation emitted via eventHandler
	if len(extraEvents) != 1 {
		t.Fatalf("expected 1 extra event, got %d", len(extraEvents))
	}
	if extraEvents[0].Table != "orders" {
		t.Errorf("second truncate table = %q, want orders", extraEvents[0].Table)
	}
}

func TestTableFilter(t *testing.T) {
	filter := map[string]struct{}{"orders": {}}
	p := NewParser(filter, false, nil)

	relMsg := buildRelationMsg(1, "public", "users", []testCol{{"id", 23}})
	p.Parse(relMsg, "0/1")
	relMsg2 := buildRelationMsg(2, "public", "orders", []testCol{{"id", 23}})
	p.Parse(relMsg2, "0/1")

	// INSERT to users — should be filtered out
	insUsers := buildInsertMsg(1, []testTupleVal{{'t', "1"}})
	event := p.Parse(insUsers, "0/2")
	if event != nil {
		t.Error("INSERT to users should be filtered out")
	}

	// INSERT to orders — should pass through
	insOrders := buildInsertMsg(2, []testTupleVal{{'t', "1"}})
	event = p.Parse(insOrders, "0/3")
	if event == nil {
		t.Fatal("INSERT to orders should pass through filter")
	}
	if event.Table != "orders" {
		t.Errorf("table = %q, want orders", event.Table)
	}
}

func TestNumericTypeMapping(t *testing.T) {
	p := NewParser(nil, false, nil)

	relMsg := buildRelationMsg(1, "public", "metrics", []testCol{
		{"int2_val", 21},   // INT2
		{"int4_val", 23},   // INT4
		{"int8_val", 20},   // INT8
		{"float4_val", 700}, // FLOAT4
		{"float8_val", 701}, // FLOAT8
		{"numeric_val", 1700}, // NUMERIC
		{"oid_val", 26},    // OID
	})
	p.Parse(relMsg, "0/1")

	insMsg := buildInsertMsg(1, []testTupleVal{
		{'t', "32767"},
		{'t', "2147483647"},
		{'t', "9223372036854775807"},
		{'t', "3.14"},
		{'t', "2.718281828"},
		{'t', "99999.99"},
		{'t', "12345"},
	})
	event := p.Parse(insMsg, "0/2")

	// All numeric OIDs should produce unquoted JSON numbers
	expected := `{"int2_val":32767, "int4_val":2147483647, "int8_val":9223372036854775807, "float4_val":3.14, "float8_val":2.718281828, "numeric_val":99999.99, "oid_val":12345}`
	if event.Data != expected {
		t.Errorf("data = %q\nwant  %q", event.Data, expected)
	}
}

func TestBoolTypeMapping(t *testing.T) {
	p := NewParser(nil, false, nil)

	relMsg := buildRelationMsg(1, "public", "flags", []testCol{
		{"is_active", 16},
		{"is_deleted", 16},
	})
	p.Parse(relMsg, "0/1")

	insMsg := buildInsertMsg(1, []testTupleVal{
		{'t', "t"},
		{'t', "f"},
	})
	event := p.Parse(insMsg, "0/2")

	expected := `{"is_active":true, "is_deleted":false}`
	if event.Data != expected {
		t.Errorf("data = %q\nwant  %q", event.Data, expected)
	}
}

func TestStringEscaping(t *testing.T) {
	p := NewParser(nil, false, nil)

	relMsg := buildRelationMsg(1, "public", "docs", []testCol{
		{"content", 25},
	})
	p.Parse(relMsg, "0/1")

	insMsg := buildInsertMsg(1, []testTupleVal{
		{'t', "line1\nline2\ttab\\backslash\"quote"},
	})
	event := p.Parse(insMsg, "0/2")

	expected := `{"content":"line1\nline2\ttab\\backslash\"quote"}`
	if event.Data != expected {
		t.Errorf("data = %q\nwant  %q", event.Data, expected)
	}
}

func TestDDLCaptureParser(t *testing.T) {
	p := NewParser(nil, true, nil) // captureDdl = true

	// Register the DDL log table
	relMsg := buildRelationMsg(99, "public", "_cdc_ddl_log", []testCol{
		{"id", 23},
		{"command_tag", 25},
		{"query", 25},
	})
	p.Parse(relMsg, "0/1")

	// INSERT into DDL log should produce DDL event
	insMsg := buildInsertMsg(99, []testTupleVal{
		{'t', "1"},
		{'t', "ALTER TABLE"},
		{'t', "ALTER TABLE users ADD COLUMN age INT"},
	})
	event := p.Parse(insMsg, "0/2")

	if event == nil {
		t.Fatal("DDL capture returned nil")
	}
	if event.Type != cdc.DDL {
		t.Errorf("type = %v, want DDL", event.Type)
	}
	if event.Table != "" {
		t.Errorf("DDL event should have empty table, got %q", event.Table)
	}
}

func TestUnknownRelationSkipped(t *testing.T) {
	p := NewParser(nil, false, nil)

	// INSERT for relation ID that was never registered
	insMsg := buildInsertMsg(999, []testTupleVal{{'t', "1"}})
	event := p.Parse(insMsg, "0/2")

	if event != nil {
		t.Error("unknown relation should return nil")
	}
}

func TestEmptyBuffer(t *testing.T) {
	p := NewParser(nil, false, nil)
	event := p.Parse([]byte{}, "0/1")
	if event != nil {
		t.Error("empty buffer should return nil")
	}
}

func TestUnknownMessageType(t *testing.T) {
	p := NewParser(nil, false, nil)
	event := p.Parse([]byte{'Z'}, "0/1")
	if event != nil {
		t.Error("unknown message type should return nil")
	}
}

// TestJSONBColumnEmittedAsRawJSON verifies jsonb columns are emitted as raw JSON
// (not quoted strings). Consumers can directly parse the "data" field.
func TestJSONBColumnEmittedAsRawJSON(t *testing.T) {
	p := NewParser(nil, false, nil)

	// OID 3802 = jsonb
	relMsg := buildRelationMsg(1, "public", "events", []testCol{
		{"id", 23},
		{"payload", 3802},
	})
	p.Parse(relMsg, "0/1")

	insMsg := buildInsertMsg(1, []testTupleVal{
		{'t', "42"},
		{'t', `{"foo": "bar", "count": 3}`},
	})
	event := p.Parse(insMsg, "0/2")

	// payload must be embedded as nested JSON, NOT quoted string
	expected := `{"id":42, "payload":{"foo": "bar", "count": 3}}`
	if event.Data != expected {
		t.Errorf("data = %q\nwant  %q", event.Data, expected)
	}
}

// TestJSONColumnEmittedAsRawJSON tests OID 114 (json).
func TestJSONColumnEmittedAsRawJSON(t *testing.T) {
	p := NewParser(nil, false, nil)

	relMsg := buildRelationMsg(1, "public", "events", []testCol{
		{"config", 114}, // json OID
	})
	p.Parse(relMsg, "0/1")

	insMsg := buildInsertMsg(1, []testTupleVal{
		{'t', `[1, 2, 3]`},
	})
	event := p.Parse(insMsg, "0/2")

	expected := `{"config":[1, 2, 3]}`
	if event.Data != expected {
		t.Errorf("data = %q\nwant  %q", event.Data, expected)
	}
}

// TestInvalidJSONFallsBackToString protects against DB returning malformed JSON
// (shouldn't happen, but defend anyway).
func TestInvalidJSONFallsBackToString(t *testing.T) {
	p := NewParser(nil, false, nil)

	relMsg := buildRelationMsg(1, "public", "events", []testCol{
		{"payload", 3802},
	})
	p.Parse(relMsg, "0/1")

	insMsg := buildInsertMsg(1, []testTupleVal{
		{'t', `not valid json`},
	})
	event := p.Parse(insMsg, "0/2")

	// Should fall back to quoted string rather than produce invalid JSON
	expected := `{"payload":"not valid json"}`
	if event.Data != expected {
		t.Errorf("data = %q\nwant  %q", event.Data, expected)
	}
}
