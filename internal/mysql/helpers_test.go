package mysql

import (
	"strings"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/schema"
)

func TestBinlogPositionString(t *testing.T) {
	p := BinlogPosition{File: "mysql-bin.000007", Position: 4242}
	if got := p.String(); got != "mysql-bin.000007:4242" {
		t.Errorf("got %q", got)
	}
}

func TestGenerateServerIDDeterministic(t *testing.T) {
	a := generateServerID("host-a")
	b := generateServerID("host-a")
	c := generateServerID("host-b")
	if a != b {
		t.Errorf("same host gave different IDs: %d vs %d", a, b)
	}
	if a == c {
		t.Errorf("different hosts gave same ID: %d", a)
	}
	if a < 65536 {
		t.Errorf("server id %d below safe lower bound", a)
	}
}

func TestAppendValueInt(t *testing.T) {
	var sb strings.Builder
	appendValue(&sb, int64(42), schema.TYPE_NUMBER)
	if sb.String() != "42" {
		t.Errorf("int: %q", sb.String())
	}
}

func TestAppendValueBool(t *testing.T) {
	var sb strings.Builder
	appendValue(&sb, true, schema.TYPE_NUMBER)
	if sb.String() != "true" {
		t.Errorf("bool true: %q", sb.String())
	}
	sb.Reset()
	appendValue(&sb, false, schema.TYPE_NUMBER)
	if sb.String() != "false" {
		t.Errorf("bool false: %q", sb.String())
	}
}

func TestAppendValueString(t *testing.T) {
	var sb strings.Builder
	appendValue(&sb, `hello "world"`, schema.TYPE_STRING)
	if sb.String() != `"hello \"world\""` {
		t.Errorf("got %q", sb.String())
	}
}

func TestAppendValueStringWithControlChars(t *testing.T) {
	var sb strings.Builder
	appendValue(&sb, "a\nb\tc", schema.TYPE_STRING)
	if sb.String() != `"a\nb\tc"` {
		t.Errorf("got %q", sb.String())
	}
}

func TestAppendValueBytes(t *testing.T) {
	var sb strings.Builder
	appendValue(&sb, []byte("bin"), schema.TYPE_BINARY)
	if sb.String() != `"bin"` {
		t.Errorf("got %q", sb.String())
	}
}

func TestAppendValueTime(t *testing.T) {
	ts := time.Date(2026, 4, 20, 12, 34, 56, 789000000, time.UTC)
	var sb strings.Builder
	appendValue(&sb, ts, schema.TYPE_TIMESTAMP)
	if sb.String() != `"2026-04-20T12:34:56.789Z"` {
		t.Errorf("got %q", sb.String())
	}
}

func TestAppendValueJSONValid(t *testing.T) {
	var sb strings.Builder
	appendValue(&sb, `{"foo":"bar"}`, schema.TYPE_JSON)
	if sb.String() != `{"foo":"bar"}` {
		t.Errorf("got %q", sb.String())
	}
}

func TestAppendValueJSONInvalidFallsBackToString(t *testing.T) {
	var sb strings.Builder
	appendValue(&sb, `not-json{`, schema.TYPE_JSON)
	if sb.String() != `"not-json{"` {
		t.Errorf("got %q", sb.String())
	}
}

func TestAppendValueJSONFromBytes(t *testing.T) {
	var sb strings.Builder
	appendValue(&sb, []byte(`[1,2,3]`), schema.TYPE_JSON)
	if sb.String() != `[1,2,3]` {
		t.Errorf("got %q", sb.String())
	}
}

func TestAppendValueNilFallback(t *testing.T) {
	var sb strings.Builder
	appendValue(&sb, struct{ X int }{X: 7}, schema.TYPE_NUMBER)
	if !strings.Contains(sb.String(), `"X":7`) {
		t.Errorf("got %q", sb.String())
	}
}

func TestRowToJSON(t *testing.T) {
	table := &schema.Table{
		Columns: []schema.TableColumn{
			{Name: "id", Type: schema.TYPE_NUMBER},
			{Name: "name", Type: schema.TYPE_STRING},
			{Name: "email", Type: schema.TYPE_STRING},
		},
	}
	row := []interface{}{int64(1), "Alice", nil}
	got := rowToJSON(table, row)
	want := `{"id":1, "name":"Alice", "email":null}`
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestEventHandlerString(t *testing.T) {
	h := &eventHandler{}
	if h.String() != "excalibase-watcher-mysql" {
		t.Errorf("got %q", h.String())
	}
}
