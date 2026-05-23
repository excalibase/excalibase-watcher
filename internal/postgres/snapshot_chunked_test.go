package postgres

import (
	"strings"
	"testing"

	"github.com/excalibase/watcher-go/internal/cdc"
)

func TestBuildChunkQueryFirstChunk(t *testing.T) {
	q, args := buildChunkQuery(`"id","name"`, `"public"`, `"users"`, `"id"`, nil, 100)
	want := `SELECT "id","name" FROM "public"."users" ORDER BY "id" LIMIT $1`
	if q != want {
		t.Errorf("query = %q, want %q", q, want)
	}
	if len(args) != 1 || args[0] != 100 {
		t.Errorf("args = %v", args)
	}
}

func TestBuildChunkQuerySubsequentChunk(t *testing.T) {
	q, args := buildChunkQuery(`"id"`, `"s"`, `"t"`, `"id"`, 42, 50)
	want := `SELECT "id" FROM "s"."t" WHERE "id" > $1 ORDER BY "id" LIMIT $2`
	if q != want {
		t.Errorf("query = %q, want %q", q, want)
	}
	if len(args) != 2 || args[0] != 42 || args[1] != 50 {
		t.Errorf("args = %v", args)
	}
}

func TestBuildSnapshotEvent_AllFields(t *testing.T) {
	ev := buildSnapshotEvent("public", "users", []string{"id", "name"}, []interface{}{42, "alice"})

	if ev.Type != cdc.Insert {
		t.Errorf("Type = %v, want Insert", ev.Type)
	}
	if ev.Schema != "public" || ev.Table != "users" {
		t.Errorf("schema/table = %s/%s", ev.Schema, ev.Table)
	}
	if !strings.Contains(ev.Data, `"id":"42"`) {
		t.Errorf("payload missing id: %s", ev.Data)
	}
	if !strings.Contains(ev.Data, `"name":"alice"`) {
		t.Errorf("payload missing name: %s", ev.Data)
	}
}

func TestBuildSnapshotEvent_NullValue(t *testing.T) {
	ev := buildSnapshotEvent("s", "t", []string{"col"}, []interface{}{nil})
	if ev.Data != `{"col":null}` {
		t.Errorf("payload = %q, want %q", ev.Data, `{"col":null}`)
	}
}

func TestBuildSnapshotEvent_FewerValuesThanColumns(t *testing.T) {
	// Defensive: when values is shorter than columns, missing entries become null.
	ev := buildSnapshotEvent("s", "t", []string{"a", "b", "c"}, []interface{}{1, "two"})
	if !strings.Contains(ev.Data, `"a":"1"`) ||
		!strings.Contains(ev.Data, `"b":"two"`) ||
		!strings.Contains(ev.Data, `"c":null`) {
		t.Errorf("payload = %s", ev.Data)
	}
}

func TestBuildSnapshotEvent_EscapesQuotes(t *testing.T) {
	ev := buildSnapshotEvent("s", "t", []string{`col"name`}, []interface{}{`val"ue`})
	if !strings.Contains(ev.Data, `col\"name`) || !strings.Contains(ev.Data, `val\"ue`) {
		t.Errorf("payload didn't escape quotes: %s", ev.Data)
	}
}

func TestBuildSnapshotEvent_EmptyColumns(t *testing.T) {
	ev := buildSnapshotEvent("s", "t", []string{}, []interface{}{})
	if ev.Data != `{}` {
		t.Errorf("payload = %q, want %q", ev.Data, `{}`)
	}
}
