package cdc

import (
	"encoding/json"
	"testing"
)

func assertJSON(t *testing.T, got, want string) {
	t.Helper()
	var gotValue, wantValue any
	if err := json.Unmarshal([]byte(got), &gotValue); err != nil {
		t.Fatalf("data %q is not JSON: %v", got, err)
	}
	if err := json.Unmarshal([]byte(want), &wantValue); err != nil {
		t.Fatalf("want %q is not JSON: %v", want, err)
	}
	gotText, _ := json.Marshal(gotValue)
	wantText, _ := json.Marshal(wantValue)
	if string(gotText) != string(wantText) {
		t.Errorf("data = %s, want %s", gotText, wantText)
	}
}

func TestTruncatedRowKeepsOnlyItsKeyColumns(t *testing.T) {
	event := NewEvent(Insert, "public", "docs", `{"id":7,"tenant":"a","body":"huge"}`, "INSERT", "0/10")
	event.KeyColumns = []string{"id", "tenant"}

	truncated := event.Truncated()

	assertJSON(t, truncated.Data, `{"id":7,"tenant":"a"}`)
	if !truncated.DataTruncated {
		t.Error("truncated event must say so")
	}
	if truncated.LSN != "0/10" || truncated.Table != "docs" || truncated.Type != Insert {
		t.Errorf("truncated event lost its identity: %+v", truncated)
	}
	if event.DataTruncated || event.Data != `{"id":7,"tenant":"a","body":"huge"}` {
		t.Error("the original event must not change")
	}
}

func TestTruncatedUpdateKeepsKeysOfOldAndNewImages(t *testing.T) {
	event := NewEvent(Update, "public", "docs", `{"old":{"id":6,"body":"x"},"new":{"id":7,"body":"huge"}}`, "UPDATE", "0/10")
	event.KeyColumns = []string{"id"}

	assertJSON(t, event.Truncated().Data, `{"old":{"id":6},"new":{"id":7}}`)
}

func TestTruncatedWithoutKnownKeyHasNoData(t *testing.T) {
	event := NewEvent(Delete, "public", "docs", `{"body":"huge"}`, "DELETE", "0/10")

	assertJSON(t, event.Truncated().Data, `{}`)
}

func TestTruncatedDDLDropsTheStatement(t *testing.T) {
	event := NewEvent(DDL, "public", "", "CREATE TABLE huge (...)", "DDL", "0/10")

	if got := event.Truncated().Data; got != "" {
		t.Errorf("DDL data = %q, want empty", got)
	}
}

func TestStrippedHasNoDataAtAll(t *testing.T) {
	event := NewEvent(Insert, "public", "docs", `{"id":7}`, "INSERT", "0/10")
	event.KeyColumns = []string{"id"}

	stripped := event.Stripped()
	if stripped.Data != "" || !stripped.DataTruncated {
		t.Errorf("stripped = %+v, want no data and marked truncated", stripped)
	}
}

func TestTruncatedFlagIsOmittedFromNormalPayloads(t *testing.T) {
	data, err := json.Marshal(NewEvent(Insert, "public", "docs", `{"id":7}`, "INSERT", "0/10"))
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]any
	if err := json.Unmarshal(data, &fields); err != nil {
		t.Fatal(err)
	}
	if _, ok := fields["dataTruncated"]; ok {
		t.Error("dataTruncated must be absent unless set")
	}
	if _, ok := fields["KeyColumns"]; ok {
		t.Error("key columns are internal")
	}
}
