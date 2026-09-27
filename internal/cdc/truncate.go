package cdc

import (
	"encoding/json"
	"slices"
)

// Truncated returns a copy that carries only the key columns of the row, in
// the same shape as Data (UPDATE keeps "old" and "new"). DDL loses its
// statement; a row whose key is unknown keeps an empty object.
func (e Event) Truncated() Event {
	truncated := e.Stripped()
	if isTableEvent(e.Type) {
		truncated.Data = keyOnly(e.Type, e.Data, e.KeyColumns)
	}
	return truncated
}

// Stripped returns a copy without any data, marked truncated.
func (e Event) Stripped() Event {
	stripped := e
	stripped.Data = ""
	stripped.DataTruncated = true
	return stripped
}

func keyOnly(eventType EventType, data string, key []string) string {
	var row map[string]json.RawMessage
	if err := json.Unmarshal([]byte(data), &row); err != nil {
		return "{}"
	}
	if eventType != Update {
		return marshalRow(keyColumns(row, key))
	}
	images := make(map[string]json.RawMessage, len(row))
	for _, image := range []string{"old", "new"} {
		var values map[string]json.RawMessage
		if json.Unmarshal(row[image], &values) == nil && values != nil {
			images[image] = json.RawMessage(marshalRow(keyColumns(values, key)))
		}
	}
	return marshalRow(images)
}

func keyColumns(row map[string]json.RawMessage, key []string) map[string]json.RawMessage {
	kept := make(map[string]json.RawMessage, len(key))
	for column, value := range row {
		if slices.Contains(key, column) {
			kept[column] = value
		}
	}
	return kept
}

func marshalRow(row map[string]json.RawMessage) string {
	data, err := json.Marshal(row)
	if err != nil {
		return "{}"
	}
	return string(data)
}
