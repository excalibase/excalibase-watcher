package postgres

import (
	"encoding/binary"
	"strings"
	"testing"
)

func makeReaderForText(value string) *reader {
	buf := make([]byte, 4+len(value))
	binary.BigEndian.PutUint32(buf[:4], uint32(len(value)))
	copy(buf[4:], value)
	return &reader{data: buf}
}

func TestWriteTupleValueNull(t *testing.T) {
	var sb strings.Builder
	writeTupleValue(&sb, &reader{}, 'n', 0)
	if sb.String() != "null" {
		t.Errorf("got %q", sb.String())
	}
}

func TestWriteTupleValueUnchangedTOAST(t *testing.T) {
	var sb strings.Builder
	writeTupleValue(&sb, &reader{}, 'u', 0)
	if sb.String() != `"unchanged"` {
		t.Errorf("got %q", sb.String())
	}
}

func TestWriteTupleValueUnknownMarker(t *testing.T) {
	var sb strings.Builder
	writeTupleValue(&sb, &reader{}, 'x', 0)
	if sb.String() != `"unknown"` {
		t.Errorf("got %q", sb.String())
	}
}

func TestWriteTextValueNumeric(t *testing.T) {
	var sb strings.Builder
	writeTextValue(&sb, makeReaderForText("42"), 23) // int4 OID
	if sb.String() != "42" {
		t.Errorf("got %q", sb.String())
	}
}

func TestWriteTextValueBoolTrue(t *testing.T) {
	var sb strings.Builder
	writeTextValue(&sb, makeReaderForText("t"), boolOID)
	if sb.String() != "true" {
		t.Errorf("got %q", sb.String())
	}
}

func TestWriteTextValueBoolFalse(t *testing.T) {
	var sb strings.Builder
	writeTextValue(&sb, makeReaderForText("f"), boolOID)
	if sb.String() != "false" {
		t.Errorf("got %q", sb.String())
	}
}

func TestWriteTextValueValidJSON(t *testing.T) {
	var sb strings.Builder
	writeTextValue(&sb, makeReaderForText(`{"a":1}`), 3802) // jsonb
	if sb.String() != `{"a":1}` {
		t.Errorf("got %q", sb.String())
	}
}

func TestWriteTextValueInvalidJSONFallsBackToString(t *testing.T) {
	var sb strings.Builder
	writeTextValue(&sb, makeReaderForText(`not-json`), 3802)
	if sb.String() != `"not-json"` {
		t.Errorf("got %q", sb.String())
	}
}

func TestWriteTextValueStringEscaped(t *testing.T) {
	var sb strings.Builder
	writeTextValue(&sb, makeReaderForText(`hello "world"`), 25) // text OID (not mapped)
	if sb.String() != `"hello \"world\""` {
		t.Errorf("got %q", sb.String())
	}
}
