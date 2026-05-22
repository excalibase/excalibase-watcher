package jsonutil

import (
	"fmt"
	"strings"
)

// EscapeString escapes a string for safe embedding in JSON.
// Handles all control characters per RFC 8259.
func EscapeString(s string) string {
	var sb strings.Builder
	sb.Grow(len(s))
	for _, c := range s {
		switch {
		case c == '\\':
			sb.WriteString(`\\`)
		case c == '"':
			sb.WriteString(`\"`)
		case c == '\n':
			sb.WriteString(`\n`)
		case c == '\r':
			sb.WriteString(`\r`)
		case c == '\t':
			sb.WriteString(`\t`)
		case c == '\b':
			sb.WriteString(`\b`)
		case c == '\f':
			sb.WriteString(`\f`)
		case c < 0x20 || c == 0x7f:
			// Control characters per RFC 8259
			sb.WriteString(fmt.Sprintf(`\u%04x`, c))
		default:
			sb.WriteRune(c)
		}
	}
	return sb.String()
}
