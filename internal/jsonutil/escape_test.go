package jsonutil

import "testing"

func TestEscapeString(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"plain", "hello", "hello"},
		{"empty", "", ""},
		{"backslash", `a\b`, `a\\b`},
		{"quote", `a"b`, `a\"b`},
		{"newline", "a\nb", `a\nb`},
		{"cr", "a\rb", `a\rb`},
		{"tab", "a\tb", `a\tb`},
		{"backspace", "a\bb", `a\bb`},
		{"formfeed", "a\fb", `a\fb`},
		{"null", "a\x00b", `a\u0000b`},
		{"bell", "a\x07b", `a\u0007b`},
		{"vertical_tab", "a\x0bb", `a\u000bb`},
		{"del", "a\x7fb", `a\u007fb`},
		{"unicode_passthrough", "héllo", "héllo"},
		{"emoji", "a🚀b", "a🚀b"},
		{"mixed", "x\"y\\z\n\x01", `x\"y\\z\n\u0001`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := EscapeString(c.in)
			if got != c.want {
				t.Errorf("EscapeString(%q) = %q, want %q", c.in, got, c.want)
			}
		})
	}
}
