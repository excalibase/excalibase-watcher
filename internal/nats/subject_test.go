package nats

import (
	"testing"
)

func TestSanitizeSubjectTokenReplacesWhitespaceAndControlCharacters(t *testing.T) {
	cases := map[string]struct{ in, want string }{
		"space":         {"my table", "my_table"},
		"tab":           {"a\tb", "a_b"},
		"newline":       {"a\nb", "a_b"},
		"carriage":      {"a\rb", "a_b"},
		"nul":           {"a\x00b", "a_b"},
		"other control": {"a\x07b\x1bc\x7fd", "a_b_c_d"},
		"unicode space": {"a b", "a_b"},
		"unicode kept":  {"données_表", "données_表"},
		"wildcards":     {"a.b*c>d", "a_b_c_d"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if got := sanitizeSubjectToken(tc.in); got != tc.want {
				t.Errorf("sanitizeSubjectToken(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestBuildSubjectHasNoControlCharactersForOddTableNames(t *testing.T) {
	got := buildSubject("cdc", "public", "bad\x00na\tme\n")
	if err := validateSubject(got); err != nil {
		t.Errorf("subject %q invalid: %v", got, err)
	}
}

func TestValidateSubjectRejectsUnpublishableSubjects(t *testing.T) {
	for _, subject := range []string{"", "a b", "a\tb", "a\x00b", "a..b", ".a", "a."} {
		if err := validateSubject(subject); err == nil {
			t.Errorf("validateSubject(%q) = nil, want error", subject)
		}
	}
	if err := validateSubject("cdc.public.users"); err != nil {
		t.Errorf("valid subject rejected: %v", err)
	}
}
