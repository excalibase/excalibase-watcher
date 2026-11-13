package nats

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/excalibase/watcher-go/internal/cdc"
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

// An invalid subject can never succeed, so retrying would hold the WAL forever.
func TestInvalidSubjectIsSkippedWithoutRetryAndPositionAdvances(t *testing.T) {
	var attempts atomic.Int32
	p := newTestPublisher(func(context.Context, string, []byte, string) error {
		attempts.Add(1)
		return nil
	})
	p.cfg.SubjectPrefix = "cdc..bad"
	var observed atomic.Int32
	p.AddOnPublished(func(cdc.Event) { observed.Add(1) })

	event := cdc.NewEvent(cdc.Insert, "public", "users", `{"id":1}`, "INSERT", "0/10")
	if err := p.publish(context.Background(), event); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if attempts.Load() != 0 {
		t.Errorf("attempts = %d, want 0", attempts.Load())
	}
	if observed.Load() != 1 {
		t.Errorf("observers notified %d times, want 1 so the source position advances", observed.Load())
	}
}
