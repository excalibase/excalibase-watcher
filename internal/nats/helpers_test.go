package nats

import (
	"testing"

	"github.com/excalibase/watcher-go/internal/cdc"
	"github.com/excalibase/watcher-go/internal/config"
)

func TestBuildSubject(t *testing.T) {
	cases := []struct {
		name, prefix, schema, table, want string
	}{
		{"basic", "cdc", "public", "users", "cdc.public.users"},
		{"tenant_prefix", "cdc.tenant-a", "public", "users", "cdc.tenant-a.public.users"},
		{"empty_schema_defaults", "cdc", "", "users", "cdc.default.users"},
		{"empty_table_is_ddl", "cdc", "public", "", "cdc.public._ddl"},
		{"both_empty", "cdc", "", "", "cdc.default._ddl"},
		{"sanitize_dots_in_schema", "cdc", "a.b", "users", "cdc.a_b.users"},
		{"sanitize_wildcard_star", "cdc", "public", "us*rs", "cdc.public.us_rs"},
		{"sanitize_wildcard_gt", "cdc", "public", "us>rs", "cdc.public.us_rs"},
		{"sanitize_space", "cdc", "my schema", "my table", "cdc.my_schema.my_table"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := buildSubject(c.prefix, c.schema, c.table); got != c.want {
				t.Errorf("buildSubject(%q,%q,%q) = %q, want %q", c.prefix, c.schema, c.table, got, c.want)
			}
		})
	}
}

func TestSanitizeSubjectToken(t *testing.T) {
	cases := map[string]string{
		"normal":  "normal",
		"a.b":     "a_b",
		"a*b":     "a_b",
		"a>b":     "a_b",
		"a b":     "a_b",
		"a.b*c>d": "a_b_c_d",
	}
	for in, want := range cases {
		if got := sanitizeSubjectToken(in); got != want {
			t.Errorf("sanitizeSubjectToken(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestShouldPublish(t *testing.T) {
	cases := []struct {
		t    cdc.EventType
		want bool
	}{
		{cdc.Insert, true},
		{cdc.Update, true},
		{cdc.Delete, true},
		{cdc.DDL, true},
		{cdc.Truncate, true},
		{cdc.Begin, false},
		{cdc.Commit, false},
		{cdc.Heartbeat, false},
	}
	for _, c := range cases {
		if got := shouldPublish(c.t); got != c.want {
			t.Errorf("shouldPublish(%v) = %v, want %v", c.t, got, c.want)
		}
	}
}

func TestNewPublisherInitialState(t *testing.T) {
	svc := cdc.NewService()
	p := NewPublisher(config.NATSConfig{URL: "nats://x", StreamName: "S", SubjectPrefix: "cdc"}, svc)
	if got := p.LastAckedLSN(); got != "" {
		t.Errorf("LastAckedLSN on new publisher = %q, want empty", got)
	}
}

func TestSetOnPublished(t *testing.T) {
	svc := cdc.NewService()
	p := NewPublisher(config.NATSConfig{}, svc)
	var called bool
	p.SetOnPublished(func(cdc.Event) { called = true })
	if p.onPublished == nil {
		t.Fatal("onPublished not set")
	}
	p.onPublished(cdc.Event{})
	if !called {
		t.Error("callback not invoked")
	}
}
