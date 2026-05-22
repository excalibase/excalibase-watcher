package postgres

import "testing"

func TestValidateIdentifier(t *testing.T) {
	cases := []struct {
		in      string
		wantErr bool
	}{
		{"users", false},
		{"my_table", false},
		{"_leading_underscore", false},
		{"TableName", false},
		{"t1", false},
		{"a", false},
		{"", true},
		{"1users", true},
		{"user-name", true},
		{"user.name", true},
		{"user name", true},
		{"users;DROP TABLE x", true},
		{"users$", true},
		{"привет", true},
	}
	for _, c := range cases {
		t.Run(c.in, func(t *testing.T) {
			err := validateIdentifier(c.in)
			if (err != nil) != c.wantErr {
				t.Errorf("validateIdentifier(%q) err=%v wantErr=%v", c.in, err, c.wantErr)
			}
		})
	}
}

func TestEnsureReplicationParam(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"postgres://h/db", "postgres://h/db?replication=database"},
		{"postgres://h/db?sslmode=disable", "postgres://h/db?sslmode=disable&replication=database"},
		{"postgres://h/db?replication=database", "postgres://h/db?replication=database"},
		{"postgres://h/db?replication=database&x=1", "postgres://h/db?replication=database&x=1"},
	}
	for _, c := range cases {
		if got := ensureReplicationParam(c.in); got != c.want {
			t.Errorf("ensureReplicationParam(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestStripReplicationParam(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"postgres://h/db?replication=database", "postgres://h/db"},
		{"postgres://h/db?sslmode=disable&replication=database", "postgres://h/db?sslmode=disable"},
		{"postgres://h/db?replication=database&sslmode=disable", "postgres://h/db?sslmode=disable"},
		{"postgres://h/db", "postgres://h/db"},
		{"postgres://h/db?sslmode=disable", "postgres://h/db?sslmode=disable"},
	}
	for _, c := range cases {
		if got := stripReplicationParam(c.in); got != c.want {
			t.Errorf("stripReplicationParam(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestParseSchema(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"postgres://h/db", "public"},
		{"postgres://h/db?currentSchema=inventory", "inventory"},
		{"postgres://h/db?search_path=app", "app"},
		{"postgres://h/db?other=x", "public"},
		{"::not-a-url::", "public"},
	}
	for _, c := range cases {
		if got := parseSchema(c.in); got != c.want {
			t.Errorf("parseSchema(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}
