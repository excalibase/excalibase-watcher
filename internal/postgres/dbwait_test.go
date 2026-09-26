package postgres

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

func refusedDial() error {
	return &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}
}

func TestDatabaseNotReadyClassifiesStartupFailures(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"connection refused", refusedDial(), true},
		{"unresolvable host", &net.DNSError{Err: "no such host", Name: "db", IsNotFound: true}, true},
		{"starting up", &pgconn.PgError{Code: "57P03"}, true},
		{"shutting down", &pgconn.PgError{Code: "57P01"}, true},
		{"too many connections", &pgconn.PgError{Code: "53300"}, true},
		{"wrapped starting up", fmt.Errorf("connect: %w", &pgconn.PgError{Code: "57P03"}), true},
		{"wrong password", &pgconn.PgError{Code: "28P01"}, false},
		{"no such database", &pgconn.PgError{Code: "3D000"}, false},
		{"cancelled", context.Canceled, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := databaseNotReady(tc.err); got != tc.want {
				t.Errorf("databaseNotReady(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

func TestStartupBackoffDoublesUpToItsCap(t *testing.T) {
	backoff := startupBackoff{next: time.Second, max: 5 * time.Second}
	var got []time.Duration
	for range 5 {
		got = append(got, backoff.step())
	}
	want := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 5 * time.Second, 5 * time.Second}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("delays = %v, want %v", got, want)
		}
	}
}

func fastBackoff() startupBackoff {
	return startupBackoff{next: time.Millisecond, max: 2 * time.Millisecond}
}

func TestWaitForDatabaseRetriesUntilTheDatabaseAnswers(t *testing.T) {
	attempts := 0
	dial := func(context.Context) (string, error) {
		attempts++
		if attempts < 3 {
			return "", refusedDial()
		}
		return "conn", nil
	}
	got, err := waitForDatabase(context.Background(), dial, fastBackoff())
	if err != nil || got != "conn" {
		t.Fatalf("waitForDatabase = (%q, %v), want (conn, nil)", got, err)
	}
	if attempts != 3 {
		t.Errorf("attempts = %d, want 3", attempts)
	}
}

func TestWaitForDatabaseFailsAtOnceOnAnError(t *testing.T) {
	attempts := 0
	dial := func(context.Context) (string, error) {
		attempts++
		return "", &pgconn.PgError{Code: "28P01", Message: "password authentication failed"}
	}
	if _, err := waitForDatabase(context.Background(), dial, fastBackoff()); err == nil {
		t.Fatal("a refused login is not a database that is still starting")
	}
	if attempts != 1 {
		t.Errorf("attempts = %d, want 1", attempts)
	}
}

func TestWaitForDatabaseStopsWhenCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	dial := func(context.Context) (string, error) {
		cancel()
		return "", refusedDial()
	}
	_, err := waitForDatabase(ctx, dial, startupBackoff{next: time.Hour, max: time.Hour})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}
