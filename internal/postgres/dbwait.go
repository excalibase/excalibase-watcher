package postgres

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

const firstStartupDelay = time.Second

// startupSQLStates are the server answers that mean "not yet", not "no".
var startupSQLStates = map[string]bool{
	"57P01": true, // admin_shutdown
	"57P02": true, // crash_shutdown
	"57P03": true, // cannot_connect_now
	"53300": true, // too_many_connections
}

type startupBackoff struct {
	next time.Duration
	max  time.Duration
}

func (b *startupBackoff) step() time.Duration {
	delay := b.next
	b.next = min(b.next*2, b.max)
	return delay
}

// databaseNotReady reports whether a connect error means the database is not
// up yet. Anything the server rejects for another reason is a real error.
func databaseNotReady(err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return startupSQLStates[pgErr.Code]
	}
	return true
}

// waitForDatabase dials until the database answers, backing off between
// attempts, so a database that comes up after the watcher is not a crash.
func waitForDatabase[T any](ctx context.Context, dial func(context.Context) (T, error), backoff startupBackoff) (T, error) {
	for {
		conn, err := dial(ctx)
		if err == nil {
			return conn, nil
		}
		if ctx.Err() != nil {
			return conn, ctx.Err()
		}
		if !databaseNotReady(err) {
			return conn, err
		}
		delay := backoff.step()
		slog.Warn("database not ready, waiting", "error", err, "retry_in", delay)
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return conn, ctx.Err()
		}
	}
}
