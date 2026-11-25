package mysql

import (
	"errors"
	"fmt"
	"strings"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
)

var (
	// ErrGTIDPurged means the server no longer holds binlogs the saved offset
	// needs; resuming would silently skip those transactions.
	ErrGTIDPurged = errors.New("mysql: the saved GTID set is older than the server's purged binlogs")
	// ErrOffsetAheadOfServer means the saved offset names transactions the
	// server has not executed: a lagging replica, or a different server.
	ErrOffsetAheadOfServer = errors.New("mysql: the saved GTID set is ahead of what this server has executed")
	// ErrBadOffset is a saved offset that is not a GTID set.
	ErrBadOffset = errors.New("mysql: the saved offset is not a GTID set")
)

// parseGTIDSet reads a GTID set as @@gtid_executed prints it, with line
// breaks between the uuids.
func parseGTIDSet(s string) (gomysql.GTIDSet, error) {
	return gomysql.ParseMysqlGTIDSet(strings.Join(strings.Fields(s), ""))
}

// resolveStart picks the GTID set to stream from. The saved set is the set of
// transactions already published and acked, so the server sends everything
// after it. It is valid on any server that has executed it, which is why a
// failover to another replica keeps the place; it is refused when the server
// has purged binlogs the set still needs, or has not yet executed it.
// fresh is true when there was nothing to resume (first start), so
// the stream begins at the server's current position.
func resolveStart(saved string, executed, purged gomysql.GTIDSet) (start gomysql.GTIDSet, fresh bool, err error) {
	if saved == "" {
		return executed.Clone(), true, nil
	}
	set, err := parseGTIDSet(saved)
	if err != nil {
		return nil, false, fmt.Errorf("%w: %q: %v", ErrBadOffset, saved, err)
	}
	if !executed.Contain(set) {
		return nil, false, fmt.Errorf("%w: saved %s, server executed %s; "+
			"wait for the replica to catch up, or set mysql.reset_offset (WATCHER_MYSQL_RESET_OFFSET=true) "+
			"if this is a different database", ErrOffsetAheadOfServer, set, executed)
	}
	if !set.Contain(purged) {
		return nil, false, fmt.Errorf("%w: saved %s, server purged %s; the changes in between are gone. "+
			"To start over from the server's current position set mysql.reset_offset "+
			"(WATCHER_MYSQL_RESET_OFFSET=true) for one start and re-snapshot the table data downstream "+
			"(mysql.snapshot_mode=backup_file with a fresh dump); then unset it", ErrGTIDPurged, set, purged)
	}
	return set, false, nil
}
