package mysql

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

// ErrServerNotReady means the server is not set up for row-based GTID
// replication, or the watcher's login lacks a privilege it needs.
var ErrServerNotReady = errors.New("mysql: server is not ready for CDC")

// serverSettings are the global variables that decide what the binlog holds.
type serverSettings struct {
	LogBin       string
	BinlogFormat string
	RowImage     string
	GTIDMode     string
	EnforceGTID  string
}

const settingsQuery = "SELECT @@GLOBAL.log_bin, @@GLOBAL.binlog_format, @@GLOBAL.binlog_row_image, " +
	"@@GLOBAL.gtid_mode, @@GLOBAL.enforce_gtid_consistency"

// checkServer returns every reason the watcher cannot stream from this
// server, so one start-up tells the operator the whole list.
func checkServer(s serverSettings, grants []string, schemaName string, tables []string) error {
	var problems []string
	want := func(name, got, expected string) {
		if !strings.EqualFold(got, expected) {
			problems = append(problems, fmt.Sprintf("%s is %q, must be %s", name, got, expected))
		}
	}
	want("log_bin", s.LogBin, "1")
	want("binlog_format", s.BinlogFormat, "ROW")
	want("binlog_row_image", s.RowImage, "FULL")
	want("gtid_mode", s.GTIDMode, "ON")
	want("enforce_gtid_consistency", s.EnforceGTID, "ON")
	problems = append(problems, missingGrants(grants, schemaName, tables)...)
	if len(problems) == 0 {
		return nil
	}
	return fmt.Errorf("%w: %s", ErrServerNotReady, strings.Join(problems, "; "))
}

// grantLine is `GRANT <privileges> ON <scope> TO ...` from SHOW GRANTS.
var grantLine = regexp.MustCompile("^GRANT (.+?) ON (.+?) TO ")

type grant struct {
	privileges string
	database   string // "*" for every database
	table      string // "*" for every table
}

func parseGrants(lines []string) []grant {
	var grants []grant
	for _, line := range lines {
		m := grantLine.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		database, table, _ := strings.Cut(m[2], ".")
		grants = append(grants, grant{
			privileges: strings.ToUpper(m[1]),
			database:   unquoteGrantName(database),
			table:      unquoteGrantName(table),
		})
	}
	return grants
}

func unquoteGrantName(name string) string {
	name = strings.Trim(name, "`")
	return strings.ReplaceAll(name, `\_`, "_")
}

func (g grant) has(privilege string) bool {
	return strings.Contains(g.privileges, privilege) || strings.HasPrefix(g.privileges, "ALL")
}

func missingGrants(lines []string, schemaName string, tables []string) []string {
	grants := parseGrants(lines)
	var problems []string
	for _, privilege := range []string{"REPLICATION SLAVE", "REPLICATION CLIENT"} {
		if !anyGrant(grants, func(g grant) bool { return g.database == "*" && g.has(privilege) }) {
			problems = append(problems, "the login lacks "+privilege+" on *.*")
		}
	}
	if !canSelect(grants, schemaName, tables) {
		problems = append(problems, fmt.Sprintf("the login lacks SELECT on %s", selectScope(schemaName, tables)))
	}
	return problems
}

func canSelect(grants []grant, schemaName string, tables []string) bool {
	if anyGrant(grants, func(g grant) bool {
		return g.has("SELECT") && (g.database == "*" || (g.database == schemaName && g.table == "*"))
	}) {
		return true
	}
	if len(tables) == 0 {
		return false
	}
	for _, table := range tables {
		if !anyGrant(grants, func(g grant) bool { return g.has("SELECT") && g.database == schemaName && g.table == table }) {
			return false
		}
	}
	return true
}

func anyGrant(grants []grant, match func(grant) bool) bool {
	for _, g := range grants {
		if match(g) {
			return true
		}
	}
	return false
}

func selectScope(schemaName string, tables []string) string {
	if len(tables) == 0 {
		return schemaName + ".*"
	}
	return schemaName + "." + strings.Join(tables, ", "+schemaName+".")
}
