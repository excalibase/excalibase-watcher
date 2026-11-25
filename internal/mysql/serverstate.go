package mysql

import (
	"fmt"

	"github.com/go-mysql-org/go-mysql/canal"
	gomysql "github.com/go-mysql-org/go-mysql/mysql"
)

// verifyServer reads the server's binlog settings and the login's grants and
// refuses to start when either is wrong.
func verifyServer(c *canal.Canal, schemaName string, tables []string) error {
	settings, err := readSettings(c)
	if err != nil {
		return err
	}
	grants, err := c.Execute("SHOW GRANTS FOR CURRENT_USER()")
	if err != nil {
		return fmt.Errorf("reading the login's grants: %w", err)
	}
	lines := make([]string, 0, grants.RowNumber())
	for row := 0; row < grants.RowNumber(); row++ {
		line, err := grants.GetString(row, 0)
		if err != nil {
			return fmt.Errorf("reading the login's grants: %w", err)
		}
		lines = append(lines, line)
	}
	return checkServer(settings, lines, schemaName, tables)
}

func readSettings(c *canal.Canal) (serverSettings, error) {
	var settings serverSettings
	result, err := c.Execute(settingsQuery)
	if err != nil {
		return settings, fmt.Errorf("reading binlog settings: %w", err)
	}
	if result.RowNumber() != 1 || result.ColumnNumber() != 5 {
		return settings, fmt.Errorf("reading binlog settings: unexpected result of %d rows", result.RowNumber())
	}
	fields := []*string{&settings.LogBin, &settings.BinlogFormat, &settings.RowImage, &settings.GTIDMode, &settings.EnforceGTID}
	for i, field := range fields {
		if *field, err = result.GetString(0, i); err != nil {
			return settings, fmt.Errorf("reading binlog settings: %w", err)
		}
	}
	return settings, nil
}

// serverGTIDSets returns the transactions the server has executed and the
// ones whose binlogs it has purged.
func serverGTIDSets(c *canal.Canal) (executed, purged gomysql.GTIDSet, err error) {
	if executed, err = queryGTIDSet(c, "executed"); err != nil {
		return nil, nil, err
	}
	if purged, err = queryGTIDSet(c, "purged"); err != nil {
		return nil, nil, err
	}
	return executed, purged, nil
}

func queryGTIDSet(c *canal.Canal, which string) (gomysql.GTIDSet, error) {
	result, err := c.Execute("SELECT @@GLOBAL.gtid_" + which)
	if err != nil {
		return nil, fmt.Errorf("reading the server's %s GTID set: %w", which, err)
	}
	text, err := result.GetString(0, 0)
	if err != nil {
		return nil, fmt.Errorf("reading the server's %s GTID set: %w", which, err)
	}
	set, err := parseServerGTIDSet(text)
	if err != nil {
		return nil, fmt.Errorf("parsing the server's %s GTID set: %w", which, err)
	}
	return set, nil
}
