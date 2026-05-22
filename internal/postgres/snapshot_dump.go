package postgres

import (
	"bufio"
	"fmt"
	"os"
	"regexp"
	"strings"

	"github.com/excalibase/watcher-go/internal/cdc"
	"github.com/excalibase/watcher-go/internal/jsonutil"
)

// COPY public.users (id, name, email) FROM stdin;
var copyHeaderRegex = regexp.MustCompile(`^COPY\s+(\S+)\.(\S+)\s+\(([^)]+)\)\s+FROM\s+stdin;`)

func ParseDumpFile(filePath, startLSN string, handler func(cdc.Event)) error {
	f, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("opening dump file: %w", err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 1024*1024), 10*1024*1024) // 10MB max line

	var (
		inCopy  bool
		schema  string
		table   string
		columns []string
	)

	for scanner.Scan() {
		line := scanner.Text()

		if inCopy {
			if line == `\.` {
				inCopy = false
				continue
			}
			event := parseCopyLine(line, schema, table, columns, startLSN)
			handler(event)
			continue
		}

		matches := copyHeaderRegex.FindStringSubmatch(line)
		if matches != nil {
			schema = matches[1]
			table = matches[2]
			if err := validateIdentifier(schema); err != nil {
				return fmt.Errorf("dump file: invalid schema %q: %w", schema, err)
			}
			if err := validateIdentifier(table); err != nil {
				return fmt.Errorf("dump file: invalid table %q: %w", table, err)
			}
			colStr := matches[3]
			columns = splitColumns(colStr)
			inCopy = true
		}
	}

	return scanner.Err()
}

func parseCopyLine(line, schema, table string, columns []string, lsn string) cdc.Event {
	values := strings.Split(line, "\t")

	var sb strings.Builder
	sb.WriteByte('{')
	for i, col := range columns {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteByte('"')
		sb.WriteString(col)
		sb.WriteString(`":`)

		if i < len(values) {
			val := values[i]
			if val == `\N` {
				sb.WriteString("null")
			} else {
				sb.WriteByte('"')
				sb.WriteString(jsonutil.EscapeString(val))
				sb.WriteByte('"')
			}
		} else {
			sb.WriteString("null")
		}
	}
	sb.WriteByte('}')

	return cdc.NewEvent(cdc.Insert, schema, table, sb.String(), "INSERT", lsn)
}

func splitColumns(s string) []string {
	parts := strings.Split(s, ",")
	result := make([]string, len(parts))
	for i, p := range parts {
		result[i] = strings.TrimSpace(p)
	}
	return result
}
