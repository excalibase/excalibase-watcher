package mysql

import (
	"bufio"
	"fmt"
	"os"
	"regexp"
	"strings"

	"github.com/excalibase/watcher-go/internal/cdc"
	"github.com/excalibase/watcher-go/internal/jsonutil"
)

// SET @@GLOBAL.GTID_PURGED=/*!80000 '+'*/ 'uuid:1-5,\nuuid2:1-3';
// mysqldump writes the set the dump already contains, which is where the
// binlog stream must pick up. A long set runs over several lines.
var (
	gtidPurgedStart = regexp.MustCompile(`GTID_PURGED\s*=\s*(?:/\*!\d+ '\+'\*/\s*)?'([^']*)('?)`)
)

// INSERT INTO `table` VALUES (...),(...);
var insertRegex = regexp.MustCompile("^INSERT INTO `([^`]+)` VALUES ")

// ParseMysqlDumpFile replays the dump's rows to handler and returns the dump's
// GTID set, empty when the dump carries none.
func ParseMysqlDumpFile(filePath, schema string, handler func(cdc.Event)) (string, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return "", fmt.Errorf("opening dump file: %w", err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 1024*1024), 50*1024*1024) // 50MB max line

	var gtidSet strings.Builder
	inGTIDSet := false

	for scanner.Scan() {
		line := scanner.Text()

		if inGTIDSet {
			text, closed := strings.CutSuffix(strings.TrimSuffix(strings.TrimSpace(line), ";"), "'")
			gtidSet.WriteString(strings.TrimSpace(text))
			inGTIDSet = !closed
			continue
		}
		if matches := gtidPurgedStart.FindStringSubmatch(line); matches != nil {
			gtidSet.WriteString(strings.TrimSpace(matches[1]))
			inGTIDSet = matches[2] == ""
			continue
		}

		// Parse INSERT INTO statements
		if matches := insertRegex.FindStringSubmatch(line); matches != nil {
			table := matches[1]
			valuesStr := line[len(matches[0]):]
			tuples := parseValuesList(valuesStr)
			for _, tuple := range tuples {
				data := tupleToJSON(tuple)
				event := cdc.NewEvent(cdc.Insert, schema, table, data, "INSERT", "")
				handler(event)
			}
		}
	}

	return gtidSet.String(), scanner.Err()
}

// parseValuesList parses "(v1,v2,...),(v1,v2,...)" from a mysqldump INSERT.
func parseValuesList(s string) [][]string {
	var tuples [][]string
	var current []string
	var value strings.Builder
	inString := false
	escaped := false

	for i := 0; i < len(s); i++ {
		c := s[i]

		if escaped {
			value.WriteByte(c)
			escaped = false
			continue
		}

		if c == '\\' && inString {
			escaped = true
			value.WriteByte(c)
			continue
		}

		if c == '\'' {
			inString = !inString
			continue
		}

		if inString {
			value.WriteByte(c)
			continue
		}

		switch c {
		case '(':
			current = nil
			value.Reset()
		case ',':
			current = append(current, value.String())
			value.Reset()
		case ')':
			current = append(current, value.String())
			tuples = append(tuples, current)
			value.Reset()
		case ';':
			// end of statement
		}
	}

	return tuples
}

func tupleToJSON(values []string) string {
	var sb strings.Builder
	sb.WriteByte('{')
	for i, v := range values {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(fmt.Sprintf(`"col_%d":`, i))
		if v == "NULL" {
			sb.WriteString("null")
		} else {
			sb.WriteByte('"')
			sb.WriteString(jsonutil.EscapeString(v))
			sb.WriteByte('"')
		}
	}
	sb.WriteByte('}')
	return sb.String()
}
