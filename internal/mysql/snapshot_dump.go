package mysql

import (
	"bufio"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"

	"github.com/excalibase/watcher-go/internal/cdc"
	"github.com/excalibase/watcher-go/internal/jsonutil"
)

// CHANGE MASTER TO MASTER_LOG_FILE='mysql-bin.000003', MASTER_LOG_POS=154;
var binlogPosRegex = regexp.MustCompile(`MASTER_LOG_FILE='([^']+)',\s*MASTER_LOG_POS=(\d+)`)

// INSERT INTO `table` VALUES (...),(...);
var insertRegex = regexp.MustCompile("^INSERT INTO `([^`]+)` VALUES ")

func ParseMysqlDumpFile(filePath, schema string, handler func(cdc.Event)) (BinlogPosition, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return BinlogPosition{}, fmt.Errorf("opening dump file: %w", err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 1024*1024), 50*1024*1024) // 50MB max line

	var pos BinlogPosition

	for scanner.Scan() {
		line := scanner.Text()

		// Extract binlog position from CHANGE MASTER TO comment
		if matches := binlogPosRegex.FindStringSubmatch(line); matches != nil {
			pos.File = matches[1]
			p, _ := strconv.ParseUint(matches[2], 10, 32)
			pos.Position = uint32(p)
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

	return pos, scanner.Err()
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

