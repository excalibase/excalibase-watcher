package postgres

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/excalibase/watcher-go/internal/cdc"
	"github.com/excalibase/watcher-go/internal/jsonutil"
	"github.com/jackc/pgx/v5"
)

func RunChunkedSnapshot(ctx context.Context, connStr, schema string, tables []string,
	chunkSize int, handler func(cdc.Event)) error {

	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		return fmt.Errorf("connecting for snapshot: %w", err)
	}
	defer conn.Close(ctx)

	if err := validateIdentifier(schema); err != nil {
		return fmt.Errorf("invalid schema: %w", err)
	}

	if len(tables) == 0 {
		tables, err = fetchAllTables(ctx, conn, schema)
		if err != nil {
			return err
		}
	}

	for _, table := range tables {
		if err := snapshotTable(ctx, conn, schema, table, chunkSize, handler); err != nil {
			return fmt.Errorf("snapshot table %s: %w", table, err)
		}
	}
	return nil
}

func snapshotTable(ctx context.Context, conn *pgx.Conn, schema, table string,
	chunkSize int, handler func(cdc.Event)) error {

	if err := validateIdentifier(table); err != nil {
		return fmt.Errorf("invalid table: %w", err)
	}

	columns, err := fetchColumnNames(ctx, conn, schema, table)
	if err != nil {
		return err
	}

	pk, err := fetchPrimaryKey(ctx, conn, schema, table)
	if err != nil {
		return err
	}

	spec := chunkSpec{schema: schema, table: table, pk: pk, columns: columns, chunkSize: chunkSize}
	if pk != "" {
		return snapshotByPK(ctx, conn, spec, handler)
	}
	return snapshotByOffset(ctx, conn, spec, handler)
}

type chunkSpec struct {
	schema, table, pk string
	columns           []string
	chunkSize         int
}

func snapshotByPK(ctx context.Context, conn *pgx.Conn, s chunkSpec, handler func(cdc.Event)) error {
	if err := validateIdentifier(s.pk); err != nil {
		return fmt.Errorf("invalid primary key: %w", err)
	}

	var lastValue interface{}
	quotedCols := make([]string, len(s.columns))
	for i, c := range s.columns {
		quotedCols[i] = `"` + c + `"`
	}
	colList := strings.Join(quotedCols, ", ")
	qSchema := `"` + s.schema + `"`
	qTable := `"` + s.table + `"`
	qPK := `"` + s.pk + `"`

	for {
		query, args := buildChunkQuery(colList, qSchema, qTable, qPK, lastValue, s.chunkSize)
		count, newLast, err := fetchChunk(ctx, conn, query, args, s, handler)
		if err != nil {
			return err
		}
		lastValue = newLast
		if count < s.chunkSize {
			break
		}
	}

	slog.Debug("snapshot complete", "table", s.table)
	return nil
}

func buildChunkQuery(colList, qSchema, qTable, qPK string, lastValue interface{}, chunkSize int) (string, []interface{}) {
	if lastValue == nil {
		return fmt.Sprintf("SELECT %s FROM %s.%s ORDER BY %s LIMIT $1",
			colList, qSchema, qTable, qPK), []interface{}{chunkSize}
	}
	return fmt.Sprintf("SELECT %s FROM %s.%s WHERE %s > $1 ORDER BY %s LIMIT $2",
		colList, qSchema, qTable, qPK, qPK), []interface{}{lastValue, chunkSize}
}

func fetchChunk(ctx context.Context, conn *pgx.Conn, query string, args []interface{},
	s chunkSpec, handler func(cdc.Event)) (int, interface{}, error) {
	rows, err := conn.Query(ctx, query, args...)
	if err != nil {
		return 0, nil, fmt.Errorf("querying chunk: %w", err)
	}
	defer rows.Close()

	count := 0
	var lastValue interface{}
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return count, lastValue, err
		}
		handler(buildSnapshotEvent(s.schema, s.table, s.columns, values))
		count++
		for i, col := range s.columns {
			if col == s.pk {
				lastValue = values[i]
				break
			}
		}
	}
	if err := rows.Err(); err != nil {
		return count, lastValue, fmt.Errorf("iterating rows: %w", err)
	}
	return count, lastValue, nil
}

func snapshotByOffset(ctx context.Context, conn *pgx.Conn, s chunkSpec, handler func(cdc.Event)) error {
	quotedCols := make([]string, len(s.columns))
	for i, c := range s.columns {
		quotedCols[i] = `"` + c + `"`
	}
	colList := strings.Join(quotedCols, ", ")
	qSchema := `"` + s.schema + `"`
	qTable := `"` + s.table + `"`
	offset := 0
	query := fmt.Sprintf("SELECT %s FROM %s.%s ORDER BY 1 LIMIT $1 OFFSET $2",
		colList, qSchema, qTable)

	for {
		count, err := fetchOffsetChunk(ctx, conn, query, s.chunkSize, offset, s, handler)
		if err != nil {
			return err
		}
		offset += count
		if count < s.chunkSize {
			break
		}
	}
	return nil
}

func fetchOffsetChunk(ctx context.Context, conn *pgx.Conn, query string, chunkSize, offset int,
	s chunkSpec, handler func(cdc.Event)) (int, error) {
	rows, err := conn.Query(ctx, query, chunkSize, offset)
	if err != nil {
		return 0, fmt.Errorf("querying chunk: %w", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return count, err
		}
		handler(buildSnapshotEvent(s.schema, s.table, s.columns, values))
		count++
	}
	if err := rows.Err(); err != nil {
		return count, fmt.Errorf("iterating rows: %w", err)
	}
	return count, nil
}

func buildSnapshotEvent(schema, table string, columns []string, values []interface{}) cdc.Event {
	var sb strings.Builder
	sb.WriteByte('{')
	for i, col := range columns {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteByte('"')
		sb.WriteString(jsonutil.EscapeString(col))
		sb.WriteString(`":`)

		if i < len(values) && values[i] != nil {
			sb.WriteByte('"')
			sb.WriteString(jsonutil.EscapeString(fmt.Sprintf("%v", values[i])))
			sb.WriteByte('"')
		} else {
			sb.WriteString("null")
		}
	}
	sb.WriteByte('}')

	return cdc.NewEvent(cdc.Insert, schema, table, sb.String(), "INSERT", "")
}

func fetchAllTables(ctx context.Context, conn *pgx.Conn, schema string) ([]string, error) {
	rows, err := conn.Query(ctx,
		"SELECT table_name FROM information_schema.tables WHERE table_schema = $1 AND table_type = 'BASE TABLE' ORDER BY table_name",
		schema)
	if err != nil {
		return nil, fmt.Errorf("fetching tables: %w", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		tables = append(tables, name)
	}
	return tables, nil
}

func fetchColumnNames(ctx context.Context, conn *pgx.Conn, schema, table string) ([]string, error) {
	rows, err := conn.Query(ctx,
		"SELECT column_name FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2 ORDER BY ordinal_position",
		schema, table)
	if err != nil {
		return nil, fmt.Errorf("fetching columns: %w", err)
	}
	defer rows.Close()

	var cols []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		cols = append(cols, name)
	}
	return cols, nil
}

func fetchPrimaryKey(ctx context.Context, conn *pgx.Conn, schema, table string) (string, error) {
	var pk string
	err := conn.QueryRow(ctx, `
		SELECT a.attname
		FROM pg_index i
		JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
		WHERE i.indrelid = ($1 || '.' || $2)::regclass AND i.indisprimary
		LIMIT 1`, schema, table).Scan(&pk)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", nil
		}
		return "", fmt.Errorf("fetching primary key: %w", err)
	}
	return pk, nil
}
