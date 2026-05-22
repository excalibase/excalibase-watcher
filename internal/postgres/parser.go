package postgres

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"strings"

	"github.com/excalibase/watcher-go/internal/cdc"
	"github.com/excalibase/watcher-go/internal/jsonutil"
	"github.com/excalibase/watcher-go/internal/schema"
)

// PostgreSQL epoch: 2000-01-01 00:00:00 UTC in Unix epoch millis
const pgEpochOffsetMillis = 946684800000

const ddlLogTable = "_cdc_ddl_log"

const unknownRelationMsg = "unknown relation ID"

// Postgres type OIDs for numeric types (emit as JSON numbers)
var numericOIDs = map[int]struct{}{
	20:   {}, // INT8 (bigint)
	21:   {}, // INT2 (smallint)
	23:   {}, // INT4 (integer)
	26:   {}, // OID
	700:  {}, // FLOAT4 (real)
	701:  {}, // FLOAT8 (double precision)
	1700: {}, // NUMERIC (decimal)
}

const boolOID = 16

// JSON/JSONB OIDs — emit as raw JSON, not a quoted string.
// PostgreSQL guarantees the text representation is valid JSON.
var jsonOIDs = map[int]struct{}{
	114:  {}, // json
	3802: {}, // jsonb
}

func isJSONOID(oid int) bool {
	_, ok := jsonOIDs[oid]
	return ok
}

type RelationInfo struct {
	Namespace string
	Name      string
	Columns   []string
	TypeOIDs  []int
}

type Parser struct {
	tableFilter  map[string]struct{} // nil = all tables
	captureDDL   bool
	schemaStore  schema.HistoryStore // nullable
	relationMap  map[uint32]*RelationInfo
	eventHandler func(cdc.Event) // for multi-event messages (TRUNCATE)
	lsnProvider  func() string   // optional: provides current LSN for schema history
}

func NewParser(tableFilter map[string]struct{}, captureDDL bool, schemaStore schema.HistoryStore) *Parser {
	return &Parser{
		tableFilter: tableFilter,
		captureDDL:  captureDDL,
		schemaStore: schemaStore,
		relationMap: make(map[uint32]*RelationInfo),
	}
}

func (p *Parser) SetEventHandler(h func(cdc.Event)) {
	p.eventHandler = h
}

func (p *Parser) SetLSNProvider(fn func() string) {
	p.lsnProvider = fn
}

func (p *Parser) Parse(data []byte, lsn string) *cdc.Event {
	if len(data) == 0 {
		return nil
	}

	msgType := data[0]
	buf := &reader{data: data, pos: 1}

	switch msgType {
	case 'B':
		return p.parseBegin(buf, lsn)
	case 'C':
		e := cdc.NewEvent(cdc.Commit, "", "", "", "COMMIT", lsn)
		return &e
	case 'R':
		return p.parseRelation(buf)
	case 'I':
		return p.parseInsert(buf, lsn)
	case 'U':
		return p.parseUpdate(buf, lsn)
	case 'D':
		return p.parseDelete(buf, lsn)
	case 'T':
		return p.parseTruncate(buf, lsn)
	default:
		slog.Debug("unknown pgoutput message type", "type", string(msgType))
		return nil
	}
}

func (p *Parser) parseBegin(buf *reader, lsn string) *cdc.Event {
	if buf.remaining() >= 20 {
		buf.readUint64() // finalLSN — skip
		pgMicros := buf.readUint64()
		sourceTS := pgEpochOffsetMillis + int64(pgMicros/1000)
		buf.readUint32() // xid — skip
		e := cdc.NewEventWithSourceTS(cdc.Begin, "", "", "", "BEGIN", lsn, sourceTS)
		return &e
	}
	e := cdc.NewEvent(cdc.Begin, "", "", "", "BEGIN", lsn)
	return &e
}

func (p *Parser) parseRelation(buf *reader) *cdc.Event {
	relID := buf.readUint32()
	namespace := buf.readCString()
	name := buf.readCString()
	buf.readByte() // replica identity

	numCols := buf.readUint16()
	columns := make([]string, 0, numCols)
	typeOIDs := make([]int, 0, numCols)

	for i := 0; i < int(numCols); i++ {
		buf.readByte() // column flags
		columns = append(columns, buf.readCString())
		typeOIDs = append(typeOIDs, int(buf.readUint32()))
		buf.readUint32() // type modifier
	}

	p.relationMap[relID] = &RelationInfo{
		Namespace: namespace,
		Name:      name,
		Columns:   columns,
		TypeOIDs:  typeOIDs,
	}

	if p.schemaStore != nil {
		colDefs := make([]schema.ColumnDef, len(columns))
		for i := range columns {
			colDefs[i] = schema.ColumnDef{Name: columns[i], TypeOID: typeOIDs[i]}
		}
		lsnStr := "unknown"
		if p.lsnProvider != nil {
			lsnStr = p.lsnProvider()
		}
		entry := schema.HistoryEntry{
			Position: lsnStr,
			Schema:   namespace,
			Table:    name,
			Columns:  colDefs,
		}
		if err := p.schemaStore.Save(entry); err != nil {
			slog.Warn("failed to save schema history", "table", name, "error", err)
		}
	}

	return nil // RELATION messages don't produce events
}

func (p *Parser) parseInsert(buf *reader, lsn string) *cdc.Event {
	relID := buf.readUint32()
	rel := p.relationMap[relID]
	if rel == nil {
		slog.Warn(unknownRelationMsg, "id", relID)
		return nil
	}

	// DDL log table interception
	if p.captureDDL && rel.Name == ddlLogTable {
		buf.readByte() // skip tuple type
		data := p.parseTupleData(buf, rel.Columns, rel.TypeOIDs)
		e := cdc.NewEvent(cdc.DDL, rel.Namespace, "", data, "DDL", lsn)
		return &e
	}

	if !p.passesFilter(rel.Name) {
		return nil
	}

	buf.readByte() // skip tuple type
	data := p.parseTupleData(buf, rel.Columns, rel.TypeOIDs)
	e := cdc.NewEvent(cdc.Insert, rel.Namespace, rel.Name, data, "INSERT", lsn)
	return &e
}

func (p *Parser) parseUpdate(buf *reader, lsn string) *cdc.Event {
	relID := buf.readUint32()
	rel := p.relationMap[relID]
	if rel == nil {
		slog.Warn(unknownRelationMsg, "id", relID)
		return nil
	}
	if !p.passesFilter(rel.Name) {
		return nil
	}

	var sb strings.Builder
	sb.WriteByte('{')

	if buf.remaining() > 0 {
		tupleType := buf.readByte()
		if tupleType == 'K' || tupleType == 'O' {
			oldData := p.parseTupleData(buf, rel.Columns, rel.TypeOIDs)
			sb.WriteString(`"old":`)
			sb.WriteString(oldData)
		} else {
			buf.pos-- // put back
		}
	}

	if buf.remaining() > 0 {
		tupleType := buf.readByte()
		if tupleType == 'N' {
			newData := p.parseTupleData(buf, rel.Columns, rel.TypeOIDs)
			if sb.Len() > 1 {
				sb.WriteString(", ")
			}
			sb.WriteString(`"new":`)
			sb.WriteString(newData)
		}
	}

	sb.WriteByte('}')
	e := cdc.NewEvent(cdc.Update, rel.Namespace, rel.Name, sb.String(), "UPDATE", lsn)
	return &e
}

func (p *Parser) parseDelete(buf *reader, lsn string) *cdc.Event {
	relID := buf.readUint32()
	rel := p.relationMap[relID]
	if rel == nil {
		slog.Warn(unknownRelationMsg, "id", relID)
		return nil
	}
	if !p.passesFilter(rel.Name) {
		return nil
	}

	if buf.remaining() > 0 {
		buf.readByte() // skip tuple type
		data := p.parseTupleData(buf, rel.Columns, rel.TypeOIDs)
		e := cdc.NewEvent(cdc.Delete, rel.Namespace, rel.Name, data, "DELETE", lsn)
		return &e
	}

	e := cdc.NewEvent(cdc.Delete, rel.Namespace, rel.Name, "{}", "DELETE", lsn)
	return &e
}

func (p *Parser) parseTruncate(buf *reader, lsn string) *cdc.Event {
	numRelations := buf.readUint32()
	options := buf.readByte()

	var first *cdc.Event
	for i := 0; i < int(numRelations); i++ {
		relID := buf.readUint32()
		rel := p.relationMap[relID]
		if rel == nil {
			continue
		}
		if !p.passesFilter(rel.Name) {
			continue
		}

		var sb strings.Builder
		sb.WriteString(`{"options":`)
		sb.WriteString(strconv.Itoa(int(options)))
		sb.WriteByte('}')

		e := cdc.NewEvent(cdc.Truncate, rel.Namespace, rel.Name, sb.String(), "TRUNCATE", lsn)
		if first == nil {
			first = &e
		} else if p.eventHandler != nil {
			p.eventHandler(e)
		}
	}
	return first
}

func (p *Parser) parseTupleData(buf *reader, columns []string, typeOIDs []int) string {
	if buf.remaining() < 2 {
		return "{}"
	}

	numCols := int(buf.readUint16())
	var sb strings.Builder
	sb.WriteByte('{')

	for i := 0; i < numCols; i++ {
		if i > 0 {
			sb.WriteString(", ")
		}

		colType := buf.readByte()
		colName := "col_" + strconv.Itoa(i)
		if i < len(columns) {
			colName = columns[i]
		}
		typeOID := 0
		if i < len(typeOIDs) {
			typeOID = typeOIDs[i]
		}

		sb.WriteByte('"')
		sb.WriteString(jsonutil.EscapeString(colName))
		sb.WriteString(`":`)

		writeTupleValue(&sb, buf, colType, typeOID)
	}

	sb.WriteByte('}')
	return sb.String()
}

func writeTupleValue(sb *strings.Builder, buf *reader, colType byte, typeOID int) {
	switch colType {
	case 'n':
		sb.WriteString("null")
	case 't':
		writeTextValue(sb, buf, typeOID)
	case 'u':
		sb.WriteString(`"unchanged"`)
	default:
		sb.WriteString(`"unknown"`)
	}
}

func writeTextValue(sb *strings.Builder, buf *reader, typeOID int) {
	length := int(buf.readUint32())
	value := string(buf.readBytes(length))
	switch {
	case isNumericOID(typeOID):
		sb.WriteString(value)
	case typeOID == boolOID:
		if value == "t" {
			sb.WriteString("true")
		} else {
			sb.WriteString("false")
		}
	case isJSONOID(typeOID) && json.Valid([]byte(value)):
		sb.WriteString(value)
	default:
		sb.WriteByte('"')
		sb.WriteString(jsonutil.EscapeString(value))
		sb.WriteByte('"')
	}
}

func (p *Parser) passesFilter(tableName string) bool {
	if len(p.tableFilter) == 0 {
		return true
	}
	_, ok := p.tableFilter[tableName]
	return ok
}

func isNumericOID(oid int) bool {
	_, ok := numericOIDs[oid]
	return ok
}

// reader is a safe binary reader over a byte slice (big-endian).
// All read methods check bounds and set r.err on overflow instead of panicking.
type reader struct {
	data []byte
	pos  int
	err  error
}

var errBufferOverflow = fmt.Errorf("pgoutput: buffer overflow")

func (r *reader) remaining() int {
	return len(r.data) - r.pos
}

func (r *reader) canRead(n int) bool {
	if r.err != nil {
		return false
	}
	if r.pos+n > len(r.data) {
		r.err = errBufferOverflow
		return false
	}
	return true
}

func (r *reader) readByte() byte {
	if !r.canRead(1) {
		return 0
	}
	b := r.data[r.pos]
	r.pos++
	return b
}

func (r *reader) readBytes(n int) []byte {
	if !r.canRead(n) {
		return nil
	}
	b := r.data[r.pos : r.pos+n]
	r.pos += n
	return b
}

func (r *reader) readUint16() uint16 {
	if !r.canRead(2) {
		return 0
	}
	v := binary.BigEndian.Uint16(r.data[r.pos:])
	r.pos += 2
	return v
}

func (r *reader) readUint32() uint32 {
	if !r.canRead(4) {
		return 0
	}
	v := binary.BigEndian.Uint32(r.data[r.pos:])
	r.pos += 4
	return v
}

func (r *reader) readUint64() uint64 {
	if !r.canRead(8) {
		return 0
	}
	v := binary.BigEndian.Uint64(r.data[r.pos:])
	r.pos += 8
	return v
}

func (r *reader) readCString() string {
	start := r.pos
	for r.pos < len(r.data) && r.data[r.pos] != 0 {
		r.pos++
	}
	s := string(r.data[start:r.pos])
	if r.pos < len(r.data) {
		r.pos++ // skip null terminator
	}
	return s
}
