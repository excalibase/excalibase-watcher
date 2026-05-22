//go:build e2e

package e2e

import (
	"strings"
	"testing"
	"time"

	"github.com/excalibase/watcher-go/internal/cdc"
)

func mysqlSubject(stream, table string) string {
	prefix := "m_" + strings.ToLower(stream)
	return prefix + ".e2edb." + table
}

func mysqlSubjectWild(stream string) string {
	prefix := "m_" + strings.ToLower(stream)
	return prefix + ".>"
}

func TestE2E_MySQL_Insert(t *testing.T) {
mysqlSetup(t)
	stream := "MY_INS"
	deleteNATSStream(stream)

	wp := startWatcher(t,mysqlOnlyConfig(stream))
	defer wp.stop(t)

	nc := newNATSConsumer(t, stream, mysqlSubject(stream, "e2e_users"))
	defer nc.close()
	time.Sleep(3 * time.Second)

	mysqlExec(t, "INSERT INTO e2e_users (name, email, score) VALUES ('Alice', 'alice@test.com', 99.50)")

	event := nc.fetchUntilType(t, cdc.Insert, 15*time.Second)
	if event.Table != "e2e_users" {
		t.Errorf("table = %q", event.Table)
	}
	if event.Schema != "e2edb" {
		t.Errorf("schema = %q", event.Schema)
	}
	if !containsString([]cdc.Event{event}, "Alice") {
		t.Errorf("data missing Alice: %s", event.Data)
	}
}

func TestE2E_MySQL_Update(t *testing.T) {
mysqlSetup(t)
	stream := "MY_UPD"
	deleteNATSStream(stream)
	mysqlExec(t, "INSERT INTO e2e_users (name, email) VALUES ('Alice', 'alice@test.com')")

	wp := startWatcher(t,mysqlOnlyConfig(stream))
	defer wp.stop(t)

	nc := newNATSConsumer(t, stream, mysqlSubject(stream, "e2e_users"))
	defer nc.close()
	time.Sleep(3 * time.Second)

	mysqlExec(t, "UPDATE e2e_users SET name = 'Bob' WHERE name = 'Alice'")

	event := nc.fetchUntilType(t, cdc.Update, 15*time.Second)
	if event.Table != "e2e_users" {
		t.Errorf("table = %q", event.Table)
	}
	if !containsString([]cdc.Event{event}, "old") {
		t.Errorf("UPDATE missing 'old': %s", event.Data)
	}
}

func TestE2E_MySQL_Delete(t *testing.T) {
mysqlSetup(t)
	stream := "MY_DEL"
	deleteNATSStream(stream)
	mysqlExec(t, "INSERT INTO e2e_users (name) VALUES ('ToDelete')")

	wp := startWatcher(t,mysqlOnlyConfig(stream))
	defer wp.stop(t)

	nc := newNATSConsumer(t, stream, mysqlSubject(stream, "e2e_users"))
	defer nc.close()
	time.Sleep(3 * time.Second)

	mysqlExec(t, "DELETE FROM e2e_users WHERE name = 'ToDelete'")

	event := nc.fetchUntilType(t, cdc.Delete, 15*time.Second)
	if event.Table != "e2e_users" {
		t.Errorf("table = %q", event.Table)
	}
}

func TestE2E_MySQL_DDL(t *testing.T) {
mysqlSetup(t)
	stream := "MY_DDL"
	deleteNATSStream(stream)

	wp := startWatcher(t,mysqlOnlyConfig(stream))
	defer wp.stop(t)

	nc := newNATSConsumer(t, stream, mysqlSubjectWild(stream))
	defer nc.close()
	time.Sleep(3 * time.Second)

	mysqlExec(t, "ALTER TABLE e2e_users ADD COLUMN bio TEXT")

	event := nc.fetchUntilType(t, cdc.DDL, 15*time.Second)
	if event.Type != cdc.DDL {
		t.Errorf("type = %v", event.Type)
	}
}

func TestE2E_MySQL_TypeMapping(t *testing.T) {
mysqlSetup(t)
	stream := "MY_TYPE"
	deleteNATSStream(stream)

	wp := startWatcher(t,mysqlOnlyConfig(stream))
	defer wp.stop(t)

	nc := newNATSConsumer(t, stream, mysqlSubject(stream, "e2e_users"))
	defer nc.close()
	time.Sleep(3 * time.Second)

	mysqlExec(t, "INSERT INTO e2e_users (name, active, score, age) VALUES ('TypeTest', true, 123.45, 30)")

	event := nc.fetchUntilType(t, cdc.Insert, 15*time.Second)
	if !containsString([]cdc.Event{event}, "TypeTest") {
		t.Errorf("missing TypeTest: %s", event.Data)
	}
}
