package postgres

import (
	"context"
	"fmt"
	"log/slog"
	"regexp"

	"github.com/jackc/pgx/v5"
)

var identifierRegex = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

func validateIdentifier(name string) error {
	if !identifierRegex.MatchString(name) {
		return fmt.Errorf("invalid identifier: %q", name)
	}
	return nil
}

func createPublicationIfNotExists(ctx context.Context, conn *pgx.Conn, pubName string) error {
	if err := validateIdentifier(pubName); err != nil {
		return err
	}

	var exists bool
	err := conn.QueryRow(ctx,
		"SELECT EXISTS(SELECT 1 FROM pg_publication WHERE pubname = $1)", pubName).Scan(&exists)
	if err != nil {
		return fmt.Errorf("checking publication: %w", err)
	}
	if exists {
		slog.Debug("publication already exists", "name", pubName)
		return nil
	}

	_, err = conn.Exec(ctx, "CREATE PUBLICATION "+pubName+" FOR ALL TABLES")
	if err != nil {
		return fmt.Errorf("creating publication: %w", err)
	}
	slog.Debug("created publication", "name", pubName)
	return nil
}

func createReplicationSlotIfNotExists(ctx context.Context, conn *pgx.Conn, slotName string) error {
	if err := validateIdentifier(slotName); err != nil {
		return err
	}

	var exists bool
	err := conn.QueryRow(ctx,
		"SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)", slotName).Scan(&exists)
	if err != nil {
		return fmt.Errorf("checking replication slot: %w", err)
	}
	if exists {
		slog.Debug("replication slot already exists", "name", slotName)
		return nil
	}

	_, err = conn.Exec(ctx,
		"SELECT pg_create_logical_replication_slot($1, 'pgoutput')", slotName)
	if err != nil {
		return fmt.Errorf("creating replication slot: %w", err)
	}
	slog.Debug("created replication slot", "name", slotName)
	return nil
}

func createDDLTriggerIfNotExists(ctx context.Context, conn *pgx.Conn) error {
	ddl := `
	CREATE TABLE IF NOT EXISTS _cdc_ddl_log (
		id SERIAL PRIMARY KEY,
		command_tag TEXT,
		object_type TEXT,
		schema_name TEXT,
		object_identity TEXT,
		query TEXT
	);

	CREATE OR REPLACE FUNCTION _cdc_ddl_log_fn() RETURNS event_trigger LANGUAGE plpgsql AS $$
	DECLARE
		obj RECORD;
	BEGIN
		SELECT INTO obj * FROM pg_event_trigger_ddl_commands() LIMIT 1;
		IF obj IS NOT NULL THEN
			INSERT INTO _cdc_ddl_log(command_tag, object_type, schema_name, object_identity, query)
			VALUES (obj.command_tag, obj.object_type, obj.schema_name, obj.object_identity, current_query());
		END IF;
	END;
	$$;

	DO $$ BEGIN
		IF NOT EXISTS (SELECT 1 FROM pg_event_trigger WHERE evtname = 'cdc_ddl_capture') THEN
			CREATE EVENT TRIGGER cdc_ddl_capture ON ddl_command_end EXECUTE FUNCTION _cdc_ddl_log_fn();
		END IF;
	END $$;

	CREATE OR REPLACE FUNCTION _cdc_ddl_drop_fn() RETURNS event_trigger LANGUAGE plpgsql AS $$
	DECLARE
		obj RECORD;
	BEGIN
		FOR obj IN SELECT * FROM pg_event_trigger_dropped_objects() LOOP
			INSERT INTO _cdc_ddl_log(command_tag, object_type, schema_name, object_identity)
			VALUES ('DROP', obj.object_type, obj.schema_name, obj.object_identity);
		END LOOP;
	END;
	$$;

	DO $$ BEGIN
		IF NOT EXISTS (SELECT 1 FROM pg_event_trigger WHERE evtname = 'cdc_ddl_drop_capture') THEN
			CREATE EVENT TRIGGER cdc_ddl_drop_capture ON sql_drop EXECUTE FUNCTION _cdc_ddl_drop_fn();
		END IF;
	END $$;
	`

	_, err := conn.Exec(ctx, ddl)
	if err != nil {
		return fmt.Errorf("creating DDL triggers: %w", err)
	}
	slog.Debug("DDL capture triggers created")
	return nil
}
