package connpostgres

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/PeerDB-io/peerdb/flow/internal"
)

const (
	ddlMessagePrefix = "peerdb_ddl"
	typeDefSQL       = `SELECT jsonb_build_object('name', format('%I.%I', n.nspname, y.typname), 'kind', y.typtype,
 'labels', (SELECT jsonb_agg(enumlabel ORDER BY enumsortorder) FROM pg_enum WHERE enumtypid = y.oid),
 'base', CASE WHEN y.typtype = 'd' THEN format_type(y.typbasetype, y.typtypmod) END,
 'checks', (SELECT coalesce(jsonb_object_agg(conname, pg_get_constraintdef(oid)), '{}') FROM pg_constraint WHERE contypid = y.oid),
 'attrs', (SELECT jsonb_agg(format('%I %s', attname, format_type(atttypid, atttypmod)) ORDER BY attnum)
  FROM pg_attribute WHERE attrelid = y.typrelid AND attnum > 0 AND NOT attisdropped))
 FROM pg_type y JOIN pg_namespace n ON n.oid = y.typnamespace WHERE y.oid = $1 AND y.typtype IN ('e', 'd', 'c')`
	ddlCaptureSQL = `CREATE SCHEMA IF NOT EXISTS peerdb_ddl;
CREATE OR REPLACE FUNCTION peerdb_ddl.table_def(oid) RETURNS jsonb LANGUAGE sql STABLE SET search_path = pg_catalog AS $peerdb$%[1]s$peerdb$;
CREATE OR REPLACE FUNCTION peerdb_ddl.type_def(oid) RETURNS jsonb LANGUAGE sql STABLE SET search_path = pg_catalog AS $peerdb$%[2]s$peerdb$;
CREATE OR REPLACE FUNCTION peerdb_ddl.on_end() RETURNS event_trigger LANGUAGE plpgsql SET search_path = pg_catalog AS $peerdb$
DECLARE r record;
BEGIN
 FOR r IN SELECT * FROM (SELECT DISTINCT CASE WHEN o.relkind IN ('r', 'p', 'i', 'I') THEN 't'
   WHEN o.relkind IS NOT NULL THEN o.relkind::text ELSE d.classid::regclass::text END k,
   CASE WHEN o.relkind IN ('r', 'p', 'i', 'I') THEN coalesce(pg_partition_root(o.tab), o.tab) ELSE d.objid END id
  FROM pg_event_trigger_ddl_commands() d
  LEFT JOIN LATERAL (SELECT c.relkind, coalesce(x.indrelid, c.oid) tab FROM pg_class c LEFT JOIN pg_index x ON x.indexrelid = c.oid
   WHERE d.classid = 'pg_class'::regclass AND c.oid = d.objid) o ON true
  WHERE d.schema_name IS DISTINCT FROM 'peerdb_ddl' AND d.schema_name IS DISTINCT FROM '_peerdb_internal'
   AND coalesce(d.schema_name, '') NOT LIKE 'pg\_temp%%' AND NOT d.in_extension) q ORDER BY k = 't', k, id
 LOOP
  PERFORM pg_logical_emit_message(true, 'peerdb_ddl', m::text) FROM (SELECT CASE r.k
   WHEN 't' THEN jsonb_build_object('table', peerdb_ddl.table_def(r.id))
   WHEN 'pg_type' THEN jsonb_build_object('type', peerdb_ddl.type_def(r.id))
   WHEN 'pg_extension' THEN (SELECT jsonb_build_object('sql', format('CREATE EXTENSION IF NOT EXISTS %%I WITH SCHEMA %%I', extname, nspname))
    FROM pg_extension e JOIN pg_namespace n ON n.oid = e.extnamespace WHERE e.oid = r.id)
   WHEN 'pg_proc' THEN (SELECT jsonb_build_object('sql', pg_get_functiondef(r.id)) FROM pg_proc WHERE oid = r.id AND prokind IN ('f', 'p'))
   WHEN 'pg_trigger' THEN (SELECT jsonb_build_object('sql', replace(pg_get_triggerdef(r.id), 'CREATE TRIGGER', 'CREATE OR REPLACE TRIGGER'))
    FROM pg_trigger WHERE oid = r.id AND NOT tgisinternal)
   WHEN 'v' THEN jsonb_build_object('sql', format('CREATE OR REPLACE VIEW %%s AS %%s', r.id::regclass, pg_get_viewdef(r.id)))
   WHEN 'S' THEN (SELECT jsonb_build_object('sql', format('CREATE SEQUENCE IF NOT EXISTS %%1$s; ALTER SEQUENCE %%1$s AS %%2$s '
    'INCREMENT %%3$s MINVALUE %%4$s MAXVALUE %%5$s CACHE %%6$s %%7$s CYCLE', r.id::regclass, seqtypid::regtype, seqincrement,
    seqmin, seqmax, seqcache, CASE WHEN seqcycle THEN '' ELSE 'NO' END)) FROM pg_sequence WHERE seqrelid = r.id
    AND NOT EXISTS (SELECT FROM pg_depend WHERE classid = 'pg_class'::regclass AND objid = r.id AND deptype = 'i'))
   END m) s WHERE m IS NOT NULL;
 END LOOP;
END $peerdb$;
CREATE OR REPLACE FUNCTION peerdb_ddl.on_drop() RETURNS event_trigger LANGUAGE plpgsql SET search_path = pg_catalog AS $peerdb$
BEGIN
 PERFORM pg_logical_emit_message(true, 'peerdb_ddl', jsonb_build_object('sql', CASE WHEN object_type = 'table'
   THEN format('DO $d$ BEGIN IF EXISTS (SELECT FROM pg_inherits WHERE inhrelid = to_regclass(%%L)) THEN DROP TABLE %%s; END IF; END $d$',
    object_identity, object_identity)
   ELSE format('DROP %%s IF EXISTS %%s CASCADE', upper(object_type), object_identity) END)::text)
  FROM pg_event_trigger_dropped_objects()
  WHERE original AND NOT is_temporary AND object_type IN ('table', 'index', 'view', 'materialized view', 'function', 'procedure',
   'type', 'domain', 'trigger', 'sequence')
   AND schema_name IS DISTINCT FROM 'peerdb_ddl' AND schema_name IS DISTINCT FROM '_peerdb_internal';
END $peerdb$;
DO $peerdb$ BEGIN
 IF NOT EXISTS (SELECT FROM pg_event_trigger WHERE evtname = 'peerdb_ddl_end') THEN
  CREATE EVENT TRIGGER peerdb_ddl_end ON ddl_command_end EXECUTE FUNCTION peerdb_ddl.on_end();
 END IF;
 IF NOT EXISTS (SELECT FROM pg_event_trigger WHERE evtname = 'peerdb_ddl_drop') THEN
  CREATE EVENT TRIGGER peerdb_ddl_drop ON sql_drop EXECUTE FUNCTION peerdb_ddl.on_drop();
 END IF;
END $peerdb$`
)

func (c *PostgresConnector) installDDLCapture(ctx context.Context, env map[string]string) {
	if enabled, err := internal.PeerDBPostgresDDLCapture(ctx, env); err != nil || !enabled {
		return
	}
	if _, err := c.conn.Exec(ctx, fmt.Sprintf(ddlCaptureSQL, tableDefSQL, typeDefSQL)); err != nil {
		c.logger.Warn("failed to install DDL capture, schema changes follow RelationMessages only", slog.Any("error", err))
	}
}
