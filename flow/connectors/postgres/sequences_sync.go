package connpostgres

import (
	"context"
	"fmt"
	"maps"
	"slices"

	"github.com/jackc/pgx/v5"
)

const sequencesSQL = `
select s.seqrelid::regclass::text, format('ALTER SEQUENCE %s AS %s INCREMENT BY %s MINVALUE %s MAXVALUE %s START WITH %s CACHE %s %s',
  s.seqrelid::regclass, format_type(s.seqtypid, null), s.seqincrement, s.seqmin, s.seqmax, s.seqstart, s.seqcache,
  case when s.seqcycle then 'CYCLE' else 'NO CYCLE' end), coalesce(pg_sequence_last_value(s.seqrelid), s.seqstart),
  pg_sequence_last_value(s.seqrelid) is not null
from pg_sequence s join pg_class c on c.oid = s.seqrelid join pg_namespace n on n.oid = c.relnamespace
where n.nspname <> '_peerdb_internal' and not exists (select from pg_depend d
  where d.classid = 'pg_class'::regclass and d.objid = s.seqrelid
    and (d.deptype = 'e' or (d.refclassid = 'pg_class'::regclass and d.deptype in ('a','i') and d.refobjid <> all($1::oid[]))))`

type sequenceState struct {
	Name, Alter string
	Value       int64
	Called      bool
}

func SyncSequences(ctx context.Context, src, dst *PostgresConnector, tables []string) error {
	var seqs []sequenceState
	if err := inCatalog(ctx, src.conn, func(tx pgx.Tx) error {
		oids, err := stepTables(ctx, tx, tables)
		if err != nil {
			return err
		}
		rows, err := tx.Query(ctx, sequencesSQL, slices.Collect(maps.Keys(oids)))
		if err != nil {
			return err
		}
		seqs, err = pgx.CollectRows(rows, pgx.RowToStructByPos[sequenceState])
		return err
	}); err != nil {
		return fmt.Errorf("failed to read source sequences: %w", err)
	}
	return pgx.BeginFunc(ctx, dst.conn, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, "set local search_path = pg_catalog"); err != nil {
			return err
		}
		for _, s := range seqs {
			var exists bool
			if err := tx.QueryRow(ctx, "select to_regclass($1) is not null", s.Name).Scan(&exists); err != nil {
				return err
			} else if !exists {
				continue
			}
			if _, err := tx.Exec(ctx, s.Alter); err != nil {
				return fmt.Errorf("failed to alter sequence %s: %w", s.Name, err)
			}
			if _, err := tx.Exec(ctx, "select setval($1::regclass, $2, $3)", s.Name, s.Value, s.Called); err != nil {
				return fmt.Errorf("failed to set sequence %s: %w", s.Name, err)
			}
		}
		return nil
	})
}
