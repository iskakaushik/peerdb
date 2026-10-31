package connpostgres

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/PeerDB-io/peerdb/flow/pkg/common"
)

const entryInfoSQL = `
select e.id, a.type, a.object_names, a.object_args
from unnest($1::int[], $2::oid[], $3::oid[]) e(id, cls, obj), pg_identify_object_as_address(e.cls, e.obj, 0) a`

const stepTablesSQL = `
select n, coalesce(to_regclass(n)::oid, 0), array(select relid::oid from pg_partition_tree(to_regclass(n)) where level > 0)
from unnest($1::text[]) n`

const existsFnSQL = `
create or replace function pg_temp.peerdb_exists(t text, n text[], a text[]) returns bool language plpgsql as $f$
declare o oid;
begin
  select objid into o from pg_get_object_address(t, n, a);
  return coalesce(o, 0) <> 0;
exception when others then return false;
end $f$`

const existsSQL = `
select ord from jsonb_array_elements($1::jsonb) with ordinality u(a, ord) where pg_temp.peerdb_exists(a->>'t',
  array(select jsonb_array_elements_text(a->'n')), array(select jsonb_array_elements_text(a->'a')))`

func inCatalog(ctx context.Context, conn *pgx.Conn, fn func(pgx.Tx) error) error {
	return pgx.BeginTxFunc(ctx, conn, pgx.TxOptions{AccessMode: pgx.ReadOnly}, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, "set local search_path = pg_catalog"); err != nil {
			return err
		}
		return fn(tx)
	})
}

func stepTables(ctx context.Context, tx pgx.Tx, tables []string) (map[uint32]bool, error) {
	names := make([]string, 0, len(tables))
	for _, t := range tables {
		qt, err := common.ParseTableIdentifier(t)
		if err != nil {
			return nil, err
		}
		names = append(names, qt.String())
	}
	rows, err := tx.Query(ctx, stepTablesSQL, names)
	if err != nil {
		return nil, err
	}
	set := make(map[uint32]bool)
	var name string
	var root uint32
	var parts []uint32
	if _, err := pgx.ForEachRow(rows, []any{&name, &root, &parts}, func() error {
		if root == 0 {
			return fmt.Errorf("source table %s not found", name)
		}
		for _, o := range append(parts, root) {
			set[o] = true
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return set, nil
}

func loadEntryInfo(ctx context.Context, tx pgx.Tx, entries []tocEntry) (map[int]objInfo, error) {
	var ids []int
	var classes, oids []uint32
	for _, e := range entries {
		if e.class != 0 {
			ids, classes, oids = append(ids, e.id), append(classes, e.class), append(oids, e.oid)
		}
	}
	rows, err := tx.Query(ctx, entryInfoSQL, ids, classes, oids)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve archive entries: %w", err)
	}
	info := make(map[int]objInfo, len(ids))
	var id int
	var a objAddr
	if _, err := pgx.ForEachRow(rows, []any{&id, &a.Type, &a.Names, &a.Args}, func() error {
		info[id], a = objInfo{addr: a}, objAddr{}
		return nil
	}); err != nil {
		return nil, fmt.Errorf("failed to resolve archive entries: %w", err)
	}
	return info, nil
}

func existingOnDestination(ctx context.Context, conn *pgx.Conn, in planInput) (map[string]bool, error) {
	var addrs []objAddr
	for _, i := range in.info {
		addrs = append(addrs, i.addr)
	}
	batch, err := json.Marshal(addrs)
	if err != nil {
		return nil, err
	}
	if _, err := conn.Exec(ctx, existsFnSQL); err != nil {
		return nil, fmt.Errorf("failed to create existence check: %w", err)
	}
	exists := make(map[string]bool)
	err = inCatalog(ctx, conn, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, existsSQL, batch)
		if err != nil {
			return err
		}
		var ord int
		_, err = pgx.ForEachRow(rows, []any{&ord}, func() error {
			exists[addrs[ord-1].key()] = true
			return nil
		})
		return err
	})
	return exists, err
}
