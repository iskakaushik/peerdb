package connpostgres

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"slices"

	"github.com/jackc/pgx/v5"

	"github.com/PeerDB-io/peerdb/flow/pkg/common"
)

const entryInfoSQL = `
select e.id, a.type, a.object_names, a.object_args, array(select rolname from pg_policy p
  join pg_roles r on r.oid = any(p.polroles) where p.oid = e.obj and e.cls = 'pg_policy'::regclass)
from unnest($1::int[], $2::oid[], $3::oid[]) e(id, cls, obj), pg_identify_object_as_address(e.cls, e.obj, 0) a`

const stepTablesSQL = `
select n, coalesce(to_regclass(n)::oid, 0), array(select relid::oid from pg_partition_tree(to_regclass(n)) where level > 0)
from unnest($1::text[]) n`

const deferredConsSQL = `
select co.oid, co.conrelid, co.conrelid::regclass::text, co.conname, pg_get_constraintdef(co.oid),
  co.contype = 'f', co.convalidated, c.relkind = 'p', a.type, a.object_names, a.object_args
from pg_constraint co join pg_class c on c.oid = co.conrelid,
  pg_identify_object_as_address('pg_constraint'::regclass, co.oid, 0) a
where co.oid = any($1::oid[]) or (co.contype = 'c' and co.conislocal and co.conrelid = any($2::oid[]) and exists (
  select from pg_depend d join pg_proc p on p.oid = d.refobjid
  where d.classid = 'pg_constraint'::regclass and d.objid = co.oid and d.refclassid = 'pg_proc'::regclass
    and p.pronamespace <> 'pg_catalog'::regnamespace))`

const tableDependentsSQL = `
select 'view ' || r.ev_class::regclass from pg_depend d join pg_rewrite r on r.oid = d.objid
where d.classid = 'pg_rewrite'::regclass and d.refobjid = to_regclass($1) and r.ev_class <> d.refobjid
union select 'foreign key ' || conname || ' on ' || conrelid::regclass from pg_constraint
where contype = 'f' and to_regclass($1) in (conrelid, confrelid)
union select 'index ' || indexrelid::regclass from pg_index where indrelid = to_regclass($1)
union select 'trigger ' || tgname from pg_trigger where tgrelid = to_regclass($1) and not tgisinternal
union select 'comment' from pg_description where classoid = 'pg_class'::regclass and objoid = to_regclass($1) and objsubid = 0`

const existsFnSQL = `
create or replace function pg_temp.peerdb_exists(t text, n text[], a text[]) returns bool language plpgsql as $f$
declare o oid;
begin
  select objid into o from pg_get_object_address(case t when 'row security' then 'table' else t end, n, a);
  return coalesce(o, 0) <> 0 and (t <> 'row security' or (select relrowsecurity from pg_class where oid = o));
exception when others then return false;
end $f$`

const existsSQL = `
select ord from jsonb_array_elements($1::jsonb) with ordinality u(a, ord) where pg_temp.peerdb_exists(a->>'t',
  array(select jsonb_array_elements_text(a->'n')), array(select jsonb_array_elements_text(a->'a')))`

type deferredCon struct {
	rel, name, def string
	addr           objAddr
	entry          int
	relOid         uint32
	fk             bool
	valid          bool
	parted         bool
}

func inCatalog(ctx context.Context, conn *pgx.Conn, fn func(pgx.Tx) error) error {
	return pgx.BeginTxFunc(ctx, conn, pgx.TxOptions{AccessMode: pgx.ReadOnly}, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, "set local search_path = pg_catalog"); err != nil {
			return err
		}
		return fn(tx)
	})
}

func (c *PostgresConnector) TableDependents(ctx context.Context, name string) (deps []string, err error) {
	table, err := common.ParseTableIdentifier(name)
	if err != nil {
		return nil, err
	}
	err = inCatalog(ctx, c.conn, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, tableDependentsSQL, table.String())
		if err != nil {
			return err
		}
		deps, err = pgx.CollectRows(rows, pgx.RowTo[string])
		return err
	})
	return deps, err
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
	var roles []string
	if _, err := pgx.ForEachRow(rows, []any{&id, &a.Type, &a.Names, &a.Args, &roles}, func() error {
		oi := objInfo{addr: a}
		for _, r := range roles {
			oi.roles = append(oi.roles, objAddr{Type: "role", Names: []string{r}})
		}
		info[id], a = oi, objAddr{}
		return nil
	}); err != nil {
		return nil, fmt.Errorf("failed to resolve archive entries: %w", err)
	}
	for _, e := range entries {
		if e.is("ROW SECURITY") && len(e.deps) == 1 {
			info[e.id] = objInfo{addr: objAddr{Type: "row security", Names: info[e.deps[0]].addr.Names}}
		}
	}
	return info, nil
}

func loadDeferred(ctx context.Context, tx pgx.Tx, in planInput) ([]deferredCon, error) {
	entryOf := make(map[uint32]int)
	for _, e := range in.entries {
		if e.is("FK CONSTRAINT") {
			entryOf[e.oid] = e.id
		}
	}
	rows, err := tx.Query(ctx, deferredConsSQL, slices.Collect(maps.Keys(entryOf)), slices.Collect(maps.Keys(in.tables)))
	if err != nil {
		return nil, fmt.Errorf("failed to read deferred constraints: %w", err)
	}
	var cons []deferredCon
	var o uint32
	var c deferredCon
	_, err = pgx.ForEachRow(rows, []any{
		&o, &c.relOid, &c.rel, &c.name, &c.def, &c.fk, &c.valid, &c.parted,
		&c.addr.Type, &c.addr.Names, &c.addr.Args,
	}, func() error {
		c.entry = entryOf[o]
		cons, c = append(cons, c), deferredCon{}
		return nil
	})
	return cons, err
}

func existingOnDestination(ctx context.Context, conn *pgx.Conn, in planInput, cons []deferredCon) (map[string]bool, error) {
	var addrs []objAddr
	for _, i := range in.info {
		addrs = append(append(addrs, i.addr), i.roles...)
	}
	for i := range cons {
		addrs = append(addrs, cons[i].addr)
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
