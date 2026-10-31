package connpostgres

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

const notValidSQL = `
select format('ALTER TABLE %s VALIDATE CONSTRAINT %I', conrelid::regclass, conname) from pg_constraint
where not convalidated and conname = any($2::text[]) and (conrelid = any(array(select to_regclass(n)::oid from unnest($1::text[]) n))
  or confrelid = any(array(select to_regclass(n)::oid from unnest($1::text[]) n)))`

func deferredSQL(plan schemaPlan, cons, add []deferredCon, version shared.PGVersion, dryRun bool) string {
	var b strings.Builder
	for i := range cons {
		if c := &cons[i]; !c.fk && slices.Contains(plan.created, c.relOid) {
			fmt.Fprintf(&b, "ALTER TABLE %s DROP CONSTRAINT IF EXISTS %s;\n", c.rel, common.QuoteIdentifier(c.name))
		}
	}
	if dryRun {
		for i := range add {
			fmt.Fprintf(&b, "%s;\n", add[i].addSQL(version))
		}
	}
	return b.String()
}

func pickDeferred(plan schemaPlan, cons []deferredCon, exists map[string]bool) []deferredCon {
	var add []deferredCon
	for i := range cons {
		if c := &cons[i]; (c.fk && slices.Contains(plan.fks, c.entry)) || (!c.fk && !exists[c.addr.key()]) {
			add = append(add, *c)
		}
	}
	return add
}

func (c deferredCon) addSQL(version shared.PGVersion) string {
	sql := fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s %s", c.rel, common.QuoteIdentifier(c.name), c.def)
	if c.valid && (!c.parted || version > shared.POSTGRES_17) {
		sql += " NOT VALID"
	}
	return sql
}

func addDeferred(ctx context.Context, conn *pgx.Conn, add []deferredCon, version shared.PGVersion) error {
	return pgx.BeginFunc(ctx, conn, func(tx pgx.Tx) error {
		for i := range add {
			if _, err := tx.Exec(ctx, add[i].addSQL(version)); err != nil {
				return fmt.Errorf("failed to add deferred constraint %s: %w", add[i].name, err)
			}
		}
		return nil
	})
}

func validateDeferred(ctx context.Context, conn *pgx.Conn, in planInput, cons []deferredCon) []string {
	var valid []string
	for i := range cons {
		if cons[i].valid {
			valid = append(valid, cons[i].name)
		}
	}
	rows, err := conn.Query(ctx, notValidSQL, mappedNames(in), valid)
	if err != nil {
		return []string{err.Error()}
	}
	stmts, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return []string{err.Error()}
	}
	return execAll(ctx, conn, stmts)
}

func execAll(ctx context.Context, conn *pgx.Conn, stmts []string) []string {
	var notes []string
	for _, stmt := range stmts {
		if _, err := conn.Exec(ctx, stmt); err != nil {
			notes = append(notes, fmt.Sprintf("%s: %v", stmt, err))
		}
	}
	return notes
}
