package connpostgres

import (
	"context"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/PeerDB-io/peerdb/flow/pkg/common"
)

const quiesceSQL = `WITH RECURSIVE rels AS (SELECT to_regclass(n)::oid AS oid FROM unnest($1::text[]) n
	UNION SELECT inhrelid FROM pg_inherits JOIN rels ON inhparent = rels.oid)
SELECT format('ALTER TABLE %s %s TRIGGER %I', tgrelid::regclass, CASE tgenabled WHEN 'A' THEN 'ENABLE' ELSE 'DISABLE' END, tgname)
	FROM pg_trigger JOIN rels ON tgrelid = rels.oid WHERE NOT tgisinternal AND tgenabled IN ('A', 'R')
UNION ALL SELECT format('ALTER TABLE %s %s RULE %I', ev_class::regclass, CASE ev_enabled WHEN 'A' THEN 'ENABLE' ELSE 'DISABLE' END, rulename)
	FROM pg_rewrite JOIN rels ON ev_class = rels.oid WHERE ev_enabled IN ('A', 'R')
UNION ALL SELECT format('ALTER EVENT TRIGGER %I %s', evtname, CASE evtenabled WHEN 'A' THEN 'ENABLE' ELSE 'DISABLE' END)
	FROM pg_event_trigger WHERE evtenabled IN ('A', 'R')`

func (c *PostgresConnector) quiesceReplicaTriggers(ctx context.Context, tables []string) error {
	names := make([]string, 0, len(tables))
	for _, table := range tables {
		qt, err := common.ParseTableIdentifier(table)
		if err != nil {
			return err
		}
		names = append(names, qt.String())
	}
	rows, err := c.conn.Query(ctx, quiesceSQL, names)
	if err != nil {
		return err
	}
	stmts, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil || len(stmts) == 0 {
		return err
	}
	c.logger.Warn("destination objects would fire on replicated writes, switching them to origin-only", "statements", stmts)
	_, err = c.conn.Exec(ctx, setSessionReplicaRoleSQL+";"+strings.Join(stmts, ";"))
	return err
}
