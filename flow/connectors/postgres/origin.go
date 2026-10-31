package connpostgres

import (
	"context"
	"log/slog"
)

func (c *PostgresConnector) OriginPrefix(ctx context.Context) (string, error) {
	var prefix string
	err := c.conn.QueryRow(ctx, `SELECT format('peerdb_%s_%s_', system_identifier, d.oid)
		FROM pg_control_system(), pg_database d WHERE d.datname = current_database()`).Scan(&prefix)
	return prefix, err
}

func (c *PostgresConnector) applyOrigin(ctx context.Context, origin string) func() {
	if origin == "" {
		return func() {}
	}
	for _, q := range []string{
		"SELECT pg_replication_origin_create($1) WHERE pg_replication_origin_oid($1) IS NULL",
		"SELECT pg_replication_origin_session_setup($1)",
	} {
		if _, err := c.conn.Exec(ctx, q, origin); err != nil {
			c.logger.Warn("normalizing without replication origin", slog.String("origin", origin), slog.Any("error", err))
			return func() {}
		}
	}
	return func() {
		if _, err := c.conn.Exec(context.WithoutCancel(ctx), "SELECT pg_replication_origin_session_reset()"); err != nil {
			c.logger.Warn("failed to reset replication origin", slog.String("origin", origin), slog.Any("error", err))
		}
	}
}

func (c *PostgresConnector) dropOrigins(ctx context.Context, flowJobName string) {
	if _, err := c.conn.Exec(ctx, `SELECT pg_replication_origin_drop(roname) FROM pg_replication_origin
		WHERE substring(roname from '^peerdb_[0-9]+_[0-9]+_(.*)$') = $1`, flowJobName); err != nil {
		c.logger.Warn("failed to drop replication origins", slog.String("flow", flowJobName), slog.Any("error", err))
	}
}
