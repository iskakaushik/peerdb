package connpostgres

import (
	"context"
	"fmt"
	"os/exec"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
)

const localeSQL = `select format('provider %s, collate %s, ctype %s, locale %s', coalesce(to_jsonb(d)->>'datlocprovider', 'c'),
  datcollate, datctype, coalesce(to_jsonb(d)->>'datlocale', to_jsonb(d)->>'daticulocale', '')) from pg_database d
where datname = current_database()`

func PreflightSchema(ctx context.Context, src, dst *PostgresConnector, cfg *protos.FlowConnectionConfigsCore) error {
	if cfg.System != protos.TypeSystem_PG || cfg.Resync || !SchemaRestorable(src.Config) || !SchemaRestorable(dst.Config) {
		return nil
	}
	if on, err := internal.PeerDBPGAutomatedSchemaDump(ctx, cfg.Env); err != nil || !on {
		return err
	}
	var srcLocale, dstLocale string
	if err := src.conn.QueryRow(ctx, localeSQL).Scan(&srcLocale); err != nil {
		return err
	}
	if err := dst.conn.QueryRow(ctx, localeSQL).Scan(&dstLocale); err != nil {
		return err
	}
	if srcLocale != dstLocale {
		return fmt.Errorf("destination database has %s, source has %s: create it with the same locale settings", dstLocale, srcLocale)
	}
	tables := make([]string, 0, len(cfg.TableMappings))
	for _, tm := range cfg.TableMappings {
		tables = append(tables, tm.SourceTableIdentifier)
	}
	if _, err := exec.LookPath("pg_dump"); err != nil {
		return nil
	}
	_, err := RestoreSchema(ctx, src, dst, SchemaStep{Tables: tables, PreData: true, PostData: true, DryRun: true})
	return err
}
