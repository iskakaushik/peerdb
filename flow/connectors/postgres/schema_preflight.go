package connpostgres

import (
	"context"
	"os/exec"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
)

func PreflightSchema(ctx context.Context, src, dst *PostgresConnector, cfg *protos.FlowConnectionConfigsCore) error {
	if cfg.System != protos.TypeSystem_PG || cfg.Resync || !SchemaRestorable(src.Config) || !SchemaRestorable(dst.Config) {
		return nil
	}
	if on, err := internal.PeerDBPGAutomatedSchemaDump(ctx, cfg.Env); err != nil || !on {
		return err
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
