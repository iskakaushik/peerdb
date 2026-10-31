package activities

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/PeerDB-io/peerdb/flow/connectors"
	connpostgres "github.com/PeerDB-io/peerdb/flow/connectors/postgres"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

func (a *FlowableActivity) withPgSchema(
	ctx context.Context, srcName, dstName string, env map[string]string,
	fn func(src, dst *connpostgres.PostgresConnector) error,
) (bool, error) {
	if on, err := internal.PeerDBPGAutomatedSchemaDump(ctx, env); err != nil || !on {
		return false, err
	}
	src, srcClose, err := connectors.GetByNameAs[*connpostgres.PostgresConnector](ctx, env, a.CatalogPool, srcName)
	if err != nil {
		return false, err
	}
	defer srcClose(ctx)
	dst, dstClose, err := connectors.GetByNameAs[*connpostgres.PostgresConnector](ctx, env, a.CatalogPool, dstName)
	if err != nil {
		return false, err
	}
	defer dstClose(ctx)
	if !connpostgres.SchemaRestorable(src.Config) || !connpostgres.SchemaRestorable(dst.Config) {
		internal.LoggerFromCtx(ctx).Info("skipping pg_dump schema migration: SSH tunnel or non-password auth")
		return false, nil
	}
	return true, fn(src, dst)
}

func (a *FlowableActivity) restoreSchema(
	ctx context.Context, flowName string, src, dst *connpostgres.PostgresConnector, step connpostgres.SchemaStep,
) error {
	notes, err := connpostgres.RestoreSchema(ctx, src, dst, step)
	for _, n := range notes {
		a.Alerter.LogFlowWarning(ctx, flowName, errors.New(n))
	}
	if err != nil {
		return a.Alerter.LogFlowError(ctx, flowName, fmt.Errorf("pg_dump schema migration failed: %w", err))
	}
	return nil
}

func (a *FlowableActivity) RunPgDumpSchema(ctx context.Context, input *protos.RunPgDumpSchemaInput) (bool, error) {
	shutdown := common.HeartbeatRoutine(ctx, func() string { return "restoring schema" })
	defer shutdown()
	ctx = context.WithValue(ctx, shared.FlowNameKey, input.FlowName)
	tables := make([]string, 0, len(input.TableMappings))
	for _, tm := range input.TableMappings {
		tables = append(tables, tm.SourceTableIdentifier)
	}
	return a.withPgSchema(ctx, input.SourceName, input.DestinationName, input.Env, func(src, dst *connpostgres.PostgresConnector) error {
		return a.restoreSchema(ctx, input.FlowName, src, dst, connpostgres.SchemaStep{
			Tables:   tables,
			PreData:  input.Section != protos.PgSchemaSection_PG_SCHEMA_SECTION_POST_DATA,
			PostData: input.Section != protos.PgSchemaSection_PG_SCHEMA_SECTION_PRE_DATA,
			Resync:   input.Resync,
		})
	})
}

func (a *FlowableActivity) renameTablesWithSchema(ctx context.Context, config *protos.RenameTablesInput) (bool, error) {
	if dbType, err := connectors.LoadPeerType(ctx, a.CatalogPool, config.PeerName); err != nil || dbType != protos.DBType_POSTGRES {
		return false, err
	}
	cfg, err := internal.FetchConfigFromDB(ctx, a.CatalogPool, config.FlowJobName)
	if err != nil || cfg.System != protos.TypeSystem_PG {
		return false, err
	}
	ran, err := a.withPgSchema(ctx, cfg.SourceName, config.PeerName, cfg.Env, func(src, dst *connpostgres.PostgresConnector) error {
		schemas := make(map[string]*protos.TableSchema, len(config.RenameTableOptions))
		tables := make([]string, 0, len(config.RenameTableOptions))
		for _, option := range config.RenameTableOptions {
			schema, err := internal.LoadTableSchemaFromCatalog(ctx, a.CatalogPool, config.FlowJobName, option.CurrentName)
			if err != nil {
				return err
			}
			schemas[option.CurrentName] = schema
			tables = append(tables, option.NewName)
		}
		for _, table := range tables {
			if err := checkDependents(ctx, src, dst, table); err != nil {
				return err
			}
		}
		if _, err := dst.RenameTablesCascade(ctx, config, schemas); err != nil {
			return err
		}
		return a.restoreSchema(ctx, config.FlowJobName, src, dst, connpostgres.SchemaStep{Tables: tables, PreData: true, PostData: true})
	})
	if !ran || err != nil {
		return ran, err
	}
	return true, a.updateTableSchemaMappingForResync(ctx, config.RenameTableOptions, config.FlowJobName)
}

func checkDependents(ctx context.Context, src, dst *connpostgres.PostgresConnector, table string) error {
	want, err := src.TableDependents(ctx, table)
	if err != nil {
		return err
	}
	have, err := dst.TableDependents(ctx, table)
	if err != nil {
		return err
	}
	if extra := slices.DeleteFunc(have, func(d string) bool { return slices.Contains(want, d) }); len(extra) > 0 {
		return fmt.Errorf("resync would drop objects that only exist on the destination: %s: %s; drop them first", table, strings.Join(extra, ", "))
	}
	return nil
}
