package activities

import (
	"context"
	"errors"
	"fmt"

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
			Tables: tables,
		})
	})
}
