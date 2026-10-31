package cmd

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/PeerDB-io/peerdb/flow/connectors"
	connpostgres "github.com/PeerDB-io/peerdb/flow/connectors/postgres"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

func (h *FlowRequestHandler) ResetMirrorSequences(
	ctx context.Context,
	req *protos.ResetMirrorSequencesRequest,
) (*protos.ResetMirrorSequencesResponse, APIError) {
	config, err := h.getFlowConfigFromCatalog(ctx, req.FlowJobName)
	if err != nil {
		return nil, NewInternalApiError(fmt.Errorf("failed to get flow config: %w", err))
	}

	dstType, err := connectors.LoadPeerType(ctx, h.pool, config.DestinationName)
	if err != nil {
		return nil, NewInternalApiError(fmt.Errorf("failed to load destination peer type: %w", err))
	}

	if dstType != protos.DBType_POSTGRES {
		return nil, NewFailedPreconditionApiError(
			fmt.Errorf("reset sequences is only supported for PostgreSQL destinations"))
	}

	if config.System != protos.TypeSystem_PG {
		return nil, NewFailedPreconditionApiError(
			fmt.Errorf("reset sequences is only supported for mirrors using the PG type system"))
	}

	dstConn, dstClose, err := connectors.GetByNameAs[*connpostgres.PostgresConnector](ctx, nil, h.pool, config.DestinationName)
	if err != nil {
		return nil, NewInternalApiError(fmt.Errorf("failed to get destination postgres connector: %w", err))
	}
	defer dstClose(ctx)

	srcConn, srcClose, err := connectors.GetByNameAs[*connpostgres.PostgresConnector](ctx, nil, h.pool, config.SourceName)
	if err != nil {
		return nil, NewInternalApiError(fmt.Errorf("failed to get source postgres connector: %w", err))
	}
	defer srcClose(ctx)

	tables := make([]string, 0, len(config.TableMappings))
	for _, tm := range config.TableMappings {
		tables = append(tables, tm.SourceTableIdentifier)
	}
	var notes []string
	err = connpostgres.SyncSequences(ctx, srcConn, dstConn, tables)
	if err == nil && req.FinalizeSchema {
		notes, err = connpostgres.RestoreSchema(ctx, srcConn, dstConn,
			connpostgres.SchemaStep{Tables: tables, PreData: true, PostData: true, Cutover: true})
	}
	if err != nil {
		slog.ErrorContext(ctx, "failed to reset sequences", slog.Any("error", err))
		return &protos.ResetMirrorSequencesResponse{
			Ok:           false,
			ErrorMessage: fmt.Sprintf("failed to reset sequences: %v", err),
		}, nil
	}

	return &protos.ResetMirrorSequencesResponse{
		Ok:    true,
		Notes: notes,
	}, nil
}
