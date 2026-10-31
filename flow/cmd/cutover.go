package cmd

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/PeerDB-io/peerdb/flow/connectors"
	connpostgres "github.com/PeerDB-io/peerdb/flow/connectors/postgres"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
)

type cutover struct {
	h        *FlowRequestHandler
	cfg      *protos.FlowConnectionConfigs
	src, dst *connpostgres.PostgresConnector
	res      *protos.CutoverMirrorResponse
}

func (h *FlowRequestHandler) CutoverMirror(
	ctx context.Context, req *protos.CutoverMirrorRequest,
) (*protos.CutoverMirrorResponse, APIError) {
	cfg, err := h.getFlowConfigFromCatalog(ctx, req.FlowJobName)
	if err != nil {
		return nil, NewNotFoundApiError(err)
	}
	if status, err := h.getWorkflowStatus(ctx, getWorkflowID(cfg.FlowJobName)); err != nil ||
		status != protos.FlowStatus_STATUS_RUNNING || cfg.System != protos.TypeSystem_PG {
		return nil, NewFailedPreconditionApiError(fmt.Errorf("cutover needs a RUNNING PG mirror, %s is %s %v", cfg.FlowJobName, status, err))
	}
	src, srcClose, err := connectors.GetByNameAs[*connpostgres.PostgresConnector](ctx, nil, h.pool, cfg.SourceName)
	if err != nil {
		return nil, NewFailedPreconditionApiError(err)
	}
	defer srcClose(ctx)
	dst, dstClose, err := connectors.GetByNameAs[*connpostgres.PostgresConnector](ctx, nil, h.pool, cfg.DestinationName)
	if err != nil {
		return nil, NewFailedPreconditionApiError(err)
	}
	defer dstClose(ctx)

	c := &cutover{h: h, cfg: cfg, src: src, dst: dst, res: &protos.CutoverMirrorResponse{ReverseFlowJobName: req.ReverseFlowJobName}}
	if err := c.run(ctx, req); err != nil {
		return nil, NewFailedPreconditionApiError(fmt.Errorf("cutover of %s stopped after %v: %w", cfg.FlowJobName, c.res.Steps, err))
	}
	return c.res, nil
}

func (c *cutover) step(format string, args ...any) {
	c.res.Steps = append(c.res.Steps, fmt.Sprintf(format, args...))
}

func (c *cutover) run(ctx context.Context, req *protos.CutoverMirrorRequest) error {
	var fenced bool
	if err := c.src.Conn().QueryRow(ctx, `SELECT EXISTS (SELECT FROM pg_db_role_setting s JOIN pg_database d ON d.oid = s.setdatabase
		WHERE d.datname = current_database() AND s.setrole = 0 AND 'default_transaction_read_only=on' = ANY(s.setconfig))`).Scan(&fenced); err != nil {
		return err
	} else if fenced {
		return fmt.Errorf("source %s is read-only: already cut over, or a cutover was interrupted", c.src.Config.Database)
	}
	if err := c.ensureReverse(ctx); err != nil {
		return err
	}
	if err := fence(ctx, c.dst, "on"); err != nil {
		return err
	}
	c.step("destination %s is read-only for new sessions", c.dst.Config.Database)
	if req.PrepareOnly {
		return nil
	}
	if err := c.switchover(ctx, req); err != nil {
		return errors.Join(err, c.unfenceSource(context.WithoutCancel(ctx)))
	}
	return nil
}

func (c *cutover) unfenceSource(ctx context.Context) error {
	src, srcClose, err := connectors.GetByNameAs[*connpostgres.PostgresConnector](ctx, nil, c.h.pool, c.cfg.SourceName)
	if err != nil {
		return err
	}
	defer srcClose(ctx)
	return fence(ctx, src, "default")
}

func (c *cutover) switchover(ctx context.Context, req *protos.CutoverMirrorRequest) error {
	if err := fence(ctx, c.src, "on"); err != nil {
		return err
	}
	var exited bool
	var lsn string
	if err := c.src.Conn().QueryRow(ctx, `SELECT coalesce(bool_and(pg_terminate_backend(pid, 30000)), true), pg_current_wal_lsn()::text
		FROM pg_stat_activity WHERE datname = current_database() AND backend_type = 'client backend'
		AND pid <> pg_backend_pid() AND application_name NOT LIKE 'peerdb%'`).Scan(&exited, &lsn); err != nil || !exited {
		return fmt.Errorf("source sessions did not exit: %v", err)
	}
	c.step("source %s fenced: read-only, other sessions terminated, fence LSN %s", c.src.Config.Database, lsn)

	slot := cmp.Or(c.cfg.ReplicationSlotName, connpostgres.GetDefaultSlotName(c.cfg.FlowJobName))
	if err := wait(func() (bool, error) {
		var flushed bool
		err := c.src.Conn().QueryRow(ctx, "SELECT confirmed_flush_lsn >= $2::pg_lsn FROM pg_replication_slots WHERE slot_name = $1",
			slot, lsn).Scan(&flushed)
		syncID, syncErr := c.dst.GetLastSyncBatchID(ctx, c.cfg.FlowJobName)
		normID, normErr := c.dst.GetLastNormalizeBatchID(ctx, c.cfg.FlowJobName)
		return flushed && syncID == normID, errors.Join(err, syncErr, normErr)
	}); err != nil {
		return fmt.Errorf("not drained: %w", err)
	}
	c.step("drained: slot %s confirmed past %s, every synced batch applied", slot, lsn)

	if req.Verify {
		counts, err := c.h.GetMirrorRowCounts(ctx, &protos.RowCountRequest{FlowJobName: c.cfg.FlowJobName, Checksum: true})
		if err != nil {
			return err
		}
		for _, t := range counts.TableCounts {
			if t.SourceCount != t.DestinationCount || t.SourceChecksum != t.DestinationChecksum {
				return fmt.Errorf("%s differs: %v", t.SourceTable, t)
			}
		}
		c.step("verified %d tables: row counts and checksums match", len(counts.TableCounts))
	}
	if !req.SkipSequences {
		if res, err := c.h.ResetMirrorSequences(ctx, &protos.ResetMirrorSequencesRequest{FlowJobName: c.cfg.FlowJobName}); err != nil {
			return err
		} else if !res.Ok {
			return errors.New(res.ErrorMessage)
		}
		c.step("sequences synced")
	}
	if err := fence(ctx, c.dst, "default"); err != nil {
		return err
	}
	c.step("destination %s is writable; reverse mirror %s carries its writes back", c.dst.Config.Database, c.res.ReverseFlowJobName)
	return nil
}

func originProblem(ctx context.Context, conn *connpostgres.PostgresConnector, origin string) error {
	var granted, readable, full bool
	if err := conn.Conn().QueryRow(ctx, `SELECT r.rolsuper OR (SELECT bool_and(has_function_privilege(f, 'execute')) FROM unnest($1::text[]) f),
		has_table_privilege('pg_replication_origin_status', 'select') FROM pg_roles r WHERE r.rolname = current_user`,
		[]string{
			"pg_replication_origin_create(text)", "pg_replication_origin_oid(text)",
			"pg_replication_origin_session_setup(text)", "pg_replication_origin_session_reset()",
		},
	).Scan(&granted, &readable); err != nil {
		return err
	}
	if !granted {
		return fmt.Errorf("%s: the role lacks EXECUTE on pg_replication_origin_create, _oid, _session_setup or _session_reset", conn.Config.Database)
	}
	if readable {
		if err := conn.Conn().QueryRow(ctx, `SELECT NOT (coalesce(pg_replication_origin_oid($1) IN (SELECT local_id FROM pg_replication_origin_status), false)
			OR coalesce(current_setting('max_active_replication_origins', true), current_setting('max_replication_slots'))::int
				> (SELECT count(*) FROM pg_replication_origin_status))`, origin).Scan(&full); err != nil {
			return err
		}
		if full {
			return fmt.Errorf("%s: no free replication state slot, raise max_active_replication_origins (max_replication_slots before PG18)", conn.Config.Database)
		}
	}
	return nil
}

func fence(ctx context.Context, conn *connpostgres.PostgresConnector, value string) error {
	_, err := conn.Conn().Exec(ctx, fmt.Sprintf("ALTER DATABASE %s SET default_transaction_read_only = %s",
		common.QuoteIdentifier(conn.Config.Database), value))
	return err
}

func wait(done func() (bool, error)) error {
	for range 600 {
		if ok, err := done(); ok || err != nil {
			return err
		}
		time.Sleep(time.Second)
	}
	return errors.New("timed out after 10 minutes")
}

func (c *cutover) ensureReverse(ctx context.Context) error {
	if err := c.h.pool.QueryRow(ctx, `SELECT coalesce(nullif($3, ''), (SELECT f.name FROM flows f JOIN peers s ON s.id = f.source_peer
		JOIN peers d ON d.id = f.destination_peer WHERE s.name = $1 AND d.name = $2 ORDER BY f.id DESC LIMIT 1), $4)`,
		c.cfg.DestinationName, c.cfg.SourceName, c.res.ReverseFlowJobName, c.cfg.FlowJobName+"_reverse",
	).Scan(&c.res.ReverseFlowJobName); err != nil {
		return err
	}
	srcPrefix, err := c.src.OriginPrefix(ctx)
	if err != nil {
		return err
	}
	dstPrefix, err := c.dst.OriginPrefix(ctx)
	if err != nil {
		return err
	}
	if err := errors.Join(
		originProblem(ctx, c.dst, srcPrefix+c.cfg.FlowJobName),
		originProblem(ctx, c.src, dstPrefix+c.res.ReverseFlowJobName),
	); err != nil {
		return fmt.Errorf("both writers must be able to use replication origins: %w", err)
	}

	rev := proto.CloneOf(c.cfg)
	rev.FlowJobName = c.res.ReverseFlowJobName
	rev.SourceName, rev.DestinationName = c.cfg.DestinationName, c.cfg.SourceName
	rev.DoInitialSnapshot, rev.InitialSnapshotOnly, rev.Resync = false, false, false
	rev.PublicationName, rev.ReplicationSlotName = "", ""
	rev.Env = maps.Clone(c.cfg.Env)
	if rev.Env == nil {
		rev.Env = map[string]string{}
	}
	rev.Env["PEERDB_PG_AUTOMATED_SCHEMA_DUMP"] = "false"
	for _, tm := range rev.TableMappings {
		tm.SourceTableIdentifier, tm.DestinationTableIdentifier = tm.DestinationTableIdentifier, tm.SourceTableIdentifier
		tm.PartitionKey = ""
	}
	if _, err := c.h.CreateCDCFlow(ctx, &protos.CreateCDCFlowRequest{ConnectionConfigs: rev, AttachToExisting: true}); err != nil {
		return err
	}
	slot := connpostgres.GetDefaultSlotName(rev.FlowJobName)
	if err := wait(func() (bool, error) {
		var exists bool
		status, err := c.h.getWorkflowStatus(ctx, getWorkflowID(rev.FlowJobName))
		if err == nil {
			err = c.dst.Conn().QueryRow(ctx, "SELECT EXISTS(SELECT FROM pg_replication_slots WHERE slot_name = $1)", slot).Scan(&exists)
		}
		return exists && status == protos.FlowStatus_STATUS_RUNNING, err
	}); err != nil {
		return fmt.Errorf("reverse mirror %s not RUNNING with its slot: %w", rev.FlowJobName, err)
	}
	c.step("reverse mirror %s is RUNNING with slot %s on %s", rev.FlowJobName, slot, c.dst.Config.Database)
	return nil
}
