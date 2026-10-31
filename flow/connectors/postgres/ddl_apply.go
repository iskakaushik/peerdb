package connpostgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"strings"

	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

const (
	schemaChangeTableName  = "_peerdb_schema_change"
	schemaChangeRecordType = 4
	schemaChangeSetupSQL   = `SET LOCAL session_replication_role = replica; SET LOCAL search_path = pg_catalog;
CREATE TABLE IF NOT EXISTS %[1]s.peerdb_ddl_state(mirror_job_name text, table_name text, definition jsonb NOT NULL,
 PRIMARY KEY (mirror_job_name, table_name));
CREATE TABLE IF NOT EXISTS %[1]s.peerdb_ddl_log(id bigserial PRIMARY KEY, at timestamptz NOT NULL DEFAULT now(),
 mirror_job_name text, batch_id bigint, table_name text, stmt text, error text)`
)

func (c *PostgresConnector) pendingSchemaChanges(
	ctx context.Context, rawTable string, normBatchID int64, schemas map[string]*protos.TableSchema,
) (map[string]*protos.TableSchema, map[int64][]*protos.TableSchemaDelta, error) {
	rows, err := c.conn.Query(ctx, fmt.Sprintf(`SELECT _peerdb_batch_id, _peerdb_data::text FROM %s.%s
		WHERE _peerdb_batch_id > $1 AND _peerdb_record_type = $2 ORDER BY _peerdb_batch_id DESC, _peerdb_timestamp DESC`,
		c.metadataSchema, rawTable), normBatchID, schemaChangeRecordType)
	if err != nil {
		return nil, nil, err
	}
	schemas = maps.Clone(schemas)
	pending := make(map[int64][]*protos.TableSchemaDelta)
	var batchID int64
	var data string
	if _, err := pgx.ForEachRow(rows, []any{&batchID, &data}, func() error {
		delta := &protos.TableSchemaDelta{}
		if err := protojson.Unmarshal([]byte(data), delta); err != nil {
			return err
		}
		if delta.DstTableName != "" {
			schemas[delta.DstTableName] = delta.PrevSchema
		}
		pending[batchID] = append([]*protos.TableSchemaDelta{delta}, pending[batchID]...)
		return nil
	}); err != nil {
		return nil, nil, fmt.Errorf("failed to read pending schema changes: %w", err)
	}
	return schemas, pending, nil
}

func (c *PostgresConnector) applySchemaChanges(
	ctx context.Context, req *model.NormalizeRecordsRequest, batchID int64,
	deltas []*protos.TableSchemaDelta, schemas map[string]*protos.TableSchema,
) error {
	for _, delta := range deltas {
		if err := c.applySchemaChange(ctx, req, batchID, delta); err != nil {
			return err
		}
		if delta.DstTableName != "" {
			schemas[delta.DstTableName] = delta.NewSchema
		}
	}
	return nil
}

func (c *PostgresConnector) applySchemaChange(
	ctx context.Context, req *model.NormalizeRecordsRequest, batchID int64, delta *protos.TableSchemaDelta,
) error {
	tx, err := c.conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer shared.RollbackTx(tx, c.logger)
	if _, err := tx.Exec(ctx, fmt.Sprintf(schemaChangeSetupSQL, c.metadataSchema)); err != nil {
		return err
	}
	if delta.DstTableName == "" {
		if err := c.applyObjectChange(ctx, tx, req.FlowJobName, batchID, delta.NewSchema.Definition); err != nil {
			return err
		}
		return tx.Commit(ctx)
	}
	dst, err := common.ParseTableIdentifier(delta.DstTableName)
	if err != nil {
		return err
	}
	var relID *uint32
	var state *string
	if err := tx.QueryRow(ctx, fmt.Sprintf(`SELECT to_regclass($1)::oid, (SELECT definition::text FROM %s.peerdb_ddl_state
		WHERE mirror_job_name = $2 AND table_name = $3)`, c.metadataSchema), dst.String(), req.FlowJobName, delta.DstTableName,
	).Scan(&relID, &state); err != nil {
		return err
	}
	if relID == nil {
		c.logger.Warn("schema change for a table missing on the destination", slog.String("table", delta.DstTableName))
		return tx.Commit(ctx)
	}
	var curJSON string
	if err := tx.QueryRow(ctx, tableDefSQL, *relID).Scan(&curJSON); err != nil {
		return err
	}
	def, err := parseTableDef(delta.NewSchema.Definition)
	if err != nil {
		return err
	}
	cur, err := parseTableDef(curJSON)
	if err != nil {
		return err
	}
	prev, err := previousTableDef(state, delta.PrevSchema)
	if err != nil {
		return err
	}
	exclude := make(map[string]struct{})
	for _, tm := range req.TableMappings {
		if tm.DestinationTableIdentifier == delta.DstTableName {
			for _, col := range tm.Exclude {
				exclude[col] = struct{}{}
			}
		}
	}
	for _, step := range planTable(def, cur, prev, exclude) {
		if err := c.execSchemaStep(ctx, tx, req.FlowJobName, batchID, delta.DstTableName, step); err != nil {
			return err
		}
	}
	if _, err := tx.Exec(ctx, fmt.Sprintf(`INSERT INTO %s.peerdb_ddl_state VALUES ($1, $2, $3)
		ON CONFLICT (mirror_job_name, table_name) DO UPDATE SET definition = EXCLUDED.definition`, c.metadataSchema),
		req.FlowJobName, delta.DstTableName, delta.NewSchema.Definition); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func previousTableDef(state *string, prev *protos.TableSchema) (*TableDef, error) {
	if state != nil {
		return parseTableDef(*state)
	} else if prev.GetDefinition() != "" {
		return parseTableDef(prev.Definition)
	}
	def := &TableDef{}
	for _, col := range prev.GetColumns() {
		def.Columns = append(def.Columns, ColumnDef{Name: col.Name, Num: col.Attnum})
	}
	return def, nil
}

func (c *PostgresConnector) execSchemaStep(
	ctx context.Context, tx pgx.Tx, flowJobName string, batchID int64, table string, step ddlStep,
) error {
	stepErr := errors.New(step.Warn)
	if step.SQL != "" {
		sp, err := tx.Begin(ctx)
		if err != nil {
			return err
		}
		if _, stepErr = sp.Exec(ctx, step.SQL); stepErr != nil && step.Strict {
			return fmt.Errorf("failed to apply schema change to %s: %s: %w", table, step.SQL, stepErr)
		} else if stepErr != nil {
			err = sp.Rollback(ctx)
		} else {
			err = sp.Commit(ctx)
		}
		if err != nil {
			return err
		}
	}
	var errText *string
	if stepErr != nil {
		c.logger.Warn("schema change not applied", slog.String("table", table), slog.String("stmt", step.SQL), slog.Any("error", stepErr))
		errText = new(stepErr.Error())
	}
	_, err := tx.Exec(ctx, fmt.Sprintf(`INSERT INTO %s.peerdb_ddl_log(mirror_job_name, batch_id, table_name, stmt, error)
		VALUES ($1, $2, $3, $4, $5)`, c.metadataSchema), flowJobName, batchID, table, step.SQL, errText)
	return err
}

func (c *PostgresConnector) applyObjectChange(ctx context.Context, tx pgx.Tx, flowJobName string, batchID int64, definition string) error {
	var obj struct {
		Type *typeDef `json:"type"`
		SQL  string   `json:"sql"`
	}
	if err := json.Unmarshal([]byte(definition), &obj); err != nil {
		return err
	}
	steps := []ddlStep{{SQL: obj.SQL}}
	if obj.Type != nil {
		var curJSON *string
		curSQL := "SELECT (" + strings.Replace(typeDefSQL, "$1", "to_regtype($1)", 1) + ")"
		if err := tx.QueryRow(ctx, curSQL, obj.Type.Name).Scan(&curJSON); err != nil {
			return err
		}
		var cur *typeDef
		if curJSON != nil {
			if err := json.Unmarshal([]byte(*curJSON), &cur); err != nil {
				return err
			}
		}
		steps = planType(obj.Type, cur)
	}
	for _, step := range steps {
		if err := c.execSchemaStep(ctx, tx, flowJobName, batchID, "", step); err != nil {
			return err
		}
	}
	return nil
}
