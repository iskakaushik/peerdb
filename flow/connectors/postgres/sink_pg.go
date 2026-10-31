package connpostgres

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"strings"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"go.temporal.io/sdk/temporal"

	"github.com/PeerDB-io/peerdb/flow/connectors/postgres/sanitize"
	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/concurrency"
	"github.com/PeerDB-io/peerdb/flow/shared/exceptions"
)

type PgCopyShared struct {
	schemaLatch *concurrency.Latch[[]string]
	err         error
	recopy      bool
}

type PgCopyWriter struct {
	*io.PipeWriter
	schema *PgCopyShared
}

type PgCopyReader struct {
	*io.PipeReader
	schema *PgCopyShared
}

func NewPgCopyPipe() (PgCopyReader, PgCopyWriter) {
	read, write := io.Pipe()
	schema := PgCopyShared{schemaLatch: concurrency.NewLatch[[]string]()}
	return PgCopyReader{PipeReader: read, schema: &schema},
		PgCopyWriter{PipeWriter: write, schema: &schema}
}

func (p PgCopyWriter) SetSchema(schema []string) {
	p.schema.schemaLatch.Set(schema)
}

func (p PgCopyWriter) HandleQRepSyncError(err error) {
	p.Close(err)
}

func (p PgCopyWriter) ExecuteQueryWithTx(
	ctx context.Context,
	qe *QRepQueryExecutor,
	tx pgx.Tx,
	query string,
	args ...any,
) (int64, int64, error) {
	defer shared.RollbackTx(tx, qe.logger)

	if qe.snapshot != "" {
		if _, err := tx.Exec(ctx, "SET TRANSACTION SNAPSHOT "+utils.QuoteLiteral(qe.snapshot)); err != nil {
			qe.logger.Error("[pg_query_executor] failed to set snapshot",
				slog.Any("error", err), slog.String("query", query))
			if shared.IsSQLStateError(err, pgerrcode.UndefinedObject, pgerrcode.InvalidParameterValue) {
				return 0, 0, temporal.NewNonRetryableApplicationError("failed to set transaction snapshot",
					exceptions.ApplicationErrorTypeIrrecoverableInvalidSnapshot.String(), err)
			} else if shared.IsSQLStateErrorSubstring(err,
				pgerrcode.ObjectNotInPrerequisiteState, "could not import the requested snapshot") {
				return 0, 0, temporal.NewNonRetryableApplicationError("failed to set transaction snapshot",
					exceptions.ApplicationErrorTypeIrrecoverableCouldNotImportSnapshot.String(), err)
			}
			return 0, 0, fmt.Errorf("[pg_query_executor] failed to set snapshot: %w", err)
		}
	}

	norows, err := tx.Query(ctx, query+" limit 0", args...)
	if err != nil {
		return 0, 0, err
	}

	fieldDescriptions := slices.Clone(norows.FieldDescriptions())
	norows.Close()
	if err := norows.Err(); err != nil {
		return 0, 0, err
	}
	cols := make([]string, 0, len(fieldDescriptions))
	for _, fd := range fieldDescriptions {
		cols = append(cols, fd.Name)
	}
	if qe.snapshot != "" && len(fieldDescriptions) > 0 && fieldDescriptions[0].TableOID != 0 {
		var err error
		if query, cols, p.schema.recopy, err = snapshotColumns(ctx, tx, query, fieldDescriptions); err != nil {
			return 0, 0, err
		} else if p.schema.recopy {
			shared.RollbackTx(tx, qe.logger)
			if tx, err = qe.conn.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly}); err != nil {
				return 0, 0, err
			}
			defer shared.RollbackTx(tx, qe.logger)
			args = nil
		}
	}
	p.SetSchema(cols)

	query, err = sanitize.SanitizeSQL(query, args...)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to apply parameters to copy subquery: %w", err)
	}

	copyQuery := fmt.Sprintf("COPY (%s) TO STDOUT", query)
	qe.logger.Info("[pg_query_executor] executing copy", slog.String("query", copyQuery))
	ct, err := qe.conn.PgConn().CopyTo(ctx, p.PipeWriter, copyQuery)
	if err != nil {
		qe.logger.Info("[pg_query_executor] failed to copy",
			slog.String("copyQuery", copyQuery), slog.Any("error", err))
		return 0, 0, fmt.Errorf("[pg_query_executor] failed to copy: %w", err)
	}

	qe.logger.Info("Committing transaction")
	if err := tx.Commit(ctx); err != nil {
		qe.logger.Error("[pg_query_executor] failed to commit transaction", slog.Any("error", err))
		return 0, 0, fmt.Errorf("[pg_query_executor] failed to commit transaction: %w", err)
	}

	totalRecordsFetched := ct.RowsAffected()
	qe.logger.Info(fmt.Sprintf("[pg_query_executor] committed transaction for query '%s', rows = %d",
		query, totalRecordsFetched))
	return totalRecordsFetched, 0, nil
}

func (p PgCopyWriter) Close(err error) {
	p.PipeWriter.CloseWithError(err)
	p.schema.err = err
	p.SetSchema(nil)
}

func (p PgCopyReader) GetColumnNames() ([]string, error) {
	return p.schema.schemaLatch.Wait(), p.schema.err
}

func (p PgCopyReader) CopyInto(ctx context.Context, c *PostgresConnector, tx pgx.Tx, table pgx.Identifier) (int64, error) {
	cols, err := p.GetColumnNames()
	if err != nil {
		return 0, err
	}
	quotedCols := make([]string, 0, len(cols))
	for _, col := range cols {
		quotedCols = append(quotedCols, common.QuoteIdentifier(col))
	}
	if p.schema.recopy {
		recopy := fmt.Sprintf("LOCK TABLE %[1]s IN SHARE ROW EXCLUSIVE MODE; DELETE FROM %[1]s", table.Sanitize())
		if _, err := tx.Exec(ctx, recopy); err != nil {
			return 0, err
		}
	}
	ct, err := tx.Conn().PgConn().CopyFrom(
		ctx,
		p.PipeReader,
		fmt.Sprintf("COPY %s (%s) FROM STDIN", table.Sanitize(), strings.Join(quotedCols, ",")),
	)
	return ct.RowsAffected(), err
}

const rewrittenSQL = `SELECT coalesce(bool_or(c.relfilenode <> pg_relation_filenode(c.oid)), false), $1::oid::regclass::text,
 NOT EXISTS (SELECT FROM pg_catalog.pg_inherits WHERE inhrelid = $1::oid OR inhparent = $1::oid)
 FROM pg_catalog.pg_class c WHERE c.oid = $1::oid OR c.oid IN (SELECT relid FROM pg_catalog.pg_partition_tree($1::oid))`

func snapshotColumns(
	ctx context.Context, tx pgx.Tx, query string, fields []pgconn.FieldDescription,
) (string, []string, bool, error) {
	var rewritten, standalone bool
	var table string
	if err := tx.QueryRow(ctx, rewrittenSQL, fields[0].TableOID).Scan(
		&rewritten, &table, &standalone); err != nil {
		return "", nil, false, err
	} else if rewritten && !standalone {
		return "", nil, false, temporal.NewNonRetryableApplicationError("table "+table+" was rewritten during the initial load",
			exceptions.ApplicationErrorTypeIrrecoverableInvalidSnapshot.String(), nil)
	} else if !rewritten {
		table = "(" + query + ") _peerdb_snapshot"
	}
	rows, err := tx.Query(ctx, `SELECT attnum, attname FROM pg_catalog.pg_attribute
		WHERE attrelid = $1 AND attnum > 0 AND NOT attisdropped AND attgenerated = ''`, fields[0].TableOID)
	if err != nil {
		return "", nil, false, err
	}
	names := make(map[uint16]string)
	var num uint16
	var name string
	if _, err := pgx.ForEachRow(rows, []any{&num, &name}, func() error {
		names[num] = name
		return nil
	}); err != nil {
		return "", nil, false, err
	}
	sel, cols := make([]string, 0, len(fields)), make([]string, 0, len(fields))
	for _, fd := range fields {
		if name, ok := names[fd.TableAttributeNumber]; ok {
			sel, cols = append(sel, common.QuoteIdentifier(fd.Name)+" AS "+common.QuoteIdentifier(name)), append(cols, name)
		}
	}
	return "SELECT " + strings.Join(sel, ",") + " FROM " + table, cols, rewritten, nil
}
