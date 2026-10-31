package connpostgres

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"

	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/proto"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

const tableDefSQL = `SELECT jsonb_build_object('rel', c.oid::int8, 'table', format('%I.%I', n.nspname, c.relname),
 'schema', quote_ident(n.nspname), 'ri', c.relreplident,
 'cols', (SELECT coalesce(jsonb_agg(jsonb_build_object('num', a.attnum, 'name', a.attname,
   'type', format_type(a.atttypid, a.atttypmod), 'oid', a.atttypid::int8, 'typmod', a.atttypmod, 'notnull', a.attnotnull,
   'default', pg_get_expr(d.adbin, d.adrelid), 'gen', a.attgenerated, 'ident', a.attidentity,
   'missing', CASE WHEN a.atthasmissing THEN (a.attmissingval::text::text[])[1] END) ORDER BY a.attnum), '[]')
  FROM pg_attribute a LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
  WHERE a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped),
 'pk', (SELECT coalesce(jsonb_agg(a.attname ORDER BY array_position(i.indkey, a.attnum)), '[]')
  FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
  WHERE i.indrelid = c.oid AND CASE WHEN c.relreplident = 'i' THEN i.indisreplident ELSE i.indisprimary END),
 'cons', (SELECT coalesce(jsonb_object_agg(k.conname, pg_get_constraintdef(k.oid)), '{}')
  FROM pg_constraint k WHERE k.conrelid = c.oid AND k.contype IN ('p', 'u', 'c', 'f', 'x')),
 'idx', (SELECT coalesce(jsonb_object_agg(i.relname, pg_get_indexdef(i.oid)), '{}')
  FROM pg_index x JOIN pg_class i ON i.oid = x.indexrelid
  WHERE x.indrelid = c.oid AND NOT EXISTS (SELECT 1 FROM pg_constraint k WHERE k.conindid = x.indexrelid AND k.conrelid = c.oid)),
 'parts', (SELECT coalesce(jsonb_agg(jsonb_build_array(format('%I.%I', pn.nspname, pc.relname), t.parentrelid::regclass::text,
   pg_get_expr(pc.relpartbound, pc.oid), pg_get_partkeydef(pc.oid)) ORDER BY t.level, pc.relname), '[]')
  FROM pg_partition_tree(c.oid) t JOIN pg_class pc ON pc.oid = t.relid JOIN pg_namespace pn ON pn.oid = pc.relnamespace
  WHERE t.level > 0))
 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.oid = $1`

type TableDef struct {
	Table       string            `json:"table"`
	Schema      string            `json:"schema"`
	RI          string            `json:"ri"`
	Columns     []ColumnDef       `json:"cols"`
	PrimaryKey  []string          `json:"pk"`
	Constraints map[string]string `json:"cons"`
	Indexes     map[string]string `json:"idx"`
	Parts       [][4]*string      `json:"parts"`
	Rel         uint32            `json:"rel"`
}

type ColumnDef struct {
	Default *string `json:"default"`
	Missing *string `json:"missing"`
	Name    string  `json:"name"`
	Type    string  `json:"type"`
	Gen     string  `json:"gen"`
	Ident   string  `json:"ident"`
	Num     int32   `json:"num"`
	OID     uint32  `json:"oid"`
	TypMod  int32   `json:"typmod"`
	NotNull bool    `json:"notnull"`
}

func parseTableDef(s string) (*TableDef, error) {
	var def TableDef
	if err := json.Unmarshal([]byte(s), &def); err != nil {
		return nil, fmt.Errorf("invalid table definition: %w", err)
	}
	return &def, nil
}

func (d *TableDef) column(name string) *ColumnDef {
	for i := range d.Columns {
		if d.Columns[i].Name == name {
			return &d.Columns[i]
		}
	}
	return nil
}

func (c *PostgresConnector) fetchTableDef(ctx context.Context, relID uint32) (string, error) {
	tx, err := c.conn.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return "", err
	}
	defer shared.RollbackTx(tx, c.logger)
	if _, err := tx.Exec(ctx, "SET LOCAL search_path = pg_catalog"); err != nil {
		return "", err
	}
	var def *string
	if err := tx.QueryRow(ctx, tableDefSQL, relID).Scan(&def); err != nil {
		return "", fmt.Errorf("failed to read definition of table %d: %w", relID, err)
	}
	if def == nil {
		return "", fmt.Errorf("table %d not found", relID)
	}
	return *def, nil
}

func schemaFromDef(
	prev *protos.TableSchema, defJSON string, def *TableDef, exclude map[string]struct{},
	typeName func(oid uint32) (string, string, error),
) (*protos.TableSchema, error) {
	next := proto.CloneOf(prev)
	next.Columns = nil
	next.Definition = defJSON
	next.TableOid = def.Rel
	next.IsReplicaIdentityFull = def.RI == "f"
	for _, col := range def.Columns {
		if _, ok := exclude[col.Name]; ok || col.Gen != "" {
			continue
		}
		name, schema, err := typeName(col.OID)
		if err != nil {
			return nil, err
		}
		next.Columns = append(next.Columns, &protos.FieldDescription{
			Name: col.Name, Type: name, TypeModifier: col.TypMod, Nullable: !col.NotNull,
			TypeSchemaName: schema, Attnum: col.Num,
		})
	}
	next.PrimaryKeyColumns = slices.Clone(def.PrimaryKey)
	if len(next.PrimaryKeyColumns) == 0 && next.IsReplicaIdentityFull {
		for _, col := range next.Columns {
			next.PrimaryKeyColumns = append(next.PrimaryKeyColumns, col.Name)
		}
	}
	return next, nil
}
