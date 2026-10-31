package connpostgres

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"

	"github.com/jackc/pglogrepl"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils/monitoring"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model"
	pkg_pg "github.com/PeerDB-io/peerdb/flow/pkg/postgres"
)

func (p *PostgresCDCSource) schemaChange(
	ctx context.Context, rel *pglogrepl.RelationMessage, relID uint32, srcName string,
	dst model.SourceTableMapping, prev *protos.TableSchema,
) (*protos.TableSchemaDelta, error) {
	names := make([]string, 0, len(rel.Columns))
	for _, col := range rel.Columns {
		if _, ok := dst.Exclude[col.Name]; !ok {
			names = append(names, col.Name)
		}
	}
	prevNames := make([]string, 0, len(prev.Columns))
	for _, col := range prev.Columns {
		prevNames = append(prevNames, col.Name)
	}
	if p.ddlCapture && slices.Equal(names, prevNames) {
		return nil, nil
	}
	defJSON, err := p.fetchTableDef(ctx, relID)
	if err != nil {
		return nil, err
	}
	def, err := parseTableDef(defJSON)
	if err != nil {
		return nil, err
	}
	if !slices.Equal(names, publishedColumns(def, dst.Exclude)) || retyped(def, rel) {
		if def, err = p.alignTableDef(ctx, def, rel, prev); err != nil {
			return nil, err
		}
		b, err := json.Marshal(def)
		if err != nil {
			return nil, err
		}
		defJSON = string(b)
	}
	if defJSON == prev.Definition && slices.Equal(prevNames, names) {
		return nil, nil
	}
	return p.tableSchemaDelta(ctx, srcName, dst, prev, defJSON, def)
}

func (p *PostgresCDCSource) tableSchemaDelta(
	ctx context.Context, srcName string, dst model.SourceTableMapping, prev *protos.TableSchema, defJSON string, def *TableDef,
) (*protos.TableSchemaDelta, error) {
	p.customTypeMapping = nil
	typeName, err := p.typeNames(ctx, def)
	if err != nil {
		return nil, err
	}
	next, err := schemaFromDef(prev, defJSON, def, dst.Exclude, typeName)
	if err != nil {
		return nil, err
	}
	p.tableNameSchemaMapping[dst.Name] = next
	return &protos.TableSchemaDelta{
		SrcTableName: srcName, DstTableName: dst.Name, System: prev.System, NullableEnabled: prev.NullableEnabled,
		PrevSchema: prev, NewSchema: next,
	}, nil
}

func publishedColumns(def *TableDef, exclude map[string]struct{}) []string {
	names := make([]string, 0, len(def.Columns))
	for _, col := range def.Columns {
		if _, ok := exclude[col.Name]; !ok && col.Gen == "" {
			names = append(names, col.Name)
		}
	}
	return names
}

func retyped(def *TableDef, rel *pglogrepl.RelationMessage) bool {
	return slices.ContainsFunc(rel.Columns, func(rc *pglogrepl.RelationMessageColumn) bool {
		dc := def.column(rc.Name)
		return dc != nil && (dc.OID != rc.DataType || dc.TypMod != rc.TypeModifier)
	})
}

func (p *PostgresCDCSource) alignTableDef(
	ctx context.Context, def *TableDef, rel *pglogrepl.RelationMessage, prev *protos.TableSchema,
) (*TableDef, error) {
	if prev.Definition != "" {
		prevDef, err := parseTableDef(prev.Definition)
		if err != nil {
			return nil, err
		}
		def.Constraints, def.Indexes, def.Parts = prevDef.Constraints, prevDef.Indexes, prevDef.Parts
	}
	prevNums := make(map[string]int32, len(prev.Columns))
	prevHas := make(map[int32]bool, len(prev.Columns))
	for _, pc := range prev.Columns {
		prevNums[pc.Name], prevHas[pc.Attnum] = pc.Attnum, true
	}
	var fresh []ColumnDef
	for _, dc := range def.Columns {
		if !prevHas[dc.Num] && dc.Gen == "" && !slices.ContainsFunc(rel.Columns, func(rc *pglogrepl.RelationMessageColumn) bool {
			return rc.Name == dc.Name
		}) {
			fresh = append(fresh, dc)
		}
	}
	cols := make([]ColumnDef, 0, len(rel.Columns))
	for i, rc := range rel.Columns {
		col := ColumnDef{Name: rc.Name, OID: rc.DataType, TypMod: rc.TypeModifier, Num: prevNums[rc.Name]}
		if dc := def.column(rc.Name); dc != nil && dc.Gen == "" && dc.OID == rc.DataType && dc.TypMod == rc.TypeModifier {
			cols = append(cols, *dc)
			continue
		} else if dc != nil && dc.Gen == "" && col.Num == 0 {
			col.Num = dc.Num
		} else if j := slices.IndexFunc(fresh, func(f ColumnDef) bool { return f.OID == rc.DataType }); col.Num == 0 && j >= 0 {
			col = fresh[j]
			col.Name, col.TypMod = rc.Name, rc.TypeModifier
			fresh = slices.Delete(fresh, j, j+1)
		} else if col.Num == 0 {
			col.Num = -int32(i + 1)
		}
		if err := p.conn.QueryRow(ctx, "SELECT pg_catalog.format_type($1, $2)", rc.DataType, rc.TypeModifier).Scan(&col.Type); err != nil {
			return nil, err
		}
		cols = append(cols, col)
	}
	for _, col := range def.Columns {
		if col.Gen != "" {
			cols = append(cols, col)
		}
	}
	def.Columns = cols
	return def, nil
}

func (p *PostgresCDCSource) typeNames(ctx context.Context, def *TableDef) (func(uint32) (string, string, error), error) {
	custom, err := p.fetchCustomTypeMapping(ctx)
	if err != nil {
		return nil, err
	}
	resolve := func(oid uint32) uint32 {
		base, _ := pkg_pg.ResolveDataType(oid, -1, custom)
		return base
	}
	oids := make([]uint32, 0, len(def.Columns))
	for _, col := range def.Columns {
		oids = append(oids, resolve(col.OID))
	}
	schemas, err := p.GetSchemaNameOfColumnTypeByOID(ctx, oids)
	if err != nil {
		return nil, err
	}
	return func(oid uint32) (string, string, error) {
		name, err := pkg_pg.OIDToName(p.typeMap, resolve(oid), custom)
		return name, schemas[resolve(oid)], err
	}, nil
}

func processDDLMessage[Items model.Items](
	ctx context.Context, p *PostgresCDCSource, msg *pglogrepl.LogicalDecodingMessage,
) (model.Record[Items], error) {
	var obj struct {
		Table json.RawMessage `json:"table"`
	}
	if err := json.Unmarshal(msg.Content, &obj); err != nil {
		return nil, fmt.Errorf("invalid %s message: %w", ddlMessagePrefix, err)
	}
	var delta *protos.TableSchemaDelta
	if obj.Table == nil {
		for _, schema := range p.tableNameSchemaMapping {
			if schema.System == protos.TypeSystem_PG {
				p.customTypeMapping = nil
				delta = &protos.TableSchemaDelta{System: schema.System, NewSchema: &protos.TableSchema{Definition: string(msg.Content)}}
			}
			break
		}
	} else if def, err := parseTableDef(string(obj.Table)); err != nil {
		return nil, err
	} else if srcName, ok := p.srcTableIDNameMapping[def.Rel]; ok {
		dst := p.tableNameMapping[srcName]
		if prev := p.tableNameSchemaMapping[dst.Name]; prev.GetSystem() == protos.TypeSystem_PG {
			if delta, err = p.tableSchemaDelta(ctx, srcName, dst, prev, string(obj.Table), def); err != nil {
				return nil, err
			}
		}
	}
	if delta == nil {
		return nil, nil
	}
	return &model.RelationRecord[Items]{BaseRecord: p.baseRecord(msg.LSN), TableSchemaDelta: delta},
		monitoring.AuditSchemaDelta(ctx, p.catalogPool.Pool, p.flowJobName, delta)
}
