package connpostgres

import (
	"log/slog"
	"testing"

	"github.com/jackc/pglogrepl"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/log"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model"
)

func TestProcessTruncateMessage(t *testing.T) {
	t.Parallel()
	pg := &protos.TableSchema{System: protos.TypeSystem_PG}
	p := &PostgresCDCSource{
		PostgresConnector:            &PostgresConnector{logger: log.NewStructuredLogger(slog.Default())},
		srcTableIDNameMapping:        map[uint32]string{1: "s.plain", 2: "s.root", 3: "s.parent", 4: "s.q"},
		tableNameMapping:             map[string]model.SourceTableMapping{"s.plain": {Name: "d.plain"}, "s.root": {Name: "d.root"}, "s.parent": {Name: "d.parent"}, "s.q": {Name: "d.q"}},
		tableNameSchemaMapping:       map[string]*protos.TableSchema{"d.plain": pg, "d.root": pg, "d.parent": pg, "d.q": {System: protos.TypeSystem_Q}},
		childToParentRelIDMapping:    map[uint32]uint32{21: 2, 22: 2, 31: 3},
		idToRelKindMap:               map[uint32]byte{2: 'p', 3: 'r'},
		hushWarnUnknownTableDetected: map[uint32]struct{}{},
	}
	truncated := func(opt uint8, ids ...uint32) *model.TruncateRecord[model.PgItems] {
		rec := processTruncateMessage[model.PgItems](p, 0, &pglogrepl.TruncateMessage{Option: opt, RelationIDs: ids})
		if rec == nil {
			return nil
		}
		return rec.(*model.TruncateRecord[model.PgItems])
	}
	r := truncated(pglogrepl.TruncateOptionCascade|pglogrepl.TruncateOptionRestartIdentity, 1, 2, 3, 31, 4)
	require.Equal(t, []string{"d.plain", "d.root", "d.parent"}, r.DestinationTableNames)
	require.True(t, r.Cascade)
	require.True(t, r.RestartIdentity)
	require.Equal(t, []string{"d.root"}, truncated(0, 21, 22).DestinationTableNames)
	require.Nil(t, truncated(0, 21))
	require.Nil(t, truncated(0, 3))
	require.Nil(t, truncated(0, 31))
	require.Nil(t, truncated(0, 4, 99))
}

func TestProcessUpdateMessageKeepsUnchangedToastOnKeyChange(t *testing.T) {
	t.Parallel()
	p := &PostgresCDCSource{
		PostgresConnector:     &PostgresConnector{logger: log.NewStructuredLogger(slog.Default())},
		srcTableIDNameMapping: map[uint32]string{1: "s.t"},
		tableNameMapping:      map[string]model.SourceTableMapping{"s.t": {Name: "d.t"}},
		relationMessageMapping: model.RelationMessageMapping{1: {RelationID: 1, Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "id", DataType: 23, Flags: 1}, {Name: "big", DataType: 25}}}},
	}
	update := func(oldType uint8, old *pglogrepl.TupleDataColumn) *model.UpdateRecord[model.PgItems] {
		rec, err := processUpdateMessage(p, 0, &pglogrepl.UpdateMessage{RelationID: 1, OldTupleType: oldType,
			OldTuple: &pglogrepl.TupleData{Columns: []*pglogrepl.TupleDataColumn{{DataType: 't', Data: []byte("1")}, old}},
			NewTuple: &pglogrepl.TupleData{Columns: []*pglogrepl.TupleDataColumn{{DataType: 't', Data: []byte("2")}, {DataType: 'u'}}},
		}, pgProcessor{}, nil, map[string]struct{}{})
		require.NoError(t, err)
		return rec.(*model.UpdateRecord[model.PgItems])
	}
	k := update(pglogrepl.UpdateMessageTupleTypeKey, &pglogrepl.TupleDataColumn{DataType: 'n'})
	require.Contains(t, k.UnchangedToastColumns, "big")
	require.NotContains(t, k.NewItems.ColToVal, "big")
	o := update(pglogrepl.UpdateMessageTupleTypeOld, &pglogrepl.TupleDataColumn{DataType: 't', Data: []byte("x")})
	require.NotContains(t, o.UnchangedToastColumns, "big")
	require.Equal(t, []byte("x"), o.NewItems.ColToVal["big"])
}
