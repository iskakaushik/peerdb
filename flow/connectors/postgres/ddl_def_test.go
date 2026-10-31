package connpostgres

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

func TestSchemaFromDef(t *testing.T) {
	defJSON := `{"rel": 42, "table": "public.t", "ri": "d", "pk": ["id"], "cons": {}, "idx": {}, "parts": [],
		"cols": [{"num": 1, "name": "id", "type": "integer", "oid": 23, "typmod": -1, "notnull": true},
		{"num": 3, "name": "v", "type": "character varying(10)", "oid": 1043, "typmod": 14},
		{"num": 4, "name": "g", "type": "integer", "oid": 23, "typmod": -1, "gen": "s", "default": "(id * 2)"},
		{"num": 5, "name": "secret", "type": "text", "oid": 25, "typmod": -1}]}`
	def, err := parseTableDef(defJSON)
	require.NoError(t, err)
	prev := &protos.TableSchema{TableIdentifier: "public.t", System: protos.TypeSystem_PG, NullableEnabled: true}
	names := map[uint32]string{23: "int4", 1043: "varchar", 25: "text"}
	next, err := schemaFromDef(prev, defJSON, def, map[string]struct{}{"secret": {}},
		func(oid uint32) (string, string, error) { return names[oid], "", nil })
	require.NoError(t, err)
	require.Equal(t, []string{"id"}, next.PrimaryKeyColumns)
	require.Equal(t, uint32(42), next.TableOid)
	require.JSONEq(t, defJSON, next.Definition)
	require.True(t, next.NullableEnabled)
	require.Len(t, next.Columns, 2)
	require.Equal(t, &protos.FieldDescription{Name: "v", Type: "varchar", TypeModifier: 14, Nullable: true, Attnum: 3}, next.Columns[1])
	require.False(t, next.Columns[0].Nullable)

	def.RI, def.PrimaryKey = "f", nil
	full, err := schemaFromDef(prev, defJSON, def, nil, func(oid uint32) (string, string, error) { return names[oid], "", nil })
	require.NoError(t, err)
	require.True(t, full.IsReplicaIdentityFull)
	require.Equal(t, []string{"id", "v", "secret"}, full.PrimaryKeyColumns)
}
