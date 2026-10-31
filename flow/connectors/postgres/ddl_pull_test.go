package connpostgres

import (
	"testing"

	"github.com/jackc/pglogrepl"
	"github.com/stretchr/testify/require"
)

func TestPublishedColumns(t *testing.T) {
	def := tdef("public.t", col(1, "id", "integer"), col(2, "secret", "text"), col(3, "g", "integer"), col(4, "c", "text"))
	def.Columns[2].Gen = "s"
	require.Equal(t, []string{"id", "c"}, publishedColumns(def, map[string]struct{}{"secret": {}}))
}

func TestRetyped(t *testing.T) {
	def := tdef("public.t", ColumnDef{Name: "a", OID: 23, TypMod: -1}, ColumnDef{Name: "b", OID: 25, TypMod: -1})
	rel := func(oid uint32, typmod int32) *pglogrepl.RelationMessage {
		return &pglogrepl.RelationMessage{Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "a", DataType: 23, TypeModifier: -1}, {Name: "b", DataType: oid, TypeModifier: typmod},
			{Name: "dropped_later", DataType: 25, TypeModifier: -1},
		}}
	}
	require.False(t, retyped(def, rel(25, -1)))
	require.True(t, retyped(def, rel(1043, -1)))
	require.True(t, retyped(def, rel(25, 14)))
}
