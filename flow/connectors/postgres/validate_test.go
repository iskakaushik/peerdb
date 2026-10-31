package connpostgres

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSameTable(t *testing.T) {
	require.True(t, sameTable("app.t", "app.t"))
	require.True(t, sameTable("My Schema.Order Items", "My Schema.Order Items"))
	require.False(t, sameTable("app.t", "app.t_renamed"))
	require.False(t, sameTable("app.t", "other.t"))
}
