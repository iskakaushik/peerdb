package connpostgres

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResyncSQL(t *testing.T) {
	in := testPlanInput(t)
	in.tables = map[uint32]bool{200: true, 201: true}
	in.exists = existing(in, 30)
	pre, post := resyncSQL(in, "POST;\n")
	require.Equal(t, `DROP TABLE IF EXISTS "app"."p_resync" CASCADE;
ALTER TABLE "app"."p" RENAME TO "_peerdb_hold_0";
DROP TABLE IF EXISTS "app"."c_resync" CASCADE;
`, pre)
	require.Equal(t, `POST;
ALTER TABLE "app"."p" RENAME TO "p_resync";
ALTER TABLE "app"."_peerdb_hold_0" RENAME TO "p";
ALTER TABLE "app"."c" RENAME TO "c_resync";
`, post)
}
