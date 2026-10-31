package connpostgres

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/shared"
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

func TestDeferredSQLDryRun(t *testing.T) {
	cons := []deferredCon{
		{rel: "app.c", name: "c_chk", def: "CHECK (n > 0)", relOid: 201},
		{rel: "app.c", name: "c_p_fkey", def: "FOREIGN KEY (p) REFERENCES app.p(id)", fk: true, valid: true, relOid: 201},
	}
	plan := schemaPlan{created: []uint32{201}}
	drop := "ALTER TABLE app.c DROP CONSTRAINT IF EXISTS \"c_chk\";\n"
	require.Equal(t, drop, deferredSQL(plan, cons, cons[1:], shared.POSTGRES_17, false))
	require.Equal(t, drop+"ALTER TABLE app.c ADD CONSTRAINT \"c_p_fkey\" FOREIGN KEY (p) REFERENCES app.p(id) NOT VALID;\n",
		deferredSQL(plan, cons, cons[1:], shared.POSTGRES_17, true))
}
