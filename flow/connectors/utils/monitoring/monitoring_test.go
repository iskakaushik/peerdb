//go:build tilt

package monitoring

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
)

func TestGetMirrorStallsCountsOnlyAnUnmovedSlot(t *testing.T) {
	ctx := t.Context()
	pool, err := internal.GetCatalogConnectionPoolFromEnv(ctx)
	require.NoError(t, err)

	name := "test_mirror_stalls_" + uuid.NewString()
	var peerID int
	require.NoError(t, pool.QueryRow(ctx, `INSERT INTO peers (name, type, options) VALUES ($1, 0, '') RETURNING id`, name).Scan(&peerID))
	_, err = pool.Exec(ctx, `INSERT INTO flows (name, source_peer, destination_peer, status) VALUES ($1, $2, $2, $3)`,
		name, peerID, int32(protos.FlowStatus_STATUS_RUNNING))
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `INSERT INTO peerdb_stats.qrep_runs (flow_name, run_uuid, parent_mirror_name, end_time)
		VALUES ($1, $2, $1, now() - interval '2 hours')`, name, uuid.NewString())
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx := context.WithoutCancel(ctx)
		_, _ = pool.Exec(ctx, `DELETE FROM peerdb_stats.peer_slot_size WHERE slot_name = $1`, name)
		_, _ = pool.Exec(ctx, `DELETE FROM peerdb_stats.qrep_runs WHERE parent_mirror_name = $1`, name)
		_, _ = pool.Exec(ctx, `DELETE FROM flows WHERE name = $1`, name)
		_, _ = pool.Exec(ctx, `DELETE FROM peers WHERE id = $1`, peerID)
	})
	sample := func(lsn string, age time.Duration) {
		_, err := pool.Exec(ctx, `INSERT INTO peerdb_stats.peer_slot_size (slot_name, peer_name, confirmed_flush_lsn, updated_at)
			VALUES ($1, $1, $2, now() - make_interval(secs => $3))`, name, lsn, age.Seconds())
		require.NoError(t, err)
	}
	slot := &protos.SlotInfo{SlotName: name, ConfirmedFlushLSN: "0/2", ConfirmedToCurrentMb: 1}

	sample("0/1", 10*time.Minute)
	_, stall, err := GetMirrorStalls(ctx, pool, name, slot)
	require.NoError(t, err)
	require.Zero(t, stall)

	sample("0/2", 5*time.Minute)
	_, stall, err = GetMirrorStalls(ctx, pool, name, slot)
	require.NoError(t, err)
	require.InDelta(t, 5*time.Minute, stall, float64(30*time.Second))
}
