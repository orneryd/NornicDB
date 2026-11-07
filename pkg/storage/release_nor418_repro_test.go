package storage

import (
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestReleaseNOR418ReservedRelationshipUpdate(t *testing.T) {
	for _, implicit := range []bool{false, true} {
		name := "explicit"
		if implicit {
			name = "implicit"
		}
		t.Run(name, func(t *testing.T) {
			b := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{wal: true})
			ctx := t.Context()
			for _, id := range []NodeID{"test:a", "test:b"} {
				_, err := b.CreateNode(ctx, &Node{ID: id})
				require.NoError(t, err)
			}
			require.NoError(t, b.CreateEdge(ctx, &Edge{ID: "test:r", StartNode: "test:a", EndNode: "test:b", Type: "COUNT", Properties: map[string]interface{}{"n": int64(0)}}))
			require.NoError(t, b.FlushWriteBehind(ctx))
			peer, err := b.BeginTransaction(ctx)
			require.NoError(t, err)
			defer peer.Rollback()
			require.NoError(t, peer.SetNamespace("test"))
			edge, err := peer.GetEdge(ctx, "test:r")
			require.NoError(t, err)
			edge.Properties["n"] = int64(1)
			require.NoError(t, peer.UpdateEdge(ctx, EdgeUpdateRequest{After: edge}))
			stageReservedPublication(t, peer)
			stale, err := b.BeginTransaction(ctx)
			require.NoError(t, err)
			defer stale.Rollback()
			require.NoError(t, stale.SetNamespace("test"))
			require.NoError(t, stale.SetImplicit(implicit))
			edge, err = stale.GetEdge(ctx, "test:r")
			require.NoError(t, err)
			require.Equal(t, int64(0), edge.Properties["n"])
			edge.Properties["n"] = int64(1)
			require.NoError(t, stale.UpdateEdge(ctx, EdgeUpdateRequest{After: edge}))
			publishReservedPublication(t, peer)
			require.ErrorIs(t, stale.Commit(), ErrConflict, "a reserved version must not permit overwriting a newer published relationship image")
		})
	}
}
