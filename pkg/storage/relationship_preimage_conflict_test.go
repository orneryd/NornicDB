package storage

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
	"time"
)

func TestReservedRelationshipUpdateRejectsStalePreimage(t *testing.T) {
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

func TestRelationshipObservationRejectsPeerUpdateBeforeStaging(t *testing.T) {
	b := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{wal: true})
	ctx := t.Context()
	for _, id := range []NodeID{"test:a", "test:b"} {
		_, err := b.CreateNode(ctx, &Node{ID: id})
		require.NoError(t, err)
	}
	require.NoError(t, b.CreateEdge(ctx, &Edge{ID: "test:r", StartNode: "test:a", EndNode: "test:b", Type: "COUNT", Properties: map[string]interface{}{"n": int64(0)}}))
	require.NoError(t, b.FlushWriteBehind(ctx))
	stale, err := b.BeginTransaction(ctx)
	require.NoError(t, err)
	defer stale.Rollback()
	require.NoError(t, stale.SetNamespace("test"))
	require.NoError(t, stale.SetImplicit(true))
	before, err := stale.GetEdge(ctx, "test:r")
	require.NoError(t, err)
	bufferedWrite(t, b, func(peer *BadgerTransaction) error {
		edge, err := peer.GetEdge(ctx, "test:r")
		if err != nil {
			return err
		}
		edge.Properties["n"] = int64(1)
		return peer.UpdateEdge(ctx, EdgeUpdateRequest{After: edge})
	})
	after := copyEdge(before)
	after.Properties["n"] = int64(1)
	require.ErrorIs(t, stale.UpdateEdge(ctx, EdgeUpdateRequest{Before: before, After: after}), ErrConflict)
	latest, err := b.GetEdge(ctx, "test:r")
	require.NoError(t, err)
	require.Equal(t, int64(1), latest.Properties["n"])
}

func TestRelationshipPreimagesFoldRepeatedAndCreatedImages(t *testing.T) {
	for _, created := range []bool{false, true} {
		name := "existing"
		if created {
			name = "created"
		}
		t.Run(name, func(t *testing.T) {
			b := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{wal: true})
			ctx := t.Context()
			for _, id := range []NodeID{"test:a", "test:b"} {
				_, err := b.CreateNode(ctx, &Node{ID: id})
				require.NoError(t, err)
			}
			edge := &Edge{ID: "test:r", StartNode: "test:a", EndNode: "test:b", Type: "COUNT", Properties: map[string]interface{}{"n": int64(0)}}
			if !created {
				require.NoError(t, b.CreateEdge(ctx, edge))
				require.NoError(t, b.FlushWriteBehind(ctx))
			}
			bufferedWrite(t, b, func(tx *BadgerTransaction) error {
				if created {
					if err := tx.CreateEdge(ctx, edge); err != nil {
						return err
					}
				}
				before, err := tx.GetEdge(ctx, "test:r")
				if err != nil {
					return err
				}
				for i := int64(1); i <= 2; i++ {
					after := copyEdge(before)
					after.Properties["n"] = i
					if err := tx.UpdateEdge(ctx, EdgeUpdateRequest{Before: before, After: after}); err != nil {
						return err
					}
				}
				return nil
			})
			require.NoError(t, b.FlushWriteBehind(ctx))
			latest, err := b.GetEdge(ctx, "test:r")
			require.NoError(t, err)
			require.Equal(t, int64(2), latest.Properties["n"])
		})
	}
}

// Hold the actual public commit after reservation, before acknowledgement.
// Exercise both an unapplied ACK and its replayed form, with a real retry.
func TestReservedRelationshipPublicationPreservesAcknowledgedIncrements(t *testing.T) {
	for _, implicit := range []bool{false, true} {
		for _, replay := range []bool{false, true} {
			t.Run(fmt.Sprintf("implicit=%t/replayed=%t", implicit, replay), func(t *testing.T) {
				b := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{manualDrain: true})
				setupCascadeBoundaryGraph(t, b)
				ctx := t.Context()
				entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
				var once, releaseOnce sync.Once
				unblock := func() { releaseOnce.Do(func() { close(release) }) }
				require.NoError(t, b.InitializeStoredEntityObserver("test", func(StoredEntitySnapshot) (StoredEntityObserver, error) {
					return func(StoredEntityPublication) (func(), error) {
						once.Do(func() { close(entered); <-release })
						return nil, nil
					}, nil
				}, nil))
				peer, err := b.BeginTransaction(ctx)
				require.NoError(t, err)
				defer peer.Rollback()
				require.NoError(t, peer.SetImplicit(true))
				before, err := peer.GetEdge(ctx, "test:r2")
				require.NoError(t, err)
				after := copyEdge(before)
				after.Properties = map[string]interface{}{"n": int64(1)}
				require.NoError(t, peer.UpdateEdge(ctx, EdgeUpdateRequest{Before: before, After: after}))
				var peerErr error
				go func() { defer close(done); peerErr = peer.Commit() }()
				t.Cleanup(func() {
					unblock()
					select {
					case <-done:
					case <-time.After(10 * time.Second):
						t.Error("peer commit did not settle")
					}
				})
				select {
				case <-entered:
				case <-time.After(10 * time.Second):
					t.Fatal("peer did not reserve publication")
				}
				stale, err := b.BeginTransaction(ctx)
				require.NoError(t, err)
				defer stale.Rollback()
				require.NoError(t, stale.SetImplicit(implicit))
				before, err = stale.GetEdge(ctx, "test:r2")
				require.NoError(t, err)
				require.NotContains(t, before.Properties, "n")
				after = copyEdge(before)
				after.Properties = map[string]interface{}{"n": int64(1)}
				require.NoError(t, stale.UpdateEdge(ctx, EdgeUpdateRequest{Before: before, After: after}))
				unblock()
				select {
				case <-done:
				case <-time.After(10 * time.Second):
					t.Fatal("peer did not publish")
				}
				require.NoError(t, peerErr)
				if replay {
					require.NoError(t, b.FlushWriteBehind(context.Background()))
				}
				require.ErrorIs(t, stale.Commit(), ErrConflict)
				bufferedWrite(t, b, func(retry *BadgerTransaction) error {
					before, err := retry.GetEdge(ctx, "test:r2")
					if err != nil {
						return err
					}
					require.Equal(t, int64(1), before.Properties["n"])
					after := copyEdge(before)
					after.Properties["n"] = int64(2)
					return retry.UpdateEdge(ctx, EdgeUpdateRequest{Before: before, After: after})
				})
				require.NoError(t, b.FlushWriteBehind(context.Background()))
				latest, err := b.GetEdge(ctx, "test:r2")
				require.NoError(t, err)
				require.Equal(t, int64(2), latest.Properties["n"])
			})
		}
	}
}
