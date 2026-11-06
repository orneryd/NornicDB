package storage

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
)

// Regression for the release CI failure
// TestWriteOrderModel/writeBehind=true seed=19 ("rejected-valid: restore with
// no transaction open: rejected backup failed: derived count underflow for
// test:LINK").
//
// A node deletion that cascades relationships appends one derived per-type
// count delta per relationship it removes. A concurrent commit that removes one
// of those relationships decrements the same counter, so the cascade must not
// apply its delta a second time: it is a write-write conflict on a relationship
// the commit writes, which the synchronous path already rejects through
// Badger's own conflict detection. The write-behind path rejected it only while
// the concurrent commit was still unapplied in the buffer; once the background
// flusher had landed it, the buffered commit was acknowledged, both decrements
// were applied, and the persisted per-type counter went negative — surfacing
// when a restore folded the deltas.
func TestNodeCascadeDeleteRejectsConcurrentlyDeletedRelationship(t *testing.T) {
	for _, writeBehind := range []bool{false, true} {
		for _, applied := range []bool{false, true} {
			writeBehind, applied := writeBehind, applied
			name := "writeBehind=false"
			if writeBehind {
				name = "writeBehind=true"
			}
			if applied {
				name += "/concurrent commit applied"
			} else {
				name += "/concurrent commit buffered"
			}
			t.Run(name, func(t *testing.T) {
				ctx := context.Background()
				engine, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir(), WriteBehind: writeBehind}))
				require.NoError(t, err)
				defer engine.Close()
				if engine.writeBehind != nil {
					// Keep generations unapplied until a step drains them, as the
					// write-order model driver does: the two timings this test
					// covers are then deterministic.
					engine.writeBehind.SetDelayBounds(time.Hour, time.Hour)
				}

				_, err = engine.CreateNode(ctx, &Node{ID: "test:n2", Properties: map[string]any{"v": int64(1)}})
				require.NoError(t, err)
				_, err = engine.CreateNode(ctx, &Node{ID: "test:n3", Properties: map[string]any{"v": int64(1)}})
				require.NoError(t, err)
				require.NoError(t, engine.CreateEdge(ctx, &Edge{ID: "test:r2", StartNode: "test:n2", EndNode: "test:n3",
					Type: "LINK", Properties: map[string]any{"v": int64(1)}}))
				require.NoError(t, engine.FlushWriteBehind(ctx))

				tx, err := engine.BeginTransaction(ctx)
				require.NoError(t, err)
				require.NoError(t, tx.SetImplicit(true))
				require.NoError(t, tx.DeleteGraphNode(ctx, "test:n3", CascadeEdges))

				// The concurrent commit removes the relationship this deletion
				// cascades, and counts that removal.
				require.NoError(t, engine.DeleteNode(ctx, "test:n2"))
				if applied {
					require.NoError(t, engine.FlushWriteBehind(ctx))
				}

				require.ErrorIs(t, tx.Commit(), ErrConflict,
					"a cascade over a relationship a concurrent commit removed is a conflict on a written relationship")
				require.NoError(t, engine.FlushWriteBehind(ctx))

				// The derived per-type count still counts exactly the stored
				// relationships of the type.
				count, err := engine.EdgeCountByType(ctx, "LINK")
				require.NoError(t, err)
				require.Equal(t, int64(0), count)
				_, err = engine.GetStoredEdge(ctx, "test:r2")
				require.ErrorIs(t, err, ErrNotFound)

				// A retry reads the state the concurrent commit left, so the
				// deletion it wants succeeds and the counters stay correct.
				retry, err := engine.BeginTransaction(ctx)
				require.NoError(t, err)
				require.NoError(t, retry.SetImplicit(true))
				require.NoError(t, retry.DeleteGraphNode(ctx, "test:n3", CascadeEdges))
				require.NoError(t, retry.Commit())
				require.NoError(t, engine.FlushWriteBehind(ctx))
				count, err = engine.EdgeCountByType(ctx, "LINK")
				require.NoError(t, err)
				require.Equal(t, int64(0), count)

				// A backup carries the counters, and restoring folds its deltas
				// as opening does: an imbalanced counter fails the restore.
				path := filepath.Join(t.TempDir(), "backup")
				require.NoError(t, engine.Backup(path))
				require.NoError(t, engine.Restore(path))
				count, err = engine.EdgeCountByType(ctx, "LINK")
				require.NoError(t, err)
				require.Equal(t, int64(0), count)
				total, err := engine.EdgeCount(ctx)
				require.NoError(t, err)
				require.Equal(t, int64(0), total)
			})
		}
	}
}

// A cascade that no concurrent commit touches is accepted and its per-type
// counter matches the relationships it left.
func TestNodeCascadeDeleteCountsOnlyRemovedRelationships(t *testing.T) {
	for _, writeBehind := range []bool{false, true} {
		writeBehind := writeBehind
		t.Run(map[bool]string{false: "writeBehind=false", true: "writeBehind=true"}[writeBehind], func(t *testing.T) {
			ctx := context.Background()
			engine, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir(), WriteBehind: writeBehind}))
			require.NoError(t, err)
			defer engine.Close()
			if engine.writeBehind != nil {
				engine.writeBehind.SetDelayBounds(time.Hour, time.Hour)
			}

			for _, id := range []string{"test:n0", "test:n1", "test:n2"} {
				_, err := engine.CreateNode(ctx, &Node{ID: NodeID(id), Properties: map[string]any{"v": int64(1)}})
				require.NoError(t, err)
			}
			require.NoError(t, engine.CreateEdge(ctx, &Edge{ID: "test:r0", StartNode: "test:n0", EndNode: "test:n2",
				Type: "LINK", Properties: map[string]any{"v": int64(1)}}))
			require.NoError(t, engine.CreateEdge(ctx, &Edge{ID: "test:r1", StartNode: "test:n1", EndNode: "test:n2",
				Type: "LINK", Properties: map[string]any{"v": int64(1)}}))
			require.NoError(t, engine.FlushWriteBehind(ctx))

			tx, err := engine.BeginTransaction(ctx)
			require.NoError(t, err)
			require.NoError(t, tx.SetImplicit(true))
			require.NoError(t, tx.DeleteGraphNode(ctx, "test:n2", CascadeEdges))
			require.NoError(t, tx.Commit())
			require.NoError(t, engine.FlushWriteBehind(ctx))

			count, err := engine.EdgeCountByType(ctx, "LINK")
			require.NoError(t, err)
			require.Equal(t, int64(0), count)
			total, err := engine.EdgeCount(ctx)
			require.NoError(t, err)
			require.Equal(t, int64(0), total)
		})
	}
}
