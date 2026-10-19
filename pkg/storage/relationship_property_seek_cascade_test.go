package storage_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Each candidate body read in a live seek checks cancellation. Compare against
// the same warmed survivor-only seek to detect attempts to load absent bodies,
// without adding a production read hook or relying on elapsed time.
type cascadeSeekContext struct {
	context.Context
	checks int
}

func (ctx *cascadeSeekContext) Err() error {
	ctx.checks++
	return ctx.Context.Err()
}

func TestRelationshipSeekCascadeDelete(t *testing.T) {
	for _, mode := range []string{"direct", "cypher", "cypher-transaction"} {
		t.Run(mode, func(t *testing.T) {
			engine, err := storage.NewBadgerEngineInMemory()
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, engine.Close()) })
			ns := storage.NewNamespacedEngine(engine, "test")
			for _, id := range []storage.NodeID{"victim", "a", "b"} {
				_, err := ns.CreateNode(&storage.Node{ID: id, Labels: []string{"Cascade"}, Properties: map[string]interface{}{"name": string(id)}})
				require.NoError(t, err)
			}
			require.NoError(t, ns.GetSchema().AddRangeIndexForEntity("rel_p", "T", []string{"p"}, storage.ConstraintEntityRelationship))
			require.NoError(t, ns.CreateEdge(&storage.Edge{ID: "survivor", StartNode: "a", EndNode: "b", Type: "T", Properties: map[string]interface{}{"p": "hot"}}))
			seek := func() (int, int) {
				t.Helper()
				ctx := &cascadeSeekContext{Context: context.Background()}
				edges, answered, err := ns.SeekRelationships(ctx, "T", []string{"p"}, []interface{}{"hot"})
				require.NoError(t, err)
				require.True(t, answered)
				return len(edges), ctx.checks
			}
			_, _ = seek() // Populate the bucket before measuring a warm lookup.
			count, survivorChecks := seek()
			require.Equal(t, 1, count)
			const n = 48
			for i := 0; i < n; i++ {
				start, end := storage.NodeID("victim"), storage.NodeID("a")
				switch i % 3 {
				case 1:
					start, end = end, start // Incoming cascade.
				case 2:
					end = start // Self-loop must be removed exactly once.
				}
				require.NoError(t, ns.CreateEdge(&storage.Edge{ID: storage.EdgeID(fmt.Sprint(i)), StartNode: start, EndNode: end, Type: "T", Properties: map[string]interface{}{"p": "hot"}}))
			}
			count, _ = seek()
			require.Equal(t, n+1, count)
			keys, candidates := storage.RelationshipSeekTestBucketSize(engine, "test", "T", "p", "hot")
			require.Equal(t, n+1, keys)
			require.Equal(t, n+1, candidates)
			if mode == "direct" {
				require.NoError(t, ns.DeleteNode("victim"))
			} else {
				exec := cypher.NewStorageExecutor(ns)
				if mode == "cypher-transaction" {
					_, err = exec.Execute(context.Background(), "BEGIN", nil)
					require.NoError(t, err)
				}
				_, err = exec.Execute(context.Background(), "MATCH (n:Cascade {name: 'victim'}) DETACH DELETE n", nil)
				require.NoError(t, err)
				if mode == "cypher-transaction" {
					_, err = exec.Execute(context.Background(), "COMMIT", nil)
					require.NoError(t, err)
				}
			}
			count, _ = seek() // Reconcile the dirty IDs once, as for explicit deletion.
			require.Equal(t, 1, count)
			keys, candidates = storage.RelationshipSeekTestBucketSize(engine, "test", "T", "p", "hot")
			require.Equal(t, 1, keys, "cascaded IDs must leave the retained key map")
			require.Equal(t, 1, candidates, "cascaded IDs must leave the hot-value bucket")
			for i := 0; i < 3; i++ {
				count, checks := seek()
				require.Equal(t, 1, count)
				require.Equal(t, survivorChecks, checks, "warm seeks must not attempt absent-body loads")
			}
		})
	}
}
