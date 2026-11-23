package storage

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCoversDeleteIntoLargeNode(t *testing.T) {
	for _, policy := range []string{"none", "allowed"} {
		t.Run(policy, func(t *testing.T) {
			ctx := t.Context()
			engine := newTestEngine(t)
			const chunkCount, chunkDimensions, sources = 64, 2048, 24
			const vectorBytes = chunkCount * chunkDimensions * 4
			vectors := make([][]float32, chunkCount)
			for index := range vectors {
				vectors[index] = make([]float32, chunkDimensions)
				vectors[index][index] = 1
			}
			_, err := engine.CreateNode(ctx, &Node{ID: "test:target", Labels: []string{"Target"}, ChunkEmbeddings: vectors})
			require.NoError(t, err)
			if policy == "allowed" {
				require.NoError(t, engine.GetSchemaForNamespace("test").AddConstraint(Constraint{
					Name: "covers_policy", Type: ConstraintPolicy, EntityType: ConstraintEntityRelationship,
					Label: "COVERS", SourceLabel: "Source", TargetLabel: "Target", PolicyMode: "ALLOWED",
				}))
			}
			for index := range sources {
				id := NodeID(fmt.Sprintf("test:source-%d", index))
				_, err := engine.CreateNode(ctx, &Node{ID: id, Labels: []string{"Source", "Searchable"}})
				require.NoError(t, err)
				require.NoError(t, engine.CreateEdge(ctx, &Edge{
					ID: EdgeID(fmt.Sprintf("test:covers-%d", index)), Type: "COVERS", StartNode: id, EndNode: "test:target",
				}))
			}
			target, err := engine.GetNode(ctx, "test:target")
			require.NoError(t, err)
			require.Equal(t, vectors, target.ChunkEmbeddings)

			tx, err := engine.BeginTransaction(ctx, OrdinaryBegin)
			require.NoError(t, err)
			t.Cleanup(func() { _ = tx.Rollback() })
			var before, staged, committed runtime.MemStats
			runtime.ReadMemStats(&before)
			for index := range sources {
				id := NodeID(fmt.Sprintf("test:source-%d", index))
				require.NoError(t, tx.UpdateGraphNode(ctx, NodeUpdateRequest{
					After: &Node{ID: id, Labels: []string{"Source"}}, Policy: ClientNodeEdits,
				}))
			}
			runtime.ReadMemStats(&staged)
			require.NoError(t, tx.Commit())
			runtime.ReadMemStats(&committed)

			statementBytes := staged.TotalAlloc - before.TotalAlloc
			commitBytes := committed.TotalAlloc - staged.TotalAlloc
			t.Logf("staging allocated %d bytes and commit allocated %d bytes for %d relationships and a %d-byte vector payload", statementBytes, commitBytes, sources, vectorBytes)
			// One full vector decode for every relationship costs 12 MiB
			// before normal write overhead. Allow generous headroom for the
			// small node writes and the in-memory store's background work.
			const limit = uint64(8 * vectorBytes)
			if statementBytes > limit {
				t.Errorf("staging allocated %d bytes, bound %d", statementBytes, limit)
			}
			if commitBytes > limit {
				t.Errorf("commit allocated %d bytes, bound %d", commitBytes, limit)
			}
			target, err = engine.GetNode(ctx, "test:target")
			require.NoError(t, err)
			require.Equal(t, vectors, target.ChunkEmbeddings)
			node, err := engine.GetNode(ctx, "test:source-0")
			require.NoError(t, err)
			require.Equal(t, []string{"Source"}, node.Labels)
		})
	}
}

