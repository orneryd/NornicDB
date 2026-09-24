package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExecuteCreate_BranchCoverage(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	t.Run("skips empty split patterns and still creates valid node", func(t *testing.T) {
		res, err := exec.executeCreate(ctx, "CREATE (a:KeepMe), , (b:KeepMe)")
		require.NoError(t, err)
		require.NotNil(t, res.Stats)
		assert.Equal(t, 2, res.Stats.NodesCreated)
	})

	t.Run("empty label name is rejected", func(t *testing.T) {
		_, err := exec.executeCreate(ctx, "CREATE (n:)")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "empty label name after colon")
	})

	t.Run("invalid label identifier is rejected", func(t *testing.T) {
		_, err := exec.executeCreate(ctx, "CREATE (n:Bad-Label)")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid label name")
	})

	t.Run("invalid relationship type is rejected", func(t *testing.T) {
		_, err := exec.executeCreate(ctx, "CREATE (a:Person)-[:BAD-TYPE]->(b:Person)")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid relationship type")
	})

	t.Run("reverse relationship direction creates correct edge orientation", func(t *testing.T) {
		res, err := exec.executeCreate(ctx, "CREATE (a:RevA {name:'a'})<-[:KNOWS]-(b:RevB {name:'b'})")
		require.NoError(t, err)
		require.NotNil(t, res.Stats)
		assert.Equal(t, 2, res.Stats.NodesCreated)
		assert.Equal(t, 1, res.Stats.RelationshipsCreated)

		edges, err := store.AllEdges()
		require.NoError(t, err)
		found := false
		for _, e := range edges {
			if e.Type == "KNOWS" {
				found = true
				break
			}
		}
		assert.True(t, found)
	})

	t.Run("path assignment with relationship variable and return accessors", func(t *testing.T) {
		res, err := exec.executeCreate(ctx, "CREATE p=(s:PathSrc {id:'s1'})-[r:LINK {w:2}]->(t:PathDst {id:'t1'}) RETURN p, r.w")
		require.NoError(t, err)
		require.Equal(t, []string{"p", "r.w"}, res.Columns)
		require.Len(t, res.Rows, 1)
		require.NotNil(t, res.Rows[0][0]) // path value
		assert.EqualValues(t, 2, res.Rows[0][1])
	})

	t.Run("relationship without a type is rejected", func(t *testing.T) {
		// CREATE needs exactly one relationship type (statement validation
		// and the CREATE core agree; there is no default type).
		_, err := exec.executeCreate(ctx, "CREATE (u:AnonA)-[rel]->(v:AnonB) RETURN rel")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "relationship type is required")
	})
}
