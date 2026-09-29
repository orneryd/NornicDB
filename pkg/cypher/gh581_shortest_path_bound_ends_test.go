package cypher

// gh581_shortest_path_bound_ends_test.go — regression tests for #581
// (consolidated with #721): shortestPath / allShortestPaths over variables
// bound by an earlier clause, OPTIONAL MATCH shortestPath projection, and
// shortestPath(...) in expression position.
//
// Pinned against neo4j:5.26.30-community (issue tables).

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func newGh581Executor(t *testing.T) *StorageExecutor {
	t.Helper()
	base, err := storage.NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, base.Close())
	})
	store := storage.NewNamespacedEngine(base, "gh581")
	return NewStorageExecutor(store)
}

// seedGh581Chains builds the #721 graph: for each i in 1..3 a
// (d:D {k: i})-[:R]->(:M)-[:R]->(:M) chain.
func seedGh581Chains(t *testing.T, exec *StorageExecutor, ctx context.Context) {
	t.Helper()
	result, err := exec.Execute(ctx, "UNWIND range(1, 3) AS i CREATE (d:D {k: i}) CREATE (d)-[:R]->(:M)-[:R]->(:M)", nil)
	require.NoError(t, err)
	require.Equal(t, 9, result.Stats.NodesCreated)
}

// seedGh581ZSP builds the original #581 graph:
// (:ZSP {id: 1})-[:ZS]->(:ZSP {id: 2})-[:ZS]->(:ZSP {id: 3}).
func seedGh581ZSP(t *testing.T, exec *StorageExecutor, ctx context.Context) {
	t.Helper()
	_, err := exec.Execute(ctx, "CREATE (:ZSP {id: 1})-[:ZS {w: 5}]->(:ZSP {id: 2})-[:ZS {w: 7}]->(:ZSP {id: 3})", nil)
	require.NoError(t, err)
}

func intRows(t *testing.T, result *ExecuteResult) [][]int64 {
	t.Helper()
	rows := make([][]int64, 0, len(result.Rows))
	for _, row := range result.Rows {
		ints := make([]int64, len(row))
		for i, value := range row {
			if value == nil {
				ints[i] = -1 // sentinel for null
				continue
			}
			v, ok := value.(int64)
			require.True(t, ok, "expected int64, got %T (%v)", value, value)
			ints[i] = v
		}
		rows = append(rows, ints)
	}
	return rows
}

func TestGh581_BoundEndShortestPath(t *testing.T) {
	exec := newGh581Executor(t)
	ctx := context.Background()
	seedGh581Chains(t, exec, ctx)

	t.Run("721_1_first_match_is_traversal", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (node:D)-[:R*1..3]->(x)
			MATCH p = shortestPath((node)-[:R*1..3]->(x))
			RETURN node.k AS k, min(length(p)) AS m ORDER BY k
		`, nil)
		require.NoError(t, err)
		require.Equal(t, [][]int64{{1, 1}, {2, 1}, {3, 1}}, intRows(t, result))
	})

	t.Run("721_2_comma_bound_pair", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (node:D), (x:M)
			MATCH p = shortestPath((node)-[:R*1..3]->(x))
			RETURN node.k AS k, count(p) AS c ORDER BY k
		`, nil)
		require.NoError(t, err)
		require.Equal(t, [][]int64{{1, 2}, {2, 2}, {3, 2}}, intRows(t, result))
	})

	t.Run("721_3_two_matches_then_where", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (node:D {k: 1})
			MATCH (x:M) WHERE (node)-->(x)
			MATCH p = shortestPath((node)-[:R*]->(x))
			RETURN length(p) AS l
		`, nil)
		require.NoError(t, err)
		require.Equal(t, [][]int64{{1}}, intRows(t, result))
	})

	t.Run("721_4_comma_pair_all_paths", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (a:D {k: 1}), (b:M)
			MATCH p = shortestPath((a)-[:R*]->(b))
			RETURN length(p) AS l ORDER BY l
		`, nil)
		require.NoError(t, err)
		require.Equal(t, [][]int64{{1}, {2}}, intRows(t, result))
	})

	t.Run("721_5_same_pattern", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH p = shortestPath((a:D {k: 1})-[:R*]->(b:M))
			RETURN length(p) AS l ORDER BY l
		`, nil)
		require.NoError(t, err)
		require.Equal(t, [][]int64{{1}, {2}}, intRows(t, result))
	})

	t.Run("721_6_one_hop_anchor", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (node:D {k: 1})-[:R]->(x:M)
			MATCH p = shortestPath((node)-[*]-(x))
			RETURN length(p) AS l
		`, nil)
		require.NoError(t, err)
		require.Equal(t, [][]int64{{1}}, intRows(t, result))
	})

	t.Run("721_7_return_value_min", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (node:D)-[:R*1..3]->(x)
			RETURN node.k AS k, min(length(shortestPath((node)-[:R*1..3]->(x)))) AS m ORDER BY k
		`, nil)
		require.NoError(t, err)
		require.Equal(t, [][]int64{{1, 1}, {2, 1}, {3, 1}}, intRows(t, result))
	})

	t.Run("721_8_with_value", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (node:D)-[:R*1..3]->(x)
			WITH node, shortestPath((node)-[:R*1..3]->(x)) AS p
			RETURN node.k AS k, min(length(p)) AS m ORDER BY k
		`, nil)
		require.NoError(t, err)
		require.Equal(t, [][]int64{{1, 1}, {2, 1}, {3, 1}}, intRows(t, result))
	})

	t.Run("721_9_count_value", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (a:D {k: 1}), (b:M)
			RETURN count(shortestPath((a)-[:R*]->(b))) AS c
		`, nil)
		require.NoError(t, err)
		require.Equal(t, [][]int64{{2}}, intRows(t, result))
	})

	t.Run("clause_where_filters_paths", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (a:D {k: 1}), (b:M)
			MATCH p = shortestPath((a)-[:R*]->(b))
			WHERE length(p) > 1
			RETURN length(p) AS l ORDER BY l
		`, nil)
		require.NoError(t, err)
		require.Equal(t, [][]int64{{2}}, intRows(t, result))
	})

	t.Run("bound_end_all_shortest_paths", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (a:D {k: 1}), (b:M)
			MATCH p = allShortestPaths((a)-[:R*]->(b))
			RETURN length(p) AS l ORDER BY l
		`, nil)
		require.NoError(t, err)
		require.Equal(t, [][]int64{{1}, {2}}, intRows(t, result))
	})
}

func TestGh581_OptionalShortestPathProjection(t *testing.T) {
	exec := newGh581Executor(t)
	ctx := context.Background()
	seedGh581ZSP(t, exec, ctx)

	t.Run("original_1_length", func(t *testing.T) {
		result, err := exec.Execute(ctx,
			"OPTIONAL MATCH p = shortestPath((a:ZSP {id: 1})-[:ZS*]->(c:ZSP {id: 3})) RETURN length(p) AS l", nil)
		require.NoError(t, err)
		require.Equal(t, [][]int64{{2}}, intRows(t, result))
	})

	t.Run("original_2_length_plus_one", func(t *testing.T) {
		result, err := exec.Execute(ctx,
			"OPTIONAL MATCH p = shortestPath((a:ZSP {id: 1})-[:ZS*]->(c:ZSP {id: 3})) RETURN length(p) + 1 AS l", nil)
		require.NoError(t, err)
		require.Equal(t, [][]int64{{3}}, intRows(t, result))
	})

	t.Run("original_3_is_null_projection", func(t *testing.T) {
		result, err := exec.Execute(ctx,
			"OPTIONAL MATCH p = shortestPath((a:ZSP {id: 1})-[:ZS*]->(c:ZSP {id: 3})) RETURN p IS NULL AS missing, 1 AS one", nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		require.Equal(t, false, result.Rows[0][0])
		require.Equal(t, int64(1), result.Rows[0][1])
	})

	t.Run("original_4_no_path_is_null", func(t *testing.T) {
		result, err := exec.Execute(ctx,
			"OPTIONAL MATCH p = shortestPath((a:ZSP {id: 1})-[:ZS*]->(c:ZSP {id: 99})) RETURN length(p) AS l", nil)
		require.NoError(t, err)
		require.Equal(t, [][]int64{{-1}}, intRows(t, result))
	})

	t.Run("original_5_anchored", func(t *testing.T) {
		result, err := exec.Execute(ctx,
			"MATCH (a:ZSP {id: 1}) OPTIONAL MATCH p = shortestPath((a)-[:ZS*]->(c:ZSP {id: 3})) RETURN length(p) + 1 AS l", nil)
		require.NoError(t, err)
		require.Equal(t, [][]int64{{3}}, intRows(t, result))
	})
}
