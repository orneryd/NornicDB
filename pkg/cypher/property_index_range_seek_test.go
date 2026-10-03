package cypher

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Index seeds only change where a MATCH reads its nodes from, never what it
// returns (#820): every query here must return the same rows with the
// property index as without it.

func newRangeSeekExecutor(t *testing.T, indexed bool) *StorageExecutor {
	t.Helper()
	exec := NewStorageExecutorWithQueryCachePolicy(storage.NewNamespacedEngine(storage.NewMemoryEngine(), "test"), 0, 0)
	ctx := context.Background()
	if indexed {
		for _, ddl := range []string{"CREATE INDEX item_v FOR (n:Item) ON (n.v)", "CREATE INDEX extra_v FOR (n:Extra) ON (n.v)"} {
			_, err := exec.Execute(ctx, ddl, nil)
			require.NoError(t, err)
		}
	}
	for _, statement := range []string{
		"UNWIND range(-3, 12) AS i CREATE (:Item {v: i, w: i % 3})",
		"CREATE (:Item {v: 2.5, w: 1}), (:Item {v: 7.0, w: 0}), (:Item {v: -0.5, w: 2})",
		"CREATE (:Item {v: 'apple', w: 1}), (:Item {v: 'banana', w: 0}), (:Item {v: 'Zebra', w: 1}), (:Item {v: '', w: 2})",
		"CREATE (:Item {v: true, w: 1}), (:Item {v: [1, 2], w: 0}), (:Item {w: 1}), (:Item:Extra {v: 4, w: 1}), (:Other {v: 3})",
	} {
		_, err := exec.Execute(ctx, statement, nil)
		require.NoError(t, err)
	}
	return exec
}

func rangeSeekRows(t *testing.T, exec *StorageExecutor, query string, params map[string]interface{}) []string {
	t.Helper()
	result, err := exec.Execute(context.Background(), query, params)
	require.NoError(t, err, query)
	rows := make([]string, 0, len(result.Rows))
	for _, row := range result.Rows {
		rows = append(rows, fmt.Sprintf("%v", row))
	}
	sort.Strings(rows)
	return rows
}

func TestPropertyIndexRangeSeekMatchesLabelScan(t *testing.T) {
	indexed := newRangeSeekExecutor(t, true)
	scanned := newRangeSeekExecutor(t, false)
	params := map[string]interface{}{"lo": int64(2), "hi": 7.0, "s": "b", "none": nil, "ids": []interface{}{int64(1), int64(4), 2.5, "apple"}}
	for _, query := range []string{
		"MATCH (n:Item) WHERE n.v < 5 RETURN n.v",
		"MATCH (n:Item) WHERE n.v <= 5 RETURN n.v",
		"MATCH (n:Item) WHERE n.v > 2 RETURN n.v",
		"MATCH (n:Item) WHERE n.v >= 2.5 RETURN n.v",
		"MATCH (n:Item) WHERE 5 > n.v RETURN n.v",
		"MATCH (n:Item) WHERE -1 <= n.v < 7 RETURN n.v",
		"MATCH (n:Item) WHERE n.v > -1 AND n.v <= 7 RETURN n.v",
		"MATCH (n:Item) WHERE n.v >= $lo AND n.v < $hi RETURN n.v",
		"MATCH (n:Item) WHERE n.v >= 'a' RETURN n.v",
		"MATCH (n:Item) WHERE n.v < $s RETURN n.v",
		"MATCH (n:Item) WHERE n.v < $none RETURN n.v",
		"MATCH (n:Item) WHERE n.v < 5 AND n.w = 1 RETURN n.v",
		"MATCH (n:Item) WHERE n.v < 0 OR n.v > 10 RETURN n.v",
		"MATCH (n:Item) WHERE n.v > 3 AND n.v > 5 RETURN n.v",
		"MATCH (n:Item:Extra) WHERE n.v < 10 RETURN n.v",
		"MATCH (n:Item:Extra) WHERE n.v IN $ids RETURN n.v",
		"MATCH (n:Item) WHERE 4 >= n.v RETURN n.v",
		"MATCH (n:Item) WHERE 4 = n.v AND n.v < 10 RETURN n.v",
		"MATCH (n:Item) WHERE n.v < 5 RETURN count(n)",
		"MATCH (n:Item) WHERE n.v >= 'a' RETURN count(*)",
		"MATCH (n:Item) WHERE n.v < $none RETURN count(n)",
		"MATCH (n:Item {v: 4}) RETURN count(n)",
		"MATCH (n:Item:Extra {v: 4}) RETURN count(n)",
		"MATCH (n:Item {v: 4}) RETURN n.w",
		"MATCH (n:Item) WHERE n.v = 4 RETURN count(n)",
		"MATCH (n:Item) WHERE n.v IN $ids RETURN count(n)",
		"MATCH (n:Item) WHERE n.v IN $ids RETURN n.v",
		"MATCH (n:Item) WHERE n.v < 3 CALL (n) { RETURN n.w AS w } RETURN sum(w)",
	} {
		require.Equal(t, rangeSeekRows(t, scanned, query, params), rangeSeekRows(t, indexed, query, params), query)
	}
}

// Writes earlier in the same transaction are seen by an index-seeded range
// read and count.
func TestPropertyIndexRangeSeekSeesOwnWrites(t *testing.T) {
	for _, indexed := range []bool{true, false} {
		exec := newRangeSeekExecutor(t, indexed)
		ctx := context.Background()
		_, err := exec.Execute(ctx, "BEGIN", nil)
		require.NoError(t, err)
		_, err = exec.Execute(ctx, "CREATE (:Item {v: 3.5, w: 9})", nil)
		require.NoError(t, err)
		_, err = exec.Execute(ctx, "MATCH (n:Item {v: 4}) SET n.v = 40", nil)
		require.NoError(t, err)
		rows := rangeSeekRows(t, exec, "MATCH (n:Item) WHERE 3 < n.v < 5 RETURN n.v", nil)
		require.Equal(t, []string{"[3.5]"}, rows, "indexed=%v", indexed)
		count := rangeSeekRows(t, exec, "MATCH (n:Item) WHERE n.v > 30 RETURN count(n)", nil)
		require.Equal(t, []string{"[2]"}, count, "indexed=%v", indexed)
		_, err = exec.Execute(ctx, "ROLLBACK", nil)
		require.NoError(t, err)
	}
}

// The comparisons above only prove anything if the indexed executor really
// seeds from the index: range bounds, inline properties and a WHERE equality
// all take an index seed, which counting shares with row reads.
func TestPropertyIndexSeedsAreUsed(t *testing.T) {
	exec := newRangeSeekExecutor(t, true)
	ctx := context.Background()
	item := nodePatternInfo{variable: "n", labels: []string{"Item"}}

	nodes, used, err := exec.tryCollectNodesFromPropertyIndexRange(item, "n.v >= 2 AND n.v < 4 AND n.w > 0", nil)
	require.NoError(t, err)
	require.True(t, used)
	values := make([]string, 0, len(nodes))
	for _, node := range nodes {
		values = append(values, fmt.Sprintf("%v", node.Properties["v"]))
	}
	require.ElementsMatch(t, []string{"2", "2.5", "3"}, values, "the seed holds every value within the bounds; the rest of the WHERE is applied later")

	_, used, err = exec.tryCollectNodesFromPropertyIndexRange(item, "n.v < 0 OR n.v > 10", nil)
	require.NoError(t, err)
	require.False(t, used, "a disjunction places no bound")

	for _, seed := range []struct {
		pattern nodePatternInfo
		where   string
	}{
		{nodePatternInfo{variable: "n", labels: []string{"Item"}, properties: map[string]interface{}{"v": int64(4)}}, ""},
		{item, "n.v = 4"},
		{item, "n.v > 10"},
		{item, "n.v IN $ids"},
	} {
		_, _, indexed, err := exec.collectPipelineIndexedNodeCandidates(withQueryParams(ctx, map[string]interface{}{"ids": []interface{}{int64(1)}}), seed.pattern, seed.where, pipelineMatchPhysicalHint{})
		require.NoError(t, err)
		require.True(t, indexed, seed.where)
	}

	unindexed := newRangeSeekExecutor(t, false)
	_, _, indexed, err := unindexed.collectPipelineIndexedNodeCandidates(ctx, item, "n.v > 10", pipelineMatchPhysicalHint{})
	require.NoError(t, err)
	require.False(t, indexed)
}
