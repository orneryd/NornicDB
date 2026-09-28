package cypher

import (
	"context"
	"strconv"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func BenchmarkCartesianMatchEqualityJoin(b *testing.B) {
	store := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "cartesian-bench")
	const nodeCount = 500
	for i := 0; i < nodeCount; i++ {
		key := strconv.Itoa(i)
		for _, node := range []*storage.Node{
			{ID: storage.NodeID("left-" + key), Labels: []string{"CartesianLeft"}, Properties: map[string]interface{}{"key": key}},
			{ID: storage.NodeID("right-" + key), Labels: []string{"CartesianRight"}, Properties: map[string]interface{}{"key": key}},
		} {
			if _, err := store.CreateNode(node); err != nil {
				b.Fatal(err)
			}
		}
	}
	exec := NewStorageExecutorWithQueryCachePolicy(store, 0, 0)
	ctx := context.Background()
	query := "MATCH (a:CartesianLeft), (b:CartesianRight) WHERE a.key = b.key RETURN count(*) AS c"
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := exec.Execute(ctx, query, nil)
		if err != nil {
			b.Fatal(err)
		}
		if len(result.Rows) != 1 || result.Rows[0][0] != int64(nodeCount) {
			b.Fatalf("got rows %v, want count %d", result.Rows, nodeCount)
		}
	}
}

func TestCartesianWherePushdown_InAndEqualityJoin(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	for _, k := range []string{"k1", "k2", "k3"} {
		_, err := store.CreateNode(&storage.Node{
			ID:     storage.NodeID("nornic:o-" + k),
			Labels: []string{"OriginalText"},
			Properties: map[string]interface{}{
				"joinKey": k,
			},
		})
		require.NoError(t, err)
	}
	for _, row := range []struct {
		id   string
		key  string
		lang string
	}{
		{"t-k1-es", "k1", "es"},
		{"t-k1-fr", "k1", "fr"},
		{"t-k2-es", "k2", "es"},
		{"t-k9-es", "k9", "es"},
	} {
		_, err := store.CreateNode(&storage.Node{
			ID:     storage.NodeID("nornic:" + row.id),
			Labels: []string{"TranslatedText"},
			Properties: map[string]interface{}{
				"joinKey": row.key,
				"lang":    row.lang,
			},
		})
		require.NoError(t, err)
	}

	res, err := exec.Execute(ctx, `
MATCH (o:OriginalText), (t:TranslatedText)
WHERE o.joinKey IN ['k1','k2'] AND t.joinKey = o.joinKey
RETURN count(*) AS c
`, nil)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.Equal(t, int64(3), res.Rows[0][0])

	res, err = exec.Execute(ctx, `
MATCH (o:OriginalText), (t:TranslatedText)
WHERE t.joinKey = o.joinKey AND o.joinKey IN ['k1','k2']
RETURN o.joinKey AS k, count(*) AS c
ORDER BY k
`, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"k", "c"}, res.Columns)
	require.Len(t, res.Rows, 2)
	got := map[string]int64{}
	for _, row := range res.Rows {
		got[row[0].(string)] = row[1].(int64)
	}
	require.Equal(t, int64(2), got["k1"])
	require.Equal(t, int64(1), got["k2"])

}

func TestCartesianWherePushdown_NullConstraint(t *testing.T) {
	exec := NewStorageExecutor(storage.NewMemoryEngine())
	patternMatches := []struct {
		variable string
		nodes    []*storage.Node
	}{
		{variable: "o", nodes: []*storage.Node{
			{ID: "o-1", Properties: map[string]interface{}{"joinKey": "k1"}},
			{ID: "o-2", Properties: map[string]interface{}{}},
			{ID: "o-3", Properties: map[string]interface{}{"joinKey": nil}},
		}},
		{variable: "t", nodes: []*storage.Node{
			{ID: "t-1", Properties: map[string]interface{}{"joinKey": "k1"}},
			{ID: "t-2", Properties: map[string]interface{}{"joinKey": "k2"}},
		}},
	}

	filtered := exec.applyCartesianWherePushdown(context.Background(), patternMatches, "o.joinKey IS NOT NULL AND t.joinKey = o.joinKey")
	require.Len(t, filtered, 2)
	require.Len(t, filtered[0].nodes, 1)
	require.Equal(t, "o-1", string(filtered[0].nodes[0].ID))
	require.Len(t, filtered[1].nodes, 1)
	require.Equal(t, "t-1", string(filtered[1].nodes[0].ID))
	joined, ok := exec.buildCombinationsUsingWhereJoin(filtered, "o.joinKey IS NOT NULL AND t.joinKey = o.joinKey")
	require.True(t, ok)
	require.Len(t, joined, 1)
	require.Equal(t, "o-1", string(joined[0]["o"].ID))
	require.Equal(t, "t-1", string(joined[0]["t"].ID))
}

func TestCartesianWherePushdown_ContradictoryNullConstraint(t *testing.T) {
	exec := NewStorageExecutor(storage.NewMemoryEngine())
	patternMatches := []struct {
		variable string
		nodes    []*storage.Node
	}{
		{variable: "o", nodes: []*storage.Node{
			{ID: "o-1", Properties: map[string]interface{}{"joinKey": "k1"}},
			{ID: "o-2", Properties: map[string]interface{}{}},
		}},
		{variable: "t", nodes: []*storage.Node{
			{ID: "t-1", Properties: map[string]interface{}{"joinKey": "k1"}},
		}},
	}

	filtered := exec.applyCartesianWherePushdown(context.Background(), patternMatches, "o.joinKey IS NULL AND o.joinKey IS NOT NULL AND t.joinKey = o.joinKey")
	require.Len(t, filtered, 2)
	require.Len(t, filtered[0].nodes, 0)
	require.Len(t, filtered[1].nodes, 1)
}

func TestCartesianWherePushdown_SingleNodeComparisons(t *testing.T) {
	exec := NewStorageExecutor(storage.NewMemoryEngine())
	patternMatches := []struct {
		variable string
		nodes    []*storage.Node
	}{
		{variable: "a", nodes: []*storage.Node{
			{ID: "a-1", Properties: map[string]interface{}{"age": int64(20)}},
			{ID: "a-2", Properties: map[string]interface{}{"age": int64(40)}},
		}},
		{variable: "b", nodes: []*storage.Node{
			{ID: "b-1", Properties: map[string]interface{}{"age": int64(20)}},
			{ID: "b-2", Properties: map[string]interface{}{"age": int64(40)}},
		}},
	}

	filtered := exec.applyCartesianWherePushdown(context.Background(), patternMatches, "a.age >= 32 AND b.age < 30")
	require.Len(t, filtered[0].nodes, 1)
	require.Equal(t, storage.NodeID("a-2"), filtered[0].nodes[0].ID)
	require.Len(t, filtered[1].nodes, 1)
	require.Equal(t, storage.NodeID("b-1"), filtered[1].nodes[0].ID)
	require.Len(t, exec.buildCartesianProduct(filtered), 1)

	for _, clause := range []string{"a.age >= b.age", "size(a.age) > 1", "a.age >= $minimum"} {
		if _, ok := parseCartesianSingleNodeComparisonTerm(clause); ok {
			t.Fatalf("unexpected pushdown eligibility for %q", clause)
		}
	}
}

func TestMatchCreate_BatchJoinWithDualInFilters(t *testing.T) {
	// Wrap the in-memory engine in a NamespacedEngine so the executor's
	// transactionStorageWrapper sees a non-empty namespace and prefixes
	// IDs uniformly. Without the wrapper, freshly-minted edge UUIDs would
	// land in BadgerTransaction unprefixed and trip the per-tx namespace
	// pin (see ErrCrossNamespaceTransaction).
	store := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "nornic")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	for _, k := range []string{"k1", "k2", "k3"} {
		_, err := store.CreateNode(&storage.Node{
			ID:     storage.NodeID("o-" + k),
			Labels: []string{"OriginalText"},
			Properties: map[string]interface{}{
				"joinKey": k,
			},
		})
		require.NoError(t, err)
		_, err = store.CreateNode(&storage.Node{
			ID:     storage.NodeID("t-" + k),
			Labels: []string{"TranslatedText"},
			Properties: map[string]interface{}{
				"joinKey": k,
			},
		})
		require.NoError(t, err)
	}

	params := map[string]interface{}{"keys": []interface{}{"k1", "k2"}}
	res, err := exec.Execute(ctx, `
MATCH (o:OriginalText), (t:TranslatedText)
WHERE o.joinKey IN $keys
  AND t.joinKey IN $keys
  AND o.joinKey = t.joinKey
  AND NOT (o)-[:TRANSLATES_TO]->(t)
CREATE (o)-[:TRANSLATES_TO]->(t)
RETURN count(*) AS created_pairs
`, params)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.Len(t, res.Rows[0], 1)
	require.Equal(t, int64(2), res.Rows[0][0])

	res2, err := exec.Execute(ctx, `
MATCH (o:OriginalText), (t:TranslatedText)
WHERE o.joinKey IN $keys
  AND t.joinKey IN $keys
  AND o.joinKey = t.joinKey
  AND NOT (o)-[:TRANSLATES_TO]->(t)
CREATE (o)-[:TRANSLATES_TO]->(t)
RETURN count(*) AS created_pairs
`, params)
	require.NoError(t, err)
	require.Len(t, res2.Rows, 1)
	require.Len(t, res2.Rows[0], 1)
	require.Equal(t, int64(0), res2.Rows[0][0])
}

func TestBuildCombinationsUsingWhereJoin_TwoVarEquality(t *testing.T) {
	exec := NewStorageExecutor(storage.NewMemoryEngine())

	o1 := &storage.Node{ID: "nornic:o1", Labels: []string{"OriginalText"}, Properties: map[string]interface{}{"joinKey": "k1"}}
	o2 := &storage.Node{ID: "nornic:o2", Labels: []string{"OriginalText"}, Properties: map[string]interface{}{"joinKey": "k2"}}
	t1 := &storage.Node{ID: "nornic:t1", Labels: []string{"TranslatedText"}, Properties: map[string]interface{}{"joinKey": "k1"}}
	t2 := &storage.Node{ID: "nornic:t2", Labels: []string{"TranslatedText"}, Properties: map[string]interface{}{"joinKey": "k2"}}
	t3 := &storage.Node{ID: "nornic:t3", Labels: []string{"TranslatedText"}, Properties: map[string]interface{}{"joinKey": "k3"}}

	patternMatches := []struct {
		variable string
		nodes    []*storage.Node
	}{
		{variable: "o", nodes: []*storage.Node{o1, o2}},
		{variable: "t", nodes: []*storage.Node{t1, t2, t3}},
	}

	joined, ok := exec.buildCombinationsUsingWhereJoin(
		patternMatches,
		"o.joinKey IN ['k1','k2'] AND t.joinKey IN ['k1','k2'] AND o.joinKey = t.joinKey",
	)
	require.True(t, ok)
	require.Len(t, joined, 2)

	got := map[string]string{}
	for _, row := range joined {
		require.Contains(t, row, "o")
		require.Contains(t, row, "t")
		got[string(row["o"].ID)] = string(row["t"].ID)
	}
	require.Equal(t, "nornic:t1", got["nornic:o1"])
	require.Equal(t, "nornic:t2", got["nornic:o2"])
}

func TestBuildCombinationsUsingWhereJoin_ThreeVarEqualityChain(t *testing.T) {
	exec := NewStorageExecutor(storage.NewMemoryEngine())

	o1 := &storage.Node{ID: "nornic:o1", Properties: map[string]interface{}{"joinKey": "k1"}}
	o2 := &storage.Node{ID: "nornic:o2", Properties: map[string]interface{}{"joinKey": "k2"}}
	t1 := &storage.Node{ID: "nornic:t1", Properties: map[string]interface{}{"joinKey": "k1"}}
	t2 := &storage.Node{ID: "nornic:t2", Properties: map[string]interface{}{"joinKey": "k2"}}
	a1 := &storage.Node{ID: "nornic:a1", Properties: map[string]interface{}{"joinKey": "k1"}}
	a2 := &storage.Node{ID: "nornic:a2", Properties: map[string]interface{}{"joinKey": "k2"}}
	a3 := &storage.Node{ID: "nornic:a3", Properties: map[string]interface{}{"joinKey": "k3"}}

	patternMatches := []struct {
		variable string
		nodes    []*storage.Node
	}{
		{variable: "o", nodes: []*storage.Node{o1, o2}},
		{variable: "t", nodes: []*storage.Node{t1, t2}},
		{variable: "a", nodes: []*storage.Node{a1, a2, a3}},
	}

	joined, ok := exec.buildCombinationsUsingWhereJoin(
		patternMatches,
		"o.joinKey IN ['k1','k2'] AND t.joinKey = o.joinKey AND a.joinKey = t.joinKey",
	)
	require.True(t, ok)
	require.Len(t, joined, 2)
	for _, row := range joined {
		require.Contains(t, row, "o")
		require.Contains(t, row, "t")
		require.Contains(t, row, "a")
		ok := row["o"].Properties["joinKey"] == row["t"].Properties["joinKey"] &&
			row["t"].Properties["joinKey"] == row["a"].Properties["joinKey"]
		require.True(t, ok)
	}
}
