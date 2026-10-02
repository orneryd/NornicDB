package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestRowPredicateStringComparisonIsPlanned: a comparison of a simple operand
// with a string literal is planned (parsed once per text), whatever the
// literal contains, and the plan gives the result the text evaluation gives.
// Text with a quote in it that isn't such a comparison is not planned.
func TestRowPredicateStringComparisonIsPlanned(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "plan"))
	ctx := context.Background()
	node := &storage.Node{ID: "n1", Labels: []string{"P"}, Properties: map[string]interface{}{
		"id": "0a3da5e4-0000-4000-8000-000000000001", "name": "Ada", "at": "2026-10-02T00:00:00Z",
		"text": "a = b AND (c) OR [d] IS NULL", "quote": "it's", "n": int64(3),
	}}
	row := map[string]interface{}{"n": node, "name": "Ada", "$p": "Ada"}

	for _, predicate := range []string{
		"n.id = '0a3da5e4-0000-4000-8000-000000000001'",
		"n.id='0a3da5e4-0000-4000-8000-000000000001'",
		"n.id = '0a3da5e4-0000-4000-8000-000000000002'",
		"n.id <> 'x'",
		"n.id != 'x'",
		"'Ada' = n.name",
		"n.name = \"Ada\"",
		"n.name < 'B'",
		"n.name >= 'Ada'",
		"n.name > 'Ada'",
		"n.name <= 'A'",
		"name = 'Ada'",
		"$p = 'Ada'",
		"n.at = '2026-10-02T00:00:00Z'",
		"n.text = 'a = b AND (c) OR [d] IS NULL'",
		"n.text = 'a = b'",
		"n.quote = 'it\\'s'",
		"n.missing = 'x'",
		"n.n = '3'",
		"n.name = 'Ada' AND n.n = 3",
		"n.name = 'Bob' OR n.id = '0a3da5e4-0000-4000-8000-000000000001'",
		"(n.name = 'Ada')",
		"'a' = 'a'",
		"'a' < 'b'",
	} {
		plan := planRowPredicate(predicate)
		require.NotNil(t, plan, predicate)
		require.True(t, plan.complete, predicate)
		want := exec.evaluateRowPredicateText(ctx, predicate, row)
		require.Equal(t, want, exec.evaluateRowPredicatePlan(ctx, plan, row), predicate)
		require.Equal(t, want, exec.evaluateRowPredicate(ctx, predicate, row), predicate)
	}

	for _, predicate := range []string{
		"n.name STARTS WITH 'A'",
		"n.name CONTAINS 'd'",
		"n.name =~ 'A.*'",
		"n.name IN ['Ada']",
		"NOT n.name = 'Ada'",
		"toUpper(n.name) = 'ADA'",
		"n.name + 'x' = 'Adax'",
		"n.name = 'Ada' + ''",
		"n:P AND n.name = 'x'",
		"n.name = 'a' = true",
		"n.n - 1 = 'x'",
	} {
		plan := planRowPredicate(predicate)
		require.True(t, plan == nil || !plan.complete, predicate)
	}
}

// TestUnwindMatchWhereBuildsRowsOnlyForMatches: for UNWIND … MATCH (n:L)
// WHERE <predicate on n and the row>, with no index on the property, the
// predicate is tested per candidate node without building a row for it and
// without parsing its text again: allocations per candidate stay small as
// rows × nodes grows.
func TestUnwindMatchWhereBuildsRowsOnlyForMatches(t *testing.T) {
	ctx := context.Background()
	const query = "UNWIND $ids AS id MATCH (n:P) WHERE n.k = id RETURN n.v AS v"
	perCandidate := func(count int) float64 {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "where"))
		rows := make([]interface{}, 0, count)
		ids := make([]interface{}, 0, count)
		for i := 0; i < count; i++ {
			key := fmt.Sprintf("0a3da5e4-0000-4000-8000-%012d", i)
			ids = append(ids, key)
			rows = append(rows, map[string]interface{}{"k": key, "v": int64(i)})
		}
		_, err := exec.Execute(ctx, "UNWIND $rows AS row CREATE (n:P) SET n = row", map[string]interface{}{"rows": rows})
		require.NoError(t, err)
		// In a transaction the label scan allocates once per node visited
		// (auto-commit decodes each node), so what the predicate adds shows.
		_, err = exec.Execute(ctx, "BEGIN", nil)
		require.NoError(t, err)
		t.Cleanup(func() { _, _ = exec.Execute(ctx, "ROLLBACK", nil) })
		run := 0
		allocations := testing.AllocsPerRun(3, func() {
			run++
			result, err := exec.Execute(ctx, query, map[string]interface{}{"ids": ids, "run": run})
			if err != nil || len(result.Rows) != count {
				t.Fatalf("rows %v, err %v", result, err)
			}
		})
		return allocations / float64(count*count)
	}
	small, large := perCandidate(50), perCandidate(400)

	// A path variable is bound for the predicate and in the rows that pass.
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "where"))
	_, err := exec.Execute(ctx, "CREATE (:P {k: 'a', v: 1}), (:P {k: 'b', v: 2})", nil)
	require.NoError(t, err)
	matched, handled, err := exec.pipelineApplyInitialNodeMatch(ctx,
		[]pipelineRow{{"id": "b"}, {"id": "missing"}}, "MATCH p = (n:P) WHERE n.k = id AND p IS NOT NULL", pipelineMatchPhysicalHint{})
	require.NoError(t, err)
	require.True(t, handled)
	require.Len(t, matched, 1)
	require.Equal(t, int64(2), matched[0]["n"].(*storage.Node).Properties["v"])
	require.NotNil(t, matched[0]["p"])
	require.Equal(t, "b", matched[0]["id"])

	// A named path of one node matches as the node pattern does; its length
	// is 0 and it holds the node.
	for query, want := range map[string][][]interface{}{
		"MATCH p = (n:P) RETURN n.v AS v ORDER BY v":                                       {{int64(1)}, {int64(2)}},
		"MATCH p = (n:P) WHERE n.k = 'b' RETURN n.v AS v, length(p) AS l":                  {{int64(2), int64(0)}},
		"MATCH p = (n:P {k: 'b'}) RETURN n.v AS v, length(p) AS l":                         {{int64(2), int64(0)}},
		"UNWIND $ids AS id MATCH p = (n:P) WHERE n.k = id RETURN n.v AS v, length(p) AS l": {{int64(2), int64(0)}},
		"MATCH p = (n:P) RETURN length(p) AS l, size(nodes(p)) AS c ORDER BY n.v":          {{int64(0), int64(1)}, {int64(0), int64(1)}},
	} {
		result, err := exec.Execute(ctx, query, map[string]interface{}{"ids": []interface{}{"b", "missing"}})
		require.NoError(t, err, query)
		require.Equal(t, want, result.Rows, query)
	}

	// The scan allocates once per node visited; a row per candidate (a map)
	// and a re-parse of the predicate add three more each.
	require.Less(t, large, 2.0, "allocations per candidate at 50: %.2f, at 400: %.2f", small, large)
}
