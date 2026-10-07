package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestSplitWhereByVariable(t *testing.T) {
	own, rest := splitWhereByVariable("id(a) = 'x' AND a.name STARTS WITH 'N' AND toLower(b.k) = 'y' AND a.k = b.k AND $flag AND c.k = 1 AND (a.v = 1 OR a.v = 2) AND a.s = 'p->q' AND EXISTS { (a)-->() } AND (a)-->(b)", []string{"a", "b"})
	require.Equal(t, map[string]string{
		"a": "(id(a) = 'x') AND (a.name STARTS WITH 'N') AND ((a.v = 1 OR a.v = 2)) AND (a.s = 'p->q')",
		"b": "toLower(b.k) = 'y'",
	}, own)
	require.Equal(t, "(a.k = b.k) AND ($flag) AND (c.k = 1) AND (EXISTS { (a)-->() }) AND ((a)-->(b))", rest)

	own, rest = splitWhereByVariable("", []string{"a"})
	require.Empty(t, own)
	require.Empty(t, rest)
}

func TestHasPatternOrSubquery(t *testing.T) {
	for expression, want := range map[string]bool{
		"a.x = 1":                               false,
		"a.s = 'p->q'":                          false,
		"a.s = \"<-\"":                          false,
		"`a-b`.x = 1":                           false,
		"a.m = {k: 1}":                          false,
		"a.x - 1 > 0":                           false,
		"size(a.xs)-1 > 0":                      true, // errs towards a pattern
		"(a)-->(b)":                             true,
		"(a)<-[:R]-(b)":                         true,
		"(a) -- (b)":                            true,
		"EXISTS { MATCH (a) }":                  true,
		"count { (a)-[:R]->() } > 0":            true,
		"COLLECT { MATCH (a) RETURN a.x } = []": true,
	} {
		require.Equal(t, want, hasPatternOrSubquery(expression), expression)
	}
}

// TestCommaMatchNarrowsEachNode pins the rows of comma-separated MATCHes
// whose WHERE conjuncts read one node each (#940), which now select and
// filter that node's candidates before the combinations are built.
func TestCommaMatchNarrowsEachNode(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "UNWIND range(0, 9) AS i CREATE (:P {k: i, name: 'Name' + toString(i), v: CASE WHEN i % 2 = 0 THEN i END})", nil)
	require.NoError(t, err)
	result, err := exec.Execute(ctx, "MATCH (n:P) RETURN n.k AS k, id(n) AS id, elementId(n) AS eid ORDER BY k", nil)
	require.NoError(t, err)
	ids := make([]interface{}, len(result.Rows))
	eids := make([]interface{}, len(result.Rows))
	for i, row := range result.Rows {
		ids[i], eids[i] = row[1], row[2]
	}
	params := map[string]interface{}{"first": ids[0], "last": ids[9], "efirst": eids[0], "elast": eids[9], "some": []interface{}{ids[1], ids[2]}, "flag": true, "off": false}

	for query, want := range map[string][][]interface{}{
		"MATCH (a),(b) WHERE id(a) = $first AND id(b) = $last RETURN a.k, b.k":                              {{int64(0), int64(9)}},
		"MATCH (a:P),(b:P) WHERE elementId(a) = $efirst AND elementId(b) = $elast RETURN a.k, b.k":          {{int64(0), int64(9)}},
		"MATCH (a),(b) WHERE id(a) IN $some AND id(b) = $last RETURN a.k, b.k ORDER BY a.k":                 {{int64(1), int64(9)}, {int64(2), int64(9)}},
		"MATCH (a),(b) WHERE a.name STARTS WITH 'Name1' AND toLower(b.name) = 'name3' RETURN a.k, b.k":      {{int64(1), int64(3)}},
		"MATCH (a),(b) WHERE a.v IS NULL AND b.v IS NOT NULL AND a.k = 1 AND b.k > 7 RETURN a.k, b.k":       {{int64(1), int64(8)}},
		"MATCH (a),(b) WHERE a.v IS NULL AND a.v IS NOT NULL RETURN a.k, b.k":                               {},
		"MATCH (a),(b) WHERE a.k >= 8 AND b.k < 1 RETURN a.k, b.k ORDER BY a.k":                             {{int64(8), int64(0)}, {int64(9), int64(0)}},
		"MATCH (a),(b) WHERE (a.k = 1 OR a.k = 2) AND b.k = a.k + 1 RETURN a.k, b.k ORDER BY a.k":           {{int64(1), int64(2)}, {int64(2), int64(3)}},
		"MATCH (a),(b) WHERE a.k = 1 AND b.k = 2 AND $flag RETURN a.k, b.k":                                 {{int64(1), int64(2)}},
		"MATCH (a),(b) WHERE a.k = 1 AND b.k = 2 AND $off RETURN a.k, b.k":                                  {},
		"MATCH (a),(b) WHERE a.name <> 'x->y' AND a.k = 4 AND b.k = 5 RETURN a.k, b.k":                      {{int64(4), int64(5)}},
		"MATCH (a),(b),(c) WHERE id(a) = $first AND b.k = 1 AND c.k > 8 AND a.k < b.k RETURN a.k, b.k, c.k": {{int64(0), int64(1), int64(9)}},
		"MATCH (a),(b) WHERE id(a) = $first AND id(b) = $last RETURN count(*) AS c":                         {{int64(1)}},
		"MATCH (a),(b) WHERE a.k < 3 AND b.k < 2 RETURN count(*) AS c":                                      {{int64(6)}},
	} {
		result, err := exec.Execute(ctx, query, params)
		require.NoError(t, err, query)
		if len(want) == 0 {
			require.Empty(t, result.Rows, query)
			continue
		}
		require.Equal(t, want, result.Rows, query)
	}
}

// TestCommaMatchByIDIndependentOfNodeCount checks that MATCH (a),(b) WHERE
// id(a) = $x AND id(b) = $y no longer builds every pair (#940), and that an
// id or elementId IN list seeks, alone or among AND conditions: their
// allocations don't grow with the number of nodes.
func TestCommaMatchByIDIndependentOfNodeCount(t *testing.T) {
	for _, query := range []string{
		"MATCH (a),(b) WHERE id(a) = $a AND id(b) = $b RETURN a.k, b.k",
		"MATCH (a),(b) WHERE id(a) IN $as AND id(b) IN [$b] RETURN a.k, b.k",
		"MATCH (a),(b) WHERE elementId(a) IN $eas AND elementId(b) IN [$eb] AND b.k > 0 RETURN a.k, b.k",
		"MATCH (a) WHERE id(a) IN $as RETURN a.k, a.k AS again",
		"MATCH (a) WHERE id(a) IN $as AND a.k = 1 RETURN a.k, a.k AS again",
	} {
		allocations := func(nodes int) float64 {
			// No result cache: every run below executes the query.
			exec := NewStorageExecutorWithQueryCachePolicy(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"), 0, 0)
			ctx := context.Background()
			_, err := exec.Execute(ctx, fmt.Sprintf("UNWIND range(1, %d) AS i CREATE (:P {k: i})", nodes), nil)
			require.NoError(t, err)
			result, err := exec.Execute(ctx, fmt.Sprintf("MATCH (a:P {k: 1}), (b:P {k: %d}) RETURN id(a), id(b), elementId(a), elementId(b)", nodes), nil)
			require.NoError(t, err)
			row := result.Rows[0]
			params := map[string]interface{}{
				"a": row[0], "b": row[1], "as": []interface{}{row[0]},
				"eas": []interface{}{row[2]}, "eb": row[3],
			}
			return testing.AllocsPerRun(5, func() {
				result, err := exec.Execute(ctx, query, params)
				require.NoError(t, err)
				require.Len(t, result.Rows, 1)
				require.Equal(t, int64(1), result.Rows[0][0])
			})
		}
		small, large := allocations(20), allocations(400)
		require.Less(t, large, 2*small, "%s: allocations at 400 nodes: %.0f, at 20: %.0f", query, large, small)
	}
}

// TestCommaMatchAnonymousNodes pins Neo4j's rows for a comma-separated MATCH
// with an anonymous node: each of its matches is a combination, and none
// match means no rows.
func TestCommaMatchAnonymousNodes(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "UNWIND range(1, 10) AS i CREATE (:P {k: i})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (:Q {k: 1}), (:Q {k: 2})", nil)
	require.NoError(t, err)
	for query, want := range map[string][][]interface{}{
		"MATCH (a:P), () RETURN count(*) AS c":                        {{int64(120)}},
		"MATCH (), (b:Q) RETURN count(*) AS c":                        {{int64(24)}},
		"MATCH (), () RETURN count(*) AS c":                           {{int64(144)}},
		"MATCH (a:P), (:Q) RETURN count(*) AS c":                      {{int64(20)}},
		"MATCH (a:P), (:Q {k: 99}) RETURN count(*) AS c":              {{int64(0)}},
		"MATCH (a:P), (:Q) WHERE a.k <= 2 RETURN a.k AS k ORDER BY k": {{int64(1)}, {int64(1)}, {int64(2)}, {int64(2)}},
		"MATCH (a:P), (:Q), (:Q) WHERE a.k = 1 RETURN count(*) AS c":  {{int64(4)}},
		"MATCH (:Q), (:Q) RETURN count(*) AS c":                       {{int64(4)}},
		"MATCH (a:P), (:Q {k: 99}) RETURN a.k AS k":                   {},
	} {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		if len(want) == 0 {
			require.Empty(t, result.Rows, query)
			continue
		}
		require.Equal(t, want, result.Rows, query)
	}
}

// TestCommaMatchPropertyMapReadsEarlierPattern: a later pattern's property
// map may read a variable bound by an earlier pattern of the same MATCH, as in
// Neo4j, with or without a WHERE that joins the patterns (#907).
func TestCommaMatchPropertyMapReadsEarlierPattern(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:PDDocument {id:'issue-d'}) CREATE (:PDVersion {id:'issue-v', document_id:'issue-d', original_id:'issue-o'}) CREATE (:PDOriginal {id:'issue-o', content_hash:'original-bytes'})", nil)
	require.NoError(t, err)
	params := map[string]any{"document_id": "issue-d", "version_id": "issue-v", "review_id": "issue-r"}
	for _, query := range []string{
		"MATCH (d:PDDocument {id: $document_id}), (v:PDVersion {id: $version_id}), (o:PDOriginal {id: v.original_id}) WHERE v.document_id = d.id RETURN o.content_hash AS h",
		"MATCH (v:PDVersion {id: $version_id}), (o:PDOriginal {id: v.original_id}) RETURN o.content_hash AS h",
		"MATCH (d:PDDocument {id: $document_id}), (v:PDVersion {id: $version_id}), (o:PDOriginal {id: v.original_id}) WHERE v.document_id = d.id MERGE (r:PDReview {id: $review_id}) SET r.reviewed_original_hash = o.content_hash MERGE (r)-[:REVIEWED_DOCUMENT]->(d) RETURN r.reviewed_original_hash AS h",
	} {
		result, err := exec.Execute(ctx, query, params)
		require.NoError(t, err, query)
		require.Equal(t, [][]interface{}{{"original-bytes"}}, result.Rows, query)
	}
}
