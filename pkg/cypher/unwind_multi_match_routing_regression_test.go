package cypher

import (
	"context"
	"fmt"
	"regexp"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type limitedLabelScanEngine struct {
	*storage.MemoryEngine
	maxCalls int
	calls    int
}

func (e *limitedLabelScanEngine) GetNodesByLabel(label string) ([]*storage.Node, error) {
	e.calls++
	if e.maxCalls > 0 && e.calls > e.maxCalls {
		return nil, fmt.Errorf("GetNodesByLabel called too many times: %d > %d", e.calls, e.maxCalls)
	}
	return e.MemoryEngine.GetNodesByLabel(label)
}

// BUG REGRESSION:
// Queries that contain UNWIND and multiple MATCH clauses should be routed through
// UNWIND-aware execution, not the multi-MATCH cartesian handler.
func TestBug_UnwindWithMultipleMatchClauses_RoutesAndExecutes(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := store.CreateNode(&storage.Node{
		ID:     "test:o1",
		Labels: []string{"OriginalText"},
		Properties: map[string]interface{}{
			"joinKey": "k1",
		},
	})
	require.NoError(t, err)

	_, err = store.CreateNode(&storage.Node{
		ID:     "test:t1",
		Labels: []string{"TranslatedText"},
		Properties: map[string]interface{}{
			"joinKey": "k1",
		},
	})
	require.NoError(t, err)

	_, err = exec.Execute(ctx, `
MATCH (o:OriginalText)
UNWIND ['k1'] AS k
MATCH (t:TranslatedText)
WHERE o.joinKey = k AND t.joinKey = k
RETURN count(*) AS c
`, nil)
	require.NoError(t, err)
}

// BUG REGRESSION:
// Top-level UNWIND followed by multiple MATCH clauses should execute.
// Current behavior can misroute and return "expected multiple MATCH clauses".
func TestBug_TopLevelUnwindThenMultipleMatchClauses_Executes(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := store.CreateNode(&storage.Node{
		ID:     "test:o2",
		Labels: []string{"OriginalText"},
		Properties: map[string]interface{}{
			"joinKey": "k2",
		},
	})
	require.NoError(t, err)

	_, err = store.CreateNode(&storage.Node{
		ID:     "test:t2",
		Labels: []string{"TranslatedText"},
		Properties: map[string]interface{}{
			"joinKey": "k2",
		},
	})
	require.NoError(t, err)

	res, err := exec.Execute(ctx, `
UNWIND ['k2'] AS joinKey
MATCH (o:OriginalText)
WHERE o.joinKey = joinKey
MATCH (t:TranslatedText)
WHERE t.joinKey = joinKey
RETURN count(*) AS c
`, nil)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.Equal(t, int64(1), res.Rows[0][0])
}

func TestNormalizeMultiMatchWhereClauses(t *testing.T) {
	in := "MATCH (o:OriginalText) WHERE o.joinKey = joinKey MATCH (t:TranslatedText) WHERE t.joinKey = joinKey RETURN count(*) AS c"
	got := normalizeMultiMatchWhereClauses(in)
	require.Equal(
		t,
		"MATCH (o:OriginalText) MATCH (t:TranslatedText) WHERE o.joinKey = joinKey AND t.joinKey = joinKey RETURN count(*) AS c",
		got,
	)
}

func TestNormalizeMultiMatchWhereClauses_SingleWhereBetweenMatches(t *testing.T) {
	in := "MATCH (o:OriginalText) WHERE o.joinKey = joinKey MATCH (t:TranslatedText) RETURN count(*) AS c"
	got := normalizeMultiMatchWhereClauses(in)
	require.Equal(
		t,
		"MATCH (o:OriginalText) MATCH (t:TranslatedText) WHERE o.joinKey = joinKey RETURN count(*) AS c",
		got,
	)
}

func TestBug_MatchWhereMatchSetReturn_Executes(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := store.CreateNode(&storage.Node{
		ID:     "test:o1",
		Labels: []string{"OriginalText"},
		Properties: map[string]interface{}{
			"textKey128": "k128",
			"textKey":    "shortkey",
		},
	})
	require.NoError(t, err)

	_, err = store.CreateNode(&storage.Node{
		ID:     "test:t1",
		Labels: []string{"TranslatedText"},
		Properties: map[string]interface{}{
			"language":       "es",
			"translatedText": "old",
			"reviewResult":   "rejected",
			"isRefetch":      false,
			"submitter":      "existing@x.test",
			"createdAt":      "2026-01-01T00:00:00Z",
		},
	})
	require.NoError(t, err)

	err = store.CreateEdge(&storage.Edge{
		ID:        "test:e1",
		Type:      "TRANSLATES_TO",
		StartNode: "test:o1",
		EndNode:   "test:t1",
	})
	require.NoError(t, err)

	q := `
MATCH (o:OriginalText)
WHERE o.textKey128 = $textKey128 OR ($textKey IS NOT NULL AND o.textKey = $textKey)
MATCH (o)-[:TRANSLATES_TO]->(t:TranslatedText {language: $targetLang})
SET t.translatedText = $translatedText,
    t.submitter = coalesce(t.submitter, $submitter),
    t.isRefetch = CASE
        WHEN t.submitter IS NOT NULL
          AND coalesce(t.isRefetch, false) = false
          AND toLower(coalesce(t.reviewResult, '')) IN ['rejected', 'reject']
        THEN true
        ELSE t.isRefetch
    END
RETURN elementId(t) AS id,
       t.createdAt AS createdAt,
       t.language AS language,
       coalesce(t.translationId, elementId(t)) AS translationId,
       t.translatedText AS translatedText,
       t.auditedText AS auditedText,
       coalesce(t.isReviewed, false) AS isReviewed,
       t.reviewResult AS reviewResult,
       t.reviewedAt AS reviewedAt,
       t.submitter AS submitter,
       t.isRefetch AS isRefetch
`
	params := map[string]interface{}{
		"textKey128":     "k128",
		"textKey":        "shortkey",
		"targetLang":     "es",
		"translatedText": "new",
		"submitter":      "new@x.test",
	}
	res, err := exec.Execute(ctx, q, params)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.Len(t, res.Columns, 11)
	idx := func(col string) int {
		for i, c := range res.Columns {
			if c == col {
				return i
			}
		}
		return -1
	}
	require.Equal(t, "new", res.Rows[0][idx("translatedText")])
	require.Equal(t, "existing@x.test", res.Rows[0][idx("submitter")])
	require.Equal(t, true, res.Rows[0][idx("isRefetch")])
}

func TestBug_MatchWhereOrCreateCreateReturn_DoesNotFanOut(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Add many distractors to catch accidental broad scans/fan-out behavior.
	for i := 0; i < 500; i++ {
		_, err := store.CreateNode(&storage.Node{
			ID:     storage.NodeID(fmt.Sprintf("test:noise-%d", i)),
			Labels: []string{"OriginalText"},
			Properties: map[string]interface{}{
				"textKey128": fmt.Sprintf("noise-%d", i),
				"textKey":    "dupKey",
			},
		})
		require.NoError(t, err)
	}
	_, err := store.CreateNode(&storage.Node{
		ID:     "test:o-target",
		Labels: []string{"OriginalText"},
		Properties: map[string]interface{}{
			"textKey128": "k128-target",
			"textKey":    "shortkey",
		},
	})
	require.NoError(t, err)

	q := `
MATCH (o:OriginalText)
WHERE o.textKey128 = $textKey128 OR ($textKey IS NOT NULL AND o.textKey = $textKey)
CREATE (t:TranslatedText {
  language: $targetLang,
  translatedText: $translatedText,
  auditedText: null,
  isReviewed: false,
  reviewResult: null,
  reviewedAt: null,
  submitter: $submitter,
  isRefetch: $isRefetch,
  createdAt: $now
})
CREATE (o)-[:TRANSLATES_TO]->(t)
RETURN elementId(t) AS id,
       t.createdAt AS createdAt,
       t.language AS language,
       coalesce(t.translationId, elementId(t)) AS translationId,
       t.translatedText AS translatedText,
       t.auditedText AS auditedText,
       coalesce(t.isReviewed, false) AS isReviewed,
       t.reviewResult AS reviewResult,
       t.reviewedAt AS reviewedAt,
       t.submitter AS submitter,
       t.isRefetch AS isRefetch
`
	params := map[string]interface{}{
		"textKey128":     "k128-target",
		"textKey":        nil, // Keep second OR arm false.
		"targetLang":     "es",
		"translatedText": "hola",
		"submitter":      "s@x.test",
		"isRefetch":      false,
		"now":            "2026-01-01T00:00:00Z",
	}
	res, err := exec.Execute(ctx, q, params)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1, "query should create exactly one translation row")

	edges, err := store.AllEdges()
	require.NoError(t, err)
	translatesTo := 0
	for _, e := range edges {
		if e != nil && e.Type == "TRANSLATES_TO" {
			translatesTo++
		}
	}
	require.Equal(t, 1, translatesTo, "should create exactly one TRANSLATES_TO edge")
}

func TestBug_MatchWhereOrCreateCreateReturn_NullTextKeySkipsSecondArm(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Many rows match textKey arm; with textKey = null that arm must be disabled.
	for i := 0; i < 100; i++ {
		_, err := store.CreateNode(&storage.Node{
			ID:     storage.NodeID(fmt.Sprintf("test:bulk-%d", i)),
			Labels: []string{"OriginalText"},
			Properties: map[string]interface{}{
				"textKey128": fmt.Sprintf("bulk-%d", i),
				"textKey":    "shared",
			},
		})
		require.NoError(t, err)
	}
	_, err := store.CreateNode(&storage.Node{
		ID:     "test:o-target-null",
		Labels: []string{"OriginalText"},
		Properties: map[string]interface{}{
			"textKey128": "needle-128",
			"textKey":    "shared",
		},
	})
	require.NoError(t, err)

	q := `
MATCH (o:OriginalText)
WHERE o.textKey128 = $textKey128 OR ($textKey IS NOT NULL AND o.textKey = $textKey)
CREATE (t:TranslatedText {language: $targetLang, translatedText: $translatedText, createdAt: $now})
CREATE (o)-[:TRANSLATES_TO]->(t)
RETURN count(*) AS c
`
	res, err := exec.Execute(ctx, q, map[string]interface{}{
		"textKey128":     "needle-128",
		"textKey":        nil,
		"targetLang":     "es",
		"translatedText": "hola",
		"now":            "2026-01-01T00:00:00Z",
	})
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.Equal(t, int64(1), res.Rows[0][0], "second OR arm must not match when textKey is null")
}

func TestRewriteUnwindCorrelationToIn(t *testing.T) {
	in := "MATCH (o:OriginalText) MATCH (t:TranslatedText) WHERE o.joinKey = joinKey AND t.joinKey = joinKey CREATE (o)-[:TRANSLATES_TO]->(t) RETURN count(*) AS c"
	got, ok := rewriteUnwindCorrelationToIn(in, "joinKey", "__unwind_items")
	require.True(t, ok)
	require.True(t, regexp.MustCompile(`o\.joinKey\s+IN\s+\$__unwind_items`).MatchString(got))
	require.True(t, regexp.MustCompile(`t\.joinKey\s*=\s*o\.joinKey`).MatchString(got))
}

func TestRewriteTopLevelMultiMatchToCartesianMatch(t *testing.T) {
	in := "MATCH (o:OriginalText) MATCH (t:TranslatedText) WHERE o.joinKey IN $__unwind_items AND t.joinKey = o.joinKey RETURN count(*) AS c"
	got := rewriteTopLevelMultiMatchToCartesianMatch(in)
	require.Equal(
		t,
		"MATCH (o:OriginalText), (t:TranslatedText) WHERE o.joinKey IN $__unwind_items AND t.joinKey = o.joinKey RETURN count(*) AS c",
		got,
	)
}

func TestBug_UnwindMatchCount_RewriteAvoidsPerItemLabelScans(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	eng := &limitedLabelScanEngine{MemoryEngine: base, maxCalls: 3}
	exec := NewStorageExecutor(eng)
	ctx := context.Background()

	for i := 1; i <= 5; i++ {
		k := fmt.Sprintf("k%d", i)
		_, err := eng.CreateNode(&storage.Node{ID: storage.NodeID(fmt.Sprintf("nornic:o%d", i)), Labels: []string{"OriginalText"}, Properties: map[string]interface{}{"joinKey": k}})
		require.NoError(t, err)
		_, err = eng.CreateNode(&storage.Node{ID: storage.NodeID(fmt.Sprintf("nornic:t%d", i)), Labels: []string{"TranslatedText"}, Properties: map[string]interface{}{"joinKey": k}})
		require.NoError(t, err)
	}

	res, err := exec.Execute(ctx, `
	UNWIND ['k1','k2','k3','k4','k5'] AS joinKey
	MATCH (o:OriginalText)
	WHERE o.joinKey = joinKey
	MATCH (t:TranslatedText)
	WHERE t.joinKey = joinKey
	RETURN count(*) AS c
	`, nil)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.Len(t, res.Rows[0], 1)
	require.Equal(t, int64(5), res.Rows[0][0])
	require.LessOrEqual(t, eng.calls, 3)
}

func TestBug_UnwindMatchCreate_CorrelatedJoinKeepsCardinality(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	for i := 1; i <= 5; i++ {
		k := fmt.Sprintf("k%d", i)
		_, err := store.CreateNode(&storage.Node{ID: storage.NodeID(fmt.Sprintf("test:o%d", i)), Labels: []string{"OriginalText"}, Properties: map[string]interface{}{"joinKey": k}})
		require.NoError(t, err)
		_, err = store.CreateNode(&storage.Node{ID: storage.NodeID(fmt.Sprintf("test:t%d", i)), Labels: []string{"TranslatedText"}, Properties: map[string]interface{}{"joinKey": k}})
		require.NoError(t, err)
	}

	res, err := exec.Execute(ctx, `
UNWIND ['k1','k2','k3','k4','k5'] AS joinKey
MATCH (o:OriginalText)
WHERE o.joinKey = joinKey
MATCH (t:TranslatedText)
WHERE t.joinKey = joinKey AND NOT (o)-[:TRANSLATES_TO]->(t)
CREATE (o)-[:TRANSLATES_TO]->(t)
RETURN count(*) AS c
	`, nil)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.Len(t, res.Rows[0], 1)
	require.Equal(t, int64(5), res.Rows[0][0])

	edges, err := store.AllEdges()
	require.NoError(t, err)
	translatesTo := 0
	for _, edge := range edges {
		if edge != nil && edge.Type == "TRANSLATES_TO" {
			translatesTo++
		}
	}
	require.Equal(t, 5, translatesTo)

	res2, err := exec.Execute(ctx, `
UNWIND ['k1','k2','k3','k4','k5'] AS joinKey
MATCH (o:OriginalText)
WHERE o.joinKey = joinKey
MATCH (t:TranslatedText)
WHERE t.joinKey = joinKey AND NOT (o)-[:TRANSLATES_TO]->(t)
CREATE (o)-[:TRANSLATES_TO]->(t)
RETURN count(*) AS c
`, nil)
	require.NoError(t, err)
	require.Len(t, res2.Rows, 1)
	require.Len(t, res2.Rows[0], 1)
	require.Equal(t, int64(0), res2.Rows[0][0])
}
