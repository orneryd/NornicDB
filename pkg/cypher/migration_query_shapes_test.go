package cypher

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

const exactBoltCreateTranslatedQueryShape = `
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

const exactBoltUpdateTranslatedQueryShape = `
MATCH (o:OriginalText)
WHERE o.textKey128 = $textKey128 OR ($textKey IS NOT NULL AND o.textKey = $textKey)
MATCH (o)-[:TRANSLATES_TO]->(t:TranslatedText {language: $targetLang})
SET t.translatedText = $translatedText,
		t.submitter = coalesce(t.submitter, $submitter),
		t.isRefetch = CASE
				WHEN t.submitter IS NOT NULL AND coalesce(t.isRefetch, false) = false AND toLower(coalesce(t.reviewResult, '')) IN ['rejected', 'reject'] THEN true
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

func TestExactShape_MatchWhereOrCreateCreateReturn_SingleMatchByTextKey128(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := store.CreateNode(&storage.Node{
		ID:     "orig-single",
		Labels: []string{"OriginalText"},
		Properties: map[string]interface{}{
			"textKey128": "needle-128",
			"textKey":    "unique-key",
		},
	})
	require.NoError(t, err)

	// Distractors should not match either arm.
	for i := 0; i < 50; i++ {
		_, err := store.CreateNode(&storage.Node{
			ID:     storage.NodeID(fmt.Sprintf("orig-noise-%d", i)),
			Labels: []string{"OriginalText"},
			Properties: map[string]interface{}{
				"textKey128": fmt.Sprintf("noise-%d", i),
				"textKey":    fmt.Sprintf("other-%d", i),
			},
		})
		require.NoError(t, err)
	}

	params := map[string]interface{}{
		"textKey128":     "needle-128",
		"textKey":        "unique-key",
		"targetLang":     "es",
		"translatedText": "hola",
		"submitter":      "submitter@example.test",
		"isRefetch":      false,
		"now":            "2026-03-23T00:00:00Z",
	}

	res, err := exec.Execute(ctx, exactBoltCreateTranslatedQueryShape, params)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1, "exact query shape should create one translated node for one matched OriginalText")
	require.Len(t, res.Columns, 11)

	col := func(name string) int {
		for i, c := range res.Columns {
			if c == name {
				return i
			}
		}
		return -1
	}
	require.NotEqual(t, -1, col("id"))
	require.NotEqual(t, -1, col("createdAt"))
	require.NotEqual(t, -1, col("translationId"))
	require.Equal(t, "es", res.Rows[0][col("language")])
	require.Equal(t, "hola", res.Rows[0][col("translatedText")])
	require.Equal(t, "submitter@example.test", res.Rows[0][col("submitter")])
	require.Equal(t, false, res.Rows[0][col("isRefetch")])
}

func TestExactShape_MatchWhereMatchSetReturn_UpdatesExistingTranslationRow(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := store.CreateNode(&storage.Node{
		ID:     "orig-upd-1",
		Labels: []string{"OriginalText"},
		Properties: map[string]interface{}{
			"textKey128": "needle-upd-128",
			"textKey":    "needle-upd-key",
		},
	})
	require.NoError(t, err)

	_, err = store.CreateNode(&storage.Node{
		ID:     "tr-upd-1",
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
		ID:        "edge-upd-1",
		Type:      "TRANSLATES_TO",
		StartNode: "orig-upd-1",
		EndNode:   "tr-upd-1",
	})
	require.NoError(t, err)

	res, err := exec.Execute(ctx, exactBoltUpdateTranslatedQueryShape, map[string]interface{}{
		"textKey128":     "needle-upd-128",
		"textKey":        "needle-upd-key",
		"targetLang":     "es",
		"translatedText": "new-value",
		"submitter":      "new-submitter@x.test",
	})
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.Len(t, res.Columns, 11)

	col := func(name string) int {
		for i, c := range res.Columns {
			if c == name {
				return i
			}
		}
		return -1
	}
	require.Equal(t, "new-value", res.Rows[0][col("translatedText")])
	require.Equal(t, "existing@x.test", res.Rows[0][col("submitter")], "coalesce should preserve existing submitter")
	require.Equal(t, true, res.Rows[0][col("isRefetch")], "rejected + existing submitter should set isRefetch=true")
}

func TestExactShape_UpdateReviewByElementID_ReturnCountUpdated(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := store.CreateNode(&storage.Node{
		ID:     "tr-review-1",
		Labels: []string{"TranslatedText"},
		Properties: map[string]interface{}{
			"isReviewed":     false,
			"translatedText": "old",
		},
	})
	require.NoError(t, err)

	q := `
MATCH (t:TranslatedText)
WHERE elementId(t) = $translationTextId
SET t.isReviewed = true,
    t.auditedText = $auditedTranslatedText,
    t.reviewResult = $reviewResult,
    t.reviewedAt = $reviewedAt,
    t.reviewerFirstName = $reviewerFirstName,
    t.reviewerLastName = $reviewerLastName,
    t.reviewerEmail = $reviewerEmail,
    t.correctionReason = $correctionReason,
    t.reviewerComments = $reviewerComments,
    t.translatedText = CASE
      WHEN $reviewResult = 'rejected' AND $auditedTranslatedText IS NOT NULL AND trim($auditedTranslatedText) <> ''
      THEN $auditedTranslatedText
      ELSE t.translatedText
    END
RETURN count(t) AS updated
`

	res, err := exec.Execute(ctx, q, map[string]interface{}{
		"translationTextId":     "tr-review-1",
		"auditedTranslatedText": "new audited",
		"reviewResult":          "rejected",
		"reviewedAt":            "2026-03-23T00:00:00Z",
		"reviewerFirstName":     "A",
		"reviewerLastName":      "B",
		"reviewerEmail":         "ab@example.test",
		"correctionReason":      "tone",
		"reviewerComments":      "ok",
	})
	require.NoError(t, err)
	require.Equal(t, []string{"updated"}, res.Columns)
	require.Len(t, res.Rows, 1)
	require.Len(t, res.Rows[0], 1)
	require.Equal(t, int64(1), res.Rows[0][0])
}

func seedTranslationJoinNodes(t *testing.T, store storage.Engine) {
	t.Helper()
	nodes := []*storage.Node{
		{
			ID:     "orig-k1",
			Labels: []string{"OriginalText"},
			Properties: map[string]interface{}{
				"__tmpJoinKey": "k1",
			},
		},
		{
			ID:     "orig-k2",
			Labels: []string{"OriginalText"},
			Properties: map[string]interface{}{
				"__tmpJoinKey": "k2",
			},
		},
		{
			ID:     "orig-k3",
			Labels: []string{"OriginalText"},
			Properties: map[string]interface{}{
				"__tmpJoinKey": "k3",
			},
		},
		{
			ID:     "tr-k1",
			Labels: []string{"TranslatedText"},
			Properties: map[string]interface{}{
				"__tmpJoinKey": "k1",
			},
		},
		{
			ID:     "tr-k2",
			Labels: []string{"TranslatedText"},
			Properties: map[string]interface{}{
				"__tmpJoinKey": "k2",
			},
		},
		{
			ID:     "tr-k3",
			Labels: []string{"TranslatedText"},
			Properties: map[string]interface{}{
				"__tmpJoinKey": "k3",
			},
		},
	}

	for _, n := range nodes {
		_, err := store.CreateNode(n)
		require.NoError(t, err)
	}
}

func TestMigrationShape_UnwindMatchMerge_ReturnCountAggregates(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	seedTranslationJoinNodes(t, store)

	result, err := exec.Execute(ctx, `
UNWIND $keys AS k
MATCH (o:OriginalText {__tmpJoinKey: k})
MATCH (t:TranslatedText {__tmpJoinKey: k})
MERGE (o)-[:TRANSLATES_TO]->(t)
RETURN count(*) AS merged_pairs
`, map[string]interface{}{
		"keys": []interface{}{"k1", "k2"},
	})
	require.NoError(t, err)
	require.Len(t, result.Rows, 1, "count(*) should aggregate to one row")
	require.Len(t, result.Rows[0], 1)
	require.Equal(t, int64(2), result.Rows[0][0])

	verify, err := exec.Execute(
		ctx,
		"MATCH (:OriginalText)-[r:TRANSLATES_TO]->(:TranslatedText) RETURN count(r) AS c",
		nil,
	)
	require.NoError(t, err)
	require.Len(t, verify.Rows, 1)
	require.Equal(t, int64(2), verify.Rows[0][0])
}

func TestMigrationShape_UnwindMatchCreate_CreatesBatchEdges(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	seedTranslationJoinNodes(t, store)

	_, err := exec.Execute(ctx, `
UNWIND $keys AS k
MATCH (o:OriginalText {__tmpJoinKey: k})
MATCH (t:TranslatedText {__tmpJoinKey: k})
CREATE (o)-[:TRANSLATES_TO]->(t)
`, map[string]interface{}{"keys": []interface{}{"k1", "k2"}})
	require.NoError(t, err)

	verify, err := exec.Execute(
		ctx,
		"MATCH (:OriginalText)-[r:TRANSLATES_TO]->(:TranslatedText) RETURN count(r) AS c",
		nil,
	)
	require.NoError(t, err)
	require.Len(t, verify.Rows, 1)
	require.Equal(t, int64(2), verify.Rows[0][0])
}

func TestMigrationShape_UnwindMatchRemoveProperty_Batched(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	seedTranslationJoinNodes(t, store)

	// Baseline: direct REMOVE with property map should work.
	_, err := exec.Execute(ctx, "MATCH (n:OriginalText {__tmpJoinKey: 'k1'}) REMOVE n.__tmpJoinKey", nil)
	require.NoError(t, err)

	beforeBatch, err := exec.Execute(ctx, `
MATCH (n:OriginalText)
WHERE n.__tmpJoinKey IS NOT NULL
RETURN count(n) AS c
`, nil)
	require.NoError(t, err)
	require.Len(t, beforeBatch.Rows, 1)
	require.Equal(t, int64(2), beforeBatch.Rows[0][0], "k1 should already be removed")

	_, err = exec.Execute(ctx, `
UNWIND $keys AS k
MATCH (n:OriginalText {__tmpJoinKey: k})
REMOVE n.__tmpJoinKey
`, map[string]interface{}{
		"keys": []interface{}{"k2", "k3"},
	})
	require.NoError(t, err)

	remaining, err := exec.Execute(ctx, `
MATCH (n:OriginalText)
WHERE n.__tmpJoinKey IS NOT NULL
RETURN count(n) AS c
	`, nil)
	require.NoError(t, err)
	require.Len(t, remaining.Rows, 1)
	require.Equal(t, int64(0), remaining.Rows[0][0], "all keys should be removed")
}

func TestMigrationShape_MatchWhereInRemoveProperty_Batched(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	seedTranslationJoinNodes(t, store)

	_, err := exec.Execute(ctx, `
MATCH (n:OriginalText)
WHERE n.__tmpJoinKey IN $keys
REMOVE n.__tmpJoinKey
`, map[string]interface{}{"keys": []interface{}{"k1", "k2", "k3"}})
	require.NoError(t, err)

	remaining, err := exec.Execute(ctx, `
MATCH (n:OriginalText)
WHERE n.__tmpJoinKey IS NOT NULL
RETURN count(n) AS c
	`, nil)
	require.NoError(t, err)
	require.Len(t, remaining.Rows, 1)
	require.Equal(t, int64(0), remaining.Rows[0][0], "all keys should be removed")
}

func TestMigrationShape_MatchInCreate_ReturnCountAlias(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	seedTranslationJoinNodes(t, store)

	res, err := exec.Execute(ctx, `
MATCH (o:OriginalText), (t:TranslatedText)
WHERE o.__tmpJoinKey IN $keys
  AND t.__tmpJoinKey = o.__tmpJoinKey
CREATE (o)-[:TRANSLATES_TO]->(t)
RETURN count(*) AS created_pairs
`, map[string]interface{}{"keys": []interface{}{"k1", "k2"}})
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.Len(t, res.Rows[0], 1)
	require.Equal(t, int64(2), res.Rows[0][0])
}

func TestMigrationShape_MatchInCreate_WithNotRelationshipModifier(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	seedTranslationJoinNodes(t, store)

	first, err := exec.Execute(ctx, `
MATCH (o:OriginalText), (t:TranslatedText)
WHERE o.__tmpJoinKey IN $keys
  AND t.__tmpJoinKey = o.__tmpJoinKey
  AND NOT (o)-[:TRANSLATES_TO]->(t)
CREATE (o)-[:TRANSLATES_TO]->(t)
RETURN count(*) AS created_pairs
`, map[string]interface{}{"keys": []interface{}{"k1", "k2"}})
	require.NoError(t, err)
	require.Len(t, first.Rows, 1)
	require.Equal(t, int64(2), first.Rows[0][0])

	second, err := exec.Execute(ctx, `
MATCH (o:OriginalText), (t:TranslatedText)
WHERE o.__tmpJoinKey IN $keys
  AND t.__tmpJoinKey = o.__tmpJoinKey
  AND NOT (o)-[:TRANSLATES_TO]->(t)
CREATE (o)-[:TRANSLATES_TO]->(t)
RETURN count(*) AS created_pairs
`, map[string]interface{}{"keys": []interface{}{"k1", "k2"}})
	require.NoError(t, err)
	require.Len(t, second.Rows, 1)
	require.Equal(t, int64(0), second.Rows[0][0])
}

func TestMigrationShape_MatchInMerge_ReturnCountAlias(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	seedTranslationJoinNodes(t, store)

	res, err := exec.Execute(ctx, `
MATCH (o:OriginalText), (t:TranslatedText)
WHERE o.__tmpJoinKey IN $keys
  AND t.__tmpJoinKey = o.__tmpJoinKey
MERGE (o)-[:TRANSLATES_TO]->(t)
RETURN count(*) AS matched_pairs
`, map[string]interface{}{"keys": []interface{}{"k1", "k2"}})
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.Len(t, res.Rows[0], 1)
	require.Equal(t, int64(2), res.Rows[0][0])
}

func TestMigrationDDL_CreateIndexVariants_ParseAndApply(t *testing.T) {
	cases := []struct {
		name  string
		query string
	}{
		{
			name:  "named_if_not_exists",
			query: "CREATE INDEX original_tmp_join_idx IF NOT EXISTS FOR (o:OriginalText) ON (o.__tmpJoinKey)",
		},
		{
			name:  "unnamed_if_not_exists",
			query: "CREATE INDEX IF NOT EXISTS FOR (t:TranslatedText) ON (t.__tmpJoinKey)",
		},
		{
			name:  "named_plain",
			query: "CREATE INDEX translated_tmp_join_idx FOR (t:TranslatedText) ON (t.__tmpJoinKey)",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			baseStore := newTestMemoryEngine(t)
			store := storage.NewNamespacedEngine(baseStore, "test")
			exec := NewStorageExecutor(store)
			ctx := context.Background()

			_, err := exec.Execute(ctx, tc.query, nil)
			require.NoError(t, err, tc.query)

			showRes, err := exec.Execute(ctx, "SHOW INDEXES", nil)
			require.NoError(t, err)
			require.NotNil(t, showRes)
			require.NotEmpty(t, showRes.Rows)
		})
	}
}

func TestExactShape_UnwindOptionalMatchCollectCaseMap_ReturnsPerKeyRows(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := store.CreateNode(&storage.Node{ID: "o-k1", Labels: []string{"OriginalText"}, Properties: map[string]interface{}{"textKey": "k1", "textKey128": "h1", "trackingId": "trk1", "page": "p1"}})
	require.NoError(t, err)
	_, err = store.CreateNode(&storage.Node{ID: "o-k2", Labels: []string{"OriginalText"}, Properties: map[string]interface{}{"textKey": "k2", "textKey128": "h2", "trackingId": "trk2", "page": "p2"}})
	require.NoError(t, err)
	_, err = store.CreateNode(&storage.Node{ID: "t-k1-es", Labels: []string{"TranslatedText"}, Properties: map[string]interface{}{"createdAt": "2026-03-23T00:00:00Z", "language": "es", "translatedText": "hola", "isReviewed": false}})
	require.NoError(t, err)
	err = store.CreateEdge(&storage.Edge{ID: "e-k1", Type: "TRANSLATES_TO", StartNode: "o-k1", EndNode: "t-k1-es"})
	require.NoError(t, err)

	q := `
UNWIND $keys AS key
MATCH (o:OriginalText)
WHERE o.textKey = key OR o.textKey128 = key
OPTIONAL MATCH (o)-[:TRANSLATES_TO]->(t:TranslatedText {language: $targetLang})
RETURN key AS lookupKey,
       elementId(o) AS originalId,
       o.textKey AS textKey,
       o.textKey128 AS textKey128,
       o.trackingId AS trackingId,
       o.page AS page,
       collect(CASE WHEN t IS NULL THEN null ELSE {
           id: elementId(t),
           createdAt: t.createdAt,
           language: t.language,
           translationId: coalesce(t.translationId, elementId(t)),
           translatedText: t.translatedText,
           auditedText: t.auditedText,
           isReviewed: coalesce(t.isReviewed, false),
           reviewResult: t.reviewResult,
           reviewedAt: t.reviewedAt,
           submitter: t.submitter,
           isRefetch: t.isRefetch
       }) AS texts
`

	// Sanity check the non-UNWIND shape for a key with no translation row.
	noUnwindQ := `
MATCH (o:OriginalText)
WHERE o.textKey = 'k2' OR o.textKey128 = 'k2'
OPTIONAL MATCH (o)-[:TRANSLATES_TO]->(t:TranslatedText {language: 'es'})
RETURN o.textKey AS textKey,
       collect(CASE WHEN t IS NULL THEN null ELSE { language: t.language } END) AS texts
`
	noUnwindRes, noUnwindErr := exec.Execute(ctx, noUnwindQ, nil)
	require.NoError(t, noUnwindErr)
	require.Len(t, noUnwindRes.Rows, 1)
	noUnwindQK1 := `
MATCH (o:OriginalText)
WHERE o.textKey = 'k1' OR o.textKey128 = 'k1'
OPTIONAL MATCH (o)-[:TRANSLATES_TO]->(t:TranslatedText {language: $targetLang})
RETURN o.textKey AS textKey,
       collect(CASE WHEN t IS NULL THEN null ELSE { language: t.language } END) AS texts
`
	noUnwindResK1, noUnwindErrK1 := exec.Execute(ctx, noUnwindQK1, map[string]interface{}{"targetLang": "es"})
	require.NoError(t, noUnwindErrK1)
	require.Len(t, noUnwindResK1.Rows, 1)
	noUnwindQK2 := `
MATCH (o:OriginalText)
WHERE o.textKey = 'k2' OR o.textKey128 = 'k2'
OPTIONAL MATCH (o)-[:TRANSLATES_TO]->(t:TranslatedText {language: $targetLang})
RETURN o.textKey AS textKey,
       collect(CASE WHEN t IS NULL THEN null ELSE { language: t.language } END) AS texts
`
	noUnwindResK2, noUnwindErrK2 := exec.Execute(ctx, noUnwindQK2, map[string]interface{}{"targetLang": "es"})
	require.NoError(t, noUnwindErrK2)
	require.Len(t, noUnwindResK2.Rows, 1)

	res, err := exec.Execute(ctx, q, map[string]interface{}{"keys": []interface{}{"k1", "k2"}, "targetLang": "es"})
	require.NoError(t, err)
	require.Len(t, res.Rows, 2, "must return one grouped row per key")

	col := func(name string) int {
		for i, c := range res.Columns {
			if c == name {
				return i
			}
		}
		return -1
	}
	lookupKeyIdx := col("lookupKey")
	textsIdx := col("texts")
	require.NotEqual(t, -1, lookupKeyIdx)
	require.NotEqual(t, -1, textsIdx)

	rowsByKey := map[string][]interface{}{}
	for _, row := range res.Rows {
		key, _ := row[lookupKeyIdx].(string)
		rowsByKey[key] = row
	}
	require.Contains(t, rowsByKey, "k1")
	require.Contains(t, rowsByKey, "k2")

	k1Texts, _ := rowsByKey["k1"][textsIdx].([]interface{})
	require.Len(t, k1Texts, 1, "k1 should include one translatedText map")
	k2Texts, _ := rowsByKey["k2"][textsIdx].([]interface{})
	require.Len(t, k2Texts, 0, "k2 should produce empty collect list for OPTIONAL MATCH miss")
}

func TestMigrationShape_UnwindMatchMergeSetMap_ComplexRowsAndClauses(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `
CREATE (f1:File {path: '/repo/a.py'})
CREATE (f2:File {path: '/repo/b.py'})
`, nil)
	require.NoError(t, err)

	rows := []map[string]interface{}{
		{
			"file_path":   "/repo/a.py",
			"line_number": int64(10),
			"name":        "alpha",
			"props": map[string]interface{}{
				"lang":             "python",
				"context":          "_find_classes",
				"class_context":    "TypescriptTreeSitterParser",
				"is_dependency":    false,
				"text_with_braces": "payload {'a':1, 'b':{'c':[1,2,3]}} with (parentheses) should stay literal",
			},
		},
		{
			"file_path":   "/repo/a.py",
			"line_number": int64(11),
			"name":        "beta",
			"props": map[string]interface{}{
				"lang":          "python",
				"context":       "_find_classes",
				"is_dependency": true,
			},
		},
		{
			"file_path":   "/repo/b.py",
			"line_number": int64(3),
			"name":        "gamma",
			"props": map[string]interface{}{
				"lang":               "typescript",
				"context":            "walk (ast)",
				"is_dependency":      false,
				"parenthetical_text": "example(value) and nested(call(arg))",
			},
		},
	}

	shape := `
UNWIND $rows AS row
MATCH (f:File {path: row.file_path})
MERGE (n:Variable {name: row.name, path: row.file_path, line_number: row.line_number})
SET n += row.props
MERGE (f)-[:CONTAINS]->(n)
WITH f, n
RETURN f.path AS file_path, n.name AS variable_name, n.line_number AS line_number, n.lang AS lang
`
	result, err := exec.Execute(ctx, shape, map[string]interface{}{"rows": rows})
	require.NoError(t, err)
	require.Equal(t, []string{"file_path", "variable_name", "line_number", "lang"}, result.Columns)
	require.Len(t, result.Rows, 3)

	ordered, err := exec.Execute(ctx, `
MATCH (f:File)-[:CONTAINS]->(n:Variable)
RETURN f.path AS file_path, n.name AS variable_name, n.line_number AS line_number, n.lang AS lang
ORDER BY file_path, line_number, variable_name
`, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"file_path", "variable_name", "line_number", "lang"}, ordered.Columns)
	require.Len(t, ordered.Rows, 3)
	require.Equal(t, []interface{}{"/repo/a.py", "alpha", int64(10), "python"}, ordered.Rows[0])
	require.Equal(t, []interface{}{"/repo/a.py", "beta", int64(11), "python"}, ordered.Rows[1])
	require.Equal(t, []interface{}{"/repo/b.py", "gamma", int64(3), "typescript"}, ordered.Rows[2])

	verifyParens, err := exec.Execute(ctx, `
MATCH (n:Variable {name: 'gamma'})
RETURN n.context AS context, n.parenthetical_text AS parenthetical_text
`, nil)
	require.NoError(t, err)
	require.Len(t, verifyParens.Rows, 1)
	require.Equal(t, "walk (ast)", verifyParens.Rows[0][0])
	require.Equal(t, "example(value) and nested(call(arg))", verifyParens.Rows[0][1])

	counts, err := exec.Execute(ctx, `
MATCH (f:File)-[r:CONTAINS]->(n:Variable)
RETURN count(f) AS files_joined, count(r) AS contains_edges, count(n) AS variable_nodes
`, nil)
	require.NoError(t, err)
	require.Len(t, counts.Rows, 1)
	require.Equal(t, int64(3), counts.Rows[0][0])
	require.Equal(t, int64(3), counts.Rows[0][1])
	require.Equal(t, int64(3), counts.Rows[0][2])
}

func TestMigrationShape_UnwindMatchMergeSetMap_IdempotentAndPaginatedProjection(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `
CREATE (f:File {path: '/repo/a.py'})
`, nil)
	require.NoError(t, err)

	rows := []map[string]interface{}{
		{
			"file_path":   "/repo/a.py",
			"line_number": int64(10),
			"name":        "alpha",
			"props": map[string]interface{}{
				"lang":  "python",
				"notes": "alpha(value)",
			},
		},
		{
			"file_path":   "/repo/a.py",
			"line_number": int64(20),
			"name":        "beta",
			"props": map[string]interface{}{
				"lang":  "python",
				"notes": "beta(value)",
			},
		},
		{
			"file_path":   "/repo/a.py",
			"line_number": int64(30),
			"name":        "gamma",
			"props": map[string]interface{}{
				"lang":  "python",
				"notes": "gamma(value)",
			},
		},
	}

	query := `
UNWIND $rows AS row
MATCH (f:File {path: row.file_path})
MERGE (n:Variable {name: row.name, path: row.file_path, line_number: row.line_number})
SET n += row.props
MERGE (f)-[:CONTAINS]->(n)
RETURN count(*) AS processed_rows
`
	first, err := exec.Execute(ctx, query, map[string]interface{}{"rows": rows})
	require.NoError(t, err)
	require.Len(t, first.Rows, 1)
	require.Equal(t, int64(3), first.Rows[0][0])

	second, err := exec.Execute(ctx, query, map[string]interface{}{"rows": rows})
	require.NoError(t, err)
	require.Len(t, second.Rows, 1)
	require.Equal(t, int64(3), second.Rows[0][0])

	edgeCount, err := exec.Execute(ctx, `
MATCH (:File)-[r:CONTAINS]->(:Variable)
RETURN count(r) AS edge_count
`, nil)
	require.NoError(t, err)
	require.Len(t, edgeCount.Rows, 1)
	require.Equal(t, int64(3), edgeCount.Rows[0][0], "MERGE should keep edge creation idempotent")

	paginated, err := exec.Execute(ctx, `
MATCH (f:File)-[:CONTAINS]->(n:Variable)
WITH f.path AS file_path, n.name AS variable_name, n.line_number AS line_number
RETURN file_path, variable_name, line_number
ORDER BY line_number ASC, variable_name ASC
SKIP 1
LIMIT 1
`, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"file_path", "variable_name", "line_number"}, paginated.Columns)
	require.Len(t, paginated.Rows, 1)
	require.Equal(t, []interface{}{"/repo/a.py", "beta", int64(20)}, paginated.Rows[0])

	notes, err := exec.Execute(ctx, `
MATCH (n:Variable)
WHERE n.notes = 'beta(value)'
RETURN count(n) AS c
`, nil)
	require.NoError(t, err)
	require.Len(t, notes.Rows, 1)
	require.Equal(t, int64(1), notes.Rows[0][0])
}

func TestExactShape_ListReviewTranslations_WithPagePathFilter_ReturnsExpectedRow(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := store.CreateNode(&storage.Node{
		ID:     "orig-list-1",
		Labels: []string{"OriginalText"},
		Properties: map[string]interface{}{
			"textKey":    "review-key-1",
			"textKey128": "review-key-1-128",
			"pagePath":   "/benefits/financial-summary",
			"page":       "https://example.test/benefits/financial-summary",
			"trackingId": "trk-list-1",
		},
	})
	require.NoError(t, err)

	_, err = store.CreateNode(&storage.Node{
		ID:     "tr-list-1",
		Labels: []string{"TranslatedText"},
		Properties: map[string]interface{}{
			"language":       "es",
			"isReviewed":     false,
			"translatedText": "hola",
			"createdAt":      "2026-03-24T00:00:00Z",
		},
	})
	require.NoError(t, err)

	err = store.CreateEdge(&storage.Edge{
		ID:        "edge-list-1",
		Type:      "TRANSLATES_TO",
		StartNode: "orig-list-1",
		EndNode:   "tr-list-1",
	})
	require.NoError(t, err)

	stageA, err := exec.Execute(ctx, `
MATCH (t:TranslatedText)
WHERE t.language = $language AND t.isReviewed = false
RETURN count(t) AS c
`, map[string]interface{}{"language": "es"})
	require.NoError(t, err)
	require.Len(t, stageA.Rows, 1)
	require.Equal(t, int64(1), stageA.Rows[0][0])

	stageB, err := exec.Execute(ctx, `
MATCH (o:OriginalText)-[:TRANSLATES_TO]->(t:TranslatedText)
WHERE o.pagePath STARTS WITH $pagePath
RETURN count(t) AS c
`, map[string]interface{}{"pagePath": "/benefits"})
	require.NoError(t, err)
	require.Len(t, stageB.Rows, 1)
	require.Equal(t, int64(1), stageB.Rows[0][0])

	stageC, err := exec.Execute(ctx, `
MATCH (t:TranslatedText)
WHERE t.language = $language AND t.isReviewed = false
MATCH (o:OriginalText)-[:TRANSLATES_TO]->(t)
RETURN count(t) AS c
`, map[string]interface{}{"language": "es"})
	require.NoError(t, err)
	require.Len(t, stageC.Rows, 1)
	require.Equal(t, int64(1), stageC.Rows[0][0])

	stageC2, err := exec.Execute(ctx, `
MATCH (t:TranslatedText)
WHERE t.language = $language AND t.isReviewed = false
MATCH (o:OriginalText)-[:TRANSLATES_TO]->(t)
RETURN count(o) AS c
`, map[string]interface{}{"language": "es"})
	require.NoError(t, err)
	require.Len(t, stageC2.Rows, 1)
	require.Equal(t, int64(1), stageC2.Rows[0][0], "chained relationship match must bind start variable o")

	stageD, err := exec.Execute(ctx, `
MATCH (t:TranslatedText)
WHERE t.language = $language AND t.isReviewed = false
MATCH (o:OriginalText)-[:TRANSLATES_TO]->(t)
WHERE o.pagePath STARTS WITH $pagePath
RETURN count(t) AS c
`, map[string]interface{}{"language": "es", "pagePath": "/benefits"})
	t.Logf("normalized stageD: %s", normalizeMultiMatchWhereClauses(`
MATCH (t:TranslatedText)
WHERE t.language = $language AND t.isReviewed = false
MATCH (o:OriginalText)-[:TRANSLATES_TO]->(t)
WHERE o.pagePath STARTS WITH $pagePath
RETURN count(t) AS c
`))
	nodeO, _ := store.GetNode("orig-list-1")
	nodeT, _ := store.GetNode("tr-list-1")
	require.NotNil(t, nodeO)
	require.NotNil(t, nodeT)
	require.True(t, exec.evaluateBindingWhere(ctx, binding{
		"o": nodeO,
		"t": nodeT,
	}, "t.language = $language AND t.isReviewed = false AND o.pagePath STARTS WITH $pagePath", map[string]interface{}{"language": "es", "pagePath": "/benefits"}))
	require.True(t, exec.evaluateBindingWhere(ctx, binding{
		"o": nodeO,
		"t": nodeT,
	}, "t.language = 'es' AND t.isReviewed = false AND o.pagePath STARTS WITH '/benefits'", nil))
	normalized := normalizeMultiMatchWhereClauses(`
MATCH (t:TranslatedText)
WHERE t.language = $language AND t.isReviewed = false
MATCH (o:OriginalText)-[:TRANSLATES_TO]->(t)
WHERE o.pagePath STARTS WITH $pagePath
RETURN count(t) AS c
`)
	returnIdx := findKeywordIndex(normalized, "RETURN")
	whereIdx := lastKeywordIndexBefore(normalized, "WHERE", returnIdx)
	matchClauses := splitMatchClauses(normalized, whereIdx, returnIdx)
	require.Len(t, matchClauses, 2)
	bindings := exec.executeFirstMatch(context.Background(), matchClauses[0])
	require.NotEmpty(t, bindings)
	bindings = exec.executeChainedMatch(context.Background(), matchClauses[1], bindings)
	require.NotEmpty(t, bindings)
	require.NotNil(t, bindings[0]["o"], "chained match must bind o variable")
	require.NotNil(t, bindings[0]["t"], "chained match must preserve t variable")
	rawWhere := strings.TrimSpace(normalized[whereIdx+5 : returnIdx])
	filteredBindings := exec.filterBindingsByWhere(ctx, bindings, rawWhere, map[string]interface{}{"language": "es", "pagePath": "/benefits"})
	require.NotEmpty(t, filteredBindings, "combined where predicate should keep matching binding")
	directCtx := context.WithValue(ctx, paramsKey, map[string]interface{}{"language": "es", "pagePath": "/benefits"})
	directRes, directErr := exec.executeMultiMatch(directCtx, normalized)
	require.NoError(t, directErr)
	require.NotNil(t, directRes)
	require.Len(t, directRes.Rows, 1, "direct executeMultiMatch should preserve matching row")
	require.Equal(t, int64(1), directRes.Rows[0][0])
	substituted := exec.substituteParams(normalized, map[string]interface{}{"language": "es", "pagePath": "/benefits"})
	directSubRes, directSubErr := exec.executeMultiMatch(ctx, substituted)
	require.NoError(t, directSubErr)
	require.NotNil(t, directSubRes)
	require.Len(t, directSubRes.Rows, 1, "executeMultiMatch with pre-substituted query should preserve matching row")
	require.Equal(t, int64(1), directSubRes.Rows[0][0])
	require.Equal(t, 2, countKeywordOccurrences(strings.ToUpper(substituted), "MATCH"))
	require.Equal(t, 0, countKeywordOccurrences(strings.ToUpper(substituted), "OPTIONAL MATCH"))
	routeRes, routeErr := exec.executeWithoutTransaction(directCtx, substituted, strings.ToUpper(strings.TrimSpace(substituted)))
	require.NoError(t, routeErr)
	require.NotNil(t, routeRes)
	require.Len(t, routeRes.Rows, 1)
	viaMatchRes, viaMatchErr := exec.executeMatch(directCtx, substituted)
	require.NoError(t, viaMatchErr)
	require.NotNil(t, viaMatchRes)
	require.Len(t, viaMatchRes.Rows, 1)
	require.Equal(t, int64(1), viaMatchRes.Rows[0][0], "executeMatch should return the same row")
	require.NoError(t, err)
	require.Len(t, stageD.Rows, 1)
	require.Equal(t, int64(1), stageD.Rows[0][0])

	shape := `
MATCH (t:TranslatedText)
WHERE t.language = $language
	AND t.isReviewed = false
MATCH (o:OriginalText)-[:TRANSLATES_TO]->(t)
WHERE o.pagePath STARTS WITH $pagePath
RETURN elementId(t) AS translationId,
       coalesce(o.textKey, o.textKey128, 'unknown') AS textKey,
       coalesce(t.isReviewed, false) AS isReviewed,
       t.translatedText AS translatedText,
       coalesce(o.originalText, '') AS originalText,
       t.translatedText AS originalTranslatedText,
       t.auditedText AS auditedTranslatedText,
       t.reviewResult AS reviewResult,
       t.reviewedAt AS reviewedAt,
       t.createdAt AS createdAt,
       o.page AS page,
       o.trackingId AS trackingId
ORDER BY t.createdAt DESC
LIMIT 30`
	t.Logf("normalized full shape: %s", normalizeMultiMatchWhereClauses(shape))
	shapeDirectCtx := context.WithValue(ctx, paramsKey, map[string]interface{}{
		"language": "es",
		"pagePath": "/benefits",
	})
	shapeDirectRes, shapeDirectErr := exec.executeMultiMatch(shapeDirectCtx, normalizeMultiMatchWhereClauses(shape))
	require.NoError(t, shapeDirectErr)
	require.NotNil(t, shapeDirectRes)
	require.Len(t, shapeDirectRes.Rows, 1, "direct executeMultiMatch should return row for full shape")

	res, err := exec.Execute(ctx, shape, map[string]interface{}{
		"language": "es",
		"pagePath": "/benefits",
	})
	require.NoError(t, err)
	require.NotNil(t, res)
	require.Equal(t, []string{
		"translationId", "textKey", "isReviewed", "translatedText", "originalText",
		"originalTranslatedText", "auditedTranslatedText", "reviewResult", "reviewedAt",
		"createdAt", "page", "trackingId",
	}, res.Columns)
	require.Len(t, res.Rows, 1, "exact list-review shape should return the matching translation row")
	require.Equal(t, "review-key-1", res.Rows[0][1], "textKey must map to the created original node")
	require.Equal(t, false, res.Rows[0][2], "isReviewed must remain false for review queue rows")
	require.Equal(t, "trk-list-1", res.Rows[0][11], "trackingId must be preserved in projection")
}

func TestExactShape_CreateOrUpdateTranslation_MergeOptionalCallUnion_ReturnsRow(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := store.CreateNode(&storage.Node{
		ID:     "orig-create-update-1",
		Labels: []string{"OriginalText"},
		Properties: map[string]interface{}{
			"textKey":    "review-key-shape",
			"textKey128": "review-key-shape-128",
			"pagePath":   "/seed",
		},
	})
	require.NoError(t, err)

	shape := `
MERGE (o:OriginalText {textKey: $lookupValue})
ON CREATE SET
  o.originalText = $originalText,
  o.page = $page,
  o.pagePath = $pagePath,
  o.trackingId = $trackingId,
  o.bypassRAG = $bypassRAG,
  o.contentType = $contentType,
  o.createdAt = $now,
  o.updatedAt = $now,
  o.textKey = $textKey,
  o.textKey128 = $textKey128
ON MATCH SET
  o.updatedAt = $now,
  o.page = CASE WHEN trim(coalesce($page, '')) <> '' THEN $page ELSE o.page END,
  o.pagePath = CASE WHEN $pagePath <> '' THEN $pagePath ELSE o.pagePath END
WITH o,
     $targetLang AS targetLang,
     $translatedText AS translatedText,
     $submitter AS submitter,
     $isRefetch AS isRefetch,
     $now AS now
OPTIONAL MATCH (o)-[:TRANSLATES_TO]->(existing:TranslatedText {language: targetLang})
WITH o, head(collect(existing)) AS existing, targetLang, translatedText, submitter, isRefetch, now
CALL {
  WITH o, existing, targetLang, translatedText, submitter, isRefetch, now
  WITH o, existing, targetLang, translatedText, submitter, isRefetch, now
  WHERE existing IS NULL
  CREATE (t:TranslatedText {
    language: targetLang,
    translatedText: translatedText,
    auditedText: null,
    isReviewed: false,
    reviewResult: null,
    reviewedAt: null,
    submitter: submitter,
    isRefetch: isRefetch,
    createdAt: now
  })
  CREATE (o)-[:TRANSLATES_TO]->(t)
  RETURN t
  UNION
  WITH existing, translatedText, submitter
  WHERE existing IS NOT NULL
  SET existing.translatedText = translatedText,
      existing.auditedText = null,
      existing.isReviewed = false,
      existing.reviewResult = null,
      existing.reviewedAt = null,
      existing.submitter = coalesce(existing.submitter, submitter),
      existing.isRefetch = CASE
        WHEN existing.submitter IS NOT NULL
          AND (existing.isRefetch IS NULL OR existing.isRefetch = false)
          AND existing.reviewResult IN ['rejected', 'reject']
        THEN true
        ELSE existing.isRefetch
      END
  RETURN existing AS t
}
RETURN elementId(o) AS originalId,
       o.textKey AS textKey,
       o.textKey128 AS textKey128,
       o.page AS page,
       o.trackingId AS trackingId,
       elementId(t) AS id,
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
		"lookupValue":    "review-key-shape",
		"originalText":   "Hello world",
		"page":           "https://example.test/review",
		"pagePath":       "/review",
		"trackingId":     "trk-shape",
		"bypassRAG":      false,
		"contentType":    "text/plain",
		"now":            "2026-03-24T00:00:00Z",
		"textKey":        "review-key-shape",
		"textKey128":     "review-key-shape-128",
		"targetLang":     "es",
		"translatedText": "hola",
		"submitter":      "submitter@example.test",
		"isRefetch":      false,
	}

	first, err := exec.Execute(ctx, shape, params)
	require.NoError(t, err)
	require.Len(t, first.Rows, 1)
	require.Contains(t, first.Columns, "originalId")
	require.Contains(t, first.Columns, "translatedText")
	firstCol := func(name string) int {
		for i, c := range first.Columns {
			if c == name {
				return i
			}
		}
		return -1
	}
	require.NotNil(t, first.Rows[0][firstCol("translatedText")])
	firstOriginalID := first.Rows[0][firstCol("originalId")]

	secondParams := map[string]interface{}{
		"lookupValue":    "review-key-shape",
		"originalText":   "Hello world",
		"page":           "https://example.test/review",
		"pagePath":       "/review",
		"trackingId":     "trk-shape",
		"bypassRAG":      false,
		"contentType":    "text/plain",
		"now":            "2026-03-24T00:00:01Z",
		"textKey":        "review-key-shape",
		"textKey128":     "review-key-shape-128",
		"targetLang":     "es",
		"translatedText": "hola-actualizada",
		"submitter":      "submitter@example.test",
		"isRefetch":      false,
	}
	second, err := exec.Execute(ctx, shape, secondParams)
	require.NoError(t, err)
	require.Len(t, second.Rows, 1)
	secondCol := func(name string) int {
		for i, c := range second.Columns {
			if c == name {
				return i
			}
		}
		return -1
	}
	require.Equal(t, firstOriginalID, second.Rows[0][secondCol("originalId")], "shape should keep targeting same original node")
	require.NotNil(t, second.Rows[0][secondCol("translatedText")])
}

func TestExactShape_ListReviewTranslations_PageFilter_CurrentShape_ReturnsExpectedRow(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := store.CreateNode(&storage.Node{
		ID:     "orig-list-current-1",
		Labels: []string{"OriginalText"},
		Properties: map[string]interface{}{
			"textKey":      "review-key-current",
			"textKey128":   "review-key-current-128",
			"originalText": "Please review me",
			"pagePath":     "/benefits/financial-summary",
			"page":         "https://example.test/benefits/financial-summary",
			"trackingId":   "trk-current-1",
		},
	})
	require.NoError(t, err)
	_, err = store.CreateNode(&storage.Node{
		ID:     "tr-list-current-1",
		Labels: []string{"TranslatedText"},
		Properties: map[string]interface{}{
			"language":       "es",
			"isReviewed":     false,
			"translatedText": "hola",
			"createdAt":      "2026-03-24T00:00:00Z",
		},
	})
	require.NoError(t, err)
	err = store.CreateEdge(&storage.Edge{
		ID:        "edge-list-current-1",
		Type:      "TRANSLATES_TO",
		StartNode: "orig-list-current-1",
		EndNode:   "tr-list-current-1",
	})
	require.NoError(t, err)

	shape := `
MATCH (t:TranslatedText {language: $language, isReviewed: false})<-[:TRANSLATES_TO]-(o:OriginalText)
WHERE o.pagePath STARTS WITH $pagePath
RETURN elementId(t) AS translationId,
       o.textKey AS textKey,
       o.textKey128 AS textKey128,
       t.isReviewed AS isReviewed,
       t.translatedText AS translatedText,
       o.originalText AS originalText,
       t.translatedText AS originalTranslatedText,
       t.auditedText AS auditedTranslatedText,
       t.reviewResult AS reviewResult,
       t.reviewedAt AS reviewedAt,
       t.createdAt AS createdAt,
       o.page AS page,
       o.trackingId AS trackingId
ORDER BY t.createdAt DESC
LIMIT 30
`

	res, err := exec.Execute(ctx, shape, map[string]interface{}{
		"language": "es",
		"pagePath": "/benefits",
	})
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.Equal(t, "review-key-current", res.Rows[0][1])
	require.Equal(t, "review-key-current-128", res.Rows[0][2])
	require.Equal(t, false, res.Rows[0][3])
	require.Equal(t, "trk-current-1", res.Rows[0][12])
}

func TestExactShape_FindTranslatedByOriginalElementID_RelTraversal(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := store.CreateNode(&storage.Node{
		ID:     "orig-find-1",
		Labels: []string{"OriginalText"},
	})
	require.NoError(t, err)
	_, err = store.CreateNode(&storage.Node{
		ID:     "tr-find-1",
		Labels: []string{"TranslatedText"},
		Properties: map[string]interface{}{
			"language":       "es",
			"translatedText": "hola",
			"isReviewed":     false,
			"createdAt":      "2026-03-24T00:00:00Z",
		},
	})
	require.NoError(t, err)
	err = store.CreateEdge(&storage.Edge{
		ID:        "edge-find-1",
		Type:      "TRANSLATES_TO",
		StartNode: "orig-find-1",
		EndNode:   "tr-find-1",
	})
	require.NoError(t, err)

	// Add noise to ensure start-node pruning path is exercised.
	for i := 0; i < 300; i++ {
		_, nerr := store.CreateNode(&storage.Node{
			ID:     storage.NodeID(fmt.Sprintf("orig-find-noise-%d", i)),
			Labels: []string{"OriginalText"},
		})
		require.NoError(t, nerr)
	}

	shape := `
MATCH (o:OriginalText)-[:TRANSLATES_TO]->(t:TranslatedText {language: $targetLang})
WHERE elementId(o) = $originalId
RETURN elementId(t) AS id,
       t.createdAt AS createdAt,
       t.language AS language,
       t.translationId AS translationId,
       t.translatedText AS translatedText,
       t.auditedText AS auditedText,
       t.isReviewed AS isReviewed,
       t.reviewResult AS reviewResult,
       t.reviewedAt AS reviewedAt,
       t.submitter AS submitter,
       t.isRefetch AS isRefetch
LIMIT 1`

	elemRes, err := exec.Execute(ctx, `
MATCH (o:OriginalText)
WHERE elementId(o) = 'orig-find-1'
RETURN elementId(o) AS id
LIMIT 1
`, nil)
	require.NoError(t, err)
	require.Len(t, elemRes.Rows, 1)
	originalID, _ := elemRes.Rows[0][0].(string)
	require.NotEmpty(t, originalID)

	res, err := exec.Execute(ctx, shape, map[string]interface{}{
		"originalId": originalID,
		"targetLang": "es",
	})
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.Equal(t, "es", res.Rows[0][2])
	require.Equal(t, "hola", res.Rows[0][4])
}

func TestExactShape_CleanupBatchedDetachDelete_ByRunIDAndKeys(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	runID := "run-cleanup-1"

	_, err := store.CreateNode(&storage.Node{
		ID:     "t-clean-1",
		Labels: []string{"TranslatedText"},
		Properties: map[string]interface{}{
			"testRun": runID,
		},
	})
	require.NoError(t, err)
	_, err = store.CreateNode(&storage.Node{
		ID:     "t-clean-2",
		Labels: []string{"TranslatedText"},
		Properties: map[string]interface{}{
			"testRun": runID,
		},
	})
	require.NoError(t, err)

	_, err = store.CreateNode(&storage.Node{
		ID:     "o-clean-1",
		Labels: []string{"OriginalText"},
		Properties: map[string]interface{}{
			"testRun":    runID,
			"textKey":    "cleanup-k1",
			"textKey128": "cleanup-k1-128",
		},
	})
	require.NoError(t, err)
	_, err = store.CreateNode(&storage.Node{
		ID:     "o-clean-2",
		Labels: []string{"OriginalText"},
		Properties: map[string]interface{}{
			"testRun":    runID,
			"textKey":    "cleanup-k2",
			"textKey128": "cleanup-k2-128",
		},
	})
	require.NoError(t, err)
	_, err = store.CreateNode(&storage.Node{
		ID:     "o-clean-3",
		Labels: []string{"OriginalText"},
		Properties: map[string]interface{}{
			"textKey":    "cleanup-k3",
			"textKey128": "cleanup-k3-128",
		},
	})
	require.NoError(t, err)

	delTranslated, err := exec.Execute(ctx, `
MATCH (t:TranslatedText)
WHERE t.testRun = $runID
WITH t LIMIT 100
DETACH DELETE t
RETURN count(t) AS deleted
`, map[string]interface{}{"runID": runID})
	require.NoError(t, err)
	require.Len(t, delTranslated.Rows, 1)
	require.Equal(t, int64(2), delTranslated.Rows[0][0])

	delOriginalByRun, err := exec.Execute(ctx, `
MATCH (o:OriginalText)
WHERE o.testRun = $runID
WITH o LIMIT 100
DETACH DELETE o
RETURN count(o) AS deleted
`, map[string]interface{}{"runID": runID})
	require.NoError(t, err)
	require.Len(t, delOriginalByRun.Rows, 1)
	require.Equal(t, int64(2), delOriginalByRun.Rows[0][0])

	delOriginalByKeys, err := exec.Execute(ctx, `
MATCH (o:OriginalText)
WHERE o.textKey IN $keys
WITH o LIMIT 100
DETACH DELETE o
RETURN count(o) AS deleted
`, map[string]interface{}{"keys": []interface{}{"cleanup-k3"}})
	require.NoError(t, err)
	require.Len(t, delOriginalByKeys.Rows, 1)
	require.Equal(t, int64(1), delOriginalByKeys.Rows[0][0])
}

func TestMigrationDDL_OpenCypherCompatibleVariants_FullStatements(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `
CREATE INDEX original_text_idx IF NOT EXISTS
FOR (o:OriginalText)
ON (o.originalText)
`, nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, `
CREATE INDEX translated_lang_idx IF NOT EXISTS
FOR (t:TranslatedText)
ON (t.language)
`, nil)
	require.NoError(t, err)

	beforeDrop, err := exec.Execute(ctx, "SHOW INDEXES", nil)
	require.NoError(t, err)
	require.NotNil(t, beforeDrop)
	require.GreaterOrEqual(t, len(beforeDrop.Rows), 2)

	showRes, err := exec.Execute(ctx, `
SHOW INDEXES
YIELD name, state, type, entityType, labelsOrTypes, properties
RETURN name, state, type, entityType, labelsOrTypes, properties
ORDER BY name
`, nil)
	require.NoError(t, err)
	require.NotEmpty(t, showRes.Rows)

	_, err = exec.Execute(ctx, "DROP INDEX translated_lang_idx IF EXISTS", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "DROP INDEX original_text_idx IF EXISTS", nil)
	require.NoError(t, err)

	afterDrop, err := exec.Execute(ctx, "SHOW INDEXES", nil)
	require.NoError(t, err)
	require.NotNil(t, afterDrop)
	require.LessOrEqual(t, len(afterDrop.Rows), len(beforeDrop.Rows))
}
