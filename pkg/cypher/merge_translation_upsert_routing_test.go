package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestMergeTranslationUpsert_ExactProductionShape_UsesSchemaLookupRouting(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	require.NoError(t, store.GetSchema().AddPropertyIndex("idx_original_textkey", "OriginalText", []string{"textKey"}))
	require.NoError(t, store.GetSchema().AddCompositeIndex("idx_translated_translation_language", "TranslatedText", []string{"translationId", "language"}))

	query := `MERGE (o:OriginalText {textKey: $textKey}) ON CREATE SET o.textKey128 = $textKey128, o.originalText = $originalText, o.page = $page, o.pagePath = $pagePath, o.trackingId = $trackingId, o.createdAt = $createdAt, o.updatedAt = $updatedAt, o.contentType = $contentType, o.bypassRAG = $bypassRAG ON MATCH SET o.textKey128 = CASE WHEN $textKey128 <> '' THEN $textKey128 ELSE o.textKey128 END, o.originalText = CASE WHEN $originalText <> '' THEN $originalText ELSE o.originalText END, o.page = CASE WHEN $page IS NOT NULL THEN $page ELSE o.page END, o.pagePath = CASE WHEN $pagePath <> '' THEN $pagePath ELSE o.pagePath END, o.trackingId = CASE WHEN $trackingId IS NOT NULL THEN $trackingId ELSE o.trackingId END, o.updatedAt = $updatedAt, o.contentType = CASE WHEN $contentType IS NOT NULL THEN $contentType ELSE o.contentType END, o.bypassRAG = CASE WHEN $bypassRAG IS NOT NULL THEN $bypassRAG ELSE o.bypassRAG END MERGE (t:TranslatedText {translationId: $textKey, language: $targetLang}) ON CREATE SET t.translationId = $textKey, t.translatedText = $translatedText, t.auditedText = $auditedText, t.isReviewed = $isReviewed, t.reviewResult = $reviewResult, t.reviewedAt = $reviewedAt, t.submitter = $submitter, t.reviewerFirstName = $reviewerFirstName, t.reviewerLastName = $reviewerLastName, t.reviewerEmail = $reviewerEmail, t.correctionReason = $correctionReason, t.reviewerComments = $reviewerComments, t.createdAt = $createdAt, t.updatedAt = $updatedAt ON MATCH SET t.translatedText = $translatedText, t.auditedText = CASE WHEN $auditedText IS NOT NULL THEN $auditedText ELSE t.auditedText END, t.isReviewed = $isReviewed, t.reviewResult = $reviewResult, t.reviewedAt = $reviewedAt, t.submitter = CASE WHEN $submitter IS NOT NULL THEN $submitter ELSE t.submitter END, t.reviewerFirstName = CASE WHEN $reviewerFirstName IS NOT NULL THEN $reviewerFirstName ELSE t.reviewerFirstName END, t.reviewerLastName = CASE WHEN $reviewerLastName IS NOT NULL THEN $reviewerLastName ELSE t.reviewerLastName END, t.reviewerEmail = CASE WHEN $reviewerEmail IS NOT NULL THEN $reviewerEmail ELSE t.reviewerEmail END, t.correctionReason = CASE WHEN $correctionReason IS NOT NULL THEN $correctionReason ELSE t.correctionReason END, t.reviewerComments = CASE WHEN $reviewerComments IS NOT NULL THEN $reviewerComments ELSE t.reviewerComments END, t.updatedAt = $updatedAt MERGE (o)-[:TRANSLATES_TO]->(t) RETURN elementId(o) AS originalId, o.textKey AS textKey, o.textKey128 AS textKey128, o.originalText AS originalText, o.page AS page, o.trackingId AS trackingId, collect({ id: elementId(t), translationId: t.translationId, language: t.language, translatedText: t.translatedText, auditedText: t.auditedText, isReviewed: t.isReviewed, reviewResult: t.reviewResult, reviewedAt: t.reviewedAt, reviewerFirstName: t.reviewerFirstName, reviewerLastName: t.reviewerLastName, reviewerEmail: t.reviewerEmail, correctionReason: t.correctionReason, reviewerComments: t.reviewerComments, submitter: t.submitter, createdAt: t.createdAt, updatedAt: t.updatedAt }) AS texts`

	paramsCreate := map[string]interface{}{
		"textKey":           "src-1",
		"textKey128":        "src-1-128",
		"originalText":      "Hello world",
		"page":              int64(7),
		"pagePath":          "/hello",
		"trackingId":        "trk-1",
		"createdAt":         "2026-04-08T20:00:00Z",
		"updatedAt":         "2026-04-08T20:00:00Z",
		"contentType":       "text/plain",
		"bypassRAG":         false,
		"targetLang":        "es",
		"translatedText":    "Hola mundo",
		"auditedText":       "Hola mundo auditado",
		"isReviewed":        true,
		"reviewResult":      "approved",
		"reviewedAt":        "2026-04-08T20:01:00Z",
		"submitter":         "submitter-1",
		"reviewerFirstName": "Ana",
		"reviewerLastName":  "García",
		"reviewerEmail":     "ana@example.com",
		"correctionReason":  "style",
		"reviewerComments":  "looks good",
	}

	resCreate, err := exec.Execute(ctx, query, paramsCreate)
	require.NoError(t, err)
	require.Equal(t, []string{"originalId", "textKey", "textKey128", "originalText", "page", "trackingId", "texts"}, resCreate.Columns)
	require.Len(t, resCreate.Rows, 1)
	require.Len(t, resCreate.Rows[0], 7)
	require.Equal(t, "src-1", resCreate.Rows[0][1])
	require.Equal(t, "src-1-128", resCreate.Rows[0][2])
	require.Equal(t, "Hello world", resCreate.Rows[0][3])
	require.Equal(t, int64(7), toInt64ForTest(t, resCreate.Rows[0][4]))
	require.Equal(t, "trk-1", resCreate.Rows[0][5])
	textsCreate, ok := resCreate.Rows[0][6].([]interface{})
	require.True(t, ok)
	require.Len(t, textsCreate, 1)
	traceCreate := exec.LastHotPathTrace()
	require.True(t, traceCreate.MergeSchemaLookupUsed)
	require.False(t, traceCreate.MergeScanFallbackUsed)
	require.False(t, traceCreate.UnwindSimpleMergeBatch)

	paramsUpdate := map[string]interface{}{
		"textKey":           "src-1",
		"textKey128":        "src-1-128-updated",
		"originalText":      "Hello world updated",
		"page":              int64(8),
		"pagePath":          "/hello-updated",
		"trackingId":        "trk-2",
		"createdAt":         "2026-04-08T20:00:00Z",
		"updatedAt":         "2026-04-08T20:02:00Z",
		"contentType":       "text/markdown",
		"bypassRAG":         true,
		"targetLang":        "es",
		"translatedText":    "Hola mundo v2",
		"auditedText":       "Hola mundo auditado v2",
		"isReviewed":        false,
		"reviewResult":      "reopened",
		"reviewedAt":        "2026-04-08T20:03:00Z",
		"submitter":         "submitter-2",
		"reviewerFirstName": "Bea",
		"reviewerLastName":  "Lopez",
		"reviewerEmail":     "bea@example.com",
		"correctionReason":  "terminology",
		"reviewerComments":  "changed wording",
	}

	resUpdate, err := exec.Execute(ctx, query, paramsUpdate)
	require.NoError(t, err)
	require.Len(t, resUpdate.Rows, 1)
	require.Equal(t, "src-1", resUpdate.Rows[0][1])
	require.Equal(t, "src-1-128-updated", resUpdate.Rows[0][2])
	require.Equal(t, "Hello world updated", resUpdate.Rows[0][3])
	require.Equal(t, int64(8), toInt64ForTest(t, resUpdate.Rows[0][4]))
	require.Equal(t, "trk-2", resUpdate.Rows[0][5])
	textsUpdate, ok := resUpdate.Rows[0][6].([]interface{})
	require.True(t, ok)
	require.Len(t, textsUpdate, 1)
	traceUpdate := exec.LastHotPathTrace()
	require.True(t, traceUpdate.MergeSchemaLookupUsed)
	require.False(t, traceUpdate.MergeScanFallbackUsed)
	require.False(t, traceUpdate.UnwindSimpleMergeBatch)

	verify, err := exec.Execute(ctx, `MATCH (o:OriginalText {textKey: 'src-1'})-[:TRANSLATES_TO]->(t:TranslatedText {translationId: 'src-1', language: 'es'}) RETURN count(o) AS originals, count(t) AS translated, o.textKey128 AS textKey128, o.originalText AS originalText, o.page AS page, o.pagePath AS pagePath, o.trackingId AS trackingId, o.contentType AS contentType, o.bypassRAG AS bypassRAG, t.translatedText AS translatedText, t.auditedText AS auditedText, t.isReviewed AS isReviewed, t.reviewResult AS reviewResult, t.submitter AS submitter, t.reviewerFirstName AS reviewerFirstName, t.reviewerLastName AS reviewerLastName, t.reviewerEmail AS reviewerEmail, t.correctionReason AS correctionReason, t.reviewerComments AS reviewerComments`, nil)
	require.NoError(t, err)
	require.Len(t, verify.Rows, 1)
	row := verify.Rows[0]
	require.Equal(t, int64(1), row[0])
	require.Equal(t, int64(1), row[1])
	require.Equal(t, "src-1-128-updated", row[2])
	require.Equal(t, "Hello world updated", row[3])
	require.Equal(t, int64(8), toInt64ForTest(t, row[4]))
	require.Equal(t, "/hello-updated", row[5])
	require.Equal(t, "trk-2", row[6])
	require.Equal(t, "text/markdown", row[7])
	require.Equal(t, true, row[8])
	require.Equal(t, "Hola mundo v2", row[9])
	require.Equal(t, "Hola mundo auditado v2", row[10])
	require.Equal(t, false, row[11])
	require.Equal(t, "reopened", row[12])
	require.Equal(t, "submitter-2", row[13])
	require.Equal(t, "Bea", row[14])
	require.Equal(t, "Lopez", row[15])
	require.Equal(t, "bea@example.com", row[16])
	require.Equal(t, "terminology", row[17])
	require.Equal(t, "changed wording", row[18])
}

func TestMergeTranslationUpsert_QueryShapeVariants_RouteDeterministically(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	require.NoError(t, store.GetSchema().AddPropertyIndex("idx_original_textkey_variants", "OriginalText", []string{"textKey"}))
	require.NoError(t, store.GetSchema().AddCompositeIndex("idx_translated_translation_language_variants", "TranslatedText", []string{"translationId", "language"}))

	variants := []struct {
		name       string
		query      string
		params     map[string]interface{}
		expectCols []string
	}{
		{
			name:       "single_merge_node_exact_key",
			query:      `MERGE (t:TranslatedText {translationId: $textKey, language: $targetLang}) ON CREATE SET t.translatedText = $translatedText ON MATCH SET t.translatedText = $translatedText RETURN t.translationId AS translationId, t.language AS language, t.translatedText AS translatedText`,
			params:     map[string]interface{}{"textKey": "var-1", "targetLang": "fr", "translatedText": "bonjour"},
			expectCols: []string{"translationId", "language", "translatedText"},
		},
		{
			name:       "chain_two_node_merges_without_rel_return_scalar",
			query:      `MERGE (o:OriginalText {textKey: $textKey}) ON CREATE SET o.originalText = $originalText ON MATCH SET o.originalText = $originalText MERGE (t:TranslatedText {translationId: $textKey, language: $targetLang}) ON CREATE SET t.translatedText = $translatedText ON MATCH SET t.translatedText = $translatedText RETURN o.textKey AS textKey, t.language AS language, t.translatedText AS translatedText`,
			params:     map[string]interface{}{"textKey": "var-2", "originalText": "source", "targetLang": "de", "translatedText": "hallo"},
			expectCols: []string{"textKey", "language", "translatedText"},
		},
		{
			name:       "chain_with_relationship_merge_and_collect",
			query:      `MERGE (o:OriginalText {textKey: $textKey}) ON CREATE SET o.originalText = $originalText ON MATCH SET o.originalText = $originalText MERGE (t:TranslatedText {translationId: $textKey, language: $targetLang}) ON CREATE SET t.translatedText = $translatedText ON MATCH SET t.translatedText = $translatedText MERGE (o)-[:TRANSLATES_TO]->(t) RETURN o.textKey AS textKey, collect(t.language) AS languages`,
			params:     map[string]interface{}{"textKey": "var-3", "originalText": "source-3", "targetLang": "it", "translatedText": "ciao"},
			expectCols: []string{"textKey", "languages"},
		},
	}

	for _, tc := range variants {
		t.Run(tc.name, func(t *testing.T) {
			res, err := exec.Execute(ctx, tc.query, tc.params)
			require.NoError(t, err)
			require.Equal(t, tc.expectCols, res.Columns)
			require.Len(t, res.Rows, 1)
			trace := exec.LastHotPathTrace()
			require.True(t, trace.MergeSchemaLookupUsed)
			require.False(t, trace.MergeScanFallbackUsed)
			require.False(t, trace.UnwindSimpleMergeBatch)
		})
	}
}
