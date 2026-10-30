package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestE2E_TranslationQueryFamily_ExactShapeReturnsTopKFrenchRows(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	seedTranslationQueryFamilyData(t, store, "", 15, 4)

	res, err := exec.Execute(ctx, "MATCH (o:OriginalText)-[:TRANSLATES_TO]->(t:TranslatedText) WHERE t.language = 'fr' RETURN o, t, t.createdAt ORDER BY t.createdAt DESC LIMIT 10", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"o", "t", "t.createdAt"}, res.Columns)
	require.Len(t, res.Rows, 10)

	for i, row := range res.Rows {
		orig, ok := row[0].(*storage.Node)
		require.True(t, ok)
		tr, ok := row[1].(*storage.Node)
		require.True(t, ok)
		require.Equal(t, "fr", tr.Properties["language"])
		require.Equal(t, tr.Properties["createdAt"], row[2])
		require.Equal(t, fmt.Sprintf("fr-%02d", 14-i), orig.Properties["textKey"])
	}
}

func TestE2E_TranslationQueryFamily_ModifierVariantsStayOrdered(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	seedTranslationQueryFamilyData(t, store, "", 15, 4)

	withoutProjectedSortKey, err := exec.Execute(ctx, `
MATCH (o:OriginalText)-[:TRANSLATES_TO]->(t:TranslatedText)
WHERE t.language = 'fr'
RETURN o.textKey AS textKey, t
ORDER BY t.createdAt DESC
LIMIT 4
`, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"textKey", "t"}, withoutProjectedSortKey.Columns)
	require.Len(t, withoutProjectedSortKey.Rows, 4)
	require.Equal(t, "fr-14", withoutProjectedSortKey.Rows[0][0])
	require.Equal(t, "fr-13", withoutProjectedSortKey.Rows[1][0])
	require.Equal(t, "fr-12", withoutProjectedSortKey.Rows[2][0])
	require.Equal(t, "fr-11", withoutProjectedSortKey.Rows[3][0])

	aliasedProjection, err := exec.Execute(ctx, `
MATCH (o:OriginalText)-[:TRANSLATES_TO]->(t:TranslatedText)
WHERE t.language = 'fr'
RETURN o.textKey AS textKey, t.createdAt AS createdAt
ORDER BY t.createdAt DESC
LIMIT 2
`, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"textKey", "createdAt"}, aliasedProjection.Columns)
	require.Len(t, aliasedProjection.Rows, 2)
	require.Equal(t, []interface{}{"fr-14", "2026-04-15T12:00:00Z"}, aliasedProjection.Rows[0])
	require.Equal(t, []interface{}{"fr-13", "2026-04-14T12:00:00Z"}, aliasedProjection.Rows[1])
}
