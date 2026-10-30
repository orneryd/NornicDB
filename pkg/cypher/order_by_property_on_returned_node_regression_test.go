package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestOrderByProperty_OnReturnedNode_WithoutProjectedPropertyColumn(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `
CREATE (o1:OriginalText {textKey:'k1', originalText:'one'})
CREATE (o2:OriginalText {textKey:'k2', originalText:'two'})
CREATE (t1:TranslatedText {language:'es', createdAt:'2026-01-01T00:00:00Z'})
CREATE (t2:TranslatedText {language:'es', createdAt:'2026-02-01T00:00:00Z'})
CREATE (o1)-[:TRANSLATES_TO]->(t1)
CREATE (o2)-[:TRANSLATES_TO]->(t2)
`, nil)
	require.NoError(t, err)

	// Shape A: include t.createdAt in projection.
	withCreatedAt, err := exec.Execute(ctx, `
MATCH (o:OriginalText)-[:TRANSLATES_TO]->(t:TranslatedText)
WHERE t.language = 'es'
RETURN o.textKey, o.originalText, t, t.createdAt
ORDER BY t.createdAt DESC
LIMIT 10
`, nil)
	require.NoError(t, err)
	require.Len(t, withCreatedAt.Rows, 2)
	require.Equal(t, "k2", withCreatedAt.Rows[0][0])
	require.Equal(t, "k1", withCreatedAt.Rows[1][0])

	// Shape B: do not project t.createdAt, still order by it.
	withoutCreatedAt, err := exec.Execute(ctx, `
MATCH (o:OriginalText)-[:TRANSLATES_TO]->(t:TranslatedText)
WHERE t.language = 'es'
RETURN o.textKey, o.originalText, t
ORDER BY t.createdAt DESC
LIMIT 10
`, nil)
	require.NoError(t, err)
	require.Len(t, withoutCreatedAt.Rows, 2)
	require.Equal(t, "k2", withoutCreatedAt.Rows[0][0], "ORDER BY t.createdAt should work even when t.createdAt is not projected")
	require.Equal(t, "k1", withoutCreatedAt.Rows[1][0])
}
