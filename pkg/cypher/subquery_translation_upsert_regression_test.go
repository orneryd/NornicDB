package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Regression: correlated CALL { WITH o,t ... UNION ... } branches must preserve
// WITH-imported variables (including OPTIONAL MATCH imports) so create-or-update
// translation flows return rows and update existing nodes instead of duplicating.
func TestExecuteMatchWithCallSubquery_CreateOrUpdateTranslationShape(t *testing.T) {
	ctx := context.Background()
	base := storage.NewMemoryEngine()
	store := storage.NewNamespacedEngine(base, "nornic")
	exec := NewStorageExecutor(store)

	_, err := exec.Execute(ctx, `CREATE (:OriginalText {textKey128: 'k1', originalText: 'hello'})`, nil)
	require.NoError(t, err)

	createOrUpdate := `
MATCH (o:OriginalText)
WHERE o.textKey128 = 'k1'
OPTIONAL MATCH (o)-[:TRANSLATES_TO]->(t:TranslatedText {language: 'es'})
WITH o, t
CALL {
  WITH o, t
	WITH o, t
  WHERE t IS NULL
  CREATE (newT:TranslatedText {language: 'es', translatedText: 'hola'})
  CREATE (o)-[:TRANSLATES_TO]->(newT)
  RETURN newT AS node
  UNION
  WITH o, t
	WITH o, t
  WHERE t IS NOT NULL
  SET t.translatedText = 'hola-2'
  RETURN t AS node
}
RETURN node.language AS language, node.translatedText AS translatedText`

	// First run: create branch should return one row.
	res1, err := exec.Execute(ctx, createOrUpdate, nil)
	require.NoError(t, err)
	require.Len(t, res1.Rows, 1)
	require.Equal(t, "es", res1.Rows[0][0])
	require.Equal(t, "hola", res1.Rows[0][1])

	checkOuter, err := exec.Execute(ctx, `
MATCH (o:OriginalText)
WHERE o.textKey128 = 'k1'
OPTIONAL MATCH (o)-[:TRANSLATES_TO]->(t:TranslatedText {language: 'es'})
WITH o, t
RETURN t.translatedText AS translatedText`, nil)
	require.NoError(t, err)
	require.Len(t, checkOuter.Rows, 1)
	require.Equal(t, "hola", checkOuter.Rows[0][0])

	// Second run: update branch should return one row and not create duplicates.
	res2, err := exec.Execute(ctx, createOrUpdate, nil)
	require.NoError(t, err)
	require.Len(t, res2.Rows, 1)
	require.Equal(t, "es", res2.Rows[0][0])
	require.Equal(t, "hola-2", res2.Rows[0][1])

	verify, err := exec.Execute(ctx, `
MATCH (o:OriginalText {textKey128: 'k1'})-[:TRANSLATES_TO]->(t:TranslatedText {language: 'es'})
RETURN count(t) AS c, collect(t.translatedText) AS texts`, nil)
	require.NoError(t, err)
	require.Len(t, verify.Rows, 1)
	require.Equal(t, int64(1), verify.Rows[0][0])
}
