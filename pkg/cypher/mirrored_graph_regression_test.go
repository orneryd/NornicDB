package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestMirroredGraph_SaveSequence_MatchesExpectedHierarchy(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	defer store.Close()

	exec := NewStorageExecutor(store)
	ctx := context.Background()

	require.NoError(t, runMirrorSave(ctx, exec, "3.18.5.1", nil, nil))
	require.NoError(t, runMirrorSave(ctx, exec, "3.18.5", strPtr("title A"), "3.18.5.1"))

	require.NoError(t, runMirrorSave(ctx, exec, "3.18.6.1", nil, nil))
	require.NoError(t, runMirrorSave(ctx, exec, "3.18.6.2", strPtr("B"), nil))
	require.NoError(t, runMirrorSave(ctx, exec, "3.18.6.3", strPtr("C"), nil))
	require.NoError(t, runMirrorSave(ctx, exec, "3.18.6", strPtr("title A B C"), []interface{}{"3.18.6.1", "3.18.6.2", "3.18.6.3"}))

	require.NoError(t, runMirrorSave(ctx, exec, "3.18", strPtr("big title title A title A B C"), []interface{}{"3.18.5", "3.18.6"}))

	counts, err := exec.Execute(ctx, `
	MATCH (s:Section)
	RETURN count(s) AS nodes
	`, nil)
	require.NoError(t, err)
	require.Len(t, counts.Rows, 1)
	require.Equal(t, int64(7), counts.Rows[0][0])

	relCounts, err := exec.Execute(ctx, `
	MATCH (:Section)-[r:SUBSECTION_OF]->(:Section)
	RETURN count(r) AS rels
	`, nil)
	require.NoError(t, err)
	require.Len(t, relCounts.Rows, 1)
	require.Equal(t, int64(6), relCounts.Rows[0][0])

	parents, err := exec.Execute(ctx, `
	MATCH (child:Section {code: '3.18'})-[:SUBSECTION_OF]->(parent:Section)
	RETURN parent.code AS code
	ORDER BY code
	`, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"3.18.5"}, {"3.18.6"}}, parents.Rows)

	children, err := exec.Execute(ctx, `
	MATCH (child:Section {code: '3.18.6'})-[:SUBSECTION_OF]->(parent:Section)
	RETURN parent.code AS code
	ORDER BY code
	`, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"3.18.6.1"}, {"3.18.6.2"}, {"3.18.6.3"}}, children.Rows)

	content, err := exec.Execute(ctx, `
	MATCH (s:Section {code: '3.18'})
	RETURN s.content AS content
	`, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"big title title A title A B C"}}, content.Rows)
}

func TestMirroredGraph_OptionalMatchRetrieval_ReturnsNodesRelationshipsAndParents(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	defer store.Close()

	exec := NewStorageExecutor(store)
	ctx := context.Background()
	seedMirrorGraph(t, ctx, exec)

	t.Run("ascii arrow", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
		MATCH (n:Section)
		OPTIONAL MATCH (n)-[r:SUBSECTION_OF]->(p)
		RETURN n, r, p
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 10)

		nonNilRels := 0
		nilRels := 0
		for _, row := range result.Rows {
			require.Len(t, row, 3)
			require.NotNil(t, row[0])
			if row[1] == nil {
				nilRels++
				require.Nil(t, row[2])
			} else {
				nonNilRels++
				require.NotNil(t, row[2])
			}
		}
		require.Equal(t, 6, nonNilRels)
		require.Equal(t, 4, nilRels)
	})

	t.Run("unicode arrow", func(t *testing.T) {
		result, err := exec.Execute(ctx, "MATCH (n:Section) OPTIONAL MATCH (n)-[r:SUBSECTION_OF]→(p) RETURN n, r, p", nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 10)
	})
}

func seedMirrorGraph(t *testing.T, ctx context.Context, exec *StorageExecutor) {
	t.Helper()
	require.NoError(t, runMirrorSave(ctx, exec, "3.18.5.1", nil, nil))
	require.NoError(t, runMirrorSave(ctx, exec, "3.18.5", strPtr("title A"), "3.18.5.1"))
	require.NoError(t, runMirrorSave(ctx, exec, "3.18.6.1", strPtr("A"), nil))
	require.NoError(t, runMirrorSave(ctx, exec, "3.18.6.2", strPtr("B"), nil))
	require.NoError(t, runMirrorSave(ctx, exec, "3.18.6.3", strPtr("C"), nil))
	require.NoError(t, runMirrorSave(ctx, exec, "3.18.6", strPtr("title A B C"), []interface{}{"3.18.6.1", "3.18.6.2", "3.18.6.3"}))
	require.NoError(t, runMirrorSave(ctx, exec, "3.18", strPtr("big title title A title A B C"), []interface{}{"3.18.5", "3.18.6"}))
}

func runMirrorSave(ctx context.Context, exec *StorageExecutor, code string, content *string, parent interface{}) error {
	switch p := parent.(type) {
	case nil:
		if content == nil {
			_, err := exec.Execute(ctx, "MERGE (s:Section {code: $code})", map[string]interface{}{"code": code})
			return err
		}
		_, err := exec.Execute(ctx, `
		MERGE (s:Section {code: $code})
		  ON CREATE SET s.content = $content
		  ON MATCH SET s.content = $content
		`, map[string]interface{}{"code": code, "content": *content})
		return err
	case string:
		if content == nil {
			_, err := exec.Execute(ctx, `
			MERGE (parent:Section {code: $parent_code})
			MERGE (child:Section {code: $code})
			MERGE (child)-[:SUBSECTION_OF]->(parent)
			`, map[string]interface{}{"code": code, "parent_code": p})
			return err
		}
		_, err := exec.Execute(ctx, `
		MERGE (parent:Section {code: $parent_code})
		WITH parent
		MERGE (child:Section {code: $code})
		  ON CREATE SET child.content = $content
		  ON MATCH SET child.content = $content
		MERGE (child)-[:SUBSECTION_OF]->(parent)
		`, map[string]interface{}{"code": code, "content": *content, "parent_code": p})
		return err
	case []interface{}:
		if content == nil {
			return nil
		}
		_, err := exec.Execute(ctx, `
		UNWIND $parent_codes AS p_code
		MERGE (parent:Section {code: p_code})
		WITH parent
		MERGE (child:Section {code: $code})
		  ON CREATE SET child.content = $content
		  ON MATCH SET child.content = $content
		WITH parent, child
		MERGE (child)-[:SUBSECTION_OF]->(parent)
		`, map[string]interface{}{"code": code, "content": *content, "parent_codes": p})
		return err
	default:
		return nil
	}
}

func strPtr(v string) *string {
	return &v
}
