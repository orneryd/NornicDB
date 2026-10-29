package cypher

import (
	"context"
	"fmt"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestRelationshipBindingCacheTypeProvenance(t *testing.T) {
	ctx := context.Background()
	e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	_, err := e.Execute(ctx, "CREATE (a:BindingStart)-[:`A|B` {p:1}]->(b:BindingEnd), (a)-[:R {p:2}]->(b)", nil)
	require.NoError(t, err)
	for _, properties := range []bool{false, true} {
		mapText := ""
		if properties {
			mapText = " {p:$p}"
		}
		query := "MATCH (a:BindingStart),(b:BindingEnd) WHERE (a)-[:$($type)" + mapText + "]->(b) RETURN count(*) AS n"
		for _, input := range []struct {
			name    string
			p, want int64
		}{{"A|B", 1, 1}, {"R", 2, 1}, {"missing", 1, 0}, {"A|B", 2, 0}, {"A|B", 1, 1}} {
			if !properties && input.name == "A|B" && input.p == 2 {
				input.want = 1
			}
			result, err := e.Execute(ctx, query, map[string]any{"type": input.name, "p": input.p})
			require.NoError(t, err)
			require.Equal(t, input.want, result.Rows[0][0], "retained statement reused a previous row's type/property values")
		}
	}
	// Exercise the retained binding predicate itself, alongside the public
	// execution above; property-map and map-free branches have different plans.
	entities, err := e.Execute(ctx, "MATCH (a:BindingStart),(b:BindingEnd) RETURN a,b", nil)
	require.NoError(t, err)
	row := binding{"a": entities.Rows[0][0].(*storage.Node), "b": entities.Rows[0][1].(*storage.Node)}
	for _, clause := range []string{"(a)-[:`A|B`]->(b)", "(a)-[:`A|B` {p:$p}]->(b)"} {
		truth, ok := e.tryCompileExecutorBindingWhereTruth(ctx, clause)
		require.True(t, ok)
		require.Equal(t, truthTrue, truth(row, map[string]any{"p": int64(1)}))
		if clause != "(a)-[:`A|B`]->(b)" {
			require.Equal(t, truthFalse, truth(row, map[string]any{"p": int64(2)}))
		}
	}
}

func TestRelationshipMergeIdentityLiteralType(t *testing.T) {
	ctx := context.Background()
	e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	_, err := e.Execute(ctx, "CREATE (:MergeStart),(:MergeEnd)", nil)
	require.NoError(t, err)
	query := "UNWIND [1,1,1] AS value MATCH (a:MergeStart),(c:MergeEnd) MERGE (a)-[:`A|B` {p:value}]->(b:MergeMiddle)-[:R]->(c) RETURN id(b) AS identity"
	for iteration := 0; iteration < 2; iteration++ {
		result, err := e.Execute(ctx, query, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 3)
		for _, row := range result.Rows {
			require.Equal(t, result.Rows[0][0], row[0])
		}
	}
	result, err := e.Execute(ctx, "MATCH ()-[r]->() RETURN type(r) AS kind,count(*) AS n ORDER BY kind", nil)
	require.NoError(t, err)
	require.Equal(t, [][]any{{"A|B", int64(1)}, {"R", int64(1)}}, result.Rows)
}

func TestRelationshipTypedIndexLiteralType(t *testing.T) {
	ctx := context.Background()
	e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	_, err := e.Execute(ctx, "CREATE TEXT INDEX literal_text FOR ()-[r:`A|B`]-() ON (r.p)", nil)
	require.NoError(t, err)
	result, err := e.Execute(ctx, "SHOW INDEXES YIELD name, labelsOrTypes WHERE name = 'literal_text' RETURN labelsOrTypes", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	require.Equal(t, "[A|B]", fmt.Sprint(result.Rows[0][0]))
}

func TestRelationshipMergeLiteralTypeAfterIdentityConflict(t *testing.T) {
	ctx := context.Background()
	engine := edgeConflictTestEngine(t)
	first, second := NewStorageExecutor(engine), NewStorageExecutor(engine)
	_, err := first.Execute(ctx, "CREATE (:ConflictStart),(:ConflictEnd)", nil)
	require.NoError(t, err)
	for _, exec := range []*StorageExecutor{first, second} {
		_, err = exec.Execute(ctx, "BEGIN", nil)
		require.NoError(t, err)
		defer exec.Execute(context.Background(), "ROLLBACK", nil)
	}
	query := "MATCH (a:ConflictStart),(b:ConflictEnd) MERGE (a)-[r:`A|B` {key:1}]->(b) RETURN type(r) AS kind"
	for _, exec := range []*StorageExecutor{first, second} {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
		require.Equal(t, [][]any{{"A|B"}}, result.Rows)
	}
	_, err = first.Execute(ctx, "COMMIT", nil)
	require.NoError(t, err)
	_, err = second.Execute(ctx, "COMMIT", nil)
	require.ErrorIs(t, err, storage.ErrConflict)
	result, err := second.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.Equal(t, [][]any{{"A|B"}}, result.Rows)
	result, err = first.Execute(ctx, "MATCH ()-[r]->() RETURN type(r) AS kind,count(*) AS n", nil)
	require.NoError(t, err)
	require.Equal(t, [][]any{{"A|B", int64(1)}}, result.Rows)
}

func TestRelationshipTypedIndexExportPreservesLiteralType(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	_, err := exec.Execute(t.Context(), "CREATE TEXT INDEX export_literal FOR ()-[r:`A|B`]-() ON (r.p)", nil)
	require.NoError(t, err)
	definition := exec.storage.GetSchema().ExportDefinition()
	found := false
	for _, index := range definition.RangeIndexes {
		if index.Name == "export_literal" {
			found = true
			require.Equal(t, "A|B", index.Label)
			require.Equal(t, storage.ConstraintEntityRelationship, index.EntityType)
			require.Equal(t, storage.IndexKindText, index.Kind)
		}
	}
	require.True(t, found, "TEXT relationship index was omitted from the persisted schema")
	require.NoError(t, exec.storage.GetSchema().ReplaceFromDefinition(definition))
	result, err := exec.Execute(t.Context(), "SHOW INDEXES YIELD name, labelsOrTypes WHERE name = 'export_literal' RETURN labelsOrTypes", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	require.Equal(t, "[A|B]", fmt.Sprint(result.Rows[0][0]))
}
