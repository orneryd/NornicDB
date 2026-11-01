package cypher

import (
	"context"
	"errors"
	"fmt"
	"testing"

	nerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// vectorInputProcedures are the two vector query procedures that resolve the
// same third argument and then run their own search.
var vectorInputProcedures = []struct {
	name         string
	procedure    string
	index        string
	entityColumn string
}{
	{name: "queryNodes", procedure: "db.index.vector.queryNodes", index: "doc_idx", entityColumn: "node"},
	{name: "queryRelationships", procedure: "db.index.vector.queryRelationships", index: "link_idx", entityColumn: "relationship"},
}

// newVectorInputExecutor builds the graph both procedures resolve their third
// argument against: two embedded Doc nodes, two embedded LINKS relationships
// of the index's type, and one embedded relationship of another type.
func newVectorInputExecutor(t testing.TB, dims int) *StorageExecutor {
	t.Helper()
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	exec := NewStorageExecutor(store)
	if dims <= 0 {
		dims = 2
	}
	for id, axis := range map[storage.NodeID]int{"n1": 0, "n2": 1} {
		_, err := store.CreateNode(&storage.Node{
			ID:         id,
			Labels:     []string{"Doc"},
			Properties: map[string]interface{}{"embedding": axisVector(dims, axis)},
		})
		require.NoError(t, err)
	}
	for _, edge := range []struct {
		id   storage.EdgeID
		typ  string
		axis int
	}{
		{id: "r1", typ: "LINKS", axis: 0},
		{id: "r2", typ: "LINKS", axis: 1},
		{id: "r3", typ: "OTHER", axis: 0},
	} {
		require.NoError(t, store.CreateEdge(&storage.Edge{
			ID:         edge.id,
			StartNode:  "n1",
			EndNode:    "n2",
			Type:       edge.typ,
			Properties: map[string]interface{}{"emb": axisVector32(dims, edge.axis)},
		}))
	}
	schema := store.GetSchema()
	require.NoError(t, schema.AddVectorIndex("doc_idx", "Doc", "embedding", dims, "cosine", storage.ConstraintEntityNode))
	require.NoError(t, schema.AddVectorIndex("link_idx", "LINKS", "emb", dims, "cosine", storage.ConstraintEntityRelationship))
	return exec
}

func axisVector(dims, axis int) []float64 {
	vector := make([]float64, dims)
	vector[axis] = 1
	return vector
}

func axisVector32(dims, axis int) []float32 {
	vector := make([]float32, dims)
	vector[axis] = 1
	return vector
}

// vectorLiteral renders the inline vector literal of an axis vector, as a
// statement writes it.
func vectorLiteral(dims, axis int) string {
	literal := "["
	for i := 0; i < dims; i++ {
		if i > 0 {
			literal += ", "
		}
		if i == axis {
			literal += "1.0"
			continue
		}
		literal += "0.0"
	}
	return literal + "]"
}

func vectorInputQuery(procedure, index, neighbours, input string) string {
	return fmt.Sprintf("CALL %s('%s', %s, %s)", procedure, index, neighbours, input)
}

// requireVectorInputRows asserts a vector query's distinct result contract:
// the procedure's own columns, the entities its index searched, each entity's
// identity, and a score beside it.
func requireVectorInputRows(t *testing.T, result *ExecuteResult, entityColumn string, wantIDs []string) {
	t.Helper()
	require.NotNil(t, result)
	require.Equal(t, []string{entityColumn, "score"}, result.Columns)
	require.Len(t, result.Rows, len(wantIDs))
	for i, wantID := range wantIDs {
		switch entity := result.Rows[i][0].(type) {
		case *storage.Node:
			require.Equal(t, storage.NodeID(wantID), entity.ID)
		case *storage.Edge:
			require.Equal(t, storage.EdgeID(wantID), entity.ID)
		default:
			require.Failf(t, "entity column holds no entity", "row %d column 0 is %T", i, result.Rows[i][0])
		}
		score, ok := result.Rows[i][1].(float64)
		require.Truef(t, ok, "row %d column 1 is %T, not a float64 score", i, result.Rows[i][1])
		require.GreaterOrEqual(t, score, 0.0)
		require.LessOrEqual(t, score, 1.0)
	}
}

// TestVectorQueryInputPublicSyntaxes pins the supported third-argument
// syntaxes of both vector query procedures: inline numeric lists, named
// parameters of every supported vector type, and search text embedded
// server-side. Every syntax resolves to the same search.
func TestVectorQueryInputPublicSyntaxes(t *testing.T) {
	ctx := context.Background()
	exec := newVectorInputExecutor(t, 2)
	embedder := &mockQueryEmbedder{embedding: []float32{1, 0}}
	exec.SetEmbedder(embedder)

	inputs := []struct {
		name   string
		value  string
		params map[string]interface{}
	}{
		{name: "float literal list", value: vectorLiteral(2, 0)},
		{name: "integer literal list", value: "[1, 0]"},
		{name: "float32 parameter", value: "$q", params: map[string]interface{}{"q": []float32{1, 0}}},
		{name: "float64 parameter", value: "$q", params: map[string]interface{}{"q": []float64{1, 0}}},
		{name: "interface parameter", value: "$q", params: map[string]interface{}{"q": []interface{}{1, 0.0}}},
		{name: "single quoted search text", value: "'hello world'"},
		{name: "double quoted search text", value: `"hello world"`},
		{name: "search text parameter", value: "$q", params: map[string]interface{}{"q": "hello world"}},
	}
	for _, procedure := range vectorInputProcedures {
		wantIDs := []string{"n1", "n2"}
		if procedure.name == "queryRelationships" {
			wantIDs = []string{"r1", "r2"}
		}
		for _, input := range inputs {
			t.Run(procedure.name+"/"+input.name, func(t *testing.T) {
				result, err := exec.Execute(ctx, vectorInputQuery(procedure.procedure, procedure.index, "5", input.value), input.params)
				require.NoError(t, err)
				requireVectorInputRows(t, result, procedure.entityColumn, wantIDs)
			})
		}
	}

	// Search text reaches the embedder as the statement wrote it.
	_, err := exec.Execute(ctx, vectorInputQuery("db.index.vector.queryNodes", "doc_idx", "5", "'machine learning'"), nil)
	require.NoError(t, err)
	require.Equal(t, "machine learning", embedder.lastQuery)

	// The neighbour count limits the search.
	result, err := exec.Execute(ctx, vectorInputQuery("db.index.vector.queryRelationships", "link_idx", "1", vectorLiteral(2, 0)), nil)
	require.NoError(t, err)
	requireVectorInputRows(t, result, "relationship", []string{"r1"})
}

// TestVectorQueryInputArgumentDiagnostics pins what a malformed third argument
// reports: the actual query value's type, never an unnamed parameter.
func TestVectorQueryInputArgumentDiagnostics(t *testing.T) {
	ctx := context.Background()
	exec := newVectorInputExecutor(t, 2)

	cases := []struct {
		name   string
		value  string
		params map[string]interface{}
		want   string
	}{
		{
			name:  "integer literal",
			value: "123",
			want:  "argument query must be []float32, []float64, []interface{}, or string, not Integer",
		},
		{
			name:   "boolean parameter",
			value:  "$q",
			params: map[string]interface{}{"q": true},
			want:   "argument query must be []float32, []float64, []interface{}, or string, not Boolean",
		},
		{
			name:   "map parameter",
			value:  "$q",
			params: map[string]interface{}{"q": map[string]interface{}{"embedding": []float32{1, 0}}},
			want:   "argument query must be []float32, []float64, []interface{}, or string, not Map",
		},
		{
			name:   "null parameter",
			value:  "$q",
			params: map[string]interface{}{"q": nil},
			want:   "argument query must be []float32, []float64, []interface{}, or string, not Null",
		},
		{
			name:   "text in a coordinate list",
			value:  "$q",
			params: map[string]interface{}{"q": []interface{}{1, "bad"}},
			want:   "argument query must be numeric coordinates, not String",
		},
	}
	for _, procedure := range vectorInputProcedures {
		for _, tc := range cases {
			t.Run(procedure.name+"/"+tc.name, func(t *testing.T) {
				_, err := exec.Execute(ctx, vectorInputQuery(procedure.procedure, procedure.index, "5", tc.value), tc.params)
				require.Error(t, err)
				require.ErrorContains(t, err, procedure.procedure+": "+tc.want)
				require.NotContains(t, err.Error(), "parameter $", "an actual value has no parameter name to report")
			})
		}
	}
}

// TestVectorQueryInputOutOfRangeCoordinate keeps the established diagnostic
// for a coordinate float32 cannot represent.
func TestVectorQueryInputOutOfRangeCoordinate(t *testing.T) {
	ctx := context.Background()
	exec := newVectorInputExecutor(t, 2)

	for _, procedure := range vectorInputProcedures {
		t.Run(procedure.name, func(t *testing.T) {
			_, err := exec.Execute(ctx, vectorInputQuery(procedure.procedure, procedure.index, "5", "[1e40, 0.0]"), nil)
			require.Error(t, err)
			require.ErrorContains(t, err, "Expected representable numeric coordinates, got: 1e+40")
			require.NotContains(t, err.Error(), "parameter $")
		})
	}
}

// TestVectorQueryInputMissingParameter pins Neo4j's ParameterMissing for a
// parameter the statement never supplied: the call does not run.
func TestVectorQueryInputMissingParameter(t *testing.T) {
	ctx := context.Background()
	exec := newVectorInputExecutor(t, 2)

	for _, procedure := range vectorInputProcedures {
		t.Run(procedure.name, func(t *testing.T) {
			query := vectorInputQuery(procedure.procedure, procedure.index, "5", "$q")
			for _, params := range []map[string]interface{}{nil, {}, {"other": []float32{1, 0}}} {
				_, err := exec.Execute(ctx, query, params)
				require.ErrorContains(t, err, "Neo.ClientError.Statement.ParameterMissing: Expected parameter(s): q")
				code, message := nerrors.Neo4jStatus(err)
				require.Equal(t, "Neo.ClientError.Statement.ParameterMissing", code)
				require.Equal(t, "Expected parameter(s): q", message)
			}
		})
	}
}

// TestVectorQueryInputMissingIndex pins Neo4j's ProcedureCallFailed for a
// vector index that does not exist, for both procedures.
func TestVectorQueryInputMissingIndex(t *testing.T) {
	ctx := context.Background()
	exec := newVectorInputExecutor(t, 2)

	for _, procedure := range vectorInputProcedures {
		t.Run(procedure.name, func(t *testing.T) {
			_, err := exec.Execute(ctx, vectorInputQuery(procedure.procedure, "missing_idx", "5", vectorLiteral(2, 0)), nil)
			require.ErrorContains(t, err, "There is no such vector schema index: missing_idx")
			code, _ := nerrors.Neo4jStatus(err)
			require.Equal(t, "Neo.ClientError.Procedure.ProcedureCallFailed", code)
		})
	}
}

// TestVectorQueryInputEmbedErrors pins the search text diagnostics: a missing
// embedder names the search text, and an embedder failure keeps its cause.
func TestVectorQueryInputEmbedErrors(t *testing.T) {
	ctx := context.Background()

	t.Run("no embedder", func(t *testing.T) {
		exec := newVectorInputExecutor(t, 2)
		for _, procedure := range vectorInputProcedures {
			t.Run(procedure.name, func(t *testing.T) {
				for _, value := range []string{"'hello'", "$q"} {
					var params map[string]interface{}
					if value == "$q" {
						params = map[string]interface{}{"q": "hello"}
					}
					_, err := exec.Execute(ctx, vectorInputQuery(procedure.procedure, procedure.index, "5", value), params)
					require.ErrorContains(t, err, "string query provided but no embedder configured")
					var localizedErr *localization.LocalizedError
					require.ErrorAs(t, err, &localizedErr)
					// Each procedure keeps its own message identity.
					want := localization.MessageCypherProceduresStringQueryEmbedderRequired
					if procedure.name == "queryNodes" {
						want = localization.MessageCypherSpecializedCallsStringQueryEmbedderRequired
					}
					require.Equal(t, want, localizedErr.Message.ID)
				}
			})
		}
	})

	t.Run("embedder failure", func(t *testing.T) {
		exec := newVectorInputExecutor(t, 2)
		cause := errors.New("forced embed error")
		exec.SetEmbedder(&failingEmbedder{err: cause})
		for _, procedure := range vectorInputProcedures {
			t.Run(procedure.name, func(t *testing.T) {
				for _, value := range []string{"'hello'", "$q"} {
					var params map[string]interface{}
					if value == "$q" {
						params = map[string]interface{}{"q": "hello"}
					}
					_, err := exec.Execute(ctx, vectorInputQuery(procedure.procedure, procedure.index, "5", value), params)
					require.ErrorContains(t, err, "failed to embed query 'hello': forced embed error")
					require.ErrorIs(t, err, cause, "the embedder failure keeps its identity")
					var localizedErr *localization.LocalizedError
					require.ErrorAs(t, err, &localizedErr)
					want := localization.MessageCypherProceduresEmbedQueryFailed
					if procedure.name == "queryNodes" {
						want = localization.MessageCypherSpecializedCallsEmbedQueryTextFailed
					}
					require.Equal(t, want, localizedErr.Message.ID)
				}
			})
		}
	})
}

// TestVectorQueryInputDimensions pins the index's dimensions as the search's
// dimensions: a query vector of another length matches nothing.
func TestVectorQueryInputDimensions(t *testing.T) {
	ctx := context.Background()
	exec := newVectorInputExecutor(t, 2)

	for _, procedure := range vectorInputProcedures {
		t.Run(procedure.name, func(t *testing.T) {
			result, err := exec.Execute(ctx, vectorInputQuery(procedure.procedure, procedure.index, "5", "[1.0, 0.0, 0.0]"), nil)
			require.NoError(t, err)
			require.NotNil(t, result)
			require.Empty(t, result.Rows)
			require.Equal(t, []string{procedure.entityColumn, "score"}, result.Columns)
		})
	}
}

// TestVectorQueryInputCancellation pins what a caller's cancellation does to
// each procedure's own search: the statement fails with the caller's cause.
func TestVectorQueryInputCancellation(t *testing.T) {
	exec := newVectorInputExecutor(t, 2)

	for _, procedure := range vectorInputProcedures {
		t.Run(procedure.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			result, err := exec.Execute(ctx, vectorInputQuery(procedure.procedure, procedure.index, "5", vectorLiteral(2, 0)), nil)
			require.ErrorIs(t, err, context.Canceled)
			require.Nil(t, result)
		})
	}
}

// TestVectorQueryInputEmptySearchText pins the diagnostics of an empty search
// text: an empty third argument names no query input, in each procedure's own
// message identity, whether or not an embedder is configured.
func TestVectorQueryInputEmptySearchText(t *testing.T) {
	ctx := context.Background()
	for _, withEmbedder := range []bool{false, true} {
		exec := newVectorInputExecutor(t, 2)
		if withEmbedder {
			exec.SetEmbedder(&mockQueryEmbedder{embedding: []float32{1, 0}})
		}
		for _, procedure := range vectorInputProcedures {
			t.Run(fmt.Sprintf("%s/embedder=%t", procedure.name, withEmbedder), func(t *testing.T) {
				_, err := exec.Execute(ctx, vectorInputQuery(procedure.procedure, procedure.index, "5", "''"), nil)
				require.Error(t, err)
				require.ErrorContains(t, err, "no query vector or search text provided")
				var localizedErr *localization.LocalizedError
				require.ErrorAs(t, err, &localizedErr)
				want := localization.MessageCypherProceduresQueryInputRequired
				if procedure.name == "queryNodes" {
					want = localization.MessageCypherSpecializedCallsQueryInputRequired
				}
				require.Equal(t, want, localizedErr.Message.ID)
			})
		}
	}
}
