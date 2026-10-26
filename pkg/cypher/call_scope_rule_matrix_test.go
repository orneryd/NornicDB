package cypher

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/internal/callscopetest"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Scope readers may ask the shared value about a binding; they may not
// reintroduce a local implicit-import classifier or mutate its carried scope.
func TestCallScopeUsesBindingValue(t *testing.T) {
	for _, source := range []string{"match_semantic_validation.go", "expression_subquery_scope.go", "static_type_scope.go", "pipeline_call_operator.go", "subquery_value_expression.go"} {
		file, err := parser.ParseFile(token.NewFileSet(), source, nil, 0)
		require.NoError(t, err)
		ast.Inspect(file, func(node ast.Node) bool {
			if selector, ok := node.(*ast.SelectorExpr); ok && (selector.Sel.Name == "implicit" || selector.Sel.Name == "carried") {
				t.Errorf("%s re-derives binding provenance outside semanticBodyScope: %s", source, selector.Sel.Name)
			}
			return true
		})
	}
	// All in-scope consumers may read owner facts; only the body owner writes
	// import provenance/current membership. Runtime projections must ask for
	// declared columns instead of rediscovering a body's schema from row keys.
	for _, source := range []string{"semantic_body_syntax.go", "result_columns_compile.go", "result_columns_bound.go", "match_semantic_validation.go", "pipeline_projection.go", "pipeline_row_stream.go", "pipeline_shared_clauses.go", "create_semantic_validation.go", "merge_semantic_validation.go", "set_semantic_validation.go", "query_structure_scope.go"} {
		file, err := parser.ParseFile(token.NewFileSet(), source, nil, 0)
		require.NoError(t, err)
		ast.Inspect(file, func(node ast.Node) bool {
			if assignment, ok := node.(*ast.AssignStmt); ok {
				for _, left := range assignment.Lhs {
					if index, ok := left.(*ast.IndexExpr); ok {
						left = index.X
					}
					if selector, ok := left.(*ast.SelectorExpr); ok && (selector.Sel.Name == "entry" || selector.Sel.Name == "implicit" || selector.Sel.Name == "carried") {
						t.Errorf("%s writes body membership outside its owner: %s", source, selector.Sel.Name)
					}
				}
			}
			if call, ok := node.(*ast.CallExpr); ok {
				if name, ok := call.Fun.(*ast.Ident); ok && name.Name == "pipelineWildcardColumns" {
					t.Errorf("%s infers body projection columns from rows", source)
				}
			}
			return true
		})
	}
}

func TestCallScopeRuleMatrix(t *testing.T) {
	uri := os.Getenv("NORNICDB_NEO4J_REFERENCE_URI")
	if uri == "" {
		t.Skip("requires the Neo4j scope reference")
	}
	ctx := context.Background()
	driver, err := neo4j.NewDriverWithContext(uri, neo4j.NoAuth())
	require.NoError(t, err)
	defer driver.Close(ctx)
	session := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "neo4j"})
	defer session.Close(ctx)
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "scope-rules"))
	reference := func(query string) callScopeOracleOutcome {
		result, err := session.Run(ctx, query, nil)
		out := callScopeOracleOutcome{}
		if err == nil {
			out.Columns, err = result.Keys()
		}
		if err == nil {
			var records []*neo4j.Record
			records, err = result.Collect(ctx)
			for _, record := range records {
				out.Rows = append(out.Rows, record.Values)
			}
		}
		if err != nil {
			var status *neo4j.Neo4jError
			require.True(t, errors.As(err, &status), "reference transport failure: %v", err)
			out.Code, out.Message = status.Code, status.Msg
		}
		return out
	}
	setup := "CREATE (:CallScopeRuleTarget {n:1}),(:CallScopeRuleTarget {n:2})"
	require.Empty(t, reference(setup).Code)
	_, err = exec.Execute(ctx, setup, nil)
	require.NoError(t, err)
	defer reference("MATCH (n:CallScopeRuleTarget) DETACH DELETE n")
	mismatches := 0
	cases := callscopetest.RuleCases()
	for _, item := range cases {
		original := reference(item.Reference)
		want := original
		if item.Compatibility != "" {
			want = reference(item.Compatibility)
		}
		result, err := exec.Execute(ctx, item.Query, nil)
		got := callScopeOracleOutcome{}
		if err != nil {
			got.Code, got.Message = nornicerrors.Neo4jStatus(err)
		} else if result != nil {
			got.Columns, got.Rows = result.Columns, result.Rows
		}
		if !got.equal(want) {
			mismatches++
			record, _ := json.Marshal(map[string]any{"case": item.Name, "query": item.Query, "control": item.Compatibility, "extension": item.Extension, "original": original, "expected": want, "actual": got})
			fmt.Println("CALL_SCOPE_RULE_MISMATCH " + string(record))
		}
	}
	fmt.Printf("CALL_SCOPE_RULES cases=%d mismatches=%d\n", len(cases), mismatches)
	require.Zero(t, mismatches)
}
