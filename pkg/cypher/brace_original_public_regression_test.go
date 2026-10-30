package cypher

import (
	"testing"

	nerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// Author: #1663 implementation lane. This identical public-API overlay also
// compiles on release d663ce8f1; it does not require the new owner API.
func TestOriginalBraceAdmissionPublicConsumers(t *testing.T) {
	for _, item := range []struct{ name, query string }{
		{"literal", "UNWIND [] AS row RETURN {key: 1,} AS out"},
		{"nested_literal", "UNWIND [] AS row RETURN {key: {inner: 1,}} AS out"},
		{"projection", "WITH {key: 1} AS n UNWIND [] AS row RETURN n{.key,} AS out"},
		{"nested_projection", "WITH {key: 1} AS n UNWIND [] AS row RETURN {key: n{.key,}} AS out"},
		{"pattern_properties", "UNWIND [] AS row MATCH (n {key: 1,}) RETURN n AS out"},
		{"relationship_properties", "UNWIND [] AS row MATCH ()-[r {key: 1,}]->() RETURN r AS out"},
		{"rewrite_properties", "UNWIND [] AS row MATCH (a)-->(b {key: a.key,}) RETURN b AS out"},
		{"subquery", "UNWIND [] AS row CALL { RETURN {key: 1,} AS out } RETURN out"},
		{"nested_subquery", "UNWIND [] AS row RETURN COUNT { WITH {key: 1,} AS n RETURN n } AS out"},
		{"projection_pipeline", "UNWIND [] AS row WITH {key: 1,} AS out RETURN out"},
		{"create_properties", "UNWIND [] AS row CREATE (n {key: 1,}) RETURN n AS out"},
		{"merge_properties", "UNWIND [] AS row MERGE (n {key: 1,}) RETURN n AS out"},
		{"set_map", "UNWIND [] AS row CREATE (n) SET n += {key: 1,} RETURN n AS out"},
		{"call_input", "UNWIND [] AS row CALL db.labels({key: 1,}) YIELD label RETURN label AS out"},
	} {
		t.Run(item.name, func(t *testing.T) {
			t.Run("Parser.Parse", func(t *testing.T) {
				if _, err := NewParser().Parse(item.query); err == nil {
					t.Fatal("public parser admitted malformed original brace")
				}
			})
			t.Run("ASTBuilder.Build", func(t *testing.T) {
				if _, err := NewASTBuilder().Build(item.query); err == nil {
					t.Fatal("public AST builder admitted malformed original brace")
				}
			})
			executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "brace_original_public"))
			_, err := executor.Execute(t.Context(), item.query, nil)
			code, _ := nerrors.Neo4jStatus(err)
			if code != "Neo.ClientError.Statement.SyntaxError" {
				t.Fatalf("original malformed brace must be rejected before empty execution; got %v", err)
			}
		})
	}
}
