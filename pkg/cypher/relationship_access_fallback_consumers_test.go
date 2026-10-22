package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// The row/shared evaluator boundary must keep the admitted statement when a
// function evaluates a nested executable expression. The expected values are
// independent scalar results; every observation uses public Cypher dispatch.
func TestRelationshipAccessProgramFallbackConsumers(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "fallback_consumers"))
	_, err := exec.Execute(context.Background(), "CREATE (:FallbackNode)-[:FALLBACK_EDGE {key:'one'}]->(:FallbackNode)", nil)
	require.NoError(t, err)
	tests := []struct {
		name, query string
		want        interface{}
	}{
		{"registered_type", "RETURN valueType(COUNT { RETURN 1 }) AS value", "INTEGER NOT NULL"},
		{"registered_null_if", "RETURN nullIf(COUNT { RETURN 1 }, 1) AS value", nil},
		{"registered_point", "RETURN point({x: COUNT { RETURN 1 }, y: 0}).x AS value", 1.0},
		{"registered_trim_collect", "RETURN trim(head(COLLECT { UNWIND [' a '] AS s RETURN s })) AS value", "a"},
		{"nested_coalesce", "RETURN coalesce(null, trim(head(COLLECT { UNWIND [' a '] AS s RETURN s }))) AS value", "a"},
		{"nested_size", "RETURN size(trim(head(COLLECT { UNWIND [' ab '] AS s RETURN s }))) AS value", int64(2)},
		{"case_result", "RETURN CASE WHEN true THEN valueType(COUNT { RETURN 1 }) ELSE 'wrong' END AS value", "INTEGER NOT NULL"},
		{"case_condition", "RETURN CASE WHEN COUNT { RETURN 1 } = 1 THEN trim(head(COLLECT { RETURN ' a ' })) ELSE 'wrong' END AS value", "a"},
		{"computed_binding", "UNWIND [1] AS x RETURN valueType(COUNT { WITH x RETURN x }) AS value", "INTEGER NOT NULL"},
		{"computed_where", "UNWIND [1] AS x WITH x WHERE trim(head(COLLECT { RETURN ' a ' })) = 'a' RETURN x AS value", int64(1)},
		{"list_scope", "RETURN head([x IN [1] | valueType(COUNT { WITH x RETURN x })]) AS value", "INTEGER NOT NULL"},
		{"relationship_source", "RETURN valueType(COUNT { MATCH ()-[r:FALLBACK_EDGE {key:$key}]->() RETURN r }) AS value", "INTEGER NOT NULL"},
		{"relationship_case", "MATCH (a:FallbackNode)-[r:FALLBACK_EDGE]->() RETURN CASE WHEN COUNT { MATCH (a)-[r:FALLBACK_EDGE]->() RETURN r } = 1 THEN 'one' ELSE 'wrong' END AS value", "one"},
	}
	for _, mode := range []string{"ordinary", "profile", "call"} {
		for _, test := range tests {
			t.Run(mode+"/"+test.name, func(t *testing.T) {
				defer func() {
					if failure := recover(); failure != nil {
						t.Errorf("public statement panicked: %v", failure)
					}
				}()
				query := test.query
				switch mode {
				case "profile":
					query = "PROFILE " + query
				case "call":
					query = "CALL { " + query + " } RETURN value"
				}
				result, err := exec.Execute(context.Background(), query, map[string]interface{}{"key": "one"})
				require.NoError(t, err)
				require.Equal(t, []string{"value"}, result.Columns)
				require.Equal(t, [][]interface{}{{test.want}}, result.Rows)
			})
		}
	}
}
