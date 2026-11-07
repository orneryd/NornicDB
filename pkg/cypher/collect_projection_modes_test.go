package cypher

import (
	"reflect"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func TestCollectSubqueryProjectionContractAcrossExecutionModes(t *testing.T) {
	withCypherParsers(t, func(t *testing.T) {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "collect_projection_modes"))
		for _, mode := range []string{"", "EXPLAIN ", "PROFILE "} {
			for _, prefix := range []string{"", "UNWIND [] AS ignored "} {
				one, two := "WITH 1 AS x RETURN *", "WITH 1 AS x, 2 AS y RETURN *"
				if prefix != "" {
					// The outer variable is in the COLLECT scope and contributes
					// one wildcard column even when the outer stream is empty.
					one, two = "RETURN *", "WITH 1 AS x RETURN *"
				}
				for _, tc := range []struct {
					name, body string
					valid      bool
				}{
					{"one wildcard", one, false},
					{"one explicit", "WITH 1 AS value RETURN value", true},
					{"two", two, false},
				} {
					t.Run(mode+prefix+tc.name, func(t *testing.T) {
						q := mode + prefix + "RETURN COLLECT { " + tc.body + " } AS values"
						result, err := exec.Execute(t.Context(), q, nil)
						if (err == nil) != tc.valid {
							t.Fatalf("query=%q err=%v valid=%v", q, err, tc.valid)
						}
						if err != nil {
							requireStatusCode(t, err, "Neo.ClientError.Statement.SyntaxError")
						}
						if err == nil && mode == "" && prefix == "" && tc.valid && !reflect.DeepEqual(result.Rows, [][]interface{}{{[]interface{}{int64(1)}}}) {
							t.Fatalf("query=%q returned %v", q, result.Rows)
						}
					})
				}
			}
		}
	})
}
