package fnboundary_test

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher"
	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// Author: #1663 implementation lane. The external compatibility contract was
// decided by orchestrator Nornic2: Eval(text) is a fresh expression in the row
// scope. This isolated test package prevents its external registry override
// from changing other packages' built-in registrations. The same public-only
// overlay compiles on release d663ce8f1.
func TestExternalEvalStandaloneCompatibility(t *testing.T) {
	for _, item := range []struct {
		name, query, fabricated, want string
		invalid                       bool
	}{
		{"equal_arguments", "RETURN [reveal({x: 1}), reveal({x: 1})] AS out", "", `[[[{"x":1},{"x":1}]]]`, false},
		{"fabricated_expression", "WITH 40 AS n RETURN reveal(n) AS out", "n + 2", `[[42]]`, false},
		{"invalid_expression", "RETURN reveal(0) AS out", "1 / 0", "", true},
	} {
		t.Run(item.name, func(t *testing.T) {
			calls := 0
			cypherfn.Register("reveal", func(ctx cypherfn.Context, args []string) (interface{}, error) {
				calls++
				text := args[0]
				if item.fabricated != "" {
					text = item.fabricated
				}
				first, err := ctx.Eval(text)
				if err != nil {
					return nil, err
				}
				second, err := ctx.Eval(text)
				if err != nil {
					return nil, err
				}
				if !reflect.DeepEqual(first, second) {
					return nil, fmt.Errorf("equal external expression calls differ: %v and %v", first, second)
				}
				return first, nil
			})
			executor := cypher.NewStorageExecutor(storage.NewMemoryEngine())
			result, err := executor.Execute(context.Background(), item.query, nil)
			if calls == 0 {
				t.Fatal("public execution did not enter the external Func boundary")
			}
			if item.invalid {
				// Neo4j's (and release's) integer division error is "/ by zero".
				if err == nil || !strings.Contains(err.Error(), "/ by zero") {
					t.Fatalf("invalid expression: %v", err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				bytes, marshalErr := json.Marshal(result.Rows)
				if marshalErr != nil || string(bytes) != item.want {
					t.Fatalf("rows=%s error=%v want=%s", bytes, marshalErr, item.want)
				}
			}
			var rows interface{}
			if result != nil {
				rows = result.Rows
			}
			message := ""
			if err != nil {
				message = err.Error()
			}
			receipt, _ := json.Marshal(map[string]interface{}{"author": "#1663 implementation lane", "case": item.name, "query": item.query, "rows": rows, "error": message, "external_calls": calls})
			t.Logf("EXTERNAL_COMPAT %s", receipt)
		})
	}
}
