package cypher

// H2 variant of c9cb917a9 "Stop long expression work when statements are
// canceled" (units H2-0456 staticOperatorChecker.check and the changed
// functions of functions_eval_functions.go, operators.go, range_semantics.go,
// row_function_expression.go, row_list_loops.go, row_subscript_validation.go,
// runtime_type_errors.go). The fix's own assertion
// (expression_cancellation_test.go requireCanceledWithinBudget: the public
// Execute under the caller's deadline ends as context.DeadlineExceeded within
// statementCancellationBudget after the deadline) applied to the sibling
// expression shapes the regression test does not cover: comprehensions,
// quantifiers, list functions over range(), operator chains of other operand
// types and operators, chains in WHERE and SET, and reduce in other clauses.
// A shape overrunning the budget is a lead only if it overruns on three runs.

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

type h2CancelShape struct {
	name   string
	query  string
	params bool // supply $items (500,000 integers)
}

func h2Chain(operand, operator string, count int) string {
	return strings.Repeat(operand+" "+operator+" ", count) + operand
}

func h2CancelShapes() []h2CancelShape {
	return []h2CancelShape{
		{name: "list-comprehension-projection", query: "RETURN [x IN $items | size(randomUUID())] AS v", params: true},
		{name: "list-comprehension-filter", query: "RETURN [x IN $items WHERE size(randomUUID()) > 0] AS v", params: true},
		{name: "any-quantifier", query: "RETURN any(x IN $items WHERE size(randomUUID()) < 0) AS v", params: true},
		{name: "all-quantifier", query: "RETURN all(x IN $items WHERE size(randomUUID()) > 0) AS v", params: true},
		{name: "none-quantifier", query: "RETURN none(x IN $items WHERE size(randomUUID()) < 0) AS v", params: true},
		{name: "single-quantifier", query: "RETURN single(x IN $items WHERE size(randomUUID()) < 0) AS v", params: true},
		{name: "reduce-in-where", query: "UNWIND [1] AS y WITH y WHERE reduce(s = 0, x IN $items | s + size(randomUUID())) > 0 RETURN y", params: true},
		{name: "reduce-in-set", query: "CREATE (n:H2Cancel) SET n.v = reduce(s = 0, x IN $items | s + size(randomUUID())) RETURN n.v", params: true},
		{name: "reduce-string-fold", query: "RETURN size(reduce(s = '', x IN $items | s + randomUUID())) AS v", params: true},
		{name: "range-in-comprehension", query: "RETURN size([x IN range(1, 5000000) | x + 1]) AS v"},
		{name: "range-size", query: "RETURN size(range(1, 5000000)) AS v"},
		{name: "range-reverse", query: "RETURN size(reverse(range(1, 5000000))) AS v"},
		{name: "range-tail", query: "RETURN size(tail(range(1, 5000000))) AS v"},
		{name: "range-step", query: "RETURN size(range(5000000, 1, -1)) AS v"},
		{name: "range-slice", query: "RETURN size(range(1, 5000000)[1..]) AS v"},
		{name: "range-in-list", query: "RETURN 0 IN range(1, 5000000) AS v"},
		{name: "range-concat", query: "RETURN size(range(1, 3000000) + range(1, 3000000)) AS v"},
		{name: "unwind-range-count", query: "UNWIND range(1, 5000000) AS x RETURN count(x) AS v"},
		{name: "unwind-items-collect", query: "UNWIND $items AS x WITH collect(randomUUID()) AS c RETURN size(c) AS v", params: true},
		{name: "integer-chain", query: "RETURN " + h2Chain("1", "+", 722) + " AS v"},
		{name: "float-chain", query: "RETURN " + h2Chain("1.5", "*", 722) + " AS v"},
		{name: "boolean-and-chain", query: "RETURN " + h2Chain("true", "AND", 722) + " AS v"},
		{name: "boolean-or-chain", query: "RETURN " + h2Chain("false", "OR", 722) + " AS v"},
		{name: "comparison-or-chain-in-where", query: "UNWIND [1] AS y WITH y WHERE " + strings.TrimSuffix(strings.Repeat("y = 2 OR ", 722), " OR ") + " RETURN y"},
		{name: "list-concat-chain", query: "RETURN size(" + h2Chain("[1]", "+", 722) + ") AS v"},
		{name: "string-chain-in-with", query: "WITH " + expressionChainText(722) + " AS v RETURN size(v)"},
		{name: "string-chain-in-where", query: "UNWIND [1] AS y WITH y WHERE " + expressionChainText(722) + " = 'z' RETURN y"},
		{name: "string-chain-in-set", query: "CREATE (n:H2Cancel) SET n.k = " + expressionChainText(722) + " RETURN n.k"},
		{name: "string-chain-in-merge", query: "MERGE (n:H2Cancel {k: " + expressionChainText(722) + "}) RETURN n.k"},
		{name: "string-chain-in-unwind", query: "UNWIND [" + expressionChainText(722) + "] AS v RETURN v"},
		{name: "parameter-chain", query: "RETURN " + h2Chain("$s", "+", 722) + " AS v"},
	}
}

func TestHuntH2ExpressionCancellationSiblings(t *testing.T) {
	items := make([]interface{}, 500000)
	for i := range items {
		items[i] = int64(i)
	}
	cases, leads := 0, 0
	const deadline = 20 * time.Millisecond
	forEachParser(t, func(t *testing.T) {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "h2_cancel"))
		for _, shape := range h2CancelShapes() {
			t.Run(shape.name, func(t *testing.T) {
				cases++
				params := map[string]interface{}{"s": "secret"}
				if shape.params {
					params["items"] = items
				}
				var overruns []string
				var lastErr string
				finished := false
				for run := 0; run < 3; run++ {
					ctx, cancel := context.WithTimeout(context.Background(), deadline)
					start := time.Now()
					_, err := exec.Execute(ctx, shape.query, params)
					elapsed := time.Since(start)
					cancel()
					if err == nil {
						finished = true
						if elapsed-deadline > statementCancellationBudget {
							overruns = append(overruns, fmt.Sprintf("success after %s", elapsed))
						}
						continue
					}
					if !errors.Is(err, context.DeadlineExceeded) {
						lastErr = err.Error()
						if len(lastErr) > 300 {
							lastErr = lastErr[:300]
						}
						overruns = append(overruns, fmt.Sprintf("other error after %s", elapsed))
						continue
					}
					if elapsed-deadline > statementCancellationBudget {
						overruns = append(overruns, fmt.Sprintf("deadline reported after %s", elapsed))
					}
				}
				if len(overruns) == 3 {
					leads++
					h2Lead(map[string]any{"harness": "cancel", "unit": "c9cb917a9", "shape": shape.name, "query": h2Trim(shape.query), "deadline": deadline.String(), "budget": statementCancellationBudget.String(), "runs": overruns, "finished_once": finished, "last_error": lastErr})
				} else {
					fmt.Printf("HUNT_NOTE H2 cancel %s overruns=%d finished=%v\n", shape.name, len(overruns), finished)
				}
			})
		}
	})
	h2Coverage(map[string]any{"harness": "cancel", "commit": "c9cb917a9", "cases": cases, "leads": leads})
}

func h2Trim(text string) string {
	if len(text) > 400 {
		return text[:400] + fmt.Sprintf("...(%d bytes)", len(text))
	}
	return text
}
