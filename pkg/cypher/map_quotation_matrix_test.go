package cypher

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// These cases are generated from lexical classes, independently of the map
// reader: all delimiters, both string quotes, escaped symbolic names, Unicode,
// whitespace and comment-looking data occur in keys and evaluated values.
func mapQuotationCases() []struct{ name, text, key, value string } {
	atoms := []string{",", ":", "{", "}", "[", "]", "(", ")", "'", "\"", "`", "a`b,c", "雪λ", " \t\n", "/*,:{}[]()*/", "//,:{}[]()\n"}
	value := "v,:{}[]()'\"`a``b 雪λ\t\n/*data*/ //data"
	single := "'" + strings.NewReplacer("\\", "\\\\", "'", "\\'", "\n", "\\n", "\t", "\\t").Replace(value) + "'"
	double := "\"" + strings.NewReplacer("\\", "\\\\", "\"", "\\\"", "\n", "\\n", "\t", "\\t").Replace(value) + "\""
	values := []string{single, double, "(" + single + ")", "[" + single + ",'other,:{}[]()'][0]", "{`inner,``key`:" + single + "}['inner,`key']"}
	var cases []struct{ name, text, key, value string }
	for i, atom := range atoms {
		key := "k" + atom + "z"
		quoted := "`" + strings.ReplaceAll(key, "`", "``") + "`"
		for j, expression := range values {
			for layout, text := range []string{
				"{" + quoted + ":" + expression + ",keep:7,tail:2}",
				"{ \n\t" + quoted + " \t: " + expression + " , keep : 7 , tail : 2 \n}",
				"{/* ,:{}[]()'\"` */ " + quoted + " /* : , */ : // ,:{}[]()'\"`\n " + expression + " /* , : */ , /* { */ keep:7, tail:2 /* } */}",
			} {
				cases = append(cases, struct{ name, text, key, value string }{fmt.Sprintf("key%d/value%d/layout%d", i, j, layout), text, key, value})
			}
		}
	}
	return cases
}

func TestMapQuotationConsumers(t *testing.T) {
	e := NewStorageExecutor(newTestMemoryEngine(t))
	for _, tc := range mapQuotationCases() {
		t.Run(tc.name, func(t *testing.T) {
			want := map[string]interface{}{tc.key: tc.value, "keep": int64(7), "tail": int64(2)}
			for _, consumer := range []string{"context", "property-map", "set-strict", "set-expressions", "set-list", "syntax-plan", "pattern-template", "literal-plan", "row", "compiled", "map-projection"} {
				t.Run(consumer, func(t *testing.T) {
					// Repetition exercises cached syntax with a fresh evaluation slot.
					for repeat := 0; repeat < 2; repeat++ {
						ctx := withExpressionFailureSlot(context.Background())
						var got interface{}
						var err error
						switch consumer {
						case "context":
							got = e.evaluateMapLiteralFull(ctx, originalBraceView(tc.text), nil, nil, nil, nil, nil, 0)
							err = getExpressionFailure(ctx)
						case "property-map":
							got, err = e.materializePropertyMap(ctx, originalBraceView(tc.text), nil, true, false)
						case "set-strict":
							got, err = e.parseSetMergeMapLiteralStrict(ctx, originalBraceView(tc.text))
						case "set-expressions":
							var expressions map[string]string
							expressions, err = parseSetMergeMapExpressionsStrict(originalBraceView(tc.text))
							result := make(map[string]interface{}, len(expressions))
							for key, expression := range expressions {
								var value interface{}
								value, err = e.checkedPropertyExpression(ctx, originalBraceView(expression), nil)
								if err != nil {
									break
								}
								result[key] = value
							}
							got = result
						case "set-list":
							assignments := setAssignmentViews(originalBraceView("n = " + tc.text + ", n.keep = 8"))
							require.Len(t, assignments, 2)
							item := readSetAssignment(assignments[0])
							require.Equal(t, "n", item.target)
							require.Empty(t, item.property)
							require.Equal(t, "=", item.operator)
							got, err = e.checkedPropertyExpression(ctx, item.right.trim(), nil)
						case "syntax-plan":
							var plan []pipelinePropertyExpression
							plan, err = parsePropertyExpressions(originalBraceView(tc.text))
							if err == nil {
								got, err = e.materializePropertyExpressions(ctx, plan, nil, true, false)
							}
						case "pattern-template":
							plan, matched := e.pipelinePropertyExpressions(originalBraceView(tc.text))
							require.True(t, matched)
							got, err = e.evaluatePipelineProperties(ctx, plan, nil)
						case "literal-plan":
							got = parseLiteralMapForPipeline(originalBraceView(tc.text))
							if !strings.Contains(tc.name, "/value0/") && !strings.Contains(tc.name, "/value1/") {
								require.Nil(t, got, "non-literal syntax declines before evaluation")
								continue
							}
						case "row":
							var resolved bool
							got, resolved, err = e.evaluateRowValue(originalBraceView(tc.text), nil)
							require.True(t, resolved)
						case "compiled":
							compiler := rowValueCompiler{}
							root, _ := compiler.compile(originalBraceView(tc.text))
							var resolved bool
							got, resolved, err = root.eval(e, nil, nil)
							require.True(t, resolved)
						case "map-projection":
							var resolved bool
							got, resolved, err = e.evaluateRowValue(originalBraceView("base"+tc.text), map[string]interface{}{"base": map[string]interface{}{}})
							require.True(t, resolved)
						}
						require.NoError(t, err, tc.text)
						require.Equal(t, want, got, tc.text)
					}
				})
			}
		})
	}
}

func mapQuotationPublicForms(text string) []string {
	return []string{
		"RETURN " + text + " AS m",
		"CREATE (n:QuotedMaps " + text + ") RETURN properties(n) AS m",
		"CREATE ()-[r:QuotedMaps " + text + "]->() RETURN properties(r) AS m",
		"CREATE (n:QuotedMaps) SET n = " + text + " RETURN properties(n) AS m",
		"CREATE (n:QuotedMaps) SET n += " + text + " RETURN properties(n) AS m",
		"MERGE (n:QuotedMaps " + text + ") RETURN properties(n) AS m",
		"MERGE (n:QuotedMaps {id:1}) ON CREATE SET n = " + text + " RETURN properties(n) AS m",
		"CREATE (:QuotedMaps {id:1}) WITH 1 AS x MERGE (n:QuotedMaps {id:1}) ON MATCH SET n = " + text + " RETURN properties(n) AS m",
	}
}

func TestMapQuotationPublicForms(t *testing.T) {
	for _, tc := range mapQuotationCases() {
		t.Run(tc.name, func(t *testing.T) {
			for route, query := range mapQuotationPublicForms(tc.text) {
				t.Run(fmt.Sprint(route), func(t *testing.T) {
					e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "quoted_maps"))
					result, err := e.Execute(context.Background(), query, nil)
					require.NoError(t, err, query)
					require.Equal(t, [][]interface{}{{map[string]interface{}{tc.key: tc.value, "keep": int64(7), "tail": int64(2)}}}, result.Rows, query)
				})
			}
		})
	}
}

func TestMapQuotationRejectsWithoutWrites(t *testing.T) {
	for _, text := range []string{"{`first,last`:properties(1)}", "{`a``b,c`:1/0}", "{`first,last`:1,}", "{`first,last`:1,,keep:2}", "{`first,last`:}", "{`first,last` 1}"} {
		for route, query := range mapQuotationPublicForms(text) {
			t.Run(fmt.Sprintf("%s/%d", text, route), func(t *testing.T) {
				store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "quoted_no_writes")
				e := NewStorageExecutor(store)
				_, err := e.Execute(context.Background(), "CREATE (:Keep {keep:7})", nil)
				require.NoError(t, err)
				before := propertyDurableSnapshot(t, store)
				_, err = e.Execute(context.Background(), query, nil)
				require.Error(t, err, query)
				require.Equal(t, before, propertyDurableSnapshot(t, store), query)
			})
		}
	}
}

func TestMapQuotationFallbackIsBounded(t *testing.T) {
	e := NewStorageExecutor(newTestMemoryEngine(t))
	for _, expr := range []string{"[1][0]]", "['escaped\\' quote'][0]]"} {
		ctx := withExpressionFailureSlot(context.Background())
		value, err := e.checkedPropertyExpression(ctx, originalBraceView(expr), nil)
		require.Error(t, err, expr)
		require.Nil(t, value)
		require.Error(t, getExpressionFailure(ctx), expr)
	}
}
