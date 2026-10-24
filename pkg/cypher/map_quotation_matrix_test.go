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
							fixtureAdmission974Context, fixtureAdmission974 := expressionTestContext(ctx, tc.text)
							_ = fixtureAdmission974Context
							got = e.evaluateMapLiteralFull(fixtureAdmission974, fixtureAdmission974Context, tc.text, nil, nil, nil, nil, nil, 0)
							err = getExpressionFailure(ctx)
						case "property-map":
							fixtureAdmission1648Context, fixtureAdmission1648 := statementTestContext(ctx, tc.text)
							_ = fixtureAdmission1648Context
							got, err = e.materializePropertyMap(fixtureAdmission1648, fixtureAdmission1648Context, tc.text, nil, true, false)
						case "set-strict":
							fixtureAdmission1649Context, fixtureAdmission1649 := expressionTestContext(ctx, tc.text)
							_ = fixtureAdmission1649Context
							got, err = e.parseSetMergeMapLiteralStrict(fixtureAdmission1649, fixtureAdmission1649Context, tc.text)
						case "set-expressions":
							var expressions map[string]string
							expressions, err = parseSetMergeMapExpressionsStrict(tc.text)
							result := make(map[string]interface{}, len(expressions))
							for key, expression := range expressions {
								var value interface{}
								fixtureAdmission993Context, fixtureAdmission993 := expressionTestContext(ctx, expression)
								_ = fixtureAdmission993Context
								value, err = e.checkedPropertyExpression(fixtureAdmission993, fixtureAdmission993Context, expression, nil)
								if err != nil {
									break
								}
								result[key] = value
							}
							got = result
						case "set-list":
							assignments := splitSetAssignments("n = " + tc.text + ", n.keep = 8")
							require.Len(t, assignments, 2)
							target, property, operator, expression := splitSetAssignment(assignments[0])
							require.Equal(t, "n", target)
							require.Empty(t, property)
							require.Equal(t, "=", operator)
							fixtureAdmission994Context, fixtureAdmission994 := expressionTestContext(ctx, expression)
							_ = fixtureAdmission994Context
							got, err = e.checkedPropertyExpression(fixtureAdmission994, fixtureAdmission994Context, expression, nil)
						case "syntax-plan":
							var plan []pipelinePropertyExpression
							plan, err = parsePropertyExpressions(tc.text)
							if err == nil {
								fixtureAdmission1902Context, fixtureAdmission1902 := statementTestContext(ctx, "RETURN "+(tc.text))
								_ = fixtureAdmission1902Context
								got, err = e.materializePropertyExpressions(fixtureAdmission1902, fixtureAdmission1902Context, plan, nil, true, false)
							}
						case "pattern-template":
							plan, matched := e.pipelinePropertyExpressions(tc.text)
							require.True(t, matched)
							fixtureAdmission1903Context, fixtureAdmission1903 := statementTestContext(ctx, "RETURN "+(tc.text))
							_ = fixtureAdmission1903Context
							got, err = e.evaluatePipelineProperties(fixtureAdmission1903, fixtureAdmission1903Context, plan, nil)
						case "literal-plan":
							got = parseLiteralMapForPipeline(tc.text)
							if !strings.Contains(tc.name, "/value0/") && !strings.Contains(tc.name, "/value1/") {
								require.Nil(t, got, "non-literal syntax declines before evaluation")
								continue
							}
						case "row":
							var resolved bool
							fixtureAdmission695Context, fixtureAdmission695 := expressionTestRow(context.Background(), tc.text, nil)
							_ = fixtureAdmission695
							got, resolved, err = e.evaluateRowValue(fixtureAdmission695, tc.text, fixtureAdmission695Context)
							require.True(t, resolved)
						case "compiled":
							compiler := rowValueCompiler{}
							root, _ := compiler.compile(tc.text)
							var resolved bool
							fixtureAdmission1650Context, fixtureAdmission1650 := statementTestContext(context.Background(), "RETURN "+(tc.text))
							_ = fixtureAdmission1650Context
							got, resolved, err = root.eval(fixtureAdmission1650, e, nil, nil)
							require.True(t, resolved)
						case "map-projection":
							var resolved bool
							fixtureAdmission696Context, fixtureAdmission696 := expressionTestRow(context.Background(), "base"+tc.text, map[string]interface{}{"base": map[string]interface{}{}})
							_ = fixtureAdmission696
							got, resolved, err = e.evaluateRowValue(fixtureAdmission696, "base"+tc.text, fixtureAdmission696Context)
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
		fixtureAdmission995Context, fixtureAdmission995 := expressionTestContext(ctx, expr)
		_ = fixtureAdmission995Context
		value, err := e.checkedPropertyExpression(fixtureAdmission995, fixtureAdmission995Context, expr, nil)
		require.Error(t, err, expr)
		require.Nil(t, value)
		require.Error(t, getExpressionFailure(ctx), expr)
	}
}
