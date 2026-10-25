package cypher

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Count actual tokenizer work through the public call visitor and the retained
// reader, including indirect predecessor/annotation queries. Time is irrelevant.
func TestFunctionSyntaxFactLinearWork(t *testing.T) {
	for _, gap := range []string{" ", " /* gap */ "} {
		for _, mode := range []string{"visitor", "retained", "validation"} {
			work := func(n int) int {
				var b strings.Builder
				fmt.Fprintf(&b, "/* mode=%s */ RETURN ", mode)
				for i := 0; i < n; i++ {
					if i > 0 {
						b.WriteString(", ")
					}
					fmt.Fprintf(&b, "abs%s(%d) AS v%d", gap, i, i)
				}
				query := b.String()
				steps := 0
				observe := func(n int) { steps += n }
				lexicalTokenWorkObserver.Store(&observe)
				defer lexicalTokenWorkObserver.Store(nil)
				calls := 0
				if mode == "validation" {
					require.NoError(t, validateStaticFunctionArguments(query, false))
					calls = n
				} else if mode == "retained" {
					syntax := uncachedFunctionSyntax(query)
					for at := 0; ; {
						call, ok := syntax.nextCall(at, true, false, false)
						if !ok {
							break
						}
						calls++
						at = call.open + 1
					}
				} else {
					require.NoError(t, forEachFunctionCall(query, func(_, _ string) error { calls++; return nil }))
				}
				require.Equal(t, n, calls)
				return steps
			}
			small, large := work(1024), work(4096)
			t.Logf("gap=%q mode=%s: N=%d 4N=%d", gap, mode, small, large)
			require.Positive(t, small)
			require.Less(t, large, small*6, "N=%d 4N=%d gap=%q mode=%s", small, large, gap, mode)
		}
	}
}

func TestFunctionSyntaxPrefixNotStaticType(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "prefix_not_type"))
	query := "UNWIND [] AS r MATCH (n) WITH n, 1 AS v RETURN NOT v AS a"
	for _, tx := range []bool{false, true} {
		if tx {
			_, err := exec.Execute(context.Background(), "BEGIN", nil)
			require.NoError(t, err)
		}
		_, err := exec.Execute(context.Background(), query, nil)
		if tx {
			_, _ = exec.Execute(context.Background(), "ROLLBACK", nil)
		}
		require.Error(t, err, query)
		require.Contains(t, err.Error(), "Type mismatch")
	}
}

func TestFunctionSyntaxKeywordReceiverRoles(t *testing.T) {
	for _, name := range []string{"count", "all", "return", "in", "call", "use", "not", "match", "typed", "local", "zoned", "distinct"} {
		for _, prefix := range []string{"", "RETURN ", "3 IN "} {
			for _, receiver := range []string{"`" + name + "`", "$" + name, "`" + name + "`" + "{.a}", "{ " + name + ": [5] }." + name} {
				expression := prefix + receiver + " /*gap*/ [0]"
				open := strings.LastIndex(expression, "[0]")
				require.Equal(t, len(prefix), rowSubscriptReceiverStart(expression, open), expression)
			}
		}
	}
}

// The lexer grammar is the reproducible keyword population. Reserved spellings
// use Cypher's quoted-variable syntax in statements; parameters and map keys
// retain their ordinary spelling. Evaluation also exercises unquoted names.
func expressionKeywordPopulation(t *testing.T) []string {
	source, err := os.ReadFile("antlr/CypherLexer.g4")
	require.NoError(t, err)
	matches := regexp.MustCompile(`(?m)^[A-Z_]+[ \t]*:[ \t]*'([A-Za-z]+)'`).FindAllSubmatch(source, -1)
	names := make([]string, 0, len(matches))
	for _, m := range matches {
		names = append(names, strings.ToLower(string(m[1])))
	}
	require.Greater(t, len(names), 150)
	return names
}

func TestFunctionSyntaxAllKeywordRoles(t *testing.T) {
	t.Run("operator_positions", func(t *testing.T) {
		for _, sample := range []struct {
			text, operator string
			role           expressionTokenRole
		}{
			{"NOT v", "NOT", expressionKeyword},
			{"RETURN NOT /*gap*/ v AS a", "NOT", expressionKeyword},
			{"NOT (v)", "NOT", expressionKeyword},
			{"NOT NOT v", "NOT", expressionKeyword},
			{"NOT [true][0]", "NOT", expressionKeyword},
			{"RETURN - v AS a", "-", expressionKeyword},
			{"RETURN + /*gap*/ v AS a", "+", expressionKeyword},
			{"1 + - v", "-", expressionKeyword},
			{"v AND true", "AND", expressionKeyword},
			{"v OR true", "OR", expressionKeyword},
			{"v XOR true", "XOR", expressionKeyword},
			{"v IN [1]", "IN", expressionKeyword},
			{"v IS NOT NULL", "IS", expressionKeyword},
			{"v CONTAINS 'x'", "CONTAINS", expressionKeyword},
			{"v STARTS WITH 'x'", "STARTS", expressionKeyword},
			{"v ENDS WITH 'x'", "ENDS", expressionKeyword},
			{"NOT", "NOT", expressionIdentifier},
			{"RETURN NOT AS a", "NOT", expressionIdentifier},
			{"NOT[0]", "NOT", expressionIdentifier},
			{"RETURN NOT[0] AS a", "NOT", expressionIdentifier},
			{"NOT.a", "NOT", expressionIdentifier},
			{"v + 1", "+", expressionIdentifier},
			{"v - 1", "-", expressionIdentifier},
		} {
			facts := readExpressionLexicalFacts(sample.text)
			at := strings.Index(sample.text, sample.operator)
			require.Equal(t, sample.role, facts.tokens[facts.at[at]].role, sample.text)
		}
	})
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "keyword_roles"))
	for _, name := range append([]string{"plain"}, expressionKeywordPopulation(t)...) {
		t.Run(name, func(t *testing.T) {
			values := map[string]interface{}{name: map[string]interface{}{name: []interface{}{int64(5)}}, rowStatementContextKey: context.Background()}
			params := map[string]interface{}{name: []interface{}{int64(5)}}
			require.False(t, traversalVariablePropertiesRead("count /*gap*/ (`"+name+"`)", name))
			require.False(t, traversalVariablePropertiesRead("{"+name+":1}", name))
			require.True(t, traversalVariablePropertiesRead("`"+name+"` /*gap*/ . "+name, name))
			end, typ, _ := staticListOperand("$"+name, 0, params)
			require.Equal(t, len(name)+1, end)
			require.Empty(t, typ)
			parameterRow := map[string]interface{}{name: []interface{}{int64(5)}, rowStatementContextKey: withParams(context.Background(), params)}
			for _, expression := range []string{"`" + name + "`[0]", "$" + name + "[0]"} {
				for _, compiled := range []bool{false, true} {
					var value interface{}
					var resolved bool
					var err error
					if compiled {
						compiler := rowValueCompiler{}
						node, _ := compiler.compile(expression)
						value, resolved, err = node.eval(exec, parameterRow, nil)
					} else {
						value, resolved, err = exec.evaluateRowValueText(expression, parameterRow)
					}
					require.NoError(t, err, expression)
					require.True(t, resolved, expression)
					require.Equal(t, int64(5), value, expression)
				}
			}
			for _, expression := range []string{"`" + name + "`." + name + "[0]", "`" + name + "`{." + name + "}." + name + "[0]", "{" + name + ":[5]}." + name + "[0]"} {
				for _, compiled := range []bool{false, true} {
					var value interface{}
					var ok bool
					var err error
					if compiled {
						compiler := rowValueCompiler{}
						node, _ := compiler.compile(expression)
						value, ok, err = node.eval(exec, values, nil)
					} else {
						value, ok, err = exec.evaluateRowValueText(expression, values)
					}
					require.NoError(t, err, expression)
					require.True(t, ok, expression)
					require.Equal(t, int64(5), value, expression)
				}
			}
			for _, sample := range []struct{ binding, expression string }{
				{"WITH [5] AS `" + name + "` ", "`" + name + "`[0]"},
				{"", "$" + name + "[0]"},
				{"", "{" + name + ":[5]}." + name + "[0]"},
				{"WITH {" + name + ":[5]} AS `" + name + "` ", "`" + name + "`{." + name + "}." + name + "[0]"},
			} {
				for _, route := range []string{"RETURN %s AS v", "WITH (%s) AS v RETURN v", "CALL { RETURN %s AS v } RETURN v"} {
					query := sample.binding + fmt.Sprintf(route, sample.expression)
					// Scoped CALL imports the variable without changing its name.
					if sample.binding != "" && strings.HasPrefix(route, "CALL") {
						query = sample.binding + "CALL (`" + name + "`) { RETURN " + sample.expression + " AS v } RETURN v"
					}
					result, err := exec.Execute(context.Background(), query, params)
					require.NoError(t, err, query)
					require.Equal(t, [][]interface{}{{int64(5)}}, result.Rows, query)
				}
			}
		})
	}
}

func TestFunctionSyntaxHeldViewsSurviveEviction(t *testing.T) {
	text := "RETURN abs /*gap*/ (1000), size([1, abs(2)]), point({x:1,y:2}), toString((NOT false)), head([x IN [1] | abs(x)]), reduce(s=0,x IN [1] | s+x)"
	v := expressionView(text)
	// Evict this particular shard deterministically, independently of hash seed.
	shard := boundedCacheStringShardIndex(text)
	for i, inserted := 0, 0; inserted < 16; i++ {
		pressure := fmt.Sprintf("/* eviction */ %d", i)
		if boundedCacheStringShardIndex(pressure) == shard {
			expressionLexicalCache.put(pressure, buildExpressionLexicalFacts(pressure))
			inserted++
		}
	}
	_, cached := expressionLexicalCache.get(text)
	require.False(t, cached)
	work := 0
	observe := func(n int) { work += n }
	lexicalTokenWorkObserver.Store(&observe)
	defer lexicalTokenWorkObserver.Store(nil)
	checked := 0
	require.NoError(t, forEachStaticFunctionArgumentView(text, func(argument staticArgumentType, part expressionLexicalView, _ int) error {
		require.Same(t, v.facts, part.facts)
		typeName := (staticTypeScope{}).operandOfView(part).typeName()
		require.True(t, argument.accepts(typeName), part.text())
		checked++
		return nil
	}, v))
	require.Positive(t, checked)
	require.Zero(t, work, "argument typing rebuilt retained facts after eviction")
	_, cached = expressionLexicalCache.get(text)
	require.False(t, cached, "a retained consumer looked the parent up again")
}

func TestFunctionSyntaxFallbackDoesNotReenterRow(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "fallback_no_reentry"))
	row := map[string]interface{}{"count": []interface{}{int64(5)}, rowStatementContextKey: context.Background()}
	_, resolved, err := exec.evaluateExpressionFromValuesResult(context.Background(), "count[0]", row)
	require.False(t, resolved)
	require.Error(t, err)
	value, resolved, err := exec.evaluateRowValueText("count[0]", row)
	require.NoError(t, err)
	require.True(t, resolved)
	require.Equal(t, int64(5), value)
}

// Reusing an expression for different questions performs no token work. In
// particular predecessors, annotations and receivers cannot read prefixes.
func TestFunctionSyntaxFactConsumersReuseTable(t *testing.T) {
	text := "abs /*gap*/ (1), p[0], p.a, {x:1}, $p"
	facts := readExpressionLexicalFacts(text)
	checker := staticOperatorChecker{}
	_, err := checker.checkAtom("{x:1}")
	require.NoError(t, err)
	work := 0
	observe := func(n int) { work += n }
	lexicalTokenWorkObserver.Store(&observe)
	defer lexicalTokenWorkObserver.Store(nil)
	require.NoError(t, lexicalFunctionInvocations(text, func(_, _ string, _ int) error { return nil }))
	calls := newCallCursor(text)
	for at := len(text) - 1; at >= 0; at-- {
		calls.callAt(at)
		calls.callOpenedAt(at)
		lexicalPreviousToken(text, at)
		precededByTypeAnnotation(text, at)
	}
	lexicalPostfixWalk(text, func(lexicalPostfix) bool { return true })
	lexicalGroupWalk(text, 0, func(lexicalTokenKind, int, int, int, int) bool { return true })
	lexicalGroupArguments(nil, text, facts.calls[0].open, false)
	require.True(t, facts.calls[0].asciiNameSpacing(text))
	key := strings.Index(text, "x:1")
	require.Equal(t, syntaxSpan{key + 2, key + 3}, facts.mapEntryValue(syntaxSpan{key, key + 3}))
	require.True(t, traversalVariablePropertiesRead(text, "p"))
	_, typeName, _ := staticListOperand(text, strings.LastIndex(text, "$p"), map[string]interface{}{"p": []interface{}{int64(1)}})
	require.Empty(t, typeName)
	_, err = checker.checkAtom("{x:1}")
	require.NoError(t, err)
	require.Equal(t, facts.source(syntaxSpan{0, len(text)}), lexicalSourceText(text))
	require.Zero(t, work, "a consumer rebuilt lexical context")
}

func TestFunctionSyntaxUnquotedKeywordBoundReceivers(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "unquoted_keyword_receivers"))
	for _, name := range []string{"count", "all", "not", "distinct", "collect", "local", "zoned", "typed"} {
		values := map[string]interface{}{name: []interface{}{int64(5)}, rowStatementContextKey: context.Background()}
		expression := name + "[0]"
		for _, compiled := range []bool{false, true} {
			var value interface{}
			var resolved bool
			var err error
			if compiled {
				compiler := rowValueCompiler{}
				node, _ := compiler.compile(expression)
				value, resolved, err = node.eval(exec, values, nil)
			} else {
				value, resolved, err = exec.evaluateRowValueText(expression, values)
			}
			require.NoError(t, err, expression)
			require.True(t, resolved, expression)
			require.Equal(t, int64(5), value, expression)
		}
		for _, route := range []string{"RETURN (%s) AS v", "WITH (%s) AS v RETURN v", "CALL"} {
			query := "WITH [5] AS " + name + " " + fmt.Sprintf(route, expression)
			if strings.HasPrefix(route, "CALL") {
				query = "WITH [5] AS " + name + " CALL (" + name + ") { RETURN (" + expression + ") AS v } RETURN v"
			}
			result, err := exec.Execute(context.Background(), query, nil)
			require.NoError(t, err, query)
			require.Equal(t, [][]interface{}{{int64(5)}}, result.Rows, query)
		}
	}
}
