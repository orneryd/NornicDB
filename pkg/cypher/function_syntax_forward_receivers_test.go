package cypher

import (
	"context"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Cross the receiver families with the same trivia, rather than retaining one
// spelling for each previously reported failure. COUNT/EXISTS are scalar
// receivers too, even though indexing their values must be rejected.
func TestFunctionSyntaxForwardReceiverSpans(t *testing.T) {
	for _, gap := range []string{"", " /*gap*/ ", "\u3000", " /*gap*/\u3000", " //gap\n\u3000"} {
		for _, base := range []string{
			"keys" + gap + "({a:1})", "`keys`" + gap + "({a:1})",
			"custom.keys" + gap + "({a:1})", "(keys({a:1}))",
			"[1,2]", "{a:[1]}", "'a]b'", "m" + gap + "{.a}",
			"COLLECT" + gap + "{ RETURN 1 AS x }",
			"COUNT" + gap + "{ RETURN 1 AS x }",
			"EXISTS" + gap + "{ RETURN 1 AS x }",
		} {
			for _, prefix := range []string{"", "RETURN ", "3 IN "} {
				text := prefix + base + gap + "[0]"
				t.Run(text, func(t *testing.T) {
					open := strings.LastIndex(text, "[0]")
					requests := observeFunctionSyntaxFacts(t, nil, func() {
						require.Equal(t, len(prefix), rowSubscriptReceiverStart(text, open))
						require.Equal(t, open, closingBracketGroupStart(text))
						if prefix == "" {
							require.Equal(t, open, rowSubscriptStart(text))
						}
					})
					require.Empty(t, requests)
				})
			}
		}
	}
}

func forwardReceiverValues(gap string) []struct {
	expression string
	value      interface{}
} {
	return []struct {
		expression string
		value      interface{}
	}{
		{"keys" + gap + "({a:1})" + gap + "[0]", "a"},
		{"COLLECT" + gap + "{ RETURN 1 AS x }" + gap + "[0]", int64(1)},
		{"COUNT" + gap + "{ RETURN 1 AS x }", int64(1)},
		{"EXISTS" + gap + "{ RETURN 1 AS x }", true},
		{"m" + gap + "{.a}" + gap + "['a']", int64(1)},
		{"m" + gap + "{.a}" + gap + ".a", int64(1)},
		{"({a:1})" + gap + ".a", int64(1)},
		{"head([{a:1}])" + gap + ".a", int64(1)},
		{"[{a:1}]" + gap + "[0]" + gap + ".a", int64(1)},
		{"COLLECT" + gap + "{ RETURN {a:1} AS x }" + gap + "[0]" + gap + ".a", int64(1)},
	}
}

func TestFunctionSyntaxForwardReceiverPublic(t *testing.T) {
	prior := config.IsCypherQueryNormalizationEnabled()
	t.Cleanup(func() { config.SetCypherQueryNormalizationEnabled(prior) })
	ctx := context.Background()
	for _, normalized := range []bool{false, true} {
		config.SetCypherQueryNormalizationEnabled(normalized)
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "forward_receivers"))
		for _, gap := range []string{"", " /*gap*/ ", "\u3000", " /*gap*/\u3000", " //gap\n\u3000"} {
			for _, sample := range forwardReceiverValues(gap) {
				for _, route := range []string{
					"WITH {a:1} AS m RETURN %s AS v",
					"WITH {a:1} AS m WITH %s AS v RETURN v",
					"WITH {a:1} AS m CALL (m) { RETURN %s AS v } RETURN v",
				} {
					query := fmt.Sprintf(route, sample.expression)
					t.Run(fmt.Sprintf("normalize=%t/%s", normalized, query), func(t *testing.T) {
						result, err := exec.Execute(ctx, query, nil)
						require.NoError(t, err)
						require.Equal(t, [][]interface{}{{sample.value}}, result.Rows)
					})
				}
			}
			for _, scalar := range []string{"COUNT", "EXISTS"} {
				expression := scalar + gap + "{ RETURN 1 AS x }" + gap + "[0]"
				for _, route := range []string{"RETURN %s AS v", "WITH %s AS v RETURN v", "CALL { RETURN %s AS v } RETURN v"} {
					query := fmt.Sprintf(route, expression)
					t.Run(fmt.Sprintf("scalar-type/normalize=%t/%s", normalized, query), func(t *testing.T) {
						_, err := exec.Execute(ctx, query, nil)
						require.Error(t, err)
						var diagnostic interface {
							BoltErrorCode() string
							BoltErrorDetail() string
						}
						require.True(t, errors.As(err, &diagnostic))
						code := "Neo.ClientError.Statement.SyntaxError"
						if strings.HasPrefix(route, "CALL") {
							code = "Neo.ClientError.Statement.TypeError"
						}
						require.Equal(t, code, diagnostic.BoltErrorCode())
						require.Equal(t, "InvalidArgumentType", diagnostic.BoltErrorDetail())
						if code == "Neo.ClientError.Statement.SyntaxError" {
							require.ErrorContains(t, err, "List<T>", "must type-check the scalar, not a map")
						}
					})
				}
			}
			// Retain the exact reviewer inputs, without a preceding WITH.
			for _, query := range []string{"RETURN COLLECT /*gap*/ { RETURN 1 AS x }[0] AS v", "RETURN keys({a:1})\u3000[0] AS v", "RETURN keys({a:1}) /*gap*/\u3000[0] AS v"} {
				t.Run(fmt.Sprintf("exact/normalize=%t/%s", normalized, query), func(t *testing.T) {
					result, err := exec.Execute(ctx, query, nil)
					require.NoError(t, err)
					want := interface{}("a")
					if strings.Contains(query, "COLLECT") {
						want = int64(1)
					}
					require.Equal(t, [][]interface{}{{want}}, result.Rows)
				})
			}
		}
	}
}

func TestFunctionSyntaxForwardReceiverEvaluation(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "forward_eval"))
	values := map[string]interface{}{"m": map[string]interface{}{"a": int64(1)}, rowStatementContextKey: context.Background()}
	for _, gap := range []string{"", " /*gap*/ ", "\u3000", " /*gap*/\u3000", " //gap\n\u3000"} {
		for _, sample := range forwardReceiverValues(gap) {
			for _, compiled := range []bool{false, true} {
				t.Run(fmt.Sprintf("compiled=%t/%s", compiled, sample.expression), func(t *testing.T) {
					var value interface{}
					var ok bool
					var err error
					if compiled {
						compiler := rowValueCompiler{}
						node, _ := compiler.compile(sample.expression)
						value, ok, err = node.eval(exec, values, nil)
					} else {
						value, ok, err = exec.evaluateRowValueText(sample.expression, values)
					}
					require.NoError(t, err)
					require.True(t, ok)
					require.Equal(t, sample.value, value)
				})
			}
		}
	}
}

func TestFunctionSyntaxGuardRejectsBackwardShapes(t *testing.T) {
	for _, source := range []string{
		`package p; func f(query string, open int) { wordEnd:=open-1; word:=wordEnd; for word>=0 && isIdentByte(query[word]) { word-- } }`,
		`package p; func f(query string, end int) { for end>0 && isASCIISpace(query[end-1]) { end-- } }`,
		`package p; func f(query []byte, end int) { for at:=end; at>0; at-=1 { _=query[at-1] } }`,
		`package p; func f(q string, i int) { for i>0 { i=i-1; _=q[i] } }`,
		`package p; func f(query string, i int) { for i<len(query) && query[i]==' ' { i++ } }`,
		`package p; func f(query string, i int) { for query[i]=='/' && query[i+1]=='*' { i+=2 } }`,
		`package p; func f(payload []byte, pos int) { input:=payload; for pos>0 { pos--; _=input[pos] } }`,
		`package p; func f(query string) { for _,c:=range query { if unicode.IsSpace(c) { continue } } }`,
		`package p; func f(query string) { for i:=0;i<len(query);i++ { c:=query[i]; if c==' ' { continue } } }`,
		`package p; func f(query string) { for i:=0;i<len(query);i++ { if query[i]=='/' && query[i+1]=='/' { i++; continue } } }`,
		`package p; func f(query string) { for i:=0;i<len(query);i++ { if strings.HasPrefix(query[i:],"/*") { i++; continue } } }`,
	} {
		file, err := parser.ParseFile(token.NewFileSet(), "shape.go", source, 0)
		require.NoError(t, err)
		require.NotEmpty(t, alternateFunctionSyntax(file.Decls[0].(*ast.FuncDecl)), source)
	}
}

func TestFunctionSyntaxForwardScalarReceiverEvaluation(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "forward_scalar_types"))
	for _, gap := range []string{"", " /*gap*/ ", "\u3000", " /*gap*/\u3000", " //gap\n\u3000"} {
		for _, scalar := range []string{"COUNT", "EXISTS"} {
			expression := scalar + gap + "{ RETURN 1 AS x }" + gap + "[0]"
			for _, compiled := range []bool{false, true} {
				t.Run(fmt.Sprintf("compiled=%t/%s", compiled, expression), func(t *testing.T) {
					values := map[string]interface{}{rowStatementContextKey: context.Background()}
					var err error
					if compiled {
						compiler := rowValueCompiler{}
						node, _ := compiler.compile(expression)
						_, _, err = node.eval(exec, values, nil)
					} else {
						_, _, err = exec.evaluateRowValueText(expression, values)
					}
					require.Error(t, err)
					var diagnostic interface {
						BoltErrorCode() string
						BoltErrorDetail() string
					}
					require.True(t, errors.As(err, &diagnostic))
					require.Equal(t, "Neo.ClientError.Statement.TypeError", diagnostic.BoltErrorCode())
					require.Equal(t, "InvalidArgumentType", diagnostic.BoltErrorDetail())
				})
			}
		}
	}
}

func TestFunctionSyntaxForwardGuardControls(t *testing.T) {
	for _, source := range []string{
		`package p; func f(query string, entries []int) { for i:=len(entries)-1;i>=0;i-- { _=entries[i] }; _=query }`,
		`package p; func f(query string) { for i:=0;i<len(query);i++ { i=skipCypherQuotedText(query,i,query[i])-1 } }`,
		`package p; func f(query string, i int) { for i<len(query) { i=queryGapEnd(query,i); break } }`,
	} {
		file, err := parser.ParseFile(token.NewFileSet(), "control.go", source, 0)
		require.NoError(t, err)
		require.Empty(t, forwardReaderShapes(file.Decls[0].(*ast.FuncDecl)), source)
	}
}

func TestFunctionSyntaxForwardWrapperConsumers(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "forward_wrappers"))
	ctx := context.Background()
	for _, wrapper := range []string{"(%s)", "((%s))", "( /*gap*/\u3000%s /*end*/ )"} {
		for _, literal := range []bool{false, true} {
			right := "$keys"
			if literal {
				right = "['a','a',null,'b']"
			}
			clause := fmt.Sprintf(wrapper, "n.k IN "+right)
			t.Run(clause, func(t *testing.T) {
				var prop string
				var vals []interface{}
				var ok bool
				if literal {
					prop, vals, ok = exec.parseSimpleIndexedInLiteral(ctx, "n", clause)
				} else {
					prop, vals, ok = exec.parseSimpleIndexedInParam("n", clause, map[string]interface{}{"keys": []interface{}{"a", "a", nil, "b"}})
				}
				require.True(t, ok)
				require.Equal(t, "k", prop)
				require.Equal(t, []interface{}{"a", "b"}, vals)
			})
		}
	}
	for _, text := range []string{"[']', /* ] */ [1]]", "['a'][0]", "[1] + [2]", "[1"} {
		t.Run(text, func(t *testing.T) {
			_, enclosed := stripEnclosingRowDelimiter(text, '[', ']')
			require.Equal(t, text == "[']', /* ] */ [1]]", isListLiteral(text))
			require.Equal(t, isListLiteral(text), enclosed)
		})
	}
}

// The old postfix reference copied the byte-before-dot heuristic and treated
// entire RETURN clauses as operands. Postfix grammar now has explicit primary
// span goldens; retained-facts equivalence still tests every group separately.
func TestFunctionSyntaxForwardPostfixSpans(t *testing.T) {
	for _, gap := range []string{"", " /*gap*/ ", "\u3000", " /*gap*/\u3000", " //gap\n\u3000"} {
		for _, base := range []string{"m", "`my m`", "keys({a:1})", "custom.fn(1)", "(m)", "[m]", "{a:1}", "COLLECT { RETURN m AS x }", "COUNT { RETURN 1 AS x }", "EXISTS { RETURN 1 AS x }", "m{.a}", "m[0].a"} {
			for _, suffix := range []struct {
				text, key string
				subscript bool
			}{
				{"[0]", "0", true},
				{".a", "a", false},
				{". `a b`", "`a b`", false},
			} {
				for _, prefix := range []string{"", "RETURN ", "3 IN "} {
					text := prefix + base + gap + suffix.text
					t.Run(text, func(t *testing.T) {
						requests := observeFunctionSyntaxFacts(t, nil, func() {
							receiver, key, subscript, ok := staticPostfixSplit(text)
							if prefix != "" {
								require.False(t, ok, "a clause or lower-precedence prefix is not the receiver")
								return
							}
							require.True(t, ok)
							require.Equal(t, base, receiver)
							require.Equal(t, suffix.key, key)
							require.Equal(t, suffix.subscript, subscript)
						})
						require.Empty(t, requests)
					})
				}
			}
		}
	}
	for _, text := range []string{"1.5", "m[0].1", "m.a + 1", "[1] + [2]", "RETURN [1]"} {
		_, _, _, ok := staticPostfixSplit(text)
		require.False(t, ok, text)
	}
}
