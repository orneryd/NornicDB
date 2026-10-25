package cypher

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestFunctionSyntaxFinalPublicReaders(t *testing.T) {
	prior := config.IsCypherQueryNormalizationEnabled()
	t.Cleanup(func() { config.SetCypherQueryNormalizationEnabled(prior) })
	for _, normalize := range []bool{false, true} {
		t.Run(fmt.Sprintf("normalize=%t", normalize), func(t *testing.T) {
			config.SetCypherQueryNormalizationEnabled(normalize)
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "final_readers"))
			ctx := context.Background()
			_, err := exec.Execute(ctx, "CREATE (:Lex {embedding:[1.0,0.0],name:'abcd'})", nil)
			require.NoError(t, err)
			for _, sample := range []struct {
				query string
				rows  [][]interface{}
				fast  bool
			}{
				{"RETURN keys /*gap*/ ({a:1})[0] AS v", [][]interface{}{{"a"}}, false},
				{"RETURN `keys` /*gap*/ ({a:1})[0] AS v", [][]interface{}{{"a"}}, false},
				{"WITH keys /*gap*/ ({a:1})[0] AS v RETURN v", [][]interface{}{{"a"}}, false},
				{"CALL { RETURN keys /*gap*/ ({a:1})[0] AS v } RETURN v", [][]interface{}{{"a"}}, false},
				{"RETURN abs((987654321))", [][]interface{}{{int64(987654321)}}, false},
				{"RETURN [987654321][0]", [][]interface{}{{int64(987654321)}}, false},
				{"MATCH (n:Lex) WITH n, vector.similarity.cosine(n.embedding,[1.0,0.0]) AS s RETURN s, (toUpper(')')) AS v ORDER BY s DESC LIMIT 1", [][]interface{}{{float64(1), ")"}}, true},
				{"MATCH (n:Lex) WITH n, vector.similarity.cosine(n.embedding,[1.0,0.0]) AS s RETURN s, substring(n.name,1,3) AS v ORDER BY s DESC LIMIT 1", [][]interface{}{{float64(1), "bcd"}}, true},
			} {
				t.Run(sample.query, func(t *testing.T) {
					result, err := exec.Execute(ctx, sample.query, nil)
					require.NoError(t, err)
					require.Equal(t, sample.rows, result.Rows)
					if sample.fast {
						require.True(t, exec.LastHotPathTrace().CosineVectorIndexFastPath, "must reach the optimized projection evaluator")
					}
				})
			}
		})
	}
}

func TestFunctionSyntaxDispatchReadersUseExistingFacts(t *testing.T) {
	for _, text := range []string{"substring(n.name,1,3)", "`substring` /*gap*/ (n.name, 1, 3) /*end*/"} {
		for _, retained := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/retained=%t", text, retained), func(t *testing.T) {
				text := fmt.Sprintf("%s /* final correction retained=%t */", text, retained)
				if retained {
					readFunctionSyntax(text)
				}
				requests := observeFunctionSyntaxFacts(t, nil, func() {
					name, args, ok := parseFunctionCallArguments(text)
					require.True(t, ok)
					require.Equal(t, "substring", name)
					require.Equal(t, []string{"n.name", "1", "3"}, args)
					_, _, ok = parseFunctionCallArguments(text + " + 1")
					require.False(t, ok, "the close must also delimit the standalone call")
				})
				require.Empty(t, requests)
			})
		}
	}
}

func TestFunctionSyntaxPostfixReadersMatchFacts(t *testing.T) {
	for _, text := range groupWalkTestInputs() {
		syntax := uncachedFunctionSyntax(text)
		wantTrailing := -1
		for _, group := range syntax.groups[1:] {
			if group.close < 0 {
				continue
			}
			require.Equal(t, group.open, lexicalGroupOpening(text, group.close), text)
			require.Equal(t, group.open, matchingRowDelimiterStart(text, group.close, text[group.open], text[group.close]), text)
			if text[group.open] == '[' && queryGapEnd(text, group.close+1) == len(text) {
				wantTrailing = group.open
			}
		}
		require.Equal(t, wantTrailing, closingBracketGroupStart(text), text)
	}
	for _, name := range []string{"keys", "`keys`", "custom.keys", "`custom name` /* dot */ . `keys`"} {
		for _, gap := range []string{"", " ", " /*gap*/ ", " //gap\n", "\u3000"} {
			text := "RETURN " + name + gap + "({a:')'})[0]"
			open := strings.LastIndex(text, "[0]")
			requests := observeFunctionSyntaxFacts(t, nil, func() {
				require.Equal(t, len("RETURN "), rowSubscriptReceiverStart(text, open), text)
			})
			require.Empty(t, requests)
		}
	}
}
