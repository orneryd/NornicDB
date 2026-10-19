package antlr

import (
	antlr4 "github.com/antlr4-go/antlr/v4"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestEmbeddingGrammarDecision(t *testing.T) {
	for _, test := range []struct {
		query  string
		option bool
	}{
		{"CREATE (embedding {k:7}) WITH embedding RETURN 7 AS k", false},
		{"CREATE (EMBEDDING {k:7}) WITH EMBEDDING RETURN EMBEDDING.k", false},
		{"CREATE (embedding) SET embedding.k=7 WITH embedding AS retained RETURN retained", false},
		{"CREATE (embedding) REMOVE embedding.k WITH embedding RETURN 7 AS k", false},
		{"WITH 7 AS embedding CREATE (n) WITH embedding RETURN embedding", false},
		{"CREATE (n) WITH EMBEDDING RETURN n", true},
		{"CREATE (n) SET n.k=7 WITH EMBEDDING RETURN n", true},
		{"CREATE (EMBEDDING) WITH EMBEDDING", true},
		{"CREATE (n) WITH EMBEDDING", true},
		{"WITH 7 AS k CREATE (n) WITH EMBEDDING RETURN k", true},
		{"CALL { CREATE (n) WITH EMBEDDING RETURN n } RETURN n", true},
		{"WITH 7 AS embedding CALL { CREATE (n) WITH EMBEDDING RETURN n } RETURN embedding", true},
		{"WITH 7 AS embedding CALL { WITH embedding CREATE (n) WITH embedding RETURN embedding AS k } RETURN k", false},
		{"WITH 7 AS embedding CALL (embedding) { CREATE (n) WITH embedding RETURN embedding AS k } RETURN k", false},
	} {
		t.Run(test.query, func(t *testing.T) {
			result, err := Parse(test.query)
			require.NoError(t, err)
			require.NoError(t, Validate(test.query), "pooled validator without parse trees")
			count := 0
			var visit func(antlr4.Tree)
			visit = func(tree antlr4.Tree) {
				if _, ok := tree.(IEmbeddingStContext); ok {
					count++
				}
				for i := 0; i < tree.GetChildCount(); i++ {
					visit(tree.GetChild(i))
				}
			}
			visit(result.Tree)
			expected := 0
			if test.option {
				expected = 1
			}
			require.Equal(t, expected, count)
		})
	}
}
