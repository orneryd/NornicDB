package antlr

import (
	"context"
	"strings"
	"testing"

	antlr4 "github.com/antlr4-go/antlr/v4"
	"github.com/orneryd/nornicdb/internal/cyphertext"
)

func TestPR1682RetainedSyntaxChildrenMatchParserTree(t *testing.T) {
	for _, n := range []int{1, 20} {
		var query strings.Builder
		query.WriteString("WITH 1 AS x ")
		for i := 0; i < n; i++ {
			query.WriteString("CALL (x) { RETURN x AS y } ")
		}
		query.WriteString("RETURN x")
		ctx := context.Background()
		var root antlr4.Tree
		data, err := parseStatementSyntax(ctx, cyphertext.NewSource(ctx, query.String(), cyphertext.AcceptedSource), StatementEntry, func(tree antlr4.Tree) { root = tree })
		if err != nil {
			t.Fatal(err)
		}
		cursor := 0
		var visit func(antlr4.Tree, int)
		visit = func(node antlr4.Tree, parent int) {
			id := cursor
			cursor++
			if id >= len(data.nodes) {
				t.Fatalf("missing node %d", id)
			}
			got := data.nodes[id]
			if got.parent != parent || len(got.children) != node.GetChildCount() {
				t.Fatalf("node %d parent=%d children=%d; want parent=%d children=%d", id, got.parent, len(got.children), parent, node.GetChildCount())
			}
			for i := 0; i < node.GetChildCount(); i++ {
				if got.children[i] != cursor {
					t.Fatalf("node %d child %d=%d; want %d", id, i, got.children[i], cursor)
				}
				visit(node.GetChild(i), id)
			}
		}
		visit(root, -1)
		if cursor != len(data.nodes) {
			t.Fatalf("visited %d of %d", cursor, len(data.nodes))
		}
	}
}
