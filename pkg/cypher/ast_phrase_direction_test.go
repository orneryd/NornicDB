package cypher

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestASTBuilderPhraseSourceAndTrivia(t *testing.T) {
	for _, gap := range []string{" ", "  ", "\t", "\n", "\r\n", "\u00a0", "/* phrase */", " // phrase\r\n "} {
		for _, kind := range []ASTClauseType{ASTClauseOptionalMatch, ASTClauseDetachDelete, ASTClauseOrderBy} {
			t.Run(fmt.Sprintf("kind=%d/gap=%q", kind, gap), func(t *testing.T) {
				var query, expectedClause string
				switch kind {
				case ASTClauseOptionalMatch:
					expectedClause = "OPTIONAL" + gap + "MATCH (`a``b`:`L``X`)"
					query = expectedClause + " RETURN `a``b`"
				case ASTClauseDetachDelete:
					expectedClause = "DETACH" + gap + "DELETE `a``b`"
					query = "MATCH (`a``b`) " + expectedClause
				case ASTClauseOrderBy:
					expectedClause = "ORDER" + gap + "BY `a``b` DESC"
					query = "MATCH (`a``b`) RETURN `a``b` " + expectedClause
				}
				ast, err := NewASTBuilder().Build(query)
				require.NoError(t, err)
				require.Equal(t, query, ast.RawQuery)
				wantClauses := 2
				if kind == ASTClauseOrderBy {
					wantClauses = 3
				}
				require.Len(t, ast.Clauses, wantClauses)
				index := len(ast.Clauses) - 1
				if kind == ASTClauseOptionalMatch {
					index = 0
				}
				clause := ast.Clauses[index]
				require.Equal(t, kind, clause.Type)
				require.Equal(t, expectedClause, clause.RawText)
				expectedStart := strings.Index(query, expectedClause)
				require.Equal(t, expectedStart, clause.StartPos)
				require.Equal(t, expectedStart+len(expectedClause), clause.EndPos)
				for _, part := range ast.Clauses {
					require.GreaterOrEqual(t, part.StartPos, 0)
					require.LessOrEqual(t, part.EndPos, len(query))
					require.Equal(t, query[part.StartPos:part.EndPos], part.RawText)
				}
				switch kind {
				case ASTClauseOptionalMatch:
					require.True(t, clause.Match.Optional)
					require.Len(t, clause.Match.Patterns, 1)
					require.Len(t, clause.Match.Patterns[0].Nodes, 1)
					require.Equal(t, "a`b", clause.Match.Patterns[0].Nodes[0].Variable)
					require.Equal(t, []string{"L`X"}, clause.Match.Patterns[0].Nodes[0].Labels)
				case ASTClauseDetachDelete:
					require.True(t, clause.Delete.Detach)
					require.Equal(t, []string{"`a``b`"}, clause.Delete.Variables)
				case ASTClauseOrderBy:
					require.Len(t, clause.OrderBy.Items, 1)
					require.True(t, clause.OrderBy.Items[0].Descending)
					require.Equal(t, "a`b", clause.OrderBy.Items[0].Expression.Variable)
					require.Equal(t, "`a``b` DESC", clause.OrderBy.Items[0].RawText)
				}
			})
		}
	}
}

func TestASTBuilderRelationshipDirectionSource(t *testing.T) {
	for _, tc := range []struct {
		name, left, right string
		direction         EdgeDirection
	}{
		{"outgoing", "-", "->", EdgeOutgoing}, {"incoming", "<-", "-", EdgeIncoming}, {"undirected", "-", "-", EdgeBoth}, {"both heads", "<-", "->", EdgeBoth},
	} {
		for _, gap := range []string{"", " ", "\t", "/* edge */"} {
			t.Run(tc.name+fmt.Sprintf("/%q", gap), func(t *testing.T) {
				pattern := "(`a``b`)" + tc.left + gap + "[`r``x`:`T``X`]" + gap + tc.right + "(b)"
				query := "MATCH " + pattern + " RETURN `r``x`"
				ast, err := NewASTBuilder().Build(query)
				require.NoError(t, err)
				require.Equal(t, query, ast.RawQuery)
				require.Len(t, ast.Clauses, 2)
				clause := ast.Clauses[0]
				require.Equal(t, query[clause.StartPos:clause.EndPos], clause.RawText)
				require.Equal(t, "MATCH "+pattern, clause.RawText)
				require.Equal(t, 0, clause.StartPos)
				require.Equal(t, len("MATCH "+pattern), clause.EndPos)
				require.Len(t, clause.Match.Patterns, 1)
				parsed := clause.Match.Patterns[0]
				require.Equal(t, pattern, parsed.RawText)
				require.Len(t, parsed.Nodes, 2)
				require.Equal(t, "a`b", parsed.Nodes[0].Variable)
				require.Len(t, parsed.Relationships, 1)
				relationship := parsed.Relationships[0]
				require.Equal(t, "r`x", relationship.Variable)
				require.Equal(t, "T`X", relationship.Type)
				require.Equal(t, tc.direction, relationship.Direction)
			})
		}
	}
}

func TestASTBuilderRelationshipShapeRemainsNarrow(t *testing.T) {
	for _, pattern := range []string{"(a)-->(b)", "(a)-[r:T {p: 1}]->(b)", "(a)-[r:T*1..2]->(b)", "(a)-[r:T:U]->(b)"} {
		t.Run(pattern, func(t *testing.T) {
			ast, err := NewASTBuilder().Build("MATCH " + pattern + " RETURN a")
			require.NoError(t, err)
			require.Len(t, ast.Clauses, 2)
			require.Len(t, ast.Clauses[0].Match.Patterns, 1)
			require.Empty(t, ast.Clauses[0].Match.Patterns[0].Relationships)
		})
	}
}
