package cypher

import (
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/stretchr/testify/require"
)

// Author: #1663 lane. Operand identity survives the assignment grammar's
// parentheses and trivia; expression consumers receive actual original bytes.
func TestSetAssignmentRetainsOperandExtents(t *testing.T) {
	for _, assignment := range []string{
		`((  n  )).x = {k: 1}`,
		`(( n ))[coalesce({k: 'key'}.k, 'key')] = {k: 2}`,
		`n:$(coalesce({k: 'Label'}.k, 'Label'))`,
	} {
		t.Run(assignment, func(t *testing.T) {
			query := "MATCH (n) SET " + assignment
			root := originalBraceView(query)
			view := root.window(len("MATCH (n) SET "), len(query))
			item := readSetAssignment(view)
			require.Equal(t, "n", item.target)
			require.Equal(t, "n", item.targetView.text())
			for _, operand := range []braceView{item.targetView, item.key, item.right} {
				if operand.empty() {
					continue
				}
				origin := operand.source.Origin(cyphertext.Span{End: len(operand.text())})
				require.False(t, origin.Generated)
				require.Equal(t, operand.text(), query[origin.Original.Start:origin.Original.End])
				if strings.Contains(operand.text(), "{") {
					_, _, fact, found := operand.firstBrace()
					require.True(t, found)
					require.Equal(t, literalBrace, fact.kind)
				}
			}
		})
	}
}
