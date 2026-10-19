package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A NOT that starts a parenthesized OR negates only its own operand:
// (NOT a OR b) is an OR, not NOT (a OR b).
func TestParenthesizedLeadingNotDisjunction(t *testing.T) {
	plan := planLogicalExpression("(NOT EXISTS { MATCH (a) } OR EXISTS { MATCH (b) })", nil)
	require.Equal(t, logicalOr, plan.kind)
	require.Len(t, plan.operands, 2)
	require.Equal(t, logicalOr, planLogicalExpression("(NOT a OR b)", nil).kind)

	ctx := context.Background()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "paren_not"))
	_, err := exec.Execute(ctx, "CREATE (d:Doc {id:'d'}), (:Src {id:'s', document_id:'d'}), (c:Collection {archived:false}), (d)-[:MEMBER_OF]->(c)", nil)
	require.NoError(t, err)
	member := "EXISTS { MATCH (:Doc {id: node.document_id})-[:MEMBER_OF]->(:Collection) }"
	active := "EXISTS { MATCH (:Doc {id: node.document_id})-[:MEMBER_OF]->(c:Collection) WHERE NOT coalesce(c.archived, false) }"
	for _, where := range []string{
		"(NOT " + member + " OR " + active + ")",
		"NOT " + member + " OR " + active,
		"((NOT " + member + ") OR " + active + ")",
		"(NOT " + member + " OR " + active + ") AND node.id = 's'",
	} {
		for _, query := range []string{
			"MATCH (node:Src) WHERE " + where + " RETURN node.id AS id",
			"MATCH (node:Src) WITH node WHERE " + where + " RETURN node.id AS id",
			"MATCH (node:Src) RETURN node.id AS id, " + where + " AS kept",
		} {
			result, err := exec.Execute(ctx, query, nil)
			require.NoError(t, err, query)
			require.Len(t, result.Rows, 1, query)
			if len(result.Rows[0]) == 2 {
				require.Equal(t, true, result.Rows[0][1], query)
			}
		}
	}
}
