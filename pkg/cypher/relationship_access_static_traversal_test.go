package cypher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRelationshipAccessProgramStaticTraversal(t *testing.T) {
	for _, pattern := range []string{
		"(a:Key)-[r:T]->(b)",
		"(a:`Key{quoted}`)-[r:`T{quoted}`]->(b)",
	} {
		_, statement := statementTestContext(context.Background(), "MATCH "+pattern+" RETURN r")
		shape := relationshipStaticTraversal(statement, pattern, "")
		require.NotNil(t, shape)
		require.Equal(t, "a", shape.StartNode.variable)
		require.Equal(t, "b", shape.EndNode.variable)
	}
	for _, pattern := range []string{
		"(a:Key {p:k})-[r:T]->(b)",
		"(a)-[r:T {p:k}]->(b)",
		"(a:$(k))-[r:T]->(b)",
		"(a)-[r:$(k)]->(b)",
	} {
		ctx, statement := statementTestContext(context.Background(), "MATCH "+pattern+" RETURN r")
		ctx = withExpressionFailureSlot(ctx)
		require.Nil(t, relationshipStaticTraversal(statement, pattern, ""))
		require.NoError(t, getExpressionFailure(ctx), "admission cannot evaluate an incoming row operand")
	}
	const query = "MATCH (a)-[r:T]->(b) RETURN r"
	source := &relationshipAccessProgram{query: query, byID: map[string]*relationshipAccessTemplate{}}
	_, statement := compileRelationshipStatement(context.Background(), query, source)
	require.Nil(t, relationshipStaticTraversal(statement, "(a)-[r:T]->(b)", ""))
	requireRelationshipAdmissionBuildRejected(t, `func(ctx context.Context) { _ = relationshipStaticTraversal(ctx, "(a)-[r:T]->(b)", "") }`, "relationshipStaticTraversal")
}
