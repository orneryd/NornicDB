package cypher

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestTransactionMergeResolvesPeerRelationshipEndpoints(t *testing.T) {
	_, writer, peer := newTransactionSnapshotCountFixture(t)
	ctx := t.Context()
	_, err := writer.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = writer.Execute(ctx, "ROLLBACK", nil) })
	require.Equal(t, int64(2), transactionSnapshotCountScalar(t, writer, ctx, `MATCH (n:A) RETURN count(n)`))
	_, err = peer.Execute(ctx, `MATCH (a:A {k:'a1'}),(b:B {k:'b1'}) CREATE (a)-[:LINK]->(b)`, nil)
	require.NoError(t, err)
	result, err := writer.Execute(ctx, `MATCH (a:A {k:'a1'}),(b:B {k:'b1'}) SET a += {} WITH a,b MERGE (a)-[r:LINK]->(b) RETURN elementId(r), startNode(r).k, endNode(r).k`, nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	require.Len(t, result.Rows[0], 3)
	require.NotEmpty(t, result.Rows[0][0])
	require.Equal(t, "a1", result.Rows[0][1])
	require.Equal(t, "b1", result.Rows[0][2])
}
