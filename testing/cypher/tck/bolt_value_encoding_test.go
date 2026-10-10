package tck

import (
	"context"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/require"
)

// Values the server sends over Bolt keep their content: a LIST<BOOLEAN>
// property read back from storage after a committed transaction is the list
// (it was null), and lists and maps of 65,536 entries or more keep every
// entry (they were cut to the size modulo 65,536).
func TestBoltValueEncodingKeepsStoredBooleanListsAndLargeCollections(t *testing.T) {
	driver, shutdown := startConformanceServer(t)
	defer shutdown()
	ctx := context.Background()
	query := func(statement string) any {
		result, err := neo4j.ExecuteQuery(ctx, driver, statement, nil, neo4j.EagerResultTransformer, neo4j.ExecuteQueryWithDatabase("nornic"))
		require.NoError(t, err, statement)
		require.Len(t, result.Records, 1, statement)
		return result.Records[0].Values[0]
	}
	query("CREATE (n:BoolList {id: 1, b: [true, false]}) RETURN n.id")
	require.Equal(t, []any{true, false}, query("MATCH (n:BoolList {id: 1}) RETURN n.b"))
	query("MATCH (n:BoolList {id: 1}) SET n.c = [x IN [1, 5] | x > 2] RETURN n.id")
	require.Equal(t, []any{false, true}, query("MATCH (n:BoolList {id: 1}) RETURN n.c"))

	require.Len(t, query("RETURN range(1, 70000)"), 70000)
	require.Len(t, query("RETURN [x IN range(1, 70000) | toString(x)]"), 70000)
	require.Len(t, query("UNWIND range(1, 70000) AS x RETURN collect(x)"), 70000)
	require.Len(t, query("RETURN apoc.map.fromPairs([x IN range(1, 70000) | [toString(x), x]])"), 70000)
}
