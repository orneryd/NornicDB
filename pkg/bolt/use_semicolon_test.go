package bolt

import (
	"context"
	"fmt"
	"testing"

	neo4jdriver "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestBoltUseSemicolon: over Bolt, USE <db>; followed by another statement
// is Neo4j's SyntaxError "Expected exactly one statement per query but got:
// 2", and USE <db>; alone is a USE with no clause.
func TestBoltUseSemicolon(t *testing.T) {
	mgr, err := multidb.NewDatabaseManager(storage.NewMemoryEngine(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = mgr.Close() })
	server := NewWithDatabaseManager(&Config{Port: 0, ReadBufferSize: 8192, WriteBufferSize: 8192}, &mockExecutor{}, mgr)
	port := startBoltTestServer(t, server)
	driver, err := neo4jdriver.NewDriverWithContext(fmt.Sprintf("bolt://127.0.0.1:%d", port), neo4jdriver.NoAuth())
	require.NoError(t, err)
	t.Cleanup(func() { _ = driver.Close(context.Background()) })
	ctx := context.Background()
	db := mgr.DefaultDatabaseName()
	for statement, message := range map[string]string{
		"USE " + db + "; MATCH (n) RETURN count(n) AS c": "Expected exactly one statement per query but got: 2",
		"USE " + db + ";": "Query cannot conclude with USE GRAPH (must be a RETURN clause, a FINISH clause, an update clause, a unit subquery call, or a procedure call with no YIELD).",
	} {
		session := driver.NewSession(ctx, neo4jdriver.SessionConfig{DatabaseName: db})
		result, err := session.Run(ctx, statement, nil)
		if err == nil {
			_, err = result.Collect(ctx)
		}
		_ = session.Close(ctx)
		require.Error(t, err, statement)
		var neo4jErr *neo4jdriver.Neo4jError
		require.ErrorAs(t, err, &neo4jErr, statement)
		require.Equal(t, "Neo.ClientError.Statement.SyntaxError", neo4jErr.Code, statement)
		require.Equal(t, message, neo4jErr.Msg, statement)
	}
}
