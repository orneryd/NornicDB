package bolt

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/cypher"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Closed-store status on the Bolt route (NOR-358; NOR-293 comment e2b952ca).
// A statement the protocol accepts and the open store answers is a healthy
// request; once the store closes, the same statement's FAILURE carries the
// retryable unavailability status with the store's own cause visible, for a
// read and for a write whose implicit transaction cannot start.
func TestNOR358BoltClosedStoreStatusClassification(t *testing.T) {
	for _, parser := range []string{config.ParserTypeNornic, config.ParserTypeANTLR} {
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previous) })
			for _, operation := range []struct {
				name  string
				query string
				// control statements run on the open store before the close.
				control []string
			}{
				{"read", "MATCH (n:Doc) RETURN count(n) AS c", []string{"CREATE (:Doc {id:'a'})"}},
				{"property_read", "MATCH (n:Doc {id:'a'}) RETURN n.id AS id", []string{"CREATE (:Doc {id:'a'})"}},
				{"write", "CREATE (n:Doc {id:'b'}) RETURN n.id AS id", []string{"CREATE (:Doc {id:'a'})"}},
			} {
				t.Run(operation.name, func(t *testing.T) {
					base := storage.NewMemoryEngine()
					t.Cleanup(func() { require.NoError(t, base.Close()) })
					executor := cypher.NewStorageExecutor(storage.NewNamespacedEngine(base, "test"), cypher.QueryCachePolicy{MaxEntries: 0, TTL: 0})
					server := New(&Config{Port: 0, MaxConnections: 10, ReadBufferSize: 8192, WriteBufferSize: 8192}, &cypherQueryExecutor{executor: executor})
					conn := openBoltTestConn(t, serveBoltIntegrationServer(t, server))
					for _, control := range operation.control {
						runBoltQueryWithParams(t, conn, control, nil)
					}
					rows := runBoltQueryWithParams(t, conn, operation.query, nil)
					require.Len(t, rows, 1, "the statement must succeed before close")
					require.NoError(t, base.Close())
					code, message := runBoltQueryExpectFailure(t, conn, operation.query)
					t.Logf("FAILURE code=%s message=%q", code, message)
					require.Contains(t, message, "storage closed")
					require.Equal(t, nornicerrors.TransientDatabaseUnavailable, code)
				})
			}
		})
	}
}
