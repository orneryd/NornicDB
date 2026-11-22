package bolt

import (
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/cypher"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestNOR358BoltClosedStoreReadStatus(t *testing.T) {
	for _, parser := range []string{config.ParserTypeNornic, config.ParserTypeANTLR} {
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previous) })
			for _, query := range []string{"MATCH (n:Doc) RETURN count(n) AS c", "MATCH (n:Doc {id:'a'}) RETURN n.id AS id"} {
				t.Run(query, func(t *testing.T) {
					base := storage.NewMemoryEngine()
					t.Cleanup(func() { require.NoError(t, base.Close()) })
					executor := cypher.NewStorageExecutor(storage.NewNamespacedEngine(base, "test"), cypher.QueryCachePolicy{MaxEntries: 0, TTL: 0})
					server := New(&Config{Port: 0, MaxConnections: 10, ReadBufferSize: 8192, WriteBufferSize: 8192}, &cypherQueryExecutor{executor: executor})
					conn := openBoltTestConn(t, serveBoltIntegrationServer(t, server))
					runBoltQueryWithParams(t, conn, "CREATE (:Doc {id:'a'})", nil)
					rows := runBoltQueryWithParams(t, conn, query, nil)
					require.Len(t, rows, 1)
					require.NoError(t, base.Close())
					code, message := runBoltQueryExpectFailure(t, conn, query)
					t.Logf("FAILURE code=%s message=%q", code, message)
					require.Contains(t, message, "storage closed")
					require.Equal(t, nornicerrors.TransientDatabaseUnavailable, code)
				})
			}
		})
	}
}
