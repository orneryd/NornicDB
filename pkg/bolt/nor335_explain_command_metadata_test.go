package bolt

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

// Assert the actual Bolt RUN fields and terminal PULL plan for the reference
// SHOW/DDL EXPLAIN cases, with a fresh connection for every assertion cell.
func TestNOR335BoltExplainAdministrativeMetadataAndNoEffects(t *testing.T) {
	for _, parser := range []string{config.ParserTypeNornic, config.ParserTypeANTLR} {
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previous) })
			for _, test := range []struct {
				name, query string
				columns     []string
			}{
				{"procedures", "SHOW PROCEDURES", []string{"name", "description", "mode", "worksOnSystem"}},
				{"procedures_projected", "SHOW PROCEDURES YIELD name RETURN name", []string{"name"}},
				{"functions_projected", "SHOW FUNCTIONS YIELD name", []string{"name"}},
				{"indexes", "SHOW INDEXES", []string{"id", "name", "state", "populationPercent", "type", "entityType", "labelsOrTypes", "properties", "indexProvider", "owningConstraint", "lastRead", "readCount"}},
				{"users_control", "SHOW USERS", []string{"user", "roles", "passwordChangeRequired", "suspended", "home"}},
				{"schema_ddl", "CREATE INDEX nor335_bolt_i FOR (n:NOR335Bolt) ON (n.p)", nil},
			} {
				t.Run(test.name, func(t *testing.T) {
					base := storage.NewMemoryEngine()
					t.Cleanup(func() { require.NoError(t, base.Close()) })
					exec := cypher.NewStorageExecutor(storage.NewNamespacedEngine(base, "nornic"), cypher.QueryCachePolicy{MaxEntries: 0, TTL: 0})
					checkAbsent := func() {
						rows, err := exec.Execute(context.Background(), "SHOW INDEXES YIELD name WHERE name='nor335_bolt_i' RETURN name", nil)
						require.NoError(t, err)
						require.Empty(t, rows.Rows)
					}
					checkAbsent()
					t.Cleanup(checkAbsent)
					server := New(&Config{Port: 0, MaxConnections: 10, ReadBufferSize: 8192, WriteBufferSize: 8192}, &cypherQueryExecutor{executor: exec})
					conn := openBoltTestConn(t, serveBoltIntegrationServer(t, server))
					require.NoError(t, conn.SetDeadline(time.Now().Add(15*time.Second)))
					require.NoError(t, SendRun(t, conn, "EXPLAIN "+test.query, nil, nil))
					runMetadata, err := AssertSuccess(t, conn)
					require.NoError(t, err)
					fields, ok := runMetadata["fields"].([]interface{})
					require.True(t, ok, "RUN metadata=%v", runMetadata)
					columns := make([]string, 0, len(fields))
					for _, field := range fields {
						name, ok := field.(string)
						require.True(t, ok)
						columns = append(columns, name)
					}
					if len(test.columns) == 0 {
						require.Empty(t, columns)
					} else {
						require.Equal(t, test.columns, columns)
					}
					require.NoError(t, SendPull(t, conn, map[string]any{"n": int64(-1)}))
					terminal, err := AssertSuccess(t, conn)
					require.NoError(t, err, "EXPLAIN must publish no RECORD messages")
					t.Logf("query=%q fields=%v terminal=%v", test.query, columns, terminal)
					require.NotEmpty(t, terminal["plan"])
				})
			}
			for _, query := range []string{"EXPLAIN SHOW PROCEDURES YIELD noSuchColumn RETURN noSuchColumn", "EXPLAIN CREATE INDEX nor335_invalid FOR (n:NOR335Bolt) ON ()"} {
				t.Run(query, func(t *testing.T) {
					base := storage.NewMemoryEngine()
					t.Cleanup(func() { require.NoError(t, base.Close()) })
					store := storage.NewNamespacedEngine(base, "nornic")
					_, port := startBoltIntegrationServer(t, store, boltIntegrationAutocommit)
					conn := openBoltTestConn(t, port)
					require.NoError(t, conn.SetDeadline(time.Now().Add(15*time.Second)))
					t.Cleanup(func() {
						exec := cypher.NewStorageExecutor(store, cypher.QueryCachePolicy{MaxEntries: 0, TTL: 0})
						rows, err := exec.Execute(context.Background(), "SHOW INDEXES YIELD name WHERE name='nor335_invalid' RETURN name", nil)
						require.NoError(t, err)
						require.Empty(t, rows.Rows)
					})
					code, message := runBoltQueryExpectFailure(t, conn, query)
					t.Logf("rejected query=%q code=%s message=%q", query, code, message)
					require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code)
				})
			}
		})
	}
}
