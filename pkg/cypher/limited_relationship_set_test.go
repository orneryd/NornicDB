package cypher_test

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"fmt"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A relationship SET in a database with storage limits commits through the
// managed path and must not report a false conflict for the transaction's
// own observation (NOR-418), on both parsers, implicit and explicit, on a
// plain and a write-behind store.
func TestRelationshipSetInLimitedDatabase(t *testing.T) {
	for _, parser := range []string{"nornic", "antlr"} {
		t.Run(parser, func(t *testing.T) {
			previousParser := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previousParser) })
			for _, writeBehind := range []bool{false, true} {
				for _, explicit := range []bool{false, true} {
					t.Run(fmt.Sprintf("write-behind=%t/explicit=%t", writeBehind, explicit), func(t *testing.T) {
						ctx := t.Context()
						base, err := runtimeFixtureOwner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{
							DataDir: t.TempDir(), LowMemory: true, WriteBehind: writeBehind, WriteBehindInterval: time.Hour,
						}))
						require.NoError(t, err)
						manager, err := multidb.NewDatabaseManager(ctx, base, nil)
						require.NoError(t, err)
						t.Cleanup(func() { _ = manager.Close() })
						name := manager.DefaultDatabaseName()
						engine, err := manager.GetStorage(name)
						require.NoError(t, err)
						executor := cypher.NewStorageExecutor(engine)
						_, err = executor.Execute(ctx, "CREATE (:P {id:'a'})-[:COUNT {n:0}]->(:P {id:'b'})", nil)
						require.NoError(t, err)
						require.NoError(t, manager.SetDatabaseLimits(ctx, name, &multidb.Limits{Storage: multidb.StorageLimits{MaxNodes: 100, MaxEdges: 10}}))

						if explicit {
							_, err = executor.Execute(ctx, "BEGIN", nil)
							require.NoError(t, err)
						}
						_, err = executor.Execute(ctx, "MATCH (:P {id:'a'})-[r:COUNT]->(:P {id:'b'}) SET r.n = r.n + 1 RETURN r.n", nil)
						require.NoError(t, err)
						if explicit {
							_, err = executor.Execute(ctx, "COMMIT", nil)
							require.NoError(t, err)
						}
						result, err := executor.Execute(ctx, "MATCH (:P {id:'a'})-[r:COUNT]->(:P {id:'b'}) RETURN r.n AS n", nil)
						require.NoError(t, err)
						require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
					})
				}
			}
		})
	}
}
