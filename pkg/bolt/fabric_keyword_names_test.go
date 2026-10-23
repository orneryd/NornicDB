package bolt

import (
	"context"
	"fmt"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/internal/fabricnametest"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestCompositeKeywordNamesBolt(t *testing.T) {
	mgr, err := multidb.NewDatabaseManager(storage.NewMemoryEngine(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, mgr.Close()) })
	require.NoError(t, mgr.CreateDatabase("one"))
	require.NoError(t, mgr.CreateCompositeDatabase("comp", []multidb.ConstituentRef{{Alias: "one", DatabaseName: "one", Type: "local", AccessMode: "read_write"}}))
	server := NewWithDatabaseManager(&Config{Port: 0, ReadBufferSize: 8192, WriteBufferSize: 8192}, &mockExecutor{}, mgr)
	t.Cleanup(func() { _ = server.Close() })
	port := startBoltTestServer(t, server)
	ctx := context.Background()
	driver, err := neo4j.NewDriverWithContext(fmt.Sprintf("bolt://127.0.0.1:%d", port), neo4j.NoAuth())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, driver.Close(ctx)) })
	seed := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "comp"})
	result, err := seed.Run(ctx, "USE comp.one CREATE (:KeywordNameSeed {v:7})", nil)
	require.NoError(t, err)
	_, err = result.Consume(ctx)
	require.NoError(t, err)
	require.NoError(t, seed.Close(ctx))
	for _, explicit := range []bool{false, true} {
		t.Run(fmt.Sprintf("explicit=%v", explicit), func(t *testing.T) {
			for _, tc := range fabricnametest.Cases() {
				t.Run(tc.Name, func(t *testing.T) {
					session := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "comp"})
					defer session.Close(ctx)
					var result neo4j.ResultWithContext
					var err error
					if explicit {
						tx, beginErr := session.BeginTransaction(ctx)
						require.NoError(t, beginErr)
						defer tx.Close(ctx)
						result, err = tx.Run(ctx, tc.Query, tc.Params)
					} else {
						result, err = session.Run(ctx, tc.Query, tc.Params)
					}
					require.NoError(t, err, "%s", tc.Query)
					keys, err := result.Keys()
					require.NoError(t, err)
					require.Equal(t, []string{"k"}, keys)
					records, err := result.Collect(ctx)
					require.NoError(t, err)
					require.Len(t, records, 1)
					require.Equal(t, []interface{}{int64(7)}, records[0].Values)
				})
			}
		})
	}
}
