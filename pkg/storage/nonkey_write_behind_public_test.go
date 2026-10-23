package storage_test

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/bolt"
	"github.com/orneryd/nornicdb/pkg/cypher"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Pause the real autocommit statement after SET has read/staged its node.
// The peer then commits and flushes both acknowledged versions before the
// loser reaches commit. No transaction or error wrapper is mocked.
func TestWriteBehindUniqueClaimConflictPublic(t *testing.T) {
	for _, route := range []string{"Execute", "Bolt"} {
		for _, persisted := range []bool{false, true} {
			for _, changedKey := range []bool{false, true} {
				name := route + "/unflushed_create/unchanged_key"
				if persisted {
					name = route + "/unflushed_update/unchanged_key"
				}
				if changedKey {
					name += "_then_claim"
				}
				t.Run(name, func(t *testing.T) {
					ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
					defer cancel()
					base, err := storage.NewBadgerEngineWithOptions(storage.BadgerOptions{
						DataDir: t.TempDir(), LowMemory: true, WriteBehind: true,
						WriteBehindInterval: time.Hour, WriteBehindMaxOps: 100000,
					})
					require.NoError(t, err)
					t.Cleanup(func() { require.NoError(t, base.Close()) })
					manager, err := multidb.NewDatabaseManager(base, nil)
					require.NoError(t, err)
					t.Cleanup(func() { require.NoError(t, manager.Close()) })
					store, err := manager.GetStorage("nornic")
					require.NoError(t, err)
					exec := cypher.NewStorageExecutorWithQueryCachePolicy(store, 0, 0)
					run := func(query string) *cypher.ExecuteResult {
						t.Helper()
						result, err := exec.Execute(ctx, query, nil)
						require.NoError(t, err)
						return result
					}
					run("CREATE CONSTRAINT FOR (n:Doc) REQUIRE n.id IS UNIQUE")
					require.NoError(t, base.FlushWriteBehind())
					base.CommitConflictStopWriteBehindForTest()
					if persisted {
						run("CREATE (:Doc {id:'persisted'})")
						require.NoError(t, base.FlushWriteBehind())
						run("MATCH (n:Doc) SET n.id='original'")
					} else {
						run("CREATE (:Doc {id:'original'})")
					}
					nodes, err := store.GetNodesByLabel("Doc")
					require.NoError(t, err)
					require.Len(t, nodes, 1)
					physicalID := storage.NodeID("nornic:" + string(nodes[0].ID))
					_, headErr := base.GetNodeCurrentHead(physicalID)
					if !persisted {
						require.ErrorIs(t, headErr, storage.ErrNotFound, "acknowledged node must still be unflushed")
					} else {
						require.NoError(t, headErr)
					}
					staged, resume := make(chan struct{}), make(chan struct{})
					var signal, release sync.Once
					defer release.Do(func() { close(resume) })
					previous := cypher.PluginFunctionLookup
					cypher.PluginFunctionLookup = func(name string) (interface{}, bool) {
						if name == "test.awaitPeer" {
							return func(value string) string {
								signal.Do(func() { close(staged) })
								select {
								case <-resume:
								case <-ctx.Done():
								}
								return value
							}, true
						}
						if previous != nil {
							return previous(name)
						}
						return nil, false
					}
					defer func() { cypher.PluginFunctionLookup = previous }()
					query := "MATCH (n:Doc {id:'original'}) SET n.summary_input='loser'"
					peerQuery := "MATCH (n:Doc {id:'original'}) SET n.summary_input='peer'"
					if changedKey {
						query += ", n.id='claimed'"
						peerQuery += ", n.id='claimed'"
					}
					query += " RETURN test.awaitPeer(n.id)"
					runLoser := func() error {
						_, err := cypher.NewStorageExecutor(store).Execute(ctx, query, nil)
						return err
					}
					if route == "Bolt" {
						listener, err := net.Listen("tcp", "127.0.0.1:0")
						require.NoError(t, err)
						server := bolt.NewWithDatabaseManager(&bolt.Config{ReadBufferSize: 8192, WriteBufferSize: 8192}, nil, manager)
						served := make(chan error, 1)
						go func() { served <- server.Serve(listener) }()
						t.Cleanup(func() { require.NoError(t, server.Close()); require.NoError(t, <-served) })
						driver, err := neo4j.NewDriverWithContext("bolt://"+listener.Addr().String(), neo4j.NoAuth())
						require.NoError(t, err)
						defer driver.Close(context.Background())
						runLoser = func() error {
							session := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "nornic"})
							defer session.Close(context.Background())
							result, err := session.Run(ctx, query, nil)
							if err == nil {
								_, err = result.Consume(ctx)
							}
							return err
						}
					}
					finished := make(chan error, 1)
					go func() { finished <- runLoser() }()
					select {
					case <-staged:
					case err := <-finished:
						t.Fatalf("loser did not reach the commit barrier: %v", err)
					case <-ctx.Done():
						t.Fatal("loser did not stage its update")
					}
					run(peerQuery)
					require.NoError(t, base.FlushWriteBehind())
					release.Do(func() { close(resume) })
					err = <-finished
					require.Error(t, err)
					t.Logf("commit outcome: %v", err)
					if route == "Execute" {
						var violation *storage.ConstraintViolationError
						require.Equal(t, changedKey, errors.As(err, &violation), "commit error: %v", err)
						if !changedKey {
							require.ErrorIs(t, err, storage.ErrConflict)
							code, _ := nornicerrors.Neo4jStatus(err)
							require.Equal(t, "Neo.TransientError.Transaction.Outdated", code)
						}
					} else {
						var failure *neo4j.Neo4jError
						require.ErrorAs(t, err, &failure)
						if !changedKey {
							require.Equal(t, "Neo.TransientError.Transaction.Outdated", failure.Code)
							require.True(t, failure.IsRetriable())
						}
					}
					if changedKey {
						require.Contains(t, err.Error(), "claimed by concurrent commit")
					} else {
						require.NotContains(t, err.Error(), "UNIQUE")
						require.Contains(t, err.Error(), "changed after transaction start")
					}
					node, err := store.GetNode(nodes[0].ID)
					require.NoError(t, err)
					require.Equal(t, "peer", node.Properties["summary_input"])
				})
			}
		}
	}
}
