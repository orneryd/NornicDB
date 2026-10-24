package storage_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/bolt"
	"github.com/orneryd/nornicdb/pkg/cypher"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Pause the real autocommit statement after SET has read/staged its node.
// No transaction or error wrapper is mocked.
func TestWriteBehindUniqueClaimConflictPublic(t *testing.T) {
	testWriteBehindUniqueClaimConflictPublic(t, false, false)
}

func TestWriteBehindDirectPeerUniqueClaimConflictPublic(t *testing.T) {
	testWriteBehindUniqueClaimConflictPublic(t, true, false)
}

func TestWriteBehindRetiredBaselineConflictPublic(t *testing.T) {
	testWriteBehindUniqueClaimConflictPublic(t, true, true)
}

type conflictPublicMode struct {
	async            bool
	explicit         bool
	delete           bool
	pauseBeforeStage bool
	claim            string
	recreate         bool
	materialization  string
	reread           bool
}

func TestNodeReadImageRepeatedReadPublic(t *testing.T) {
	testWriteBehindUniqueClaimConflictPublic(t, true, false, conflictPublicMode{async: true, explicit: true, claim: "claimed", reread: true})
}

func TestNodeClaimIdentityPublic(t *testing.T) {
	for _, recreate := range []bool{false, true} {
		t.Run(fmt.Sprintf("recreate=%t", recreate), func(t *testing.T) {
			testWriteBehindUniqueClaimConflictPublic(t, true, false, conflictPublicMode{async: true, claim: "persisted", recreate: recreate})
		})
	}
}

func TestNodeMaterializationIdentityPublic(t *testing.T) {
	for _, source := range []string{"retrieve", "shortest_path"} {
		for _, explicit := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/explicit=%t", source, explicit), func(t *testing.T) {
				testWriteBehindUniqueClaimConflictPublic(t, true, false, conflictPublicMode{async: true, explicit: explicit, claim: "claimed", materialization: source})
			})
		}
	}
}

// Configure the real database-scoped Bolt executor with the same retrieval
// service as Execute. This adapter constructs no transaction or error wrappers.
type conflictSearchConfigurator struct {
	executor *cypher.StorageExecutor
	service  *search.Service
}

func (c *conflictSearchConfigurator) ConfigureDatabaseExecutor(executor *cypher.StorageExecutor, _ string, _ storage.Engine) {
	executor.SetSearchService(c.service)
}

func (c *conflictSearchConfigurator) Execute(ctx context.Context, query string, params map[string]any) (*bolt.QueryResult, error) {
	result, err := c.executor.Execute(ctx, query, params)
	if err != nil {
		return nil, err
	}
	return &bolt.QueryResult{Columns: result.Columns, Rows: result.Rows}, nil
}

func TestNodeObservedKeyConflictPublic(t *testing.T) {
	testWriteBehindUniqueClaimConflictPublic(t, true, false, conflictPublicMode{async: true, pauseBeforeStage: true})
}

func TestNodeWriteConflictPublicModes(t *testing.T) {
	for _, async := range []bool{false, true} {
		for _, explicit := range []bool{false, true} {
			for _, deleting := range []bool{false, true} {
				t.Run(fmt.Sprintf("async=%t/explicit=%t/delete=%t", async, explicit, deleting), func(t *testing.T) {
					testWriteBehindUniqueClaimConflictPublic(t, true, false, conflictPublicMode{async: async, explicit: explicit, delete: deleting})
				})
			}
		}
	}
}

func testWriteBehindUniqueClaimConflictPublic(t *testing.T, directPeer, retireBeforeClassify bool, modes ...conflictPublicMode) {
	mode := conflictPublicMode{async: true}
	if len(modes) > 0 {
		mode = modes[0]
	}
	persistedCases := []bool{false, true}
	changedKeyCases := []bool{false, true}
	claim := mode.claim
	if claim == "" {
		claim = "claimed"
	} else {
		changedKeyCases = []bool{true}
	}
	if directPeer {
		persistedCases = []bool{true}
	}
	if retireBeforeClassify || mode.delete {
		changedKeyCases = []bool{false}
	}
	for _, route := range []string{"Execute", "Bolt"} {
		for _, persisted := range persistedCases {
			for _, changedKey := range changedKeyCases {
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
						DataDir: t.TempDir(), LowMemory: true, WriteBehind: mode.async,
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
					if mode.async {
						base.CommitConflictStopWriteBehindForTest()
					}
					if persisted {
						run("CREATE (:Doc {id:'persisted'})")
						require.NoError(t, base.FlushWriteBehind())
						run("MATCH (n:Doc) SET n.id='original'")
					} else {
						run("CREATE (:Doc {id:'original'})")
					}
					var service *search.Service
					if mode.materialization != "" {
						run("MATCH (n:Doc) SET n.content='zephyrine harbour notes'")
						if mode.materialization == "shortest_path" {
							run("MATCH (n:Doc) CREATE (:Start)-[:LINK]->(n)-[:LINK]->(:Finish)")
						}
						require.NoError(t, base.FlushWriteBehind())
						service = search.NewService(store)
						t.Cleanup(func() { require.NoError(t, service.Close()) })
						require.NoError(t, service.BuildIndexes(ctx))
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
					detected, classify := make(chan struct{}), make(chan struct{})
					var allowClassify sync.Once
					defer allowClassify.Do(func() { close(classify) })
					if retireBeforeClassify {
						base.CommitConflictAfterDetectionForTest(func(storage.NodeID) {
							close(detected)
							select {
							case <-classify:
							case <-ctx.Done():
							}
						})
					}
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
						query += ", n.id='" + claim + "'"
						peerQuery += ", n.id='" + claim + "'"
					}
					query += " RETURN test.awaitPeer(n.id)"
					if mode.pauseBeforeStage {
						query = "MATCH (n:Doc {id:'original'}) WITH n, test.awaitPeer(n.id) AS observed SET n.summary_input='loser'"
						if changedKey {
							query += ", n.id='claimed'"
						}
						query += " RETURN observed"
					}
					if mode.delete {
						query = "MATCH (n:Doc {id:'original'}) DELETE n RETURN test.awaitPeer('original')"
					}
					if mode.materialization != "" {
						query = "CALL db.retrieve({query:'zephyrine', types:['Doc'], limit:1}) YIELD node AS n "
						if mode.materialization == "shortest_path" {
							query = "MATCH (a:Start), (z:Finish) MATCH p = shortestPath((a)-[:LINK*]->(z)) WITH nodes(p)[1] AS n "
						}
						query += "WITH n, test.awaitPeer(n.id) AS observed SET n.id='claimed', n.summary_input='loser' RETURN observed"
					}
					secondQuery := "MATCH (n:Doc {id:'claimed'}) SET n.summary_input='loser' RETURN n.id"
					if mode.reread {
						query = "MATCH (n:Doc {id:'original'}) RETURN test.awaitPeer(n.id)"
					}
					runLoser := func() error {
						loser := cypher.NewStorageExecutor(store)
						if service != nil {
							loser.SetSearchService(service)
						}
						if mode.explicit {
							if _, err := loser.Execute(ctx, "BEGIN", nil); err != nil {
								return err
							}
							defer loser.Execute(context.Background(), "ROLLBACK", nil)
						}
						_, err := loser.Execute(ctx, query, nil)
						if err == nil && mode.reread {
							result, readErr := loser.Execute(ctx, secondQuery, nil)
							err = readErr
							if err == nil && (len(result.Rows) != 1 || result.Rows[0][0] != claim) {
								return fmt.Errorf("second read did not observe claimed key: %#v", result.Rows)
							}
						}
						if err == nil && mode.explicit {
							_, err = loser.Execute(ctx, "COMMIT", nil)
						}
						return err
					}
					if route == "Bolt" {
						listener, err := net.Listen("tcp", "127.0.0.1:0")
						require.NoError(t, err)
						var configured bolt.QueryExecutor
						if service != nil {
							configured = &conflictSearchConfigurator{executor: exec, service: service}
						}
						server := bolt.NewWithDatabaseManager(&bolt.Config{ReadBufferSize: 8192, WriteBufferSize: 8192}, configured, manager)
						served := make(chan error, 1)
						go func() { served <- server.Serve(listener) }()
						t.Cleanup(func() { require.NoError(t, server.Close()); require.NoError(t, <-served) })
						driver, err := neo4j.NewDriverWithContext("bolt://"+listener.Addr().String(), neo4j.NoAuth())
						require.NoError(t, err)
						defer driver.Close(context.Background())
						runLoser = func() error {
							session := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "nornic"})
							defer session.Close(context.Background())
							if mode.explicit {
								tx, err := session.BeginTransaction(ctx)
								if err != nil {
									return err
								}
								defer tx.Close(context.Background())
								result, err := tx.Run(ctx, query, nil)
								if err == nil {
									_, err = result.Consume(ctx)
								}
								if err != nil {
									return err
								}
								if mode.reread {
									result, err = tx.Run(ctx, secondQuery, nil)
									if err != nil {
										return err
									}
									rows, err := result.Collect(ctx)
									if err != nil {
										return err
									}
									if len(rows) != 1 || rows[0].Values[0] != claim {
										return fmt.Errorf("second read did not observe claimed key: %#v", rows)
									}
								}
								return tx.Commit(ctx)
							}
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
					if directPeer {
						peer, readErr := store.GetNode(nodes[0].ID)
						require.NoError(t, readErr)
						require.Equal(t, "original", peer.Properties["id"])
						peer.Properties["summary_input"] = "peer"
						if changedKey {
							peer.Properties["id"] = claim
						}
						if mode.recreate {
							require.NoError(t, store.DeleteNode(peer.ID))
							_, err := store.CreateNode(peer)
							require.NoError(t, err)
						} else {
							require.NoError(t, store.UpdateNode(peer))
						}
						// Keep the acknowledged rename buffered while the public
						// direct writer publishes its newer physical head.
					} else {
						run(peerQuery)
						require.NoError(t, base.FlushWriteBehind())
					}
					release.Do(func() { close(resume) })
					if retireBeforeClassify {
						select {
						case <-detected:
						case <-ctx.Done():
							t.Fatal("loser did not detect the direct writer's conflict")
						}
						require.NoError(t, base.FlushWriteBehind())
						allowClassify.Do(func() { close(classify) })
					}
					err = <-finished
					require.Error(t, err)
					t.Logf("commit outcome: %v", err)
					wantUnique := changedKey && !mode.reread
					if route == "Execute" {
						var violation *storage.ConstraintViolationError
						require.Equal(t, wantUnique, errors.As(err, &violation), "commit error: %v", err)
						if !wantUnique {
							require.ErrorIs(t, err, storage.ErrConflict)
							code, _ := nornicerrors.Neo4jStatus(err)
							require.Equal(t, "Neo.TransientError.Transaction.Outdated", code)
						}
					} else {
						var failure *neo4j.Neo4jError
						require.ErrorAs(t, err, &failure)
						if !wantUnique {
							require.Equal(t, "Neo.TransientError.Transaction.Outdated", failure.Code)
							require.True(t, failure.IsRetriable())
						}
					}
					if wantUnique {
						require.Contains(t, err.Error(), "claimed by concurrent commit")
					} else {
						require.NotContains(t, err.Error(), "UNIQUE")
						require.Contains(t, err.Error(), "changed after transaction start")
					}
					node, err := store.GetNode(nodes[0].ID)
					require.NoError(t, err)
					if directPeer {
						require.NotEqual(t, "loser", node.Properties["summary_input"])
					} else {
						require.Equal(t, "peer", node.Properties["summary_input"])
					}
				})
			}
		}
	}
}
