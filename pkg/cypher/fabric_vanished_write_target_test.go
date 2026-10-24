package cypher

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A local Fabric constituent's write whose target a peer deleted after the
// fragment matched it reports the ordinary retryable conflict through the
// shard transaction, and never revives the target or stores a relationship to
// a deleted node.
func TestFabricVanishedWriteTarget(t *testing.T) {
	const nodeMatch = "MATCH (n:Doc {id:'t'}) WITH n, test.fabricVanishedAwait(n.id) AS observed "
	const relMatch = "MATCH (:Doc {id:'a'})-[r:R]->() WITH r, test.fabricVanishedAwait('a') AS observed "
	const pairMatch = "MATCH (a:Doc {id:'a'}), (b:Doc {id:'b'}) WITH a, b, test.fabricVanishedAwait(b.id) AS observed "
	const nodeSeed = "CREATE (:Doc {id:'t', note:'seed'})"
	const pairSeed = "CREATE (:Doc {id:'a'})-[:R {w:1}]->(:Doc {id:'b'})"
	const endpointSeed = "CREATE (:Doc {id:'a'}), (:Doc {id:'b'})"
	const deleteNode = "MATCH (n:Doc {id:'t'}) DETACH DELETE n"
	const deleteRel = "MATCH ()-[r:R]->() DELETE r"
	const deleteEndpoint = "MATCH (n:Doc {id:'b'}) DETACH DELETE n"
	const nodesLeft = "MATCH (n:Doc {id:'t'}) RETURN count(n)"
	const relsLeft = "MATCH ()-[r:R]->() RETURN count(r)"
	cases := []struct {
		name, seed, loser, peer, remaining string
	}{
		{"node_set", nodeSeed, nodeMatch + "SET n.note = 'loser' RETURN observed", deleteNode, nodesLeft},
		{"node_map", nodeSeed, nodeMatch + "SET n += {note: 'loser'} RETURN observed", deleteNode, nodesLeft},
		{"node_remove", nodeSeed, nodeMatch + "REMOVE n.note RETURN observed", deleteNode, nodesLeft},
		{"node_label", nodeSeed, nodeMatch + "SET n:Extra RETURN observed", deleteNode, nodesLeft},
		{"node_remove_label", nodeSeed, nodeMatch + "REMOVE n:Doc RETURN observed", deleteNode, "MATCH (n) WHERE n.id = 't' RETURN count(n)"},
		{"node_key", nodeSeed, nodeMatch + "SET n.id = 'moved' RETURN observed", deleteNode, "MATCH (n) WHERE n.id IN ['t', 'moved'] RETURN count(n)"},
		{"edge_set", pairSeed, relMatch + "SET r.w = 2 RETURN observed", deleteRel, relsLeft},
		{"edge_remove", pairSeed, relMatch + "REMOVE r.w RETURN observed", deleteRel, relsLeft},
		{"create_endpoint", endpointSeed, pairMatch + "CREATE (a)-[:R]->(b) RETURN observed", deleteEndpoint, relsLeft},
		{"merge_endpoint", endpointSeed, pairMatch + "MERGE (a)-[:R]->(b) RETURN observed", deleteEndpoint, relsLeft},
	}
	for _, async := range []bool{false, true} {
		for _, explicit := range []bool{false, true} {
			t.Run(fmt.Sprintf("async=%t/explicit=%t", async, explicit), func(t *testing.T) {
				if !explicit {
					// Autocommit local fragments write outside a shard transaction
					// until #1638 runs the whole statement in one; merge #1638 first.
					t.Skip("autocommit local Fabric writes require the statement transaction from #1638")
				}
				b, err := storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: t.TempDir(), LowMemory: true, WriteBehind: async, WriteBehindInterval: time.Hour})
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, b.Close()) })
				mgr, err := multidb.NewDatabaseManager(b, nil)
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, mgr.Close()) })
				require.NoError(t, mgr.CreateDatabase("leaf"))
				require.NoError(t, mgr.CreateCompositeDatabase("comp", []multidb.ConstituentRef{{Alias: "one", DatabaseName: "leaf", Type: "local", AccessMode: "read_write"}}))
				leaf, err := mgr.GetStorage("leaf")
				require.NoError(t, err)
				comp, err := mgr.GetStorage("comp")
				require.NoError(t, err)
				admin := NewStorageExecutor(leaf)
				run := func(query string) *ExecuteResult {
					result, err := admin.Execute(context.Background(), query, nil)
					require.NoError(t, err)
					return result
				}
				run("CREATE CONSTRAINT FOR (n:Doc) REQUIRE n.id IS UNIQUE")
				for _, tc := range cases {
					t.Run(tc.name, func(t *testing.T) {
						ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
						defer cancel()
						require.NoError(t, b.FlushWriteBehind())
						run("MATCH (n) DETACH DELETE n")
						require.NoError(t, b.FlushWriteBehind())
						run(tc.seed)
						require.NoError(t, b.FlushWriteBehind())
						loser := NewStorageExecutor(comp)
						loser.SetDatabaseManager(&testDatabaseManagerAdapter{manager: mgr})
						if explicit {
							_, err := loser.Execute(ctx, "BEGIN", nil)
							require.NoError(t, err)
							defer loser.Execute(context.Background(), "ROLLBACK", nil)
						}
						matched, resume := make(chan struct{}), make(chan struct{})
						var signal, release sync.Once
						defer release.Do(func() { close(resume) })
						previous := PluginFunctionLookup
						PluginFunctionLookup = func(name string) (interface{}, bool) {
							if name == "test.fabricVanishedAwait" {
								return func(value string) string {
									signal.Do(func() { close(matched) })
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
						defer func() { PluginFunctionLookup = previous }()
						finished := make(chan error, 1)
						go func() {
							_, err := loser.Execute(ctx, "CALL { USE comp.one "+tc.loser+" } RETURN observed", nil)
							finished <- err
						}()
						select {
						case <-matched:
						case err := <-finished:
							t.Fatalf("loser finished before the barrier: %v", err)
						case <-ctx.Done():
							t.Fatal(ctx.Err())
						}
						peer := run(tc.peer)
						require.EqualValues(t, 1, peer.Stats.NodesDeleted+peer.Stats.RelationshipsDeleted, "peer must delete exactly its target")
						release.Do(func() { close(resume) })
						err := <-finished
						if err == nil && explicit {
							_, err = loser.Execute(ctx, "COMMIT", nil)
						}
						require.Error(t, err, "the loser wrote a target a peer deleted")
						code, _ := nornicerrors.Neo4jStatus(err)
						require.Equal(t, nornicerrors.TransientOutdated, code, "%v", err)
						require.NoError(t, b.FlushWriteBehind())
						require.EqualValues(t, 0, run(tc.remaining).Rows[0][0], "deleted target revived or dangling relationship stored")
					})
				}
			})
		}
	}
}
