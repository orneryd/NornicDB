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

// A composite statement whose write target another session deletes after the
// statement read it, before the statement writes it, fails and keeps
// nothing: neither the write (a deleted node is not revived, no relationship
// to a deleted endpoint is stored) nor what an earlier part of the statement
// wrote (the sentinel). It runs autocommit and in an explicit transaction,
// with write-behind on and off. Without write-behind the failure is
// Neo.TransientError.Transaction.Outdated, the commit's conflict; with it
// the code is the storage's classification of the vanished target.
func TestFabricConcurrentDeleteLeavesNoPartialWrite(t *testing.T) {
	const node = "MATCH (n:Doc {id: 't'}) WITH n, test.acatomVanishAwait(n.id) AS observed "
	const edge = "MATCH (:Doc {id: 'a'})-[r:R]->() WITH r, test.acatomVanishAwait('a') AS observed "
	const pair = "MATCH (a:Doc {id: 'a'}), (b:Doc {id: 'b'}) WITH a, b, test.acatomVanishAwait(b.id) AS observed "
	const seedNode = "CREATE (:Doc {id: 't', note: 'seed'})"
	const deleteNode = "MATCH (n:Doc {id: 't'}) DETACH DELETE n"
	const nodeLeft = "MATCH (n:Doc {id: 't'}) RETURN count(n)"
	cases := []struct{ name, seed, write, peer, left string }{
		{"node_set", seedNode, node + "SET n.note = 'late' RETURN observed", deleteNode, nodeLeft},
		{"node_map", seedNode, node + "SET n += {note: 'late'} RETURN observed", deleteNode, nodeLeft},
		{"node_remove", seedNode, node + "REMOVE n.note RETURN observed", deleteNode, nodeLeft},
		{"node_label", seedNode, node + "SET n:Extra RETURN observed", deleteNode, nodeLeft},
		{"node_remove_label", seedNode, node + "REMOVE n:Doc RETURN observed", deleteNode, "MATCH (n) WHERE n.id = 't' RETURN count(n)"},
		{"node_key", seedNode, node + "SET n.id = 'moved' RETURN observed", deleteNode, "MATCH (n) WHERE n.id IN ['t', 'moved'] RETURN count(n)"},
		{"edge_set", "CREATE (:Doc {id: 'a'})-[:R {w: 1}]->(:Doc {id: 'b'})", edge + "SET r.w = 2 RETURN observed", "MATCH ()-[r:R]->() DELETE r", "MATCH ()-[r:R]->() RETURN count(r)"},
		{"edge_remove", "CREATE (:Doc {id: 'a'})-[:R {w: 1}]->(:Doc {id: 'b'})", edge + "REMOVE r.w RETURN observed", "MATCH ()-[r:R]->() DELETE r", "MATCH ()-[r:R]->() RETURN count(r)"},
		{"create_endpoint", "CREATE (:Doc {id: 'a'}), (:Doc {id: 'b'})", pair + "CREATE (a)-[:R]->(b) RETURN observed", "MATCH (n:Doc {id: 'b'}) DETACH DELETE n", "MATCH ()-[r:R]->() RETURN count(r)"},
		{"merge_endpoint", "CREATE (:Doc {id: 'a'}), (:Doc {id: 'b'})", pair + "MERGE (a)-[:R]->(b) RETURN observed", "MATCH (n:Doc {id: 'b'}) DETACH DELETE n", "MATCH ()-[r:R]->() RETURN count(r)"},
	}
	for _, writeBehind := range []bool{false, true} {
		t.Run(fmt.Sprintf("write_behind=%v", writeBehind), func(t *testing.T) {
			engine, err := storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: t.TempDir(), LowMemory: true, WriteBehind: writeBehind, WriteBehindInterval: time.Hour})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, engine.Close()) })
			mgr, err := multidb.NewDatabaseManager(engine, nil)
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
				t.Helper()
				result, err := admin.Execute(context.Background(), query, nil)
				require.NoError(t, err, query)
				return result
			}
			run("CREATE CONSTRAINT FOR (n:Doc) REQUIRE n.id IS UNIQUE")
			for _, tc := range cases {
				for _, split := range []bool{false, true} {
					for _, explicit := range []bool{false, true} {
						t.Run(fmt.Sprintf("%s/split=%v/explicit=%v", tc.name, split, explicit), func(t *testing.T) {
							ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
							defer cancel()
							require.NoError(t, engine.FlushWriteBehind())
							run("MATCH (n) DETACH DELETE n")
							require.NoError(t, engine.FlushWriteBehind())
							run(tc.seed)
							require.NoError(t, engine.FlushWriteBehind())

							statement := NewStorageExecutor(comp)
							statement.SetDatabaseManager(&testDatabaseManagerAdapter{manager: mgr})
							if explicit {
								_, err := statement.Execute(ctx, "BEGIN", nil)
								require.NoError(t, err)
							}
							read, resume := make(chan struct{}), make(chan struct{})
							var once, release sync.Once
							defer release.Do(func() { close(resume) })
							previous := PluginFunctionLookup
							PluginFunctionLookup = func(name string) (interface{}, bool) {
								if name == "test.acatomVanishAwait" {
									return func(value string) string {
										once.Do(func() { close(read) })
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
							query := "CALL { USE comp.one " + tc.write + " } RETURN observed"
							if split {
								query = "CALL { USE comp.one CREATE (m:Marker {id: 'sentinel'}) RETURN m.id AS made } " + query
							}
							finished := make(chan error, 1)
							go func() {
								_, err := statement.Execute(ctx, query, nil)
								finished <- err
							}()
							select {
							case <-read:
							case err := <-finished:
								t.Fatalf("the statement ended before its read: %v", err)
							case <-ctx.Done():
								t.Fatal(ctx.Err())
							}
							peer := run(tc.peer)
							require.EqualValues(t, 1, peer.Stats.NodesDeleted+peer.Stats.RelationshipsDeleted)
							release.Do(func() { close(resume) })
							err := <-finished
							if explicit {
								if err == nil {
									_, err = statement.Execute(context.Background(), "COMMIT", nil)
								} else {
									_, _ = statement.Execute(context.Background(), "ROLLBACK", nil)
								}
							}
							require.Error(t, err, "the statement fails")
							if !writeBehind {
								code, message := nornicerrors.Neo4jStatus(err)
								require.Equal(t, nornicerrors.TransientOutdated, code, message)
							}
							require.NoError(t, engine.FlushWriteBehind())
							require.EqualValues(t, 0, run(tc.left).Rows[0][0], "no deleted target is revived and no relationship to a deleted endpoint is stored")
							require.EqualValues(t, 0, run("MATCH (m:Marker) RETURN count(m)").Rows[0][0], "nothing an earlier part of the statement wrote is kept")
						})
					}
				}
			}
		})
	}
}
