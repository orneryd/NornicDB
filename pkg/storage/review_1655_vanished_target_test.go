package storage_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/stretchr/testify/require"
)

// A statement has matched its target; a peer deletes that target before the
// statement writes it. Every such write must report the ordinary retryable
// conflict on every route, never a client error, and must not revive the
// target or attach a relationship to a deleted node.
func TestReview1655VanishedWriteTarget(t *testing.T) {
	const nodeSeed = "CREATE (:Doc {id:'t', note:'seed'})"
	const pairSeed = "CREATE (:Doc {id:'a'})-[:R {w:1}]->(:Doc {id:'b'})"
	const endpointSeed = "CREATE (:Doc {id:'a'}), (:Doc {id:'b'})"
	const deleteNode = "MATCH (n:Doc {id:'t'}) DETACH DELETE n"
	const deleteRel = "MATCH (:Doc {id:'a'})-[r:R]->() DELETE r"
	const deleteEndpoint = "MATCH (n:Doc {id:'b'}) DETACH DELETE n"
	const nodeMatch = "MATCH (n:Doc {id:'t'}) WITH n, test.r2AwaitVanished(n.id) AS observed "
	const relMatch = "MATCH (:Doc {id:'a'})-[r:R]->() WITH r, test.r2AwaitVanished('a') AS observed "
	const pairMatch = "MATCH (a:Doc {id:'a'}), (b:Doc {id:'b'}) WITH a, b, test.r2AwaitVanished(b.id) AS observed "
	const nodesLeft = "MATCH (n:Doc {id:'t'}) RETURN count(n)"
	const relsLeft = "MATCH ()-[r:R]->() RETURN count(r)"
	cases := []struct {
		name, seed, loser, peer, remaining string
	}{
		{"node_set_property", nodeSeed, nodeMatch + "SET n.note = 'loser' RETURN observed", deleteNode, nodesLeft},
		{"node_set_map", nodeSeed, nodeMatch + "SET n += {note: 'loser'} RETURN observed", deleteNode, nodesLeft},
		{"node_remove_property", nodeSeed, nodeMatch + "REMOVE n.note RETURN observed", deleteNode, nodesLeft},
		{"node_set_label", nodeSeed, nodeMatch + "SET n:Extra RETURN observed", deleteNode, nodesLeft},
		{"node_remove_label", nodeSeed, nodeMatch + "REMOVE n:Doc RETURN observed", deleteNode, nodesLeft},
		{"node_change_key", nodeSeed, nodeMatch + "SET n.id = 'moved' RETURN observed", deleteNode, "MATCH (n:Doc) WHERE n.id IN ['t', 'moved'] RETURN count(n)"},
		{"relationship_set_property", pairSeed, relMatch + "SET r.w = 2 RETURN observed", deleteRel, relsLeft},
		{"relationship_remove_property", pairSeed, relMatch + "REMOVE r.w RETURN observed", deleteRel, relsLeft},
		{"create_relationship_to_deleted_node", endpointSeed, pairMatch + "CREATE (a)-[:R]->(b) RETURN observed", deleteEndpoint, relsLeft},
		{"merge_relationship_to_deleted_node", endpointSeed, pairMatch + "MERGE (a)-[:R]->(b) RETURN observed", deleteEndpoint, relsLeft},
	}
	for _, async := range []bool{false, true} {
		for _, route := range r2Routes {
			t.Run(fmt.Sprintf("async=%t/%s/explicit=%t", async, route.name, route.explicit), func(t *testing.T) {
				// Peer deletes stay acknowledged and unflushed while the loser
				// writes; seeds are flushed so they are existing stored data.
				env := r2Open(t, t.TempDir(), async, time.Hour)
				admin := cypher.NewStorageExecutor(env.store)
				r2MustRun(t, admin, "CREATE CONSTRAINT FOR (n:Doc) REQUIRE n.id IS UNIQUE", nil)
				flush := func() {
					if async {
						require.NoError(t, env.badger.FlushWriteBehind())
					}
				}
				for _, tc := range cases {
					t.Run(tc.name, func(t *testing.T) {
						ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
						defer cancel()
						flush()
						r2MustRun(t, admin, "MATCH (n) DETACH DELETE n", nil)
						flush()
						r2MustRun(t, admin, tc.seed, nil)
						flush()
						matched, resume := make(chan struct{}), make(chan struct{})
						var signal, release sync.Once
						defer release.Do(func() { close(resume) })
						previous := cypher.PluginFunctionLookup
						cypher.PluginFunctionLookup = func(name string) (interface{}, bool) {
							if name == "test.r2AwaitVanished" {
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
						defer func() { cypher.PluginFunctionLookup = previous }()
						finished := make(chan r2Outcome, 1)
						go func() { finished <- env.run(ctx, cypher.NewStorageExecutor(env.store), route, tc.loser, nil) }()
						select {
						case <-matched:
						case outcome := <-finished:
							t.Fatalf("loser finished before the barrier: %+v", outcome)
						case <-ctx.Done():
							t.Fatal("loser never reached the barrier")
						}
						result, err := cypher.NewStorageExecutor(env.store).Execute(ctx, tc.peer, nil)
						require.NoError(t, err)
						require.Equal(t, 1, result.Stats.NodesDeleted+result.Stats.RelationshipsDeleted, "peer must delete exactly its target")
						release.Do(func() { close(resume) })
						outcome := <-finished
						require.Error(t, outcome.err, "the loser wrote a target a peer deleted")
						require.Equal(t, "Neo.TransientError.Transaction.Outdated", outcome.code, "%s", outcome.msg)
						require.False(t, outcome.unique, "%s", outcome.msg)
						remaining := r2MustRun(t, admin, tc.remaining, nil)
						require.EqualValues(t, 0, remaining.Rows[0][0], "the deleted target or a dangling relationship survived")
					})
				}
			})
		}
	}
}
