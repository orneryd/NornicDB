package cypher

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A CREATE that fails anywhere - a property expression, a property value, a
// constraint - writes nothing, on the memory engine and on the server stack's
// async auto-commit route, which writes without a transaction (#628). Neo4j
// 5.26.30 rejects every failing statement with the same error and stores
// nothing; the valid statements write the whole pattern.
func TestCreateFailureWritesNothing(t *testing.T) {
	failing := []struct {
		stmt    string
		wantErr string
	}{
		{"CREATE (:T {x: 1 / 0})", "/ by zero"},
		{"CREATE (:T {id: 1}), (:T {x: 1 / 0})", "/ by zero"},
		{"CREATE (:T {id: 1}), (:T {x: 1 / 0}) RETURN 1 AS one", "/ by zero"},
		{"CREATE (a:T {id: 1}) CREATE (b:T {x: 1 / 0})", "/ by zero"},
		{"CREATE (:T)-[:R]->(:T), (:T {x: 1 / 0})", "/ by zero"},
		{"CREATE (:T)-[:R]->(:T)-[:S {x: 1 / 0}]->(:T)", "/ by zero"},
		{"CREATE (a:T)-[:R]->(b:T) CREATE (c:T {x: 1 / 0})", "/ by zero"},
		{"CREATE (a:T {id: 1}) WITH a CREATE (b:T {x: 1 / 0})", "/ by zero"},
		{"UNWIND [1, 0] AS d CREATE (:T {x: 1 / d})", "/ by zero"},
		{"CREATE (:T {x: 1})-[:R]->(:T {m: [[1]]})", "unsupported type"},
		{"CREATE (:T {x: 1}), (:T {m: {a: 1}})", "unsupported type"},
		{"CREATE (:U {k: 1}), (:U {k: 1})", "already exists"},
		{"CREATE (:U {k: 1})-[:R]->(:U {k: 1})", "already exists"},
		{"CREATE (:U {k: 2})-[:R]->(:T), (:U {k: 2})", "already exists"},
		{"CREATE (:T)-[:R]->(:T), (:U {k: 5})", "already exists"},
	}
	stacks := map[string]func(t *testing.T) *StorageExecutor{
		"memory": func(t *testing.T) *StorageExecutor {
			exec, _ := newTestExecutor(t)
			return exec
		},
		"server stack": newSetRouteServerStackExecutor,
	}
	for stack, build := range stacks {
		for _, mode := range []string{"auto-commit", "explicit transaction"} {
			for _, tc := range failing {
				t.Run(stack+"/"+mode+"/"+tc.stmt, func(t *testing.T) {
					exec := build(t)
					ctx := context.Background()
					_, err := exec.Execute(ctx, "CREATE CONSTRAINT u_k FOR (u:U) REQUIRE u.k IS UNIQUE", nil)
					require.NoError(t, err)
					_, err = exec.Execute(ctx, "CREATE (:U {k: 5})", nil)
					require.NoError(t, err)
					if mode == "explicit transaction" {
						_, err = exec.Execute(ctx, "BEGIN", nil)
						require.NoError(t, err)
					}
					_, err = exec.Execute(ctx, tc.stmt, nil)
					if mode == "explicit transaction" {
						if err == nil {
							_, err = exec.Execute(ctx, "COMMIT", nil)
						} else {
							_, _ = exec.Execute(ctx, "ROLLBACK", nil)
						}
					}
					require.Error(t, err)
					assert.Contains(t, err.Error(), tc.wantErr)
					nodes, err := exec.Execute(ctx, "MATCH (n) RETURN labels(n) AS l, properties(n) AS p", nil)
					require.NoError(t, err)
					assert.Equal(t, [][]interface{}{{[]interface{}{"U"}, map[string]interface{}{"k": int64(5)}}}, nodes.Rows, "nothing written")
					rels, err := exec.Execute(ctx, "MATCH ()-[r]->() RETURN count(r) AS c", nil)
					require.NoError(t, err)
					assert.Equal(t, [][]interface{}{{int64(0)}}, rels.Rows, "no relationship written")
				})
			}
		}
	}
}

// Valid CREATE statements still write every node and relationship of the
// pattern, bind variables across patterns and clauses, and report them.
func TestCreatePatternWritesWholePattern(t *testing.T) {
	cases := []struct {
		stmt                string
		nodes, rels         int64
		statsNodes, statsRs int
	}{
		{"CREATE (a:T {id: 1})-[:R]->(b:T {id: 2}), (a)-[:S]->(c:T {id: 3})", 3, 2, 3, 2},
		{"CREATE (a:T {id: 1}), (b:T {id: 2 * 3}), (a)-[:R {w: 1 + 1}]->(b)", 2, 1, 2, 1},
		{"CREATE p = (:T {id: 1})-[:R]->(:T)<-[:S]-(:T) RETURN length(p) AS l", 3, 2, 3, 2},
		{"CREATE (a:T {id: 1}) CREATE (a)-[:R]->(b:T {id: a.id + 1})", 2, 1, 2, 1},
		{"UNWIND [1, 2] AS i CREATE (:T {id: i})-[:R]->(:T)", 4, 2, 4, 2},
		{"MATCH (u:U) CREATE (u)-[:R]->(:T {k: u.k})", 2, 1, 1, 1},
	}
	stacks := map[string]func(t *testing.T) *StorageExecutor{
		"memory": func(t *testing.T) *StorageExecutor {
			exec, _ := newTestExecutor(t)
			return exec
		},
		"server stack": newSetRouteServerStackExecutor,
	}
	for stack, build := range stacks {
		for _, tc := range cases {
			t.Run(stack+"/"+tc.stmt, func(t *testing.T) {
				exec := build(t)
				ctx := context.Background()
				if tc.stmt[:5] == "MATCH" {
					_, err := exec.Execute(ctx, "CREATE (:U {k: 5})", nil)
					require.NoError(t, err)
				}
				res, err := exec.Execute(ctx, tc.stmt, nil)
				require.NoError(t, err)
				assert.Equal(t, tc.statsNodes, res.Stats.NodesCreated)
				assert.Equal(t, tc.statsRs, res.Stats.RelationshipsCreated)
				counts, err := exec.Execute(ctx, "MATCH (n) WITH count(n) AS n OPTIONAL MATCH ()-[r]->() RETURN n, count(r) AS r", nil)
				require.NoError(t, err)
				assert.Equal(t, [][]interface{}{{tc.nodes, tc.rels}}, counts.Rows)
			})
		}
	}
}

// edgeRejectingEngine rejects every relationship write, as a relationship
// quota would, and is not transactional: the executor writes to it directly.
type edgeRejectingEngine struct {
	storage.Engine
}

var errEdgeRejected = errors.New("relationship rejected")

func (e *edgeRejectingEngine) CreateEdge(*storage.Edge) error        { return errEdgeRejected }
func (e *edgeRejectingEngine) BulkCreateEdges([]*storage.Edge) error { return errEdgeRejected }

// When the relationships of a CREATE are rejected after its nodes were
// written, the nodes are removed again: the statement writes nothing even on
// storage without a transaction.
func TestCreateRejectedRelationshipRemovesItsNodes(t *testing.T) {
	base := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	exec := NewStorageExecutor(&edgeRejectingEngine{Engine: base})
	ctx := context.Background()
	for _, stmt := range []string{
		"CREATE (:T {id: 1})-[:R]->(:T {id: 2})",
		"CREATE (a:T)-[:R]->(b:T), (b)-[:S]->(c:T)",
		"CREATE (a:T) CREATE (a)-[:R]->(:T)",
	} {
		_, err := exec.Execute(ctx, stmt, nil)
		require.ErrorIs(t, err, errEdgeRejected, stmt)
		count, err := base.NodeCount()
		require.NoError(t, err)
		assert.Zero(t, count, stmt)
	}
}
