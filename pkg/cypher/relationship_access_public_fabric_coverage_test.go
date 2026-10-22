package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestRelationshipAccessPublicFabricCoverage(t *testing.T) {
	exec := newCompositeFixture(t)
	exec.cache = nil
	for _, item := range []struct{ graph, origin, index string }{
		{"pcomp.other", "left", "coverage_left"},
		{"pcomp.third", "right", "coverage_right"},
	} {
		_, err := exec.Execute(context.Background(), "USE "+item.graph+" CREATE ()-[:COVERAGE_SHARED {key:'hit',origin:$origin}]->()", map[string]interface{}{"origin": item.origin})
		require.NoError(t, err)
		_, err = exec.Execute(context.Background(), "USE "+item.graph+" CREATE RANGE INDEX "+item.index+" FOR ()-[r:COVERAGE_SHARED]-() ON (r.key)", nil)
		require.NoError(t, err)
	}
	const left = "USE pcomp.other MATCH ()-[r:COVERAGE_SHARED]->() WHERE r.key=$key RETURN r.origin AS value"
	const right = "USE pcomp.third MATCH ()-[r:COVERAGE_SHARED]->() WHERE r.key=$key RETURN r.origin AS value"
	for _, tc := range []struct {
		name, query string
		dynamic     bool
		want        [][]interface{}
	}{
		{"static", left, false, [][]interface{}{{"left"}}},
		{"call", "CALL { " + left + " } RETURN value", false, [][]interface{}{{"left"}}},
		{"nested", "CALL { CALL { " + left + " } RETURN value } RETURN value", false, [][]interface{}{{"left"}}},
		{"union", "CALL { " + left + " UNION ALL " + right + " } RETURN value", false, [][]interface{}{{"left"}, {"right"}}},
		{"leaf_projection", "CALL { " + left + " } WITH value RETURN value", false, [][]interface{}{{"left"}}},
		{"apply", "UNWIND ['hit'] AS key CALL (key) { USE pcomp.other MATCH ()-[r:COVERAGE_SHARED]->() WHERE r.key=key RETURN r.origin AS value } RETURN value", false, [][]interface{}{{"left"}}},
		{"dynamic", "CALL { USE graph.byName($graph) MATCH ()-[r:COVERAGE_SHARED]->() WHERE r.key=$key RETURN r.origin AS value } RETURN value", true, [][]interface{}{{"right"}}},
	} {
		for _, mode := range []string{"", "EXPLAIN ", "PROFILE "} {
			t.Run(tc.name+"/"+mode, func(t *testing.T) {
				result, err := exec.Execute(context.Background(), mode+tc.query, map[string]interface{}{"key": "hit", "graph": "pcomp.third"})
				require.NoError(t, err, mode+tc.query)
				if mode == "EXPLAIN " {
					require.Empty(t, result.Rows)
					if tc.dynamic {
						require.Contains(t, planStringFromResult(t, result), "DynamicGraphAccess")
					} else {
						require.Contains(t, planStringFromResult(t, result), "RelationshipIndexSeek")
					}
				} else {
					require.ElementsMatch(t, tc.want, result.Rows)
				}
			})
		}
	}
	for _, query := range []string{
		"CALL { USE pcomp.missing MATCH ()-[r:COVERAGE_SHARED]->() RETURN r.origin AS value } RETURN value",
		"CALL { USE graph.byName($graph) MATCH ()-[r:COVERAGE_SHARED]->() RETURN r.origin AS value } RETURN value",
	} {
		_, err := exec.Execute(context.Background(), query, map[string]interface{}{"graph": "pcomp.missing"})
		require.Error(t, err, query)
	}
}

// Reads of a published equality index and its transaction overlay must agree
// before and after a key moves. Every mutation and observation is public Cypher.
func TestRelationshipAccessPublicSeekMutationCoverage(t *testing.T) {
	engine, err := storage.NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	exec := NewStorageExecutor(storage.NewNamespacedEngine(engine, "public_seek_mutation_coverage"))
	exec.cache = nil
	for _, query := range []string{
		"CREATE RANGE INDEX coverage_key_pair FOR ()-[r:COVERAGE_MUTATION]-() ON (r.key,r.version)",
		"CREATE ()-[:COVERAGE_MUTATION {key:'old',version:1}]->(), ()-[:COVERAGE_MUTATION {key:'other',version:1}]->()",
	} {
		_, err := exec.Execute(context.Background(), query, nil)
		require.NoError(t, err)
	}
	assertKey := func(key string, want int64) {
		result, err := exec.Execute(context.Background(), "PROFILE MATCH ()-[r:COVERAGE_MUTATION]->() WHERE r.key=$key AND r.version=1 RETURN count(r)", map[string]interface{}{"key": key})
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{want}}, result.Rows)
	}
	assertKey("old", 1)
	for _, query := range []string{
		"BEGIN",
		"MATCH ()-[r:COVERAGE_MUTATION]->() WHERE r.key='old' SET r.key='new'",
		"CREATE ()-[:COVERAGE_MUTATION {key:'new',version:1}]->(), ()-[:COVERAGE_MUTATION {key:'new'}]->(), ()-[:OTHER {key:'new',version:1}]->()",
		"MATCH ()-[r:COVERAGE_MUTATION]->() WHERE r.key='other' DELETE r",
	} {
		_, err := exec.Execute(context.Background(), query, nil)
		require.NoError(t, err)
	}
	assertKey("old", 0)
	assertKey("new", 2)
	assertKey("other", 0)
	_, err = exec.Execute(context.Background(), "ROLLBACK", nil)
	require.NoError(t, err)
	assertKey("old", 1)
	assertKey("new", 0)
	assertKey("other", 1)
	_, err = exec.Execute(context.Background(), "MATCH ()-[r:COVERAGE_MUTATION]->() WHERE r.key='old' SET r.key='new'", nil)
	require.NoError(t, err)
	assertKey("old", 0)
	assertKey("new", 1)
}
