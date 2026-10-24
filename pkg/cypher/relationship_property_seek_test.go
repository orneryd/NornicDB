package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// The server obtains storage through DatabaseManager, whose size-tracking
// wrapper must preserve the seek capability and the database's namespace.
func TestRelationshipPropertySeekDatabaseManager(t *testing.T) {
	base := storage.NewMemoryEngine()
	defer base.Close()
	manager, err := multidb.NewDatabaseManager(base, nil)
	require.NoError(t, err)
	defer manager.Close()
	for _, name := range []string{"seek_a", "seek_b"} {
		require.NoError(t, manager.CreateDatabase(name))
		store, err := manager.GetStorage(name)
		require.NoError(t, err)
		exec := NewStorageExecutor(store)
		_, err = exec.Execute(context.Background(), "CREATE ()-[:KEYED {_ci_key:'same', tenant:$tenant}]->()", map[string]interface{}{"tenant": name})
		require.NoError(t, err)
		_, err = exec.Execute(context.Background(), "CREATE RANGE INDEX rel_key FOR ()-[r:KEYED]-() ON (r._ci_key)", nil)
		require.NoError(t, err)
	}
	for _, name := range []string{"seek_a", "seek_b"} {
		store, err := manager.GetStorage(name)
		require.NoError(t, err)
		exec := NewStorageExecutor(store)
		result, err := exec.Execute(context.Background(), "PROFILE UNWIND ['same'] AS k MATCH ()-[r:KEYED]->() WHERE r._ci_key = k RETURN r.tenant", nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{name}}, result.Rows)
		require.Contains(t, planStringFromResult(t, result), "RelationshipIndexSeek")
	}
}

// The public query must select the relationship index, including a key bound
// by UNWIND. Correct rows alone did not catch the repeated type/graph scans.
func TestRelationshipPropertySeek(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	run := func(query string, params map[string]interface{}) *ExecuteResult {
		t.Helper()
		result, err := exec.Execute(context.Background(), query, params)
		require.NoError(t, err, query)
		return result
	}
	run("CREATE (:Probe)-[:KEYED {_ci_key:'a'}]->(:Probe)", nil)
	run("CREATE RANGE INDEX rel_key FOR ()-[r:KEYED]-() ON (r._ci_key)", nil)
	for _, query := range []string{
		"MATCH ()-[r:KEYED {_ci_key:'a'}]->() RETURN r._ci_key AS key",
		"MATCH ()-[r:KEYED]->() WHERE r._ci_key = 'a' RETURN r._ci_key AS key",
		"UNWIND $keys AS k MATCH ()-[r:KEYED {_ci_key:k}]->() RETURN r._ci_key AS key",
		"UNWIND $keys AS k MATCH ()-[r:KEYED]->() WHERE r._ci_key = k RETURN r._ci_key AS key",
		"UNWIND $keys AS k MATCH ()-[r:KEYED]->() WHERE r._ci_key = toString(k) RETURN r._ci_key AS key",
	} {
		params := map[string]interface{}{"keys": []interface{}{"a", "missing", "a"}}
		profile := run("PROFILE "+query, params)
		want := [][]interface{}{{"a"}}
		if query[0] == 'U' {
			want = append(want, []interface{}{"a"})
		}
		require.Equal(t, want, profile.Rows)
		require.Contains(t, planStringFromResult(t, profile), "RelationshipIndexSeek", query)
		explain := run("EXPLAIN "+query, params)
		require.Empty(t, explain.Rows)
		require.Contains(t, planStringFromResult(t, explain), "RelationshipIndexSeek", query)
	}
}

func TestRelationshipPropertySeekProfileAccounting(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	for _, query := range []string{
		"CREATE (:Probe)-[:KEYED {_ci_key:'a'}]->(:Probe)",
		"CREATE RANGE INDEX rel_key FOR ()-[r:KEYED]-() ON (r._ci_key)",
	} {
		_, err := exec.Execute(context.Background(), query, nil)
		require.NoError(t, err)
	}
	result, err := exec.Execute(context.Background(), "PROFILE UNWIND ['a','missing','a'] AS k MATCH ()-[r:KEYED {_ci_key:k}]->() RETURN count(r)", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(2)}}, result.Rows)
	plan, ok := result.Metadata["plan"].(*ExecutionPlan)
	if !ok {
		t.Fatalf("unexpected plan metadata: %T", result.Metadata["plan"])
	}
	var find func(*PlanOperator) *PlanOperator
	find = func(op *PlanOperator) *PlanOperator {
		if op.OperatorType == "RelationshipIndexSeek" {
			return op
		}
		for _, child := range op.Children {
			if found := find(child); found != nil {
				return found
			}
		}
		return nil
	}
	seek := find(plan.Root)
	require.NotNil(t, seek)
	require.Equal(t, int64(2), seek.ActualRows)
	require.Equal(t, int64(3), seek.Arguments["indexLookups"])
	require.Equal(t, int64(2), seek.Arguments["relationshipCandidates"])
	require.Empty(t, seek.Children, "a seek must not claim a graph scan ran")
	require.Equal(t, plan.TotalDBHits, plan.Root.DBHits, "ancestors must lose the replaced scan's cumulative work too")
}

func TestRelationshipPropertySeekExpressionPlans(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	for _, query := range []string{
		"CREATE (:Probe)-[:KEYED {_ci_key:1}]->(:Probe)",
		"CREATE RANGE INDEX rel_key FOR ()-[r:KEYED]-() ON (r._ci_key)",
	} {
		_, err := exec.Execute(context.Background(), query, nil)
		require.NoError(t, err)
	}
	for _, test := range []struct {
		expression string
		key        interface{}
	}{
		{"toInteger(k)", int64(1)},
		{"size(k)", []interface{}{int64(1)}},
		{"k.value", map[string]interface{}{"value": int64(1)}},
	} {
		query := "UNWIND $keys AS k MATCH ()-[r:KEYED]->() WHERE r._ci_key=" + test.expression + " RETURN r._ci_key"
		params := map[string]interface{}{"keys": []interface{}{test.key}}
		for _, mode := range []string{"EXPLAIN", "PROFILE"} {
			result, err := exec.Execute(context.Background(), mode+" "+query, params)
			require.NoError(t, err)
			require.Contains(t, planStringFromResult(t, result), "RelationshipIndexSeek", query)
			if mode == "PROFILE" {
				require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
			}
		}
	}
}

func TestRelationshipPropertySeekExactVariableOwnership(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	for _, query := range []string{
		"CREATE (:Start {key:'x'})-[:KEYED {key:'different'}]->(:End)",
		"CREATE RANGE INDEX rel_key FOR ()-[r:KEYED]-() ON (r.key)",
	} {
		_, err := exec.Execute(context.Background(), query, nil)
		require.NoError(t, err)
	}
	result, err := exec.Execute(context.Background(), "MATCH (R)-[r:KEYED]->(b) WHERE R.key='x' RETURN r.key", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"different"}}, result.Rows)
}

func TestRelationshipPropertySeekEquivalentRows(t *testing.T) {
	base := newTestMemoryEngine(t)
	stores := []*storage.NamespacedEngine{storage.NewNamespacedEngine(base, "indexed"), storage.NewNamespacedEngine(base, "scanned")}
	executors := []*StorageExecutor{NewStorageExecutor(stores[0]), NewStorageExecutor(stores[1])}
	run := func(exec *StorageExecutor, query string, params map[string]interface{}) *ExecuteResult {
		t.Helper()
		result, err := exec.Execute(context.Background(), query, params)
		require.NoError(t, err, query)
		return result
	}
	for _, exec := range executors {
		run(exec, `CREATE (a:Left {name:'a'}), (b:Right {name:'b'}), (c:Left:Right {name:'c'}),
		(a)-[:KEYED {_ci_key:'x',n:1,part:'p',active:true}]->(b),
		(a)-[:KEYED {_ci_key:'x',n:2,part:'q',active:false}]->(c),
		(c)-[:KEYED {_ci_key:'loop',n:3,part:'p'}]->(c),
		(b)-[:OTHER {_ci_key:'x'}]->(a),
		(b)-[:KEYED {_ci_key:'list',n:[1,2]}]->(c)`, nil)
	}
	run(executors[0], "CREATE RANGE INDEX rel_key FOR ()-[r:KEYED]-() ON (r._ci_key)", nil)
	run(executors[0], "CREATE RANGE INDEX rel_number FOR ()-[r:KEYED]-() ON (r.n)", nil)
	run(executors[0], "CREATE RANGE INDEX rel_composite FOR ()-[r:KEYED]-() ON (r._ci_key,r.part)", nil)
	queries := []string{
		"MATCH (a)-[r:KEYED {_ci_key:$key}]->(b) RETURN a.name,r._ci_key,b.name ORDER BY a.name,r._ci_key,b.name",
		"MATCH (a:Left)-[r:KEYED]->(b:Right) WHERE r._ci_key=$key AND r.active=true RETURN a.name,b.name ORDER BY a.name,b.name",
		"MATCH (a)<-[r:KEYED {_ci_key:$key}]-(b) RETURN a.name,b.name ORDER BY a.name,b.name",
		"MATCH (a)-[r:KEYED {_ci_key:$key}]-(b) RETURN a.name,b.name ORDER BY a.name,b.name",
		"MATCH (a)-[r:KEYED {_ci_key:$key}]->(a) RETURN a.name ORDER BY a.name",
		"MATCH (a)-[r:KEYED {_ci_key:$key}]->(b) WHERE r._ci_key='contradiction' RETURN a.name",
		"MATCH ()-[r:KEYED]->() WHERE r._ci_key=$key OR r._ci_key='loop' RETURN r._ci_key ORDER BY r._ci_key",
		"MATCH ()-[r:KEYED {_ci_key:$key,part:'p'}]->() RETURN r.n ORDER BY r.n",
		"UNWIND [$key,$key,'missing'] AS k MATCH ()-[r:KEYED]->() WHERE k=r._ci_key RETURN r._ci_key ORDER BY r._ci_key",
		"MATCH (a:Left {name:'a'}) WITH a MATCH (a)-[r:KEYED {_ci_key:$key}]->(b) RETURN b.name ORDER BY b.name",
		"MATCH (b:Right {name:'b'}) WITH b MATCH (a)-[r:KEYED {_ci_key:$key}]->(b) RETURN a.name ORDER BY a.name",
		"MATCH ()-[r:KEYED]->() WHERE r.n=$number RETURN r._ci_key ORDER BY r._ci_key",
	}
	compare := func() {
		t.Helper()
		for _, key := range []interface{}{"x", "loop", "new", "peer", "missing", nil} {
			for _, number := range []interface{}{int64(1), float64(1), []interface{}{int64(1), int64(2)}} {
				for _, query := range queries {
					params := map[string]interface{}{"key": key, "number": number}
					indexed := run(executors[0], query, params)
					scanned := run(executors[1], query, params)
					require.Equal(t, scanned.Rows, indexed.Rows, "%s params=%v", query, params)
				}
			}
		}
	}
	compare()
	for _, exec := range executors {
		run(exec, "MATCH ()-[r:KEYED {_ci_key:'x'}]->() SET r._ci_key='new',r.part='updated'", nil)
		run(exec, "MATCH ()-[r:KEYED {_ci_key:'loop'}]->() DELETE r", nil)
	}
	compare()
	for _, exec := range executors {
		run(exec, "BEGIN", nil)
	}
	for _, exec := range executors {
		run(exec, "CREATE (:Left {name:'pending'})-[:KEYED {_ci_key:'x',n:1}]->(:Right {name:'pending-end'})", nil)
		run(exec, "MATCH ()-[r:KEYED {_ci_key:'new'}]->() SET r._ci_key='x'", nil)
		run(exec, "MATCH ()-[r:KEYED {_ci_key:'list'}]->() DELETE r", nil)
	}
	compare() // Own creates, updates, and deletes must be visible before COMMIT.
	for _, exec := range executors {
		run(exec, "ROLLBACK", nil)
	}
	compare()
	for _, exec := range executors {
		run(exec, "BEGIN", nil)
	}
	// Change the committed key before either transaction first seeks it. The
	// equality owner must build the pinned view rather than trust today's index.
	for _, store := range stores {
		peer := NewStorageExecutor(store)
		run(peer, "MATCH ()-[r:KEYED {_ci_key:'new'}]->() SET r._ci_key='peer'", nil)
	}
	compare()
	for _, exec := range executors {
		run(exec, "ROLLBACK", nil)
	}
	compare()
	for _, exec := range executors {
		run(exec, "BEGIN", nil)
		run(exec, "MATCH ()-[r:KEYED {_ci_key:'peer'}]->() SET r._ci_key='x'", nil)
		run(exec, "CREATE (:Left {name:'committed'})-[:KEYED {_ci_key:'new',n:1}]->(:Right {name:'committed-end'})", nil)
		run(exec, "MATCH ()-[r:KEYED {_ci_key:'list'}]->() DELETE r", nil)
	}
	compare()
	for _, exec := range executors {
		run(exec, "COMMIT", nil)
	}
	compare()
	for _, exec := range executors {
		run(exec, "MATCH ()-[r:KEYED {_ci_key:'new'}]->() REMOVE r._ci_key", nil)
	}
	compare()
	// Schema lifecycle: a dropped index must stop being selected and recreating
	// it after writes must include those writes.
	run(executors[0], "DROP INDEX rel_key", nil)
	compare()
	run(executors[0], "CREATE RANGE INDEX rel_key FOR ()-[r:KEYED]-() ON (r._ci_key)", nil)
	compare()
}

func TestRelationshipPropertySeekConstraintAndNamespace(t *testing.T) {
	base := newTestMemoryEngine(t)
	for _, namespace := range []string{"one", "two"} {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(base, namespace))
		for _, query := range []string{
			"CREATE CONSTRAINT rel_unique FOR ()-[r:KEYED]-() REQUIRE r._ci_key IS UNIQUE",
			"CREATE (:Probe {db:'" + namespace + "'})-[:KEYED {_ci_key:'shared'}]->(:Probe)",
		} {
			_, err := exec.Execute(context.Background(), query, nil)
			require.NoError(t, err)
		}
		result, err := exec.Execute(context.Background(), "PROFILE MATCH (a)-[r:KEYED {_ci_key:'shared'}]->() RETURN a.db", nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{namespace}}, result.Rows)
		require.Contains(t, planStringFromResult(t, result), "RelationshipIndexSeek")
		_, err = exec.Execute(context.Background(), "CREATE (:Probe)-[:KEYED {_ci_key:'shared'}]->(:Probe)", nil)
		require.Error(t, err, "seek must preserve uniqueness validation")
	}
}
