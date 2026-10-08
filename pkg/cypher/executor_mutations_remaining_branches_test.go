package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestSetTrailingProjectionCanonicalRowSet(t *testing.T) {
	exec, _ := newTestExecutor(t)
	ctx := context.WithValue(context.Background(), paramsKey, map[string]interface{}{"offset": int64(10), "skip": int64(1)})
	node := &storage.Node{ID: "n", Properties: map[string]interface{}{"v": int64(2)}}
	input := &ExecuteResult{Columns: []string{"n", "value"}, Rows: [][]interface{}{{node, int64(3)}, {node, int64(1)}, {node, int64(3)}}}
	stats := &QueryStats{PropertiesSet: 3}
	for _, test := range []struct {
		clause  string
		columns []string
		rows    [][]interface{}
	}{
		{"WITH DISTINCT value AS x ORDER BY x DESC SKIP $skip LIMIT 1 RETURN x + $offset AS total", []string{"total"}, [][]interface{}{{int64(11)}}},
		{"WITH n, count(*) AS c RETURN n.v + c AS total, c", []string{"total", "c"}, [][]interface{}{{int64(5), int64(3)}}},
		{"WITH value AS x RETURN DISTINCT x ORDER BY x DESC SKIP 1 LIMIT 1", []string{"x"}, [][]interface{}{{int64(1)}}},
		{"WITH * RETURN value ORDER BY n.v, value DESC LIMIT 1", []string{"value"}, [][]interface{}{{int64(3)}}},
	} {
		t.Run(test.clause, func(t *testing.T) {
			result, handled, err := exec.sharedTrailingRowsHandledForTest(ctx, test.clause, input, &ExecuteResult{Stats: stats})
			require.NoError(t, err)
			require.True(t, handled)
			require.Equal(t, test.columns, result.Columns)
			require.Equal(t, test.rows, result.Rows)
			require.Same(t, stats, result.Stats)
		})
	}
}

func TestExecuteSet_TrailingFallbackMatchProjection(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "set_trailing_fallback_cov")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE (:Person {id:'p1', name:'alice'})", nil)
	require.NoError(t, err)

	res, err := exec.Execute(ctx, "MATCH (n:Person {id:'p1'}) SET n.flag = true WITH n MATCH (m:Person {id:'p1'}) RETURN m.flag AS flag", getParamsFromContext(ctx))
	require.NoError(t, err)
	require.Equal(t, []string{"flag"}, res.Columns)
	require.Len(t, res.Rows, 1)
	require.Equal(t, true, res.Rows[0][0])
}

func TestCountSubqueryMatches_NoDirectionCountsBoth(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "count_both_dirs_cov")
	exec := NewStorageExecutor(store)

	_, err := store.CreateNode(&storage.Node{ID: "a", Labels: []string{"N"}})
	require.NoError(t, err)
	_, err = store.CreateNode(&storage.Node{ID: "b", Labels: []string{"N"}})
	require.NoError(t, err)
	_, err = store.CreateNode(&storage.Node{ID: "c", Labels: []string{"N"}})
	require.NoError(t, err)
	require.NoError(t, store.CreateEdge(&storage.Edge{ID: "e1", Type: "R", StartNode: "a", EndNode: "b"}))
	require.NoError(t, store.CreateEdge(&storage.Edge{ID: "e2", Type: "R", StartNode: "c", EndNode: "a"}))

	nodeA, err := store.GetNode("a")
	require.NoError(t, err)
	require.NotNil(t, nodeA)

	count := subqueryCount(t, exec, nodeA, "n", "MATCH (n)--(x)")
	require.EqualValues(t, 2, count)
}

func TestPolicyHelpers_AdditionalBranches(t *testing.T) {
	allowed := []storage.Constraint{
		{
			Name:        "allow_rel",
			Type:        storage.ConstraintPolicy,
			Label:       "REL",
			SourceLabel: "A",
			TargetLabel: "B",
			PolicyMode:  "ALLOWED",
		},
	}

	require.NoError(t, checkPolicyForEdge("REL", []string{"A"}, []string{"B"}, allowed))
	err := checkPolicyForEdge("REL", []string{"A"}, []string{"C"}, allowed)
	require.Error(t, err)
	require.Contains(t, err.Error(), "no ALLOWED policy permits edge")

	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "policy_label_change_empty_cov")
	node := &storage.Node{ID: "n1", Labels: []string{"User"}, Properties: map[string]interface{}{}}
	_, err = store.CreateNode(node)
	require.NoError(t, err)

	// No policy constraints in schema: label change validation should be a no-op.
	require.NoError(t, validatePolicyOnLabelChange(store, node, []string{"OldUser"}))
}

func TestExecuteSet_MergeAssignmentErrorAndFallbackBranches(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "set_merge_exec_cov")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE (:P {id:'p1'})", nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, "MATCH (n:P) SET += {a:1}", getParamsFromContext(ctx))
	requireSyntaxErrorStatus(t, err, "MATCH (n:P) SET += {a:1}")

	_, err = exec.Execute(ctx, "MATCH (n:P) SET n += $", getParamsFromContext(ctx))
	requireSyntaxErrorStatus(t, err, "MATCH (n:P) SET n += $")

	_, err = exec.Execute(ctx, "MATCH (n:P) SET n += $props", getParamsFromContext(ctx))
	require.Error(t, err)
	require.Contains(t, err.Error(), "Neo.ClientError.Statement.ParameterMissing")

	ctxParams := context.WithValue(ctx, paramsKey, map[string]interface{}{"other": map[string]interface{}{"x": int64(1)}})
	_, err = exec.Execute(ctxParams, "MATCH (n:P) SET n += $props", getParamsFromContext(ctxParams))
	require.Error(t, err)
	require.Contains(t, err.Error(), "Neo.ClientError.Statement.ParameterMissing")

	_, err = exec.Execute(ctx, "MATCH (n:P) SET n += props", getParamsFromContext(ctx))
	require.Error(t, err)
	require.Contains(t, err.Error(), "Neo.ClientError.Statement.SyntaxError")

	// The source's type is known before the statement runs (Neo4j 5.26.30).
	_, err = exec.Execute(ctx, "MATCH (n:P) WITH n, 1 AS props SET n += props", getParamsFromContext(ctx))
	require.Error(t, err)
	require.Contains(t, err.Error(), "Type mismatch: expected Map, Node or Relationship but was Integer")

	// n is out of scope after WITH n AS p: SET n += ... must not fall back
	// to writing whatever node the row holds.
	_, err = exec.Execute(ctx, "MATCH (n:P {id:'p1'}) WITH n AS p SET n += {score: 7} RETURN p.score AS score", getParamsFromContext(ctx))
	require.Error(t, err)
	res, err := exec.Execute(ctx, "MATCH (n:P {id:'p1'}) RETURN n.score AS score", nil)
	require.NoError(t, err)
	require.Nil(t, res.Rows[0][0])
}

func TestValidatePolicyOnLabelChange_OutgoingAndIncomingViolations(t *testing.T) {
	t.Run("outgoing disallowed violation", func(t *testing.T) {
		base := newTestMemoryEngine(t)
		store := storage.NewNamespacedEngine(base, "policy_outgoing_cov")

		src := &storage.Node{ID: "u1", Labels: []string{"User"}, Properties: map[string]interface{}{}}
		dst := &storage.Node{ID: "d1", Labels: []string{"Doc"}, Properties: map[string]interface{}{}}
		_, err := store.CreateNode(src)
		require.NoError(t, err)
		_, err = store.CreateNode(dst)
		require.NoError(t, err)
		require.NoError(t, store.CreateEdge(&storage.Edge{ID: "e1", Type: "CAN_EDIT", StartNode: "u1", EndNode: "d1"}))

		err = store.GetSchema().AddConstraint(storage.Constraint{
			Name:        "policy_disallow_user_doc",
			Type:        storage.ConstraintPolicy,
			Label:       "CAN_EDIT",
			SourceLabel: "User",
			TargetLabel: "Doc",
			PolicyMode:  "DISALLOWED",
		}, false)
		require.NoError(t, err)

		err = validatePolicyOnLabelChange(store, src, []string{"User"})
		require.Error(t, err)
		require.Contains(t, err.Error(), "DISALLOWED")
	})

	t.Run("incoming disallowed violation", func(t *testing.T) {
		base := newTestMemoryEngine(t)
		store := storage.NewNamespacedEngine(base, "policy_incoming_cov")

		src := &storage.Node{ID: "t1", Labels: []string{"Team"}, Properties: map[string]interface{}{}}
		dst := &storage.Node{ID: "u1", Labels: []string{"User"}, Properties: map[string]interface{}{}}
		_, err := store.CreateNode(src)
		require.NoError(t, err)
		_, err = store.CreateNode(dst)
		require.NoError(t, err)
		require.NoError(t, store.CreateEdge(&storage.Edge{ID: "e1", Type: "CAN_ASSIGN", StartNode: "t1", EndNode: "u1"}))

		err = store.GetSchema().AddConstraint(storage.Constraint{
			Name:        "policy_disallow_team_user",
			Type:        storage.ConstraintPolicy,
			Label:       "CAN_ASSIGN",
			SourceLabel: "Team",
			TargetLabel: "User",
			PolicyMode:  "DISALLOWED",
		}, false)
		require.NoError(t, err)

		err = validatePolicyOnLabelChange(store, dst, []string{"User"})
		require.Error(t, err)
		require.Contains(t, err.Error(), "DISALLOWED")
	})
}
