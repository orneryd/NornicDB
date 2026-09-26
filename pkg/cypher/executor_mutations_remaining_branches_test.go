package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestExecuteSet_TrailingFallbackMatchProjection(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "set_trailing_fallback_cov")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE (:Person {id:'p1', name:'alice'})", nil)
	require.NoError(t, err)

	res, err := exec.executeSet(ctx, "MATCH (n:Person {id:'p1'}) SET n.flag = true MATCH (m:Person {id:'p1'}) RETURN m.flag AS flag")
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

	_, err = exec.executeSet(ctx, "MATCH (n:P) SET += {a:1}")
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid SET assignment")

	_, err = exec.executeSet(ctx, "MATCH (n:P) SET n += $")
	require.Error(t, err)
	require.Contains(t, err.Error(), "valid parameter name")

	_, err = exec.executeSet(ctx, "MATCH (n:P) SET n += $props")
	require.Error(t, err)
	require.Contains(t, err.Error(), "requires parameters")

	ctxParams := context.WithValue(ctx, paramsKey, map[string]interface{}{"other": map[string]interface{}{"x": int64(1)}})
	_, err = exec.executeSet(ctxParams, "MATCH (n:P) SET n += $props")
	require.Error(t, err)
	require.Contains(t, err.Error(), "not found")

	_, err = exec.executeSet(ctx, "MATCH (n:P) SET n += props")
	require.Error(t, err)
	require.Contains(t, err.Error(), "requires a map")

	_, err = exec.executeSet(ctx, "MATCH (n:P) WITH n, 1 AS props SET n += props")
	require.Error(t, err)
	require.Contains(t, err.Error(), "requires a map")

	// n is out of scope after WITH n AS p: SET n += ... must not fall back
	// to writing whatever node the row holds.
	_, err = exec.executeSet(ctx, "MATCH (n:P {id:'p1'}) WITH n AS p SET n += {score: 7} RETURN p.score AS score")
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
