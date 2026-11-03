package storage

import (
	"context"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// TestBadgerConstraintValidationBindsTheWritesOwnLifetime pins the private
// before-durability validation contract of every migrated owner: constraint
// discovery runs on the caller's context, so a canceled write context returns
// that context's own error instead of an integrity outcome. The healthy control
// of each call still reports the same integrity result on an uncanceled
// context, so the migration did not change what the walk decides.
func TestBadgerConstraintValidationBindsTheWritesOwnLifetime(t *testing.T) {
	engine := newTestEngine(t)
	schema := engine.GetSchemaForNamespace("test")
	require.NoError(t, schema.AddConstraint(Constraint{
		Name: "u_email", Type: ConstraintUnique, Label: "User", Properties: []string{"email"},
	}))

	_, err := engine.CreateNode(t.Context(), &Node{ID: "test:u1", Labels: []string{"User"}, Properties: map[string]any{"email": "taken@example.com"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(t.Context(), &Node{ID: "test:secret", Labels: []string{"Secret"}})
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(t.Context(), &Edge{ID: "test:blocked", StartNode: "test:u1", EndNode: "test:secret", Type: "LINK"}))

	// Registered after the edge exists, the policy is enforced by the walk below
	// rather than by the write that created the edge.
	require.NoError(t, schema.AddConstraint(Constraint{
		Name: "rel_policy", Type: ConstraintPolicy, EntityType: ConstraintEntityRelationship,
		Label: "LINK", SourceLabel: "User", TargetLabel: "Secret", PolicyMode: "DISALLOWED",
	}))

	// Every healthy control below breaks a constraint, so an uncanceled context
	// still produces the violation it produced before the context moved in.
	duplicateEmail := &Node{ID: "test:u2", Labels: []string{"User"}, Properties: map[string]any{"email": "taken@example.com"}}
	forbiddenEdge := &Edge{ID: "test:forbidden", StartNode: "test:u1", EndNode: "test:secret", Type: "LINK"}
	relabelled := &Node{ID: "test:u1", Labels: []string{"User", "Extra"}}
	stored := &Node{ID: "test:u1", Labels: []string{"User"}}

	cases := []struct {
		name    string
		call    func(ctx context.Context, txn *badger.Txn) error
		healthy func(t *testing.T, err error)
	}{
		{
			name: "node constraints",
			call: func(ctx context.Context, txn *badger.Txn) error {
				return engine.validateNodeConstraintsInTxn(ctx, txn, duplicateEmail, schema, "test", "")
			},
			healthy: func(t *testing.T, err error) {
				var violation *ConstraintViolationError
				require.ErrorAs(t, err, &violation)
				require.Equal(t, ConstraintUnique, violation.Type)
			},
		},
		{
			name: "edge constraints",
			call: func(ctx context.Context, txn *badger.Txn) error {
				return engine.validateEdgeConstraintsInTxn(ctx, txn, forbiddenEdge, schema, "test", "")
			},
			healthy: requirePolicyViolation,
		},
		{
			name: "edge policy",
			call: func(ctx context.Context, txn *badger.Txn) error {
				return engine.checkEdgePolicyInTxn(ctx, txn, forbiddenEdge, schema, "test")
			},
			healthy: requirePolicyViolation,
		},
		{
			name: "label change policy",
			call: func(ctx context.Context, txn *badger.Txn) error {
				return engine.validatePolicyOnNodeLabelChangeInTxn(ctx, txn, relabelled, stored, schema, "test")
			},
			healthy: requirePolicyViolation,
		},
		{
			name: "adjacent edge policy",
			call: func(ctx context.Context, txn *badger.Txn) error {
				return engine.validatePolicyForAdjacentEdgesInTxn(ctx, txn, relabelled, schema, "test")
			},
			healthy: requirePolicyViolation,
		},
		{
			name: "edges with prefix policy",
			call: func(ctx context.Context, txn *badger.Txn) error {
				prefix := engine.outgoingIndexPrefixString("test:u1")
				require.NotNil(t, prefix)
				return engine.validatePolicyForEdgesWithPrefixInTxn(ctx, txn, prefix, relabelled, true, schema, "test")
			},
			healthy: requirePolicyViolation,
		},
	}

	require.NoError(t, engine.withView(func(txn *badger.Txn) error {
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				err := tc.call(canceledWriteContext(t), txn)
				require.ErrorIs(t, err, context.Canceled,
					"a canceled write context must stop the walk before it decides anything")
				require.Equal(t, context.Canceled, err,
					"the walk returns the canceled context's own error, not a mapped outcome")

				tc.healthy(t, tc.call(t.Context(), txn))
			})
		}

		require.NoError(t, engine.validateNodeConstraintsInTxn(t.Context(), txn,
			&Node{ID: "test:u3", Labels: []string{"User"}, Properties: map[string]any{"email": "free@example.com"}},
			schema, "test", ""),
			"the healthy integrity control of a satisfied node is unchanged")
		return nil
	}))
}

func requirePolicyViolation(t *testing.T, err error) {
	t.Helper()
	var violation *ConstraintViolationError
	require.ErrorAs(t, err, &violation)
	require.Equal(t, ConstraintPolicy, violation.Type)
}

func canceledWriteContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}
