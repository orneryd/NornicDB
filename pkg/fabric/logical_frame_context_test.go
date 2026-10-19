package fabric

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLogicalFrameContextPreservesScopeAndLifetime(t *testing.T) {
	type statementValueKey struct{}
	parent, cancel := context.WithCancel(context.WithValue(context.Background(), statementValueKey{}, 17))
	defer cancel()
	outer := map[string]interface{}{"x": 1, "g": 2, "removed": 9}
	parent = WithRecordBindings(parent, outer)
	tx := NewFabricTransaction("frame-lifetime")
	sub := &SubTransaction{ShardName: "physical"}
	parent = WithFabricTransaction(parent, tx)
	parent = WithSubTransaction(parent, sub)
	clause := &QueryClause{Scoped: true, ScopeNames: []string{"g"}}
	parent, _ = withCallScope(parent, clause, outer)
	frame := &LogicalFrame{Inputs: []string{"x"}, Globals: []string{"g"}}
	ctx := withLogicalFrame(parent, frame)
	bindings, present := RecordBindingsFromContext(ctx)
	require.True(t, present)
	require.Equal(t, map[string]interface{}{"x": 1, "g": 2}, bindings)
	require.Same(t, frame, LogicalFrameFromContext(ctx))
	require.Equal(t, CallScope{Clause: clause, Globals: map[string]interface{}{"g": 2}}, CallScopeFromContext(ctx))
	gotTx, _ := FabricTransactionFromContext(ctx)
	gotSub, _ := SubTransactionFromContext(ctx)
	require.Same(t, tx, gotTx)
	require.Same(t, sub, gotSub)
	require.Equal(t, 17, ctx.Value(statementValueKey{}))
	previous, _ := RecordBindingsFromContext(parent)
	require.Equal(t, outer, previous)

	empty := withLogicalFrame(ctx, &LogicalFrame{})
	bindings, present = RecordBindingsFromContext(empty)
	require.False(t, present)
	require.Empty(t, bindings)
	require.Empty(t, CallScopeFromContext(empty).Globals)
	rebound := WithRecordBindings(empty, map[string]interface{}{"local": 3})
	rebound = WithSubTransaction(rebound, nil)
	rebound = WithFabricTransaction(rebound, nil)
	bindings, present = RecordBindingsFromContext(rebound)
	require.True(t, present)
	require.Equal(t, map[string]interface{}{"local": 3}, bindings)
	require.Same(t, LogicalFrameFromContext(empty), LogicalFrameFromContext(rebound))
	require.Equal(t, CallScopeFromContext(empty), CallScopeFromContext(rebound))
	gotTx, present = FabricTransactionFromContext(rebound)
	require.False(t, present)
	require.Nil(t, gotTx)
	gotSub, present = SubTransactionFromContext(rebound)
	require.False(t, present)
	require.Nil(t, gotSub)
	gotTx, _ = FabricTransactionFromContext(empty)
	gotSub, _ = SubTransactionFromContext(empty)
	require.Same(t, tx, gotTx)
	require.Same(t, sub, gotSub)
	bindings, present = RecordBindingsFromContext(empty)
	require.False(t, present)
	require.Empty(t, bindings)
	require.Equal(t, 17, empty.Value(statementValueKey{}))
	cancel()
	require.ErrorIs(t, ctx.Err(), context.Canceled)
	require.ErrorIs(t, empty.Err(), context.Canceled)
	require.Equal(t, parent.Done(), empty.Done())
}
