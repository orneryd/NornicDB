package storage

import (
	"context"
	"errors"
	"github.com/stretchr/testify/require"
	"reflect"
	"testing"
)

type foundationOwnershipBoundary struct {
	Engine
	binding MembershipBinding
}

func (e *foundationOwnershipBoundary) StorageMembership() MembershipBinding { return e.binding }

type foundationOwnerDecorator struct {
	Engine
	walks *int
}

func (e *foundationOwnerDecorator) CapabilityOwnerSource(OwnerPolicy) (Engine, bool) {
	*e.walks++
	return e.Engine, true
}
func (e *foundationOwnerDecorator) ReadCapabilitySource() (Engine, bool) { return e.Engine, true }

type foundationReadOnlyDecorator struct{ Engine }

func (e *foundationReadOnlyDecorator) ReadCapabilitySource() (Engine, bool) { return e.Engine, true }
func (e *foundationReadOnlyDecorator) GetInnerEngine() Engine               { return e.Engine }

func TestFoundationMembershipPolicies(t *testing.T) {
	token := new(int)
	inner := &foundationOwnershipBoundary{binding: MembershipBinding{Status: MembershipBound, Owner: token, Adapter: token, Controller: token, PendingActions: token, SchemaPublication: token, ReadLease: token, StatementMode: token, Enforcement: token}}
	for _, status := range []MembershipStatus{MembershipBound, MembershipUnbound, MembershipEnded} {
		t.Run(reflect.ValueOf(status).String(), func(t *testing.T) {
			outer := &foundationOwnershipBoundary{Engine: inner, binding: MembershipBinding{Status: status, Owner: token}}
			walks := 0
			var engine Engine = outer
			for i := 0; i < 4; i++ {
				engine = &foundationOwnerDecorator{engine, &walks}
			}
			handle := BindMembership(engine)
			require.Equal(t, 4, walks)
			for i := 0; i < 1000; i++ {
				b := handle.Current()
				require.Equal(t, status, b.Status)
				require.Same(t, outer, b.Boundary)
				require.Same(t, token, b.Owner)
			}
			require.Equal(t, 4, walks, "bound status reads cannot rediscover topology")
			outer.binding.Status = MembershipEnded
			require.Equal(t, MembershipEnded, handle.Current().Status, "status must stay live")
			_, exact := OwnedCapability[*foundationOwnershipBoundary](engine)
			require.False(t, exact)
		})
	}
	b := MembershipForEngine(inner)
	for _, field := range []any{b.Adapter, b.Controller, b.PendingActions, b.SchemaPublication, b.ReadLease, b.StatementMode, b.Enforcement} {
		require.Same(t, token, field)
	}
	opaque := &foundationReadOnlyDecorator{inner}
	require.Equal(t, MembershipUnowned, MembershipForEngine(opaque).Status, "read/physical transparency cannot invent an ownership edge")
	// Negative control: retaining all read methods but dropping the explicit
	// membership edge must be detected by the independent token oracle.
	require.NotEqual(t, token, MembershipForEngine(opaque).Owner)
	require.Equal(t, MembershipUnbound, MembershipForEngine(&foundationOwnershipBoundary{Engine: inner}).Status, "empty first boundary cannot fall through")
}

type foundationCommitter struct {
	Engine
	calls    int
	received *BadgerTransaction
}

func (e *foundationCommitter) CommitTransaction(_ context.Context, tx *BadgerTransaction) error {
	e.calls++
	e.received = tx
	return nil
}
func TestFoundationCommitOwnerAndExactMutations(t *testing.T) {
	managed := &foundationCommitter{}
	walks := 0
	engine := &foundationOwnerDecorator{managed, &walks}
	committer, ok := CommitOwnerForEngine(engine)
	require.True(t, ok)
	tx := &BadgerTransaction{}
	require.NoError(t, committer.CommitTransaction(context.Background(), tx))
	require.Equal(t, 1, managed.calls)
	require.Same(t, tx, managed.received)
	_, ok = CommitOwnerForEngine(&foundationReadOnlyDecorator{managed})
	require.False(t, ok, "undeclared physical/read unwrap must not authorize commit")
	observed := &foundationObservedWriter{err: ErrNotImplemented}
	before, after := &Node{ID: "before"}, &Node{ID: "after"}
	require.ErrorIs(t, UpdateObservedNode(observed, before, after), ErrNotImplemented)
	require.Equal(t, 1, observed.observed)
	require.Zero(t, observed.ordinary)
	require.Same(t, before, observed.before)
	decorator := &foundationMutationDecorator{Engine: observed}
	require.NoError(t, UpdateObservedNode(decorator, before, after))
	require.Equal(t, 1, decorator.ordinary)
	require.Equal(t, 1, observed.observed, "exact mutation cannot unwrap to an observed child")
	edgeWriter := &foundationObservedEdgeWriter{}
	oldEdge, newEdge := &Edge{}, &Edge{}
	require.ErrorIs(t, UpdateObservedEdge(edgeWriter, oldEdge, newEdge), ErrNotImplemented)
	require.Same(t, oldEdge, edgeWriter.before)
	require.Equal(t, 1, edgeWriter.calls)
}

type foundationObservedWriter struct {
	Engine
	observed, ordinary int
	before             *Node
	err                error
}

func (e *foundationObservedWriter) UpdateNodeObserved(before, after *Node) error {
	e.observed++
	e.before = before
	return e.err
}
func (e *foundationObservedWriter) UpdateNode(*Node) error {
	e.ordinary++
	return errors.New("replayed")
}

type foundationMutationDecorator struct {
	Engine
	ordinary int
}

func (e *foundationMutationDecorator) UpdateNode(*Node) error               { e.ordinary++; return nil }
func (e *foundationMutationDecorator) ReadCapabilitySource() (Engine, bool) { return e.Engine, true }

type foundationObservedEdgeWriter struct {
	Engine
	before *Edge
	calls  int
}

func (e *foundationObservedEdgeWriter) UpdateEdgeObserved(before, after *Edge) error {
	e.before = before
	e.calls++
	return ErrNotImplemented
}

func TestFoundationDeclarationAllocationBudget(t *testing.T) {
	policy := BoundaryReadPolicy{{Methods: "StreamNodes Namespace", Disposition: CapabilityOwned, Scope: "fixture"}}
	request := reflect.TypeFor[nodeStreamer]()
	require.True(t, policy.Scoped(request))
	allocations := testing.AllocsPerRun(100, func() {
		if !policy.Scoped(request) {
			panic("unexpected refusal")
		}
	})
	require.Zero(t, allocations, "warm declaration admission must allocate nothing per row")
}

type foundationPromotedBindingView struct{ *TracedEngine }

func (e *foundationPromotedBindingView) ReadCapabilitySource() (Engine, bool) { return nil, false }
func (e *foundationPromotedBindingView) GetNodeProjected(NodeID, []string) (*Node, error) {
	return &Node{ID: "view-owned"}, nil
}

func TestFoundationPromotedBindingKeepsViewBoundary(t *testing.T) {
	base := &foundationCore{}
	view := &foundationPromotedBindingView{NewTracedEngine(base)}
	node, err := ReadWithCapability(view, func(reader ProjectedNodeReader) (*Node, error) { return reader.GetNodeProjected("n", nil) }, nil)
	require.NoError(t, err)
	require.Equal(t, NodeID("view-owned"), node.ID, "a promoted cached binding must not bypass a newly declared view")
}

type foundationReboundView struct {
	Engine
	id    NodeID
	walks *int
}

func (e *foundationReboundView) ReadCapabilitySource() (Engine, bool) { *e.walks++; return nil, false }
func (e *foundationReboundView) GetNodeProjected(NodeID, []string) (*Node, error) {
	return &Node{ID: e.id}, nil
}

func TestFoundationReadBindingRebindsWithoutRowDiscovery(t *testing.T) {
	walks := 0
	first := &foundationReboundView{id: "first", walks: &walks}
	inner := NewTracedEngine(first)
	outer := NewWALEngine(NewTracedEngine(inner), nil)
	read := func() NodeID {
		node, err := ReadWithCapability(outer, func(reader ProjectedNodeReader) (*Node, error) { return reader.GetNodeProjected("n", nil) }, nil)
		require.NoError(t, err)
		return node.ID
	}
	require.Equal(t, NodeID("first"), read())
	walks = 0
	for i := 0; i < 1000; i++ {
		require.Equal(t, NodeID("first"), read())
	}
	require.Zero(t, walks, "bound operations cannot call source discovery per row")
	second := &foundationReboundView{id: "second", walks: &walks}
	inner.Engine = second // source replacement before concurrent readers start
	RebindReadView(outer) // the issuance owner publishes the new topology once
	require.Equal(t, NodeID("second"), read(), "all parent bindings must observe rebinding of a descendant")
	require.Equal(t, 1, walks, "replacement must discover the new leaf once while parents reuse its binding")
	walks = 0
	for i := 0; i < 1000; i++ {
		require.Equal(t, NodeID("second"), read())
	}
	require.Zero(t, walks)
}

func TestFoundationIssuedContainerAndRemoteMembership(t *testing.T) {
	token := new(int)
	provider := &foundationOwnershipBoundary{binding: MembershipBinding{Status: MembershipBound, Owner: token}}
	admissions := 0
	root := NewCompositeEngine(nil, nil, nil).WithConstituentAccess(func(_ string, engine Engine, _ bool) (Engine, error) { admissions++; return engine, nil })
	remote := &RemoteEngine{}
	require.Equal(t, MembershipUnowned, MembershipForEngine(root).Status)
	require.Equal(t, MembershipUnowned, MembershipForEngine(remote).Status)
	for _, view := range []Engine{root.WithMembership(provider), remote.WithMembership(provider)} {
		wrapped := NewTracedEngine(NewWALEngine(view, nil))
		handle := BindMembership(wrapped)
		for _, status := range []MembershipStatus{MembershipBound, MembershipUnbound, MembershipEnded} {
			provider.binding.Status = status
			binding := handle.Current()
			require.Equal(t, status, binding.Status)
			require.Same(t, token, binding.Owner)
			require.Same(t, view, binding.Boundary)
		}
	}
	require.Zero(t, admissions, "membership/catalog discovery cannot open a data participant")
	empty := root.WithMembership(&foundationOwnershipBoundary{})
	require.Equal(t, MembershipUnbound, MembershipForEngine(empty).Status, "an issued empty owner remains a boundary")
}
