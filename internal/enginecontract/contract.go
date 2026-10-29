// Package enginecontract supplies semantic adapters for the structural Engine
// population gate. Factories seed an independent oracle before issuing a view.
package enginecontract

import (
	"context"
	"fmt"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"reflect"
	"sort"
	"testing"
)

type Fixture struct {
	Engine     storage.Engine
	IDs        []string
	AbsentIDs  []string
	Membership storage.MembershipStatus
	Owner      any
	Namespace  string
	Binding    *storage.MembershipBinding
	End        func()
}
type StoredDisposition uint8

const (
	StoredNative StoredDisposition = iota + 1
	StoredNodesOnly
	StoredEmpty
	StoredRefused
)

type Adapter struct {
	Stored StoredDisposition

	Factory func(*testing.T) Fixture
	Oracle  func(*testing.T, Fixture)
}

// Check proves that the registered factory reaches the actual type and that
// every logical obligation is declared. Oracle is a required semantic adapter,
// not the resolver's own output used as its expected result.
func Check(t *testing.T, name string, adapter Adapter) {
	t.Helper()
	require.NotNil(t, adapter.Factory)
	require.NotNil(t, adapter.Oracle)
	f := adapter.Factory(t)
	typ := reflect.TypeOf(f.Engine)
	if typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	require.Equal(t, name, typ.PkgPath()+"."+typ.Name())
	declaration, ok := f.Engine.(storage.EngineBoundaryDeclaration)
	require.True(t, ok)
	require.Empty(t, PolicyProblems(declaration.EngineCapabilityPolicy()))
	StoredModes(t, f, adapter.Stored)
	adapter.Oracle(t, f)
}

func PolicyProblems(policy storage.EngineBoundaryPolicy) []string {
	var problems []string
	if policy.Kind < storage.EngineLeaf || policy.Kind > storage.EngineContainer {
		problems = append(problems, "missing structural disposition")
	}
	value := reflect.ValueOf(policy)
	for i := 1; i < value.NumField(); i++ {
		d := storage.BindingDisposition(value.Field(i).Uint())
		if d < storage.BindingOwned || d > storage.BindingRefused {
			problems = append(problems, fmt.Sprintf("missing logical obligation %s", value.Type().Field(i).Name))
		}
	}
	return problems
}

// Rows compares the public core and optional-operation paths with fixture IDs
// captured independently from the seeded physical/pending view.
func Rows(t *testing.T, f Fixture) {
	t.Helper()
	var logicalHandles []storage.MembershipHandle
	for _, word := range []string{"", "T", "W", "TW", "WT", "TT", "WW", "TWT", "WTW"} {
		subject := f.Engine
		for _, token := range word {
			switch token {
			case 'T':
				subject = storage.NewTracedEngine(subject)
			case 'W':
				subject = storage.NewWALEngine(subject, nil)
			default:
				t.Fatal("invalid finite stack grammar")
			}
		}
		bound := storage.BindMembership(subject)
		require.Equal(t, f.Membership, bound.Current().Status)
		want := storage.MembershipBinding{}
		if f.Binding != nil {
			want = *f.Binding
		}
		require.Empty(t, BindingProblems(want, bound.Current()), word)
		logicalHandles = append(logicalHandles, bound)
		for _, kind := range []string{"kernel", "ordinary", "iterator"} {
			var ids []string
			visit := func(node *storage.Node) error { ids = append(ids, string(node.ID)); return nil }
			var err error
			switch kind {
			case "kernel":
				err = subject.StreamNodesWithOptions(context.Background(), storage.StreamNodesOptions{WithEmbeddings: true, ApplyDecayFilter: true}, visit)
			case "ordinary":
				err = storage.StreamNodesWithFallback(context.Background(), subject, 8, visit)
			case "iterator":
				err = storage.VisitNodes(subject, func(node *storage.Node) bool { _ = visit(node); return true })
			}
			require.NoError(t, err, kind)
			sort.Strings(ids)
			want := append([]string{}, f.IDs...)
			sort.Strings(want)
			require.Equal(t, want, append([]string{}, ids...), kind)
		}
		// Batch hydration is a separate public consumer of the same issued
		// node view. Its membership cannot be inferred from stream success.
		requests := make([]storage.NodeID, 0, len(f.IDs)+len(f.AbsentIDs))
		for _, id := range append(append([]string{}, f.IDs...), f.AbsentIDs...) {
			requests = append(requests, storage.NodeID(id))
		}
		for _, contextual := range []bool{false, true} {
			var nodes map[storage.NodeID]*storage.Node
			var err error
			if contextual {
				nodes, err = storage.BatchGetNodesWithContext(context.Background(), subject, requests)
			} else {
				nodes, err = subject.BatchGetNodes(requests)
			}
			require.NoError(t, err, "batch contextual=%v stack=%s", contextual, word)
			var ids []string
			for id, node := range nodes {
				require.NotNil(t, node)
				require.Equal(t, id, node.ID, "batch preserves request/result identity")
				ids = append(ids, string(id))
			}
			sort.Strings(ids)
			want := append([]string{}, f.IDs...)
			sort.Strings(want)
			require.Equal(t, want, append([]string{}, ids...), "batch contextual=%v stack=%s", contextual, word)
		}
		NodeBatchPoint(t, requests, subject.BatchGetNodes, subject.GetNode)
		NodeBatchPoint(t, requests, func(ids []storage.NodeID) (map[storage.NodeID]*storage.Node, error) {
			return storage.BatchGetNodesWithContext(context.Background(), subject, ids)
		}, subject.GetNode)
	}
	handle := storage.BindMembership(f.Engine)
	binding := handle.Current()
	require.Equal(t, f.Membership, binding.Status)
	require.Equal(t, f.Owner, binding.Owner)
	if f.Membership != storage.MembershipUnowned {
		require.NotNil(t, f.Binding, "bound members need an independent complete logical oracle")
	}
	expected := storage.MembershipBinding{}
	if f.Binding != nil {
		expected = *f.Binding
	}
	require.Empty(t, BindingProblems(expected, binding))
	if f.End != nil {
		f.End()
		require.Equal(t, storage.MembershipEnded, handle.Current().Status, "retained owner must remain a boundary after ending")
		require.Empty(t, BindingProblems(expected, handle.Current()))
		for _, bound := range logicalHandles {
			require.Equal(t, storage.MembershipEnded, bound.Current().Status)
			require.Empty(t, BindingProblems(expected, bound.Current()))
		}
		// A live descriptor is not a stored-read outcome. Cross the actual
		// mandatory consumers after ending, including empty/container adapters
		// which could otherwise answer without entering an ended native tx.
		for _, mode := range []storage.StoredReadMode{storage.StoredMetadataOnly, storage.StoredSelectedProperties, storage.StoredFullMaterialization} {
			var work storage.StoredReadWork
			result, err := storage.ReadStored(context.Background(), f.Engine, storage.StoredReadRequest{Mode: mode, Nodes: []storage.NodeID{"sentinel"}, Work: &work})
			require.Error(t, err, "ended issued view cannot answer stored mode %v", mode)
			require.Nil(t, result)
			require.Zero(t, work.Captures)
		}
	}

	if f.Namespace != "" {
		provider, ok := storage.ViewMetadataForEngine[interface{ Namespace() string }](f.Engine)
		require.True(t, ok)
		require.Equal(t, f.Namespace, provider.Namespace())
	}
}

// BindingProblems compares actual causal references with independently seeded
// owner tokens. Equal pointee values do not authorize a copied/replaced owner.
func BindingProblems(expected, actual storage.MembershipBinding) []string {
	var problems []string
	left, right := reflect.ValueOf(expected), reflect.ValueOf(actual)
	for _, name := range []string{"Owner", "Adapter", "Controller", "PendingActions", "SchemaPublication", "ReadLease", "StatementMode", "Enforcement"} {
		a, b := left.FieldByName(name).Interface(), right.FieldByName(name).Interface()
		equal := false
		if a == nil || b == nil {
			equal = a == nil && b == nil
		} else if reflect.TypeOf(a) == reflect.TypeOf(b) && reflect.TypeOf(a).Comparable() {
			equal = a == b
		}
		if !equal {
			problems = append(problems, "lost or replaced "+name+" owner")
		}
	}
	return problems
}
