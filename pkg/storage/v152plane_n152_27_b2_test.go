// NOR-312 audit: the constituent-access contract of the composite engine.
//
// The owner of a composite statement's participant access is
// CompositeEngine.accessConstituent, installed by WithConstituentAccess (see
// pkg/cypher/fabric_participants.go router). This file records, for every
// affected read family, what the current tree does when that access function
// reports a participant it could not open.
package storage

import (
	"testing"
)

// constituentAccessError is the production access-owner failure shape: the
// statement could not open (or register) a participant.
type constituentAccessError struct{ alias string }

func (e *constituentAccessError) Error() string {
	return "constituent " + e.alias + " could not be opened"
}

func newConstituentAccessAudit(t *testing.T) (*CompositeEngine, *CompositeEngine) {
	t.Helper()
	a := NewNamespacedEngine(newTestMemoryEngine(t), "a")
	b := NewNamespacedEngine(newTestMemoryEngine(t), "b")
	for _, fixture := range []struct {
		engine Engine
		values []string
	}{{a, []string{"a-1", "a-2"}}, {b, []string{"b-1", "b-2"}}} {
		for _, id := range fixture.values {
			_, err := fixture.engine.CreateNode(t.Context(), &Node{ID: NodeID(id), Labels: []string{"Doc"}, Properties: map[string]any{"value": id}})
			if err != nil {
				t.Fatalf("seed %s: %v", id, err)
			}
		}
		if err := fixture.engine.CreateEdge(t.Context(), &Edge{ID: EdgeID("r-" + fixture.values[0]), StartNode: NodeID(fixture.values[0]), EndNode: NodeID(fixture.values[1]), Type: "R"}); err != nil {
			t.Fatalf("seed edge in %s: %v", fixture.values[0], err)
		}
	}
	healthy := NewCompositeEngine(
		map[string]Engine{"a": a, "b": b},
		map[string]string{"a": "a", "b": "b"},
		map[string]string{"a": "read", "b": "read"},
	)
	denied := healthy.WithConstituentAccess(nil, func(alias string, engine Engine, _ bool) (Engine, error) {
		if alias == "b" {
			return nil, &constituentAccessError{alias: alias}
		}
		return engine, nil
	})
	return healthy, denied
}

func nodeValues(nodes []*Node) []string {
	values := make([]string, 0, len(nodes))
	for _, node := range nodes {
		value, _ := node.Properties["value"].(string)
		values = append(values, value)
	}
	return values
}

// TestCompositeHealthyConstituentAccessIsComplete is the healthy control for
// the audit matrix: two available constituents keep their complete results,
// so a failure case cannot be explained by a fixture that was partial anyway.
func TestCompositeHealthyConstituentAccessIsComplete(t *testing.T) {
	healthy, _ := newConstituentAccessAudit(t)
	ctx := t.Context()

	nodes, err := healthy.GetNodesByLabel(ctx, "Doc")
	if err != nil {
		t.Fatalf("GetNodesByLabel: %v", err)
	}
	if got, want := sortedValues(nodeValues(nodes)), []string{"a-1", "a-2", "b-1", "b-2"}; !equalValues(got, want) {
		t.Fatalf("GetNodesByLabel values = %v, want %v", got, want)
	}
	all, err := healthy.AllNodes(ctx)
	if err != nil || len(all) != 4 {
		t.Fatalf("AllNodes = %d, %v, want 4, nil", len(all), err)
	}
	edges, err := healthy.AllEdges(ctx)
	if err != nil || len(edges) != 2 {
		t.Fatalf("AllEdges = %d, %v, want 2, nil", len(edges), err)
	}
	if count, err := healthy.NodeCount(ctx); err != nil || count != 4 {
		t.Fatalf("NodeCount = %d, %v, want 4, nil", count, err)
	}
	if count, err := healthy.EdgeCount(ctx); err != nil || count != 2 {
		t.Fatalf("EdgeCount = %d, %v, want 2, nil", count, err)
	}
	if count, err := healthy.EdgeCountByType(ctx, "R"); err != nil || count != 2 {
		t.Fatalf("EdgeCountByType = %d, %v, want 2, nil", count, err)
	}
	node, err := healthy.GetNode(ctx, "4:b:b-2")
	if err != nil || node == nil || node.Properties["value"] != "b-2" {
		t.Fatalf("GetNode(b-2) = %v, %v", node, err)
	}
	iterated := 0
	if err := healthy.IterateNodes(func(*Node) bool { iterated++; return true }); err != nil || iterated != 4 {
		t.Fatalf("IterateNodes = %d, %v, want 4, nil", iterated, err)
	}
	streamed := 0
	if err := healthy.StreamNodes(ctx, func(*Node) error { streamed++; return nil }); err != nil || streamed != 4 {
		t.Fatalf("StreamNodes = %d, %v, want 4, nil", streamed, err)
	}
	ids := 0
	if err := healthy.ForEachNodeIDByLabel(ctx, "Doc", func(NodeID) bool { ids++; return true }); err != nil || ids != 4 {
		t.Fatalf("ForEachNodeIDByLabel = %d, %v, want 4, nil", ids, err)
	}
	batch, err := healthy.BatchGetNodes(ctx, []NodeID{"4:a:a-1", "4:b:b-1"})
	if err != nil || len(batch) != 2 {
		t.Fatalf("BatchGetNodes = %d, %v, want 2, nil", len(batch), err)
	}
	if degree, err := healthy.GetInDegree(ctx, "4:b:b-2"); err != nil || degree != 1 {
		t.Fatalf("GetInDegree(b-2) = %d, %v, want 1, nil", degree, err)
	}
	if degree, err := healthy.GetOutDegree(ctx, "4:b:b-1"); err != nil || degree != 1 {
		t.Fatalf("GetOutDegree(b-1) = %d, %v, want 1, nil", degree, err)
	}
	if edge, err := healthy.GetEdge(ctx, "5:b:r-b-1"); err != nil || edge == nil {
		t.Fatalf("GetEdge(b:r-b-1) = %v, %v", edge, err)
	}
	if schema := healthy.GetSchema(); schema == nil {
		t.Fatal("GetSchema returned nil for available constituents")
	}
}

// TestCompositeNonErrorConstituentReadsObservePartialData records the read
// APIs that have no error channel at all. Their audited current behaviour is
// reported successfully with only the available constituents' contribution:
// this is the shape NOR-312 must resolve with an explicit error owner, so the
// assertions below are an audit record, not an approved contract. When the
// owner adds propagation, this test is the place that must record the change.
func TestCompositeNonErrorConstituentReadsObservePartialData(t *testing.T) {
	healthy, denied := newConstituentAccessAudit(t)

	healthyNamespaces := healthy.ListNamespaces()
	deniedNamespaces := denied.ListNamespaces()
	t.Logf("ListNamespaces healthy=%v denied=%v", healthyNamespaces, deniedNamespaces)
	if len(deniedNamespaces) >= len(healthyNamespaces) {
		t.Fatalf("ListNamespaces denied = %v, healthy = %v: expected the failed constituent's namespace to be absent",
			deniedNamespaces, healthyNamespaces)
	}
	healthyVersion, healthySupported := healthy.GraphMutationVersion()
	deniedVersion, deniedSupported := denied.GraphMutationVersion()
	t.Logf("GraphMutationVersion healthy=%d/%t denied=%d/%t", healthyVersion, healthySupported, deniedVersion, deniedSupported)
	if !deniedSupported || deniedVersion == healthyVersion {
		t.Fatalf("GraphMutationVersion healthy=%d/%t denied=%d/%t: expected a reported, smaller version",
			healthyVersion, healthySupported, deniedVersion, deniedSupported)
	}
	healthyPending, deniedPending := healthy.PendingEmbeddingsCount(), denied.PendingEmbeddingsCount()
	t.Logf("PendingEmbeddingsCount healthy=%d denied=%d", healthyPending, deniedPending)
	if status := denied.LifecycleStatus(); status == nil {
		t.Fatalf("LifecycleStatus returned nil after a constituent access failure")
	}
	t.Logf("NOR-312 recorded contract: APIs without an error channel report success while omitting the "+
		"failed constituent; healthy namespaces=%v denied namespaces=%v", healthyNamespaces, deniedNamespaces)
}

func sortedValues(values []string) []string {
	out := append([]string(nil), values...)
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j] < out[j-1]; j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out
}

func equalValues(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
