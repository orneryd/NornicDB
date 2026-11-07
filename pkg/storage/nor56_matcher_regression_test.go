package storage

import (
	"context"
	"errors"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
)

type nor56MatcherProbe struct {
	Engine
	matcher EdgesBetweenMatcher
	calls   int
}

func (p *nor56MatcherProbe) MatchEdgesBetween(ctx context.Context, start, end NodeID, typ string, properties []string, match func(*Edge) bool) ([]*Edge, error) {
	p.calls++
	return p.matcher.MatchEdgesBetween(ctx, start, end, typ, properties, match)
}
func TestNOR56NamespacedMatcherForwarding(t *testing.T) {
	base := fixtureowner.Own(t, fixtureowner.Track(t, NewMemoryEngine(), ErrStorageClosed))
	probe := &nor56MatcherProbe{Engine: base, matcher: base}
	for _, name := range []string{"a", "b"} {
		tenant := NewNamespacedEngine(base, name)
		for _, id := range []NodeID{"x", "y"} {
			if _, err := tenant.CreateNode(t.Context(), &Node{ID: id}); err != nil {
				t.Fatal(err)
			}
		}
		if err := tenant.CreateEdge(t.Context(), &Edge{ID: "e", StartNode: "x", EndNode: "y", Type: "R", Properties: map[string]interface{}{"value": name}}); err != nil {
			t.Fatal(err)
		}
	}
	views := []Engine{
		NewNamespacedEngine(probe, "a"),
		NewTracedEngine(NewNamespacedEngine(probe, "a")),
		NewNamespacedEngine(NewTracedEngine(NewWALEngine(probe, nil)), "a"),
	}
	for _, view := range views {
		probe.calls = 0
		edges, err := MatchEdgesBetween(t.Context(), view, "x", "y", "R", []string{"value"}, func(edge *Edge) bool {
			return edge.ID == "e" && edge.StartNode == "x" && edge.EndNode == "y" && edge.Properties["value"] == "a"
		})
		if err != nil {
			t.Fatal(err)
		}
		if len(edges) != 1 || edges[0].ID != "e" {
			t.Fatalf("wrong tenant edges: %#v", edges)
		}
		if probe.calls != 1 {
			t.Fatalf("optional matcher not forwarded: got %d calls", probe.calls)
		}
	}
	// An engine that hides the optional interface retains its ordinary
	// namespaced GetEdgesBetween fallback through tracing.
	withoutMatcher := struct{ Engine }{NewNamespacedEngine(base, "a")}
	rows, err := MatchEdgesBetween(t.Context(), NewTracedEngine(withoutMatcher), "x", "y", "R", []string{"value"}, func(edge *Edge) bool { return edge.Properties["value"] == "a" })
	if err != nil || len(rows) != 1 || rows[0].ID != "e" {
		t.Fatalf("unsupported matcher fallback: rows=%v err=%v", rows, err)
	}
	view := views[len(views)-1]
	if _, err := MatchEdgesBetween(t.Context(), view, "x", "y", "R", nil, nil); !errors.Is(err, ErrInvalidData) {
		t.Fatalf("nil predicate error = %v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := MatchEdgesBetween(ctx, view, "x", "y", "R", nil, func(*Edge) bool { t.Fatal("visited after cancellation"); return false }); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation error = %v", err)
	}
}
