package storage

import (
	"context"
	"errors"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
)

type nor56StreamingProbe struct {
	Engine
	node *Node
}

func (p *nor56StreamingProbe) AllNodes(context.Context) ([]*Node, error) {
	return nil, errors.New("unexpected full-store fallback")
}
func (p *nor56StreamingProbe) StreamNodes(ctx context.Context, fn func(*Node) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return fn(p.node)
}
func (p *nor56StreamingProbe) StreamEdges(context.Context, func(*Edge) error) error { return nil }
func (p *nor56StreamingProbe) StreamNodeChunks(ctx context.Context, _ int, fn func([]*Node) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return fn([]*Node{p.node})
}

func TestRebaseNOR56TracedStreamingConsumer(t *testing.T) {
	base := fixtureowner.Own(t, fixtureowner.Track(t, NewMemoryEngine(), ErrStorageClosed))
	probe := &nor56StreamingProbe{Engine: base, node: &Node{ID: "tenant:one"}}
	for _, tc := range []struct {
		name   string
		engine Engine
	}{{"direct", probe}, {"traced", NewTracedEngine(probe)}} {
		t.Run(tc.name, func(t *testing.T) {
			var seen []NodeID
			err := StreamNodesWithFallback(t.Context(), tc.engine, 1, func(n *Node) error { seen = append(seen, n.ID); return nil })
			if err != nil {
				t.Fatalf("supported streaming path was lost: %v", err)
			}
			if len(seen) != 1 || seen[0] != "tenant:one" {
				t.Fatalf("unexpected streamed nodes: %v", seen)
			}
		})
	}
}

func TestRebaseNOR56StreamingResolutionContract(t *testing.T) {
	base := fixtureowner.Own(t, fixtureowner.Track(t, NewMemoryEngine(), ErrStorageClosed))
	probe := &nor56StreamingProbe{Engine: base, node: &Node{ID: "tenant:one"}}
	for _, engine := range []Engine{probe, NewTracedEngine(probe), NewTracedEngine(NewTracedEngine(probe))} {
		streamer, ok := ResolveStreamingEngine(engine)
		if !ok {
			t.Fatal("streaming capability not resolved")
		}
		want := errors.New("stop")
		if err := streamer.StreamNodeChunks(t.Context(), 1, func([]*Node) error { return want }); !errors.Is(err, want) {
			t.Fatalf("chunk callback error = %v, want %v", err, want)
		}
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		if err := streamer.StreamNodes(ctx, func(*Node) error { t.Fatal("visited after cancellation"); return nil }); !errors.Is(err, context.Canceled) {
			t.Fatalf("cancellation error = %v", err)
		}
	}
	// Embedding the Engine interface deliberately hides the concrete memory
	// engine's optional streaming methods.
	unsupported := struct{ Engine }{base}
	if _, ok := ResolveStreamingEngine(NewTracedEngine(unsupported)); ok {
		t.Fatal("tracing falsely advertised streaming on an unsupported engine")
	}
}

func TestRebaseNOR56TracedChunksRespectNamespace(t *testing.T) {
	base := fixtureowner.Own(t, fixtureowner.Track(t, NewMemoryEngine(), ErrStorageClosed))
	a := NewNamespacedEngine(base, "a")
	b := NewNamespacedEngine(base, "b")
	for _, tenant := range []*NamespacedEngine{a, b} {
		if _, err := tenant.CreateNode(t.Context(), &Node{ID: "one"}); err != nil {
			t.Fatal(err)
		}
	}
	for _, view := range []Engine{NewTracedEngine(a), NewNamespacedEngine(NewTracedEngine(base), "a")} {
		streamer, ok := ResolveStreamingEngine(view)
		if !ok {
			t.Fatal("namespace streaming capability was lost")
		}
		var ids []NodeID
		if err := streamer.StreamNodeChunks(t.Context(), 1, func(nodes []*Node) error {
			for _, node := range nodes {
				ids = append(ids, node.ID)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if len(ids) != 1 || ids[0] != "one" {
			t.Fatalf("namespace stream returned %v", ids)
		}
	}
}
