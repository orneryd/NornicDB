package storage

import (
	"context"
	"errors"
	"testing"
)

type nor56StreamingProbe struct {
	Engine
	node *Node
}

func (p *nor56StreamingProbe) AllNodes(context.Context) ([]*Node, error) {
	return nil, errors.New("unexpected full-store fallback")
}
func (p *nor56StreamingProbe) StreamNodes(ctx context.Context, fn func(*Node) error) error {
	return fn(p.node)
}
func (p *nor56StreamingProbe) StreamEdges(context.Context, func(*Edge) error) error { return nil }
func (p *nor56StreamingProbe) StreamNodeChunks(ctx context.Context, _ int, fn func([]*Node) error) error {
	return fn([]*Node{p.node})
}

func TestRebaseNOR56TracedStreamingConsumer(t *testing.T) {
	base := NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
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
