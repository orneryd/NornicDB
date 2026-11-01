// Package relatedgraph builds shared related-node benchmark populations.
package relatedgraph

import (
	"fmt"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"testing"
)

// Fanout builds root -> branches -> leaves on a lifetime-tracked memory engine.
// Callers select branch/leaf counts to preserve their consumer-specific cap;
// the caller starts its benchmark timer only after construction completes.
func Fanout(b *testing.B, branches, leaves int) storage.Engine {
	b.Helper()
	engine := fixtureowner.Track(b, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	b.Cleanup(func() { _ = engine.Close() })
	if _, err := engine.CreateNode(&storage.Node{ID: "bench:root", Labels: []string{"Root"}}); err != nil {
		b.Fatalf("create root: %v", err)
	}
	for i := 0; i < branches; i++ {
		branch := storage.NodeID(fmt.Sprintf("bench:branch-%02d", i))
		if _, err := engine.CreateNode(&storage.Node{ID: branch, Labels: []string{"Branch"}, Properties: map[string]interface{}{"title": string(branch)}}); err != nil {
			b.Fatalf("create branch: %v", err)
		}
		if err := engine.CreateEdge(&storage.Edge{ID: storage.EdgeID(fmt.Sprintf("bench:e-root-%02d", i)), StartNode: "bench:root", EndNode: branch, Type: "LINK"}); err != nil {
			b.Fatalf("create branch edge: %v", err)
		}
		for j := 0; j < leaves; j++ {
			leaf := storage.NodeID(fmt.Sprintf("bench:leaf-%02d-%02d", i, j))
			if _, err := engine.CreateNode(&storage.Node{ID: leaf, Labels: []string{"Leaf"}, Properties: map[string]interface{}{"title": string(leaf)}}); err != nil {
				b.Fatalf("create leaf: %v", err)
			}
			if err := engine.CreateEdge(&storage.Edge{ID: storage.EdgeID(fmt.Sprintf("bench:e-%02d-%02d", i, j)), StartNode: branch, EndNode: leaf, Type: "LINK"}); err != nil {
				b.Fatalf("create leaf edge: %v", err)
			}
		}
	}
	return engine
}
