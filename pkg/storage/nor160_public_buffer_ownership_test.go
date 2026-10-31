// Author: Codex, NOR-160 public storage read ownership, 29 October 2026.
package storage

import (
	"sort"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/internal/testfixture/readownership"
	"github.com/stretchr/testify/require"
)

// Freeze unordered image populations without changing the returned container.
// Only entity populations are sorted; property lists and vector order remain
// significant. Mutate still receives the original public result afterwards.
func nor160CapturePublicImages(value any) any {
	switch values := value.(type) {
	case []*Node:
		ordered := append([]*Node(nil), values...)
		sort.Slice(ordered, func(i, j int) bool { return ordered[i].ID < ordered[j].ID })
		return readownership.Capture(ordered)
	case []*Edge:
		ordered := append([]*Edge(nil), values...)
		sort.Slice(ordered, func(i, j int) bool { return ordered[i].ID < ordered[j].ID })
		return readownership.Capture(ordered)
	case []any:
		captured := make([]any, len(values))
		for i, item := range values {
			captured[i] = nor160CapturePublicImages(item)
		}
		return captured
	default:
		return readownership.Capture(value)
	}
}

func nor160PublicBufferFixture(t *testing.T) *WriteBehindBuffer {
	t.Helper()
	engine := newWriteBehindTestEngine(t, time.Hour)
	bufferedWrite(t, engine, func(writer *BadgerTransaction) error {
		for _, id := range []NodeID{"test:a", "test:b"} {
			_, err := writer.CreateNode(&Node{ID: id, Labels: []string{"Held"}, Properties: nor160OwnershipProperties(),
				ChunkEmbeddings: [][]float32{{1, 2, 3}}, NamedEmbeddings: map[string][]float32{"original": {4, 5, 6}},
				EmbedMeta: map[string]any{"nested": map[string]any{"list": []any{"original", []byte{1, 2}}}}})
			if err != nil {
				return err
			}
		}
		return writer.CreateEdge(&Edge{ID: "test:e", StartNode: "test:a", EndNode: "test:b", Type: "T", Properties: nor160OwnershipProperties()})
	})
	require.Positive(t, engine.writeBehind.PendingOps(), "read the retained buffer before apply")
	return engine.writeBehind
}

func nor160ContainsPublicImage(value any) bool {
	switch values := value.(type) {
	case *Node:
		return values != nil
	case *Edge:
		return values != nil
	case []*Node:
		return len(values) > 0
	case []*Edge:
		return len(values) > 0
	case []any:
		for _, item := range values {
			if nor160ContainsPublicImage(item) {
				return true
			}
		}
	}
	return false
}

func TestNOR160CallerOwnedPublicBufferGetters(t *testing.T) {
	cases := []struct {
		name string
		read func(*testing.T, *WriteBehindBuffer) any
	}{
		{"GetNode", func(t *testing.T, w *WriteBehindBuffer) any {
			n, found, deleted := w.GetNode("test:a")
			require.True(t, found)
			require.False(t, deleted)
			return n
		}},
		{"GetEdge", func(t *testing.T, w *WriteBehindBuffer) any {
			e, found, deleted := w.GetEdge("test:e")
			require.True(t, found)
			require.False(t, deleted)
			return e
		}},
		{"LookupNode", func(t *testing.T, w *WriteBehindBuffer) any {
			n, found := w.LookupNode("test:a")
			require.True(t, found)
			return n
		}},
		{"LookupEdge", func(t *testing.T, w *WriteBehindBuffer) any {
			e, found := w.LookupEdge("test:e")
			require.True(t, found)
			return e
		}},
		{"KeyCandidates", func(t *testing.T, w *WriteBehindBuffer) any {
			key, ok := indexValueKey("original")
			require.True(t, ok)
			return w.KeyCandidates("Held", "key", key)
		}},
		{"LabelNodes", func(t *testing.T, w *WriteBehindBuffer) any { return w.LabelNodes("Held") }},
		{"LabelOverlay", func(t *testing.T, w *WriteBehindBuffer) any {
			nodes, touched := w.LabelOverlay("Held")
			return []any{nodes, touched}
		}},
		{"TypeEdges", func(t *testing.T, w *WriteBehindBuffer) any { return w.TypeEdges("T") }},
		{"TypeOverlay", func(t *testing.T, w *WriteBehindBuffer) any {
			edges, touched := w.TypeOverlay("T")
			return []any{edges, touched}
		}},
		{"AllNodesOverlay", func(t *testing.T, w *WriteBehindBuffer) any {
			nodes, touched := w.AllNodesOverlay()
			return []any{nodes, touched}
		}},
		{"AllEdgesOverlay", func(t *testing.T, w *WriteBehindBuffer) any {
			edges, touched := w.AllEdgesOverlay()
			return []any{edges, touched}
		}},
		{"EdgeAdjacencyOverlay", func(t *testing.T, w *WriteBehindBuffer) any {
			edges, hidden, deleted := w.EdgeAdjacencyOverlay("test:a", true)
			require.False(t, deleted)
			return []any{edges, hidden}
		}},
		{"EdgesBetweenOverlay", func(t *testing.T, w *WriteBehindBuffer) any {
			edges, hidden, left, right := w.EdgesBetweenOverlay("test:a", "test:b")
			require.False(t, left)
			require.False(t, right)
			return []any{edges, hidden}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// A failed baseline case cannot contaminate the next API's oracle.
			buffer := nor160PublicBufferFixture(t)
			for pass := 0; pass < 3; pass++ {
				result := tc.read(t, buffer)
				require.True(t, nor160ContainsPublicImage(result), "the public getter must return a fixture image")
				want := nor160CapturePublicImages(result)
				readownership.Mutate(result)
				again := tc.read(t, buffer)
				require.Equal(t, want, nor160CapturePublicImages(again), "public %s mutation changed a later read, pass %d", tc.name, pass)
			}
		})
	}
}
