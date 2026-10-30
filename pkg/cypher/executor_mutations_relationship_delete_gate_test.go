package cypher

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// These tests lock the bound-relationship-delete eligibility gate: the pure
// predicates that decide whether a DELETE query is routed through the fast
// path. They cover the rejection branches (blocked clauses, malformed
// WITH ... LIMIT, unknown relationship types) that the query-level tests do
// not exercise directly.

func TestIsSimpleBoundRelationshipDeleteMatchSegment(t *testing.T) {
	cases := []struct {
		name    string
		segment string
		want    bool
	}{
		{"empty", "   ", false},
		{"plain", "MATCH (a) MATCH (a)-[r]->(b)", true},
		{"with", "MATCH (a)-[r]->(b) WITH r", false},
		{"limit", "MATCH (a)-[r]->(b) LIMIT 1", false},
		{"skip", "MATCH (a)-[r]->(b) SKIP 1", false},
		{"order by", "MATCH (a)-[r]->(b) ORDER BY r", false},
		{"call", "CALL db.foo() MATCH (a)-[r]->(b)", false},
		{"unwind", "UNWIND [1] AS x MATCH (a)-[r]->(b)", false},
		{"optional match", "MATCH (a) OPTIONAL MATCH (a)-[r]->(b)", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isSimpleBoundRelationshipDeleteMatchSegment(tc.segment); got != tc.want {
				t.Fatalf("isSimpleBoundRelationshipDeleteMatchSegment(%q) = %v, want %v", tc.segment, got, tc.want)
			}
		})
	}
}

func TestParseBoundRelationshipDeleteWithLimitSegment(t *testing.T) {
	const deleteVar = "r"
	base := "MATCH (a) MATCH (a)-[r]->(b)"

	t.Run("valid", func(t *testing.T) {
		gotBase, gotLimit, ok := parseBoundRelationshipDeleteWithLimitSegment(base+" WITH r LIMIT 5", deleteVar)
		if !ok {
			t.Fatal("expected ok for WITH r LIMIT 5")
		}
		if gotLimit != 5 {
			t.Fatalf("limit = %d, want 5", gotLimit)
		}
		if gotBase != base {
			t.Fatalf("base = %q, want %q", gotBase, base)
		}
	})

	rejections := []struct {
		name    string
		segment string
	}{
		{"blocked order by", base + " WITH r ORDER BY r LIMIT 1"},
		{"blocked skip", base + " WITH r SKIP 1 LIMIT 1"},
		{"blocked call", base + " CALL db.x() WITH r LIMIT 1"},
		{"blocked unwind", base + " UNWIND [1] AS x WITH r LIMIT 1"},
		{"blocked optional match", base + " OPTIONAL MATCH (a)-[r2]->(c) WITH r LIMIT 1"},
		{"no with", base + " LIMIT 1"},
		{"non-simple base", "MATCH (a) WITH a MATCH (a)-[r]->(b) WITH r LIMIT 1"},
		{"no limit", base + " WITH r"},
		{"wrong var", base + " WITH x LIMIT 1"},
		{"non-numeric limit", base + " WITH r LIMIT abc"},
		{"zero limit", base + " WITH r LIMIT 0"},
		{"negative limit", base + " WITH r LIMIT -1"},
		{"multi-field limit", base + " WITH r LIMIT 1 2"},
	}
	for _, tc := range rejections {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, ok := parseBoundRelationshipDeleteWithLimitSegment(tc.segment, deleteVar); ok {
				t.Fatalf("expected rejection for %q", tc.segment)
			}
		})
	}
}

func TestBoundRelationshipDeleteTargetNode(t *testing.T) {
	store := newTestMemoryEngine(t)
	exec := NewStorageExecutor(store)

	src := storage.NodeID("test:s")
	dst := storage.NodeID("test:t")
	_, err := store.CreateNode(&storage.Node{ID: src, Labels: []string{"N"}})
	require.NoError(t, err)
	_, err = store.CreateNode(&storage.Node{ID: dst, Labels: []string{"N"}})
	require.NoError(t, err)
	edge := &storage.Edge{ID: "test:e", StartNode: src, EndNode: dst, Type: "LINKS"}

	t.Run("outgoing resolves end node", func(t *testing.T) {
		target, ok, err := exec.boundRelationshipDeleteTargetNode(store, src, edge, "outgoing")
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, dst, target.ID)
	})
	t.Run("outgoing rejects when source is not the start", func(t *testing.T) {
		_, ok, err := exec.boundRelationshipDeleteTargetNode(store, dst, edge, "outgoing")
		require.NoError(t, err)
		require.False(t, ok)
	})
	t.Run("incoming resolves start node", func(t *testing.T) {
		target, ok, err := exec.boundRelationshipDeleteTargetNode(store, dst, edge, "incoming")
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, src, target.ID)
	})
	t.Run("incoming rejects when source is not the end", func(t *testing.T) {
		_, ok, err := exec.boundRelationshipDeleteTargetNode(store, src, edge, "incoming")
		require.NoError(t, err)
		require.False(t, ok)
	})
	t.Run("both resolves from either endpoint", func(t *testing.T) {
		fromStart, ok, err := exec.boundRelationshipDeleteTargetNode(store, src, edge, "both")
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, dst, fromStart.ID)

		fromEnd, ok, err := exec.boundRelationshipDeleteTargetNode(store, dst, edge, "both")
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, src, fromEnd.ID)
	})
	t.Run("both rejects unrelated source", func(t *testing.T) {
		_, ok, err := exec.boundRelationshipDeleteTargetNode(store, storage.NodeID("test:other"), edge, "both")
		require.NoError(t, err)
		require.False(t, ok)
	})
	t.Run("unknown direction rejects", func(t *testing.T) {
		_, ok, err := exec.boundRelationshipDeleteTargetNode(store, src, edge, "sideways")
		require.NoError(t, err)
		require.False(t, ok)
	})
	t.Run("missing target node skips candidate", func(t *testing.T) {
		danglingEdge := &storage.Edge{ID: "test:e2", StartNode: src, EndNode: storage.NodeID("test:ghost"), Type: "LINKS"}
		// A missing target resolves to "no match"; whether the engine reports
		// that as ErrNotFound or a nil node, the fast path must not select it.
		_, ok, _ := exec.boundRelationshipDeleteTargetNode(store, src, danglingEdge, "outgoing")
		require.False(t, ok)
	})
}

func TestRelationshipTypeAllowed(t *testing.T) {
	multi := []string{"LINKS", "OWNS"}
	multiSet := buildRelTypeSet(multi)

	cases := []struct {
		name       string
		edgeType   string
		allowed    []string
		allowedSet map[string]struct{}
		want       bool
	}{
		{"no restriction", "ANY", nil, nil, true},
		{"single match", "LINKS", []string{"LINKS"}, nil, true},
		{"single mismatch", "OWNS", []string{"LINKS"}, nil, false},
		{"multi match", "OWNS", multi, multiSet, true},
		{"multi mismatch", "REFERS", multi, multiSet, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := relationshipTypeAllowed(tc.edgeType, tc.allowed, tc.allowedSet); got != tc.want {
				t.Fatalf("relationshipTypeAllowed(%q, %v) = %v, want %v", tc.edgeType, tc.allowed, got, tc.want)
			}
		})
	}
}
