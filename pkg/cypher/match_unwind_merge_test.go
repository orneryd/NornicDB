package cypher

import (
	"context"
	"sort"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestBug_MatchUnwindParamMerge_ExpandsListValues verifies that a
// parameterized UNWIND between MATCH and MERGE iterates over the list
// values, not the loop variable identifier.
//
// Regression: UNWIND $names AS name produced {id: "name"} (the literal
// variable name) instead of iterating ["proj-a", "proj-b"].
func TestBug_MatchUnwindParamMerge_ExpandsListValues(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Seed anchor node.
	_, err := exec.Execute(ctx, `CREATE (n:Node {id: 'test-node'})`, nil)
	require.NoError(t, err)

	// The exact query shape from the bug report.
	res, err := exec.Execute(ctx, `
		MATCH (anchor:Node {id: $anchor})
		UNWIND $names AS name
		MERGE (p:Node {id: name})
		MERGE (anchor)-[:LINKS]->(p)
		RETURN p.id AS linked
	`, map[string]interface{}{
		"anchor": "test-node",
		"names":  []string{"proj-a", "proj-b"},
	})
	require.NoError(t, err)

	// Must produce one row per list element, not one row with the literal "name".
	require.Len(t, res.Rows, 2, "expected 2 rows, one per UNWIND item")

	got := make([]string, len(res.Rows))
	for i, row := range res.Rows {
		require.Len(t, row, 1)
		s, ok := row[0].(string)
		require.True(t, ok, "expected string, got %T", row[0])
		got[i] = s
	}
	sort.Strings(got)
	require.Equal(t, []string{"proj-a", "proj-b"}, got)
}

// TestMatchUnwindMerge_InlineList ensures the fix also works when the
// UNWIND source is an inline list literal rather than a $parameter.
func TestMatchUnwindMerge_InlineList(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `CREATE (n:Node {id: 'root'})`, nil)
	require.NoError(t, err)

	res, err := exec.Execute(ctx, `
		MATCH (anchor:Node {id: 'root'})
		UNWIND ['x', 'y', 'z'] AS name
		MERGE (p:Node {id: name})
		MERGE (anchor)-[:LINKS]->(p)
		RETURN p.id AS linked
	`, nil)
	require.NoError(t, err)

	require.Len(t, res.Rows, 3)

	got := make([]string, len(res.Rows))
	for i, row := range res.Rows {
		s, ok := row[0].(string)
		require.True(t, ok)
		got[i] = s
	}
	sort.Strings(got)
	require.Equal(t, []string{"x", "y", "z"}, got)
}

// TestMatchUnwindMerge_EmptyList confirms that UNWIND of an empty list
// produces zero rows (no error).
func TestMatchUnwindMerge_EmptyList(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `CREATE (n:Node {id: 'anchor'})`, nil)
	require.NoError(t, err)

	res, err := exec.Execute(ctx, `
		MATCH (anchor:Node {id: $anchor})
		UNWIND $names AS name
		MERGE (p:Node {id: name})
		RETURN p.id AS linked
	`, map[string]interface{}{
		"anchor": "anchor",
		"names":  []string{},
	})
	require.NoError(t, err)
	require.Len(t, res.Rows, 0)
}

// TestMatchUnwindMerge_NoMatchProducesNoRows ensures that when the
// MATCH finds no anchor, the UNWIND+MERGE does not execute.
func TestMatchUnwindMerge_NoMatchProducesNoRows(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	res, err := exec.Execute(ctx, `
		MATCH (anchor:Node {id: 'nonexistent'})
		UNWIND $names AS name
		MERGE (p:Node {id: name})
		RETURN p.id AS linked
	`, map[string]interface{}{
		"names": []string{"a", "b"},
	})
	require.NoError(t, err)
	require.Len(t, res.Rows, 0)
}
