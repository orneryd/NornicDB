//go:build nornic_copycount

package storage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// The predicate contract (NOR-160): a predicate reads a read-only candidate, a
// rejected candidate costs no copy, and an accepted one is copied once.
func TestNOR160PredicateCandidatesCopyOnlyWhatIsAccepted(t *testing.T) {
	engine := newTestMemoryEngine(t)
	const count = 8
	for index := 0; index < count; index++ {
		_, err := engine.CreateNode(&Node{ID: NodeID(fmt.Sprintf("test:n%d", index)), Labels: []string{"P"}, Properties: map[string]any{"k": int64(index)}})
		require.NoError(t, err)
	}
	require.NoError(t, engine.CreateEdge(&Edge{ID: "test:e", Type: "T", StartNode: "test:n0", EndNode: "test:n1", Properties: map[string]any{"payload": map[string]any{"x": int64(1)}}}))

	t.Run("label scan candidates are read-only and uncopied", func(t *testing.T) {
		tx, err := engine.BeginTransaction()
		require.NoError(t, err)
		defer func() { _ = tx.Rollback() }()
		require.NoError(t, tx.SetNamespace("test"))
		properties := []string{"k"}
		first := map[NodeID]*Node{}
		require.NoError(t, tx.VisitReadOnlyNodesByLabel("P", properties, func(node *Node) error {
			first[node.ID] = node
			return nil
		}))
		require.Len(t, first, count)
		nodes, _ := CountStructuralCopies(func() {
			require.NoError(t, tx.VisitReadOnlyNodesByLabel("P", properties, func(node *Node) error {
				require.Same(t, first[node.ID], node, "a retained candidate must be handed over as it is")
				return nil
			}))
		})
		require.Empty(t, nodes, "reading candidates must copy nothing")
		delivered, _ := CountStructuralCopies(func() {
			require.NoError(t, tx.StreamNodesByLabelProjected("P", properties, func(node *Node) error {
				require.NotSame(t, first[node.ID], node, "the public stream delivers a copy")
				return nil
			}))
		})
		require.Len(t, delivered, count)
		for id, copies := range delivered {
			require.Equal(t, 1, copies, id)
		}
	})

	t.Run("MatchEdgesBetween copies only an accepted cached edge, once", func(t *testing.T) {
		_, err := engine.GetEdge("test:e")
		require.NoError(t, err)
		properties := []string{"payload"}
		_, rejected := CountStructuralCopies(func() {
			matched, err := MatchEdgesBetween(engine, "test:n0", "test:n1", "T", properties, func(*Edge) bool { return false })
			require.NoError(t, err)
			require.Empty(t, matched)
		})
		require.Empty(t, rejected, "a rejected candidate must cost no copy")
		var candidate *Edge
		var matched []*Edge
		_, accepted := CountStructuralCopies(func() {
			matched, err = MatchEdgesBetween(engine, "test:n0", "test:n1", "T", properties, func(edge *Edge) bool {
				candidate = edge
				return true
			})
			require.NoError(t, err)
		})
		require.Len(t, matched, 1)
		require.Equal(t, 1, accepted["test:e"], "an accepted candidate is copied once")
		require.NotSame(t, candidate, matched[0])

		// What is delivered is the caller's: changing it reaches no later read.
		matched[0].Properties["payload"].(map[string]any)["x"] = int64(99)
		again, err := MatchEdgesBetween(engine, "test:n0", "test:n1", "T", properties, func(*Edge) bool { return true })
		require.NoError(t, err)
		require.Equal(t, int64(1), again[0].Properties["payload"].(map[string]any)["x"])
	})
}
