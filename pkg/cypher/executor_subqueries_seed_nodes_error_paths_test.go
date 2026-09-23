package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type seedLabelBehaviorEngine struct {
	*storage.MemoryEngine
	errorLabel string
	nilLabel   string
}

func (e *seedLabelBehaviorEngine) GetNodesByLabel(label string) ([]*storage.Node, error) {
	if label == e.errorLabel {
		return nil, fmt.Errorf("forced label error for %s", label)
	}
	nodes, err := e.MemoryEngine.GetNodesByLabel(label)
	if err != nil {
		return nil, err
	}
	if label == e.nilLabel {
		return append([]*storage.Node{nil}, nodes...), nil
	}
	return nodes, nil
}

func (e *seedLabelBehaviorEngine) StreamNodesByLabelProjected(label string, properties []string, visit func(*storage.Node) error) error {
	if label == e.errorLabel {
		return fmt.Errorf("forced label error for %s", label)
	}
	if label == e.nilLabel {
		if err := visit(nil); err != nil {
			return err
		}
	}
	return e.MemoryEngine.StreamNodesByLabelProjected(label, properties, visit)
}

func TestSeedNodesFromOuterMatch_ErrorAndNilBranches(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	eng := &seedLabelBehaviorEngine{MemoryEngine: base, errorLabel: "ErrSeed", nilLabel: "NilSeed"}
	store := storage.NewNamespacedEngine(eng, "seed_nodes_err_paths_cov")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := store.CreateNode(&storage.Node{ID: storage.NodeID("e1"), Labels: []string{"ErrSeed"}, Properties: map[string]interface{}{"id": "e1"}})
	require.NoError(t, err)
	_, err = store.CreateNode(&storage.Node{ID: storage.NodeID("n1"), Labels: []string{"NilSeed"}, Properties: map[string]interface{}{"id": "n1", "team": "red"}})
	require.NoError(t, err)

	_, err = exec.seedNodesFromOuterMatch(ctx, "MATCH (n:ErrSeed)", "n")
	require.Error(t, err)
	require.Contains(t, err.Error(), "forced label error")

	// Property-matching loop should skip nil nodes in the candidate list.
	nodes, err := exec.seedNodesFromOuterMatch(ctx, "MATCH (n:NilSeed {team:'red'})", "n")
	require.NoError(t, err)
	require.Len(t, nodes, 1)
	require.Equal(t, storage.NodeID("n1"), nodes[0].ID)
}

func TestSeedNodesFromOuterMatch_PropertyAndWhereFallbackBranch(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "seed_nodes_prop_where_cov")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE (:Seed {id:'s1', team:'red'}), (:Seed {id:'s2', team:'blue'})", nil)
	require.NoError(t, err)

	// Hits the branch where pattern properties are present and a non-empty WHERE
	// forces fallback execution to preserve full semantics.
	nodes, err := exec.seedNodesFromOuterMatch(ctx, "MATCH (n:Seed {team:'red'}) WHERE n.id = 's1'", "n")
	require.NoError(t, err)
	require.Len(t, nodes, 1)
	require.Equal(t, "s1", nodes[0].Properties["id"])
}
