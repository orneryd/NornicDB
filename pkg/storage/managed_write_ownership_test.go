package storage

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestManagedStagingOwnsCallerUpdateBodies(t *testing.T) {
	for _, buffered := range []bool{false, true} {
		for _, entity := range []string{"node", "edge"} {
			t.Run(fmt.Sprintf("buffered=%v/%s", buffered, entity), func(t *testing.T) {
				base, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir(), LowMemory: true, WriteBehind: buffered, WriteBehindInterval: time.Hour})
				require.NoError(t, err)
				defer base.Close()
				for _, id := range []NodeID{"owned:a", "owned:b", "owned:n"} {
					_, err := base.CreateNode(&Node{ID: id, Labels: []string{"Doc"}})
					require.NoError(t, err)
				}
				require.NoError(t, base.CreateEdge(&Edge{ID: "owned:e", StartNode: "owned:a", EndNode: "owned:b", Type: "LINK"}))
				properties := map[string]any{"payload": "admitted", "nested": []any{"admitted"}}
				labels := []string{"Doc"}
				op := Operation{Type: OpUpdateNode, Node: &Node{ID: "n", Labels: labels, Properties: properties}}
				if entity == "edge" {
					op = Operation{Type: OpUpdateEdge, Edge: &Edge{ID: "e", StartNode: "a", EndNode: "b", Type: "LINK", Properties: properties}}
				}
				tx := NewManagedWrite(base, "owned", op)
				defer tx.Rollback()
				require.NoError(t, tx.PrepareChanges())
				require.NoError(t, tx.SetImplicit(buffered))
				properties["payload"] = "caller mutation"
				properties["nested"] = []any{"caller mutation"}
				labels[0] = "CallerMutation"
				require.NoError(t, tx.CommitContextWithPublication(context.Background(), func() {}))
				view := NewNamespacedEngine(base, "owned")
				var stored map[string]any
				if entity == "node" {
					node, err := GetStoredNode(view, "n")
					require.NoError(t, err)
					require.Equal(t, []string{"Doc"}, node.Labels)
					stored = node.Properties
				} else {
					edge, err := GetStoredEdge(view, "e")
					require.NoError(t, err)
					stored = edge.Properties
				}
				require.Equal(t, "admitted", stored["payload"])
				require.Equal(t, "[admitted]", fmt.Sprint(stored["nested"]))
			})
		}
	}
}
