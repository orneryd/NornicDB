package storage

import (
	"context"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestWriteBehindCountsDuringCommitPublication(t *testing.T) {
	engine := newWriteBehindTestEngine(t, time.Hour)
	type reading struct {
		name  string
		value int64
		err   error
	}
	observed := make(chan []reading, 1)
	engine.OnNodeCreated(func(node *Node) {
		if node.ID != "test:product" {
			return
		}
		checks := []struct {
			name string
			read func() (int64, error)
		}{
			{"label namespace", func() (int64, error) { return engine.NodeCountByLabelInNamespace(t.Context(), "test", "Product") }},
			{"label all namespaces", func() (int64, error) { return engine.NodeCountByLabel(t.Context(), "Product") }},
			{"type namespace", func() (int64, error) {
				return engine.EdgeCountByTypeInNamespace(context.Background(), "test", "PART_OF")
			}},
			{"type all namespaces", func() (int64, error) { return engine.EdgeCountByType(context.Background(), "PART_OF") }},
			{"start namespace", func() (int64, error) {
				return engine.EdgeCountByStartLabelInNamespace(context.Background(), "test", "Product", "PART_OF")
			}},
			{"end namespace", func() (int64, error) {
				return engine.EdgeCountByEndLabelInNamespace(context.Background(), "test", "Category", "PART_OF")
			}},
			{"start all namespaces", func() (int64, error) { return engine.EdgeCountByStartLabel(context.Background(), "Product", "PART_OF") }},
			{"end all namespaces", func() (int64, error) { return engine.EdgeCountByEndLabel(context.Background(), "Category", "PART_OF") }},
		}
		var readings []reading
		for _, check := range checks {
			value, err := check.read()
			readings = append(readings, reading{check.name, value, err})
		}
		observed <- readings
	})
	tx, err := engine.BeginTransaction(t.Context())
	require.NoError(t, err)
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, tx.SetNamespace("test"))
	_, err = tx.CreateNode(t.Context(), &Node{ID: "test:product", Labels: []string{"Product"}})
	require.NoError(t, err)
	_, err = tx.CreateNode(t.Context(), &Node{ID: "test:category", Labels: []string{"Category"}})
	require.NoError(t, err)
	require.NoError(t, tx.CreateEdge(t.Context(), &Edge{ID: "test:part", StartNode: "test:product", EndNode: "test:category", Type: "PART_OF"}))
	require.NoError(t, tx.Commit())
	require.NoError(t, engine.FlushWriteBehind(context.Background()))
	var readings []reading
	select {
	case readings = <-observed:
	default:
		t.Fatal("missing committed node callback")
	}
	for _, got := range readings {
		require.NoError(t, got.err, got.name)
		require.Equal(t, int64(1), got.value, got.name)
	}
}
