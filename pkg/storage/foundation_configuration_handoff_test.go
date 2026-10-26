// Author: Lane #1644 (Codex). Configuration identity is fixed at handoff;
// the selected provider remains live and is never copied as graph data.
package storage_test

import (
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestFoundationConfigurationHandoff(t *testing.T) {
	for _, mutate := range []string{"provider", "provider-deleted", "database-name", "access-mode"} {
		t.Run(mutate, func(t *testing.T) {
			original := storage.NewMemoryEngine()
			defer original.Close()
			replacement := storage.NewMemoryEngine()
			defer replacement.Close()
			_, err := original.CreateNode(&storage.Node{ID: "native:n", Properties: map[string]any{"value": "original"}})
			require.NoError(t, err)
			_, err = replacement.CreateNode(&storage.Node{ID: "native:n", Properties: map[string]any{"value": "replacement"}})
			require.NoError(t, err)
			engines := map[string]storage.Engine{"a": original}
			names := map[string]string{"a": "database"}
			modes := map[string]string{"a": "read_write"}
			view := storage.NewCompositeEngine(engines, names, modes)
			switch mutate {
			case "provider":
				engines["a"] = replacement
			case "provider-deleted":
				delete(engines, "a")
			case "database-name":
				names["a"] = "changed"
			case "access-mode":
				modes["a"] = "write"
			}
			got, err := view.GetNode("native:n")
			require.NoError(t, err)
			require.Equal(t, "original", got.Properties["value"])
			require.Equal(t, "database", view.ConstituentDatabaseForNode("native:n"))
			provider, err := view.GetConstituentByAlias("a")
			require.NoError(t, err)
			require.Same(t, original, provider)
			require.NoError(t, original.UpdateNode(&storage.Node{ID: "native:n", Properties: map[string]any{"value": "live"}}))
			got, err = view.GetNode("native:n")
			require.NoError(t, err)
			require.Equal(t, "live", got.Properties["value"])
		})
	}
	t.Run("analyzer-routes", func(t *testing.T) {
		analyzer := storage.NewQueryAnalyzer()
		routes := []string{"a"}
		analyzer.SetLabelRouting("Doc", routes)
		routes[0] = "b"
		require.Equal(t, []string{"a"}, analyzer.RouteQuery(&storage.QueryInfo{Labels: []string{"Doc"}}, []string{"a", "b"}))
		analyzer.SetLabelRouting("Doc", []string{"b"})
		require.Equal(t, []string{"b"}, analyzer.RouteQuery(&storage.QueryInfo{Labels: []string{"Doc"}}, []string{"a", "b"}))
	})
}
