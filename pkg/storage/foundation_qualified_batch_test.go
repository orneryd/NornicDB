package storage_test

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Author: Lane #1644 (Codex). Qualified batch requests have the same required
// constituent population as the public point operation; admission is live.
func TestFoundationQualifiedBatchAdmission(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	for _, namespace := range []string{"tenant", "foreign"} {
		_, err := base.CreateNode(&storage.Node{ID: storage.NodeID(namespace + ":n"), Labels: []string{namespace}})
		require.NoError(t, err)
	}
	denied := errors.New("unavailable batch constituent")
	var opened []string
	rejectTarget := false
	composite := storage.NewCompositeEngine(map[string]storage.Engine{"a-denied": storage.NewNamespacedEngine(base, "foreign"), "z-selected": storage.NewNamespacedEngine(base, "tenant")}, map[string]string{"a-denied": "foreign", "z-selected": "tenant"}, map[string]string{"a-denied": "read", "z-selected": "read"}).WithConstituentAccess(func(database string, engine storage.Engine, _ bool) (storage.Engine, error) {
		opened = append(opened, database)
		if database == "foreign" || rejectTarget {
			return nil, denied
		}
		return engine, nil
	})
	id := storage.NodeID(storage.SourceNodeElementID("tenant", "n"))
	_, err := composite.GetNode(id)
	require.NoError(t, err, "qualified point-read setup control")
	for _, contextual := range []bool{false, true} {
		t.Run(map[bool]string{false: "core", true: "context"}[contextual], func(t *testing.T) {
			read := func(ids []storage.NodeID) (map[storage.NodeID]*storage.Node, error) {
				if contextual {
					return storage.BatchGetNodesWithContext(context.Background(), composite, ids)
				}
				return composite.BatchGetNodes(ids)
			}
			rejectTarget = false
			opened = nil
			got, err := read([]storage.NodeID{id})
			require.NoError(t, err)
			require.Contains(t, got, id)
			assert.Equal(t, []string{"tenant"}, opened, "unrequested peer must not be admitted")
			opened = nil
			got, err = read([]storage.NodeID{"n"})
			assert.ErrorIs(t, err, denied, "an unqualified batch requires the denied peer and cannot silently omit it")
			assert.Empty(t, got)
			_, pointErr := composite.GetNode("n")
			assert.ErrorIs(t, pointErr, denied, "the same source resolver preserves point admission failure")
			rejectTarget = true
			opened = nil
			got, err = read([]storage.NodeID{id})
			assert.ErrorIs(t, err, denied, "a required target refusal is not node absence")
			assert.Empty(t, got)
			assert.Equal(t, []string{"tenant"}, opened)
			_, pointErr = composite.GetNode(id)
			assert.ErrorIs(t, pointErr, denied, "point and batch share selected-source refusal")
		})
	}
}
