package storage

import (
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/stretchr/testify/require"
)

func requireCompositeLocalizedError(t *testing.T, err error, messageID localization.MessageID, text string, cause error) *localization.LocalizedError {
	t.Helper()

	require.EqualError(t, err, text)
	var localizedErr *localization.LocalizedError
	require.ErrorAs(t, err, &localizedErr)
	require.Equal(t, messageID, localizedErr.Message.ID)
	require.Equal(t, string(messageID), localizedErr.Code)
	if cause != nil {
		require.ErrorIs(t, err, cause)
	}
	return localizedErr
}

func TestCompositeEngineLocalizedRoutingErrors(t *testing.T) {
	t.Run("missing constituent preserves alias", func(t *testing.T) {
		composite := NewCompositeEngine(nil, nil, nil)

		_, err := composite.GetConstituentByAlias("tenant-a")
		localizedErr := requireCompositeLocalizedError(t, err, localization.MessageStorageCompositeConstituentNotFound, "constituent 'tenant-a' not found", nil)
		require.Equal(t, "tenant-a", localizedErr.Message.Data["Alias"])
	})

	t.Run("write validation has typed identity", func(t *testing.T) {
		composite := NewCompositeEngine(nil, nil, nil)

		_, err := composite.CreateNode(&Node{})
		requireCompositeLocalizedError(t, err, localization.MessageStorageCompositeNoWritableConstituents, "no writable constituents available", nil)
	})
}

func TestCompositeEngineLocalizedQueryWrappersPreserveCause(t *testing.T) {
	testCases := []struct {
		name string
		run  func(*compositeErrorEngine, *CompositeEngine) error
	}{
		{name: "outgoing edges", run: func(engine *compositeErrorEngine, composite *CompositeEngine) error {
			engine.outgoingErr = ErrConflict
			_, err := composite.GetOutgoingEdges("node-1")
			return err
		}},
		{name: "incoming edges", run: func(engine *compositeErrorEngine, composite *CompositeEngine) error {
			engine.incomingErr = ErrConflict
			_, err := composite.GetIncomingEdges("node-1")
			return err
		}},
		{name: "edges between", run: func(engine *compositeErrorEngine, composite *CompositeEngine) error {
			engine.betweenErr = ErrConflict
			_, err := composite.GetEdgesBetween("node-1", "node-2")
			return err
		}},
		{name: "edges by type", run: func(engine *compositeErrorEngine, composite *CompositeEngine) error {
			engine.byTypeErr = ErrConflict
			_, err := composite.GetEdgesByType("REL")
			return err
		}},
		{name: "all nodes", run: func(engine *compositeErrorEngine, composite *CompositeEngine) error {
			engine.allNodesErr = ErrConflict
			_, err := composite.AllNodes()
			return err
		}},
		{name: "all edges", run: func(engine *compositeErrorEngine, composite *CompositeEngine) error {
			engine.allEdgesErr = ErrConflict
			_, err := composite.AllEdges()
			return err
		}},
		{name: "batch nodes", run: func(engine *compositeErrorEngine, composite *CompositeEngine) error {
			engine.batchGetErr = ErrConflict
			_, err := composite.BatchGetNodes([]NodeID{"node-1"})
			return err
		}},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			engine := &compositeErrorEngine{MemoryEngine: NewMemoryEngine()}
			t.Cleanup(func() { _ = engine.MemoryEngine.Close() })
			composite := NewCompositeEngine(
				map[string]Engine{"tenant-a": engine},
				map[string]string{"tenant-a": "database-a"},
				map[string]string{"tenant-a": "read_write"},
			)

			err := testCase.run(engine, composite)
			localizedErr := requireCompositeLocalizedError(t, err, localization.MessageStorageCompositeConstituentQueryFailed, "error querying constituent 'tenant-a': conflict", ErrConflict)
			require.Equal(t, "tenant-a", localizedErr.Message.Data["Alias"])
		})
	}
}

func TestCompositeEngineLocalizedBulkWrapperPreservesCause(t *testing.T) {
	cause := errors.New("bulk write failed")
	engine := &compositeErrorEngine{MemoryEngine: NewMemoryEngine(), bulkCreateNodesErr: cause}
	t.Cleanup(func() { _ = engine.MemoryEngine.Close() })
	composite := NewCompositeEngine(
		map[string]Engine{"tenant-a": engine},
		map[string]string{"tenant-a": "database-a"},
		map[string]string{"tenant-a": "read_write"},
	)

	err := composite.BulkCreateNodes([]*Node{{ID: "node-1"}})
	localizedErr := requireCompositeLocalizedError(t, err, localization.MessageStorageCompositeNodeBulkCreateFailed, "failed to create nodes in constituent 'tenant-a': bulk write failed", cause)
	require.Equal(t, "tenant-a", localizedErr.Message.Data["Alias"])
}
