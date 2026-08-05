package cypher

import (
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type relationshipMergeCreateEngine struct {
	storage.Engine
	createEdge      func(*storage.Edge) error
	getEdge         func(storage.EdgeID) (*storage.Edge, error)
	getEdgeBetween  func(storage.NodeID, storage.NodeID, string) *storage.Edge
	getEdgesBetween func(storage.NodeID, storage.NodeID) ([]*storage.Edge, error)
}

func (e *relationshipMergeCreateEngine) CreateEdge(edge *storage.Edge) error {
	return e.createEdge(edge)
}

func (e *relationshipMergeCreateEngine) GetEdge(id storage.EdgeID) (*storage.Edge, error) {
	return e.getEdge(id)
}

func (e *relationshipMergeCreateEngine) GetEdgeBetween(
	startID storage.NodeID,
	endID storage.NodeID,
	relType string,
) *storage.Edge {
	return e.getEdgeBetween(startID, endID, relType)
}

func (e *relationshipMergeCreateEngine) GetEdgesBetween(
	startID storage.NodeID,
	endID storage.NodeID,
) ([]*storage.Edge, error) {
	return e.getEdgesBetween(startID, endID)
}

func TestCreateRelationshipForMergeBareRetriesIDCollisions(t *testing.T) {
	exec := &StorageExecutor{}
	edge := relationshipMergeTestEdge("initial", nil)
	attempts := 0
	store := &relationshipMergeCreateEngine{
		createEdge: func(*storage.Edge) error {
			attempts++
			if attempts < 3 {
				return storage.ErrAlreadyExists
			}
			return nil
		},
		getEdgeBetween:  func(storage.NodeID, storage.NodeID, string) *storage.Edge { return nil },
		getEdgesBetween: unexpectedRelationshipMergePairScan(t),
		getEdge:         unexpectedRelationshipMergeEdgeRead(t),
	}

	created, didCreate, err := createRelationshipForMerge(exec, store, edge, nil)
	require.NoError(t, err)
	require.True(t, didCreate)
	require.Same(t, edge, created)
	require.Equal(t, 3, attempts)
	require.NotEqual(t, storage.EdgeID("initial"), edge.ID)
}

func TestCreateRelationshipForMergeBareReturnsExistingAndErrors(t *testing.T) {
	existing := relationshipMergeTestEdge("existing", nil)
	t.Run("existing", func(t *testing.T) {
		store := &relationshipMergeCreateEngine{
			createEdge: func(*storage.Edge) error { return storage.ErrAlreadyExists },
			getEdgeBetween: func(storage.NodeID, storage.NodeID, string) *storage.Edge {
				return existing
			},
			getEdgesBetween: unexpectedRelationshipMergePairScan(t),
			getEdge:         unexpectedRelationshipMergeEdgeRead(t),
		}
		got, created, err := createRelationshipForMerge(&StorageExecutor{}, store, relationshipMergeTestEdge("new", nil), nil)
		require.NoError(t, err)
		require.False(t, created)
		require.Same(t, existing, got)
	})

	t.Run("create error", func(t *testing.T) {
		wantErr := errors.New("create failed")
		store := &relationshipMergeCreateEngine{
			createEdge:      func(*storage.Edge) error { return wantErr },
			getEdgeBetween:  func(storage.NodeID, storage.NodeID, string) *storage.Edge { return nil },
			getEdgesBetween: unexpectedRelationshipMergePairScan(t),
			getEdge:         unexpectedRelationshipMergeEdgeRead(t),
		}
		got, created, err := createRelationshipForMerge(&StorageExecutor{}, store, relationshipMergeTestEdge("new", nil), nil)
		require.Nil(t, got)
		require.False(t, created)
		require.ErrorIs(t, err, wantErr)
	})

	t.Run("collision limit", func(t *testing.T) {
		store := &relationshipMergeCreateEngine{
			createEdge:      func(*storage.Edge) error { return storage.ErrAlreadyExists },
			getEdgeBetween:  func(storage.NodeID, storage.NodeID, string) *storage.Edge { return nil },
			getEdgesBetween: unexpectedRelationshipMergePairScan(t),
			getEdge:         unexpectedRelationshipMergeEdgeRead(t),
		}
		got, created, err := createRelationshipForMerge(&StorageExecutor{}, store, relationshipMergeTestEdge("new", nil), nil)
		require.Nil(t, got)
		require.False(t, created)
		require.ErrorContains(t, err, "after 3 edge ID collisions")
	})
}

func TestCreateRelationshipForMergePropertyRaceReturnsWinner(t *testing.T) {
	matchProps := map[string]interface{}{"scope_id": "scope-a"}
	edge := relationshipMergeTestEdge("candidate", matchProps)
	existing := relationshipMergeTestEdge("candidate", matchProps)
	reads := 0
	store := &relationshipMergeCreateEngine{
		createEdge: func(*storage.Edge) error { return storage.ErrAlreadyExists },
		getEdge: func(storage.EdgeID) (*storage.Edge, error) {
			reads++
			if reads == 1 {
				return nil, storage.ErrNotFound
			}
			return existing, nil
		},
		getEdgeBetween: unexpectedRelationshipMergePointLookup(t),
		getEdgesBetween: func(storage.NodeID, storage.NodeID) ([]*storage.Edge, error) {
			return nil, nil
		},
	}

	got, created, err := createRelationshipForMerge(&StorageExecutor{}, store, edge, matchProps)
	require.NoError(t, err)
	require.False(t, created)
	require.Same(t, existing, got)
}

func TestCreateRelationshipForMergePropertyPropagatesLookupErrors(t *testing.T) {
	matchProps := map[string]interface{}{"scope_id": "scope-a"}
	wantErr := errors.New("lookup failed")

	t.Run("pre-create selection", func(t *testing.T) {
		store := relationshipMergeSelectEngine(t, func(storage.EdgeID) (*storage.Edge, error) {
			return nil, wantErr
		}, unexpectedRelationshipMergePairScan(t))
		got, created, err := createRelationshipForMerge(
			&StorageExecutor{},
			store,
			relationshipMergeTestEdge("candidate", matchProps),
			matchProps,
		)
		require.Nil(t, got)
		require.False(t, created)
		require.ErrorIs(t, err, wantErr)
	})

	t.Run("post-collision scan", func(t *testing.T) {
		store := &relationshipMergeCreateEngine{
			createEdge: func(*storage.Edge) error { return storage.ErrAlreadyExists },
			getEdge: func(storage.EdgeID) (*storage.Edge, error) {
				return nil, storage.ErrNotFound
			},
			getEdgeBetween: unexpectedRelationshipMergePointLookup(t),
			getEdgesBetween: func(storage.NodeID, storage.NodeID) ([]*storage.Edge, error) {
				return nil, wantErr
			},
		}
		got, created, err := createRelationshipForMerge(
			&StorageExecutor{},
			store,
			relationshipMergeTestEdge("candidate", matchProps),
			matchProps,
		)
		require.Nil(t, got)
		require.False(t, created)
		require.ErrorIs(t, err, wantErr)
	})
}

func TestSelectRelationshipMergeCreateIDBranches(t *testing.T) {
	matchProps := map[string]interface{}{"scope_id": "scope-a"}
	exact := relationshipMergeTestEdge("candidate", matchProps)
	stale := relationshipMergeTestEdge("candidate", map[string]interface{}{"scope_id": "retired"})

	t.Run("bare", func(t *testing.T) {
		got, err := selectRelationshipMergeCreateID(nil, relationshipMergeTestEdge("candidate", nil), nil)
		require.NoError(t, err)
		require.Nil(t, got)
	})

	t.Run("exact candidate", func(t *testing.T) {
		store := relationshipMergeSelectEngine(t, func(storage.EdgeID) (*storage.Edge, error) {
			return exact, nil
		}, unexpectedRelationshipMergePairScan(t))
		got, err := selectRelationshipMergeCreateID(store, relationshipMergeTestEdge("candidate", matchProps), matchProps)
		require.NoError(t, err)
		require.Same(t, exact, got)
	})

	t.Run("point error", func(t *testing.T) {
		wantErr := errors.New("point read failed")
		store := relationshipMergeSelectEngine(t, func(storage.EdgeID) (*storage.Edge, error) {
			return nil, wantErr
		}, unexpectedRelationshipMergePairScan(t))
		got, err := selectRelationshipMergeCreateID(store, relationshipMergeTestEdge("candidate", matchProps), matchProps)
		require.Nil(t, got)
		require.ErrorIs(t, err, wantErr)
	})

	t.Run("pair scan error", func(t *testing.T) {
		wantErr := errors.New("pair scan failed")
		store := relationshipMergeSelectEngine(t, func(storage.EdgeID) (*storage.Edge, error) {
			return stale, nil
		}, func(storage.NodeID, storage.NodeID) ([]*storage.Edge, error) {
			return nil, wantErr
		})
		got, err := selectRelationshipMergeCreateID(store, relationshipMergeTestEdge("candidate", matchProps), matchProps)
		require.Nil(t, got)
		require.ErrorIs(t, err, wantErr)
	})

	t.Run("selects free ordinal", func(t *testing.T) {
		reads := 0
		store := relationshipMergeSelectEngine(t, func(storage.EdgeID) (*storage.Edge, error) {
			reads++
			if reads == 1 {
				return stale, nil
			}
			return nil, storage.ErrNotFound
		}, func(storage.NodeID, storage.NodeID) ([]*storage.Edge, error) {
			return []*storage.Edge{stale}, nil
		})
		edge := relationshipMergeTestEdge("candidate", matchProps)
		got, err := selectRelationshipMergeCreateID(store, edge, matchProps)
		require.NoError(t, err)
		require.Nil(t, got)
		require.Equal(t, deterministicRelationshipMergeEdgeID("source", "target", "ASSERTS", matchProps, 1), edge.ID)
	})

	t.Run("candidate read error", func(t *testing.T) {
		wantErr := errors.New("candidate read failed")
		reads := 0
		store := relationshipMergeSelectEngine(t, func(storage.EdgeID) (*storage.Edge, error) {
			reads++
			if reads == 1 {
				return stale, nil
			}
			return nil, wantErr
		}, func(storage.NodeID, storage.NodeID) ([]*storage.Edge, error) {
			return []*storage.Edge{stale}, nil
		})
		got, err := selectRelationshipMergeCreateID(store, relationshipMergeTestEdge("candidate", matchProps), matchProps)
		require.Nil(t, got)
		require.ErrorIs(t, err, wantErr)
	})

	t.Run("finds exact collision ordinal", func(t *testing.T) {
		reads := 0
		store := relationshipMergeSelectEngine(t, func(storage.EdgeID) (*storage.Edge, error) {
			reads++
			if reads == 1 {
				return stale, nil
			}
			return exact, nil
		}, func(storage.NodeID, storage.NodeID) ([]*storage.Edge, error) {
			return []*storage.Edge{stale}, nil
		})
		got, err := selectRelationshipMergeCreateID(store, relationshipMergeTestEdge("candidate", matchProps), matchProps)
		require.NoError(t, err)
		require.Same(t, exact, got)
	})

	t.Run("exhausted", func(t *testing.T) {
		store := relationshipMergeSelectEngine(t, func(storage.EdgeID) (*storage.Edge, error) {
			return stale, nil
		}, func(storage.NodeID, storage.NodeID) ([]*storage.Edge, error) {
			return []*storage.Edge{stale}, nil
		})
		got, err := selectRelationshipMergeCreateID(store, relationshipMergeTestEdge("candidate", matchProps), matchProps)
		require.Nil(t, got)
		require.ErrorContains(t, err, "no free storage key")
	})
}

func relationshipMergeSelectEngine(
	t *testing.T,
	getEdge func(storage.EdgeID) (*storage.Edge, error),
	getEdgesBetween func(storage.NodeID, storage.NodeID) ([]*storage.Edge, error),
) *relationshipMergeCreateEngine {
	t.Helper()
	return &relationshipMergeCreateEngine{
		createEdge:      func(*storage.Edge) error { t.Fatal("unexpected create"); return nil },
		getEdge:         getEdge,
		getEdgeBetween:  unexpectedRelationshipMergePointLookup(t),
		getEdgesBetween: getEdgesBetween,
	}
}

func relationshipMergeTestEdge(id storage.EdgeID, props map[string]interface{}) *storage.Edge {
	return &storage.Edge{
		ID:         id,
		Type:       "ASSERTS",
		StartNode:  "source",
		EndNode:    "target",
		Properties: props,
	}
}

func unexpectedRelationshipMergeEdgeRead(t *testing.T) func(storage.EdgeID) (*storage.Edge, error) {
	t.Helper()
	return func(storage.EdgeID) (*storage.Edge, error) {
		t.Fatal("unexpected edge read")
		return nil, nil
	}
}

func unexpectedRelationshipMergePointLookup(
	t *testing.T,
) func(storage.NodeID, storage.NodeID, string) *storage.Edge {
	t.Helper()
	return func(storage.NodeID, storage.NodeID, string) *storage.Edge {
		t.Fatal("unexpected point lookup")
		return nil
	}
}

func unexpectedRelationshipMergePairScan(
	t *testing.T,
) func(storage.NodeID, storage.NodeID) ([]*storage.Edge, error) {
	t.Helper()
	return func(storage.NodeID, storage.NodeID) ([]*storage.Edge, error) {
		t.Fatal("unexpected pair scan")
		return nil, nil
	}
}
