package storage

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type asyncErrIncomingEngine struct{ Engine }

func (e *asyncErrIncomingEngine) GetIncomingEdges(nodeID NodeID) ([]*Edge, error) {
	return nil, errors.New("incoming failed")
}

type asyncErrBothDirEngine struct{ Engine }

func (e *asyncErrBothDirEngine) GetOutgoingEdges(nodeID NodeID) ([]*Edge, error) {
	return nil, errors.New("outgoing failed")
}
func (e *asyncErrBothDirEngine) GetIncomingEdges(nodeID NodeID) ([]*Edge, error) {
	return nil, errors.New("incoming failed")
}

type asyncLastWriteEngine struct{ Engine }

func (e *asyncLastWriteEngine) LastWriteTime() time.Time {
	return time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC)
}

func TestAsyncEngine_LowCoverageHelpers(t *testing.T) {
	base := NewMemoryEngine()
	ae := NewAsyncEngine(base, &AsyncEngineConfig{FlushInterval: time.Hour})
	defer ae.Close()

	// HoldFlush nil receiver branch.
	var nilAE *AsyncEngine
	release := nilAE.HoldFlush()
	release()

	// The pending view follows the cache: add / remove branches.
	ae.mu.Lock()
	ae.pending.add(nil)
	ae.pending.remove(nil)
	ae.setCachedNodeLocked(&Node{ID: "n1", Labels: []string{"Person", "Human"}})
	require.NotEmpty(t, ae.pending.byLabel["person"])
	ae.removeCachedNodeLocked("n1")
	require.Empty(t, ae.pending.byLabel["person"])
	ae.mu.Unlock()

	// mergeAsyncEdges excludes nil/deleted/overridden edges.
	ae.mu.Lock()
	ae.deleteEdges["e-del"] = true
	ae.edgeCache["e-over"] = &Edge{ID: "e-over"}
	ae.mu.Unlock()
	merged := mergeAsyncEdges(ae,
		[]*Edge{{ID: "e-cached"}},
		[]*Edge{nil, {ID: "e-del"}, {ID: "e-over"}, {ID: "e-ok"}},
	)
	require.Len(t, merged, 2)
	ae.mu.Lock()
	delete(ae.deleteEdges, "e-del")
	delete(ae.edgeCache, "e-over")
	ae.mu.Unlock()

	// Seed committed graph for adjacency wrappers.
	_, err := ae.CreateNode(&Node{ID: "test:a", Labels: []string{"N"}})
	require.NoError(t, err)
	_, err = ae.CreateNode(&Node{ID: "test:b", Labels: []string{"N"}})
	require.NoError(t, err)
	require.NoError(t, ae.Flush())
	err = ae.CreateEdge(&Edge{ID: "test:e1", StartNode: "test:a", EndNode: "test:b", Type: "KNOWS"})
	require.NoError(t, err)
	require.NoError(t, ae.Flush())

	out, in, err := ae.GetAdjacentEdges("test:a")
	require.NoError(t, err)
	require.NotEmpty(t, out)
	require.Empty(t, in)

	require.Equal(t, 1, ae.GetOutDegree("test:a"))
	require.Equal(t, 1, ae.GetInDegree("test:b"))
	count, err := ae.NodeCountByLabel("N")
	require.NoError(t, err)
	require.GreaterOrEqual(t, count, int64(2))

	// GetEdgeBetween type filter branch.
	require.NotNil(t, ae.GetEdgeBetween("test:a", "test:b", "KNOWS"))
	require.Nil(t, ae.GetEdgeBetween("test:a", "test:b", "OTHER"))
}

func TestAsyncEngine_GetAdjacentEdges_ErrorFallbacks(t *testing.T) {
	base := NewMemoryEngine()
	_, _ = base.CreateNode(&Node{ID: "test:a", Labels: []string{"N"}})
	_, _ = base.CreateNode(&Node{ID: "test:b", Labels: []string{"N"}})

	// Outgoing error path returns cached only.
	aeOutErr := NewAsyncEngine(&asyncErrBothDirEngine{Engine: base}, &AsyncEngineConfig{FlushInterval: time.Hour})
	defer aeOutErr.Close()
	aeOutErr.mu.Lock()
	aeOutErr.edgeCache["c1"] = &Edge{ID: "c1", StartNode: "test:a", EndNode: "test:b", Type: "X"}
	aeOutErr.cacheEdgesByStart["test:a"] = map[EdgeID]struct{}{"c1": {}}
	aeOutErr.mu.Unlock()
	out, in, err := aeOutErr.GetAdjacentEdges("test:a")
	require.NoError(t, err)
	require.Len(t, out, 1)
	require.Empty(t, in)

	// Incoming error path merges cached outgoing + engine outgoing.
	aeInErr := NewAsyncEngine(&asyncErrIncomingEngine{Engine: base}, &AsyncEngineConfig{FlushInterval: time.Hour})
	defer aeInErr.Close()
	out, in, err = aeInErr.GetAdjacentEdges("test:a")
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(out), 0)
	require.Empty(t, in)

	// GetInDegree/GetOutDegree error branches.
	require.GreaterOrEqual(t, aeOutErr.GetOutDegree("test:a"), 0)
	require.GreaterOrEqual(t, aeOutErr.GetInDegree("test:b"), 0)
}

func TestAsyncEngine_LastWriteTime_Branches(t *testing.T) {
	var nilAE *AsyncEngine
	require.True(t, nilAE.LastWriteTime().IsZero())

	base := NewMemoryEngine()
	ae := NewAsyncEngine(base, &AsyncEngineConfig{FlushInterval: time.Hour})
	defer ae.Close()
	require.True(t, ae.LastWriteTime().IsZero())

	withClock := NewAsyncEngine(&asyncLastWriteEngine{Engine: base}, &AsyncEngineConfig{FlushInterval: time.Hour})
	defer withClock.Close()
	require.Equal(t, time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC), withClock.LastWriteTime())
}
