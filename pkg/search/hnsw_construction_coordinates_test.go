package search

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHNSWConstructionCoordinatesPinFileGenerationUntilPublication(t *testing.T) {
	for _, mapped := range []bool{true, false} {
		t.Run(map[bool]string{true: "mapped", false: "read-at"}[mapped], func(t *testing.T) {
			servingTestEnv(t)
			t.Setenv("NORNICDB_VECTOR_VFS_COMPACT_MIN_OBSOLETE", "1")
			t.Setenv("NORNICDB_VECTOR_VFS_COMPACT_MIN_SIZE_MB", "0")
			t.Setenv("NORNICDB_VECTOR_VFS_COMPACT_DEAD_RATIO", "0")
			file, err := NewVectorFileStore(filepath.Join(t.TempDir(), "vectors"), 3)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, file.Close()) })
			require.NoError(t, file.Add("a", []float32{1, 0, 0}))
			require.NoError(t, file.Add("b", []float32{0, 1, 0}))
			if !mapped {
				file.mapFile = func(*os.File, int) ([]byte, func() error, error) { return nil, nil, errors.New("mapping unavailable") }
			}
			svc := NewService(newNamespacedEngine(t), 3, nil)
			t.Cleanup(func() { require.NoError(t, svc.Close()) })
			svc.mu.Lock()
			svc.vectorFileStore = file
			svc.mu.Unlock()
			graph, _, err := svc.buildHNSWSnapshot(t.Context(), 3, svc.hnswConfig(), nil, file, nil)
			require.NoError(t, err)
			coordinates := graph.construction
			require.NotNil(t, coordinates)
			t.Cleanup(func() { require.NoError(t, coordinates.Close()) })
			var closes int
			closeSource := coordinates.release
			coordinates.release = func() error { closes++; return closeSource() }
			read := func(id string) ([]float32, bool) {
				graph.mu.RLock()
				defer graph.mu.RUnlock()
				v, ok := graph.vectorLookup(id)
				return append([]float32(nil), v...), ok
			}
			require.NoError(t, file.Add("a", []float32{0, 0, 1}))
			require.True(t, file.Remove("b"))
			require.NoError(t, file.Add("c", []float32{0, 1, 0}))
			compacted, err := file.CompactIfNeeded()
			require.NoError(t, err)
			require.True(t, compacted)
			v, ok := read("a")
			require.True(t, ok)
			require.Equal(t, []float32{1, 0, 0}, v)
			v, ok = read("b")
			require.True(t, ok)
			require.Equal(t, []float32{0, 1, 0}, v)
			_, ok = read("c")
			require.False(t, ok)
			// Applied graph changes advance coordinates, not canonical admission.
			nextA, ok := file.GetVector("a")
			require.True(t, ok)
			require.NoError(t, applyCatchUpChange(graph, "a", nextA))
			require.Same(t, &nextA[0], &coordinates.applied["a"][0], "replay retains the existing immutable record, not another float payload")
			graph.Remove("b")
			require.NoError(t, graph.Add("c", []float32{0, 1, 0}))
			for i := 0; i < 128; i++ {
				require.NoError(t, graph.Add("churn", []float32{1, 0, 0}))
				graph.Remove("churn")
			}
			require.Len(t, coordinates.applied, 2, "retire transient keys instead of retaining historical churn")
			v, ok = read("a")
			require.True(t, ok)
			require.Equal(t, []float32{0, 0, 1}, v)
			coordinates.publish(svc.getVectorLookup())
			require.NoError(t, coordinates.Close())
			require.NoError(t, coordinates.Close())
			require.Equal(t, 1, closes)
			require.Nil(t, graph.construction)
			require.Nil(t, coordinates.baseline)
			require.Nil(t, coordinates.applied)
			v, ok = read("a")
			require.True(t, ok)
			require.Equal(t, []float32{0, 0, 1}, v)
			_, ok = read("b")
			require.False(t, ok)
		})
	}
}

func TestHNSWConstructionCoordinateCloseDoesNotReleaseActiveLookup(t *testing.T) {
	graph := newHNSWFixture(t, 2, 8, "a", []hnswFixtureNode{{id: "a", vec: []float32{1, 0}, level: 0}})
	entered, resume := make(chan struct{}), make(chan struct{})
	var once sync.Once
	releaseLookup := func() { once.Do(func() { close(resume) }) }
	t.Cleanup(releaseLookup)
	var active, early atomic.Bool
	var released atomic.Int32
	coordinates := &hnswConstructionCoordinates{graph: graph}
	coordinates.baseline = func(string) ([]float32, bool) {
		active.Store(true)
		close(entered)
		<-resume
		active.Store(false)
		return []float32{1, 0}, true
	}
	coordinates.release = func() error {
		if active.Load() {
			early.Store(true)
		}
		released.Add(1)
		return nil
	}
	graph.construction = coordinates
	graph.SetVectorLookup(coordinates.lookup)
	graph.mu.Lock()
	graph.vecOff[0] = -1
	graph.vectors = nil
	graph.mu.Unlock()
	readDone, closeDone := make(chan struct{}), make(chan error, 1)
	go func() { graph.mu.RLock(); _ = graph.vectorAtLocked(0); graph.mu.RUnlock(); close(readDone) }()
	awaitEvent(t, "active coordinate lookup", entered)
	started := make(chan struct{})
	go func() { close(started); closeDone <- coordinates.Close() }()
	awaitEvent(t, "coordinate close started", started)
	releaseLookup()
	awaitEvent(t, "coordinate lookup finished", readDone)
	require.NoError(t, awaitEvent(t, "coordinate close finished", closeDone))
	require.False(t, early.Load(), "a retired construction must not unmap a vector still being consumed")
	require.EqualValues(t, 1, released.Load())
	require.Nil(t, graph.vectorLookup, "late work on the discarded graph cannot reenter the released source")
	require.NoError(t, graph.Add("late", []float32{0, 1}), "a detached payer can finish against the discarded graph without touching the released mapping")
	require.NoError(t, coordinates.Close())
	require.EqualValues(t, 1, released.Load())
}
