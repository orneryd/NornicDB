package search

import (
	"context"
	"fmt"
	"math"
	"path/filepath"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestCompressedModesKeepWritesMadeDuringBuild runs the vector lifecycle under
// every HNSW compression mode and both ANN qualities: an update, an insert and
// a delete made while an explicit build traverses storage wait for that build
// (#1372's buildMu model) and are what searches return afterwards, after a
// restart from the saved indexes, and after the database is dropped and
// recreated on the same service.
func TestCompressedModesKeepWritesMadeDuringBuild(t *testing.T) {
	for _, quality := range []string{"fast", "compressed"} {
		for _, mode := range []string{"none", "scalar", "pq", "binary"} {
			t.Run(quality+"/"+mode, func(t *testing.T) {
				t.Setenv("NORNICDB_GPU_BACKEND", "cpu")
				t.Setenv("NORNICDB_HNSW_BUILD_GPU_ENABLED", "false")
				t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "0")
				t.Setenv("NORNICDB_VECTOR_ANN_QUALITY", quality)
				t.Setenv("NORNICDB_VECTOR_HNSW_QUANTIZATION", mode)
				runCompressedWriteDuringBuild(t)
			})
		}
	}
}

const compressedLifecyclePopulation = 300

// compressedLifecycleNode places node i on the unit circle of the xy-plane;
// the written nodes use the z axis, which no population node shares.
func compressedLifecycleNode(id string, vec []float32) *storage.Node {
	// Text keeps the saved BM25 checkpoint non-empty: develop rejects an empty one.
	return &storage.Node{ID: storage.NodeID(id), Properties: map[string]any{"text": "lifecycle " + id}, NamedEmbeddings: map[string][]float32{"default": vec}}
}

func compressedLifecyclePopulate(t *testing.T, engine storage.Engine) {
	t.Helper()
	for i := 0; i < compressedLifecyclePopulation; i++ {
		angle := float64(i) * 2 * math.Pi / compressedLifecyclePopulation
		_, err := engine.CreateNode(compressedLifecycleNode(fmt.Sprint(i), []float32{float32(math.Cos(angle)), float32(math.Sin(angle)), 0}))
		require.NoError(t, err)
	}
}

func compressedLifecycleTop(t *testing.T, svc *Service, query []float32) (string, float64) {
	t.Helper()
	hits, err := svc.VectorQueryNodesWithOptions(context.Background(), query,
		VectorQuerySpec{Property: "default", Limit: 1, ScoreScale: VectorScoreSimilarity},
		&SearchOptions{HNSWEf: 512, QuantizationOversampling: compressedLifecyclePopulation})
	require.NoError(t, err)
	require.Len(t, hits, 1)
	return hits[0].ID, hits[0].Score
}

// requireCompressedLifecycleWrites checks the three writes: "0" moved to +z,
// "new" was inserted at -z, and "1" was deleted.
func requireCompressedLifecycleWrites(t *testing.T, svc *Service) {
	t.Helper()
	id, score := compressedLifecycleTop(t, svc, []float32{0, 0, 1})
	require.Equal(t, "0", id, "update made during the build")
	require.InDelta(t, 1, score, 1e-6)
	id, score = compressedLifecycleTop(t, svc, []float32{0, 0, -1})
	require.Equal(t, "new", id, "insert made during the build")
	require.InDelta(t, 1, score, 1e-6)
	angle := 2 * math.Pi / compressedLifecyclePopulation
	id, _ = compressedLifecycleTop(t, svc, []float32{float32(math.Cos(angle)), float32(math.Sin(angle)), 0})
	require.NotEqual(t, "1", id, "delete made during the build")
}

func compressedLifecycleService(engine storage.Engine, dir string) *Service {
	svc := NewServiceWithDimensions(engine, 3)
	svc.SetFulltextIndexPath(filepath.Join(dir, "bm25"))
	svc.SetVectorIndexPath(filepath.Join(dir, "vectors"))
	svc.SetHNSWIndexPath(filepath.Join(dir, "hnsw"))
	svc.SetPersistenceEnabled(true)
	return svc
}

func waitCompressedLifecycle(t *testing.T, done <-chan error, what string) {
	t.Helper()
	select {
	case err := <-done:
		require.NoError(t, err, what)
	case <-time.After(2 * time.Minute):
		t.Fatalf("%s did not finish", what)
	}
}

func runCompressedWriteDuringBuild(t *testing.T) {
	base := newNamespacedEngine(t)
	compressedLifecyclePopulate(t, base)
	dir := t.TempDir()
	engine := &pausedPopulationEngine{Engine: base, captured: make(chan struct{}), resume: make(chan struct{})}
	svc := compressedLifecycleService(engine, dir)

	buildDone := make(chan error, 1)
	go func() { buildDone <- svc.BuildIndexes(context.Background()) }()
	<-engine.captured

	// Storage commits first, as a transaction does; the index writes follow.
	moved := compressedLifecycleNode("0", []float32{0, 0, 1})
	require.NoError(t, base.UpdateNode(moved))
	inserted := compressedLifecycleNode("new", []float32{0, 0, -1})
	_, err := base.CreateNode(inserted)
	require.NoError(t, err)
	require.NoError(t, base.DeleteNode("1"))
	writesDone := make(chan error, 1)
	go func() {
		if err := svc.IndexNode(moved); err != nil {
			writesDone <- err
			return
		}
		if err := svc.IndexNode(inserted); err != nil {
			writesDone <- err
			return
		}
		writesDone <- svc.RemoveNode("1")
	}()

	close(engine.resume)
	waitCompressedLifecycle(t, buildDone, "build")
	waitCompressedLifecycle(t, writesDone, "writes made during the build")
	requireCompressedLifecycleWrites(t, svc)

	t.Run("restart", func(t *testing.T) {
		svc.PersistIndexesToDisk()
		require.NoError(t, svc.Close())
		restarted := compressedLifecycleService(base, dir)
		t.Cleanup(func() { require.NoError(t, restarted.Close()) })
		require.NoError(t, restarted.BuildIndexes(context.Background()))
		requireCompressedLifecycleWrites(t, restarted)

		t.Run("drop and recreate", func(t *testing.T) {
			nodes, err := base.AllNodes()
			require.NoError(t, err)
			for _, n := range nodes {
				require.NoError(t, base.DeleteNode(n.ID))
			}
			require.NoError(t, restarted.ClearIndexes())
			// The recreated database's writes reach the index as live writes do.
			compressedLifecyclePopulate(t, base)
			nodes, err = base.AllNodes()
			require.NoError(t, err)
			for _, n := range nodes {
				require.NoError(t, restarted.IndexNode(n))
			}
			id, score := compressedLifecycleTop(t, restarted, []float32{0, 0, -1})
			require.NotEqual(t, "new", id, "the dropped database's insert")
			require.Less(t, score, 0.5)
			angle := 2 * math.Pi / compressedLifecyclePopulation
			id, score = compressedLifecycleTop(t, restarted, []float32{float32(math.Cos(angle)), float32(math.Sin(angle)), 0})
			require.Equal(t, "1", id, "the recreated database's node")
			require.InDelta(t, 1, score, 1e-6)
		})
	})
}

// TestQuantizationThresholdCrossedWhileWritesArrive grows an HNSW index from
// below to above PQ's 256-vector training threshold with concurrent writes and
// reads. PQ stays pending (original coordinates) below it and encodes once
// crossed; scalar, the default, encodes at any size. Every written vector is
// found exactly either way.
func TestQuantizationThresholdCrossedWhileWritesArrive(t *testing.T) {
	for _, mode := range []string{"", "pq"} {
		name := mode
		if name == "" {
			name = "default-scalar"
		}
		t.Run(name, func(t *testing.T) {
			t.Setenv("NORNICDB_GPU_BACKEND", "cpu")
			t.Setenv("NORNICDB_HNSW_BUILD_GPU_ENABLED", "false")
			t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "0")
			t.Setenv("NORNICDB_VECTOR_ANN_QUALITY", "fast")
			if mode != "" {
				t.Setenv("NORNICDB_VECTOR_HNSW_QUANTIZATION", mode)
			}
			const start, added = 250, 24
			vec := func(i int) []float32 {
				angle := float64(i) * 2 * math.Pi / (start + added)
				return []float32{float32(math.Cos(angle)), float32(math.Sin(angle)), 0}
			}
			engine := newNamespacedEngine(t)
			for i := 0; i < start; i++ {
				_, err := engine.CreateNode(compressedLifecycleNode(fmt.Sprint(i), vec(i)))
				require.NoError(t, err)
			}
			svc := NewServiceWithDimensions(engine, 3)
			t.Cleanup(func() { require.NoError(t, svc.Close()) })
			require.NoError(t, svc.BuildIndexes(context.Background()))
			compressedLifecycleTop(t, svc, vec(0)) // installs the graph
			idx, err := svc.getOrCreateHNSWIndex(context.Background(), 3)
			require.NoError(t, err)
			if mode == "pq" {
				require.Zero(t, idx.QuantizationBytes(), "below the training threshold")
				require.Equal(t, "HNSW (quantization pending)", idx.quantizedStrategyName())
			} else {
				require.Positive(t, idx.QuantizationBytes(), "scalar encodes at any size")
			}

			writes := make(chan error, added)
			for i := start; i < start+added; i++ {
				go func(i int) {
					n := compressedLifecycleNode(fmt.Sprint(i), vec(i))
					if _, err := engine.CreateNode(n); err != nil {
						writes <- err
						return
					}
					writes <- svc.IndexNode(n)
				}(i)
			}
			readsDone := make(chan struct{})
			readErr := make(chan error, 1)
			go func() {
				defer close(readsDone)
				for i := 0; i < 50; i++ {
					if _, err := svc.VectorQueryNodesWithOptions(context.Background(), vec(i), VectorQuerySpec{Property: "default", Limit: 3, ScoreScale: VectorScoreSimilarity}, &SearchOptions{HNSWEf: 128}); err != nil {
						readErr <- err
						return
					}
				}
			}()
			for i := 0; i < added; i++ {
				require.NoError(t, <-writes)
			}
			<-readsDone
			select {
			case err := <-readErr:
				require.NoError(t, err)
			default:
			}

			for i := start; i < start+added; i++ {
				id, score := compressedLifecycleTop(t, svc, vec(i))
				require.Equal(t, fmt.Sprint(i), id)
				require.InDelta(t, 1, score, 1e-6)
			}
			idx, err = svc.getOrCreateHNSWIndex(context.Background(), 3)
			require.NoError(t, err)
			require.Positive(t, idx.QuantizationBytes(), "encoded once %d vectors are live", start+added)
		})
	}
}
