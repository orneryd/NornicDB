package search

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func configureNodeAdmissionService(t *testing.T, source storage.Engine, dir, backend string, options *ServiceOptions) *Service {
	t.Helper()
	svc := NewServiceWithDimensionsAndBM25EngineAndOptions(source, 3, backend, options)
	t.Cleanup(func() { require.NoError(t, svc.Close()) })
	svc.FixVectorDimensions()
	svc.SetFulltextIndexPath(filepath.Join(dir, "bm25"))
	svc.SetVectorIndexPath(filepath.Join(dir, "vectors"))
	svc.SetHNSWIndexPath(filepath.Join(dir, "hnsw"))
	svc.SetPersistenceEnabled(true)
	return svc
}

func admissionNode(id, text string, value []float32, shape string) *storage.Node {
	n := &storage.Node{ID: storage.NodeID(id), Labels: []string{"Doc"}, Properties: map[string]any{"text": text}}
	switch shape {
	case "named":
		n.NamedEmbeddings = map[string][]float32{"v": value}
	case "property":
		n.Properties["v"] = value
	case "chunks":
		n.ChunkEmbeddings = [][]float32{value}
	}
	return n
}

func TestNodePopulationFirstMutationPreservesRestart(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "0")
	for _, backend := range []string{"v1", "v2"} {
		for _, traced := range []bool{false, true} {
			for _, operation := range []string{"update", "remove", "text_only", "named", "property", "chunks"} {
				t.Run(fmt.Sprintf("%s/traced=%t/%s", backend, traced, operation), func(t *testing.T) {
					ctx := context.Background()
					engine := &authoritativeSourceEngine{MemoryEngine: storage.NewMemoryEngine()}
					t.Cleanup(func() { require.NoError(t, engine.Close()) })
					var source storage.Engine = engine
					if traced {
						source = storage.NewTracedEngine(engine)
					}
					dir := t.TempDir()
					a := admissionNode("nornic:a", "aterm", []float32{1, 0, 0}, "named")
					_, err := engine.CreateNode(a)
					require.NoError(t, err)
					seed := configureNodeAdmissionService(t, source, dir, backend, nil)
					require.NoError(t, seed.BuildIndexes(ctx))
					require.NoError(t, seed.Close())
					live := configureNodeAdmissionService(t, source, dir, backend, nil)
					wantText := map[string]string{"aterm": "nornic:a"}
					switch operation {
					case "update":
						a.Properties["text"] = "updatedterm"
						a.NamedEmbeddings["v"] = []float32{0, 1, 0}
						require.NoError(t, engine.UpdateNode(a))
						require.NoError(t, live.IndexNode(a))
						delete(wantText, "aterm")
						wantText["updatedterm"] = "nornic:a"
					case "remove":
						require.NoError(t, engine.DeleteNode(a.ID))
						require.NoError(t, live.RemoveNode(a.ID))
						delete(wantText, "aterm")
					default:
						b := admissionNode("nornic:b", "bterm", []float32{0, 1, 0}, operation)
						_, err := engine.CreateNode(b)
						require.NoError(t, err)
						require.NoError(t, live.IndexNode(b))
						wantText["bterm"] = "nornic:b"
					}
					// No middle Build: the mutation itself must have admitted a
					// complete initial population before its immediate saved image.
					live.PersistIndexesToDisk()
					require.NoError(t, live.Close())
					restored := configureNodeAdmissionService(t, source, dir, backend, nil)
					engine.nodeReads = 0
					engine.denyNodeRead = true
					require.NoError(t, restored.BuildIndexes(ctx))
					assert.Zero(t, engine.nodeReads)
					for term, id := range wantText {
						resp, err := restored.Search(ctx, term, nil, &SearchOptions{Limit: 10})
						require.NoError(t, err)
						if assert.Len(t, resp.Results, 1) {
							assert.Equal(t, id, resp.Results[0].ID)
						}
					}
					if operation == "remove" || operation == "update" {
						resp, err := restored.Search(ctx, "aterm", nil, &SearchOptions{Limit: 10})
						require.NoError(t, err)
						assert.Empty(t, resp.Results)
					}
					if operation == "remove" {
						assert.NotContains(t, restored.nodeNamedVector, "nornic:a")
						assert.NotContains(t, restored.indexCapacityByNode, "nornic:a")
					} else {
						value := []float32{1, 0, 0}
						if operation == "update" {
							value = []float32{0, 1, 0}
						}
						hits, err := restored.VectorQueryNodes(ctx, value, VectorQuerySpec{Label: "Doc", Property: "v", Similarity: "dot", Limit: 10})
						require.NoError(t, err)
						require.NotEmpty(t, hits)
						assert.Equal(t, "nornic:a", hits[0].ID)
						assert.InDelta(t, 1, hits[0].Score, 1e-6)
						assert.Contains(t, restored.indexCapacityByNode, "nornic:a")
						if operation != "update" && operation != "text_only" {
							graph, err := restored.getOrCreateHNSWIndex(ctx, 3)
							require.NoError(t, err)
							candidates, err := graph.Search(ctx, []float32{0, 1, 0}, 1, -1)
							require.NoError(t, err)
							require.Len(t, candidates, 1)
							assert.Equal(t, "nornic:b", normalizeVectorResultIDToNodeID(candidates[0].ID))
						}
					}
				})
			}
		}
	}
}

func TestNodePopulationUninitializedPersistAndClosePreserveArtifacts(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "0")
	for _, backend := range []string{"v1", "v2"} {
		t.Run(backend, func(t *testing.T) {
			engine := storage.NewMemoryEngine()
			t.Cleanup(func() { require.NoError(t, engine.Close()) })
			dir := t.TempDir()
			_, err := engine.CreateNode(admissionNode("nornic:a", "aterm", []float32{1, 0, 0}, "named"))
			require.NoError(t, err)
			seed := configureNodeAdmissionService(t, engine, dir, backend, nil)
			require.NoError(t, seed.BuildIndexes(context.Background()))
			require.NoError(t, seed.Close())
			paths := []string{"bm25", "vectors.vec", "vectors.meta", "hnsw", "build_settings"}
			before := map[string][]byte{}
			for _, p := range paths {
				value, err := os.ReadFile(filepath.Join(dir, p))
				require.NoError(t, err)
				before[p] = value
			}
			fresh := configureNodeAdmissionService(t, engine, dir, backend, nil)
			fresh.PersistIndexesToDisk()
			require.Nil(t, fresh.vectorFileStore)
			require.NoError(t, fresh.Close())
			for _, p := range paths {
				value, err := os.ReadFile(filepath.Join(dir, p))
				require.NoError(t, err)
				assert.Equal(t, before[p], value, p)
			}
		})
	}
}

func TestNodePopulationWaiterProceedsWhileANNIsBlocked(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "0")
	for _, backend := range []string{"v1", "v2"} {
		t.Run(backend, func(t *testing.T) {
			engine := storage.NewMemoryEngine()
			t.Cleanup(func() { require.NoError(t, engine.Close()) })
			a := admissionNode("nornic:a", "aterm", []float32{1, 0, 0}, "named")
			_, err := engine.CreateNode(a)
			require.NoError(t, err)
			svc := configureNodeAdmissionService(t, engine, t.TempDir(), backend, nil)
			logGate := &baseCheckpointBuildLog{entered: make(chan struct{}), release: make(chan struct{})}
			var sourceOnce, graphOnce sync.Once
			releaseSource := func() { sourceOnce.Do(func() { close(logGate.release) }) }
			defer releaseSource()
			svc.SetLogger(slog.New(logGate))
			graphStarted, graphRelease := make(chan struct{}), make(chan struct{})
			releaseGraph := func() { graphOnce.Do(func() { close(graphRelease) }) }
			defer releaseGraph()
			hnswBeforeInstallHook = func(ctx context.Context) {
				close(graphStarted)
				select {
				case <-graphRelease:
				case <-ctx.Done():
				}
			}
			defer func() { hnswBeforeInstallHook = nil }()
			buildDone := make(chan error, 1)
			go func() { buildDone <- svc.BuildIndexes(context.Background()) }()
			select {
			case <-logGate.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("node phase did not enter source gate")
			}
			b := admissionNode("nornic:b", "bterm", []float32{0, 1, 0}, "named")
			_, err = engine.CreateNode(b)
			require.NoError(t, err)
			// The write is queued on the population and never waits for it.
			requireReturnsWhileBlocked(t, "a write during the node phase", func() error { return svc.IndexNode(b) })
			releaseSource()
			select {
			case <-graphStarted:
			case <-time.After(5 * time.Second):
				t.Fatal("ANN did not reach retained hook")
			}
			removeDone := make(chan error, 1)
			require.NoError(t, engine.DeleteNode(a.ID))
			go func() { removeDone <- svc.RemoveNode(a.ID) }()
			select {
			case err := <-removeDone:
				require.NoError(t, err)
			case <-time.After(5 * time.Second):
				t.Fatal("live delete waited for ANN")
			}
			select {
			case err := <-buildDone:
				t.Fatalf("build escaped blocked ANN: %v", err)
			default:
			}
			releaseGraph()
			require.NoError(t, <-buildDone)
			resp, err := svc.Search(context.Background(), "bterm", nil, &SearchOptions{Limit: 10})
			require.NoError(t, err)
			require.Len(t, resp.Results, 1)
			assert.Equal(t, "nornic:b", resp.Results[0].ID)
			resp, err = svc.Search(context.Background(), "aterm", nil, &SearchOptions{Limit: 10})
			require.NoError(t, err)
			assert.Empty(t, resp.Results)
			graph, err := svc.getOrCreateHNSWIndex(context.Background(), 3)
			require.NoError(t, err)
			hits, err := graph.Search(context.Background(), []float32{0, 1, 0}, 10, -1)
			require.NoError(t, err)
			require.Len(t, hits, 1)
			assert.Equal(t, "nornic:b-named-v", hits[0].ID)
		})
	}
}

func TestNodePopulationAdmissionErrorAndCloseWakeMutations(t *testing.T) {
	for _, closeOwner := range []bool{false, true} {
		t.Run(fmt.Sprint("close=", closeOwner), func(t *testing.T) {
			engine := storage.NewMemoryEngine()
			t.Cleanup(func() { require.NoError(t, engine.Close()) })
			_, err := engine.CreateNode(admissionNode("nornic:a", "aterm", []float32{1, 0, 0}, "named"))
			require.NoError(t, err)
			svc := configureNodeAdmissionService(t, engine, t.TempDir(), "v2", nil)
			gate := &baseCheckpointBuildLog{entered: make(chan struct{}), release: make(chan struct{})}
			var once sync.Once
			release := func() { once.Do(func() { close(gate.release) }) }
			defer release()
			svc.SetLogger(slog.New(gate))
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			buildDone := make(chan error, 1)
			go func() { buildDone <- svc.BuildIndexes(ctx) }()
			select {
			case <-gate.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("missing node gate")
			}
			// The write is queued on the population and returns at once; the
			// population applies it on every exit, also a cancelled one.
			requireReturnsWhileBlocked(t, "a write during the node phase", func() error {
				return svc.IndexNode(admissionNode("nornic:b", "bterm", []float32{0, 1, 0}, "named"))
			})
			closed := make(chan error, 1)
			if closeOwner {
				go func() { closed <- svc.Close() }()
			} else {
				cancel()
			}
			release()
			require.Error(t, <-buildDone)
			if closeOwner {
				require.NoError(t, <-closed)
			}
			assert.False(t, svc.IsReady())
			if !closeOwner {
				assert.NotEmpty(t, svc.fulltext().Search("bterm", 10), "a cancelled population dropped the queued write")
				svc.indexMu.RLock()
				assert.Contains(t, svc.nodeNamedVector, "nornic:b")
				svc.indexMu.RUnlock()
			}
			svc.persistMu.Lock()
			timer := svc.persistTimer
			svc.persistMu.Unlock()
			assert.Nil(t, timer)
		})
	}
}
