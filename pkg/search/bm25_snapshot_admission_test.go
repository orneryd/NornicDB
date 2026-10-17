package search

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/pkg/security"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBM25RejectedCheckpointRebuildsInitialNodePopulation(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "10")
	for _, backend := range []string{"v1", "v2"} {
		for _, traced := range []bool{false, true} {
			for _, rejection := range []string{"corrupt", "incompatible"} {
				for _, entry := range []string{"explicit_build", "live_before_first_build"} {
					t.Run(fmt.Sprintf("%s/traced=%t/%s/%s", backend, traced, rejection, entry), func(t *testing.T) {
						ctx := context.Background()
						engine := &authoritativeSourceEngine{MemoryEngine: storage.NewMemoryEngine()}
						t.Cleanup(func() { require.NoError(t, engine.Close()) })
						var source storage.Engine = engine
						if traced {
							source = storage.NewTracedEngine(engine)
						}
						_, iterator := source.(NodeIterator)
						require.Equal(t, !traced, iterator, "select the real traversal adapter")
						a := admissionNode("nornic:a", "aterm", []float32{1, 0, 0}, "named")
						_, err := engine.CreateNode(a)
						require.NoError(t, err)
						dir := t.TempDir()
						bm25Path := filepath.Join(dir, "bm25")
						seed := configureNodeAdmissionService(t, source, dir, backend, &ServiceOptions{BM25MemoryMaxBytes: 1 << 20})
						require.NoError(t, seed.BuildIndexes(ctx))
						require.Equal(t, 1, seed.fulltext().Count())
						require.NotNil(t, seed.vectorFileStore)
						require.Equal(t, 1, seed.vectorFileStore.Count())
						require.Equal(t, "nornic:a-named-v", seed.nodeNamedVector["nornic:a"]["v"])
						seed.PersistIndexesToDisk()
						require.NoError(t, seed.Close(), "stop the seed before replacing its artifact")
						for _, path := range []string{bm25Path, filepath.Join(dir, "vectors.vec"), filepath.Join(dir, "vectors.meta")} {
							info, statErr := os.Stat(path)
							require.NoError(t, statErr)
							require.Positive(t, info.Size())
						}
						if rejection == "corrupt" {
							require.NoError(t, os.WriteFile(bm25Path, []byte("not-msgpack"), 0600))
						} else if backend == "v1" {
							require.NoError(t, writeMsgpackSnapshot(bm25Path, &fulltextIndexSnapshot{Version: "99.0.0"}))
						} else {
							require.NoError(t, writeMsgpackSnapshot(bm25Path, &bm25V2Snapshot{Version: "99.0.0"}))
						}
						probe, _ := newBM25Index(backend)
						probe.Index("probe", "probecontent")
						require.NoError(t, probe.Load(bm25Path), "legacy Load intentionally returns nil for rejection")
						require.Zero(t, probe.Count(), "the fixture must actually reject this checkpoint")

						svc := configureNodeAdmissionService(t, source, dir, backend, &ServiceOptions{BM25MemoryMaxBytes: 1 << 20})
						saved, err := loadSearchBuildSettings(searchBuildSettingsPath(bm25Path, filepath.Join(dir, "vectors"), filepath.Join(dir, "hnsw")))
						require.NoError(t, err)
						require.NotNil(t, saved)
						current := svc.currentSearchBuildSettings()
						require.Equal(t, saved.BM25, current.BM25, "a settings rebuild must not hide the admission bug")
						require.Equal(t, saved.Vector, current.Vector)
						engine.nodeReads = 0
						if entry == "explicit_build" {
							require.NoError(t, svc.BuildIndexes(ctx))
							assert.True(t, svc.IsReady())
						}
						b := admissionNode("nornic:b", "bterm", []float32{0, 1, 0}, "named")
						_, err = engine.CreateNode(b)
						require.NoError(t, err)
						require.NoError(t, svc.IndexNode(b))
						// The write is searchable at once; the restored node needs the population.
						if written, err := svc.Search(ctx, "bterm", nil, &SearchOptions{Limit: 10}); assert.NoError(t, err) && assert.Len(t, written.Results, 1, "the write must be searchable without waiting") {
							assert.Equal(t, "nornic:b", written.Results[0].ID)
						}
						require.NoError(t, waitNodeAdmission(t, svc), "the write-started population")
						// In the live-first route these assertions precede any later
						// BuildIndexes call that could repair a partial initial base.
						assert.Positive(t, engine.nodeReads, "rejected text requires real NODE rebuilding")
						for _, item := range []struct {
							text string
							id   string
						}{{"aterm", "nornic:a"}, {"bterm", "nornic:b"}} {
							response, searchErr := svc.Search(ctx, item.text, nil, &SearchOptions{Limit: 10})
							require.NoError(t, searchErr)
							if assert.Len(t, response.Results, 1, "complete public text for %s", item.text) {
								assert.Equal(t, item.id, response.Results[0].ID)
							}
						}
						hits, err := svc.VectorQueryNodes(ctx, []float32{1, 0, 0}, VectorQuerySpec{Property: "v", Similarity: "dot", Limit: 10})
						require.NoError(t, err)
						if assert.Len(t, hits, 2) {
							assert.Equal(t, "nornic:a", hits[0].ID)
							assert.InDelta(t, 1, hits[0].Score, 1e-6)
						}
						svc.indexMu.RLock()
						assert.Contains(t, svc.nodeNamedVector, "nornic:a")
						assert.Contains(t, svc.nodeNamedVector, "nornic:b")
						assert.Positive(t, svc.indexCapacityByNode["nornic:a"].vectorMetadata)
						assert.Positive(t, svc.indexCapacityByNode["nornic:b"].vectorMetadata)
						assert.Positive(t, svc.indexCapacityByNode["nornic:a"].bm25Resident)
						svc.indexMu.RUnlock()
						require.NotNil(t, svc.vectorFileStore)
						assert.Equal(t, 2, svc.vectorFileStore.Count())
						assert.Zero(t, svc.vectorIndex.Count())
						assert.Zero(t, svc.vectorResidentBytes)
					})
				}
			}
		}
	}
}

func TestBM25AcceptedCheckpointPreservesNodeRestore(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "10")
	for _, backend := range []string{"v1", "v2"} {
		kinds := []string{"valid_nonempty", "valid_empty"}
		if backend == "v2" {
			kinds = append(kinds, "v1_migration")
		}
		for _, traced := range []bool{false, true} {
			for _, kind := range kinds {
				for _, entry := range []string{"explicit_build", "live_before_first_build"} {
					t.Run(fmt.Sprintf("%s/traced=%t/%s/%s", backend, traced, kind, entry), func(t *testing.T) {
						ctx := context.Background()
						engine := &authoritativeSourceEngine{MemoryEngine: storage.NewMemoryEngine()}
						t.Cleanup(func() { require.NoError(t, engine.Close()) })
						var source storage.Engine = engine
						if traced {
							source = storage.NewTracedEngine(engine)
						}
						_, iterator := source.(NodeIterator)
						require.Equal(t, !traced, iterator)
						a := admissionNode("nornic:a", "aterm", []float32{1, 0, 0}, "named")
						empty := kind == "valid_empty"
						if empty {
							a.Labels = nil
							a.Properties = nil
						}
						_, err := engine.CreateNode(a)
						require.NoError(t, err)
						dir := t.TempDir()
						bm25Path := filepath.Join(dir, "bm25")
						seedBackend := backend
						if kind == "v1_migration" {
							seedBackend = "v1"
						}
						seed := configureNodeAdmissionService(t, source, dir, seedBackend, &ServiceOptions{BM25MemoryMaxBytes: 1 << 20})
						if empty {
							// Write a genuine compatible empty checkpoint. Missing
							// artifacts and rejected zero-count loads are different.
							require.Zero(t, seed.fulltext().Count())
							require.NoError(t, seed.fulltext().Save(bm25Path))
						}
						require.NoError(t, seed.BuildIndexes(ctx))
						wantSeedDocs := 1
						if empty {
							wantSeedDocs = 0
						}
						require.Equal(t, wantSeedDocs, seed.fulltext().Count())
						seed.PersistIndexesToDisk()
						require.NoError(t, seed.Close())
						if kind == "v1_migration" {
							// Construct the supported legacy schema from the real
							// seed's installed population after its writers stop.
							// Current v1 Save writes 1.1.0, not this legacy format.
							legacy, ok := seed.fulltext().(*FulltextIndex)
							require.True(t, ok)
							legacy.mu.RLock()
							snapshot := bm25V1Snapshot{
								Version:       "1.0.0",
								Documents:     legacy.documents,
								InvertedIndex: legacy.invertedIndex,
								DocLengths:    legacy.docLengths,
								AvgDocLength:  legacy.avgDocLength,
								DocCount:      legacy.docCount,
							}
							legacy.mu.RUnlock()
							require.NoError(t, writeMsgpackSnapshot(bm25Path, &snapshot))
						}
						for _, path := range []string{bm25Path, filepath.Join(dir, "vectors.vec"), filepath.Join(dir, "vectors.meta")} {
							info, statErr := os.Stat(path)
							require.NoError(t, statErr)
							require.Positive(t, info.Size())
						}
						probe, _ := newBM25Index(backend)
						require.NoError(t, probe.Load(bm25Path))
						require.Equal(t, wantSeedDocs, probe.Count())

						svc := configureNodeAdmissionService(t, source, dir, backend, &ServiceOptions{BM25MemoryMaxBytes: 1 << 20})
						saved, err := loadSearchBuildSettings(searchBuildSettingsPath(bm25Path, filepath.Join(dir, "vectors"), filepath.Join(dir, "hnsw")))
						require.NoError(t, err)
						require.NotNil(t, saved)
						current := svc.currentSearchBuildSettings()
						require.True(t, bm25SettingsEquivalent(saved.BM25, current.BM25, svc.currentBM25FormatVersion()))
						require.Equal(t, saved.Vector, current.Vector)
						engine.nodeReads = 0
						engine.denyNodeRead = true
						if entry == "explicit_build" {
							require.NoError(t, svc.BuildIndexes(ctx), "an accepted checkpoint must not force NODE iteration")
							assert.True(t, svc.IsReady())
						}
						b := admissionNode("nornic:b", "bterm", []float32{0, 1, 0}, "named")
						_, err = engine.CreateNode(b)
						require.NoError(t, err)
						require.NoError(t, svc.IndexNode(b))
						// The write is searchable at once; the restored node needs the population.
						if written, err := svc.Search(ctx, "bterm", nil, &SearchOptions{Limit: 10}); assert.NoError(t, err) && assert.Len(t, written.Results, 1, "the write must be searchable without waiting") {
							assert.Equal(t, "nornic:b", written.Results[0].ID)
						}
						require.NoError(t, waitNodeAdmission(t, svc), "the write-started population")
						assert.Zero(t, engine.nodeReads, "accepted restoration must retain its no-traversal route")
						response, err := svc.Search(ctx, "aterm", nil, &SearchOptions{Limit: 10})
						require.NoError(t, err)
						if empty {
							assert.Empty(t, response.Results)
						} else if assert.Len(t, response.Results, 1) {
							assert.Equal(t, "nornic:a", response.Results[0].ID)
						}
						response, err = svc.Search(ctx, "bterm", nil, &SearchOptions{Limit: 10})
						require.NoError(t, err)
						if assert.Len(t, response.Results, 1) {
							assert.Equal(t, "nornic:b", response.Results[0].ID)
						}
						hits, err := svc.VectorQueryNodes(ctx, []float32{1, 0, 0}, VectorQuerySpec{Property: "v", Similarity: "dot", Limit: 10})
						require.NoError(t, err)
						if assert.Len(t, hits, 2) {
							assert.Equal(t, "nornic:a", hits[0].ID)
							assert.InDelta(t, 1, hits[0].Score, 1e-6)
						}
						svc.indexMu.RLock()
						assert.Positive(t, svc.indexCapacityByNode["nornic:a"].vectorMetadata)
						assert.Positive(t, svc.indexCapacityByNode["nornic:b"].vectorMetadata)
						if empty {
							assert.Zero(t, svc.indexCapacityByNode["nornic:a"].bm25Resident)
						} else {
							assert.Positive(t, svc.indexCapacityByNode["nornic:a"].bm25Resident)
						}
						svc.indexMu.RUnlock()
						require.NotNil(t, svc.vectorFileStore)
						assert.Equal(t, 2, svc.vectorFileStore.Count())
						assert.Zero(t, svc.vectorIndex.Count())
						assert.Zero(t, svc.vectorResidentBytes)
					})
				}
			}
		}
	}
}

func TestBM25SnapshotAcceptanceOutcome(t *testing.T) {
	for _, backend := range []string{"v1", "v2"} {
		kinds := []string{"valid_nonempty", "valid_empty", "missing", "corrupt", "incompatible", "io_error"}
		if backend == "v2" {
			kinds = append(kinds, "legacy_nonempty", "legacy_empty")
		}
		for _, kind := range kinds {
			t.Run(backend+"/"+kind, func(t *testing.T) {
				dir := t.TempDir()
				path := filepath.Join(dir, "bm25")
				legacy := kind == "legacy_nonempty" || kind == "legacy_empty"
				nonempty := kind == "valid_nonempty" || kind == "legacy_nonempty"
				wantAccepted := nonempty || kind == "valid_empty" || kind == "legacy_empty"
				seedBackend := backend
				if legacy {
					seedBackend = "v1"
				}
				seed, _ := newBM25Index(seedBackend)
				if nonempty {
					seed.Index("nornic:a", "aterm")
				}
				var originalIO *os.PathError
				switch kind {
				case "valid_nonempty", "valid_empty":
					require.NoError(t, seed.Save(path))
				case "legacy_nonempty", "legacy_empty":
					index, ok := seed.(*FulltextIndex)
					require.True(t, ok)
					index.mu.RLock()
					snapshot := bm25V1Snapshot{
						Version:       "1.0.0",
						Documents:     index.documents,
						InvertedIndex: index.invertedIndex,
						DocLengths:    index.docLengths,
						AvgDocLength:  index.avgDocLength,
						DocCount:      index.docCount,
					}
					index.mu.RUnlock()
					require.NoError(t, writeMsgpackSnapshot(path, &snapshot))
				case "corrupt":
					require.NoError(t, os.WriteFile(path, []byte("not-msgpack"), 0600))
				case "incompatible":
					if backend == "v1" {
						require.NoError(t, writeMsgpackSnapshot(path, &fulltextIndexSnapshot{Version: "99.0.0"}))
					} else {
						require.NoError(t, writeMsgpackSnapshot(path, &bm25V2Snapshot{Version: "99.0.0"}))
					}
				case "io_error":
					blocker := filepath.Join(dir, "not-a-directory")
					require.NoError(t, os.WriteFile(blocker, []byte("file"), 0600))
					path = filepath.Join(blocker, "child", "bm25")
					file, err := os.Open(path)
					if file != nil {
						require.NoError(t, file.Close())
					}
					require.Error(t, err, "the real I/O fixture must fail before either decoder")
					require.ErrorAs(t, err, &originalIO)
					rootedFile, rootedErr := security.OpenRootedFile(path, os.O_RDONLY, 0)
					if rootedFile != nil {
						require.NoError(t, rootedFile.Close())
					}
					require.Error(t, rootedErr, "rooted reference must fail before either decoder")
					var rootedPathError *os.PathError
					require.ErrorAs(t, rootedErr, &rootedPathError)
					require.False(t, errors.Is(rootedErr, os.ErrNotExist), "fixture must reach an unexpected I/O cause")
					require.ErrorIs(t, rootedErr, originalIO.Err, "rooted and ordinary opens must share the traversal cause")
				}

				internal, _ := newBM25Index(backend)
				public, _ := newBM25Index(backend)
				internal.Index("nornic:probe", "probeterm")
				public.Index("nornic:probe", "probeterm")
				accepted, err := internal.loadSnapshot(path)
				publicErr := public.Load(path)
				assert.Equal(t, wantAccepted, accepted)
				if kind == "io_error" {
					require.Error(t, err)
					require.Error(t, publicErr)
					assert.ErrorIs(t, err, originalIO.Err)
					assert.ErrorIs(t, publicErr, originalIO.Err)
					var privatePathError, publicPathError *os.PathError
					require.ErrorAs(t, err, &privatePathError)
					require.ErrorAs(t, publicErr, &publicPathError)
					assert.Equal(t, privatePathError.Op, publicPathError.Op)
					assert.Equal(t, privatePathError.Path, publicPathError.Path)
					assert.Equal(t, privatePathError.Err, publicPathError.Err)
				} else {
					require.NoError(t, err)
					require.NoError(t, publicErr, "public Load retains its recovery-to-rebuild error contract")
				}
				wantCount := 0
				if nonempty || kind == "missing" || kind == "io_error" {
					wantCount = 1
				}
				assert.Equal(t, wantCount, internal.Count())
				assert.Equal(t, internal.Count(), public.Count())
				for _, id := range []string{"nornic:a", "nornic:probe"} {
					internalText, internalFound := internal.GetDocument(id)
					publicText, publicFound := public.GetDocument(id)
					assert.Equal(t, internalFound, publicFound)
					assert.Equal(t, internalText, publicText)
				}
				assert.Equal(t, internal.Search("aterm", 10), public.Search("aterm", 10))
				if nonempty {
					text, found := internal.GetDocument("nornic:a")
					assert.True(t, found)
					assert.Equal(t, "aterm", text)
					assert.Len(t, internal.Search("aterm", 10), 1)
				}
				if kind == "missing" || kind == "io_error" {
					text, found := internal.GetDocument("nornic:probe")
					assert.True(t, found, "missing/I/O recovery preserves the prior installed state")
					assert.Equal(t, "probeterm", text)
				}
			})
		}
	}
}
