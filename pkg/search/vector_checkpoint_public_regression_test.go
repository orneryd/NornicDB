package search

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Process boundaries use only public store operations. In particular, the
// after_compact boundary does not run Close, Save or a deferred cleanup in the
// child. A service rebuild must not conceal an unreadable/mixed pair here.
func TestVectorCheckpointCrashAtomic(t *testing.T) {
	if directory := os.Getenv("NOR351_CHECKPOINT_CHILD_DIR"); directory != "" {
		kind, phase := os.Getenv("NOR351_CHECKPOINT_CHILD_KIND"), os.Getenv("NOR351_CHECKPOINT_CHILD_PHASE")
		v, err := NewVectorFileStore(filepath.Join(directory, "vectors"), 4)
		require.NoError(t, err)
		for i, id := range []string{"z", "a", "dead"} {
			require.NoError(t, v.Add(id, vectorOwnerAxis(i)))
		}
		require.NoError(t, v.Save())
		require.NoError(t, v.Add("z", vectorOwnerAxis(3)))
		require.True(t, v.Remove("dead"))
		if kind == "empty" {
			require.True(t, v.Remove("z"))
			require.True(t, v.Remove("a"))
		}
		require.NoError(t, v.Save()) // old checkpoint has the exact live set below
		if strings.HasPrefix(phase, "during_") {
			vectorCheckpointHook = func(_ string, boundary string) {
				if boundary == strings.TrimPrefix(phase, "during_") {
					os.Exit(86)
				}
			}
		}
		if phase != "before_compact" {
			compacted, err := v.CompactIfNeeded()
			require.NoError(t, err)
			require.True(t, compacted)
		}
		if phase == "after_save" {
			require.NoError(t, v.Save())
		}
		os.Exit(86)
	}
	t.Setenv("NORNICDB_VECTOR_VFS_COMPACT_MIN_OBSOLETE", "1")
	t.Setenv("NORNICDB_VECTOR_VFS_COMPACT_MIN_SIZE_MB", "0")
	t.Setenv("NORNICDB_VECTOR_VFS_COMPACT_DEAD_RATIO", "0")
	binaryPath, err := os.Executable()
	require.NoError(t, err)
	for _, kind := range []string{"reordered", "empty"} {
		for _, phase := range []string{"before_compact", "during_prepared", "during_decided", "during_payload", "during_metadata", "during_committed", "after_compact", "after_save"} {
			t.Run(kind+"/"+phase, func(t *testing.T) {
				dir := t.TempDir()
				ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
				defer cancel()
				child := exec.CommandContext(ctx, binaryPath, "-test.run=^TestVectorCheckpointCrashAtomic$", "-test.count=1")
				child.Env = append(os.Environ(), "NOR351_CHECKPOINT_CHILD_DIR="+dir,
					"NOR351_CHECKPOINT_CHILD_KIND="+kind, "NOR351_CHECKPOINT_CHILD_PHASE="+phase)
				output, err := child.CombinedOutput()
				var exit *exec.ExitError
				require.ErrorAs(t, err, &exit, "%s", output)
				require.Equal(t, 86, exit.ExitCode(), "child must reach the admitted crash boundary: %s", output)
				require.NoError(t, ctx.Err())
				v, err := NewVectorFileStore(filepath.Join(dir, "vectors"), 4)
				require.NoError(t, err, "the old or new complete checkpoint must open")
				t.Cleanup(func() { require.NoError(t, v.Close()) })
				require.NoError(t, v.Load(), "compaction must not pair a new payload with old ordinals")
				want := map[string][]float32{}
				if kind != "empty" {
					want["z"], want["a"] = vectorOwnerAxis(3), vectorOwnerAxis(1)
				}
				require.Equal(t, len(want), v.Count())
				for _, id := range []string{"z", "a", "dead"} {
					got, exists := v.GetVector(id)
					expected, live := want[id]
					require.Equal(t, live, exists, id)
					if live {
						require.Equal(t, expected, got, id)
					}
				}
				require.NoError(t, v.Add("after-recovery", vectorOwnerAxis(0)))
				require.NoError(t, v.Save())
			})
		}
	}
}

func vectorCheckpointPhysicalSource(t *testing.T) storage.Engine {
	t.Helper()
	base, err := fixtureowner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{
		DataDir: t.TempDir(), LowMemory: true,
	}))
	require.NoError(t, err)
	return storage.NewNamespacedEngine(base, "checkpoint")
}

func vectorCheckpointService(t *testing.T, source storage.Engine, directory, mode string) *Service {
	t.Helper()
	options := DefaultServiceOptions("checkpoint")
	options.VectorStorageMode = mode
	s := NewService(source, vectorOwnerDims, &options)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	s.FixVectorDimensions()
	s.SetFulltextIndexPath(filepath.Join(directory, "bm25"))
	s.SetVectorIndexPath(filepath.Join(directory, "vectors"))
	s.SetPersistenceEnabled(true)
	return s
}

// A regular file as a path parent fails deterministically even as root. This
// probes the build and first-live-write entries, without chmod assumptions.
// The operator-visible VectorStorageStatus is part of the contract: an explicit
// disk failure reports the failed backing instead of a memory admission, and
// auto either exposes its accepted memory fallback with a reason or refuses the
// unusable destination through the returned error.
func TestExplicitDiskFailurePublic(t *testing.T) {
	for _, entry := range []string{"build", "live_create", "live_load"} {
		for _, mode := range []string{"disk", "auto"} {
			t.Run(entry+"/"+mode, func(t *testing.T) {
				source := vectorCheckpointPhysicalSource(t)
				node := vectorOwnerChunkNode("doc", 0)
				_, err := source.CreateNode(t.Context(), cloneVectorOwnerNode(node))
				require.NoError(t, err)
				dir := t.TempDir()
				s := vectorCheckpointService(t, source, dir, mode)
				if entry == "live_load" {
					path := filepath.Join(dir, "vectors")
					saved, err := NewVectorFileStore(path, vectorOwnerDims)
					require.NoError(t, err)
					require.NoError(t, saved.Add("doc", vectorOwnerAxis(0)))
					require.NoError(t, saved.Save())
					require.NoError(t, saved.Close())
					// Nornic2's disposition: malformed derived content can be
					// rebuilt on disk. Reading a directory as metadata is instead
					// an operational I/O failure, which strict disk must return.
					require.NoError(t, os.Remove(path+".meta"))
					require.NoError(t, os.Mkdir(path+".meta", 0700))
				} else {
					blocked := filepath.Join(dir, "not-a-directory")
					require.NoError(t, os.WriteFile(blocked, []byte("path obstruction"), 0600))
					s.SetVectorIndexPath(filepath.Join(blocked, "vectors"))
				}
				if entry == "build" {
					err = s.BuildIndexes(t.Context())
				} else {
					err = s.IndexNode(node)
				}
				status := s.VectorStorageStatus()
				require.Equal(t, mode, status.Configured)
				if mode == "disk" {
					require.Error(t, err, "explicit disk must not acknowledge an in-memory replacement")
					if entry == "build" {
						// The obstruction fails the build's population-artifact
						// preparation, so the operational filesystem error is
						// this entry's outcome. The status must still identify
						// the failed disk backing rather than a working memory
						// index.
						require.Contains(t, err.Error(), "not-a-directory",
							"the obstructed vector destination must surface as an operational error")
					} else {
						require.Contains(t, err.Error(), "explicit vector disk storage",
							"the public error must identify the operator's selected storage")
					}
					assert.Equal(t, "error", status.Active,
						"explicit disk must report the failed backing, not memory or disk")
					assert.NotEmpty(t, status.Reason, "the failed disk admission must stay operator-visible")
					if entry == "build" {
						s.SetVectorIndexPath(filepath.Join(dir, "vectors"))
						require.NoError(t, s.BuildIndexes(t.Context()))
						recovered := s.VectorStorageStatus()
						require.Equal(t, "disk", recovered.Active)
						require.Empty(t, recovered.Reason)
						require.Equal(t, 1, s.EmbeddingCount())
					}
					return
				}
				if err != nil {
					// Auto is allowed to refuse an unusable persistence
					// destination rather than replace it with memory. The
					// returned error is then the outcome; the status must not
					// present the obstructed destination as an admitted store.
					require.NotEqual(t, "disk", status.Active,
						"an obstructed destination must not be reported as an admitted disk store")
					t.Logf("auto %s refused the obstruction: err=%v active=%q reason=%q", entry, err, status.Active, status.Reason)
					return
				}
				// Auto may fall back; the public vector source must still work.
				require.Equal(t, "memory", status.Active, "the allowed fallback must be the reported active mode")
				assert.NotEmpty(t, status.Reason, "the fallback reason must be operator-visible")
				hits, err := s.VectorQueryNodes(t.Context(), vectorOwnerAxis(0), VectorQuerySpec{Label: "Doc", Limit: 1, ScoreScale: VectorScoreSimilarity})
				require.NoError(t, err)
				require.Len(t, hits, 1)
				require.Equal(t, "doc", hits[0].ID)
				require.Equal(t, float64(1), hits[0].Score)
			})
		}
	}
}

func TestExplicitDiskPopulationDestinationStatus(t *testing.T) {
	for _, tc := range []struct {
		name, active string
		shareParent  bool
	}{
		{name: "shared vector parent", active: "error", shareParent: true},
		{name: "separate fulltext parent", active: "memory"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			s := vectorCheckpointService(t, vectorCheckpointPhysicalSource(t), dir, "disk")
			blocked := filepath.Join(dir, "not-a-directory")
			require.NoError(t, os.WriteFile(blocked, nil, 0o600))
			s.SetFulltextIndexPath(filepath.Join(blocked, "bm25"))
			if tc.shareParent {
				s.SetVectorIndexPath(filepath.Join(blocked, "vectors"))
			}
			require.ErrorContains(t, s.BuildIndexes(t.Context()), "not-a-directory")
			status := s.VectorStorageStatus()
			require.Equal(t, "disk", status.Configured)
			require.Equal(t, tc.active, status.Active)
			if tc.shareParent {
				require.Contains(t, status.Reason, "not-a-directory")
			} else {
				require.Empty(t, status.Reason)
			}
		})
	}
}

// Matching counts, dimensions and a recent payload mtime are deliberately kept.
// The answer must come from the database's current vector and query metadata,
// not merely from a valid-looking checkpoint or old sidecar associations.
func TestVectorCheckpointSourcePublic(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "1000")
	for _, damage := range []string{"stale_vector", "payload_flip", "metadata_owner", "same_shape_swap"} {
		t.Run(damage, func(t *testing.T) {
			source := vectorCheckpointPhysicalSource(t)
			nodes := []*storage.Node{vectorOwnerChunkNode("doc", 0), vectorOwnerChunkNode("other", 1)}
			for _, node := range nodes {
				_, err := source.CreateNode(t.Context(), cloneVectorOwnerNode(node))
				require.NoError(t, err)
			}
			dir := t.TempDir()
			first := vectorCheckpointService(t, source, dir, "disk")
			require.NoError(t, first.BuildIndexes(t.Context()))
			first.PersistIndexesToDisk()
			require.NoError(t, first.Close())
			base := filepath.Join(dir, "vectors")
			switch damage {
			case "stale_vector":
				current, err := source.GetNode(t.Context(), "doc")
				require.NoError(t, err)
				current.ChunkEmbeddings = [][]float32{vectorOwnerAxis(2)}
				require.NoError(t, source.UpdateNode(t.Context(), storage.NodeUpdateRequest{After: current}))
			case "payload_flip":
				// Replace every physical row by a different finite unit vector;
				// this is structurally well-formed, not a malformed-header case.
				data, err := os.ReadFile(base + ".vec")
				require.NoError(t, err)
				for offset := vecHeaderSize; offset < len(data); offset += 4 * 4 {
					for axis := range 4 {
						binary.LittleEndian.PutUint32(data[offset+axis*4:], math.Float32bits(vectorOwnerAxis(3)[axis]))
					}
				}
				require.NoError(t, os.WriteFile(base+".vec", data, 0600))
			case "metadata_owner", "same_shape_swap":
				// Use the public writer to produce a coherent but wrong same-size
				// checkpoint; a payload digest alone cannot establish DB equality.
				wrongBase := filepath.Join(t.TempDir(), "vectors")
				wrong, err := NewVectorFileStore(wrongBase, vectorOwnerDims)
				require.NoError(t, err)
				for _, node := range nodes {
					axis := 3
					if damage == "metadata_owner" {
						axis = 0
						if node.ID == "other" {
							axis = 1
						}
					}
					require.NoError(t, wrong.Add(string(node.ID), vectorOwnerAxis(axis)))
				}
				wrong.SetNodeQueryMetadataWithTypes(map[string][]string{"doc": {"Doc"}, "other": {"Doc"}}, map[string]string{}, nil, nil,
					map[string][]string{"doc": {"other"}, "other": {"doc"}})
				require.NoError(t, wrong.Save())
				require.NoError(t, wrong.Close())
				for _, suffix := range []string{".vec", ".meta"} {
					data, err := os.ReadFile(wrongBase + suffix)
					require.NoError(t, err)
					require.NoError(t, os.WriteFile(base+suffix, data, 0600))
				}
			}
			// mtime cannot prove that the checkpoint equals the database.
			future := time.Now().Add(time.Hour)
			require.NoError(t, os.Chtimes(base+".vec", future, future))
			second := vectorCheckpointService(t, source, dir, "disk")
			require.NoError(t, second.BuildIndexes(t.Context()))
			for _, node := range nodes {
				current, err := source.GetNode(t.Context(), node.ID)
				require.NoError(t, err)
				query := current.ChunkEmbeddings[0]
				hits, err := second.VectorQueryNodes(t.Context(), query, VectorQuerySpec{Label: "Doc", Limit: 1, ScoreScale: VectorScoreSimilarity})
				require.NoError(t, err)
				require.Len(t, hits, 1)
				require.Equal(t, string(node.ID), hits[0].ID, fmt.Sprintf("%s must rebuild the authoritative entry", damage))
				require.Equal(t, float64(1), hits[0].Score)
			}
			// A rejected derived checkpoint is replaced by an ordinary on-disk
			// population, not a silent memory index; the rejection stays visible
			// as the recorded rebuild reason on the same public status.
			status := second.VectorStorageStatus()
			require.Equal(t, "disk", status.Configured)
			require.Equal(t, "disk", status.Active, "the rejected checkpoint must be rebuilt on disk")
			require.NotEmpty(t, status.LastRebuild, "the operator must be able to see that vectors were rebuilt")
		})
	}
}
