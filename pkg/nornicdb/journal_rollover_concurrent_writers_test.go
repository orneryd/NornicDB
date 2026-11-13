package nornicdb

import (
	"context"
	"encoding/base64"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// ====================================================================================
// BUG 42: concurrent writers break a journal roll-over; the store does not reopen
// ====================================================================================
// Discovered: 12 Nov 2026, real-disk measurements of a build with the product
// files of main 5b593da1e: with 16 clients updating large nodes in 18 fresh
// stores, 94 updates failed with TransactionCommitFailed and all 7 stores that
// had a failed update refused to open with "wal: unlisted segment ... overlaps
// the manifest".
// Impact: with two or more journal writers (clients, write-behind commits, the
// embedding worker), every journal append after a failed roll-over failed
// ("file already closed"), so updates committing then failed, and the
// database could not be started again.
// Root Cause: journal records took their sequence numbers before the journal
// lock, so they reached the file out of order and a roll-over could seal a
// segment overlapping the previous one (pkg/storage/wal.go).
// ====================================================================================

const (
	journalRollOverChunks = 600  // stored chunk vectors of each updated node
	journalRollOverDims   = 1024 // values per chunk vector
)

// journalRollOverBlob is the deterministic value of one acknowledged create:
// 1.5 MiB of pseudo-random bytes from a seed fixed by the writer and index, in
// base64 (2 MiB of text).
func journalRollOverBlob(writer, index int) string {
	raw := make([]byte, 1536<<10)
	rand.New(rand.NewSource(int64(writer)<<32 | int64(index))).Read(raw)
	return base64.StdEncoding.EncodeToString(raw)
}

func journalRollOverVectors() [][]float32 {
	vectors := make([][]float32, journalRollOverChunks)
	for i := range vectors {
		vector := make([]float32, journalRollOverDims)
		for j := range vector {
			vector[j] = 1 + float32(i*journalRollOverDims+j)/3
		}
		vectors[i] = vector
	}
	return vectors
}

type journalRollOverAck struct{ writer, index int }

// TestConcurrentWritersAcrossJournalRollOversReopen writes through the
// database API at the default journal limits (100 MiB or 100,000 records per
// segment) with the embedding worker enabled: one client, or six clients at
// once. Each client alternates a create of a node with about 2 MiB of text and
// an update of its own node, which holds 600 stored chunk vectors. It requires
// that no write fails, that the journal has at least two sealed segments, that
// its records are in sequence order, that it holds records written by the
// embedding worker (not that the worker wrote while the clients did), and that
// both an ordinary start and the automatic recovery's rebuild from the journal
// hold every acknowledged create exactly once with the value its client wrote,
// every acknowledged update's marker exactly once, and each client's node with
// the value of its last acknowledged update (requireJournalRollOverWrites).
//
// It runs in the write-behind mode only. Run in the durable mode on an earlier
// head of this change, the same two cases wrote 194,016 and 213,688 bytes of
// journal and sealed no segment, so they did not reach a roll-over there; the
// durable mode is covered over Bolt (pkg/bolt
// TestConcurrentWritersAcrossJournalRollOversOverBolt).
func TestConcurrentWritersAcrossJournalRollOversReopen(t *testing.T) {
	cases := []struct {
		name       string
		clients    int
		iterations int
	}{
		{name: "one_client_embedding_worker_enabled", clients: 1, iterations: 220},
		{name: "six_clients", clients: 6, iterations: 40},
	}
	for _, mode := range []string{"write_behind"} {
		for _, tc := range cases {
			t.Run(mode+"/"+tc.name, func(t *testing.T) {
				cfg := DefaultConfig()
				cfg.Database.AsyncWritesEnabled = true
				cfg.Database.StrictDurability = false
				cfg.Database.WALSyncMode = "batch"
				// No snapshot truncates the journal during the run: it keeps every
				// record, so the rebuild below replays every write.
				cfg.Database.WALAutoCompactionEnabled = false
				cfg.Memory.DecayEnabled, cfg.Memory.AutoLinksEnabled = false, false
				cfg.Memory.EmbeddingEnabled, cfg.Memory.EmbeddingDimensions = true, journalRollOverDims
				cfg.EmbeddingWorker.PropertiesInclude = []string{"text"}
				cfg.EmbeddingWorker.IncludeLabels = false
				cfg.EmbeddingWorker.BatchDelay = 0
				cfg.EmbeddingWorker.TriggerDebounceDelay = 0
				root := t.TempDir()
				db, err := Open(root, cfg)
				require.NoError(t, err)
				db.SetEmbedder(&unitVectorEmbedder{dims: journalRollOverDims})

				// Each client's own node, with stored chunk vectors (no text: the
				// embedding worker leaves it alone).
				for c := 0; c < tc.clients; c++ {
					_, err := db.ExecuteCypher(t.Context(), "CREATE (:RollOverDoc {writer: $w, v: -1})", map[string]any{"w": int64(c)})
					require.NoError(t, err)
				}
				docs, err := db.GetStorage().GetNodesByLabel(t.Context(), "RollOverDoc")
				require.NoError(t, err)
				require.Len(t, docs, tc.clients)
				inner, ok := db.GetBaseStorageForManager().(*storage.WALEngine).GetInnerEngine().(storage.EmbeddingSidecarUpdater)
				require.True(t, ok)
				vectors := journalRollOverVectors()
				for _, doc := range docs {
					embedded := storage.CopyNode(doc)
					embedded.ID = storage.NodeID(db.defaultDatabaseName() + ":" + string(doc.ID))
					embedded.ChunkEmbeddings = vectors
					embedded.EmbedMeta = map[string]any{"has_embedding": true, "chunk_count": journalRollOverChunks, "embedding_dimensions": journalRollOverDims, "embedding_model": "synthetic"}
					require.NoError(t, inner.UpdateNodeEmbeddingSidecar(t.Context(), embedded))
				}

				var mu sync.Mutex
				var createsAcked, updatesAcked []journalRollOverAck
				var failures []string
				var writers sync.WaitGroup
				for c := 0; c < tc.clients; c++ {
					writers.Add(1)
					go func(c int) {
						defer writers.Done()
						for i := 0; i < tc.iterations; i++ {
							var err error
							if i%2 == 0 {
								_, err = db.ExecuteCypher(t.Context(), "CREATE (:RollOver {writer: $w, idx: $i, text: $text, blob: $blob})", map[string]any{
									"w": int64(c), "i": int64(i), "text": fmt.Sprintf("document %d of client %d", i, c), "blob": journalRollOverBlob(c, i),
								})
							} else {
								_, err = db.ExecuteCypher(t.Context(), "MATCH (d:RollOverDoc {writer: $w}) SET d.v = $i CREATE (:RollOverAck {writer: $w, idx: $i})", map[string]any{
									"w": int64(c), "i": int64(i),
								})
							}
							mu.Lock()
							switch {
							case err != nil:
								failures = append(failures, fmt.Sprintf("client %d write %d: %v", c, i, err))
							case i%2 == 0:
								createsAcked = append(createsAcked, journalRollOverAck{writer: c, index: i})
							default:
								updatesAcked = append(updatesAcked, journalRollOverAck{writer: c, index: i})
							}
							mu.Unlock()
							if err != nil {
								return
							}
						}
					}(c)
				}
				writers.Wait()
				waitCtx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
				embedErr := db.WaitForEmbeddings(waitCtx)
				cancel()
				require.Empty(t, failures, "no write may fail because of a journal roll-over")
				require.NoError(t, embedErr)
				require.NoError(t, db.Close())

				walDir := filepath.Join(root, "wal")
				segments, err := os.ReadDir(filepath.Join(walDir, "segments"))
				require.NoError(t, err)
				sealed, journalBytes := 0, int64(0)
				for _, entry := range segments {
					if strings.HasPrefix(entry.Name(), "seg-") {
						sealed++
						if info, err := entry.Info(); err == nil {
							journalBytes += info.Size()
						}
					}
				}
				if info, err := os.Stat(filepath.Join(walDir, "wal.log")); err == nil {
					journalBytes += info.Size()
				}
				t.Logf("sealed segments %d, journal bytes %d, creates %d, updates %d", sealed, journalBytes, len(createsAcked), len(updatesAcked))
				require.GreaterOrEqual(t, sealed, 2, "the journal must have rolled over at its default limits")
				entries, err := storage.ReadWALEntriesFromDir(t.Context(), walDir)
				require.NoError(t, err)
				embeddingRecords := 0
				for i, entry := range entries {
					if i > 0 {
						require.Greater(t, entry.Sequence, entries[i-1].Sequence, "journal records must be in sequence order")
					}
					if entry.Operation == storage.OpUpdateEmbedding {
						embeddingRecords++
					}
				}
				require.Positive(t, embeddingRecords, "the embedding worker must have written to the journal during the run")

				// An ordinary start.
				reopened, err := Open(root, cfg)
				require.NoError(t, err, "the store must reopen after concurrent journal roll-overs")
				nodes, err := storage.CollectStoredNodes(t.Context(), reopened.GetStorage())
				require.NoError(t, err)
				requireJournalRollOverWrites(t, "reopened store", nodes, createsAcked, updatesAcked)
				require.NoError(t, reopened.Close())

				// The automatic recovery's rebuild: a fresh store replayed from the
				// journal alone (no snapshot was taken during the run).
				badgerOpts, _ := resolveDurabilityOptions(root, cfg)
				recovered, preserved, err := recoverBadgerFromSnapshotAndWAL(t.Context(), root, badgerOpts)
				require.NoError(t, err, "the store must be rebuilt from its journal after concurrent roll-overs")
				t.Logf("rebuilt from the journal; original kept at %s", preserved)
				nodes, err = storage.CollectStoredNodes(t.Context(), recovered)
				require.NoError(t, err)
				requireJournalRollOverWrites(t, "journal rebuild", nodes, createsAcked, updatesAcked)
				require.NoError(t, recovered.Close())
			})
		}
	}
}

// requireJournalRollOverWrites requires every acknowledged create among nodes
// exactly once with the value its client wrote, every acknowledged update's
// marker exactly once, and each client's node holding the value of its last
// acknowledged update.
func requireJournalRollOverWrites(t *testing.T, where string, nodes []*storage.Node, createsAcked, updatesAcked []journalRollOverAck) {
	t.Helper()
	creates := make(map[journalRollOverAck]string)
	updates := make(map[journalRollOverAck]bool)
	docValue := make(map[int]int64)
	for _, node := range nodes {
		w, wok := node.Properties["writer"].(int64)
		if !wok {
			continue
		}
		switch {
		case hasJournalRollOverLabel(node, "RollOver"):
			i, _ := node.Properties["idx"].(int64)
			blob, _ := node.Properties["blob"].(string)
			key := journalRollOverAck{writer: int(w), index: int(i)}
			_, dup := creates[key]
			require.False(t, dup, "%s: create %v is present twice", where, key)
			creates[key] = blob
		case hasJournalRollOverLabel(node, "RollOverAck"):
			i, _ := node.Properties["idx"].(int64)
			key := journalRollOverAck{writer: int(w), index: int(i)}
			require.False(t, updates[key], "%s: update %v is present twice", where, key)
			updates[key] = true
		case hasJournalRollOverLabel(node, "RollOverDoc"):
			v, _ := node.Properties["v"].(int64)
			docValue[int(w)] = v
		}
	}
	for _, ack := range createsAcked {
		blob, ok := creates[ack]
		require.True(t, ok, "%s: acknowledged create %v is missing", where, ack)
		require.True(t, blob == journalRollOverBlob(ack.writer, ack.index), "%s: create %v has a different value", where, ack)
	}
	last := make(map[int]int)
	for _, ack := range updatesAcked {
		require.True(t, updates[ack], "%s: acknowledged update %v is missing", where, ack)
		if ack.index > last[ack.writer] {
			last[ack.writer] = ack.index
		}
	}
	for writer, index := range last {
		require.Equal(t, int64(index), docValue[writer], "%s: client %d's node has the value of its last acknowledged update", where, writer)
	}
}

func hasJournalRollOverLabel(node *storage.Node, label string) bool {
	for _, l := range node.Labels {
		if l == label {
			return true
		}
	}
	return false
}
