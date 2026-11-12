package bolt

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

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// ====================================================================================
// BUG 42: concurrent writers break a journal roll-over; the store does not reopen
// ====================================================================================
// Discovered: 12 Nov 2026, real-disk measurements of the v1.5.0 release head:
// 16 clients updating their own large node over Bolt, 94 of 576 updates failed
// with TransactionCommitFailed and every affected store refused to open with
// "wal: unlisted segment ... overlaps the manifest".
// Root Cause: journal records took their sequence numbers before the journal
// lock, so they reached the file out of order and a roll-over could seal a
// segment overlapping the previous one (pkg/storage/wal.go).
// ====================================================================================

const rollOverBoltChunks = 300 // stored chunk vectors (2,048 values each) of each updated node

// rollOverBoltBlob is the deterministic value of one acknowledged create:
// about 2 MiB of random text, which compression cannot shrink much.
func rollOverBoltBlob(writer, index int) string {
	raw := make([]byte, 1536<<10)
	rand.New(rand.NewSource(int64(writer)<<32 | int64(index))).Read(raw)
	return base64.StdEncoding.EncodeToString(raw)
}

type rollOverBoltAck struct{ writer, index int }

// TestConcurrentWritersAcrossJournalRollOversOverBolt runs Bolt clients (one,
// or six at once) against a single-node server with default journal limits
// (100 MiB or 100,000 records per segment) and the embedding worker enabled, in
// the durable mode (synchronous writes, strict durability) and the write-behind
// mode. Each client alternates a create of a node with about 2 MiB of text and
// an update of its own node holding 300 stored chunk vectors. It requires that
// no write fails, that the journal has at least two sealed segments, that its
// records are in sequence order, that it holds records written by the
// embedding worker (not that the worker wrote while the clients did), and that
// both a reopened store and a store rebuilt from the journal alone hold every
// acknowledged create and update by identity and value.
func TestConcurrentWritersAcrossJournalRollOversOverBolt(t *testing.T) {
	cases := []struct {
		name       string
		clients    int
		iterations int
	}{
		{name: "one_client_beside_embedding_worker", clients: 1, iterations: 160},
		{name: "six_clients", clients: 6, iterations: 30},
	}
	for _, mode := range []string{"durable", "write_behind"} {
		for _, tc := range cases {
			t.Run(mode+"/"+tc.name, func(t *testing.T) {
				config := journalWorkerConfig()
				if mode == "write_behind" {
					config.Database.AsyncWritesEnabled = true
					config.Database.StrictDurability = false
					config.Database.WALSyncMode = "batch"
				}
				root := t.TempDir()
				lab := journalOpen(t, root, config)
				lab.db.SetEmbedder(&journalEmbedder{chunks: 1})
				for c := 0; c < tc.clients; c++ {
					lab.mustRun("CREATE (:RollOverDoc {writer: $w, v: -1})", map[string]any{"w": int64(c)})
				}
				lab.embed("RollOverDoc", rollOverBoltChunks)

				var mu sync.Mutex
				var createsAcked, updatesAcked []rollOverBoltAck
				var failures []string
				var writers sync.WaitGroup
				for c := 0; c < tc.clients; c++ {
					writers.Add(1)
					go func(c int) {
						defer writers.Done()
						for i := 0; i < tc.iterations; i++ {
							var err error
							if i%2 == 0 {
								_, err = lab.run("CREATE (:RollOver {writer: $w, idx: $i, text: $text, blob: $blob})", map[string]any{
									"w": int64(c), "i": int64(i), "text": fmt.Sprintf("document %d of client %d", i, c), "blob": rollOverBoltBlob(c, i),
								}, false)
							} else {
								_, err = lab.run("MATCH (d:RollOverDoc {writer: $w}) SET d.v = $i CREATE (:RollOverAck {writer: $w, idx: $i})", map[string]any{
									"w": int64(c), "i": int64(i),
								}, i%4 == 1)
							}
							mu.Lock()
							switch {
							case err != nil:
								failures = append(failures, fmt.Sprintf("client %d write %d: %v", c, i, err))
							case i%2 == 0:
								createsAcked = append(createsAcked, rollOverBoltAck{writer: c, index: i})
							default:
								updatesAcked = append(updatesAcked, rollOverBoltAck{writer: c, index: i})
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
				embedErr := lab.db.WaitForEmbeddings(waitCtx)
				cancel()
				require.Empty(t, failures, "no write may fail because of a journal roll-over")
				require.NoError(t, embedErr)
				lab.close()

				walDir := filepath.Join(root, "wal")
				segments, err := os.ReadDir(filepath.Join(walDir, "segments"))
				require.NoError(t, err)
				sealed := 0
				for _, entry := range segments {
					if strings.HasPrefix(entry.Name(), "seg-") {
						sealed++
					}
				}
				t.Logf("sealed segments %d, creates %d, updates %d", sealed, len(createsAcked), len(updatesAcked))
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
				lab = journalOpen(t, root, config)
				nodes, err := storage.CollectStoredNodes(t.Context(), lab.db.GetStorage())
				require.NoError(t, err)
				requireRollOverBoltWrites(t, "reopened store", nodes, createsAcked, updatesAcked)
				lab.close()

				// A store rebuilt from the journal alone, by the replay the
				// automatic recovery runs.
				fresh, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: t.TempDir()}))
				require.NoError(t, err)
				t.Cleanup(func() { _ = fresh.Close() })
				result, _, err := storage.RecoverIntoEngine(t.Context(), fresh, walDir, "")
				require.NoError(t, err, "the journal must replay after concurrent roll-overs")
				require.Zero(t, result.Failed, "%s", result.Summary())
				nodes, err = storage.CollectStoredNodes(t.Context(), fresh)
				require.NoError(t, err)
				requireRollOverBoltWrites(t, "journal rebuild", nodes, createsAcked, updatesAcked)
			})
		}
	}
}

// requireRollOverBoltWrites requires every acknowledged create among nodes
// exactly once with the value its client wrote, every acknowledged update's
// marker exactly once, and each client's node holding the value of its last
// acknowledged update.
func requireRollOverBoltWrites(t *testing.T, where string, nodes []*storage.Node, createsAcked, updatesAcked []rollOverBoltAck) {
	t.Helper()
	creates := make(map[rollOverBoltAck]string)
	updates := make(map[rollOverBoltAck]bool)
	docValue := make(map[int]int64)
	for _, node := range nodes {
		w, ok := node.Properties["writer"].(int64)
		if !ok || len(node.Labels) == 0 {
			continue
		}
		i, _ := node.Properties["idx"].(int64)
		key := rollOverBoltAck{writer: int(w), index: int(i)}
		switch node.Labels[0] {
		case "RollOver":
			_, dup := creates[key]
			require.False(t, dup, "%s: create %v is present twice", where, key)
			creates[key], _ = node.Properties["blob"].(string)
		case "RollOverAck":
			require.False(t, updates[key], "%s: update %v is present twice", where, key)
			updates[key] = true
		case "RollOverDoc":
			docValue[int(w)], _ = node.Properties["v"].(int64)
		}
	}
	for _, ack := range createsAcked {
		blob, ok := creates[ack]
		require.True(t, ok, "%s: acknowledged create %v is missing", where, ack)
		require.True(t, blob == rollOverBoltBlob(ack.writer, ack.index), "%s: create %v has a different value", where, ack)
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
