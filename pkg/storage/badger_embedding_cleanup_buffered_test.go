package storage

import (
	"context"
	"fmt"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"strings"

	"log/slog"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Embedding range lifecycle regressions, including buffered ACK and recovery.

func embeddingCleanupColdNode(t *testing.T, b *BadgerEngine, id NodeID) *Node {
	t.Helper()
	b.invalidateCachesAfterRestore()
	n, err := b.GetNode(id)
	require.NoError(t, err)
	return n
}

func TestEmbeddingCleanup_BufferedReplacement(t *testing.T) {
	for _, finish := range []string{"delete", "inline", "separate"} {
		for _, recovery := range []bool{false, true} {
			name := finish + map[bool]string{false: "/flush", true: "/recovery"}[recovery]
			t.Run(name, func(t *testing.T) {
				dir := t.TempDir()
				b, wal := newWriteBehindWALTestEngine(t, dir, time.Hour)
				id := NodeID("test:r2-buffered")
				first, err := b.BeginTransaction()
				require.NoError(t, err)
				require.NoError(t, first.SetImplicit(true))
				_, err = first.CreateNode(separatelyStoredNode(id))
				require.NoError(t, err)
				require.NoError(t, first.Commit())
				require.True(t, first.AcknowledgedBuffered())
				require.Zero(t, countEmbeddingKeys(t, b, id))

				second, err := b.BeginTransaction()
				require.NoError(t, err)
				require.NoError(t, second.SetImplicit(true))
				var final [][]float32
				if finish == "delete" {
					require.NoError(t, second.DeleteNode(id))
				} else {
					n, err := second.GetNode(id)
					require.NoError(t, err)
					final = [][]float32{{7, 8}}
					if finish == "separate" {
						final = [][]float32{make([]float32, 32768)}
						final[0][0] = 7
					}
					n.ChunkEmbeddings = final
					require.NoError(t, second.UpdateNode(n))
				}
				require.NoError(t, second.Commit())
				require.True(t, second.AcknowledgedBuffered())
				if recovery {
					crashClose(t, b, wal)
					b, err = fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir, WriteBehind: true, WriteBehindInterval: time.Hour}))
					if b != nil {
						fixtureowner.Own(t, b)
					}
					require.NoError(t, err)
					wal2, openErr := NewWAL(wal.Config().Dir, wal.Config())
					require.NoError(t, openErr)
					defer wal2.Close()
					replayed, replayErr := RecoverBufferedCommits(b, wal2)
					require.NoError(t, replayErr)
					require.Equal(t, 2, replayed)
					defer b.Close()
				} else {
					defer wal.Close()
					defer b.Close()
					require.NoError(t, b.FlushWriteBehind())
				}
				keys := countEmbeddingKeys(t, b, id)
				t.Logf("after %s: %d embedding keys", name, keys)
				if finish == "delete" {
					b.invalidateCachesAfterRestore()
					_, err := b.GetNode(id)
					require.ErrorIs(t, err, ErrNotFound)
					require.Zero(t, keys, "deleted buffered node must leave no chunk keys")
				} else {
					require.Equal(t, final, embeddingCleanupColdNode(t, b, id).ChunkEmbeddings)
					if finish == "inline" {
						require.Zero(t, keys, "inline buffered replacement must remove previous chunks")
					} else {
						require.Equal(t, 4, keys, "one 128KiB vector has one marker and three parts")
					}
				}
			})
		}
	}
}

func TestEmbeddingCleanup_PostValidationConflictAndRetry(t *testing.T) {
	for _, action := range []string{"delete", "inline"} {
		t.Run(action, func(t *testing.T) {
			barrier := &writeConflictBarrier{reached: make(chan struct{}), resume: make(chan struct{})}
			b, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir(), Logger: slog.New(barrier)}))
			require.NoError(t, err)
			defer b.Close()
			id := NodeID("test:r2-conflict")
			_, err = b.CreateNode(separatelyStoredNode(id))
			require.NoError(t, err)
			stale, err := b.BeginTransaction()
			require.NoError(t, err)
			if action == "delete" {
				require.NoError(t, stale.DeleteNode(id))
			} else {
				require.NoError(t, stale.UpdateNode(&Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]any{"k": "v"}, ChunkEmbeddings: [][]float32{{9}}}))
			}
			require.NoError(t, stale.SetMetadata(map[string]any{"r2": true}))
			barrier.target = stale.ID
			var once sync.Once
			unblock := func() { once.Do(func() { close(barrier.resume) }) }
			defer unblock()
			done := make(chan error, 1)
			go func() { done <- stale.Commit() }()
			select {
			case <-barrier.reached:
			case <-time.After(5 * time.Second):
				t.Fatal("commit did not reach post-validation barrier")
			}
			peer := separatelyStoredNode(id)
			peer.ChunkEmbeddings[0][0] = 42
			require.NoError(t, b.UpdateNode(peer))
			unblock()
			require.ErrorIs(t, <-done, ErrConflict)
			require.Equal(t, peer.ChunkEmbeddings, embeddingCleanupColdNode(t, b, id).ChunkEmbeddings)
			require.Equal(t, 16, countEmbeddingKeys(t, b, id))
			retry, err := b.BeginTransaction()
			require.NoError(t, err)
			if action == "delete" {
				require.NoError(t, retry.DeleteNode(id))
			} else {
				require.NoError(t, retry.UpdateNode(&Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]any{"k": "v"}, ChunkEmbeddings: [][]float32{{9}}}))
			}
			require.NoError(t, retry.Commit())
			require.Zero(t, countEmbeddingKeys(t, b, id))
		})
	}
}

func TestEmbeddingCleanup_BufferedPostValidationConflict(t *testing.T) {
	for _, action := range []string{"delete", "inline"} {
		t.Run(action, func(t *testing.T) {
			barrier := &writeConflictBarrier{reached: make(chan struct{}), resume: make(chan struct{})}
			b, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{
				DataDir: t.TempDir(), Logger: slog.New(barrier),
				WriteBehind: true, WriteBehindInterval: time.Hour,
			}))
			require.NoError(t, err)
			defer b.Close()
			id := NodeID("test:r2-buffered-conflict")
			_, err = b.CreateNode(&Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]any{"k": "v"}})
			require.NoError(t, err)
			stale, err := b.BeginTransaction()
			require.NoError(t, err)
			require.NoError(t, stale.SetImplicit(true))
			if action == "delete" {
				require.NoError(t, stale.DeleteNode(id))
			} else {
				require.NoError(t, stale.UpdateNode(&Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]any{"k": "v"}, ChunkEmbeddings: [][]float32{{9}}}))
			}
			require.NoError(t, stale.SetMetadata(map[string]any{"r2": true}))
			barrier.target = stale.ID
			var once sync.Once
			unblock := func() { once.Do(func() { close(barrier.resume) }) }
			defer unblock()
			done := make(chan error, 1)
			go func() { done <- stale.Commit() }()
			select {
			case <-barrier.reached:
			case <-time.After(5 * time.Second):
				t.Fatal("commit did not reach post-validation barrier")
			}
			peer := separatelyStoredNode(id)
			peer.ChunkEmbeddings[0][0] = 42
			require.NoError(t, b.UpdateNode(peer))
			unblock()
			require.ErrorIs(t, <-done, ErrConflict)
			require.False(t, stale.AcknowledgedBuffered())
			require.NoError(t, b.FlushWriteBehind())
			require.Equal(t, peer.ChunkEmbeddings, embeddingCleanupColdNode(t, b, id).ChunkEmbeddings)
			require.Equal(t, 16, countEmbeddingKeys(t, b, id))
			retry, err := b.BeginTransaction()
			require.NoError(t, err)
			require.NoError(t, retry.SetImplicit(true))
			if action == "delete" {
				require.NoError(t, retry.DeleteNode(id))
			} else {
				require.NoError(t, retry.UpdateNode(&Node{ID: id, ChunkEmbeddings: [][]float32{{9}}}))
			}
			require.NoError(t, retry.Commit())
			require.NoError(t, b.FlushWriteBehind())
			require.Zero(t, countEmbeddingKeys(t, b, id))
		})
	}
}

func TestEmbeddingCleanup_InlineIntentCost(t *testing.T) {
	for _, shape := range []string{"plain", "inline_vectors", "large_properties"} {
		vectors := shape == "inline_vectors"
		for _, action := range []string{"update", "delete", "transaction_update", "transaction_delete", "transaction_prefix_update", "transaction_prefix_delete", "transaction_unrelated_update", "transaction_unrelated_delete"} {
			t.Run(fmt.Sprintf("%s/%s", shape, action), func(t *testing.T) {
				b := newTestEngine(t)
				id := NodeID("test:no-separate-chunks")
				node := &Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]any{"k": "v"}}
				if shape == "large_properties" {
					node.Properties["payload"] = strings.Repeat("x", maxNodeSize+1)
				}
				if vectors {
					node.ChunkEmbeddings = [][]float32{{1, 2}}
				}
				_, err := b.CreateNode(node)
				require.NoError(t, err)
				var unrelated *Node
				if strings.Contains(action, "unrelated_") {
					unrelated = &Node{ID: "test:unrelated-sidecar"}
					_, err := b.CreateNode(unrelated)
					require.NoError(t, err)
					unrelated.ChunkEmbeddings = [][]float32{{1, 2}}
					unrelated.EmbedMeta = map[string]any{"chunk_count": 1}
				}
				b.invalidateCachesAfterRestore()
				scans := 0
				hook := func(scanned NodeID) {
					if scanned == id {
						scans++
					}
				}
				embeddingKeyScanHook.Store(&hook)
				defer embeddingKeyScanHook.Store(nil)
				next := copyNode(node)
				next.Properties["k"] = "v2"
				switch action {
				case "update":
					require.NoError(t, b.UpdateNode(next))
				case "delete":
					require.NoError(t, b.DeleteNode(id))
				default:
					tx, err := b.BeginTransaction()
					require.NoError(t, err)
					if strings.Contains(action, "prefix_") && !vectors && shape != "large_properties" {
						require.ErrorIs(t, tx.StreamNodesByLabelProjected("Doc", nil, func(*Node) error { return ErrIterationStopped }), ErrIterationStopped)
						require.NotEmpty(t, tx.snapshotPrefixNodeByID)
					}
					if strings.HasSuffix(action, "delete") {
						require.NoError(t, tx.DeleteNode(id))
					} else {
						require.NoError(t, tx.UpdateNode(next))
					}
					require.Nil(t, tx.embeddingChanges, "no embedding-specific staging allocation")
					if unrelated != nil {
						require.NoError(t, b.UpdateNodeEmbeddingSidecar(unrelated))
					}
					require.NoError(t, tx.Commit())
				}
				require.Zero(t, scans, "no embedding prefix scans on the common path")
				if unrelated != nil {
					require.Equal(t, 2, countEmbeddingKeys(t, b, unrelated.ID))
				}
			})
		}
	}
}

func TestEmbeddingCleanup_SidecarWithoutMetadata(t *testing.T) {
	for _, action := range []string{"update", "delete", "transaction_update", "transaction_delete"} {
		t.Run(action, func(t *testing.T) {
			b := newTestEngine(t)
			id := NodeID("test:sidecar-without-metadata")
			_, err := b.CreateNode(&Node{ID: id})
			require.NoError(t, err)
			n, err := b.GetNode(id)
			require.NoError(t, err)
			n.ChunkEmbeddings = [][]float32{{1, 2}}
			require.NoError(t, b.UpdateNodeEmbeddingSidecar(n))
			require.Equal(t, n.ChunkEmbeddings, embeddingCleanupColdNode(t, b, id).ChunkEmbeddings)
			next := &Node{ID: id, Properties: map[string]any{"text": "new source"}}
			switch action {
			case "update":
				require.NoError(t, b.UpdateNode(next))
			case "delete":
				require.NoError(t, b.DeleteNode(id))
			default:
				tx, err := b.BeginTransaction()
				require.NoError(t, err)
				if action == "transaction_delete" {
					require.NoError(t, tx.DeleteNode(id))
				} else {
					require.NoError(t, tx.UpdateNode(next))
				}
				require.NoError(t, tx.Commit())
			}
			require.Zero(t, countEmbeddingKeys(t, b, id))
		})
	}
}

func TestEmbeddingCleanup_SidecarFromCallback(t *testing.T) {
	if os.Getenv("NORNIC_EMBEDDING_CLEANUP_CALLBACK_CHILD") == "1" {
		for _, action := range []string{"create", "update", "transaction_create", "transaction_update", "buffered_create", "buffered_update"} {
			t.Run(action, func(t *testing.T) {
				b, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{InMemory: true, WriteBehind: strings.HasPrefix(action, "buffered"), WriteBehindInterval: time.Hour}))
				require.NoError(t, err)
				defer b.Close()
				id := NodeID("test:callback")
				updating := strings.HasSuffix(action, "update")
				if updating {
					_, err := b.CreateNode(&Node{ID: id})
					require.NoError(t, err)
				}
				called := false
				callback := func(n *Node) {
					if len(n.ChunkEmbeddings) > 0 {
						return
					}
					called = true
					embedded := copyNode(n)
					embedded.ChunkEmbeddings = [][]float32{{1, 2}}
					embedded.EmbedMeta = map[string]any{"chunk_count": 1}
					require.NoError(t, b.UpdateNodeEmbeddingSidecar(embedded))
				}
				if updating {
					b.OnNodeUpdated(callback)
				} else {
					b.OnNodeCreated(callback)
				}
				next := &Node{ID: id, Properties: map[string]any{"k": "v"}}
				if strings.Contains(action, "_") {
					tx, err := b.BeginTransaction()
					require.NoError(t, err)
					require.NoError(t, tx.SetImplicit(strings.HasPrefix(action, "buffered")))
					if updating {
						require.NoError(t, tx.UpdateNode(next))
					} else {
						_, err := tx.CreateNode(next)
						require.NoError(t, err)
					}
					require.NoError(t, tx.Commit())
					require.NoError(t, b.FlushWriteBehind())
				} else if updating {
					require.NoError(t, b.UpdateNode(next))
				} else {
					_, err := b.CreateNode(next)
					require.NoError(t, err)
				}
				require.True(t, called)
				require.Equal(t, [][]float32{{1, 2}}, embeddingCleanupColdNode(t, b, id).ChunkEmbeddings)
			})
		}
		t.Run("close_reentry", func(t *testing.T) {
			b := newTestEngine(t)
			started, resume := make(chan struct{}), make(chan struct{})
			sidecar := make(chan error, 1)
			b.OnNodeCreated(func(n *Node) {
				close(started)
				<-resume
				sidecar <- b.UpdateNodeEmbeddingSidecar(n)
			})
			created := make(chan error, 1)
			go func() { _, err := b.CreateNode(&Node{ID: "test:closing-callback"}); created <- err }()
			<-started
			closed := make(chan error, 1)
			go func() { closed <- b.Close() }()
			require.Eventually(t, func() bool { b.mu.RLock(); defer b.mu.RUnlock(); return b.closed }, 5*time.Second, time.Millisecond)
			close(resume)
			require.ErrorIs(t, <-sidecar, ErrStorageClosed)
			require.NoError(t, <-created)
			require.NoError(t, <-closed)
		})
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestEmbeddingCleanup_SidecarFromCallback$", "-test.timeout=15s", "-test.v")
	cmd.Env = append(os.Environ(), "NORNIC_EMBEDDING_CLEANUP_CALLBACK_CHILD=1")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Logf("child output: %s", output)
	}
	require.NoError(t, err, "callbacks may synchronously publish embedding sidecars")
}
