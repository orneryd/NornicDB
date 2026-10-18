package storage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// This is an analysis-only overlay. All mutations use the public engine/tx API;
// the private key reader only checks the physical postcondition.
func TestRelease1285OwnerTransitions(t *testing.T) {
	for _, publication := range []string{"sync", "buffered", "recovery"} {
		for _, operation := range []string{"create-delete", "create-inline", "create-replace-large", "committed-inline", "committed-replace-large", "invalidate-source", "retain-source", "delete-recreate", "rollback-create", "rollback-delete", "rollback-replace"} {
			t.Run(publication+"/"+operation, func(t *testing.T) {
				var e *BadgerEngine
				var wal *WAL
				dir := t.TempDir()
				if publication == "sync" {
					e = newTestEngine(t)
				} else if publication == "buffered" {
					e = newWriteBehindTestEngine(t, time.Hour)
				} else {
					e, wal = newWriteBehindWALTestEngine(t, dir, time.Hour)
					defer func() { _ = e.Close(); _ = wal.Close() }()
				}
				id := NodeID("test:owner-probe")
				created := operation == "create-delete" || operation == "create-inline" || operation == "create-replace-large" || operation == "rollback-create"
				if !created {
					_, err := e.CreateNode(sol1474Large(id))
					require.NoError(t, err)
				}
				tx, err := e.BeginTransaction()
				require.NoError(t, err)
				defer tx.Rollback()
				if publication != "sync" {
					require.NoError(t, tx.SetImplicit(true))
				}
				if created {
					_, err = tx.CreateNode(sol1474Large(id))
					require.NoError(t, err)
				}
				want := sol1474Large(id)
				deleted, rollback := false, false
				switch operation {
				case "create-delete", "delete-recreate", "rollback-delete":
					require.NoError(t, tx.DeleteNode(id))
					deleted = true
					if operation == "delete-recreate" {
						want.ChunkEmbeddings = nil
						_, err = tx.CreateNode(want)
						require.NoError(t, err)
						deleted = false
					}
				case "create-inline", "committed-inline", "rollback-replace":
					want.ChunkEmbeddings = [][]float32{{2, 3, 5}}
					require.NoError(t, tx.UpdateNode(want))
				case "create-replace-large", "committed-replace-large":
					for i := range want.ChunkEmbeddings { want.ChunkEmbeddings[i][0] = 99 }
					require.NoError(t, tx.UpdateNode(want))
				case "invalidate-source":
					want.ChunkEmbeddings = nil
					want.Properties["text"] = "changed"
					require.NoError(t, tx.UpdateNode(want))
				case "retain-source":
					require.NoError(t, tx.UpdateNode(want))
				}
				rollback = operation == "rollback-create" || operation == "rollback-delete" || operation == "rollback-replace"
				if rollback {
					require.NoError(t, tx.Rollback())
					want = sol1474Large(id)
					deleted = created
				} else {
					require.NoError(t, tx.Commit())
					if publication != "sync" { require.True(t, tx.AcknowledgedBuffered()) }
				}
				if publication == "buffered" {
					require.NoError(t, e.FlushWriteBehind())
				} else if publication == "recovery" {
					crashClose(t, e, wal)
					e, wal = newWriteBehindWALTestEngine(t, dir, time.Hour)
					_, err = RecoverBufferedCommits(e, wal)
					require.NoError(t, err)
				}
				if deleted {
					_, err := e.GetNode(id)
					require.ErrorIs(t, err, ErrNotFound)
					require.Zero(t, sol1474Keys(t, e, id))
					return
				}
				got, err := e.GetNode(id)
				require.NoError(t, err)
				require.True(t, sameChunkEmbeddings(want.ChunkEmbeddings, got.ChunkEmbeddings), "committed vectors must match the final public write: expected %d chunks, got %d", len(want.ChunkEmbeddings), len(got.ChunkEmbeddings))
				keys := 0
				if len(want.ChunkEmbeddings) == 16 { keys = 16 }
				require.Equal(t, keys, sol1474Keys(t, e, id), "no obsolete physical range")
			})
		}
	}
}

func TestRelease1285OwnerForms(t *testing.T) {
	for _, form := range []string{"sharded", "sidecar", "metadata-only"} {
		for _, action := range []string{"delete", "invalidate", "replace", "rollback"} {
			t.Run(form+"/"+action, func(t *testing.T) {
				e := newTestEngine(t)
				id := NodeID("test:λ/owner")
				n := sol1474Large(id)
				if form == "sharded" {
					n.ChunkEmbeddings = [][]float32{make([]float32, 100000)}
					n.ChunkEmbeddings[0][99999] = 71
				} else { n.ChunkEmbeddings = nil }
				_, err := e.CreateNode(n)
				require.NoError(t, err)
				if form != "sharded" {
					n, err = e.GetNode(id)
					require.NoError(t, err)
					n.EmbedMeta = map[string]any{"chunk_count":16, "provider":"probe"}
					if form == "metadata-only" { n.EmbedMeta["chunk_count"] = 0 }
					if form == "sidecar" { n.ChunkEmbeddings = sol1474Large(id).ChunkEmbeddings }
					require.NoError(t, e.UpdateNodeEmbeddingSidecar(n))
				}
				before := sol1474Keys(t, e, id)
				require.Positive(t, before)
				tx, err := e.BeginTransaction()
				require.NoError(t, err)
				defer tx.Rollback()
				if action == "delete" || action == "rollback" {
					require.NoError(t, tx.DeleteNode(id))
				} else {
					n, err := tx.GetNode(id)
					require.NoError(t, err)
					n.ChunkEmbeddings, n.EmbedMeta = nil, nil
					if action == "replace" { n.ChunkEmbeddings = [][]float32{{19,23}} } else { n.Properties["text"] = "different" }
					require.NoError(t, tx.UpdateNode(n))
				}
				if action == "rollback" {
					require.NoError(t, tx.Rollback())
					require.Equal(t, before, sol1474Keys(t, e, id))
				} else {
					require.NoError(t, tx.Commit())
					require.Zero(t, sol1474Keys(t, e, id))
				}
			})
		}
	}
}
