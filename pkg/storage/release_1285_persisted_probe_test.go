package storage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRelease1285PersistedReplacement(t *testing.T) {
	for _, publication := range []string{"sync", "buffered"} {
		for _, start := range []string{"created", "committed"} {
			t.Run(publication+"/"+start, func(t *testing.T) {
				dir := t.TempDir()
				e, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir:dir, WriteBehind:publication == "buffered", WriteBehindInterval:time.Hour})
				require.NoError(t, err)
				defer func() { _ = e.Close() }()
				id := NodeID("test:persisted-replacement")
				if start == "committed" { _, err = e.CreateNode(sol1474Large(id)); require.NoError(t, err) }
				tx, err := e.BeginTransaction()
				require.NoError(t, err)
				defer tx.Rollback()
				if publication == "buffered" { require.NoError(t, tx.SetImplicit(true)) }
				if start == "created" { _, err = tx.CreateNode(sol1474Large(id)); require.NoError(t, err) }
				n := sol1474Large(id)
				for i := range n.ChunkEmbeddings { n.ChunkEmbeddings[i][0] = 99 }
				require.NoError(t, tx.UpdateNode(n))
				require.NoError(t, tx.Commit())
				if publication == "buffered" { require.True(t, tx.AcknowledgedBuffered()); require.NoError(t, e.FlushWriteBehind()) }
				require.NoError(t, e.Close())
				e, err = NewBadgerEngine(dir)
				require.NoError(t, err)
				got, err := e.GetNode(id)
				require.NoError(t, err)
				require.True(t, sameChunkEmbeddings(n.ChunkEmbeddings, got.ChunkEmbeddings), "the final replacement must survive close/reopen; cached values are insufficient proof")
			})
		}
	}
}
