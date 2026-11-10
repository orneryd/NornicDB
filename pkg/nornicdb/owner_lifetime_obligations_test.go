package nornicdb

import (
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
)

// Before Open sets buildCtx, the DB's background work runs under the DB
// owner itself, so stopping the owner ends it (no substituted Background).
func TestLifetimeContextFallsBackToDBOwner(t *testing.T) {
	db := &DB{}
	ctx := db.LifetimeContext()
	require.NoError(t, ctx.Err())
	db.work.CloseAdmission()
	db.work.Cancel()
	fixtureowner.WaitForBarrier(t, ctx.Done(), 5*time.Second, nil, "the DB lifetime context did not end with its owner")
}

// Every embed worker generation is rooted at the worker's own owner: the
// constructed one, a zero-value worker's lazy one, and the one Reset
// publishes.
func TestEmbedWorkerGenerationsEndWithWorkerOwner(t *testing.T) {
	t.Run("constructed", func(t *testing.T) {
		w := NewEmbedWorker(nil, nil, &EmbedWorkerConfig{NumWorkers: 0, EmbedBatchSize: 1, ChunkSize: 512, MaxRetries: 1, DeferWorkerStart: true})
		gen := w.generation()
		require.NoError(t, gen.Context().Err())
		w.work.CloseAdmission()
		w.work.Cancel()
		fixtureowner.WaitForBarrier(t, gen.Context().Done(), 5*time.Second, nil, "the constructed worker generation did not end with its owner")
		fixtureowner.WaitForBarrier(t, w.ctx.Done(), 5*time.Second, nil, "the worker context did not end with its owner")
	})
	t.Run("lazy", func(t *testing.T) {
		w := &EmbedWorker{}
		gen := w.generation()
		w.work.CloseAdmission()
		w.work.Cancel()
		fixtureowner.WaitForBarrier(t, gen.Context().Done(), 5*time.Second, nil, "the lazy worker generation did not end with its owner")
	})
}
