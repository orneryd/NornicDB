package nornicdb

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func requireDone(t *testing.T, done <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("%s did not end with its owner", what)
	}
}

// Before Open sets buildCtx, the DB's background work runs under the DB
// owner itself, so stopping the owner ends it (no substituted Background).
func TestLifetimeContextFallsBackToDBOwner(t *testing.T) {
	db := &DB{}
	ctx := db.LifetimeContext()
	require.NoError(t, ctx.Err())
	db.work.CloseAdmission()
	db.work.Cancel()
	requireDone(t, ctx.Done(), "the DB lifetime context")
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
		requireDone(t, gen.Context().Done(), "the constructed worker generation")
		requireDone(t, w.ctx.Done(), "the worker context")
	})
	t.Run("lazy", func(t *testing.T) {
		w := &EmbedWorker{}
		gen := w.generation()
		w.work.CloseAdmission()
		w.work.Cancel()
		requireDone(t, gen.Context().Done(), "the lazy worker generation")
	})
}
