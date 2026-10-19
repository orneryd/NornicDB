package nornicdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// An asynchronous initializer may finish loading its model after shutdown.
// Installing that model must not restart a closed database's embed workers.
func TestReleaseEmbedderInstallationAfterCloseDoesNotRestartDB(t *testing.T) {
	db, err := Open("", nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	t.Cleanup(db.StopEmbedQueue)
	require.NoError(t, db.Close())

	db.SetEmbedder(&mockEmbedder{dims: 4, model: "late-initializer"})

	assert.Nil(t, db.GetCypherExecutor().GetEmbedder(),
		"a closed database must refuse late embedder initialization")
	if queue := db.GetEmbedQueue(); queue != nil {
		assert.True(t, queue.closed.Load(), "late initialization must not start a new queue")
	}
}
