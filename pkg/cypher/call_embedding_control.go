package cypher

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/localization"
)

// EmbeddingStatus is the state db.embedding.pause, resume and status report
// (and the /nornicdb/embed/pause, resume and status routes, #1021): whether
// automatic embedding is paused, and how many nodes wait for it.
type EmbeddingStatus struct {
	Paused  bool `json:"paused"`
	Pending int  `json:"pending"`
}

// EmbeddingController pauses and resumes a database's automatic embedding;
// the nornicdb.DB implements it.
type EmbeddingController interface {
	SetEmbeddingPaused(ctx context.Context, paused bool) (EmbeddingStatus, error)
	EmbeddingStatus() EmbeddingStatus
}

// SetEmbeddingController sets what db.embedding.pause, resume and status
// act on. Executors sharing this one's embedder share it too
// (ShareEmbedderFrom).
func (e *StorageExecutor) SetEmbeddingController(controller EmbeddingController) {
	e.embeddingController = controller
}

// callEmbeddingControl implements db.embedding.pause() (pause true),
// db.embedding.resume() (false) and db.embedding.status() (nil): each
// returns the state in effect, one row (paused, pending).
func (e *StorageExecutor) callEmbeddingControl(ctx context.Context, pause *bool) (*ExecuteResult, error) {
	controller := e.embeddingController
	if controller == nil {
		return nil, localizedError(localization.NornicDBCoreAutoEmbedNotEnabled(), nil)
	}
	status := controller.EmbeddingStatus()
	if pause != nil {
		var err error
		if status, err = controller.SetEmbeddingPaused(ctx, *pause); err != nil {
			return nil, err
		}
	}
	return &ExecuteResult{
		Columns: []string{"paused", "pending"},
		Rows:    [][]interface{}{{status.Paused, int64(status.Pending)}},
	}, nil
}
