package nornicdb

import (
	"context"
	"errors"
	"log"
	"sync"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/embed"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// An administrator's pause of automatic embedding (db.embedding.pause, the
// /nornicdb/embed/pause route; #1021) is kept in the system database, so a
// restart starts paused, before any worker runs.
const (
	embeddingControlNodeID = storage.NodeID("embedding_control:automatic")
	embeddingControlLabel  = "_EmbeddingControl"
)

// embeddingPause is the pause state every automatic embedding path reads:
// the node workers (EmbedWorker.SetPaused) and relationship search
// (wait). Foreground embedding (query text, reranking) never reads it.
type embeddingPause struct {
	// setMu serializes changes, so the stored and the applied state agree.
	setMu sync.Mutex

	mu     sync.Mutex
	paused bool
	// resumed is closed by the resume that ends a pause.
	resumed chan struct{}
}

func (p *embeddingPause) isPaused() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.paused
}

func (p *embeddingPause) set(paused bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.paused == paused {
		return
	}
	p.paused = paused
	if paused {
		p.resumed = make(chan struct{})
	} else {
		close(p.resumed)
	}
}

// wait blocks while paused, until the resume or ctx's end.
func (p *embeddingPause) wait(ctx context.Context) error {
	p.mu.Lock()
	if !p.paused {
		p.mu.Unlock()
		return nil
	}
	resumed := p.resumed
	p.mu.Unlock()
	select {
	case <-resumed:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (db *DB) embeddingControlStorage() storage.Engine {
	if db.baseStorage == nil {
		return nil
	}
	return storage.NewNamespacedEngine(db.baseStorage, "system")
}

// loadEmbeddingPause restores the stored pause at Open. A state that can't
// be read starts paused: a restart must not start the backlog an
// administrator paused; status reports it and a resume clears it.
func (db *DB) loadEmbeddingPause() {
	system := db.embeddingControlStorage()
	if system == nil {
		return
	}
	node, err := system.GetNode(embeddingControlNodeID)
	if errors.Is(err, storage.ErrNotFound) {
		return
	}
	if err != nil {
		log.Printf("⚠️  Automatic embedding starts paused: its stored pause state could not be read: %v", err)
		db.embeddingPause.set(true)
		return
	}
	if paused, _ := node.Properties["paused"].(bool); paused {
		log.Printf("⏸️  Automatic embedding is paused (db.embedding.resume() resumes it)")
		db.embeddingPause.set(true)
	}
}

func (db *DB) storeEmbeddingPause(paused bool) error {
	system := db.embeddingControlStorage()
	if system == nil {
		return nil
	}
	node := &storage.Node{
		ID:         embeddingControlNodeID,
		Labels:     []string{embeddingControlLabel, "_System"},
		Properties: map[string]any{"paused": paused},
	}
	existing, err := system.GetNode(embeddingControlNodeID)
	if errors.Is(err, storage.ErrNotFound) {
		if _, err = system.CreateNode(node); !errors.Is(err, storage.ErrAlreadyExists) {
			return err
		}
		// Written meanwhile (a buffered write becoming visible): update it.
		return system.UpdateNode(node)
	}
	if err != nil {
		return err
	}
	node.CreatedAt = existing.CreatedAt
	return system.UpdateNode(node)
}

// SetEmbeddingPaused pauses or resumes automatic embedding, for every
// database, and keeps the state across restarts. Paused, the workers claim
// no new node; a node already claimed is embedded and written, and pending
// nodes stay pending (none is dropped or marked failed). Resuming has the
// workers take the pending nodes, those queued during the pause included.
// It is idempotent and returns the state in effect.
func (db *DB) SetEmbeddingPaused(_ context.Context, paused bool) (cypher.EmbeddingStatus, error) {
	db.embeddingPause.setMu.Lock()
	defer db.embeddingPause.setMu.Unlock()
	if db.embeddingPause.isPaused() != paused {
		if err := db.storeEmbeddingPause(paused); err != nil {
			return db.EmbeddingStatus(), err
		}
		db.embeddingPause.set(paused)
		db.mu.RLock()
		queue := db.embedQueue
		db.mu.RUnlock()
		if queue != nil {
			queue.SetPaused(paused)
		}
		if paused {
			log.Printf("⏸️  Automatic embedding paused")
		} else {
			log.Printf("▶️  Automatic embedding resumed")
		}
	}
	return db.EmbeddingStatus(), nil
}

// EmbeddingStatus reports whether automatic embedding is paused, and the
// nodes waiting for it (PendingEmbeddingsCount).
func (db *DB) EmbeddingStatus() cypher.EmbeddingStatus {
	return cypher.EmbeddingStatus{Paused: db.embeddingPause.isPaused(), Pending: db.PendingEmbeddingsCount()}
}

// newEmbedQueue creates the embed queue in the current pause state, so no
// queue created later (a provider loaded or reloaded) starts unpaused. Call
// it with db.mu held, as SetEmbeddingPaused reads the queue under it.
func (db *DB) newEmbedQueue(embedder embed.Embedder, config *EmbedWorkerConfig) *EmbedQueue {
	if config == nil {
		config = DefaultEmbedWorkerConfig() // as NewEmbedWorker defaults it
	}
	workerConfig := *config
	workerConfig.StartPaused = db.embeddingPause.isPaused()
	return NewEmbedQueue(embedder, db.baseStorage, &workerConfig)
}
