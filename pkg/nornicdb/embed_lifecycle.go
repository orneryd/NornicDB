package nornicdb

import (
	"reflect"

	"github.com/orneryd/nornicdb/pkg/embed"
)

// The embedding lifecycle (F-EMBED). The DB is the one owner of its embed
// queue and of every embedder it takes:
//
//   - db.embedQueue is published under db.mu and touched only in this file
//     (TestEmbedQueueFieldOwnedByLifecycle). Everyone else reads it with
//     GetEmbedQueue, or with embedQueueLocked while already holding db.mu.
//   - SetEmbedder and the per-database registry decide under db.mu: a closing
//     database refuses an embedder and closes it at once; one it takes is
//     owned until Close. A provider reload's previous embedder stays owned,
//     as workers may still hold it, so each reload holds one model until
//     Close.
//   - Close marks the database closed (closingEmbedQueue), joins every user
//     of the embedders (workers, the server's initialisation task, search),
//     and then closes each owned embedder once (closeOwnedEmbedders).

// GetEmbedQueue returns the embedding queue for this database.
// This is used by GraphQL and other integrations that need to wire up
// embedding callbacks for namespaced executors.
func (db *DB) GetEmbedQueue() *EmbedQueue {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.embedQueue
}

// embedQueueLocked returns the queue to a caller that holds db.mu.
func (db *DB) embedQueueLocked() *EmbedQueue {
	return db.embedQueue
}

// setEmbedQueueLocked publishes queue; the caller holds db.mu for writing.
func (db *DB) setEmbedQueueLocked(queue *EmbedQueue) {
	db.embedQueue = queue
}

// StopEmbedQueue stops the embed queue and waits for workers to exit.
// Call this as soon as shutdown is requested (e.g. on SIGINT) so embeddings stop
// before the server is torn down. Safe to call if already stopped or never started.
func (db *DB) StopEmbedQueue() {
	db.mu.Lock()
	q := db.embedQueue
	db.embedQueue = nil
	db.mu.Unlock()
	if q != nil {
		q.Close()
	}
}

// enqueueMutatedNode queues a node a Cypher statement created or changed.
func (db *DB) enqueueMutatedNode(nodeID string) {
	if queue := db.GetEmbedQueue(); queue != nil {
		queue.Enqueue(nodeID)
	}
}

// closingEmbedQueue marks the database closed, so SetEmbedder and the
// registry refuse from here on, and returns the queue for Close to stop.
func (db *DB) closingEmbedQueue() *EmbedQueue {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.closed = true
	return db.embedQueue
}

// adoptEmbedderLocked records an embedder the database took; the caller
// holds db.mu and has checked that the database isn't closed.
func (db *DB) adoptEmbedderLocked(embedder embed.Embedder) {
	for _, owned := range db.ownedEmbedders {
		if sameEmbedder(owned, embedder) {
			return
		}
	}
	db.ownedEmbedders = append(db.ownedEmbedders, embedder)
}

// adoptEmbedder takes an embedder the registry built, unless the database
// is closing; then it closes the embedder and reports false.
func (db *DB) adoptEmbedder(embedder embed.Embedder) bool {
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		closeEmbedder(embedder)
		return false
	}
	db.adoptEmbedderLocked(embedder)
	db.mu.Unlock()
	return true
}

// closeOwnedEmbedders closes every embedder the database took, once. Close
// calls it after joining the workers, initialisation and search, the last
// users of the embedders.
func (db *DB) closeOwnedEmbedders() {
	db.mu.Lock()
	owned := db.ownedEmbedders
	db.ownedEmbedders = nil
	db.mu.Unlock()
	for _, embedder := range owned {
		closeEmbedder(embedder)
	}
}

// closeEmbedder releases what an embedder holds (a local model and its
// warmup goroutine); one without Close holds nothing to release.
func closeEmbedder(embedder embed.Embedder) {
	if closer, ok := embedder.(interface{ Close() error }); ok {
		_ = closer.Close()
	}
}

// sameEmbedder compares two embedders without panicking on a dynamic type
// that isn't comparable.
func sameEmbedder(a, b embed.Embedder) bool {
	typ := reflect.TypeOf(a)
	return typ == reflect.TypeOf(b) && typ != nil && typ.Comparable() && a == b
}
