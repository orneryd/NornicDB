package storage

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"

	"github.com/dgraph-io/badger/v4"
)

// The pending marker carries the admission that created the work, including
// while workers have not started. Runtime epochs from a previous process are
// recovered as startup work (epoch zero), before this process can DROP.
var pendingEmbeddingOriginSession = func() [16]byte {
	var session [16]byte
	_, _ = rand.Read(session[:])
	return session
}()

func encodePendingEmbeddingOrigin(origin uint64) []byte {
	value := make([]byte, 24)
	copy(value, pendingEmbeddingOriginSession[:])
	binary.LittleEndian.PutUint64(value[16:], origin)
	return value
}

// decodePendingEmbeddingOrigin distinguishes an explicit admission from a
// recovered marker whose original admission is unavailable. Both observe epoch
// zero on recovery; requeue may supply the identity of its still-owned attempt.
func decodePendingEmbeddingOrigin(value []byte) (uint64, bool) {
	if len(value) != 24 || !bytes.Equal(value[:16], pendingEmbeddingOriginSession[:]) {
		return 0, false
	}
	return binary.LittleEndian.Uint64(value[16:]), true
}

// This is transport of an existing identity, without reading a schema clock.
func (node *Node) bindEmbeddingWorkOrigin(origin uint64) {
	state := node.runtimeState()
	if state.embeddingOriginBound && state.embeddingWorkOrigin == origin {
		return
	}
	state.embeddingWorkOrigin, state.embeddingOriginBound = origin, true
	node.setRuntimeState(state)
}

func (node *Node) EmbeddingWorkOrigin() (uint64, bool) {
	state := node.runtimeState()
	return state.embeddingWorkOrigin, state.embeddingOriginBound
}

// Select the marker with the exact transaction that selected the body.
func readPendingEmbeddingOrigin(txn *badger.Txn, id NodeID, node *Node) error {
	item, err := txn.Get(pendingEmbedKey(id))
	if err == badger.ErrKeyNotFound {
		return nil
	}
	if err != nil {
		return err
	}
	return item.Value(func(value []byte) error {
		origin, _ := decodePendingEmbeddingOrigin(value)
		node.bindEmbeddingWorkOrigin(origin)
		return nil
	})
}
