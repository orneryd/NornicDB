package storage

import (
	"errors"

	"github.com/dgraph-io/badger/v4"
)

// StoredEntityPresenceReader is the existence-only projection of StoredReader.
// It retains hidden entities and acknowledged overlays without hydrating body
// properties or vectors when an accounting fold only needs the population.
type StoredEntityPresenceReader interface {
	StoredNodeExists(NodeID) (bool, error)
	StoredEdgeExists(EdgeID) (bool, error)
}

func StoredNodeExists(engine Engine, id NodeID) (bool, error) {
	if reader, ok := StoredReader(engine); ok {
		if presence, ok := reader.(StoredEntityPresenceReader); ok {
			return presence.StoredNodeExists(id)
		}
	}
	node, err := GetStoredNode(engine, id)
	if errors.Is(err, ErrNotFound) {
		return false, nil
	}
	return node != nil, err
}

func StoredEdgeExists(engine Engine, id EdgeID) (bool, error) {
	if reader, ok := StoredReader(engine); ok {
		if presence, ok := reader.(StoredEntityPresenceReader); ok {
			return presence.StoredEdgeExists(id)
		}
	}
	edge, err := GetStoredEdge(engine, id)
	if errors.Is(err, ErrNotFound) {
		return false, nil
	}
	return edge != nil, err
}

func (b *BadgerEngine) StoredNodeExists(id NodeID) (bool, error) {
	return b.storedEntityExists(bufferedNodeEntity, string(id), nodeKey(id))
}

func (b *BadgerEngine) StoredEdgeExists(id EdgeID) (bool, error) {
	return b.storedEntityExists(bufferedEdgeEntity, string(id), edgeKey(id))
}

func (b *BadgerEngine) storedEntityExists(kind bufferedEntityKind, id string, key []byte) (bool, error) {
	if id == "" {
		return false, ErrInvalidID
	}
	if err := b.ensureOpen(); err != nil {
		return false, err
	}
	var exists bool
	cached := func(w *WriteBehindBuffer) (bool, error) {
		if w != nil {
			state := w.entityStateLocked(kind, id)
			if state.found {
				exists = !state.deleted
				return true, nil
			}
		}
		return false, nil
	}
	// Use the same capture/retirement owner as full stored reads: an overlay
	// miss and physical snapshot capture cannot straddle replay retirement.
	namespace, _, _ := ParseDatabasePrefix(id)
	view, err := b.captureReadView(nil, readViewOptions{scope: namespace, cached: cached})
	if err != nil || view == nil {
		return exists, err
	}
	err = view.readAndClose(func(txn *badger.Txn) error {
		_, err := txn.Get(key)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		exists = err == nil
		return err
	})
	return exists, err
}

func (n *NamespacedEngine) StoredNodeExists(id NodeID) (bool, error) {
	return StoredNodeExists(n.inner, n.prefixNodeID(id))
}
func (n *NamespacedEngine) StoredEdgeExists(id EdgeID) (bool, error) {
	return StoredEdgeExists(n.inner, n.prefixEdgeID(id))
}
func (w *WALEngine) StoredNodeExists(id NodeID) (bool, error) { return StoredNodeExists(w.engine, id) }
func (w *WALEngine) StoredEdgeExists(id EdgeID) (bool, error) { return StoredEdgeExists(w.engine, id) }
