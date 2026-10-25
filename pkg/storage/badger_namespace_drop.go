package storage

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"strings"

	"github.com/dgraph-io/badger/v4"
)

const prefixMVCCMetaNamespaceDrop = byte(0x16)
const namespaceDropChunkIDs = 4096

// namespaceWriteKey carries a metadata namespace to the existing prephysical
// commit owner without pretending that a schema is a graph entity.
type namespaceWriteKey string

func wholeNamespaceFromPrefix(prefix string) (string, bool) {
	namespace, whole := strings.CutSuffix(prefix, ":")
	return namespace, whole && namespace != "" && !strings.Contains(namespace, ":")
}

// namespaceDropIntent is the one pending whole-namespace destructive operation.
// Numeric victims survive dictionary cleanup and are immutable until retirement.
type namespaceDropIntent struct {
	namespace                  string
	nodesDeleted, edgesDeleted int64
	nodes, edges               map[uint64]struct{}
}

func (b *BadgerEngine) pendingNamespaceConflict(namespace string) error {
	if b.prefixDrops.pendingCount.Load() == 0 {
		return nil
	}
	if err := b.ensureOpen(false); err != nil {
		return err
	}
	b.prefixDrops.mu.Lock()
	defer b.prefixDrops.mu.Unlock()
	if namespace == "" {
		for ns := range b.prefixDrops.pending {
			return fmt.Errorf("%w: namespace %q has unfinished DROP cleanup", ErrConflict, ns)
		}
	} else if b.prefixDrops.pending[namespace] != nil {
		return fmt.Errorf("%w: namespace %q has unfinished DROP cleanup", ErrConflict, strings.Clone(namespace))
	}
	return nil
}

func (b *BadgerEngine) pendingPrefixConflict(prefix string) error {
	return b.pendingPrefixConflictWithContext(nil, prefix)
}

// pendingPrefixConflictWithContext keeps scope admission in the DROP owner.
// A nil context belongs to an existing context-free/physical entrance.
func (b *BadgerEngine) pendingPrefixConflictWithContext(ctx context.Context, prefix string) error {
	if b.prefixDrops.pendingCount.Load() == 0 {
		return nil
	}
	if err := b.ensureOpen(false); err != nil {
		return err
	}
	b.prefixDrops.mu.Lock()
	defer b.prefixDrops.mu.Unlock()
	for ns := range b.prefixDrops.pending {
		if ctx != nil {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		scope := ns + ":"
		if strings.HasPrefix(scope, prefix) || strings.HasPrefix(prefix, scope) {
			return fmt.Errorf("%w: namespace %q has unfinished DROP cleanup", ErrConflict, ns)
		}
	}
	return nil
}

func (b *BadgerEngine) pendingNamespaceDrop(namespace string) *namespaceDropIntent {
	b.prefixDrops.mu.Lock()
	defer b.prefixDrops.mu.Unlock()
	return b.prefixDrops.pending[namespace]
}

type namespaceDropTransition uint8

const (
	namespaceDropInstall namespaceDropTransition = iota
	namespaceDropRetire
	namespaceDropReplace
	namespaceDropRelease
)

// transitionPending is the only pending-map/count writer. Its caller owns the
// destructive lifecycle; installation/replacement additionally hold commitGate.
func (b *BadgerEngine) transitionPending(action namespaceDropTransition, namespace string, intents []*namespaceDropIntent) {
	drops := &b.prefixDrops
	// Installation/replacement owns the physical cut against all metadata and
	// entity writers; every caller already owns destructive lifecycle admission.
	if action == namespaceDropInstall || action == namespaceDropReplace {
		b.db.commitGate.Lock()
		defer b.db.commitGate.Unlock()
	}
	drops.mu.Lock()
	defer drops.mu.Unlock()
	switch action {
	case namespaceDropInstall:
		if drops.pending == nil {
			drops.pending = make(map[string]*namespaceDropIntent)
		}
		drops.pending[intents[0].namespace] = intents[0]
	case namespaceDropRetire:
		delete(drops.pending, namespace)
	case namespaceDropRelease:
		drops.pending = nil
	case namespaceDropReplace:
		drops.pending = make(map[string]*namespaceDropIntent, len(intents))
		for _, intent := range intents {
			drops.pending[intent.namespace] = intent
		}
	}
	drops.pendingCount.Store(int64(len(drops.pending)))
}

func namespaceDropKey(namespace string, kind byte, chunk uint64) []byte {
	key := []byte{prefixMVCCMeta, prefixMVCCMetaNamespaceDrop}
	key = binary.AppendUvarint(key, uint64(len(namespace)))
	key = append(key, namespace...)
	key = append(key, kind)
	if kind != 0 {
		key = binary.BigEndian.AppendUint64(key, chunk)
	}
	return key
}

func (intent *namespaceDropIntent) head() []byte {
	value := []byte{1}
	for _, count := range []uint64{uint64(intent.nodesDeleted), uint64(intent.edgesDeleted), uint64(len(intent.nodes)), uint64(len(intent.edges))} {
		value = binary.BigEndian.AppendUint64(value, count)
	}
	return value
}

// writeNamespaceDropIntent uses the current atomic/large commit owner. The head
// and bounded victim chunks become visible together, before any destructive I/O.
func (b *BadgerEngine) writeNamespaceDropIntent(intent *namespaceDropIntent, lifecycle *lifecycleWrite) error {
	err := b.commitEngineWrite(func(cw *commitWriter) error {
		for kind, victims := range map[byte]map[uint64]struct{}{1: intent.nodes, 2: intent.edges} {
			if len(victims) == 0 {
				continue
			}
			chunk := uint64(0)
			value := make([]byte, 0, namespaceDropChunkIDs*8)
			flush := func() error {
				if len(value) == 0 {
					return nil
				}
				key, data := namespaceDropKey(intent.namespace, kind, chunk), value
				if err := cw.write(func(txn *badger.Txn) error { return txn.Set(key, data) }); err != nil {
					return err
				}
				chunk++
				value = make([]byte, 0, namespaceDropChunkIDs*8)
				return nil
			}
			for id := range victims {
				value = binary.BigEndian.AppendUint64(value, id)
				if len(value) == namespaceDropChunkIDs*8 {
					if err := flush(); err != nil {
						return err
					}
				}
			}
			if err := flush(); err != nil {
				return err
			}
		}
		return cw.write(func(txn *badger.Txn) error { return txn.Set(namespaceDropKey(intent.namespace, 0, 0), intent.head()) })
	}, engineWriteOptions{lifecycle: lifecycle})
	if err != nil {
		return err
	}
	b.transitionPending(namespaceDropInstall, "", []*namespaceDropIntent{intent})
	return b.db.syncIfPersistent()
}

func (b *BadgerEngine) completeNamespaceDrop(intent *namespaceDropIntent, lifecycle *lifecycleWrite) error {
	// Earlier independently committed cleanup must be durable before a
	// completion tombstone can possibly become durable (as in largeCommit.finish).
	if err := b.db.syncIfPersistent(); err != nil {
		return err
	}
	err := b.commitEngineWrite(func(cw *commitWriter) error {
		for kind, count := range map[byte]int{1: len(intent.nodes), 2: len(intent.edges)} {
			for chunk := 0; chunk < (count+namespaceDropChunkIDs-1)/namespaceDropChunkIDs; chunk++ {
				key := namespaceDropKey(intent.namespace, kind, uint64(chunk))
				if err := cw.write(func(txn *badger.Txn) error { return txn.Delete(key) }); err != nil {
					return err
				}
			}
		}
		return cw.write(func(txn *badger.Txn) error {
			if err := txn.Delete(schemaKey(intent.namespace)); err != nil {
				return err
			}
			return txn.Delete(namespaceDropKey(intent.namespace, 0, 0))
		})
	}, engineWriteOptions{lifecycle: lifecycle})
	if err != nil {
		return err
	}
	// This is the same per-engine intrinsic-tail seam's second boundary:
	// final metadata is visible, but its durable completion is still unconfirmed.
	if b.dropPrefixTailHook != nil {
		if err := b.dropPrefixTailHook(intent.namespace + ":"); err != nil {
			return err
		}
	}
	// If syncing the tombstones fails, memory admission stays fenced. Retry
	// repeats the same final atomic metadata operation before publishing success.
	return b.db.syncIfPersistent()
}

func (b *BadgerEngine) loadNamespaceDrops() ([]*namespaceDropIntent, error) {
	var intents []*namespaceDropIntent
	err := b.db.viewHeld(func(txn *badger.Txn) error {
		prefix := []byte{prefixMVCCMeta, prefixMVCCMetaNamespaceDrop}
		it := txn.NewIterator(badgerPrefixIteratorOptions(prefix))
		defer it.Close()
		for it.Rewind(); it.ValidForPrefix(prefix); it.Next() {
			key := it.Item().Key()
			if len(key) < 4 {
				return fmt.Errorf("invalid namespace DROP intent key")
			}
			length, used := binary.Uvarint(key[2:])
			if used <= 0 || used > len(key)-3 || length > uint64(len(key)-2-used-1) {
				return fmt.Errorf("invalid namespace DROP intent key")
			}
			end := 2 + used + int(length)
			if len(key) != end+1 || key[end] != 0 {
				continue
			}
			namespace := string(key[2+used : end])
			if namespace == "" || strings.Contains(namespace, ":") {
				return fmt.Errorf("invalid namespace DROP intent namespace")
			}
			intent := &namespaceDropIntent{namespace: namespace, nodes: make(map[uint64]struct{}), edges: make(map[uint64]struct{})}
			var counts [4]uint64
			if err := it.Item().Value(func(value []byte) error {
				if len(value) != 33 || value[0] != 1 {
					return fmt.Errorf("unsupported namespace DROP intent")
				}
				for i := range counts {
					counts[i] = binary.BigEndian.Uint64(value[1+i*8:])
				}
				return nil
			}); err != nil {
				return err
			}
			intent.nodesDeleted, intent.edgesDeleted = int64(counts[0]), int64(counts[1])
			if intent.nodesDeleted < 0 || intent.edgesDeleted < 0 {
				return fmt.Errorf("invalid namespace DROP intent counts")
			}
			for kind, count := range map[byte]uint64{1: counts[2], 2: counts[3]} {
				victims := intent.nodes
				if kind == 2 {
					victims = intent.edges
				}
				for offset := uint64(0); offset < count; {
					item, err := txn.Get(namespaceDropKey(namespace, kind, offset/namespaceDropChunkIDs))
					if err != nil {
						return err
					}
					want := count - offset
					if want > namespaceDropChunkIDs {
						want = namespaceDropChunkIDs
					}
					if err := item.Value(func(value []byte) error {
						if uint64(len(value)) != want*8 {
							return fmt.Errorf("invalid namespace DROP victim chunk")
						}
						for len(value) > 0 {
							victims[binary.BigEndian.Uint64(value)] = struct{}{}
							value = value[8:]
						}
						return nil
					}); err != nil {
						return err
					}
					offset += want
				}
			}
			intents = append(intents, intent)
		}
		return nil
	})
	return intents, err
}

// recoverNamespaceDrops runs at open or under Restore's existing lifecycle.
// Only open obtains a lease here; Restore must never recursively take its W lock.
func (b *BadgerEngine) recoverNamespaceDrops(held *lifecycleWrite) error {
	intents, err := b.loadNamespaceDrops()
	if err != nil {
		return err
	}
	if held != nil {
		// Restore replaced the entire physical store. Its pending namespace state
		// must come from that store, not from the engine image that was replaced.
		b.transitionPending(namespaceDropReplace, "", intents)
	}
	if len(intents) == 0 {
		return nil
	}
	if held == nil {
		lease, err := b.beginLifecycleWrite("")
		if err != nil {
			return err
		}
		defer lease.finish()
		held = &lease
	}
	for _, intent := range intents {
		b.transitionPending(namespaceDropInstall, "", []*namespaceDropIntent{intent})
		labelReset := &labelIndexReset{b: b, offline: true}
		_, _, _, dropErr := b.deleteByPrefixHeld(intent.namespace+":", held, true, labelReset)
		if err := labelReset.finish(dropErr); err != nil {
			return err
		}
	}
	return nil
}

func (cw *commitWriter) pendingNamespaceWriteConflict() error {
	if cw.engine.prefixDrops.pendingCount.Load() == 0 {
		return nil
	}
	for _, key := range cw.publishKeys {
		var namespace string
		switch id := key.(type) {
		case NodeID:
			namespace = namespaceForNodeID(id)
		case EdgeID:
			namespace = namespaceForEdgeID(id)
		case namespaceWriteKey:
			namespace = string(id)
		default:
			continue
		}
		if err := cw.engine.pendingNamespaceConflict(namespace); err != nil {
			return err
		}
	}
	return nil
}

// Kept local to this operation: no normal namespace cleanup owns unfinished
// intent records, even while dictionaries and all graph bodies have gone.
func isNamespaceDropIntentKey(key []byte) bool {
	return bytes.HasPrefix(key, []byte{prefixMVCCMeta, prefixMVCCMetaNamespaceDrop})
}
