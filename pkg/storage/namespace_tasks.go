package storage

import (
	"context"
	"encoding/binary"
	"fmt"
	"strings"
	"sync"

	"github.com/orneryd/nornicdb/pkg/util"
)

func (b *BadgerEngine) namespaceTaskGate(namespace string) *sync.RWMutex {
	value, ok := b.namespaceTaskGates.Load(namespace)
	if !ok {
		value, _ = b.namespaceTaskGates.LoadOrStore(namespace, new(sync.RWMutex))
	}
	return value.(*sync.RWMutex)
}

func namespaceForSideStateID(entityID string) string {
	entityID = strings.TrimPrefix(entityID, policyWorkMetaIDPrefix)
	entityID = strings.TrimPrefix(entityID, "deindex:")
	namespace, _, _ := ParseDatabasePrefix(entityID)
	return namespace
}

// Background batches capture their logical lifetime before loading work. Their
// target lease covers all read/modify/write and follow-up effects, so a drained
// old batch cannot write into a recreated namespace.
func (b *BadgerEngine) CaptureNamespaceTask() any { return b.namespaceLifetimeRevision() }

func (b *BadgerEngine) RunNamespaceTask(token any, entityID string, run func() error) error {
	namespace := namespaceForSideStateID(entityID)
	gate := b.namespaceTaskGate(namespace)
	gate.RLock()
	defer gate.RUnlock()
	if err := b.checkNamespaceOperation(namespace); err != nil {
		return err
	}
	if revision, ok := token.(uint64); ok {
		if state := b.namespaceLifetimes.Load(); state != nil && state.retired[namespace] > revision {
			return fmt.Errorf("%w: background task predates namespace retirement", ErrConflict)
		}
	}
	return run()
}

func (b *BadgerEngine) admitNamespaceTasks(ctx context.Context, namespace string) (func(), error) {
	gate := b.namespaceTaskGate(namespace)
	return util.LockContext(ctx, gate.Lock, gate.Unlock)
}

func (b *BadgerEngine) withNamespaceSideStateWrite(entityID string, write func() error) error {
	return b.withNamespaceSideStateWriteAt(entityID, b.namespaceLifetimeRevision(), write)
}

// A synchronous writer captures its lifetime before draining buffered work;
// retirement during that wait must still refuse a write into its replacement.
func (b *BadgerEngine) withNamespaceSideStateWriteAt(entityID string, revision uint64, write func() error) error {
	release, err := b.beginWrite()
	if err != nil {
		return err
	}
	defer release()
	if err := b.checkNamespaceLifetimeRevision(namespaceForSideStateID(entityID), revision); err != nil {
		return err
	}
	return write()
}

// Raw index APIs have numeric owners, not a caller-supplied namespace hint.
// Resolve their point IDs before admission; background batches additionally
// hold RunNamespaceTask's logical lifetime across loading and follow-up work.
func (b *BadgerEngine) withNamespaceIndexWrite(keys [][]byte, write func() error) error {
	revision := b.namespaceLifetimeRevision()
	namespaces := make(map[string]struct{})
	for _, key := range keys {
		if len(key) < 9 {
			continue
		}
		addNode := func(offset int) {
			if len(key) >= offset+8 {
				if id, ok := b.idDict.lookupNodeIDByNum(binary.BigEndian.Uint64(key[offset:])); ok {
					namespaces[namespaceForNodeID(id)] = struct{}{}
				}
			}
		}
		addEdge := func(offset int) {
			if len(key) >= offset+8 {
				if id, ok := b.idDict.lookupEdgeIDByNum(binary.BigEndian.Uint64(key[offset:])); ok {
					namespaces[namespaceForEdgeID(id)] = struct{}{}
				}
			}
		}
		switch key[0] {
		case prefixLabelIndex:
			addNode(len(key) - 8)
		case prefixEdgeTypeIndex:
			addEdge(len(key) - 8)
		case prefixOutgoingIndex, prefixIncomingIndex:
			addNode(1)
			addEdge(9)
		case prefixEdgeBetweenIndex:
			addNode(1)
			addNode(9)
			addEdge(len(key) - 8)
		case prefixEdgeBetweenHead:
			addNode(1)
			addNode(9)
		}
	}
	release, err := b.beginWrite()
	if err != nil {
		return err
	}
	defer release()
	for namespace := range namespaces {
		if err := b.checkNamespaceLifetimeRevision(namespace, revision); err != nil {
			return err
		}
	}
	return write()
}
