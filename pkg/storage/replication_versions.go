package storage

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"

	"github.com/dgraph-io/badger/v4"
	"github.com/google/uuid"
)

type commandVersionKey struct{}

func WithCommandVersion(ctx context.Context, token [16]byte) context.Context {
	return context.WithValue(ctx, commandVersionKey{}, token)
}

func CommandVersionFromContext(ctx context.Context) [16]byte {
	token, _ := ctx.Value(commandVersionKey{}).([16]byte)
	return token
}

func newReplicationVersion() [16]byte { return [16]byte(uuid.New()) }

func replicationBase(engine Engine) *BadgerEngine {
	for engine != nil {
		switch base := engine.(type) {
		case *BadgerEngine:
			return base
		case *MemoryEngine:
			return base.BadgerEngine
		}
		wrapper, ok := engine.(EngineUnwrapper)
		if !ok {
			return nil
		}
		engine = wrapper.GetInnerEngine()
	}
	return nil
}

// Activate before replicated writes begin. The write barrier prevents a native
// writer that reserved an anonymous version from publishing across activation.
func EnableReplicatedVersions(engine Engine) {
	if b := replicationBase(engine); b != nil {
		release, err := b.beginSchemaWrite()
		if err == nil {
			defer release()
		}
		b.replicatedVersions.Store(true)
	}
}

type ReplayConditions struct {
	Reject    bool
	Nodes     map[NodeID][16]byte
	Edges     map[EdgeID][16]byte
	Adjacency map[NodeID][]EdgeID
	Schema    *ReplicationVersions
}

func (b *BadgerEngine) nodeCommandVersion(id NodeID) ([16]byte, error) {
	return b.commandVersion(id, func() (MVCCHead, error) { return b.GetNodeCurrentHead(id) })
}

func (b *BadgerEngine) edgeCommandVersion(id EdgeID) ([16]byte, error) {
	return b.commandVersion(id, func() (MVCCHead, error) { return b.GetEdgeCurrentHead(id) })
}

func (b *BadgerEngine) commandVersion(id any, read func() (MVCCHead, error)) ([16]byte, error) {
	for {
		var epoch uint64
		if wb := b.writeBehind; wb != nil {
			epoch = wb.readEpoch.Load()
			wb.mu.RLock()
			token, found := wb.active.replicationVersions[id]
			for i := len(wb.draining) - 1; !found && i >= 0; i-- {
				if !wb.draining[i].IsApplied() {
					token, found = wb.draining[i].replicationVersions[id]
				}
			}
			wb.mu.RUnlock()
			if found {
				return token, nil
			}
		}
		head, err := read()
		if b.writeBehind != nil && epoch != b.writeBehind.readEpoch.Load() {
			continue
		}
		if errors.Is(err, ErrNotFound) {
			return [16]byte{}, nil
		}
		return head.Version.ReplicationID, err
	}
}

func (b *BadgerEngine) replayAdjacency(id NodeID) ([]EdgeID, error) {
	outgoing, incoming, err := b.readAdjacency(id, true, true, storedRead)
	if err != nil {
		return nil, err
	}
	ids := make([]EdgeID, 0, len(outgoing)+len(incoming))
	seen := make(map[EdgeID]bool)
	for _, edges := range [][]*Edge{outgoing, incoming} {
		for _, edge := range edges {
			if !seen[edge.ID] {
				seen[edge.ID] = true
				ids = append(ids, edge.ID)
			}
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids, nil
}

// Capture at the proposal boundary, after original-snapshot validation. The
// same storage owner validates this complete write/dependency footprint on
// every peer. A command identity catches identical-payload rewrites and ABA.
func (m *ManagedWriteTransaction) captureReplayConditions() error {
	tx := m.BadgerTransaction
	tx.mu.Lock()
	defer tx.mu.Unlock()
	c := &ReplayConditions{Nodes: make(map[NodeID][16]byte), Edges: make(map[EdgeID][16]byte), Adjacency: make(map[NodeID][]EdgeID)}
	node := func(id NodeID) error { token, err := tx.engine.nodeCommandVersion(id); c.Nodes[id] = token; return err }
	edge := func(id EdgeID) error { token, err := tx.engine.edgeCommandVersion(id); c.Edges[id] = token; return err }
	for _, op := range tx.physicalOperationsLocked() {
		switch op.Type {
		case OpCreateNode, OpUpdateNode, OpDeleteNode, OpUpdateEmbedding:
			if err := node(op.NodeID); err != nil {
				return err
			}
			if op.Type == OpDeleteNode {
				ids, err := tx.engine.replayAdjacency(op.NodeID)
				if err != nil {
					return err
				}
				c.Adjacency[op.NodeID] = ids
				for _, id := range ids {
					if err := edge(id); err != nil {
						return err
					}
				}
			}
		case OpCreateEdge, OpUpdateEdge, OpDeleteEdge:
			if err := edge(op.EdgeID); err != nil {
				return err
			}
			if op.Type != OpDeleteEdge && op.Edge != nil {
				for _, id := range []NodeID{op.Edge.StartNode, op.Edge.EndNode} {
					if err := node(id); err != nil {
						return err
					}
				}
			}
		default:
			return fmt.Errorf("replication precondition for %s: %w", op.Type, ErrNotImplemented)
		}
	}
	if tx.schemaRuntime != nil {
		versions, err := captureReplicationVersions(tx.engine, tx.namespace)
		if err != nil {
			return err
		}
		c.Schema = &versions
	}
	// Keep local commit's existing error classification. Every peer still
	// receives the same rejection when this snapshot already lost before
	// proposal; later races are fenced by the captured command identities.
	c.Reject = tx.validateSnapshotIsolationConflicts() != nil
	if tx.schemaRuntime != nil && tx.engine.currentMVCCReadVersion(tx.namespace).CommitSequence != tx.readTS.CommitSequence {
		c.Reject = true
	}
	m.replayConditions, tx.replayConditions = c, c
	return nil
}

func (tx *BadgerTransaction) validateReplayConditionsLocked() error {
	c := tx.replayConditions
	if c == nil {
		return nil
	}
	if c.Reject {
		return ErrConflict
	}
	for id, expected := range c.Nodes {
		actual, err := tx.engine.nodeCommandVersion(id)
		if err != nil {
			return err
		}
		if actual != expected {
			return fmt.Errorf("node %s changed since replicated proposal: %w", id, ErrConflict)
		}
		if key := tx.engine.mvccNodeHeadKeyStringLookup(id); key != nil {
			if err := tx.enrollReplayHeadLocked(key, "node replication version"); err != nil {
				return err
			}
		}
	}
	for id, expected := range c.Edges {
		actual, err := tx.engine.edgeCommandVersion(id)
		if err != nil {
			return err
		}
		if actual != expected {
			return fmt.Errorf("edge %s changed since replicated proposal: %w", id, ErrConflict)
		}
		if key := tx.engine.mvccEdgeHeadKeyStringLookup(id); key != nil {
			if err := tx.enrollReplayHeadLocked(key, "edge replication version"); err != nil {
				return err
			}
		}
	}
	for id, expected := range c.Adjacency {
		actual, err := tx.engine.replayAdjacency(id)
		if err != nil {
			return err
		}
		if len(actual) != len(expected) {
			return ErrConflict
		}
		for i := range actual {
			if actual[i] != expected[i] {
				return ErrConflict
			}
		}
	}
	if c.Schema != nil {
		actual, err := captureReplicationVersions(tx.engine, tx.namespace)
		if err != nil {
			return err
		}
		if !sameReplicationVersions(actual, *c.Schema) {
			return ErrConflict
		}
	}
	return nil
}

func (tx *BadgerTransaction) enrollReplayHeadLocked(key []byte, kind string) error {
	// As in snapshotHeadConflict, replay materializes a buffered ACK after
	// the pinned physical snapshot. Its command identity is authoritative;
	// enrolling the newly materialized key would manufacture a conflict.
	// The existing whole-buffer admission still fences a newer ACK.
	if tx.engine.writeBehind != nil {
		return nil
	}
	return tx.enrollCommittedWriteLocked(key, kind)
}

// Snapshot identities include tombstones, so a restored peer keeps the same
// absence/recreation history as the leader without sharing local MVCC clocks.
type ReplicationVersions struct {
	Nodes map[NodeID]MVCCHead
	Edges map[EdgeID]MVCCHead
}

func CaptureReplicationVersions(engine Engine) (ReplicationVersions, error) {
	b := replicationBase(engine)
	if b == nil {
		return ReplicationVersions{}, nil
	}
	if err := b.FlushWriteBehind(); err != nil {
		return ReplicationVersions{}, err
	}
	return captureReplicationVersions(b, "")
}

func captureReplicationVersions(b *BadgerEngine, namespace string) (ReplicationVersions, error) {
	result := ReplicationVersions{Nodes: make(map[NodeID]MVCCHead), Edges: make(map[EdgeID]MVCCHead)}
	err := b.withView(func(txn *badger.Txn) error {
		for _, prefix := range []byte{prefixMVCCNodeHead, prefixMVCCEdgeHead} {
			opts := badgerIteratorOptions()
			opts.Prefix = []byte{prefix}
			it := txn.NewIterator(opts)
			for it.Rewind(); it.Valid(); it.Next() {
				key := it.Item().Key()
				if len(key) != 9 {
					continue
				}
				id, ok := b.replicationHeadID(prefix, key)
				if !ok {
					continue
				}
				if namespace != "" && namespaceForNodeID(NodeID(id)) != namespace {
					continue
				}
				data, err := it.Item().ValueCopy(nil)
				if err != nil {
					it.Close()
					return err
				}
				head, err := decodeMVCCHead(data)
				if err != nil {
					it.Close()
					return err
				}
				if prefix == prefixMVCCNodeHead {
					result.Nodes[NodeID(id)] = head
				} else {
					result.Edges[EdgeID(id)] = head
				}
			}
			it.Close()
		}
		return nil
	})
	return result, err
}

func sameReplicationVersions(first, second ReplicationVersions) bool {
	if len(first.Nodes) != len(second.Nodes) || len(first.Edges) != len(second.Edges) {
		return false
	}
	for id, head := range first.Nodes {
		other, ok := second.Nodes[id]
		if !ok || head.Tombstoned != other.Tombstoned || head.Version.ReplicationID != other.Version.ReplicationID {
			return false
		}
	}
	for id, head := range first.Edges {
		other, ok := second.Edges[id]
		if !ok || head.Tombstoned != other.Tombstoned || head.Version.ReplicationID != other.Version.ReplicationID {
			return false
		}
	}
	return true
}

func (b *BadgerEngine) replicationHeadID(prefix byte, key []byte) (string, bool) {
	number := binary.BigEndian.Uint64(key[1:])
	if prefix == prefixMVCCNodeHead {
		id, ok := b.idDict.lookupNodeIDByNum(number)
		return string(id), ok
	}
	id, ok := b.idDict.lookupEdgeIDByNum(number)
	return string(id), ok
}

func RestoreReplicationVersions(engine Engine, versions ReplicationVersions) error {
	b := replicationBase(engine)
	if b == nil {
		return nil
	}
	restore := func(id string, source MVCCHead, node bool) error {
		return b.withUpdate(func(txn *badger.Txn) error {
			var head MVCCHead
			var err error
			if node {
				head, err = b.loadNodeMVCCHeadInTxn(txn, NodeID(id))
			} else {
				head, err = b.loadEdgeMVCCHeadInTxn(txn, EdgeID(id))
			}
			if errors.Is(err, ErrNotFound) {
				head.Version, err = b.allocateMVCCVersion(txn, namespaceForNodeID(NodeID(id)), source.Version.CommitTimestamp)
			}
			if err != nil {
				return err
			}
			head.Version.ReplicationID = source.Version.ReplicationID
			if node {
				return b.writeNodeMVCCHeadInTxn(txn, NodeID(id), head.Version, source.Tombstoned)
			}
			return b.writeEdgeMVCCHeadInTxn(txn, EdgeID(id), head.Version, source.Tombstoned)
		})
	}
	for id, head := range versions.Nodes {
		if err := restore(string(id), head, true); err != nil {
			return err
		}
	}
	for id, head := range versions.Edges {
		if err := restore(string(id), head, false); err != nil {
			return err
		}
	}
	return nil
}
