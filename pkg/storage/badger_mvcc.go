package storage

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/orneryd/nornicdb/pkg/util"
	"github.com/vmihailenco/msgpack/v5"
)

type mvccNodeRecord struct {
	Node       *Node
	Tombstoned bool
}

type mvccEdgeRecord struct {
	Edge       *Edge
	Tombstoned bool
}

func encodeMVCCNodeRecord(node *Node, tombstoned bool) ([]byte, error) {
	return msgpack.Marshal(mvccNodeRecord{Node: mvccSnapshotNode(node), Tombstoned: tombstoned})
}

func decodeMVCCNodeRecord(data []byte) (mvccNodeRecord, error) {
	var record mvccNodeRecord
	if err := util.DecodeMsgpackBytes(data, &record); err != nil {
		return mvccNodeRecord{}, err
	}
	return record, nil
}

func encodeMVCCEdgeRecord(edge *Edge, tombstoned bool) ([]byte, error) {
	return msgpack.Marshal(mvccEdgeRecord{Edge: copyEdge(edge), Tombstoned: tombstoned})
}

func mvccSnapshotNode(node *Node) *Node {
	if node == nil {
		return nil
	}
	snapshot := copyNode(node)
	// Historical MVCC records keep logical graph state but do not inline large
	// embedding payloads. The materialized current row continues to store full
	// embeddings using the existing separate-chunk path.
	snapshot.ChunkEmbeddings = nil
	snapshot.NamedEmbeddings = nil
	snapshot.EmbeddingsStoredSeparately = false
	return snapshot
}

func decodeMVCCEdgeRecord(data []byte) (mvccEdgeRecord, error) {
	var record mvccEdgeRecord
	if err := util.DecodeMsgpackBytes(data, &record); err != nil {
		return mvccEdgeRecord{}, err
	}
	return record, nil
}

func encodeMVCCHead(head MVCCHead) ([]byte, error) {
	head = normalizeMVCCHead(head)
	return msgpack.Marshal(head)
}

func decodeMVCCHead(data []byte) (MVCCHead, error) {
	var head MVCCHead
	if err := util.DecodeMsgpackBytes(data, &head); err != nil {
		return MVCCHead{}, err
	}
	return normalizeMVCCHead(head), nil
}

func normalizeMVCCHead(head MVCCHead) MVCCHead {
	if head.FloorVersion.IsZero() {
		head.FloorVersion = head.Version
	}
	return head
}

func (b *BadgerEngine) beginMVCCSnapshotRead(version MVCCVersion) (func(), error) {
	return b.acquireSnapshotReader(SnapshotReaderInfo{
		SnapshotVersion: version,
		StartTime:       time.Now(),
	})
}

func (b *BadgerEngine) effectiveMVCCPruneOptions(opts MVCCPruneOptions) MVCCPruneOptions {
	policy := normalizeRetentionPolicy(b.retentionPolicy)
	effective := opts
	if effective.MaxVersionsPerKey <= 0 {
		effective.MaxVersionsPerKey = policy.MaxVersionsPerKey
	}
	if effective.MinRetentionAge <= 0 {
		effective.MinRetentionAge = policy.TTL
	}
	if effective.MinRetentionAge < 0 {
		effective.MinRetentionAge = 0
	}
	return effective
}

func (b *BadgerEngine) writeNodeMVCCVersionInTxn(txn *badger.Txn, node *Node, version MVCCVersion) error {
	encoded, err := encodeMVCCNodeRecord(node, false)
	if err != nil {
		return err
	}
	return txn.Set(mvccNodeVersionKey(node.ID, version), encoded)
}

func (b *BadgerEngine) writeNodeMVCCTombstoneInTxn(txn *badger.Txn, id NodeID, version MVCCVersion) error {
	encoded, err := encodeMVCCNodeRecord(nil, true)
	if err != nil {
		return err
	}
	return txn.Set(mvccNodeVersionKey(id, version), encoded)
}

func (b *BadgerEngine) writeEdgeMVCCVersionInTxn(txn *badger.Txn, edge *Edge, version MVCCVersion) error {
	encoded, err := encodeMVCCEdgeRecord(edge, false)
	if err != nil {
		return err
	}
	return txn.Set(mvccEdgeVersionKey(edge.ID, version), encoded)
}

func (b *BadgerEngine) writeEdgeMVCCTombstoneInTxn(txn *badger.Txn, id EdgeID, version MVCCVersion) error {
	encoded, err := encodeMVCCEdgeRecord(nil, true)
	if err != nil {
		return err
	}
	return txn.Set(mvccEdgeVersionKey(id, version), encoded)
}

func (b *BadgerEngine) writeNodeMVCCHeadInTxn(txn *badger.Txn, id NodeID, version MVCCVersion, tombstoned bool) error {
	floorVersion := version
	if existing, err := b.loadNodeMVCCHeadInTxn(txn, id); err == nil {
		floorVersion = existing.FloorVersion
	} else if err != ErrNotFound {
		return err
	}
	return b.writeNodeMVCCHeadWithFloorInTxn(txn, id, version, tombstoned, floorVersion)
}

func (b *BadgerEngine) writeEdgeMVCCHeadInTxn(txn *badger.Txn, id EdgeID, version MVCCVersion, tombstoned bool) error {
	floorVersion := version
	if existing, err := b.loadEdgeMVCCHeadInTxn(txn, id); err == nil {
		floorVersion = existing.FloorVersion
	} else if err != ErrNotFound {
		return err
	}
	return b.writeEdgeMVCCHeadWithFloorInTxn(txn, id, version, tombstoned, floorVersion)
}

func (b *BadgerEngine) writeNodeMVCCHeadWithFloorInTxn(txn *badger.Txn, id NodeID, version MVCCVersion, tombstoned bool, floorVersion MVCCVersion) error {
	encoded, err := encodeMVCCHead(MVCCHead{Version: version, Tombstoned: tombstoned, FloorVersion: floorVersion})
	if err != nil {
		return err
	}
	return txn.Set(mvccNodeHeadKey(id), encoded)

}

func (b *BadgerEngine) writeEdgeMVCCHeadWithFloorInTxn(txn *badger.Txn, id EdgeID, version MVCCVersion, tombstoned bool, floorVersion MVCCVersion) error {
	encoded, err := encodeMVCCHead(MVCCHead{Version: version, Tombstoned: tombstoned, FloorVersion: floorVersion})
	if err != nil {
		return err
	}
	return txn.Set(mvccEdgeHeadKey(id), encoded)
}

func (b *BadgerEngine) loadNodeMVCCHeadInTxn(txn *badger.Txn, id NodeID) (MVCCHead, error) {
	item, err := txn.Get(mvccNodeHeadKey(id))
	if err == badger.ErrKeyNotFound {
		return MVCCHead{}, ErrNotFound
	}
	if err != nil {
		return MVCCHead{}, err
	}
	var head MVCCHead
	err = item.Value(func(val []byte) error {
		var decodeErr error
		head, decodeErr = decodeMVCCHead(val)
		return decodeErr
	})
	if err != nil {
		return MVCCHead{}, err
	}
	return head, nil
}

func (b *BadgerEngine) loadEdgeMVCCHeadInTxn(txn *badger.Txn, id EdgeID) (MVCCHead, error) {
	item, err := txn.Get(mvccEdgeHeadKey(id))
	if err == badger.ErrKeyNotFound {
		return MVCCHead{}, ErrNotFound
	}
	if err != nil {
		return MVCCHead{}, err
	}
	var head MVCCHead
	err = item.Value(func(val []byte) error {
		var decodeErr error
		head, decodeErr = decodeMVCCHead(val)
		return decodeErr
	})
	if err != nil {
		return MVCCHead{}, err
	}
	return head, nil
}

func (b *BadgerEngine) loadNodeMVCCRecordExactInTxn(txn *badger.Txn, id NodeID, version MVCCVersion) (mvccNodeRecord, error) {
	item, err := txn.Get(mvccNodeVersionKey(id, version))
	if err == badger.ErrKeyNotFound {
		return mvccNodeRecord{}, ErrNotFound
	}
	if err != nil {
		return mvccNodeRecord{}, err
	}
	var record mvccNodeRecord
	err = item.Value(func(val []byte) error {
		var decodeErr error
		record, decodeErr = decodeMVCCNodeRecord(val)
		return decodeErr
	})
	if err != nil {
		return mvccNodeRecord{}, err
	}
	return record, nil
}

func (b *BadgerEngine) loadEdgeMVCCRecordExactInTxn(txn *badger.Txn, id EdgeID, version MVCCVersion) (mvccEdgeRecord, error) {
	item, err := txn.Get(mvccEdgeVersionKey(id, version))
	if err == badger.ErrKeyNotFound {
		return mvccEdgeRecord{}, ErrNotFound
	}
	if err != nil {
		return mvccEdgeRecord{}, err
	}
	var record mvccEdgeRecord
	err = item.Value(func(val []byte) error {
		var decodeErr error
		record, decodeErr = decodeMVCCEdgeRecord(val)
		return decodeErr
	})
	if err != nil {
		return mvccEdgeRecord{}, err
	}
	return record, nil
}

func (b *BadgerEngine) loadNodeMVCCRecordAtOrBeforeInTxn(txn *badger.Txn, id NodeID, version MVCCVersion) (mvccNodeRecord, MVCCVersion, error) {
	prefix := mvccNodeVersionPrefix(id)
	seek := append(append([]byte{}, prefix...), encodeMVCCSortVersion(version)...)
	opts := badger.DefaultIteratorOptions
	opts.Prefix = prefix
	opts.PrefetchValues = true
	opts.Reverse = true
	it := txn.NewIterator(opts)
	defer it.Close()
	it.Seek(seek)
	if it.ValidForPrefix(prefix) {
		key := append([]byte(nil), it.Item().Key()...)
		parsedVersion, err := extractMVCCVersionFromKey(key)
		if err != nil {
			return mvccNodeRecord{}, MVCCVersion{}, err
		}
		var record mvccNodeRecord
		if err := it.Item().Value(func(val []byte) error {
			var decodeErr error
			record, decodeErr = decodeMVCCNodeRecord(val)
			return decodeErr
		}); err != nil {
			return mvccNodeRecord{}, MVCCVersion{}, err
		}
		return record, parsedVersion, nil
	}
	return mvccNodeRecord{}, MVCCVersion{}, ErrNotFound
}

func (b *BadgerEngine) loadEdgeMVCCRecordAtOrBeforeInTxn(txn *badger.Txn, id EdgeID, version MVCCVersion) (mvccEdgeRecord, MVCCVersion, error) {
	prefix := mvccEdgeVersionPrefix(id)
	seek := append(append([]byte{}, prefix...), encodeMVCCSortVersion(version)...)
	opts := badger.DefaultIteratorOptions
	opts.Prefix = prefix
	opts.PrefetchValues = true
	opts.Reverse = true
	it := txn.NewIterator(opts)
	defer it.Close()
	it.Seek(seek)
	if it.ValidForPrefix(prefix) {
		key := append([]byte(nil), it.Item().Key()...)
		parsedVersion, err := extractMVCCVersionFromKey(key)
		if err != nil {
			return mvccEdgeRecord{}, MVCCVersion{}, err
		}
		var record mvccEdgeRecord
		if err := it.Item().Value(func(val []byte) error {
			var decodeErr error
			record, decodeErr = decodeMVCCEdgeRecord(val)
			return decodeErr
		}); err != nil {
			return mvccEdgeRecord{}, MVCCVersion{}, err
		}
		return record, parsedVersion, nil
	}
	return mvccEdgeRecord{}, MVCCVersion{}, ErrNotFound
}

func (b *BadgerEngine) latestNodeMVCCVersionInTxn(txn *badger.Txn, id NodeID) (mvccNodeRecord, MVCCVersion, error) {
	return b.loadNodeMVCCRecordAtOrBeforeInTxn(txn, id, maxMVCCVersion())
}

func (b *BadgerEngine) latestEdgeMVCCVersionInTxn(txn *badger.Txn, id EdgeID) (mvccEdgeRecord, MVCCVersion, error) {
	return b.loadEdgeMVCCRecordAtOrBeforeInTxn(txn, id, maxMVCCVersion())
}

func (b *BadgerEngine) AppendNodeVersion(node *Node, version MVCCVersion) error {
	if node == nil {
		return ErrInvalidData
	}
	return b.withUpdate(func(txn *badger.Txn) error {
		if err := b.writeNodeMVCCVersionInTxn(txn, node, version); err != nil {
			return err
		}
		return b.writeNodeMVCCHeadInTxn(txn, node.ID, version, false)
	})
}

func (b *BadgerEngine) AppendNodeTombstone(id NodeID, version MVCCVersion) error {
	return b.withUpdate(func(txn *badger.Txn) error {
		if err := b.writeNodeMVCCTombstoneInTxn(txn, id, version); err != nil {
			return err
		}
		return b.writeNodeMVCCHeadInTxn(txn, id, version, true)
	})
}

func (b *BadgerEngine) AppendEdgeVersion(edge *Edge, version MVCCVersion) error {
	if edge == nil {
		return ErrInvalidData
	}
	return b.withUpdate(func(txn *badger.Txn) error {
		if err := b.writeEdgeMVCCVersionInTxn(txn, edge, version); err != nil {
			return err
		}
		return b.writeEdgeMVCCHeadInTxn(txn, edge.ID, version, false)
	})
}

func (b *BadgerEngine) AppendEdgeTombstone(id EdgeID, version MVCCVersion) error {
	return b.withUpdate(func(txn *badger.Txn) error {
		if err := b.writeEdgeMVCCTombstoneInTxn(txn, id, version); err != nil {
			return err
		}
		return b.writeEdgeMVCCHeadInTxn(txn, id, version, true)
	})
}

func (b *BadgerEngine) UpdateNodeCurrentHead(id NodeID, version MVCCVersion, tombstoned bool) error {
	return b.withUpdate(func(txn *badger.Txn) error {
		return b.writeNodeMVCCHeadInTxn(txn, id, version, tombstoned)
	})
}

func (b *BadgerEngine) UpdateEdgeCurrentHead(id EdgeID, version MVCCVersion, tombstoned bool) error {
	return b.withUpdate(func(txn *badger.Txn) error {
		return b.writeEdgeMVCCHeadInTxn(txn, id, version, tombstoned)
	})
}

func (b *BadgerEngine) GetNodeCurrentHead(id NodeID) (MVCCHead, error) {
	var head MVCCHead
	err := b.withView(func(txn *badger.Txn) error {
		var innerErr error
		head, innerErr = b.loadNodeMVCCHeadInTxn(txn, id)
		return innerErr
	})
	return head, err
}

func (b *BadgerEngine) GetEdgeCurrentHead(id EdgeID) (MVCCHead, error) {
	var head MVCCHead
	err := b.withView(func(txn *badger.Txn) error {
		var innerErr error
		head, innerErr = b.loadEdgeMVCCHeadInTxn(txn, id)
		return innerErr
	})
	return head, err
}

func (b *BadgerEngine) GetNodeLatestVisible(id NodeID) (*Node, error) {
	if node, err := b.GetNode(id); err == nil {
		return node, nil
	}
	var node *Node
	err := b.withView(func(txn *badger.Txn) error {
		head, err := b.loadNodeMVCCHeadInTxn(txn, id)
		if err != nil {
			return err
		}
		if head.Tombstoned {
			return ErrNotFound
		}
		record, err := b.loadNodeMVCCRecordExactInTxn(txn, id, head.Version)
		if err != nil {
			if err == ErrNotFound {
				fallback, _, fallbackErr := b.loadNodeMVCCRecordAtOrBeforeInTxn(txn, id, head.Version)
				if fallbackErr != nil {
					return fallbackErr
				}
				record = fallback
			} else {
				return err
			}
		}
		if record.Tombstoned || record.Node == nil {
			return ErrNotFound
		}
		node = copyNode(record.Node)
		if b.filterNodeByDecay(node, DecayScoringTime()) {
			node = nil
			return ErrNotFound
		}
		return nil
	})
	return node, err
}

func (b *BadgerEngine) GetNodeVisibleAt(id NodeID, version MVCCVersion) (*Node, error) {
	deregister, err := b.beginMVCCSnapshotRead(version)
	if err != nil {
		return nil, err
	}
	defer deregister()
	var node *Node
	err = b.withView(func(txn *badger.Txn) error {
		head, err := b.loadNodeMVCCHeadInTxn(txn, id)
		if err != nil {
			return err
		}
		if version.Compare(head.FloorVersion) < 0 {
			return ErrNotFound
		}

		var record mvccNodeRecord
		switch {
		case version.Compare(head.Version) >= 0:
			record, err = b.loadNodeMVCCRecordExactInTxn(txn, id, head.Version)
		case version.Compare(head.FloorVersion) == 0:
			record, err = b.loadNodeMVCCRecordExactInTxn(txn, id, head.FloorVersion)
		default:
			record, _, err = b.loadNodeMVCCRecordAtOrBeforeInTxn(txn, id, version)
		}
		if err != nil {
			if err == ErrNotFound && version.Compare(head.Version) >= 0 {
				record, _, err = b.loadNodeMVCCRecordAtOrBeforeInTxn(txn, id, head.Version)
			}
			if err != nil {
				return err
			}
		}
		if record.Tombstoned || record.Node == nil {
			return ErrNotFound
		}
		node = copyNode(record.Node)
		if b.filterNodeByDecay(node, DecayScoringTime()) {
			node = nil
			return ErrNotFound
		}
		return nil
	})
	return node, err
}

func (b *BadgerEngine) GetEdgeLatestVisible(id EdgeID) (*Edge, error) {
	if edge, err := b.GetEdge(id); err == nil {
		return edge, nil
	}
	var edge *Edge
	err := b.withView(func(txn *badger.Txn) error {
		head, err := b.loadEdgeMVCCHeadInTxn(txn, id)
		if err != nil {
			return err
		}
		if head.Tombstoned {
			return ErrNotFound
		}
		record, err := b.loadEdgeMVCCRecordExactInTxn(txn, id, head.Version)
		if err != nil {
			if err == ErrNotFound {
				fallback, _, fallbackErr := b.loadEdgeMVCCRecordAtOrBeforeInTxn(txn, id, head.Version)
				if fallbackErr != nil {
					return fallbackErr
				}
				record = fallback
			} else {
				return err
			}
		}
		if record.Tombstoned || record.Edge == nil {
			return ErrNotFound
		}
		edge = copyEdge(record.Edge)
		if b.filterEdgeByDecay(edge, DecayScoringTime()) {
			edge = nil
			return ErrNotFound
		}
		return nil
	})
	return edge, err
}

func (b *BadgerEngine) GetEdgeVisibleAt(id EdgeID, version MVCCVersion) (*Edge, error) {
	deregister, err := b.beginMVCCSnapshotRead(version)
	if err != nil {
		return nil, err
	}
	defer deregister()
	var edge *Edge
	err = b.withView(func(txn *badger.Txn) error {
		head, err := b.loadEdgeMVCCHeadInTxn(txn, id)
		if err != nil {
			return err
		}
		if version.Compare(head.FloorVersion) < 0 {
			return ErrNotFound
		}

		var record mvccEdgeRecord
		switch {
		case version.Compare(head.Version) >= 0:
			record, err = b.loadEdgeMVCCRecordExactInTxn(txn, id, head.Version)
		case version.Compare(head.FloorVersion) == 0:
			record, err = b.loadEdgeMVCCRecordExactInTxn(txn, id, head.FloorVersion)
		default:
			record, _, err = b.loadEdgeMVCCRecordAtOrBeforeInTxn(txn, id, version)
		}
		if err != nil {
			if err == ErrNotFound && version.Compare(head.Version) >= 0 {
				record, _, err = b.loadEdgeMVCCRecordAtOrBeforeInTxn(txn, id, head.Version)
			}
			if err != nil {
				return err
			}
		}
		if record.Tombstoned || record.Edge == nil {
			return ErrNotFound
		}
		edge = copyEdge(record.Edge)
		if b.filterEdgeByDecay(edge, DecayScoringTime()) {
			edge = nil
			return ErrNotFound
		}
		return nil
	})
	return edge, err
}

func (b *BadgerEngine) BatchGetNodesLatestVisible(ids []NodeID) (map[NodeID]*Node, error) {
	result := make(map[NodeID]*Node, len(ids))
	for _, id := range ids {
		node, err := b.GetNodeLatestVisible(id)
		if err == nil && node != nil {
			result[id] = node
		}
	}
	return result, nil
}

func (b *BadgerEngine) iterateNodesVisibleAtInTxn(txn *badger.Txn, version MVCCVersion, yield func(*Node) error) error {
	opts := badger.DefaultIteratorOptions
	opts.Prefix = []byte{prefixMVCCNode}
	opts.PrefetchValues = true
	it := txn.NewIterator(opts)
	defer it.Close()

	nowNanos := DecayScoringTime()
	var currentID NodeID
	var candidate *Node
	var tombstoned bool
	var haveCurrent bool
	flush := func() error {
		if !haveCurrent || tombstoned || candidate == nil {
			candidate = nil
			tombstoned = false
			haveCurrent = false
			return nil
		}
		node := copyNode(candidate)
		candidate = nil
		tombstoned = false
		haveCurrent = false
		if b.filterNodeByDecay(node, nowNanos) {
			return nil
		}
		return yield(node)
	}

	for it.Rewind(); it.Valid(); it.Next() {
		key := append([]byte(nil), it.Item().Key()...)
		nodeID, recordVersion, err := extractNodeIDAndMVCCVersionFromVersionKey(key)
		if err != nil {
			return err
		}
		if haveCurrent && nodeID != currentID {
			if err := flush(); err != nil {
				return err
			}
		}
		currentID = nodeID
		haveCurrent = true
		if recordVersion.Compare(version) > 0 {
			continue
		}
		if err := it.Item().Value(func(val []byte) error {
			record, decodeErr := decodeMVCCNodeRecord(val)
			if decodeErr != nil {
				return decodeErr
			}
			tombstoned = record.Tombstoned
			candidate = record.Node
			return nil
		}); err != nil {
			return err
		}
	}
	return flush()
}

func (b *BadgerEngine) iterateEdgesVisibleAtInTxn(txn *badger.Txn, version MVCCVersion, yield func(*Edge) error) error {
	opts := badger.DefaultIteratorOptions
	opts.Prefix = []byte{prefixMVCCEdge}
	opts.PrefetchValues = true
	it := txn.NewIterator(opts)
	defer it.Close()

	nowNanos := DecayScoringTime()
	var currentID EdgeID
	var candidate *Edge
	var tombstoned bool
	var haveCurrent bool
	flush := func() error {
		if !haveCurrent || tombstoned || candidate == nil {
			candidate = nil
			tombstoned = false
			haveCurrent = false
			return nil
		}
		edge := copyEdge(candidate)
		candidate = nil
		tombstoned = false
		haveCurrent = false
		if b.filterEdgeByDecay(edge, nowNanos) {
			return nil
		}
		return yield(edge)
	}

	for it.Rewind(); it.Valid(); it.Next() {
		key := append([]byte(nil), it.Item().Key()...)
		edgeID, recordVersion, err := extractEdgeIDAndMVCCVersionFromVersionKey(key)
		if err != nil {
			return err
		}
		if haveCurrent && edgeID != currentID {
			if err := flush(); err != nil {
				return err
			}
		}
		currentID = edgeID
		haveCurrent = true
		if recordVersion.Compare(version) > 0 {
			continue
		}
		if err := it.Item().Value(func(val []byte) error {
			record, decodeErr := decodeMVCCEdgeRecord(val)
			if decodeErr != nil {
				return decodeErr
			}
			tombstoned = record.Tombstoned
			candidate = record.Edge
			return nil
		}); err != nil {
			return err
		}
	}
	return flush()
}

func (b *BadgerEngine) GetNodesByLabelVisibleAt(label string, version MVCCVersion) ([]*Node, error) {
	deregister, err := b.beginMVCCSnapshotRead(version)
	if err != nil {
		return nil, err
	}
	defer deregister()
	var nodes []*Node
	normalizedLabel := normalizeLabel(label)
	err = b.withView(func(txn *badger.Txn) error {
		return b.iterateNodesVisibleAtInTxn(txn, version, func(node *Node) error {
			if node == nil {
				return nil
			}
			if normalizedLabel != "" {
				matched := false
				for _, existing := range node.Labels {
					if normalizeLabel(existing) == normalizedLabel {
						matched = true
						break
					}
				}
				if !matched {
					return nil
				}
			}
			nodes = append(nodes, node)
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	return nodes, nil
}

func (b *BadgerEngine) GetEdgesByTypeVisibleAt(edgeType string, version MVCCVersion) ([]*Edge, error) {
	deregister, err := b.beginMVCCSnapshotRead(version)
	if err != nil {
		return nil, err
	}
	defer deregister()
	var edges []*Edge
	normalizedType := strings.ToLower(edgeType)
	err = b.withView(func(txn *badger.Txn) error {
		return b.iterateEdgesVisibleAtInTxn(txn, version, func(edge *Edge) error {
			if edge == nil {
				return nil
			}
			if normalizedType != "" && strings.ToLower(edge.Type) != normalizedType {
				return nil
			}
			edges = append(edges, edge)
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	return edges, nil
}

func (b *BadgerEngine) GetEdgesBetweenVisibleAt(startID, endID NodeID, version MVCCVersion) ([]*Edge, error) {
	if startID == "" || endID == "" {
		return nil, ErrInvalidID
	}
	deregister, err := b.beginMVCCSnapshotRead(version)
	if err != nil {
		return nil, err
	}
	defer deregister()
	var edges []*Edge
	err = b.withView(func(txn *badger.Txn) error {
		return b.iterateEdgesVisibleAtInTxn(txn, version, func(edge *Edge) error {
			if edge == nil {
				return nil
			}
			if edge.StartNode == startID && edge.EndNode == endID {
				edges = append(edges, edge)
			}
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	return edges, nil
}

func (b *BadgerEngine) IterateLatestVisibleNodes(yield func(*Node) error) error {
	nodes, err := b.AllNodes()
	if err != nil {
		return err
	}
	for _, node := range nodes {
		if err := yield(node); err != nil {
			return err
		}
	}
	return nil
}

func (b *BadgerEngine) IterateLatestVisibleEdges(yield func(*Edge) error) error {
	edges, err := b.AllEdges()
	if err != nil {
		return err
	}
	for _, edge := range edges {
		if err := yield(edge); err != nil {
			return err
		}
	}
	return nil
}

func (b *BadgerEngine) materializeMVCCCommitInTxn(txn *badger.Txn, version MVCCVersion, operations []Operation) error {
	for _, op := range operations {
		switch op.Type {
		case OpCreateNode, OpUpdateNode:
			if op.Node == nil {
				continue
			}
			if err := b.writeNodeMVCCVersionInTxn(txn, op.Node, version); err != nil {
				return err
			}
			if err := b.writeNodeMVCCHeadInTxn(txn, op.Node.ID, version, false); err != nil {
				return err
			}
		case OpDeleteNode:
			if err := b.writeNodeMVCCTombstoneInTxn(txn, op.NodeID, version); err != nil {
				return err
			}
			if err := b.writeNodeMVCCHeadInTxn(txn, op.NodeID, version, true); err != nil {
				return err
			}
			for _, edgeID := range op.DeletedEdgeIDs {
				if err := b.writeEdgeMVCCTombstoneInTxn(txn, edgeID, version); err != nil {
					return err
				}
				if err := b.writeEdgeMVCCHeadInTxn(txn, edgeID, version, true); err != nil {
					return err
				}
			}
		case OpCreateEdge, OpUpdateEdge:
			if op.Edge == nil {
				continue
			}
			if err := b.writeEdgeMVCCVersionInTxn(txn, op.Edge, version); err != nil {
				return err
			}
			if err := b.writeEdgeMVCCHeadInTxn(txn, op.Edge.ID, version, false); err != nil {
				return err
			}
		case OpDeleteEdge:
			if err := b.writeEdgeMVCCTombstoneInTxn(txn, op.EdgeID, version); err != nil {
				return err
			}
			if err := b.writeEdgeMVCCHeadInTxn(txn, op.EdgeID, version, true); err != nil {
				return err
			}
		}
	}
	return nil
}

const mvccRebuildScanBatchSize = 512

type nodeHeadWrite struct {
	id           NodeID
	version      MVCCVersion
	tombstoned   bool
	floorVersion MVCCVersion
}

type edgeHeadWrite struct {
	id           EdgeID
	version      MVCCVersion
	tombstoned   bool
	floorVersion MVCCVersion
}

func nextScanStart(key []byte) []byte {
	return append(append([]byte(nil), key...), 0x00)
}

func (b *BadgerEngine) RebuildMVCCHeads(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := b.clearBadgerPrefix(ctx, prefixMVCCNodeHead); err != nil {
		return err
	}
	if err := b.clearBadgerPrefix(ctx, prefixMVCCEdgeHead); err != nil {
		return err
	}
	if err := b.rebuildMVCCHeadsFromVersionRecords(ctx); err != nil {
		return err
	}
	return b.bootstrapMVCCHeadsFromCurrentState(ctx)
}

func (b *BadgerEngine) rebuildMVCCHeadsFromVersionRecords(ctx context.Context) error {
	if err := b.rebuildNodeMVCCHeadsFromVersions(ctx); err != nil {
		return err
	}
	return b.rebuildEdgeMVCCHeadsFromVersions(ctx)
}

func (b *BadgerEngine) rebuildNodeMVCCHeadsFromVersions(ctx context.Context) error {
	start := []byte{prefixMVCCNode}
	var lastID NodeID
	var lastHead MVCCHead
	var firstVersion MVCCVersion
	var haveLast bool
	batch := make([]nodeHeadWrite, 0, mvccRebuildScanBatchSize)
	flush := func() {
		if !haveLast {
			return
		}
		batch = append(batch, nodeHeadWrite{
			id:           lastID,
			version:      lastHead.Version,
			tombstoned:   lastHead.Tombstoned,
			floorVersion: firstVersion,
		})
		firstVersion = MVCCVersion{}
	}

	for {
		lastScanned, reachedEnd, err := b.withViewNodeMVCCVersionsFromKey(start, mvccRebuildScanBatchSize, func(nodeID NodeID, version MVCCVersion, tombstoned bool) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			if haveLast && nodeID != lastID {
				flush()
			}
			if firstVersion.IsZero() {
				firstVersion = version
			}
			lastID = nodeID
			lastHead = MVCCHead{Version: version, Tombstoned: tombstoned}
			haveLast = true
			return nil
		})
		if err != nil {
			return err
		}
		if len(batch) > 0 {
			if err := b.applyNodeHeadBatch(batch); err != nil {
				return err
			}
			batch = batch[:0]
		}
		if reachedEnd {
			flush()
			if len(batch) > 0 {
				if err := b.applyNodeHeadBatch(batch); err != nil {
					return err
				}
			}
			return nil
		}
		start = nextScanStart(lastScanned)
	}
}

func (b *BadgerEngine) rebuildEdgeMVCCHeadsFromVersions(ctx context.Context) error {
	start := []byte{prefixMVCCEdge}
	var lastID EdgeID
	var lastHead MVCCHead
	var firstVersion MVCCVersion
	var haveLast bool
	batch := make([]edgeHeadWrite, 0, mvccRebuildScanBatchSize)
	flush := func() {
		if !haveLast {
			return
		}
		batch = append(batch, edgeHeadWrite{
			id:           lastID,
			version:      lastHead.Version,
			tombstoned:   lastHead.Tombstoned,
			floorVersion: firstVersion,
		})
		firstVersion = MVCCVersion{}
	}

	for {
		lastScanned, reachedEnd, err := b.withViewEdgeMVCCVersionsFromKey(start, mvccRebuildScanBatchSize, func(edgeID EdgeID, version MVCCVersion, tombstoned bool) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			if haveLast && edgeID != lastID {
				flush()
			}
			if firstVersion.IsZero() {
				firstVersion = version
			}
			lastID = edgeID
			lastHead = MVCCHead{Version: version, Tombstoned: tombstoned}
			haveLast = true
			return nil
		})
		if err != nil {
			return err
		}
		if len(batch) > 0 {
			if err := b.applyEdgeHeadBatch(batch); err != nil {
				return err
			}
			batch = batch[:0]
		}
		if reachedEnd {
			flush()
			if len(batch) > 0 {
				if err := b.applyEdgeHeadBatch(batch); err != nil {
					return err
				}
			}
			return nil
		}
		start = nextScanStart(lastScanned)
	}
}

func (b *BadgerEngine) applyNodeHeadBatch(batch []nodeHeadWrite) error {
	return b.withUpdate(func(txn *badger.Txn) error {
		for _, write := range batch {
			if err := b.writeNodeMVCCHeadWithFloorInTxn(txn, write.id, write.version, write.tombstoned, write.floorVersion); err != nil {
				return err
			}
		}
		return nil
	})
}

func (b *BadgerEngine) applyEdgeHeadBatch(batch []edgeHeadWrite) error {
	return b.withUpdate(func(txn *badger.Txn) error {
		for _, write := range batch {
			if err := b.writeEdgeMVCCHeadWithFloorInTxn(txn, write.id, write.version, write.tombstoned, write.floorVersion); err != nil {
				return err
			}
		}
		return nil
	})
}

func (b *BadgerEngine) withViewNodeMVCCVersionsFromKey(start []byte, limit int, yield func(NodeID, MVCCVersion, bool) error) ([]byte, bool, error) {
	var lastScanned []byte
	reachedEnd := true
	err := b.withView(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte{prefixMVCCNode}
		opts.PrefetchValues = true
		it := txn.NewIterator(opts)
		defer it.Close()
		count := 0
		for it.Seek(start); it.ValidForPrefix(opts.Prefix); it.Next() {
			key := append([]byte(nil), it.Item().Key()...)
			nodeID, version, err := extractNodeIDAndMVCCVersionFromVersionKey(key)
			if err != nil {
				return err
			}
			var tombstoned bool
			if err := it.Item().Value(func(val []byte) error {
				record, decodeErr := decodeMVCCNodeRecord(val)
				if decodeErr != nil {
					return decodeErr
				}
				tombstoned = record.Tombstoned
				return nil
			}); err != nil {
				return err
			}
			if err := yield(nodeID, version, tombstoned); err != nil {
				return err
			}
			lastScanned = key
			count++
			if count >= limit {
				reachedEnd = false
				break
			}
		}
		return nil
	})
	return lastScanned, reachedEnd, err
}

func (b *BadgerEngine) withViewEdgeMVCCVersionsFromKey(start []byte, limit int, yield func(EdgeID, MVCCVersion, bool) error) ([]byte, bool, error) {
	var lastScanned []byte
	reachedEnd := true
	err := b.withView(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte{prefixMVCCEdge}
		opts.PrefetchValues = true
		it := txn.NewIterator(opts)
		defer it.Close()
		count := 0
		for it.Seek(start); it.ValidForPrefix(opts.Prefix); it.Next() {
			key := append([]byte(nil), it.Item().Key()...)
			edgeID, version, err := extractEdgeIDAndMVCCVersionFromVersionKey(key)
			if err != nil {
				return err
			}
			var tombstoned bool
			if err := it.Item().Value(func(val []byte) error {
				record, decodeErr := decodeMVCCEdgeRecord(val)
				if decodeErr != nil {
					return decodeErr
				}
				tombstoned = record.Tombstoned
				return nil
			}); err != nil {
				return err
			}
			if err := yield(edgeID, version, tombstoned); err != nil {
				return err
			}
			lastScanned = key
			count++
			if count >= limit {
				reachedEnd = false
				break
			}
		}
		return nil
	})
	return lastScanned, reachedEnd, err
}

func (b *BadgerEngine) bootstrapMVCCHeadsFromCurrentState(ctx context.Context) error {
	if err := b.bootstrapNodeMVCCFromCurrentState(ctx); err != nil {
		return err
	}
	return b.bootstrapEdgeMVCCFromCurrentState(ctx)
}

func (b *BadgerEngine) bootstrapNodeMVCCFromCurrentState(ctx context.Context) error {
	start := []byte{prefixNode}
	for {
		batch, lastScanned, reachedEnd, err := b.collectNodeBootstrapBatch(ctx, start, mvccRebuildScanBatchSize)
		if err != nil {
			return err
		}
		if len(batch) > 0 {
			if err := b.applyNodeBootstrapBatch(batch); err != nil {
				return err
			}
		}
		if reachedEnd {
			return nil
		}
		start = nextScanStart(lastScanned)
	}
}

func (b *BadgerEngine) collectNodeBootstrapBatch(ctx context.Context, start []byte, limit int) ([]*Node, []byte, bool, error) {
	nodes := make([]*Node, 0, limit)
	var lastScanned []byte
	reachedEnd := true
	err := b.withView(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte{prefixNode}
		opts.PrefetchValues = true
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(start); it.ValidForPrefix(opts.Prefix); it.Next() {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			key := append([]byte(nil), it.Item().Key()...)
			if len(key) <= 1 {
				continue
			}
			id := NodeID(key[1:])
			var node *Node
			if err := it.Item().Value(func(val []byte) error {
				var decodeErr error
				node, decodeErr = decodeNodeWithEmbeddings(txn, val, id)
				return decodeErr
			}); err != nil {
				return err
			}
			nodes = append(nodes, node)
			lastScanned = key
			if len(nodes) >= limit {
				reachedEnd = false
				break
			}
		}
		return nil
	})
	return nodes, lastScanned, reachedEnd, err
}

func (b *BadgerEngine) applyNodeBootstrapBatch(nodes []*Node) error {
	return b.withUpdate(func(txn *badger.Txn) error {
		for _, node := range nodes {
			if node == nil {
				continue
			}
			if _, err := b.loadNodeMVCCHeadInTxn(txn, node.ID); err == nil {
				continue
			} else if err != ErrNotFound {
				return err
			}
			version, err := b.allocateMVCCVersion(txn, mvccBootstrapTime(node.CreatedAt, node.UpdatedAt))
			if err != nil {
				return err
			}
			if err := b.writeNodeMVCCVersionInTxn(txn, node, version); err != nil {
				return err
			}
			if err := b.writeNodeMVCCHeadInTxn(txn, node.ID, version, false); err != nil {
				return err
			}
		}
		return nil
	})
}

func (b *BadgerEngine) bootstrapEdgeMVCCFromCurrentState(ctx context.Context) error {
	start := []byte{prefixEdge}
	for {
		batch, lastScanned, reachedEnd, err := b.collectEdgeBootstrapBatch(ctx, start, mvccRebuildScanBatchSize)
		if err != nil {
			return err
		}
		if len(batch) > 0 {
			if err := b.applyEdgeBootstrapBatch(batch); err != nil {
				return err
			}
		}
		if reachedEnd {
			return nil
		}
		start = nextScanStart(lastScanned)
	}
}

func (b *BadgerEngine) collectEdgeBootstrapBatch(ctx context.Context, start []byte, limit int) ([]*Edge, []byte, bool, error) {
	edges := make([]*Edge, 0, limit)
	var lastScanned []byte
	reachedEnd := true
	err := b.withView(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte{prefixEdge}
		opts.PrefetchValues = true
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(start); it.ValidForPrefix(opts.Prefix); it.Next() {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			key := append([]byte(nil), it.Item().Key()...)
			if len(key) <= 1 {
				continue
			}
			id := EdgeID(key[1:])
			var edge *Edge
			if err := it.Item().Value(func(val []byte) error {
				var decodeErr error
				edge, decodeErr = decodeEdge(val)
				return decodeErr
			}); err != nil {
				return err
			}
			if edge == nil {
				continue
			}
			edge.ID = id
			edges = append(edges, edge)
			lastScanned = key
			if len(edges) >= limit {
				reachedEnd = false
				break
			}
		}
		return nil
	})
	return edges, lastScanned, reachedEnd, err
}

func (b *BadgerEngine) applyEdgeBootstrapBatch(edges []*Edge) error {
	return b.withUpdate(func(txn *badger.Txn) error {
		for _, edge := range edges {
			if edge == nil {
				continue
			}
			if _, err := b.loadEdgeMVCCHeadInTxn(txn, edge.ID); err == nil {
				continue
			} else if err != ErrNotFound {
				return err
			}
			version, err := b.allocateMVCCVersion(txn, mvccBootstrapTime(edge.CreatedAt, edge.UpdatedAt))
			if err != nil {
				return err
			}
			if err := b.writeEdgeMVCCVersionInTxn(txn, edge, version); err != nil {
				return err
			}
			if err := b.writeEdgeMVCCHeadInTxn(txn, edge.ID, version, false); err != nil {
				return err
			}
		}
		return nil
	})
}

func (b *BadgerEngine) bootstrapNodeMVCCFromCurrentStateInTxn(txn *badger.Txn) error {
	opts := badger.DefaultIteratorOptions
	opts.Prefix = []byte{prefixNode}
	opts.PrefetchValues = true
	it := txn.NewIterator(opts)
	defer it.Close()
	for it.Rewind(); it.Valid(); it.Next() {
		key := append([]byte(nil), it.Item().Key()...)
		if len(key) <= 1 {
			continue
		}
		id := NodeID(key[1:])
		if _, err := b.loadNodeMVCCHeadInTxn(txn, id); err == nil {
			continue
		} else if err != ErrNotFound {
			return err
		}
		var node *Node
		if err := it.Item().Value(func(val []byte) error {
			var decodeErr error
			node, decodeErr = decodeNodeWithEmbeddings(txn, val, id)
			return decodeErr
		}); err != nil {
			return err
		}
		version, err := b.allocateMVCCVersion(txn, mvccBootstrapTime(node.CreatedAt, node.UpdatedAt))
		if err != nil {
			return err
		}
		if err := b.writeNodeMVCCVersionInTxn(txn, node, version); err != nil {
			return err
		}
		if err := b.writeNodeMVCCHeadInTxn(txn, id, version, false); err != nil {
			return err
		}
	}
	return nil
}

func (b *BadgerEngine) bootstrapEdgeMVCCFromCurrentStateInTxn(txn *badger.Txn) error {
	opts := badger.DefaultIteratorOptions
	opts.Prefix = []byte{prefixEdge}
	opts.PrefetchValues = true
	it := txn.NewIterator(opts)
	defer it.Close()
	for it.Rewind(); it.Valid(); it.Next() {
		key := append([]byte(nil), it.Item().Key()...)
		if len(key) <= 1 {
			continue
		}
		id := EdgeID(key[1:])
		if _, err := b.loadEdgeMVCCHeadInTxn(txn, id); err == nil {
			continue
		} else if err != ErrNotFound {
			return err
		}
		var edge *Edge
		if err := it.Item().Value(func(val []byte) error {
			var decodeErr error
			edge, decodeErr = decodeEdge(val)
			return decodeErr
		}); err != nil {
			return err
		}
		version, err := b.allocateMVCCVersion(txn, mvccBootstrapTime(edge.CreatedAt, edge.UpdatedAt))
		if err != nil {
			return err
		}
		if err := b.writeEdgeMVCCVersionInTxn(txn, edge, version); err != nil {
			return err
		}
		if err := b.writeEdgeMVCCHeadInTxn(txn, id, version, false); err != nil {
			return err
		}
	}
	return nil
}

func (b *BadgerEngine) PruneMVCCVersions(ctx context.Context, opts MVCCPruneOptions) (int64, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if b.lifecycleController != nil && b.lifecycleController.IsLifecycleEnabled() {
		return b.lifecycleController.RunPruneNow(ctx, opts)
	}
	effectiveOpts := b.effectiveMVCCPruneOptions(opts)
	activeSnapshotReaders := b.activeMVCCSnapshotReaders.Load() > 0
	var deleted int64
	err := b.withUpdate(func(txn *badger.Txn) error {
		nodeDeleted, err := b.pruneMVCCKeyspaceInTxn(ctx, txn, []byte{prefixMVCCNode}, effectiveOpts, activeSnapshotReaders)
		if err != nil {
			return err
		}
		edgeDeleted, err := b.pruneMVCCKeyspaceInTxn(ctx, txn, []byte{prefixMVCCEdge}, effectiveOpts, activeSnapshotReaders)
		if err != nil {
			return err
		}
		deleted = nodeDeleted + edgeDeleted
		return nil
	})
	if err != nil {
		return 0, err
	}
	return deleted, nil
}

func (b *BadgerEngine) pruneMVCCKeyspaceInTxn(ctx context.Context, txn *badger.Txn, prefix []byte, opts MVCCPruneOptions, activeSnapshotReaders bool) (int64, error) {
	optsIter := badger.DefaultIteratorOptions
	optsIter.Prefix = prefix
	optsIter.PrefetchValues = true
	it := txn.NewIterator(optsIter)
	defer it.Close()

	type versionKey struct {
		logical    []byte
		full       []byte
		version    MVCCVersion
		tombstoned bool
	}

	currentGroup := make([]versionKey, 0)
	var currentLogical []byte
	var deleted int64
	protectedAfter := time.Time{}
	if opts.MinRetentionAge > 0 {
		protectedAfter = time.Now().UTC().Add(-opts.MinRetentionAge)
	}
	flush := func() error {
		if len(currentGroup) == 0 {
			currentGroup = currentGroup[:0]
			return nil
		}
		head, err := b.loadMVCCHeadForLogicalKeyInTxn(txn, currentGroup[0].logical)
		if err != nil {
			if err != ErrNotFound {
				return err
			}
			latest := currentGroup[len(currentGroup)-1]
			head = MVCCHead{Version: latest.version, Tombstoned: latest.tombstoned}
		}

		if head.Tombstoned && !activeSnapshotReaders && protectedAfter.IsZero() {
			for _, entry := range currentGroup {
				if entry.version.Compare(head.Version) == 0 {
					continue
				}
				if err := txn.Delete(entry.full); err != nil {
					return err
				}
				deleted++
			}
			if err := b.writeMVCCHeadForLogicalKeyInTxn(txn, currentGroup[0].logical, head.Version, true, head.Version); err != nil {
				return err
			}
			currentGroup = currentGroup[:0]
			return nil
		}

		if len(currentGroup) <= 1 {
			if err := b.writeMVCCHeadForLogicalKeyInTxn(txn, currentGroup[0].logical, head.Version, head.Tombstoned, currentGroup[0].version); err != nil {
				return err
			}
			currentGroup = currentGroup[:0]
			return nil
		}

		keepHistorical := opts.MaxVersionsPerKey
		maxDeleteIndex := len(currentGroup) - 1
		retainFrom := maxDeleteIndex - keepHistorical
		if retainFrom < 0 {
			retainFrom = 0
		}
		floorVersion := MVCCVersion{}
		for i := 0; i < maxDeleteIndex; i++ {
			if currentGroup[i].version.Compare(head.Version) == 0 {
				if floorVersion.IsZero() {
					floorVersion = currentGroup[i].version
				}
				continue
			}
			if i >= retainFrom {
				if floorVersion.IsZero() {
					floorVersion = currentGroup[i].version
				}
				continue
			}
			if !protectedAfter.IsZero() && currentGroup[i].version.CommitTimestamp.After(protectedAfter) {
				if floorVersion.IsZero() {
					floorVersion = currentGroup[i].version
				}
				continue
			}
			if err := txn.Delete(currentGroup[i].full); err != nil {
				return err
			}
			deleted++
		}
		if floorVersion.IsZero() {
			floorVersion = head.Version
		}
		if err := b.writeMVCCHeadForLogicalKeyInTxn(txn, currentGroup[0].logical, head.Version, head.Tombstoned, floorVersion); err != nil {
			return err
		}
		currentGroup = currentGroup[:0]
		return nil
	}

	for it.Rewind(); it.Valid(); it.Next() {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		default:
		}
		key := append([]byte(nil), it.Item().Key()...)
		logical, version, err := extractMVCCLogicalKeyAndVersion(key)
		if err != nil {
			return 0, err
		}
		var tombstoned bool
		if err := it.Item().Value(func(val []byte) error {
			switch prefix[0] {
			case prefixMVCCNode:
				record, decodeErr := decodeMVCCNodeRecord(val)
				if decodeErr != nil {
					return decodeErr
				}
				tombstoned = record.Tombstoned
			case prefixMVCCEdge:
				record, decodeErr := decodeMVCCEdgeRecord(val)
				if decodeErr != nil {
					return decodeErr
				}
				tombstoned = record.Tombstoned
			default:
				return fmt.Errorf("unsupported mvcc prune prefix: %x", prefix)
			}
			return nil
		}); err != nil {
			return 0, err
		}
		if currentLogical != nil && !bytes.Equal(currentLogical, logical) {
			if err := flush(); err != nil {
				return 0, err
			}
		}
		currentLogical = logical
		currentGroup = append(currentGroup, versionKey{logical: logical, full: key, version: version, tombstoned: tombstoned})
	}
	if err := flush(); err != nil {
		return 0, err
	}
	return deleted, nil
}

func (b *BadgerEngine) loadMVCCHeadForLogicalKeyInTxn(txn *badger.Txn, logical []byte) (MVCCHead, error) {
	if len(logical) < 2 {
		return MVCCHead{}, ErrInvalidData
	}
	switch logical[0] {
	case prefixMVCCNode:
		return b.loadNodeMVCCHeadInTxn(txn, NodeID(logical[1:]))
	case prefixMVCCEdge:
		return b.loadEdgeMVCCHeadInTxn(txn, EdgeID(logical[1:]))
	default:
		return MVCCHead{}, fmt.Errorf("unknown mvcc logical key prefix: %x", logical[0])
	}
}

func (b *BadgerEngine) writeMVCCHeadForLogicalKeyInTxn(txn *badger.Txn, logical []byte, version MVCCVersion, tombstoned bool, floorVersion MVCCVersion) error {
	if len(logical) < 2 {
		return ErrInvalidData
	}
	switch logical[0] {
	case prefixMVCCNode:
		return b.writeNodeMVCCHeadWithFloorInTxn(txn, NodeID(logical[1:]), version, tombstoned, floorVersion)
	case prefixMVCCEdge:
		return b.writeEdgeMVCCHeadWithFloorInTxn(txn, EdgeID(logical[1:]), version, tombstoned, floorVersion)
	default:
		return fmt.Errorf("unknown mvcc logical key prefix: %x", logical[0])
	}
}

func mvccBootstrapTime(createdAt, updatedAt time.Time) time.Time {
	if !updatedAt.IsZero() {
		return updatedAt.UTC()
	}
	if !createdAt.IsZero() {
		return createdAt.UTC()
	}
	return time.Now().UTC()
}

// ----- Plan 04-04-01 RISK-2: read-only accessors for observability D-15b ---
//
// PinnedBytes / OldestReaderAgeSeconds / ActiveReaders are pure-read
// accessors used by the observability MVCC bag's GaugeFunc callbacks
// (pkg/observability/catalog_mvcc.go). They satisfy the MVCCProbe interface
// declared next to NewMVCCMetrics — D-02d leaf-package boundary preserved
// because pkg/observability never imports pkg/storage; the engine here
// merely satisfies an interface that the observability layer declares.
//
// Concurrency: each method either reads an atomic counter or takes the
// existing reader-registry RWMutex via the lifecycle controller's
// SnapshotReaderRegistry. No new lock surface is introduced; all three
// methods are safe to call concurrently with reader open/close and with
// each other.
//
// Fallback path (no lifecycle controller wired): the engine tracks an
// atomic counter only. PinnedBytes returns 0 (no per-reader byte
// accounting in fallback); OldestReaderAgeSeconds returns 0 (no per-reader
// StartTime tracked in fallback); ActiveReaders returns the atomic count.
// Per RESEARCH RISK-2 the GaugeFunc callbacks tolerate 0 — the metric
// surface is what matters, not numeric fidelity in the unconfigured path.

// ActiveReaders returns the count of currently-open MVCC reader snapshots.
// Always non-nil; returns 0 when the engine has no active readers.
//
// Plan 04-04-01 RISK-2 fix — used by the observability MVCC bag's
// active_readers GaugeFunc callback.
func (b *BadgerEngine) ActiveReaders() int64 {
	b.mu.RLock()
	controller := b.lifecycleController
	b.mu.RUnlock()
	if controller != nil {
		if reg := controller.ReaderRegistry(); reg != nil {
			return reg.ActiveCount()
		}
	}
	return b.activeMVCCSnapshotReaders.Load()
}

// PinnedBytes returns the cumulative byte count pinned by all active MVCC
// readers. Returns 0 when no controller is wired (fallback path tracks
// counts only, not bytes). Always non-nil.
//
// Plan 04-04-01 RISK-2 fix — used by the observability MVCC bag's
// pinned_bytes GaugeFunc callback.
func (b *BadgerEngine) PinnedBytes() int64 {
	b.mu.RLock()
	controller := b.lifecycleController
	b.mu.RUnlock()
	if controller == nil {
		return 0
	}
	// Optional accessor: not every controller implementation tracks bytes.
	// We probe via the optional interface to avoid coupling the storage
	// types.go MVCCLifecycleController surface to a metric-only accessor.
	if probe, ok := controller.(interface{ PinnedBytes() int64 }); ok {
		return probe.PinnedBytes()
	}
	return 0
}

// OldestReaderAgeSeconds returns the wall-clock age (in seconds) of the
// oldest still-open MVCC reader snapshot. Returns 0 when no readers are
// active or no controller is wired (atomic-counter fallback does not
// track per-reader StartTime).
//
// Plan 04-04-01 RISK-2 fix — used by the observability MVCC bag's
// oldest_reader_age_seconds GaugeFunc callback.
func (b *BadgerEngine) OldestReaderAgeSeconds() float64 {
	b.mu.RLock()
	controller := b.lifecycleController
	b.mu.RUnlock()
	if controller == nil {
		return 0
	}
	// Probe via optional interface: the lifecycle.ReaderRegistry exposes
	// OldestReaderAge(); the observability layer wants seconds as float64.
	type ageProvider interface{ OldestReaderAge() time.Duration }
	if probe, ok := controller.(ageProvider); ok {
		return probe.OldestReaderAge().Seconds()
	}
	if reg := controller.ReaderRegistry(); reg != nil {
		if probe, ok := reg.(ageProvider); ok {
			return probe.OldestReaderAge().Seconds()
		}
	}
	return 0
}
