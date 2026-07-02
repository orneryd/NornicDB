package storage

import (
	"bytes"
	"context"
	"encoding/binary"
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

type mvccAdjacencyRecord struct {
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

func encodeMVCCAdjacencyRecord(tombstoned bool) []byte {
	if tombstoned {
		return []byte{1}
	}
	return []byte{0}
}

func decodeMVCCAdjacencyRecord(data []byte) (mvccAdjacencyRecord, error) {
	if len(data) != 1 {
		return mvccAdjacencyRecord{}, fmt.Errorf("invalid mvcc adjacency record length: %d", len(data))
	}
	return mvccAdjacencyRecord{Tombstoned: data[0] != 0}, nil
}

// MVCCHead is written once per entity, updated on every commit, and
// consulted on every read. Encoded as a compact binary blob to minimize
// on-disk footprint and allocation pressure:
//
//	[0]        : version byte (currently 1 for compact binary)
//	[1]        : flags (bit 0 = tombstoned, bit 1 = floor != version)
//	[2..9]     : Version.CommitTimestamp nanos (big-endian int64 UTC)
//	[10..17]   : Version.CommitSequence (big-endian uint64)
//	[18..25]   : FloorVersion.CommitTimestamp nanos (only if flag bit 1)
//	[26..33]   : FloorVersion.CommitSequence (only if flag bit 1)
//
// The common case — a fresh CREATE or an update where floor==version —
// emits only 18 bytes. Prior msgpack encoding was ~170 bytes per head.
//
// Legacy msgpack-encoded heads are still decodable via fallback so
// existing data directories continue to work across the format change.
const (
	mvccHeadCompactVersion byte = 1
	mvccHeadFlagTombstoned byte = 1 << 0
	mvccHeadFlagHasFloor   byte = 1 << 1

	mvccHeadCompactMinLen  = 18
	mvccHeadCompactFullLen = 34
)

func encodeMVCCHead(head MVCCHead) ([]byte, error) {
	head = normalizeMVCCHead(head)
	flags := byte(0)
	if head.Tombstoned {
		flags |= mvccHeadFlagTombstoned
	}
	hasFloor := head.FloorVersion.Compare(head.Version) != 0
	if hasFloor {
		flags |= mvccHeadFlagHasFloor
	}
	size := mvccHeadCompactMinLen
	if hasFloor {
		size = mvccHeadCompactFullLen
	}
	buf := make([]byte, size)
	buf[0] = mvccHeadCompactVersion
	buf[1] = flags
	binary.BigEndian.PutUint64(buf[2:10], uint64(head.Version.CommitTimestamp.UTC().UnixNano()))
	binary.BigEndian.PutUint64(buf[10:18], head.Version.CommitSequence)
	if hasFloor {
		binary.BigEndian.PutUint64(buf[18:26], uint64(head.FloorVersion.CommitTimestamp.UTC().UnixNano()))
		binary.BigEndian.PutUint64(buf[26:34], head.FloorVersion.CommitSequence)
	}
	return buf, nil
}

func decodeMVCCHead(data []byte) (MVCCHead, error) {
	if len(data) >= 1 && data[0] == mvccHeadCompactVersion {
		if len(data) < mvccHeadCompactMinLen {
			return MVCCHead{}, fmt.Errorf("mvcc head truncated: %d bytes", len(data))
		}
		flags := data[1]
		head := MVCCHead{
			Version: MVCCVersion{
				CommitTimestamp: time.Unix(0, int64(binary.BigEndian.Uint64(data[2:10]))).UTC(),
				CommitSequence:  binary.BigEndian.Uint64(data[10:18]),
			},
			Tombstoned: flags&mvccHeadFlagTombstoned != 0,
		}
		if flags&mvccHeadFlagHasFloor != 0 {
			if len(data) < mvccHeadCompactFullLen {
				return MVCCHead{}, fmt.Errorf("mvcc head missing floor: %d bytes", len(data))
			}
			head.FloorVersion = MVCCVersion{
				CommitTimestamp: time.Unix(0, int64(binary.BigEndian.Uint64(data[18:26]))).UTC(),
				CommitSequence:  binary.BigEndian.Uint64(data[26:34]),
			}
		}
		return normalizeMVCCHead(head), nil
	}
	// Legacy msgpack fallback for pre-compact data directories.
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
	key, err := b.mvccNodeVersionKeyString(txn, node.ID, version)
	if err != nil {
		return err
	}
	return txn.Set(key, encoded)
}

func (b *BadgerEngine) writeNodeMVCCTombstoneInTxn(txn *badger.Txn, id NodeID, version MVCCVersion) error {
	encoded, err := encodeMVCCNodeRecord(nil, true)
	if err != nil {
		return err
	}
	key, err := b.mvccNodeVersionKeyString(txn, id, version)
	if err != nil {
		return err
	}
	return txn.Set(key, encoded)
}

func (b *BadgerEngine) writeEdgeMVCCVersionInTxn(txn *badger.Txn, edge *Edge, version MVCCVersion) error {
	encoded, err := encodeMVCCEdgeRecord(edge, false)
	if err != nil {
		return err
	}
	key, err := b.mvccEdgeVersionKeyString(txn, edge.ID, version)
	if err != nil {
		return err
	}
	return txn.Set(key, encoded)
}

func (b *BadgerEngine) writeEdgeMVCCTombstoneInTxn(txn *badger.Txn, id EdgeID, version MVCCVersion) error {
	encoded, err := encodeMVCCEdgeRecord(nil, true)
	if err != nil {
		return err
	}
	key, err := b.mvccEdgeVersionKeyString(txn, id, version)
	if err != nil {
		return err
	}
	return txn.Set(key, encoded)
}

func (b *BadgerEngine) writeOutgoingAdjacencyMVCCVersionInTxn(txn *badger.Txn, nodeID NodeID, edgeID EdgeID, version MVCCVersion, tombstoned bool) error {
	key, err := b.mvccOutgoingAdjacencyKeyString(txn, nodeID, edgeID, version)
	if err != nil {
		return err
	}
	return txn.Set(key, encodeMVCCAdjacencyRecord(tombstoned))
}

func (b *BadgerEngine) writeIncomingAdjacencyMVCCVersionInTxn(txn *badger.Txn, nodeID NodeID, edgeID EdgeID, version MVCCVersion, tombstoned bool) error {
	key, err := b.mvccIncomingAdjacencyKeyString(txn, nodeID, edgeID, version)
	if err != nil {
		return err
	}
	return txn.Set(key, encodeMVCCAdjacencyRecord(tombstoned))
}

func (b *BadgerEngine) writeEdgeAdjacencyLiveInTxn(txn *badger.Txn, edge *Edge, version MVCCVersion) error {
	if edge == nil {
		return nil
	}
	if err := b.writeOutgoingAdjacencyMVCCVersionInTxn(txn, edge.StartNode, edge.ID, version, false); err != nil {
		return err
	}
	return b.writeIncomingAdjacencyMVCCVersionInTxn(txn, edge.EndNode, edge.ID, version, false)
}

func (b *BadgerEngine) writeEdgeAdjacencyTombstoneInTxn(txn *badger.Txn, edge *Edge, version MVCCVersion) error {
	if edge == nil {
		return nil
	}
	if err := b.writeOutgoingAdjacencyMVCCVersionInTxn(txn, edge.StartNode, edge.ID, version, true); err != nil {
		return err
	}
	return b.writeIncomingAdjacencyMVCCVersionInTxn(txn, edge.EndNode, edge.ID, version, true)
}

func (b *BadgerEngine) writeEdgeAdjacencyDeltaInTxn(txn *badger.Txn, oldEdge, newEdge *Edge, version MVCCVersion) error {
	if oldEdge != nil {
		if err := b.writeEdgeAdjacencyTombstoneInTxn(txn, oldEdge, version); err != nil {
			return err
		}
	}
	if newEdge != nil {
		if err := b.writeEdgeAdjacencyLiveInTxn(txn, newEdge, version); err != nil {
			return err
		}
	}
	return nil
}

func (b *BadgerEngine) loadEdgeForAdjacencyTombstoneInTxn(txn *badger.Txn, id EdgeID) (*Edge, error) {
	item, err := txn.Get(edgeKey(id))
	if err == nil {
		var edge *Edge
		if err := item.Value(func(val []byte) error {
			var decodeErr error
			edge, decodeErr = b.decodeEdgeBodyByID(val, id)
			return decodeErr
		}); err != nil {
			return nil, err
		}
		return edge, nil
	}
	if err != badger.ErrKeyNotFound {
		return nil, err
	}
	head, err := b.loadEdgeMVCCHeadInTxn(txn, id)
	if err != nil {
		return nil, err
	}
	record, _, err := b.loadEdgeMVCCRecordAtOrBeforeInTxn(txn, id, head.Version)
	if err != nil {
		return nil, err
	}
	if record.Tombstoned || record.Edge == nil {
		return nil, ErrNotFound
	}
	return copyEdge(record.Edge), nil
}

func (b *BadgerEngine) getEdgeVisibleAtInTxn(txn *badger.Txn, id EdgeID, version MVCCVersion) (*Edge, error) {
	head, err := b.loadEdgeMVCCHeadInTxn(txn, id)
	if err != nil {
		return nil, err
	}
	if version.Compare(head.FloorVersion) < 0 {
		return nil, ErrNotVisibleAtSnapshot
	}

	if version.Compare(head.Version) >= 0 && !head.Tombstoned {
		item, getErr := txn.Get(edgeKey(id))
		if getErr == nil {
			var edge *Edge
			if err := item.Value(func(val []byte) error {
				decoded, decodeErr := b.decodeEdgeBodyByID(val, id)
				if decodeErr != nil {
					return decodeErr
				}
				decoded.ID = id
				edge = decoded
				return nil
			}); err != nil {
				return nil, err
			}
			if b.filterEdgeByDecay(edge, DecayScoringTime()) {
				return nil, ErrNotFound
			}
			return edge, nil
		}
		if getErr != badger.ErrKeyNotFound {
			return nil, getErr
		}
	}
	if head.Tombstoned && version.Compare(head.Version) >= 0 {
		return nil, ErrNotFound
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
			return nil, err
		}
	}
	if record.Tombstoned || record.Edge == nil {
		return nil, ErrNotFound
	}
	edge := copyEdge(record.Edge)
	if b.filterEdgeByDecay(edge, DecayScoringTime()) {
		return nil, ErrNotFound
	}
	return edge, nil
}

func (b *BadgerEngine) collectVisibleAdjacencyEdgeIDsInTxn(txn *badger.Txn, prefix []byte, version MVCCVersion) ([]EdgeID, error) {
	if len(prefix) == 0 {
		return nil, nil
	}
	seek := append(append([]byte{}, prefix...), bytes.Repeat([]byte{0xFF}, 24)...)
	seen := make(map[uint64]struct{})
	edgeIDs := make([]EdgeID, 0)
	opts := badger.DefaultIteratorOptions
	opts.Prefix = prefix
	opts.PrefetchValues = true
	opts.Reverse = true
	it := txn.NewIterator(opts)
	defer it.Close()
	for it.Seek(seek); it.ValidForPrefix(prefix); it.Next() {
		key := append([]byte(nil), it.Item().Key()...)
		edgeNum, recordVersion, err := extractEdgeNumIDAndMVCCVersionFromAdjacencyKey(key)
		if err != nil {
			return nil, err
		}
		if _, ok := seen[edgeNum]; ok {
			continue
		}
		if recordVersion.Compare(version) > 0 {
			continue
		}
		seen[edgeNum] = struct{}{}
		var record mvccAdjacencyRecord
		if err := it.Item().Value(func(val []byte) error {
			var decodeErr error
			record, decodeErr = decodeMVCCAdjacencyRecord(val)
			return decodeErr
		}); err != nil {
			return nil, err
		}
		if record.Tombstoned {
			continue
		}
		edgeID, ok := b.idDict.lookupEdgeIDByNum(edgeNum)
		if ok && edgeID != "" {
			edgeIDs = append(edgeIDs, edgeID)
		}
	}
	return edgeIDs, nil
}

func (b *BadgerEngine) writeNodeMVCCHeadInTxn(txn *badger.Txn, id NodeID, version MVCCVersion, tombstoned bool) error {
	floorVersion := version
	// Read the existing head via a SEPARATE read txn so the lookup does
	// not enter the user txn's SSI read set. Without this, an OpUpdateNode
	// targeting a peer-committed node (a MERGE that matched a node
	// committed by a concurrent writer between this txn's begin and
	// commit) would put the peer's mvcc-head key into the user txn's
	// read set, causing Badger to reject the commit with a generic
	// "Transaction Conflict" instead of letting the consumer-pinned
	// constraint-violation shape surface (see
	// docs/plans/consumer-pinned-error-contract-plan.md §2.1).
	if existing, err := b.loadNodeMVCCHead(id); err == nil {
		floorVersion = existing.FloorVersion
	} else if err != ErrNotFound {
		return err
	}
	return b.writeNodeMVCCHeadWithFloorInTxn(txn, id, version, tombstoned, floorVersion)
}

// writeNodeMVCCHeadForFreshCreateInTxn skips the pre-read that carries an
// existing FloorVersion forward. Callers must only invoke this for brand-new
// IDs where no prior head can exist (OpCreateNode in the commit loop). For a
// fresh entity the floor == the current version. Halves the Badger op count
// on create-heavy workloads (one Set instead of one Get + one Set per entity).
func (b *BadgerEngine) writeNodeMVCCHeadForFreshCreateInTxn(txn *badger.Txn, id NodeID, version MVCCVersion) error {
	return b.writeNodeMVCCHeadWithFloorInTxn(txn, id, version, false, version)
}

// retentionRetainsHistory reports whether the engine is configured to keep
// any closed historical MVCC versions. When false, every archival call is a
// no-op — updates overwrite in place, deletes drop the primary key outright,
// and there's no write amplification from duplicating bodies into the MVCC
// keyspace.
func (b *BadgerEngine) retentionRetainsHistory() bool {
	return normalizeRetentionPolicy(b.retentionPolicy).RetainsHistory()
}

// mustArchiveForHistory reports whether an in-flight write MUST archive the
// superseded body before overwriting the primary key. This is true when
// either retention is configured OR there's an active snapshot reader that
// may need to resolve reads at the pre-update version. The latter keeps
// snapshot isolation correct even when retention is head-only (the
// archived body stays until pruning fires after all readers drain).
func (b *BadgerEngine) mustArchiveForHistory() bool {
	if b.retentionRetainsHistory() {
		return true
	}
	return b.activeMVCCSnapshotReaders.Load() > 0
}

// archiveNodePrimaryIntoMVCCVersionInTxn moves the current primary-key body
// into a versioned MVCC record so snapshot reads at prior versions can still
// resolve it. Callers must invoke this BEFORE overwriting or deleting the
// primary key. If the primary key doesn't exist yet (brand-new entity, or
// already archived), this is a no-op. When retention is disabled
// (MaxVersionsPerKey <= 0) this is also a no-op — callers can invoke
// unconditionally and pay only a single bool check.
//
// Storage-layout invariant (post-refactor): the primary key always holds the
// CURRENT head body. Historical bodies live at mvccNodeVersionKey(id, v) for
// each past head version v. A fresh CREATE writes only the primary key + head;
// UPDATE and DELETE migrate the superseded body here before overwriting.
func (b *BadgerEngine) archiveNodePrimaryIntoMVCCVersionInTxn(txn *badger.Txn, id NodeID, oldVersion MVCCVersion) error {
	if !b.mustArchiveForHistory() {
		return nil
	}
	item, err := txn.Get(nodeKey(id))
	if err == badger.ErrKeyNotFound {
		return nil
	}
	if err != nil {
		return fmt.Errorf("archiving node %s: reading primary: %w", id, err)
	}
	// If a version record already exists at oldVersion (e.g. test seeded both
	// forms, or legacy data), don't stomp it.
	if existingKey := b.mvccNodeVersionKeyStringLookup(id, oldVersion); existingKey != nil {
		if _, existsErr := txn.Get(existingKey); existsErr == nil {
			return nil
		} else if existsErr != badger.ErrKeyNotFound {
			return fmt.Errorf("archiving node %s: probing version: %w", id, existsErr)
		}
	}
	var node *Node
	if err := item.Value(func(val []byte) error {
		var decodeErr error
		node, decodeErr = b.decodeNodeWithEmbeddings(txn, val, id)
		return decodeErr
	}); err != nil {
		return fmt.Errorf("archiving node %s: decoding primary: %w", id, err)
	}
	if node == nil {
		return nil
	}
	return b.writeNodeMVCCVersionInTxn(txn, node, oldVersion)
}

// archiveEdgePrimaryIntoMVCCVersionInTxn is the edge analogue of
// archiveNodePrimaryIntoMVCCVersionInTxn. See that function's doc for invariant.
func (b *BadgerEngine) archiveEdgePrimaryIntoMVCCVersionInTxn(txn *badger.Txn, id EdgeID, oldVersion MVCCVersion) error {
	if !b.mustArchiveForHistory() {
		return nil
	}
	item, err := txn.Get(edgeKey(id))
	if err == badger.ErrKeyNotFound {
		return nil
	}
	if err != nil {
		return fmt.Errorf("archiving edge %s: reading primary: %w", id, err)
	}
	if existingKey := b.mvccEdgeVersionKeyStringLookup(id, oldVersion); existingKey != nil {
		if _, existsErr := txn.Get(existingKey); existsErr == nil {
			return nil
		} else if existsErr != badger.ErrKeyNotFound {
			return fmt.Errorf("archiving edge %s: probing version: %w", id, existsErr)
		}
	}
	var edge *Edge
	if err := item.Value(func(val []byte) error {
		var decodeErr error
		edge, decodeErr = b.decodeEdgeBodyByID(val, id)
		return decodeErr
	}); err != nil {
		return fmt.Errorf("archiving edge %s: decoding primary: %w", id, err)
	}
	if edge == nil {
		return nil
	}
	edge.ID = id
	return b.writeEdgeMVCCVersionInTxn(txn, edge, oldVersion)
}

// archiveNodeOnUpdateInTxn archives the current primary-key body (if any) and
// then writes the new head. Used by update/delete paths. When no prior head
// exists (fresh insert via upsert), this is equivalent to writing the head.
// Becomes a pure no-op when retention is disabled.
func (b *BadgerEngine) archiveNodeOnUpdateInTxn(txn *badger.Txn, id NodeID) error {
	if !b.mustArchiveForHistory() {
		return nil
	}
	existing, err := b.loadNodeMVCCHeadInTxn(txn, id)
	if err == ErrNotFound {
		return nil
	}
	if err != nil {
		return err
	}
	if existing.Tombstoned {
		return nil
	}
	return b.archiveNodePrimaryIntoMVCCVersionInTxn(txn, id, existing.Version)
}

// archiveEdgeOnUpdateInTxn is the edge analogue of archiveNodeOnUpdateInTxn.
func (b *BadgerEngine) archiveEdgeOnUpdateInTxn(txn *badger.Txn, id EdgeID) error {
	if !b.mustArchiveForHistory() {
		return nil
	}
	existing, err := b.loadEdgeMVCCHeadInTxn(txn, id)
	if err == ErrNotFound {
		return nil
	}
	if err != nil {
		return err
	}
	if existing.Tombstoned {
		return nil
	}
	return b.archiveEdgePrimaryIntoMVCCVersionInTxn(txn, id, existing.Version)
}

// archiveNodeBodyInTxn writes a known node body to mvccNodeVersionKey(id,
// atVersion). Used by the delete path where the caller already has the old
// body in memory — avoids re-reading the primary key.
func (b *BadgerEngine) archiveNodeBodyInTxn(txn *badger.Txn, id NodeID, body *Node, atVersion MVCCVersion) error {
	if !b.mustArchiveForHistory() {
		return nil
	}
	if body == nil {
		return nil
	}
	if existing := b.mvccNodeVersionKeyStringLookup(id, atVersion); existing != nil {
		if _, err := txn.Get(existing); err == nil {
			return nil
		} else if err != badger.ErrKeyNotFound {
			return err
		}
	}
	return b.writeNodeMVCCVersionInTxn(txn, body, atVersion)
}

// archiveEdgeBodyInTxn is the edge analogue of archiveNodeBodyInTxn.
func (b *BadgerEngine) archiveEdgeBodyInTxn(txn *badger.Txn, id EdgeID, body *Edge, atVersion MVCCVersion) error {
	if !b.mustArchiveForHistory() {
		return nil
	}
	if body == nil {
		return nil
	}
	if existing := b.mvccEdgeVersionKeyStringLookup(id, atVersion); existing != nil {
		if _, err := txn.Get(existing); err == nil {
			return nil
		} else if err != badger.ErrKeyNotFound {
			return err
		}
	}
	return b.writeEdgeMVCCVersionInTxn(txn, body, atVersion)
}

func (b *BadgerEngine) writeEdgeMVCCHeadInTxn(txn *badger.Txn, id EdgeID, version MVCCVersion, tombstoned bool) error {
	floorVersion := version
	// Read via a fresh read txn so the lookup stays out of the user
	// txn's SSI read set — see the doc on writeNodeMVCCHeadInTxn for
	// the same rationale on the node side.
	if existing, err := b.loadEdgeMVCCHead(id); err == nil {
		floorVersion = existing.FloorVersion
	} else if err != ErrNotFound {
		return err
	}
	return b.writeEdgeMVCCHeadWithFloorInTxn(txn, id, version, tombstoned, floorVersion)
}

// loadEdgeMVCCHead is the edge analogue of loadNodeMVCCHead. Reads the
// edge MVCC head via a fresh read txn.
func (b *BadgerEngine) loadEdgeMVCCHead(id EdgeID) (MVCCHead, error) {
	key := b.mvccEdgeHeadKeyStringLookup(id)
	if key == nil {
		return MVCCHead{}, ErrNotFound
	}
	var head MVCCHead
	err := b.db.View(func(rtxn *badger.Txn) error {
		item, err := rtxn.Get(key)
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			var decodeErr error
			head, decodeErr = decodeMVCCHead(val)
			return decodeErr
		})
	})
	if err == badger.ErrKeyNotFound {
		return MVCCHead{}, ErrNotFound
	}
	if err != nil {
		return MVCCHead{}, err
	}
	return head, nil
}

// writeEdgeMVCCHeadForFreshCreateInTxn is the edge analogue of
// writeNodeMVCCHeadForFreshCreateInTxn. Skips the pre-read for OpCreateEdge in
// the commit loop — fresh edge IDs cannot have an existing head.
func (b *BadgerEngine) writeEdgeMVCCHeadForFreshCreateInTxn(txn *badger.Txn, id EdgeID, version MVCCVersion) error {
	return b.writeEdgeMVCCHeadWithFloorInTxn(txn, id, version, false, version)
}

func (b *BadgerEngine) writeNodeMVCCHeadWithFloorInTxn(txn *badger.Txn, id NodeID, version MVCCVersion, tombstoned bool, floorVersion MVCCVersion) error {
	encoded, err := encodeMVCCHead(MVCCHead{Version: version, Tombstoned: tombstoned, FloorVersion: floorVersion})
	if err != nil {
		return err
	}
	key, err := b.mvccNodeHeadKeyString(txn, id)
	if err != nil {
		return err
	}
	return txn.Set(key, encoded)
}

func (b *BadgerEngine) writeEdgeMVCCHeadWithFloorInTxn(txn *badger.Txn, id EdgeID, version MVCCVersion, tombstoned bool, floorVersion MVCCVersion) error {
	encoded, err := encodeMVCCHead(MVCCHead{Version: version, Tombstoned: tombstoned, FloorVersion: floorVersion})
	if err != nil {
		return err
	}
	key, err := b.mvccEdgeHeadKeyString(txn, id)
	if err != nil {
		return err
	}
	return txn.Set(key, encoded)
}

// loadNodeMVCCHead reads the MVCC head record via a fresh read-only
// transaction. Use this on the writer-side commit path to keep the
// FloorVersion-carry-forward read out of the user txn's SSI read set
// (see writeNodeMVCCHeadInTxn). Read-after-write within the SAME
// transaction is rare for MVCC heads in practice — the user txn writes
// the head once at materialize time — but if you need it, use
// loadNodeMVCCHeadInTxn instead.
func (b *BadgerEngine) loadNodeMVCCHead(id NodeID) (MVCCHead, error) {
	key := b.mvccNodeHeadKeyStringLookup(id)
	if key == nil {
		return MVCCHead{}, ErrNotFound
	}
	var head MVCCHead
	err := b.db.View(func(rtxn *badger.Txn) error {
		item, err := rtxn.Get(key)
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			var decodeErr error
			head, decodeErr = decodeMVCCHead(val)
			return decodeErr
		})
	})
	if err == badger.ErrKeyNotFound {
		return MVCCHead{}, ErrNotFound
	}
	if err != nil {
		return MVCCHead{}, err
	}
	return head, nil
}

func (b *BadgerEngine) loadNodeMVCCHeadInTxn(txn *badger.Txn, id NodeID) (MVCCHead, error) {
	key := b.mvccNodeHeadKeyStringLookup(id)
	if key == nil {
		return MVCCHead{}, ErrNotFound
	}
	item, err := txn.Get(key)
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
	key := b.mvccEdgeHeadKeyStringLookup(id)
	if key == nil {
		return MVCCHead{}, ErrNotFound
	}
	item, err := txn.Get(key)
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
	key := b.mvccNodeVersionKeyStringLookup(id, version)
	if key == nil {
		return mvccNodeRecord{}, ErrNotFound
	}
	item, err := txn.Get(key)
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
	key := b.mvccEdgeVersionKeyStringLookup(id, version)
	if key == nil {
		return mvccEdgeRecord{}, ErrNotFound
	}
	item, err := txn.Get(key)
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
	prefix := b.mvccNodeVersionPrefixString(id)
	if prefix == nil {
		return mvccNodeRecord{}, MVCCVersion{}, ErrNotFound
	}
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
	prefix := b.mvccEdgeVersionPrefixString(id)
	if prefix == nil {
		return mvccEdgeRecord{}, MVCCVersion{}, ErrNotFound
	}
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
	// Post-refactor invariant: the primary nodeKey holds the current head
	// body. GetNode already applies decay filtering. If the primary-key
	// read succeeds, we're done — no need to resolve a version record.
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
		// Head says the node lives but GetNode failed or was filtered —
		// fall back to version records (e.g. pre-refactor data where the
		// primary key is absent but a version record exists).
		record, _, err := b.loadNodeMVCCRecordAtOrBeforeInTxn(txn, id, head.Version)
		if err != nil {
			return err
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
			// Head exists but the caller's snapshot cannot see it —
			// SI violation if we let the caller fall back to a
			// primary-key read. Distinct sentinel so the
			// transaction-layer fallback can recognize this and
			// return ErrNotFound to the user without exposing the
			// peer's commit.
			return ErrNotVisibleAtSnapshot
		}

		// Fast path: the caller is reading at or after the current head
		// version and the entity is not tombstoned — the primary key IS
		// the current head body. This avoids the extra version-record
		// read and halves write amplification on the commit path.
		if version.Compare(head.Version) >= 0 && !head.Tombstoned {
			item, getErr := txn.Get(nodeKey(id))
			if getErr == nil {
				itemVersion := item.Version()
				if cached, ok := b.cacheLoadNodeBody(id, itemVersion); ok {
					node = cached
					if b.filterNodeByDecay(node, DecayScoringTime()) {
						node = nil
						return ErrNotFound
					}
					return nil
				}
				return item.Value(func(val []byte) error {
					decoded, decodeErr := b.decodeNodeWithEmbeddings(txn, val, id)
					if decodeErr != nil {
						return decodeErr
					}
					if decoded == nil {
						return ErrNotFound
					}
					node = decoded
					b.cacheStoreNodeBody(id, itemVersion, decoded)
					if b.filterNodeByDecay(node, DecayScoringTime()) {
						node = nil
						return ErrNotFound
					}
					return nil
				})
			}
			if getErr != badger.ErrKeyNotFound {
				return getErr
			}
			// Primary key missing despite head claiming live — fall
			// through to version-record resolution so we can still
			// serve pre-refactor data that lacks a primary-key body.
		}
		if head.Tombstoned && version.Compare(head.Version) >= 0 {
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
		// Fallback path only — primary-key read already attempted via
		// GetEdge above. See GetNodeLatestVisible for the node analogue
		// and the post-refactor storage-layout invariant.
		record, _, err := b.loadEdgeMVCCRecordAtOrBeforeInTxn(txn, id, head.Version)
		if err != nil {
			return err
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
		var innerErr error
		edge, innerErr = b.getEdgeVisibleAtInTxn(txn, id, version)
		return innerErr
	})
	return edge, err
}

func (b *BadgerEngine) GetOutgoingEdgesVisibleAt(nodeID NodeID, version MVCCVersion) ([]*Edge, error) {
	if nodeID == "" {
		return nil, ErrInvalidID
	}
	deregister, err := b.beginMVCCSnapshotRead(version)
	if err != nil {
		return nil, err
	}
	defer deregister()
	var edges []*Edge
	err = b.withView(func(txn *badger.Txn) error {
		edgeIDs, err := b.collectVisibleAdjacencyEdgeIDsInTxn(txn, b.mvccOutgoingAdjacencyPrefixString(nodeID), version)
		if err != nil {
			return err
		}
		for _, edgeID := range edgeIDs {
			edge, edgeErr := b.getEdgeVisibleAtInTxn(txn, edgeID, version)
			if edgeErr == ErrNotFound || edgeErr == ErrNotVisibleAtSnapshot {
				continue
			}
			if edgeErr != nil {
				return edgeErr
			}
			if edge != nil && edge.StartNode == nodeID {
				edges = append(edges, edge)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return edges, nil
}

func (b *BadgerEngine) GetIncomingEdgesVisibleAt(nodeID NodeID, version MVCCVersion) ([]*Edge, error) {
	if nodeID == "" {
		return nil, ErrInvalidID
	}
	deregister, err := b.beginMVCCSnapshotRead(version)
	if err != nil {
		return nil, err
	}
	defer deregister()
	var edges []*Edge
	err = b.withView(func(txn *badger.Txn) error {
		edgeIDs, err := b.collectVisibleAdjacencyEdgeIDsInTxn(txn, b.mvccIncomingAdjacencyPrefixString(nodeID), version)
		if err != nil {
			return err
		}
		for _, edgeID := range edgeIDs {
			edge, edgeErr := b.getEdgeVisibleAtInTxn(txn, edgeID, version)
			if edgeErr == ErrNotFound || edgeErr == ErrNotVisibleAtSnapshot {
				continue
			}
			if edgeErr != nil {
				return edgeErr
			}
			if edge != nil && edge.EndNode == nodeID {
				edges = append(edges, edge)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return edges, nil
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
	// Post-refactor: we walk the head keyspace (the definitive list of
	// live node IDs) and resolve each ID to a body. If the request is at
	// or after the current head version and the entity is live, the body
	// comes from the primary nodeKey. Otherwise we fall back to historic
	// mvccNodeVersionKey records (only present when retention > 0).
	headPrefix := []byte{prefixMVCCNodeHead}
	opts := badger.DefaultIteratorOptions
	opts.Prefix = headPrefix
	opts.PrefetchValues = true
	it := txn.NewIterator(opts)
	defer it.Close()

	nowNanos := DecayScoringTime()
	for it.Rewind(); it.Valid(); it.Next() {
		key := append([]byte(nil), it.Item().Key()...)
		if len(key) != 1+8 {
			continue
		}
		nodeNum := binary.BigEndian.Uint64(key[1:])
		nodeID, ok := b.idDict.lookupNodeIDByNum(nodeNum)
		if !ok {
			continue
		}
		var head MVCCHead
		if err := it.Item().Value(func(val []byte) error {
			decoded, decodeErr := decodeMVCCHead(val)
			if decodeErr != nil {
				return decodeErr
			}
			head = decoded
			return nil
		}); err != nil {
			return err
		}
		if version.Compare(head.FloorVersion) < 0 {
			continue
		}
		var body *Node
		if version.Compare(head.Version) >= 0 {
			if head.Tombstoned {
				continue
			}
			item, getErr := txn.Get(nodeKey(nodeID))
			if getErr == badger.ErrKeyNotFound {
				// Primary missing — fall through to version-record lookup.
			} else if getErr != nil {
				return getErr
			} else {
				if err := item.Value(func(val []byte) error {
					decoded, decodeErr := b.decodeNodeWithEmbeddings(txn, val, nodeID)
					if decodeErr != nil {
						return decodeErr
					}
					body = decoded
					return nil
				}); err != nil {
					return err
				}
			}
		}
		if body == nil {
			record, _, err := b.loadNodeMVCCRecordAtOrBeforeInTxn(txn, nodeID, version)
			if err == ErrNotFound {
				continue
			}
			if err != nil {
				return err
			}
			if record.Tombstoned || record.Node == nil {
				continue
			}
			body = copyNode(record.Node)
		}
		if body == nil {
			continue
		}
		if b.filterNodeByDecay(body, nowNanos) {
			continue
		}
		if err := yield(body); err != nil {
			return err
		}
	}
	return nil
}

func (b *BadgerEngine) iterateEdgesVisibleAtInTxn(txn *badger.Txn, version MVCCVersion, yield func(*Edge) error) error {
	// See iterateNodesVisibleAtInTxn for the walk-heads pattern and why
	// we can't just scan the version prefix post-refactor.
	headPrefix := []byte{prefixMVCCEdgeHead}
	opts := badger.DefaultIteratorOptions
	opts.Prefix = headPrefix
	opts.PrefetchValues = true
	it := txn.NewIterator(opts)
	defer it.Close()

	nowNanos := DecayScoringTime()
	for it.Rewind(); it.Valid(); it.Next() {
		key := append([]byte(nil), it.Item().Key()...)
		if len(key) != 1+8 {
			continue
		}
		edgeNum := binary.BigEndian.Uint64(key[1:])
		edgeID, ok := b.idDict.lookupEdgeIDByNum(edgeNum)
		if !ok {
			continue
		}
		var head MVCCHead
		if err := it.Item().Value(func(val []byte) error {
			decoded, decodeErr := decodeMVCCHead(val)
			if decodeErr != nil {
				return decodeErr
			}
			head = decoded
			return nil
		}); err != nil {
			return err
		}
		if version.Compare(head.FloorVersion) < 0 {
			continue
		}
		var body *Edge
		if version.Compare(head.Version) >= 0 {
			if head.Tombstoned {
				continue
			}
			item, getErr := txn.Get(edgeKey(edgeID))
			if getErr == badger.ErrKeyNotFound {
				// Primary missing — fall through to version-record lookup.
			} else if getErr != nil {
				return getErr
			} else {
				if err := item.Value(func(val []byte) error {
					decoded, decodeErr := b.decodeEdgeBodyByID(val, edgeID)
					if decodeErr != nil {
						return decodeErr
					}
					if decoded != nil {
						decoded.ID = edgeID
					}
					body = decoded
					return nil
				}); err != nil {
					return err
				}
			}
		}
		if body == nil {
			record, _, err := b.loadEdgeMVCCRecordAtOrBeforeInTxn(txn, edgeID, version)
			if err == ErrNotFound {
				continue
			}
			if err != nil {
				return err
			}
			if record.Tombstoned || record.Edge == nil {
				continue
			}
			body = copyEdge(record.Edge)
		}
		if body == nil {
			continue
		}
		if b.filterEdgeByDecay(body, nowNanos) {
			continue
		}
		if err := yield(body); err != nil {
			return err
		}
	}
	return nil
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

// materializeMVCCCommitInTxn writes MVCC head metadata for every committed
// operation. Post-refactor invariant: primary keys (nodeKey/edgeKey) hold
// the current head body; historical bodies live only at mvccVersionKey for
// the version they were superseded at. This function stores only heads on
// the create path, and archives op.OldNode / op.OldEdge into a version
// record on update/delete (gated on retentionRetainsHistory). No-op arcs
// vanish when the engine runs head-only retention — the common case.
func (b *BadgerEngine) materializeMVCCCommitInTxn(txn *badger.Txn, version MVCCVersion, operations []Operation) error {
	// Archive superseded bodies when retention policy demands history OR
	// when an active snapshot reader needs to resolve the old version —
	// snapshot isolation must hold regardless of retention config.
	retainsHistory := b.mustArchiveForHistory()
	for _, op := range operations {
		switch op.Type {
		case OpCreateNode:
			if op.Node == nil {
				continue
			}
			// FreshID is set only when the ID was asserted new at
			// CreateNode time (UUID-shape). For explicit user-supplied
			// IDs we fall back to the load-existing-floor path to keep
			// snapshot reads correct across tombstone → recreate cycles.
			if op.FreshID {
				if err := b.writeNodeMVCCHeadForFreshCreateInTxn(txn, op.Node.ID, version); err != nil {
					return err
				}
			} else {
				if err := b.writeNodeMVCCHeadInTxn(txn, op.Node.ID, version, false); err != nil {
					return err
				}
			}
		case OpUpdateNode:
			if op.Node == nil {
				continue
			}
			if retainsHistory && op.OldNode != nil {
				// Read via a separate read txn so the lookup stays out
				// of the user txn's SSI read set — see
				// writeNodeMVCCHeadInTxn doc.
				if head, headErr := b.loadNodeMVCCHead(op.Node.ID); headErr == nil && !head.Tombstoned {
					if err := b.archiveNodeBodyInTxn(txn, op.Node.ID, op.OldNode, head.Version); err != nil {
						return err
					}
				} else if headErr != nil && headErr != ErrNotFound {
					return headErr
				}
			}
			if err := b.writeNodeMVCCHeadInTxn(txn, op.Node.ID, version, false); err != nil {
				return err
			}
		case OpDeleteNode:
			if retainsHistory && op.OldNode != nil {
				if head, headErr := b.loadNodeMVCCHead(op.NodeID); headErr == nil && !head.Tombstoned {
					if err := b.archiveNodeBodyInTxn(txn, op.NodeID, op.OldNode, head.Version); err != nil {
						return err
					}
				} else if headErr != nil && headErr != ErrNotFound {
					return headErr
				}
			}
			// Tombstone marker (tiny, no body) preserves delete semantics.
			if err := b.writeNodeMVCCTombstoneInTxn(txn, op.NodeID, version); err != nil {
				return err
			}
			if err := b.writeNodeMVCCHeadInTxn(txn, op.NodeID, version, true); err != nil {
				return err
			}
			for _, edgeID := range op.DeletedEdgeIDs {
				edge, err := b.loadEdgeForAdjacencyTombstoneInTxn(txn, edgeID)
				if err == nil {
					if err := b.writeEdgeAdjacencyDeltaInTxn(txn, edge, nil, version); err != nil {
						return err
					}
				} else if err != ErrNotFound {
					return err
				}
				if err := b.writeEdgeMVCCTombstoneInTxn(txn, edgeID, version); err != nil {
					return err
				}
				if err := b.writeEdgeMVCCHeadInTxn(txn, edgeID, version, true); err != nil {
					return err
				}
			}
		case OpCreateEdge:
			if op.Edge == nil {
				continue
			}
			if err := b.writeEdgeAdjacencyDeltaInTxn(txn, nil, op.Edge, version); err != nil {
				return err
			}
			if op.FreshID {
				if err := b.writeEdgeMVCCHeadForFreshCreateInTxn(txn, op.Edge.ID, version); err != nil {
					return err
				}
			} else {
				if err := b.writeEdgeMVCCHeadInTxn(txn, op.Edge.ID, version, false); err != nil {
					return err
				}
			}
		case OpUpdateEdge:
			if op.Edge == nil {
				continue
			}
			if op.OldEdge != nil && (op.OldEdge.StartNode != op.Edge.StartNode || op.OldEdge.EndNode != op.Edge.EndNode) {
				if err := b.writeEdgeAdjacencyDeltaInTxn(txn, op.OldEdge, op.Edge, version); err != nil {
					return err
				}
			}
			if retainsHistory && op.OldEdge != nil {
				if head, headErr := b.loadEdgeMVCCHead(op.Edge.ID); headErr == nil && !head.Tombstoned {
					if err := b.archiveEdgeBodyInTxn(txn, op.Edge.ID, op.OldEdge, head.Version); err != nil {
						return err
					}
				} else if headErr != nil && headErr != ErrNotFound {
					return headErr
				}
			}
			if err := b.writeEdgeMVCCHeadInTxn(txn, op.Edge.ID, version, false); err != nil {
				return err
			}
		case OpDeleteEdge:
			if op.OldEdge != nil {
				if err := b.writeEdgeAdjacencyTombstoneInTxn(txn, op.OldEdge, version); err != nil {
					return err
				}
			}
			if retainsHistory && op.OldEdge != nil {
				if head, headErr := b.loadEdgeMVCCHead(op.EdgeID); headErr == nil && !head.Tombstoned {
					if err := b.archiveEdgeBodyInTxn(txn, op.EdgeID, op.OldEdge, head.Version); err != nil {
						return err
					}
				} else if headErr != nil && headErr != ErrNotFound {
					return headErr
				}
			}
			// Tombstone marker preserves delete semantics.
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
			nodeNum, version, err := extractNodeNumIDAndMVCCVersionFromVersionKey(key)
			if err != nil {
				return err
			}
			nodeID, ok := b.idDict.lookupNodeIDByNum(nodeNum)
			if !ok {
				lastScanned = key
				count++
				if count >= limit {
					reachedEnd = false
					break
				}
				continue
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
			edgeNum, version, err := extractEdgeNumIDAndMVCCVersionFromVersionKey(key)
			if err != nil {
				return err
			}
			edgeID, ok := b.idDict.lookupEdgeIDByNum(edgeNum)
			if !ok {
				lastScanned = key
				count++
				if count >= limit {
					reachedEnd = false
					break
				}
				continue
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
				node, decodeErr = b.decodeNodeWithEmbeddings(txn, val, id)
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
			version, err := b.allocateMVCCVersion(txn, namespaceForNodeID(node.ID), mvccBootstrapTime(node.CreatedAt, node.UpdatedAt))
			if err != nil {
				return err
			}
			// Post-refactor: primary key IS the head body. Bootstrap
			// only needs to seed the head pointing at a fresh version.
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
				edge, decodeErr = b.decodeEdgeBodyByID(val, id)
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
			version, err := b.allocateMVCCVersion(txn, namespaceForEdgeID(edge.ID), mvccBootstrapTime(edge.CreatedAt, edge.UpdatedAt))
			if err != nil {
				return err
			}
			// Post-refactor: primary key IS the head body. Bootstrap
			// only seeds the head pointing at a fresh version.
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
			node, decodeErr = b.decodeNodeWithEmbeddings(txn, val, id)
			return decodeErr
		}); err != nil {
			return err
		}
		version, err := b.allocateMVCCVersion(txn, namespaceForNodeID(node.ID), mvccBootstrapTime(node.CreatedAt, node.UpdatedAt))
		if err != nil {
			return err
		}
		// Primary key already holds the body — just seed the head.
		if err := b.writeNodeMVCCHeadInTxn(txn, id, version, false); err != nil {
			return err
		}
		_ = node // keep decoded copy for any future hooks
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
			edge, decodeErr = b.decodeEdgeBodyByID(val, id)
			return decodeErr
		}); err != nil {
			return err
		}
		version, err := b.allocateMVCCVersion(txn, namespaceForEdgeID(edge.ID), mvccBootstrapTime(edge.CreatedAt, edge.UpdatedAt))
		if err != nil {
			return err
		}
		// Primary key already holds the body — just seed the head.
		if err := b.writeEdgeMVCCHeadInTxn(txn, id, version, false); err != nil {
			return err
		}
		_ = edge
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
		outgoingDeleted, err := b.pruneMVCCKeyspaceInTxn(ctx, txn, []byte{prefixMVCCOutgoingAdj}, effectiveOpts, activeSnapshotReaders)
		if err != nil {
			return err
		}
		incomingDeleted, err := b.pruneMVCCKeyspaceInTxn(ctx, txn, []byte{prefixMVCCIncomingAdj}, effectiveOpts, activeSnapshotReaders)
		if err != nil {
			return err
		}
		deleted = nodeDeleted + edgeDeleted + outgoingDeleted + incomingDeleted
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
		hasExternalHead := len(currentGroup[0].logical) == 1+8
		var head MVCCHead
		if hasExternalHead {
			var err error
			head, err = b.loadMVCCHeadForLogicalKeyInTxn(txn, currentGroup[0].logical)
			if err != nil {
				if err != ErrNotFound {
					return err
				}
				latest := currentGroup[len(currentGroup)-1]
				head = MVCCHead{Version: latest.version, Tombstoned: latest.tombstoned, FloorVersion: latest.version}
			}
		} else {
			latest := currentGroup[len(currentGroup)-1]
			head = MVCCHead{Version: latest.version, Tombstoned: latest.tombstoned, FloorVersion: latest.version}
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
			// Also drop the tombstone marker + head + dict entry so the
			// numID is fully reclaimable. This runs only when retention
			// is not being requested (opts.MaxVersionsPerKey == 0 or the
			// group is already at one tombstone marker).
			if opts.MaxVersionsPerKey <= 0 {
				// Delete the tombstone marker (head.Version entry).
				for _, entry := range currentGroup {
					if entry.version.Compare(head.Version) == 0 {
						if err := txn.Delete(entry.full); err != nil {
							return err
						}
						deleted++
						break
					}
				}
				// Delete the MVCC head itself, drop the dict entry, and
				// push the numID onto the debounced freelist so future
				// allocations can recycle it after the TTL window.
				if hasExternalHead {
					logical := currentGroup[0].logical
					switch logical[0] {
					case prefixMVCCNode:
						numID := binary.BigEndian.Uint64(logical[1:])
						if err := txn.Delete(mvccNodeHeadKey(numID)); err != nil {
							return err
						}
						if err := b.idDict.deleteNodeEntryByNumInTxn(txn, numID); err != nil {
							return err
						}
						if err := b.idDict.pushFreeNodeInTxn(txn, numID); err != nil {
							return err
						}
					case prefixMVCCEdge:
						numID := binary.BigEndian.Uint64(logical[1:])
						if err := txn.Delete(mvccEdgeHeadKey(numID)); err != nil {
							return err
						}
						if err := b.idDict.deleteEdgeEntryByNumInTxn(txn, numID); err != nil {
							return err
						}
						if err := b.idDict.pushFreeEdgeInTxn(txn, numID); err != nil {
							return err
						}
					}
				}
				currentGroup = currentGroup[:0]
				return nil
			}
			if hasExternalHead {
				if err := b.writeMVCCHeadForLogicalKeyInTxn(txn, currentGroup[0].logical, head.Version, true, head.Version); err != nil {
					return err
				}
			}
			currentGroup = currentGroup[:0]
			return nil
		}

		// headInGroup is true when the current head version has a
		// matching record in the version keyspace. Post-refactor this
		// is no longer guaranteed — the live body lives at the primary
		// key and the version keyspace holds only archived predecessors
		// (plus tombstone markers on delete).
		headInGroup := false
		for i := range currentGroup {
			if currentGroup[i].version.Compare(head.Version) == 0 {
				headInGroup = true
				break
			}
		}

		if len(currentGroup) <= 1 && headInGroup {
			if hasExternalHead {
				if err := b.writeMVCCHeadForLogicalKeyInTxn(txn, currentGroup[0].logical, head.Version, head.Tombstoned, currentGroup[0].version); err != nil {
					return err
				}
			}
			currentGroup = currentGroup[:0]
			return nil
		}

		keepHistorical := opts.MaxVersionsPerKey
		// maxDeleteIndex bounds the range of entries eligible for
		// deletion. When the head IS in the group, the last entry is
		// the head (never delete it here). When the head is NOT in the
		// group (post-refactor head-only or head-only-plus-archives),
		// every entry is historical and eligible.
		maxDeleteIndex := len(currentGroup)
		if headInGroup {
			maxDeleteIndex = len(currentGroup) - 1
		}
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
		if hasExternalHead {
			if err := b.writeMVCCHeadForLogicalKeyInTxn(txn, currentGroup[0].logical, head.Version, head.Tombstoned, floorVersion); err != nil {
				return err
			}
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
			case prefixMVCCOutgoingAdj, prefixMVCCIncomingAdj:
				record, decodeErr := decodeMVCCAdjacencyRecord(val)
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
	if len(logical) != 1+8 {
		return MVCCHead{}, ErrInvalidData
	}
	numID := binary.BigEndian.Uint64(logical[1:])
	switch logical[0] {
	case prefixMVCCNode:
		headKey := mvccNodeHeadKey(numID)
		item, err := txn.Get(headKey)
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
		return head, err
	case prefixMVCCEdge:
		headKey := mvccEdgeHeadKey(numID)
		item, err := txn.Get(headKey)
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
		return head, err
	default:
		return MVCCHead{}, fmt.Errorf("unknown mvcc logical key prefix: %x", logical[0])
	}
}

func (b *BadgerEngine) writeMVCCHeadForLogicalKeyInTxn(txn *badger.Txn, logical []byte, version MVCCVersion, tombstoned bool, floorVersion MVCCVersion) error {
	if len(logical) != 1+8 {
		return ErrInvalidData
	}
	numID := binary.BigEndian.Uint64(logical[1:])
	encoded, err := encodeMVCCHead(MVCCHead{Version: version, Tombstoned: tombstoned, FloorVersion: floorVersion})
	if err != nil {
		return err
	}
	switch logical[0] {
	case prefixMVCCNode:
		return txn.Set(mvccNodeHeadKey(numID), encoded)
	case prefixMVCCEdge:
		return txn.Set(mvccEdgeHeadKey(numID), encoded)
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
