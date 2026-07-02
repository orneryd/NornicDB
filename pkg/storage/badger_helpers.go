// Package storage provides storage engine implementations for NornicDB.
package storage

import (
	"encoding/binary"
	"fmt"
	"strings"
	"time"

	"github.com/dgraph-io/badger/v4"
)

// Key encoding helpers
// ============================================================================

// nodeKey creates a key for storing a node.
func nodeKey(id NodeID) []byte {
	return append([]byte{prefixNode}, []byte(id)...)
}

// edgeKey creates a key for storing an edge.
func edgeKey(id EdgeID) []byte {
	return append([]byte{prefixEdge}, []byte(id)...)
}

// mvccSequenceKey returns the legacy engine-global MVCC sequence key.
// New commits write per-namespace keys via mvccNamespaceSequenceKey; the
// legacy key is read once on open as a migration seed.
func mvccSequenceKey() []byte {
	return []byte{prefixMVCCMeta, 0x01}
}

// mvccNamespaceSequenceKey returns the on-disk key for one namespace's
// committed MVCC sequence. The namespace is appended as a length-bounded
// trailing segment so distinct namespaces hash to distinct keys.
func mvccNamespaceSequenceKey(namespace string) []byte {
	out := make([]byte, 0, 2+len(namespace))
	out = append(out, prefixMVCCMeta, prefixMVCCMetaNamespaceSeq)
	out = append(out, namespace...)
	return out
}

func mvccSchemaVersionKey() []byte {
	return []byte{prefixMVCCMeta, prefixMVCCMetaSchemaVersion}
}

func encodeMVCCSortVersion(version MVCCVersion) []byte {
	key := make([]byte, 16)
	timestamp := uint64(version.CommitTimestamp.UTC().UnixNano()) ^ (1 << 63)
	binary.BigEndian.PutUint64(key[:8], timestamp)
	binary.BigEndian.PutUint64(key[8:], version.CommitSequence)
	return key
}

func decodeMVCCSortVersion(encoded []byte) (MVCCVersion, error) {
	if len(encoded) != 16 {
		return MVCCVersion{}, fmt.Errorf("invalid mvcc version length: %d", len(encoded))
	}
	timestamp := int64(binary.BigEndian.Uint64(encoded[:8]) ^ (1 << 63))
	return MVCCVersion{
		CommitTimestamp: time.Unix(0, timestamp).UTC(),
		CommitSequence:  binary.BigEndian.Uint64(encoded[8:]),
	}, nil
}

// MVCC version / head keys now use 8-byte numeric IDs from the id
// dictionary. Writers allocate numIDs before addressing these keys;
// readers look up numIDs via the engine-level string wrappers.
func mvccNodeVersionPrefix(numID uint64) []byte {
	key := make([]byte, 0, 1+8)
	key = append(key, prefixMVCCNode)
	key = append(key, encodeNumID(numID)...)
	return key
}

func mvccNodeVersionKey(numID uint64, version MVCCVersion) []byte {
	key := mvccNodeVersionPrefix(numID)
	key = append(key, encodeMVCCSortVersion(version)...)
	return key
}

func mvccEdgeVersionPrefix(numID uint64) []byte {
	key := make([]byte, 0, 1+8)
	key = append(key, prefixMVCCEdge)
	key = append(key, encodeNumID(numID)...)
	return key
}

func mvccEdgeVersionKey(numID uint64, version MVCCVersion) []byte {
	key := mvccEdgeVersionPrefix(numID)
	key = append(key, encodeMVCCSortVersion(version)...)
	return key
}

func mvccNodeHeadKey(numID uint64) []byte {
	out := make([]byte, 1+8)
	out[0] = prefixMVCCNodeHead
	copy(out[1:], encodeNumID(numID))
	return out
}

func mvccEdgeHeadKey(numID uint64) []byte {
	out := make([]byte, 1+8)
	out[0] = prefixMVCCEdgeHead
	copy(out[1:], encodeNumID(numID))
	return out
}

func maxMVCCVersion() MVCCVersion {
	return MVCCVersion{
		CommitTimestamp: time.Unix(0, int64(^uint64(0)>>1)).UTC(),
		CommitSequence:  ^uint64(0),
	}
}

func extractMVCCVersionFromKey(key []byte) (MVCCVersion, error) {
	if len(key) < 17 {
		return MVCCVersion{}, fmt.Errorf("invalid mvcc key length: %d", len(key))
	}
	return decodeMVCCSortVersion(key[len(key)-16:])
}

// MVCC version keys are now fixed-width: [prefix(1)][numID(8)][version(16)].
// The old variable-length form used string IDs with a 0x00 separator.
func extractNodeNumIDAndMVCCVersionFromVersionKey(key []byte) (uint64, MVCCVersion, error) {
	if len(key) != 1+8+16 || key[0] != prefixMVCCNode {
		return 0, MVCCVersion{}, fmt.Errorf("invalid mvcc node version key: len=%d", len(key))
	}
	numID := binary.BigEndian.Uint64(key[1:9])
	version, err := decodeMVCCSortVersion(key[9:])
	if err != nil {
		return 0, MVCCVersion{}, err
	}
	return numID, version, nil
}

func extractEdgeNumIDAndMVCCVersionFromVersionKey(key []byte) (uint64, MVCCVersion, error) {
	if len(key) != 1+8+16 || key[0] != prefixMVCCEdge {
		return 0, MVCCVersion{}, fmt.Errorf("invalid mvcc edge version key: len=%d", len(key))
	}
	numID := binary.BigEndian.Uint64(key[1:9])
	version, err := decodeMVCCSortVersion(key[9:])
	if err != nil {
		return 0, MVCCVersion{}, err
	}
	return numID, version, nil
}

func extractMVCCLogicalKeyAndVersion(key []byte) ([]byte, MVCCVersion, error) {
	// Layout (post-refactor): [prefix(1)][numID(8)][version(16)].
	if len(key) != 1+8+16 {
		return nil, MVCCVersion{}, fmt.Errorf("invalid mvcc key length: %d", len(key))
	}
	version, err := decodeMVCCSortVersion(key[9:])
	if err != nil {
		return nil, MVCCVersion{}, err
	}
	logical := append([]byte(nil), key[:9]...)
	return logical, version, nil
}

// labelIndexKey creates a key for the label index.
// Format: prefix + label (lowercase) + 0x00 + nodeNumID (8B big-endian)
// Labels are normalized to lowercase for case-insensitive matching (Neo4j compatible)
func labelIndexKey(label string, nodeNumID uint64) []byte {
	normalizedLabel := strings.ToLower(label)
	key := make([]byte, 0, 1+len(normalizedLabel)+1+8)
	key = append(key, prefixLabelIndex)
	key = append(key, []byte(normalizedLabel)...)
	key = append(key, 0x00) // Separator
	key = append(key, encodeNumID(nodeNumID)...)
	return key
}

// labelIndexPrefix returns the prefix for scanning all nodes with a label.
// Labels are normalized to lowercase for case-insensitive matching (Neo4j compatible)
func labelIndexPrefix(label string) []byte {
	normalizedLabel := strings.ToLower(label)
	key := make([]byte, 0, 1+len(normalizedLabel)+1)
	key = append(key, prefixLabelIndex)
	key = append(key, []byte(normalizedLabel)...)
	key = append(key, 0x00)
	return key
}

// outgoingIndexKey creates a key for the outgoing edge index.
// Layout: [prefix][nodeNumID 8B][edgeNumID 8B]. Uses numeric IDs from the
// id dictionary so keys stay tight regardless of UUID length.
func outgoingIndexKey(nodeNumID, edgeNumID uint64) []byte {
	key := make([]byte, 0, 1+8+8)
	key = append(key, prefixOutgoingIndex)
	key = append(key, encodeNumID(nodeNumID)...)
	key = append(key, encodeNumID(edgeNumID)...)
	return key
}

// outgoingIndexPrefix returns the prefix for scanning outgoing edges.
func outgoingIndexPrefix(nodeNumID uint64) []byte {
	key := make([]byte, 0, 1+8)
	key = append(key, prefixOutgoingIndex)
	key = append(key, encodeNumID(nodeNumID)...)
	return key
}

// incomingIndexKey creates a key for the incoming edge index.
func incomingIndexKey(nodeNumID, edgeNumID uint64) []byte {
	key := make([]byte, 0, 1+8+8)
	key = append(key, prefixIncomingIndex)
	key = append(key, encodeNumID(nodeNumID)...)
	key = append(key, encodeNumID(edgeNumID)...)
	return key
}

// incomingIndexPrefix returns the prefix for scanning incoming edges.
func incomingIndexPrefix(nodeNumID uint64) []byte {
	key := make([]byte, 0, 1+8)
	key = append(key, prefixIncomingIndex)
	key = append(key, encodeNumID(nodeNumID)...)
	return key
}

func mvccOutgoingAdjacencyPrefix(nodeNumID uint64) []byte {
	key := make([]byte, 0, 1+8)
	key = append(key, prefixMVCCOutgoingAdj)
	key = append(key, encodeNumID(nodeNumID)...)
	return key
}

func mvccOutgoingAdjacencyEdgePrefix(nodeNumID, edgeNumID uint64) []byte {
	key := make([]byte, 0, 1+8+8)
	key = append(key, prefixMVCCOutgoingAdj)
	key = append(key, encodeNumID(nodeNumID)...)
	key = append(key, encodeNumID(edgeNumID)...)
	return key
}

func mvccOutgoingAdjacencyKey(nodeNumID, edgeNumID uint64, version MVCCVersion) []byte {
	key := mvccOutgoingAdjacencyEdgePrefix(nodeNumID, edgeNumID)
	key = append(key, encodeMVCCSortVersion(version)...)
	return key
}

func mvccIncomingAdjacencyPrefix(nodeNumID uint64) []byte {
	key := make([]byte, 0, 1+8)
	key = append(key, prefixMVCCIncomingAdj)
	key = append(key, encodeNumID(nodeNumID)...)
	return key
}

func mvccIncomingAdjacencyEdgePrefix(nodeNumID, edgeNumID uint64) []byte {
	key := make([]byte, 0, 1+8+8)
	key = append(key, prefixMVCCIncomingAdj)
	key = append(key, encodeNumID(nodeNumID)...)
	key = append(key, encodeNumID(edgeNumID)...)
	return key
}

func mvccIncomingAdjacencyKey(nodeNumID, edgeNumID uint64, version MVCCVersion) []byte {
	key := mvccIncomingAdjacencyEdgePrefix(nodeNumID, edgeNumID)
	key = append(key, encodeMVCCSortVersion(version)...)
	return key
}

// edgeTypeIndexKey creates a key for the edge type index.
// Layout: [prefix][type-lowercased]\x00[edgeNumID 8B]. Type stays as
// string because it has good shared-prefix compression in the LSM.
func edgeTypeIndexKey(edgeType string, edgeNumID uint64) []byte {
	normalizedType := strings.ToLower(edgeType)
	key := make([]byte, 0, 1+len(normalizedType)+1+8)
	key = append(key, prefixEdgeTypeIndex)
	key = append(key, []byte(normalizedType)...)
	key = append(key, 0x00) // Separator
	key = append(key, encodeNumID(edgeNumID)...)
	return key
}

// edgeTypeIndexPrefix returns the prefix for scanning all edges of a type.
// Edge types are normalized to lowercase for case-insensitive matching (Neo4j compatible)
func edgeTypeIndexPrefix(edgeType string) []byte {
	normalizedType := strings.ToLower(edgeType)
	key := make([]byte, 0, 1+len(normalizedType)+1)
	key = append(key, prefixEdgeTypeIndex)
	key = append(key, []byte(normalizedType)...)
	key = append(key, 0x00)
	return key
}

// edgeBetweenIndexKey stores an exact relationship lookup entry.
//
// Keys are composed of 8-byte numeric IDs from the id dictionary instead
// of full string IDs. For typical 40-byte UUID strings this cuts each
// key from ~140 bytes to ~30 bytes — tens of MiB on a 100k-edge graph.
//
// Layout: [prefixEdgeBetweenIndex][startNodeNumID 8B][endNodeNumID 8B]
//
//	[type 0x00-terminated][edgeNumID 8B]
//
// The key order lets GetEdgesBetween scan by (start, end) and lets
// GetEdgeBetween narrow further by type without scanning every outgoing
// edge from the start node.
func edgeBetweenIndexKey(startNumID, endNumID uint64, edgeType string, edgeNumID uint64) []byte {
	normalizedType := strings.ToLower(edgeType)
	key := make([]byte, 0, 1+8+8+len(normalizedType)+1+8)
	key = append(key, prefixEdgeBetweenIndex)
	key = append(key, encodeNumID(startNumID)...)
	key = append(key, encodeNumID(endNumID)...)
	key = append(key, []byte(normalizedType)...)
	key = append(key, 0x00)
	key = append(key, encodeNumID(edgeNumID)...)
	return key
}

// edgeBetweenHeadKey stores the fastest typed lookup for one edge between nodes.
//
// The set index remains the source for GetEdgesBetween and for multiple edges
// of the same type. The head is a compatibility-friendly accelerator: if it is
// absent or stale, reads can fall back to the set or legacy outgoing scan and
// repopulate it. Uses numeric IDs for the same reason edgeBetweenIndexKey does.
func edgeBetweenHeadKey(startNumID, endNumID uint64, edgeType string) []byte {
	normalizedType := strings.ToLower(edgeType)
	key := make([]byte, 0, 1+8+8+len(normalizedType))
	key = append(key, prefixEdgeBetweenHead)
	key = append(key, encodeNumID(startNumID)...)
	key = append(key, encodeNumID(endNumID)...)
	key = append(key, []byte(normalizedType)...)
	return key
}

// writeEdgeBetweenIndexesInTxn maintains both relationship lookup indexes.
// Allocates numeric IDs for the edge and its endpoints (if not already
// allocated) via the engine's id dictionary.
//
// The set-index entry stores the string edge ID in the VALUE. This lets
// scanners recover the string ID without a reverse-dict lookup while the
// KEY stays compact (8+8+type+8 bytes).
func (b *BadgerEngine) writeEdgeBetweenIndexesInTxn(txn *badger.Txn, edge *Edge) error {
	startNum, err := b.idDict.resolveOrAllocateNodeNumIDInTxn(txn, edge.StartNode)
	if err != nil {
		return err
	}
	endNum, err := b.idDict.resolveOrAllocateNodeNumIDInTxn(txn, edge.EndNode)
	if err != nil {
		return err
	}
	edgeNum, err := b.idDict.resolveOrAllocateEdgeNumIDInTxn(txn, edge.ID)
	if err != nil {
		return err
	}
	if err := txn.Set(edgeBetweenIndexKey(startNum, endNum, edge.Type, edgeNum), []byte(edge.ID)); err != nil {
		return err
	}
	return txn.Set(edgeBetweenHeadKey(startNum, endNum, edge.Type), []byte(edge.ID))
}

// deleteEdgeBetweenIndexesInTxn removes an edge from the set index and clears
// the head only when it points at the same edge. Uses the existing dict
// entries for the endpoints (must have been allocated at write time).
func (b *BadgerEngine) deleteEdgeBetweenIndexesInTxn(txn *badger.Txn, edge *Edge) error {
	startNum, ok1 := b.idDict.lookupNodeNumID(edge.StartNode)
	endNum, ok2 := b.idDict.lookupNodeNumID(edge.EndNode)
	edgeNum, ok3 := b.idDict.lookupEdgeNumID(edge.ID)
	if !ok1 || !ok2 || !ok3 {
		// Missing numID means the edge was never indexed under the
		// numeric scheme (e.g. pre-migration data that didn't populate
		// the dict). Nothing to delete on the compact keyspace.
		return b.deleteEdgeBetweenHeadIfMatchesInTxn(txn, edge)
	}
	if err := txn.Delete(edgeBetweenIndexKey(startNum, endNum, edge.Type, edgeNum)); err != nil {
		return err
	}
	return b.deleteEdgeBetweenHeadIfMatchesInTxn(txn, edge)
}

// deleteEdgeBetweenHeadIfMatchesInTxn avoids removing a head already advanced
// to another same-type edge.
func (b *BadgerEngine) deleteEdgeBetweenHeadIfMatchesInTxn(txn *badger.Txn, edge *Edge) error {
	startNum, ok1 := b.idDict.lookupNodeNumID(edge.StartNode)
	endNum, ok2 := b.idDict.lookupNodeNumID(edge.EndNode)
	if !ok1 || !ok2 {
		return nil
	}
	key := edgeBetweenHeadKey(startNum, endNum, edge.Type)
	item, err := txn.Get(key)
	if err == badger.ErrKeyNotFound {
		return nil
	}
	if err != nil {
		return err
	}

	var matches bool
	if err := item.Value(func(val []byte) error {
		matches = EdgeID(val) == edge.ID
		return nil
	}); err != nil {
		return err
	}
	if !matches {
		return nil
	}
	return txn.Delete(key)
}

// outgoingIndexKeyString resolves the node/edge string IDs via the dict
// (allocating num IDs if missing) and returns the compact-keyed key.
// Callers on write paths pass a txn so the allocation persists.
func (b *BadgerEngine) outgoingIndexKeyString(txn *badger.Txn, nodeID NodeID, edgeID EdgeID) ([]byte, error) {
	nodeNum, err := b.idDict.resolveOrAllocateNodeNumIDInTxn(txn, nodeID)
	if err != nil {
		return nil, err
	}
	edgeNum, err := b.idDict.resolveOrAllocateEdgeNumIDInTxn(txn, edgeID)
	if err != nil {
		return nil, err
	}
	return outgoingIndexKey(nodeNum, edgeNum), nil
}

// incomingIndexKeyString is the incoming-side analogue.
func (b *BadgerEngine) incomingIndexKeyString(txn *badger.Txn, nodeID NodeID, edgeID EdgeID) ([]byte, error) {
	nodeNum, err := b.idDict.resolveOrAllocateNodeNumIDInTxn(txn, nodeID)
	if err != nil {
		return nil, err
	}
	edgeNum, err := b.idDict.resolveOrAllocateEdgeNumIDInTxn(txn, edgeID)
	if err != nil {
		return nil, err
	}
	return incomingIndexKey(nodeNum, edgeNum), nil
}

// edgeTypeIndexKeyString is the edge-type analogue.
func (b *BadgerEngine) edgeTypeIndexKeyString(txn *badger.Txn, edgeType string, edgeID EdgeID) ([]byte, error) {
	edgeNum, err := b.idDict.resolveOrAllocateEdgeNumIDInTxn(txn, edgeID)
	if err != nil {
		return nil, err
	}
	return edgeTypeIndexKey(edgeType, edgeNum), nil
}

// outgoingIndexKeyStringLookup is a non-allocating variant used by
// delete/scan paths where the num IDs are expected to already exist.
// Returns nil (not an error) when the dict lookup misses — caller treats
// that as "no index entry existed".
func (b *BadgerEngine) outgoingIndexKeyStringLookup(nodeID NodeID, edgeID EdgeID) []byte {
	nodeNum, ok1 := b.idDict.lookupNodeNumID(nodeID)
	edgeNum, ok2 := b.idDict.lookupEdgeNumID(edgeID)
	if !ok1 || !ok2 {
		return nil
	}
	return outgoingIndexKey(nodeNum, edgeNum)
}

func (b *BadgerEngine) incomingIndexKeyStringLookup(nodeID NodeID, edgeID EdgeID) []byte {
	nodeNum, ok1 := b.idDict.lookupNodeNumID(nodeID)
	edgeNum, ok2 := b.idDict.lookupEdgeNumID(edgeID)
	if !ok1 || !ok2 {
		return nil
	}
	return incomingIndexKey(nodeNum, edgeNum)
}

func (b *BadgerEngine) edgeTypeIndexKeyStringLookup(edgeType string, edgeID EdgeID) []byte {
	edgeNum, ok := b.idDict.lookupEdgeNumID(edgeID)
	if !ok {
		return nil
	}
	return edgeTypeIndexKey(edgeType, edgeNum)
}

// outgoingIndexPrefixString resolves the node string ID and returns the
// scan prefix. Returns nil if the node has no numID (no outgoing edges
// ever indexed — empty scan is correct).
func (b *BadgerEngine) outgoingIndexPrefixString(nodeID NodeID) []byte {
	nodeNum, ok := b.idDict.lookupNodeNumID(nodeID)
	if !ok {
		return nil
	}
	return outgoingIndexPrefix(nodeNum)
}

func (b *BadgerEngine) incomingIndexPrefixString(nodeID NodeID) []byte {
	nodeNum, ok := b.idDict.lookupNodeNumID(nodeID)
	if !ok {
		return nil
	}
	return incomingIndexPrefix(nodeNum)
}

// labelIndexKeyString resolves the node string ID via the dict,
// allocating a numID if missing, and returns the compact label key.
func (b *BadgerEngine) labelIndexKeyString(txn *badger.Txn, label string, nodeID NodeID) ([]byte, error) {
	nodeNum, err := b.idDict.resolveOrAllocateNodeNumIDInTxn(txn, nodeID)
	if err != nil {
		return nil, err
	}
	return labelIndexKey(label, nodeNum), nil
}

// labelIndexKeyStringLookup is a non-allocating form for delete/probe
// paths. Returns nil if the node has no numID yet.
func (b *BadgerEngine) labelIndexKeyStringLookup(label string, nodeID NodeID) []byte {
	nodeNum, ok := b.idDict.lookupNodeNumID(nodeID)
	if !ok {
		return nil
	}
	return labelIndexKey(label, nodeNum)
}

// mvccNodeHeadKeyString / mvccEdgeHeadKeyString wrap the numeric-keyed
// MVCC head key with dict-backed string ID resolution. Writers allocate
// the numID; readers use lookup-only.
func (b *BadgerEngine) mvccNodeHeadKeyString(txn *badger.Txn, id NodeID) ([]byte, error) {
	num, err := b.idDict.resolveOrAllocateNodeNumIDInTxn(txn, id)
	if err != nil {
		return nil, err
	}
	return mvccNodeHeadKey(num), nil
}

func (b *BadgerEngine) mvccEdgeHeadKeyString(txn *badger.Txn, id EdgeID) ([]byte, error) {
	num, err := b.idDict.resolveOrAllocateEdgeNumIDInTxn(txn, id)
	if err != nil {
		return nil, err
	}
	return mvccEdgeHeadKey(num), nil
}

func (b *BadgerEngine) mvccNodeHeadKeyStringLookup(id NodeID) []byte {
	num, ok := b.idDict.lookupNodeNumID(id)
	if !ok {
		return nil
	}
	return mvccNodeHeadKey(num)
}

func (b *BadgerEngine) mvccEdgeHeadKeyStringLookup(id EdgeID) []byte {
	num, ok := b.idDict.lookupEdgeNumID(id)
	if !ok {
		return nil
	}
	return mvccEdgeHeadKey(num)
}

// mvccNodeVersionKeyString / mvccEdgeVersionKeyString wrap the
// numeric-keyed MVCC version keys.
func (b *BadgerEngine) mvccNodeVersionKeyString(txn *badger.Txn, id NodeID, version MVCCVersion) ([]byte, error) {
	num, err := b.idDict.resolveOrAllocateNodeNumIDInTxn(txn, id)
	if err != nil {
		return nil, err
	}
	return mvccNodeVersionKey(num, version), nil
}

func (b *BadgerEngine) mvccEdgeVersionKeyString(txn *badger.Txn, id EdgeID, version MVCCVersion) ([]byte, error) {
	num, err := b.idDict.resolveOrAllocateEdgeNumIDInTxn(txn, id)
	if err != nil {
		return nil, err
	}
	return mvccEdgeVersionKey(num, version), nil
}

func (b *BadgerEngine) mvccNodeVersionKeyStringLookup(id NodeID, version MVCCVersion) []byte {
	num, ok := b.idDict.lookupNodeNumID(id)
	if !ok {
		return nil
	}
	return mvccNodeVersionKey(num, version)
}

func (b *BadgerEngine) mvccEdgeVersionKeyStringLookup(id EdgeID, version MVCCVersion) []byte {
	num, ok := b.idDict.lookupEdgeNumID(id)
	if !ok {
		return nil
	}
	return mvccEdgeVersionKey(num, version)
}

// mvccNodeVersionPrefixString / mvccEdgeVersionPrefixString return the
// scan prefix for an entity's version history. Nil when the entity has
// no numID (no history recorded).
func (b *BadgerEngine) mvccNodeVersionPrefixString(id NodeID) []byte {
	num, ok := b.idDict.lookupNodeNumID(id)
	if !ok {
		return nil
	}
	return mvccNodeVersionPrefix(num)
}

func (b *BadgerEngine) mvccEdgeVersionPrefixString(id EdgeID) []byte {
	num, ok := b.idDict.lookupEdgeNumID(id)
	if !ok {
		return nil
	}
	return mvccEdgeVersionPrefix(num)
}

func (b *BadgerEngine) mvccOutgoingAdjacencyKeyString(txn *badger.Txn, nodeID NodeID, edgeID EdgeID, version MVCCVersion) ([]byte, error) {
	nodeNum, err := b.idDict.resolveOrAllocateNodeNumIDInTxn(txn, nodeID)
	if err != nil {
		return nil, err
	}
	edgeNum, err := b.idDict.resolveOrAllocateEdgeNumIDInTxn(txn, edgeID)
	if err != nil {
		return nil, err
	}
	return mvccOutgoingAdjacencyKey(nodeNum, edgeNum, version), nil
}

func (b *BadgerEngine) mvccIncomingAdjacencyKeyString(txn *badger.Txn, nodeID NodeID, edgeID EdgeID, version MVCCVersion) ([]byte, error) {
	nodeNum, err := b.idDict.resolveOrAllocateNodeNumIDInTxn(txn, nodeID)
	if err != nil {
		return nil, err
	}
	edgeNum, err := b.idDict.resolveOrAllocateEdgeNumIDInTxn(txn, edgeID)
	if err != nil {
		return nil, err
	}
	return mvccIncomingAdjacencyKey(nodeNum, edgeNum, version), nil
}

func (b *BadgerEngine) mvccOutgoingAdjacencyPrefixString(nodeID NodeID) []byte {
	nodeNum, ok := b.idDict.lookupNodeNumID(nodeID)
	if !ok {
		return nil
	}
	return mvccOutgoingAdjacencyPrefix(nodeNum)
}

func (b *BadgerEngine) mvccIncomingAdjacencyPrefixString(nodeID NodeID) []byte {
	nodeNum, ok := b.idDict.lookupNodeNumID(nodeID)
	if !ok {
		return nil
	}
	return mvccIncomingAdjacencyPrefix(nodeNum)
}

// edgeBetweenIndexKeyFromStringIDs is a test-support convenience that
// resolves string IDs through the dictionary and returns the numeric-keyed
// compact index key. Returns nil if any string ID is unknown.
func (b *BadgerEngine) edgeBetweenIndexKeyFromStringIDs(startID, endID NodeID, edgeType string, edgeID EdgeID) []byte {
	startNum, sOK := b.idDict.lookupNodeNumID(startID)
	endNum, eOK := b.idDict.lookupNodeNumID(endID)
	edgeNum, edOK := b.idDict.lookupEdgeNumID(edgeID)
	if !sOK || !eOK || !edOK {
		return nil
	}
	return edgeBetweenIndexKey(startNum, endNum, edgeType, edgeNum)
}

// edgeBetweenHeadKeyFromStringIDs is the head analogue of
// edgeBetweenIndexKeyFromStringIDs.
func (b *BadgerEngine) edgeBetweenHeadKeyFromStringIDs(startID, endID NodeID, edgeType string) []byte {
	startNum, sOK := b.idDict.lookupNodeNumID(startID)
	endNum, eOK := b.idDict.lookupNodeNumID(endID)
	if !sOK || !eOK {
		return nil
	}
	return edgeBetweenHeadKey(startNum, endNum, edgeType)
}

// edgeBetweenIndexPrefix returns the prefix for all edges between two nodes,
// keyed on 8-byte numeric IDs from the id dictionary.
func edgeBetweenIndexPrefix(startNumID, endNumID uint64) []byte {
	key := make([]byte, 0, 1+8+8)
	key = append(key, prefixEdgeBetweenIndex)
	key = append(key, encodeNumID(startNumID)...)
	key = append(key, encodeNumID(endNumID)...)
	return key
}

// typedEdgeBetweenIndexPrefix returns the prefix for edges of one type between two nodes.
func typedEdgeBetweenIndexPrefix(startNumID, endNumID uint64, edgeType string) []byte {
	normalizedType := strings.ToLower(edgeType)
	key := edgeBetweenIndexPrefix(startNumID, endNumID)
	key = append(key, []byte(normalizedType)...)
	key = append(key, 0x00)
	return key
}

// pendingEmbedKey creates a key for the pending embeddings index.
// Format: prefix + nodeID
func pendingEmbedKey(nodeID NodeID) []byte {
	return append([]byte{prefixPendingEmbed}, []byte(nodeID)...)
}

const (
	// Keep embedding payloads below the 64KiB Badger entry limit used by this engine profile.
	maxEmbeddingValueBytes = 60 << 10
	embeddingShardMarker   = "EMBSHARDv1"
)

// embeddingKey creates a key for storing a chunk embedding separately.
// Format: prefix + nodeID + 0x00 + chunkIndex (as bytes)
func embeddingKey(nodeID NodeID, chunkIndex int) []byte {
	key := make([]byte, 0, 1+len(nodeID)+1+4)
	key = append(key, prefixEmbedding)
	key = append(key, []byte(nodeID)...)
	key = append(key, 0x00) // Separator
	// Encode chunk index as 4 bytes (big-endian)
	key = append(key, byte(chunkIndex>>24), byte(chunkIndex>>16), byte(chunkIndex>>8), byte(chunkIndex))
	return key
}

// embeddingChunkPartKey creates a key for a sharded embedding payload part.
// Format: embeddingKey(nodeID, chunkIndex) + 0x00 + partIndex (2 bytes big-endian)
func embeddingChunkPartKey(nodeID NodeID, chunkIndex, partIndex int) []byte {
	base := embeddingKey(nodeID, chunkIndex)
	key := make([]byte, 0, len(base)+1+2)
	key = append(key, base...)
	key = append(key, 0x00)
	key = append(key, byte(partIndex>>8), byte(partIndex))
	return key
}

func embeddingChunkPartPrefix(nodeID NodeID, chunkIndex int) []byte {
	base := embeddingKey(nodeID, chunkIndex)
	prefix := make([]byte, 0, len(base)+1)
	prefix = append(prefix, base...)
	prefix = append(prefix, 0x00)
	return prefix
}

func makeEmbeddingShardMarker(partCount int) []byte {
	marker := make([]byte, 0, len(embeddingShardMarker)+binary.MaxVarintLen64)
	marker = append(marker, embeddingShardMarker...)
	scratch := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(scratch, uint64(partCount))
	marker = append(marker, scratch[:n]...)
	return marker
}

func parseEmbeddingShardMarker(data []byte) (int, bool) {
	if len(data) < len(embeddingShardMarker) || string(data[:len(embeddingShardMarker)]) != embeddingShardMarker {
		return 0, false
	}
	count, n := binary.Uvarint(data[len(embeddingShardMarker):])
	if n <= 0 || count == 0 {
		return 0, false
	}
	return int(count), true
}

type embeddingWriteKV struct {
	key []byte
	val []byte
}

func buildEmbeddingChunkWriteKVs(nodeID NodeID, chunkIndex int, emb []float32) ([]embeddingWriteKV, error) {
	embData, err := encodeEmbedding(emb)
	if err != nil {
		return nil, fmt.Errorf("failed to encode embedding chunk %d: %w", chunkIndex, err)
	}
	baseKey := embeddingKey(nodeID, chunkIndex)
	if len(embData) <= maxEmbeddingValueBytes {
		return []embeddingWriteKV{{key: baseKey, val: embData}}, nil
	}
	parts := (len(embData) + maxEmbeddingValueBytes - 1) / maxEmbeddingValueBytes
	kvs := make([]embeddingWriteKV, 0, parts+1)
	kvs = append(kvs, embeddingWriteKV{key: baseKey, val: makeEmbeddingShardMarker(parts)})
	for part := 0; part < parts; part++ {
		start := part * maxEmbeddingValueBytes
		end := start + maxEmbeddingValueBytes
		if end > len(embData) {
			end = len(embData)
		}
		kvs = append(kvs, embeddingWriteKV{
			key: embeddingChunkPartKey(nodeID, chunkIndex, part),
			val: append([]byte(nil), embData[start:end]...),
		})
	}
	return kvs, nil
}

// schemaKey stores the persisted schema definition for a database namespace.
// Format: prefixSchema + namespace + 0x00
func schemaKey(namespace string) []byte {
	key := make([]byte, 0, 1+len(namespace)+1)
	key = append(key, prefixSchema)
	key = append(key, []byte(namespace)...)
	key = append(key, 0x00)
	return key
}

// embeddingPrefix returns the prefix for scanning all embeddings for a node.
// Format: prefix + nodeID + 0x00
func embeddingPrefix(nodeID NodeID) []byte {
	key := make([]byte, 0, 1+len(nodeID)+1)
	key = append(key, prefixEmbedding)
	key = append(key, []byte(nodeID)...)
	key = append(key, 0x00)
	return key
}

// extractEdgeNumIDFromOutgoingKey pulls the trailing 8-byte edge numID from
// an outgoing/incoming index key. Returns (0, false) if the key is malformed.
//
// Layout: [prefix(1)][nodeNumID(8)][edgeNumID(8)].
func extractEdgeNumIDFromOutgoingKey(key []byte) (uint64, bool) {
	if len(key) != 1+8+8 {
		return 0, false
	}
	return binary.BigEndian.Uint64(key[1+8:]), true
}

// extractEdgeNumIDFromEdgeTypeKey pulls the trailing 8-byte edge numID
// from an edge-type index key.
//
// Layout: [prefix(1)][type...][0x00][edgeNumID(8)].
func extractEdgeNumIDFromEdgeTypeKey(key []byte) (uint64, bool) {
	if len(key) < 1+1+8 {
		return 0, false
	}
	suffix := key[len(key)-8:]
	return binary.BigEndian.Uint64(suffix), true
}

// extractNodeNumIDFromLabelIndex extracts the 8-byte node numID from a
// label index key. Layout: [prefix(1)][label(labelLen)][0x00][numID(8)].
func extractNodeNumIDFromLabelIndex(key []byte, labelLen int) (uint64, bool) {
	offset := 1 + labelLen + 1
	if offset+8 != len(key) {
		return 0, false
	}
	return binary.BigEndian.Uint64(key[offset:]), true
}

func extractEdgeNumIDAndMVCCVersionFromAdjacencyKey(key []byte) (uint64, MVCCVersion, error) {
	if len(key) != 1+8+8+16 {
		return 0, MVCCVersion{}, fmt.Errorf("invalid mvcc adjacency key length: %d", len(key))
	}
	version, err := decodeMVCCSortVersion(key[17:])
	if err != nil {
		return 0, MVCCVersion{}, err
	}
	return binary.BigEndian.Uint64(key[9:17]), version, nil
}

// ============================================================================
// Serialization helpers
// ============================================================================

// encodeNodeV1 produces the legacy (non-tokenized) node body. Used
// only by the V1→V2 migration tool to read in-place V1 bodies; the
// engine's hot path emits V2 exclusively via (b *BadgerEngine).encodeNodeInTxn.
//
// If the node exceeds maxNodeSize, embeddings are stored separately and a flag is set.
// Returns: (nodeData, embeddingsStoredSeparately, error)
func encodeNodeV1(n *Node) ([]byte, bool, error) {
	if err := validatePropertiesForStorage(n.Properties); err != nil {
		return nil, false, fmt.Errorf("invalid node properties: %w", err)
	}
	return encodeNodeV1Body(n)
}

func encodeNodeV1Body(n *Node) ([]byte, bool, error) {
	// First, try encoding with embeddings
	data, err := encodeValue(n)
	if err != nil {
		return nil, false, err
	}

	// If size is acceptable, return as-is
	if len(data) <= maxNodeSize {
		return data, false, nil
	}

	// Node is too large - store embeddings separately
	nodeCopy := *n
	chunkCount := len(nodeCopy.ChunkEmbeddings)
	nodeCopy.ChunkEmbeddings = nil

	nodeCopy.EmbeddingsStoredSeparately = true
	if nodeCopy.EmbedMeta == nil {
		nodeCopy.EmbedMeta = make(map[string]any)
	}
	if _, hasChunkCount := nodeCopy.EmbedMeta["chunk_count"]; !hasChunkCount {
		nodeCopy.EmbedMeta["chunk_count"] = chunkCount
	}

	data, err = encodeValue(&nodeCopy)
	if err != nil {
		return nil, false, err
	}

	return data, true, nil
}

// encodeNodeInTxn produces a V2 tokenized node body. Properties are
// extracted, tokenized via the per-namespace property-key dictionary,
// and prepended ahead of the standard Node msgpack body (with
// Properties cleared so the inline copy does not duplicate them).
//
// Layout:
//
//	[nodeFormatTokenizedV1 = 0x10]
//	[varint(propsLen)]
//	[tokenizedPropsBytes]   -- msgpack(map[uint64]any)
//	[standard Node msgpack body, Properties=nil]
//
// Returns: (nodeData, embeddingsStoredSeparately, error). The
// embeddings-separate logic mirrors V1 — if the encoded body exceeds
// maxNodeSize, ChunkEmbeddings are dropped from the inline body and
// persisted under their own keys by the caller.
func (b *BadgerEngine) encodeNodeInTxn(txn *badger.Txn, namespace string, n *Node) ([]byte, bool, error) {
	if err := validatePropertiesForStorage(n.Properties); err != nil {
		return nil, false, fmt.Errorf("invalid node properties: %w", err)
	}

	propsBytes, err := b.encodeTokenizedProperties(txn, namespace, n.Properties)
	if err != nil {
		return nil, false, err
	}

	bodyCopy := *n
	bodyCopy.Properties = nil

	assemble := func(bodyBytes []byte) []byte {
		out := make([]byte, 0, 1+binary.MaxVarintLen64+len(propsBytes)+len(bodyBytes))
		out = append(out, nodeFormatTokenizedV1)
		out = binary.AppendUvarint(out, uint64(len(propsBytes)))
		out = append(out, propsBytes...)
		out = append(out, bodyBytes...)
		return out
	}

	bodyBytes, err := encodeValue(&bodyCopy)
	if err != nil {
		return nil, false, err
	}
	data := assemble(bodyBytes)

	if len(data) <= maxNodeSize {
		return data, false, nil
	}

	chunkCount := len(bodyCopy.ChunkEmbeddings)
	bodyCopy.ChunkEmbeddings = nil
	bodyCopy.EmbeddingsStoredSeparately = true
	if bodyCopy.EmbedMeta == nil {
		bodyCopy.EmbedMeta = make(map[string]any)
	}
	if _, hasChunkCount := bodyCopy.EmbedMeta["chunk_count"]; !hasChunkCount {
		bodyCopy.EmbedMeta["chunk_count"] = chunkCount
	}

	bodyBytes, err = encodeValue(&bodyCopy)
	if err != nil {
		return nil, false, err
	}
	return assemble(bodyBytes), true, nil
}

// encodeEmbedding serializes a single embedding chunk.
func encodeEmbedding(emb []float32) ([]byte, error) {
	return encodeValue(emb)
}

// decodeEmbedding deserializes a single embedding chunk.
func decodeEmbedding(data []byte) ([]float32, error) {
	var emb []float32
	if err := decodeValue(data, &emb); err != nil {
		return nil, err
	}
	return emb, nil
}

// decodeNodeV1 deserializes a legacy V1 node body. Used only by V1→V2
// migration code; the engine's hot path goes through (b *BadgerEngine).decodeNode.
func decodeNodeV1(data []byte) (*Node, error) {
	var node Node
	if err := decodeValue(data, &node); err != nil {
		return nil, err
	}
	return &node, nil
}

// namespaceForNodeID extracts the per-namespace bucket from a node ID
// using the standard "<db>:<id>" prefix convention. Used by decode
// call sites that already have a NodeID in hand.
func namespaceForNodeID(id NodeID) string {
	ns, _, _ := ParseDatabasePrefix(string(id))
	return ns
}

// namespaceForEdgeID extracts the per-namespace bucket from an edge ID
// using the standard "<db>:<id>" prefix convention.
func namespaceForEdgeID(id EdgeID) string {
	ns, _, _ := ParseDatabasePrefix(string(id))
	return ns
}

// encodeNode is the engine-attached entry point matching the V1 free
// function's signature. Resolves the namespace from the node's ID and
// emits a V2 tokenized body. Call sites that have a *badger.Txn in
// scope should use it.
func (b *BadgerEngine) encodeNode(txn *badger.Txn, node *Node) ([]byte, bool, error) {
	namespace := namespaceForNodeID(node.ID)
	if namespace == "" {
		return nil, false, fmt.Errorf("encodeNode: node ID %q lacks namespace prefix", node.ID)
	}
	return b.encodeNodeInTxn(txn, namespace, node)
}

// decodeEdgeBodyByID is the engine-attached convenience wrapper that
// derives the namespace from the edge ID. Replaces the old
// decodeEdgeBodyWithID(data, id) signature at call sites.
func (b *BadgerEngine) decodeEdgeBodyByID(data []byte, id EdgeID) (*Edge, error) {
	return b.decodeEdgeBodyWithID(namespaceForEdgeID(id), data, id)
}

// decodeNode is the V2 hot-path decoder. Rejects any body that does
// not start with the V2 format byte — V2 stores have been fully
// rewritten by the V1→V2 migration, so a non-V2 body indicates
// corruption or a forgotten upgrade.
func (b *BadgerEngine) decodeNode(namespace string, data []byte) (*Node, error) {
	return b.decodeNodeProjected(namespace, data, nil)
}

func (b *BadgerEngine) decodeNodeProjected(namespace string, data []byte, include map[string]struct{}) (*Node, error) {
	if len(data) < 1 {
		return nil, fmt.Errorf("node body empty")
	}
	if data[0] != nodeFormatTokenizedV1 {
		return nil, fmt.Errorf("node body has unexpected format byte 0x%02x; expected V2 (0x%02x)", data[0], nodeFormatTokenizedV1)
	}
	rest := data[1:]
	propsLen, n := binary.Uvarint(rest)
	if n <= 0 {
		return nil, fmt.Errorf("node body: malformed properties length varint")
	}
	rest = rest[n:]
	if uint64(len(rest)) < propsLen {
		return nil, fmt.Errorf("node body: properties payload truncated")
	}
	propsBytes := rest[:propsLen]
	bodyBytes := rest[propsLen:]

	var node Node
	if err := decodeValue(bodyBytes, &node); err != nil {
		return nil, err
	}
	props, err := b.decodeTokenizedPropertiesProjected(namespace, propsBytes, include)
	if err != nil {
		return nil, err
	}
	node.Properties = props
	return &node, nil
}

// decodeNodeWithEmbeddings deserializes a Node and loads separately stored embeddings from transaction.
// Works in both View and Update transactions (only reads embeddings).
func (b *BadgerEngine) decodeNodeWithEmbeddings(txn *badger.Txn, data []byte, nodeID NodeID) (*Node, error) {
	namespace, _, _ := ParseDatabasePrefix(string(nodeID))
	node, err := b.decodeNode(namespace, data)
	if err != nil {
		return nil, err
	}

	// Check if embeddings are stored separately (struct flag set during encode)
	if node.EmbeddingsStoredSeparately {
		// Use chunk_count from EmbedMeta (set by embed queue) to know how many chunks to load
		// Handle various integer types (msgpack may decode as different types depending on value size)
		var chunkCount int
		switch v := node.EmbedMeta["chunk_count"].(type) {
		case int:
			chunkCount = v
		case int8:
			chunkCount = int(v)
		case int16:
			chunkCount = int(v)
		case int32:
			chunkCount = int(v)
		case int64:
			chunkCount = int(v)
		case uint:
			chunkCount = int(v)
		case uint8:
			chunkCount = int(v)
		case uint16:
			chunkCount = int(v)
		case uint32:
			chunkCount = int(v)
		case uint64:
			chunkCount = int(v)
		case float64:
			// JSON numbers might be decoded as float64
			chunkCount = int(v)
		default:
			// Try to convert via fmt.Sprintf and strconv if needed
			if v != nil {
				// Last resort: try string conversion
				if str, ok := v.(string); ok {
					if parsed, err := fmt.Sscanf(str, "%d", &chunkCount); err == nil && parsed == 1 {
						// Successfully parsed
					}
				}
			}
		}
		if chunkCount > 0 {
			node.ChunkEmbeddings = make([][]float32, 0, chunkCount)

			// Load each chunk embedding
			for i := 0; i < chunkCount; i++ {
				embKey := embeddingKey(nodeID, i)
				item, err := txn.Get(embKey)
				if err != nil {
					if err == badger.ErrKeyNotFound {
						// Missing chunk - continue with what we have
						continue
					}
					return nil, fmt.Errorf("failed to get embedding chunk %d: %w", i, err)
				}

				var embData []byte
				if err := item.Value(func(val []byte) error {
					embData = append([]byte(nil), val...)
					return nil
				}); err != nil {
					return nil, fmt.Errorf("failed to read embedding chunk %d: %w", i, err)
				}

				emb, err := decodeEmbedding(embData)
				if err != nil {
					partCount, sharded := parseEmbeddingShardMarker(embData)
					if !sharded {
						return nil, fmt.Errorf("failed to decode embedding chunk %d: %w", i, err)
					}
					assembled := make([]byte, 0, partCount*maxEmbeddingValueBytes)
					for part := 0; part < partCount; part++ {
						partItem, getErr := txn.Get(embeddingChunkPartKey(nodeID, i, part))
						if getErr != nil {
							return nil, fmt.Errorf("failed to get embedding chunk %d part %d: %w", i, part, getErr)
						}
						if err := partItem.Value(func(val []byte) error {
							assembled = append(assembled, val...)
							return nil
						}); err != nil {
							return nil, fmt.Errorf("failed to read embedding chunk %d part %d: %w", i, part, err)
						}
					}
					emb, err = decodeEmbedding(assembled)
					if err != nil {
						return nil, fmt.Errorf("failed to decode sharded embedding chunk %d: %w", i, err)
					}
				}

				node.ChunkEmbeddings = append(node.ChunkEmbeddings, emb)
			}

			// Clear internal storage flag after loading (chunk_count is user-facing, keep it)
			node.EmbeddingsStoredSeparately = false
		}
	}

	return node, nil
}

// encodeEdge serializes an Edge using the active storage serializer.
func encodeEdge(e *Edge) ([]byte, error) {
	if err := validatePropertiesForStorage(e.Properties); err != nil {
		return nil, fmt.Errorf("invalid edge properties: %w", err)
	}
	return encodeValue(e)
}

// decodeEdge deserializes an Edge from a legacy non-compact body.
// Does NOT handle the compact-V1 format. On the BadgerEngine read path,
// prefer engine.decodeEdgeBody which dispatches based on the format byte.
func decodeEdge(data []byte) (*Edge, error) {
	var edge Edge
	if err := decodeValue(data, &edge); err != nil {
		return nil, err
	}
	return &edge, nil
}

// ============================================================================
