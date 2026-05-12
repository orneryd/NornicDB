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

// mvccSequenceKey stores the last committed MVCC sequence.
func mvccSequenceKey() []byte {
	return []byte{prefixMVCCMeta, 0x01}
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

// ============================================================================
// Serialization helpers
// ============================================================================

// encodeNode serializes a Node using gob (preserves Go types like int64).
// If the node exceeds maxNodeSize, embeddings are stored separately and a flag is set.
// Returns: (nodeData, embeddingsStoredSeparately, error)
func encodeNode(n *Node) ([]byte, bool, error) {
	if err := validatePropertiesForStorage(n.Properties); err != nil {
		return nil, false, fmt.Errorf("invalid node properties: %w", err)
	}
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
	// Create a copy without embeddings for encoding
	nodeCopy := *n
	chunkCount := len(nodeCopy.ChunkEmbeddings)
	nodeCopy.ChunkEmbeddings = nil // Remove embeddings for encoding

	// Re-encode without embeddings
	// Set struct flag to indicate embeddings are stored separately
	nodeCopy.EmbeddingsStoredSeparately = true
	// Ensure chunk_count is set in EmbedMeta (embed queue sets it, but direct creates might not)
	if nodeCopy.EmbedMeta == nil {
		nodeCopy.EmbedMeta = make(map[string]any)
	}
	if _, hasChunkCount := nodeCopy.EmbedMeta["chunk_count"]; !hasChunkCount {
		nodeCopy.EmbedMeta["chunk_count"] = chunkCount
	}

	// Final encode with flag
	data, err = encodeValue(&nodeCopy)
	if err != nil {
		return nil, false, err
	}

	return data, true, nil
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

// decodeNode deserializes a Node from gob and loads embeddings separately if needed.
func decodeNode(data []byte) (*Node, error) {
	var node Node
	if err := decodeValue(data, &node); err != nil {
		return nil, err
	}
	return &node, nil
}

// decodeNodeWithEmbeddings deserializes a Node and loads separately stored embeddings from transaction.
// Works in both View and Update transactions (only reads embeddings).
func decodeNodeWithEmbeddings(txn *badger.Txn, data []byte, nodeID NodeID) (*Node, error) {
	node, err := decodeNode(data)
	if err != nil {
		return nil, err
	}

	// Check if embeddings are stored separately (struct flag set during encode)
	if node.EmbeddingsStoredSeparately {
		// Use chunk_count from EmbedMeta (set by embed queue) to know how many chunks to load
		// Handle various integer types (gob may decode as different types depending on value size)
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
					return nil, fmt.Errorf("failed to decode embedding chunk %d: %w", i, err)
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

// decodeEdge deserializes an Edge using the active storage serializer.
// Legacy gob/msgpack fallback — does NOT handle the compact format.
// On the BadgerEngine read path, prefer engine.decodeEdgeBody which
// dispatches based on the format byte.
func decodeEdge(data []byte) (*Edge, error) {
	var edge Edge
	if err := decodeValue(data, &edge); err != nil {
		return nil, err
	}
	return &edge, nil
}

// ============================================================================
