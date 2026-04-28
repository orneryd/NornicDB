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

func mvccNodeVersionPrefix(id NodeID) []byte {
	key := make([]byte, 0, 1+len(id)+1)
	key = append(key, prefixMVCCNode)
	key = append(key, []byte(id)...)
	key = append(key, 0x00)
	return key
}

func mvccNodeVersionKey(id NodeID, version MVCCVersion) []byte {
	key := mvccNodeVersionPrefix(id)
	key = append(key, encodeMVCCSortVersion(version)...)
	return key
}

func mvccEdgeVersionPrefix(id EdgeID) []byte {
	key := make([]byte, 0, 1+len(id)+1)
	key = append(key, prefixMVCCEdge)
	key = append(key, []byte(id)...)
	key = append(key, 0x00)
	return key
}

func mvccEdgeVersionKey(id EdgeID, version MVCCVersion) []byte {
	key := mvccEdgeVersionPrefix(id)
	key = append(key, encodeMVCCSortVersion(version)...)
	return key
}

func mvccNodeHeadKey(id NodeID) []byte {
	return append([]byte{prefixMVCCNodeHead}, []byte(id)...)
}

func mvccEdgeHeadKey(id EdgeID) []byte {
	return append([]byte{prefixMVCCEdgeHead}, []byte(id)...)
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

func extractNodeIDAndMVCCVersionFromVersionKey(key []byte) (NodeID, MVCCVersion, error) {
	if len(key) < 18 || key[0] != prefixMVCCNode {
		return "", MVCCVersion{}, fmt.Errorf("invalid mvcc node version key")
	}
	idEnd := len(key) - 17
	if key[idEnd] != 0x00 {
		return "", MVCCVersion{}, fmt.Errorf("invalid mvcc node version separator")
	}
	version, err := decodeMVCCSortVersion(key[len(key)-16:])
	if err != nil {
		return "", MVCCVersion{}, err
	}
	return NodeID(key[1:idEnd]), version, nil
}

func extractEdgeIDAndMVCCVersionFromVersionKey(key []byte) (EdgeID, MVCCVersion, error) {
	if len(key) < 18 || key[0] != prefixMVCCEdge {
		return "", MVCCVersion{}, fmt.Errorf("invalid mvcc edge version key")
	}
	idEnd := len(key) - 17
	if key[idEnd] != 0x00 {
		return "", MVCCVersion{}, fmt.Errorf("invalid mvcc edge version separator")
	}
	version, err := decodeMVCCSortVersion(key[len(key)-16:])
	if err != nil {
		return "", MVCCVersion{}, err
	}
	return EdgeID(key[1:idEnd]), version, nil
}

func extractMVCCLogicalKeyAndVersion(key []byte) ([]byte, MVCCVersion, error) {
	if len(key) < 18 {
		return nil, MVCCVersion{}, fmt.Errorf("invalid mvcc key length: %d", len(key))
	}
	version, err := decodeMVCCSortVersion(key[len(key)-16:])
	if err != nil {
		return nil, MVCCVersion{}, err
	}
	separatorIndex := len(key) - 17
	if key[separatorIndex] != 0x00 {
		return nil, MVCCVersion{}, fmt.Errorf("invalid mvcc logical key separator")
	}
	logical := append([]byte(nil), key[:separatorIndex]...)
	return logical, version, nil
}

// labelIndexKey creates a key for the label index.
// Format: prefix + label (lowercase) + 0x00 + nodeID
// Labels are normalized to lowercase for case-insensitive matching (Neo4j compatible)
func labelIndexKey(label string, nodeID NodeID) []byte {
	normalizedLabel := strings.ToLower(label)
	key := make([]byte, 0, 1+len(normalizedLabel)+1+len(nodeID))
	key = append(key, prefixLabelIndex)
	key = append(key, []byte(normalizedLabel)...)
	key = append(key, 0x00) // Separator
	key = append(key, []byte(nodeID)...)
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
func outgoingIndexKey(nodeID NodeID, edgeID EdgeID) []byte {
	key := make([]byte, 0, 1+len(nodeID)+1+len(edgeID))
	key = append(key, prefixOutgoingIndex)
	key = append(key, []byte(nodeID)...)
	key = append(key, 0x00)
	key = append(key, []byte(edgeID)...)
	return key
}

// outgoingIndexPrefix returns the prefix for scanning outgoing edges.
func outgoingIndexPrefix(nodeID NodeID) []byte {
	key := make([]byte, 0, 1+len(nodeID)+1)
	key = append(key, prefixOutgoingIndex)
	key = append(key, []byte(nodeID)...)
	key = append(key, 0x00)
	return key
}

// incomingIndexKey creates a key for the incoming edge index.
func incomingIndexKey(nodeID NodeID, edgeID EdgeID) []byte {
	key := make([]byte, 0, 1+len(nodeID)+1+len(edgeID))
	key = append(key, prefixIncomingIndex)
	key = append(key, []byte(nodeID)...)
	key = append(key, 0x00)
	key = append(key, []byte(edgeID)...)
	return key
}

// incomingIndexPrefix returns the prefix for scanning incoming edges.
func incomingIndexPrefix(nodeID NodeID) []byte {
	key := make([]byte, 0, 1+len(nodeID)+1)
	key = append(key, prefixIncomingIndex)
	key = append(key, []byte(nodeID)...)
	key = append(key, 0x00)
	return key
}

// edgeTypeIndexKey creates a key for the edge type index.
// Format: prefix + edgeType (lowercase) + 0x00 + edgeID
func edgeTypeIndexKey(edgeType string, edgeID EdgeID) []byte {
	normalizedType := strings.ToLower(edgeType)
	key := make([]byte, 0, 1+len(normalizedType)+1+len(edgeID))
	key = append(key, prefixEdgeTypeIndex)
	key = append(key, []byte(normalizedType)...)
	key = append(key, 0x00) // Separator
	key = append(key, []byte(edgeID)...)
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
// The key order lets GetEdgesBetween scan by start/end and lets GetEdgeBetween
// narrow further by relationship type without scanning every outgoing edge from
// the start node.
func edgeBetweenIndexKey(startID, endID NodeID, edgeType string, edgeID EdgeID) []byte {
	normalizedType := strings.ToLower(edgeType)
	key := make([]byte, 0, 1+len(startID)+1+len(endID)+1+len(normalizedType)+1+len(edgeID))
	key = append(key, prefixEdgeBetweenIndex)
	key = append(key, []byte(startID)...)
	key = append(key, 0x00)
	key = append(key, []byte(endID)...)
	key = append(key, 0x00)
	key = append(key, []byte(normalizedType)...)
	key = append(key, 0x00)
	key = append(key, []byte(edgeID)...)
	return key
}

// edgeBetweenHeadKey stores the fastest typed lookup for one edge between nodes.
//
// The set index remains the source for GetEdgesBetween and for multiple edges
// of the same type. The head is a compatibility-friendly accelerator: if it is
// absent or stale, reads can fall back to the set or legacy outgoing scan and
// repopulate it.
func edgeBetweenHeadKey(startID, endID NodeID, edgeType string) []byte {
	normalizedType := strings.ToLower(edgeType)
	key := make([]byte, 0, 1+len(startID)+1+len(endID)+1+len(normalizedType))
	key = append(key, prefixEdgeBetweenHead)
	key = append(key, []byte(startID)...)
	key = append(key, 0x00)
	key = append(key, []byte(endID)...)
	key = append(key, 0x00)
	key = append(key, []byte(normalizedType)...)
	return key
}

// writeEdgeBetweenIndexesInTxn maintains both relationship lookup indexes.
func writeEdgeBetweenIndexesInTxn(txn *badger.Txn, edge *Edge) error {
	if err := txn.Set(edgeBetweenIndexKey(edge.StartNode, edge.EndNode, edge.Type, edge.ID), []byte{}); err != nil {
		return err
	}
	return txn.Set(edgeBetweenHeadKey(edge.StartNode, edge.EndNode, edge.Type), []byte(edge.ID))
}

// deleteEdgeBetweenIndexesInTxn removes an edge from the set index and clears
// the head only when it points at the same edge.
func deleteEdgeBetweenIndexesInTxn(txn *badger.Txn, edge *Edge) error {
	if err := txn.Delete(edgeBetweenIndexKey(edge.StartNode, edge.EndNode, edge.Type, edge.ID)); err != nil {
		return err
	}
	return deleteEdgeBetweenHeadIfMatchesInTxn(txn, edge)
}

// deleteEdgeBetweenHeadIfMatchesInTxn avoids removing a head already advanced
// to another same-type edge.
func deleteEdgeBetweenHeadIfMatchesInTxn(txn *badger.Txn, edge *Edge) error {
	key := edgeBetweenHeadKey(edge.StartNode, edge.EndNode, edge.Type)
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

// edgeBetweenIndexPrefix returns the prefix for all edges between two nodes.
func edgeBetweenIndexPrefix(startID, endID NodeID) []byte {
	key := make([]byte, 0, 1+len(startID)+1+len(endID)+1)
	key = append(key, prefixEdgeBetweenIndex)
	key = append(key, []byte(startID)...)
	key = append(key, 0x00)
	key = append(key, []byte(endID)...)
	key = append(key, 0x00)
	return key
}

// typedEdgeBetweenIndexPrefix returns the prefix for edges of one type between two nodes.
func typedEdgeBetweenIndexPrefix(startID, endID NodeID, edgeType string) []byte {
	normalizedType := strings.ToLower(edgeType)
	key := edgeBetweenIndexPrefix(startID, endID)
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

// extractEdgeIDFromIndexKey extracts the edgeID from an index key.
// Format: prefix + nodeID + 0x00 + edgeID
func extractEdgeIDFromIndexKey(key []byte) EdgeID {
	// Find the separator (0x00) - reverse iteration (separator typically near end)
	for i := len(key) - 1; i >= 1; i-- {
		if key[i] == 0x00 {
			return EdgeID(key[i+1:])
		}
	}
	return ""
}

// extractEdgeIDFromEdgeBetweenIndexKey returns the edge ID suffix from an
// edge-between index entry.
func extractEdgeIDFromEdgeBetweenIndexKey(key []byte) EdgeID {
	if len(key) < 2 || key[0] != prefixEdgeBetweenIndex {
		return ""
	}
	for i := len(key) - 1; i >= 1; i-- {
		if key[i] == 0x00 {
			if i+1 >= len(key) {
				return ""
			}
			return EdgeID(key[i+1:])
		}
	}
	return ""
}

// extractNodeIDFromLabelIndex extracts the nodeID from a label index key.
// Format: prefix + label + 0x00 + nodeID
func extractNodeIDFromLabelIndex(key []byte, labelLen int) NodeID {
	// Skip prefix (1) + label (labelLen) + separator (1)
	offset := 1 + labelLen + 1
	if offset >= len(key) {
		return ""
	}
	return NodeID(key[offset:])
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
func decodeEdge(data []byte) (*Edge, error) {
	var edge Edge
	if err := decodeValue(data, &edge); err != nil {
		return nil, err
	}
	return &edge, nil
}

// ============================================================================
