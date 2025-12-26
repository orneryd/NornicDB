// Package storage provides storage engine implementations for NornicDB.
package storage

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"strings"

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
	// First, try encoding with embeddings
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(n); err != nil {
		return nil, false, err
	}

	data := buf.Bytes()

	// If size is acceptable, return as-is
	if len(data) <= maxNodeSize {
		return data, false, nil
	}

	// Node is too large - store embeddings separately
	// Create a copy without embeddings for encoding
	nodeCopy := *n
	embeddingsToStore := nodeCopy.ChunkEmbeddings
	nodeCopy.ChunkEmbeddings = nil // Remove embeddings for encoding

	// Re-encode without embeddings
	buf.Reset()
	if err := gob.NewEncoder(&buf).Encode(&nodeCopy); err != nil {
		return nil, false, err
	}

	// Set flag in properties to indicate embeddings are stored separately
	if nodeCopy.Properties == nil {
		nodeCopy.Properties = make(map[string]any)
	}
	nodeCopy.Properties["_embeddings_stored_separately"] = true
	nodeCopy.Properties["_embedding_chunk_count"] = len(embeddingsToStore)

	// Final encode with flag
	buf.Reset()
	if err := gob.NewEncoder(&buf).Encode(&nodeCopy); err != nil {
		return nil, false, err
	}

	return buf.Bytes(), true, nil
}

// encodeEmbedding serializes a single embedding chunk.
func encodeEmbedding(emb []float32) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(emb); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// decodeEmbedding deserializes a single embedding chunk.
func decodeEmbedding(data []byte) ([]float32, error) {
	var emb []float32
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&emb); err != nil {
		return nil, err
	}
	return emb, nil
}

// decodeNode deserializes a Node from gob and loads embeddings separately if needed.
func decodeNode(data []byte) (*Node, error) {
	var node Node
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&node); err != nil {
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

	// Check if embeddings are stored separately
	if storedSeparately, ok := node.Properties["_embeddings_stored_separately"].(bool); ok && storedSeparately {
		chunkCount, _ := node.Properties["_embedding_chunk_count"].(int)
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

			// Remove internal flags from properties
			delete(node.Properties, "_embeddings_stored_separately")
			delete(node.Properties, "_embedding_chunk_count")
		}
	}

	return node, nil
}

// encodeEdge serializes an Edge using gob (preserves Go types).
func encodeEdge(e *Edge) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(e); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// decodeEdge deserializes an Edge from gob.
func decodeEdge(data []byte) (*Edge, error) {
	var edge Edge
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&edge); err != nil {
		return nil, err
	}
	return &edge, nil
}

// ============================================================================
