// Frozen pre-optimization codec from 16923aef7 (also identical on release).
// The differential keeps its allocations, traversal and msgpack.Marshal path.
package storage

import (
	"bytes"
	"encoding/json"
	"time"

	"github.com/vmihailenco/msgpack/v5"
)

// referenceEncodeRecoveryPayload preserves entity metadata and exact property types.
// Non-entity control records retain their historical JSON representation.
func referenceEncodeRecoveryPayload(value any) ([]byte, error) {
	payload, typed := referenceCopyRecoveryPayload(value)
	if !typed {
		return json.Marshal(value)
	}
	referenceTransformRecoveryEntities(payload, referencePrepareRecoveryNode, referencePrepareRecoveryEdge)
	return referenceEncodeValue(payload)
}

type referenceWalRecoverySidecar struct {
	Encoding string `json:"_nornicdb_encoding,omitempty"`
	Data     []byte `json:"_nornicdb_data,omitempty"`
}

const referenceWalRecoveryEncoding = "storage-msgpack-v1"

// referenceMarshalWALRecoveryPayload retains the original JSON projection so older
// readers still recover the same baseline data. The sidecar owns full fidelity
// for current readers, without exposing storage extensions in client JSON.
func referenceMarshalWALRecoveryPayload(buffer *bytes.Buffer, value any) ([]byte, error) {
	var legacy []byte
	var err error
	if buffer == nil {
		legacy, err = json.Marshal(value)
	} else {
		legacy, err = marshalJSONCompact(buffer, value)
	}
	if err != nil {
		return nil, err
	}
	if _, typed := referenceCopyRecoveryPayload(value); !typed || bytes.Equal(legacy, []byte("null")) {
		return legacy, nil
	}
	encoded, err := referenceEncodeRecoveryPayload(value)
	if err != nil {
		return nil, err
	}
	sidecar, err := json.Marshal(referenceWalRecoverySidecar{Encoding: referenceWalRecoveryEncoding, Data: encoded})
	if err != nil {
		return nil, err
	}
	result := make([]byte, 0, len(legacy)+len(sidecar))
	result = append(result, legacy[:len(legacy)-1]...)
	if len(legacy) > 2 {
		result = append(result, ',')
	}
	return append(result, sidecar[1:]...), nil
}

func referenceCopyRecoveryPayload(value any) (any, bool) {
	switch value := value.(type) {
	case Node:
		return &value, true
	case *Node:
		if value == nil {
			return value, false
		}
		copy := *value
		return &copy, true
	case Edge:
		return &value, true
	case *Edge:
		if value == nil {
			return value, false
		}
		copy := *value
		return &copy, true
	case WALNodeData:
		return &value, true
	case *WALNodeData:
		if value == nil {
			return value, false
		}
		copy := *value
		return &copy, true
	case WALEdgeData:
		return &value, true
	case *WALEdgeData:
		if value == nil {
			return value, false
		}
		copy := *value
		return &copy, true
	case WALDeleteData:
		return &value, true
	case *WALDeleteData:
		if value == nil {
			return value, false
		}
		copy := *value
		return &copy, true
	case WALBulkNodesData:
		return &value, true
	case *WALBulkNodesData:
		if value == nil {
			return value, false
		}
		copy := *value
		return &copy, true
	case WALBulkEdgesData:
		return &value, true
	case *WALBulkEdgesData:
		if value == nil {
			return value, false
		}
		copy := *value
		return &copy, true
	case WALBulkDeleteNodesData:
		return &value, true
	case *WALBulkDeleteNodesData:
		if value == nil {
			return value, false
		}
		copy := *value
		return &copy, true
	case WALBulkDeleteEdgesData:
		return &value, true
	case *WALBulkDeleteEdgesData:
		if value == nil {
			return value, false
		}
		copy := *value
		return &copy, true
	default:
		return value, false
	}
}

func referencePrepareRecoveryNode(node *Node) *Node {
	if node == nil {
		return nil
	}
	result := *node
	if node.Properties != nil {
		result.Properties = referenceEncodeStoredTemporalValue(node.Properties).(map[string]any)
	}
	if node.EmbedMeta != nil {
		result.EmbedMeta = referenceEncodeStoredTemporalValue(node.EmbedMeta).(map[string]any)
	}
	return &result
}
func referencePrepareRecoveryEdge(edge *Edge) *Edge {
	if edge == nil {
		return nil
	}
	result := *edge
	if edge.Properties != nil {
		result.Properties = referenceEncodeStoredTemporalValue(edge.Properties).(map[string]any)
	}
	return &result
}
func referenceMapRecoveryEntities[T any](values []*T, transform func(*T) *T) []*T {
	if values == nil {
		return nil
	}
	result := make([]*T, len(values))
	for i, value := range values {
		result[i] = transform(value)
	}
	return result
}

// Every entity-bearing redo/undo payload and both snapshot formats share
// the same property/type boundary. Scalar payload fields remain untouched.
func referenceTransformRecoveryEntities(value any, node func(*Node) *Node, edge func(*Edge) *Edge) {
	switch value := value.(type) {
	case *Node:
		*value = *node(value)
	case *Edge:
		*value = *edge(value)
	case *WALBufferedCommitData:
		for i := range value.Ops {
			operation := &value.Ops[i]
			operation.Node = node(operation.Node)
			operation.OldNode = node(operation.OldNode)
			operation.Edge = edge(operation.Edge)
			operation.OldEdge = edge(operation.OldEdge)
		}
	case *Snapshot:
		value.Nodes = referenceMapRecoveryEntities(value.Nodes, node)
		value.Edges = referenceMapRecoveryEntities(value.Edges, edge)
	case *WALNodeData:
		value.Node = node(value.Node)
		value.OldNode = node(value.OldNode)
	case *WALEdgeData:
		value.Edge = edge(value.Edge)
		value.OldEdge = edge(value.OldEdge)
	case *WALDeleteData:
		value.OldNode = node(value.OldNode)
		value.OldEdge = edge(value.OldEdge)
	case *WALBulkNodesData:
		value.Nodes = referenceMapRecoveryEntities(value.Nodes, node)
	case *WALBulkEdgesData:
		value.Edges = referenceMapRecoveryEntities(value.Edges, edge)
	case *WALBulkDeleteNodesData:
		value.OldNodes = referenceMapRecoveryEntities(value.OldNodes, node)
	case *WALBulkDeleteEdgesData:
		value.OldEdges = referenceMapRecoveryEntities(value.OldEdges, edge)

	}
}
func referenceEncodeValue(value any) ([]byte, error) {
	payload, err := msgpack.Marshal(value)
	if err != nil {
		return nil, err
	}
	out := append([]byte(serializationMagic), serializationVersion, serializerIDMsgpack)
	return append(out, payload...), nil
}

func referenceEncodeStoredTemporalValue(value any) any {
	switch value := value.(type) {
	case time.Time:
		return storedTime{value: value}
	case []any:
		result := make([]any, len(value))
		for i, v := range value {
			result[i] = referenceEncodeStoredTemporalValue(v)
		}
		return result
	case map[string]any:
		result := make(map[string]any, len(value))
		for k, v := range value {
			result[k] = referenceEncodeStoredTemporalValue(v)
		}
		return result
	default:
		return value
	}
}
