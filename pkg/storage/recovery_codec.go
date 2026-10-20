package storage

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// encodeRecoveryPayload preserves entity metadata and exact property types.
// Non-entity control records retain their historical JSON representation.
func encodeRecoveryPayload(value any) ([]byte, error) {
	payload, typed := copyRecoveryPayload(value)
	if !typed {
		return json.Marshal(value)
	}
	transformRecoveryEntities(payload, prepareRecoveryNode, prepareRecoveryEdge)
	return encodeValue(payload)
}

type walRecoverySidecar struct {
	Encoding string `json:"_nornicdb_encoding,omitempty"`
	Data     []byte `json:"_nornicdb_data,omitempty"`
}

const walRecoveryEncoding = "storage-msgpack-v1"

// marshalWALRecoveryPayload retains the original JSON projection so older
// readers still recover the same baseline data. The sidecar owns full fidelity
// for current readers, without exposing storage extensions in client JSON.
func marshalWALRecoveryPayload(buffer *bytes.Buffer, value any) ([]byte, error) {
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
	if _, typed := copyRecoveryPayload(value); !typed || bytes.Equal(legacy, []byte("null")) {
		return legacy, nil
	}
	encoded, err := encodeRecoveryPayload(value)
	if err != nil {
		return nil, err
	}
	sidecar, err := json.Marshal(walRecoverySidecar{Encoding: walRecoveryEncoding, Data: encoded})
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

func decodeRecoveryPayload(data []byte, target any) error {
	_, _, hasHeader, err := splitSerializationHeader(data)
	if err != nil {
		return err
	}
	if !hasHeader {
		trimmed := bytes.TrimSpace(data)
		if len(trimmed) == 0 || trimmed[0] != '{' {
			return decodeLegacyRecoveryJSON(bytes.NewReader(data), target)
		}
		var sidecar walRecoverySidecar
		if err := json.Unmarshal(data, &sidecar); err != nil {
			return err
		}
		if sidecar.Encoding == "" {
			return decodeLegacyRecoveryJSON(bytes.NewReader(data), target)
		}
		if sidecar.Encoding != walRecoveryEncoding {
			return fmt.Errorf("unsupported recovery encoding %q", sidecar.Encoding)
		}
		if len(sidecar.Data) == 0 {
			return fmt.Errorf("empty recovery record")
		}
		data = sidecar.Data
	}
	if err := decodeValue(data, target); err != nil {
		return fmt.Errorf("decode recovery record: %w", err)
	}
	transformRecoveryEntities(target, restoreRecoveryNode, restoreRecoveryEdge)
	return nil
}

func copyRecoveryPayload(value any) (any, bool) {
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

func prepareRecoveryNode(node *Node) *Node {
	if node == nil {
		return nil
	}
	result := *node
	if node.Properties != nil {
		result.Properties = encodeStoredTemporalValue(node.Properties).(map[string]any)
	}
	if node.EmbedMeta != nil {
		result.EmbedMeta = encodeStoredTemporalValue(node.EmbedMeta).(map[string]any)
	}
	return &result
}
func prepareRecoveryEdge(edge *Edge) *Edge {
	if edge == nil {
		return nil
	}
	result := *edge
	if edge.Properties != nil {
		result.Properties = encodeStoredTemporalValue(edge.Properties).(map[string]any)
	}
	return &result
}
func restoreRecoveryNode(node *Node) *Node {
	if node != nil {
		restoreStoredTemporalValue(node.Properties)
		restoreStoredTemporalValue(node.EmbedMeta)
	}
	return node
}
func restoreRecoveryEdge(edge *Edge) *Edge {
	if edge != nil {
		restoreStoredTemporalValue(edge.Properties)
	}
	return edge
}

func mapRecoveryEntities[T any](values []*T, transform func(*T) *T) []*T {
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
func transformRecoveryEntities(value any, node func(*Node) *Node, edge func(*Edge) *Edge) {
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
	case *namespaceWALRecord:
		if value.Mutation != nil {
			value.Mutation.Record = node(value.Mutation.Record)
		}
	case *Snapshot:
		value.Nodes = mapRecoveryEntities(value.Nodes, node)
		value.Edges = mapRecoveryEntities(value.Edges, edge)
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
		value.Nodes = mapRecoveryEntities(value.Nodes, node)
	case *WALBulkEdgesData:
		value.Edges = mapRecoveryEntities(value.Edges, edge)
	case *WALBulkDeleteNodesData:
		value.OldNodes = mapRecoveryEntities(value.OldNodes, node)
	case *WALBulkDeleteEdgesData:
		value.OldEdges = mapRecoveryEntities(value.OldEdges, edge)

	}
}
