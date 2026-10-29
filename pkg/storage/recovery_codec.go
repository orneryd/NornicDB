package storage

import (
	"bytes"
	"fmt"
	"github.com/orneryd/nornicdb/internal/jsontext"
)

// recovery encodes value for WAL sidecars and snapshot frames. Entity
// payloads keep their metadata and exact property types; non-entity control
// records retain their historical JSON representation. The bytes stay valid
// until the encoder's next use.
func (e *valueEncoder) recovery(value any) ([]byte, error) {
	payload, typed := prepareRecoveryPayload(value)
	if !typed {
		return jsontext.Marshal(value)
	}
	return e.encode(payload)
}

// prepareRecoveryPayload returns the value to encode for an entity-bearing
// payload, with temporal property values wrapped for storage, and whether
// value is such a payload. An entity or property map is copied only when a
// temporal value inside it has to be wrapped; a node or edge without one is
// encoded as it is. A WAL payload struct is copied shallowly so its entity
// slots can be replaced without touching the caller's.
func prepareRecoveryPayload(value any) (any, bool) {
	switch payload := value.(type) {
	case Node:
		return prepareRecoveryPayload(&payload)

	case Edge:
		return prepareRecoveryPayload(&payload)

	case WALNodeData:
		return prepareRecoveryPayload(&payload)

	case WALEdgeData:
		return prepareRecoveryPayload(&payload)

	case WALDeleteData:
		return prepareRecoveryPayload(&payload)

	case WALBulkNodesData:
		return prepareRecoveryPayload(&payload)

	case WALBulkEdgesData:
		return prepareRecoveryPayload(&payload)

	case WALBulkDeleteNodesData:
		return prepareRecoveryPayload(&payload)

	case WALBulkDeleteEdgesData:
		return prepareRecoveryPayload(&payload)
	case walEntityCommitData:
		return prepareRecoveryPayload(&payload)
	case *walEntityCommitData:
		return value, payload != nil
	case walBufferedCommitPayload:
		return prepareRecoveryPayload(&payload)
	case WALBufferedCommitData:
		return prepareRecoveryPayload(&payload)
	case *Node:
		if payload == nil {
			return value, false
		}
		return prepareRecoveryNode(payload), true
	case *Edge:
		if payload == nil {
			return value, false
		}
		return prepareRecoveryEdge(payload), true
	case *WALNodeData:
		if payload == nil {
			return value, false
		}
		preparedNode := prepareRecoveryNode(payload.Node)
		preparedOldNode := prepareRecoveryNode(payload.OldNode)
		if preparedNode == payload.Node && preparedOldNode == payload.OldNode {
			return value, true
		}
		result := *payload
		result.Node = preparedNode
		result.OldNode = preparedOldNode
		return &result, true
	case *WALEdgeData:
		if payload == nil {
			return value, false
		}
		preparedEdge := prepareRecoveryEdge(payload.Edge)
		preparedOldEdge := prepareRecoveryEdge(payload.OldEdge)
		if preparedEdge == payload.Edge && preparedOldEdge == payload.OldEdge {
			return value, true
		}
		result := *payload
		result.Edge = preparedEdge
		result.OldEdge = preparedOldEdge
		return &result, true
	case *WALDeleteData:
		if payload == nil {
			return value, false
		}
		preparedOldNode := prepareRecoveryNode(payload.OldNode)
		preparedOldEdge := prepareRecoveryEdge(payload.OldEdge)
		if preparedOldNode == payload.OldNode && preparedOldEdge == payload.OldEdge {
			return value, true
		}
		result := *payload
		result.OldNode = preparedOldNode
		result.OldEdge = preparedOldEdge
		return &result, true
	case *WALBulkNodesData:
		if payload == nil {
			return value, false
		}
		converted, changed := mapRecoveryEntities(payload.Nodes, prepareRecoveryNode)
		if !changed {
			return value, true
		}
		result := *payload
		result.Nodes = converted
		return &result, true
	case *WALBulkEdgesData:
		if payload == nil {
			return value, false
		}
		converted, changed := mapRecoveryEntities(payload.Edges, prepareRecoveryEdge)
		if !changed {
			return value, true
		}
		result := *payload
		result.Edges = converted
		return &result, true
	case *WALBulkDeleteNodesData:
		if payload == nil {
			return value, false
		}
		converted, changed := mapRecoveryEntities(payload.OldNodes, prepareRecoveryNode)
		if !changed {
			return value, true
		}
		result := *payload
		result.OldNodes = converted
		return &result, true
	case *WALBulkDeleteEdgesData:
		if payload == nil {
			return value, false
		}
		converted, changed := mapRecoveryEntities(payload.OldEdges, prepareRecoveryEdge)
		if !changed {
			return value, true
		}
		result := *payload
		result.OldEdges = converted
		return &result, true
	case *walBufferedCommitPayload:
		if payload == nil {
			return value, false
		}
		operations, changed := prepareRecoveryOperations(payload.Ops)
		if !changed {
			return value, true
		}
		result := *payload
		result.Ops = operations
		return &result, true
	case *WALBufferedCommitData:
		if payload == nil {
			return value, false
		}
		operations, changed := prepareRecoveryOperations(payload.Ops)
		if !changed {
			return value, true
		}
		result := *payload
		result.Ops = operations
		return &result, true
	default:
		return value, false
	}
}

// prepareRecoveryOperations copies only when a temporal value needs the
// stored representation. Encoding never mutates a transaction's images.
func prepareRecoveryOperations(ops []Operation) ([]Operation, bool) {
	var result []Operation
	for i, op := range ops {
		node, oldNode := prepareRecoveryNode(op.Node), prepareRecoveryNode(op.OldNode)
		edge, oldEdge := prepareRecoveryEdge(op.Edge), prepareRecoveryEdge(op.OldEdge)
		if result == nil {
			if node == op.Node && oldNode == op.OldNode && edge == op.Edge && oldEdge == op.OldEdge {
				continue
			}
			result = make([]Operation, len(ops))
			copy(result, ops[:i])
		}
		op.Node, op.OldNode, op.Edge, op.OldEdge = node, oldNode, edge, oldEdge
		result[i] = op
	}
	return result, result != nil
}

type walRecoverySidecar struct {
	Encoding string `json:"_nornicdb_encoding,omitempty"`
	Data     []byte `json:"_nornicdb_data,omitempty"`
}

const walRecoveryEncoding = "storage-msgpack-v1"

// marshalWALRecoveryPayload uses one lossless typed representation for
// entity-bearing records. Control records retain their compact JSON encoding.
// With a buffer the result is valid until it is reused; without one it is
// owned by the caller, including when the encoder is returned to its pool.
func marshalWALRecoveryPayload(buffer *bytes.Buffer, value any) ([]byte, error) {
	payload, typed := prepareRecoveryPayload(value)
	if !typed {
		if buffer == nil {
			return jsontext.Marshal(value)
		}
		return marshalJSONCompact(buffer, value)
	}
	encoder := getValueEncoder()
	defer putValueEncoder(encoder)
	encoded, err := encoder.encode(payload)
	if err != nil {
		return nil, err
	}
	if buffer == nil {
		return bytes.Clone(encoded), nil
	}
	buffer.Reset()
	_, _ = buffer.Write(encoded)
	return buffer.Bytes(), nil
}

func decodeRecoveryPayload(data []byte, target any) error {
	_, err := decodeRecoveryPayloadAuthority(data, target)
	return err
}

// Decode once and report whether the current envelope preserves typed named
// properties. Historical JSON is never promoted to lossless authority.
func decodeRecoveryPayloadAuthority(data []byte, target any) (bool, error) {
	_, _, hasHeader, err := splitSerializationHeader(data)
	if err != nil {
		return false, err
	}
	if !hasHeader {
		trimmed := bytes.TrimSpace(data)
		if len(trimmed) == 0 || trimmed[0] != '{' {
			return false, decodeLegacyRecoveryJSON(bytes.NewReader(data), target)
		}
		var sidecar walRecoverySidecar
		if err := jsontext.Unmarshal(data, &sidecar); err != nil {
			return false, err
		}
		if sidecar.Encoding == "" {
			return false, decodeLegacyRecoveryJSON(bytes.NewReader(data), target)
		}
		if sidecar.Encoding != walRecoveryEncoding {
			return false, fmt.Errorf("unsupported recovery encoding %q", sidecar.Encoding)
		}
		if len(sidecar.Data) == 0 {
			return false, fmt.Errorf("empty recovery record")
		}
		data = sidecar.Data
	}
	if err := decodeValue(data, target); err != nil {
		return false, fmt.Errorf("decode recovery record: %w", err)
	}
	transformRecoveryEntities(target, restoreRecoveryNode, restoreRecoveryEdge)
	return true, nil
}

// prepareRecoveryNode returns node with its temporal property values
// wrapped for storage, copying it only when one is.
func prepareRecoveryNode(node *Node) *Node {
	if node == nil {
		return nil
	}
	properties, propertiesChanged := storedTemporalProperties(node.Properties)
	embedMeta, embedMetaChanged := storedTemporalProperties(node.EmbedMeta)
	if !propertiesChanged && !embedMetaChanged {
		return node
	}
	result := *node
	result.Properties = properties
	result.EmbedMeta = embedMeta
	return &result
}

// prepareRecoveryEdge is prepareRecoveryNode for an edge.
func prepareRecoveryEdge(edge *Edge) *Edge {
	if edge == nil {
		return nil
	}
	properties, changed := storedTemporalProperties(edge.Properties)
	if !changed {
		return edge
	}
	result := *edge
	result.Properties = properties
	return &result
}

func storedTemporalProperties(properties map[string]any) (map[string]any, bool) {
	if properties == nil {
		return nil, false
	}
	converted, changed := storedTemporalValue(properties)
	return converted.(map[string]any), changed
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

// mapRecoveryEntities applies transform to every entity, copying the slice
// only when transform replaces one.
func mapRecoveryEntities[T any](values []*T, transform func(*T) *T) ([]*T, bool) {
	var result []*T
	for i, value := range values {
		transformed := transform(value)
		if transformed != value && result == nil {
			result = make([]*T, len(values))
			copy(result, values[:i])
		}
		if result != nil {
			result[i] = transformed
		}
	}
	if result == nil {
		return values, false
	}
	return result, true
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
	case *Snapshot:
		value.Nodes, _ = mapRecoveryEntities(value.Nodes, node)
		value.Edges, _ = mapRecoveryEntities(value.Edges, edge)
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
		value.Nodes, _ = mapRecoveryEntities(value.Nodes, node)
	case *WALBulkEdgesData:
		value.Edges, _ = mapRecoveryEntities(value.Edges, edge)
	case *WALBulkDeleteNodesData:
		value.OldNodes, _ = mapRecoveryEntities(value.OldNodes, node)
	case *WALBulkDeleteEdgesData:
		value.OldEdges, _ = mapRecoveryEntities(value.OldEdges, edge)

	}
}
