package storage

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
)

// encodeRecoveryPayload is valueEncoder.recovery for a caller without an
// encoder of its own; the result is owned by the caller.
func encodeRecoveryPayload(value any) ([]byte, error) {
	encoder := getValueEncoder()
	defer putValueEncoder(encoder)
	encoded, err := encoder.recovery(value)
	if err != nil {
		return nil, err
	}
	return bytes.Clone(encoded), nil
}

// recovery encodes value for WAL sidecars and snapshot frames. Entity
// payloads keep their metadata and exact property types; non-entity control
// records retain their historical JSON representation. The bytes stay valid
// until the encoder's next use.
func (e *valueEncoder) recovery(value any) ([]byte, error) {
	payload, typed := prepareRecoveryPayload(value)
	if !typed {
		return json.Marshal(value)
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
	default:
		return value, false
	}
}

type walRecoverySidecar struct {
	Encoding string `json:"_nornicdb_encoding,omitempty"`
	Data     []byte `json:"_nornicdb_data,omitempty"`
}

const walRecoveryEncoding = "storage-msgpack-v1"

// walRecoverySidecarPrefix is json.Marshal of walRecoverySidecar up to the
// opening quote of the base64 data, without the leading brace;
// marshalWALRecoveryPayload writes the same bytes without the marshal.
const walRecoverySidecarPrefix = `"_nornicdb_encoding":"` + walRecoveryEncoding + `","_nornicdb_data":"`

// marshalWALRecoveryPayload retains the original JSON projection so older
// readers still recover the same baseline data. The sidecar owns full fidelity
// for current readers, without exposing storage extensions in client JSON.
// With a buffer the result is the buffer's bytes, valid until it is reused;
// without one the result is owned by the caller.
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
	payload, typed := prepareRecoveryPayload(value)
	if !typed || bytes.Equal(legacy, []byte("null")) {
		return legacy, nil
	}
	encoder := getValueEncoder()
	defer putValueEncoder(encoder)
	encoded, err := encoder.encode(payload)
	if err != nil {
		return nil, err
	}
	// legacy is a JSON object: replace its closing brace with the sidecar
	// fields, as json.Marshal(walRecoverySidecar) would spell them.
	size := len(legacy) + len(walRecoverySidecarPrefix) + base64.StdEncoding.EncodedLen(len(encoded)) + 2
	var result []byte
	if buffer == nil {
		result = make([]byte, 0, size)
		result = append(result, legacy[:len(legacy)-1]...)
	} else {
		buffer.Truncate(len(legacy) - 1)
		buffer.Grow(size - len(legacy) + 1)
		result = buffer.AvailableBuffer()
	}
	if len(legacy) > 2 {
		result = append(result, ',')
	}
	result = append(result, walRecoverySidecarPrefix...)
	result = base64.StdEncoding.AppendEncode(result, encoded)
	result = append(result, '"', '}')
	if buffer == nil {
		return result, nil
	}
	buffer.Write(result)
	return buffer.Bytes(), nil
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
	case *namespaceWALRecord:
		if value.Mutation != nil {
			value.Mutation.Record = node(value.Mutation.Record)
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
