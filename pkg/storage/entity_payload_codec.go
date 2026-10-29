package storage

import (
	"fmt"

	"github.com/vmihailenco/msgpack/v5"
)

// EncodeCommandPayload encodes one replication message as MessagePack.
func EncodeCommandPayload(value any) ([]byte, error) {
	return msgpack.Marshal(value)
}

// DecodeCommandPayload decodes exactly one replication message. Structural
// validation rejects forged collection lengths before the decoder allocates.
func DecodeCommandPayload(data []byte, value any) error {
	// A tagged property list has two MessagePack containers per value, plus
	// a few enclosing message/image containers.
	if err := validateMessagePackValue(data, 2*maxTransportPropertyDepth+16); err != nil {
		return fmt.Errorf("invalid replication payload: %w", err)
	}
	return msgpack.Unmarshal(data, value)
}

// EncodeNodePayload encodes a complete node image for replication.
func EncodeNodePayload(node *Node) ([]byte, error) {
	if node == nil {
		return nil, fmt.Errorf("nil node")
	}
	return EncodeCommandPayload(transportNodeFrom(node))
}

// DecodeNodePayload restores a node image from the current replication format.
func DecodeNodePayload(data []byte) (*Node, error) {
	var image transportNodeImage
	if err := DecodeCommandPayload(data, &image); err != nil {
		return nil, err
	}
	if image.ID == "" {
		return nil, ErrInvalidID
	}
	return image.storedNode(), nil
}

// EncodeEdgePayload encodes a complete edge image for replication.
func EncodeEdgePayload(edge *Edge) ([]byte, error) {
	if edge == nil {
		return nil, fmt.Errorf("nil edge")
	}
	return EncodeCommandPayload(transportEdgeFrom(edge))
}

// DecodeEdgePayload restores an edge image from the current replication format.
func DecodeEdgePayload(data []byte) (*Edge, error) {
	var image transportEdgeImage
	if err := DecodeCommandPayload(data, &image); err != nil {
		return nil, err
	}
	if image.ID == "" {
		return nil, ErrInvalidID
	}
	return image.storedEdge(), nil
}
