package storage

import (
	"time"

	"github.com/vmihailenco/msgpack/v5"
)

// ReplicationSnapshot is the full set of stored entities at a WAL position.
// Its MessagePack wire representation preserves dynamic property types and
// embedding metadata without changing the database's stored entity format.
type ReplicationSnapshot struct {
	WALPosition uint64
	Nodes       []*Node
	Edges       []*Edge
}

// transportTime uses time.Time's binary contract, as the former gob
// transport did, so a fixed zone offset survives replication.
type transportTime time.Time

func (t transportTime) EncodeMsgpack(enc *msgpack.Encoder) error {
	data, err := time.Time(t).MarshalBinary()
	if err != nil {
		return err
	}
	return enc.EncodeBytes(data)
}

func (t *transportTime) DecodeMsgpack(dec *msgpack.Decoder) error {
	data, err := dec.DecodeBytes()
	if err != nil {
		return err
	}
	var value time.Time
	if err := value.UnmarshalBinary(data); err != nil {
		return err
	}
	*t = transportTime(value)
	return nil
}

// The same node and edge images serve direct commands and snapshots. These
// types are private to the transport; Node and Edge retain their Badger codec.
//
// transportNodeImage is the Node layout itself rather than a copied field
// struct, so encoding and decoding use the stored fields directly and
// callers retype the pointer without copying. A defined type does not inherit
// Node's methods, so EncodeMsgpack/DecodeMsgpack below own exactly the node
// wire map the copied struct produced: the same keys in the same order, with
// transportPropertyMap and transportTime keeping dynamic property and time
// semantics unchanged.
type transportNodeImage Node

func (node *transportNodeImage) EncodeMsgpack(enc *msgpack.Encoder) error {
	metadata := (*Node)(node).RuntimeMetadata()
	if err := enc.EncodeMapLen(11); err != nil {
		return err
	}
	if err := enc.EncodeString("Database"); err != nil {
		return err
	}
	if err := enc.EncodeString(metadata.Database); err != nil {
		return err
	}
	if err := enc.EncodeString("ID"); err != nil {
		return err
	}
	if err := enc.EncodeString(string(node.ID)); err != nil {
		return err
	}
	if err := enc.EncodeString("Labels"); err != nil {
		return err
	}
	if err := enc.Encode(node.Labels); err != nil {
		return err
	}
	if err := enc.EncodeString("Properties"); err != nil {
		return err
	}
	if err := enc.Encode(transportPropertyMap(node.Properties)); err != nil {
		return err
	}
	if err := enc.EncodeString("CreatedAt"); err != nil {
		return err
	}
	if err := enc.Encode(transportTime(node.CreatedAt)); err != nil {
		return err
	}
	if err := enc.EncodeString("UpdatedAt"); err != nil {
		return err
	}
	if err := enc.Encode(transportTime(node.UpdatedAt)); err != nil {
		return err
	}
	if err := enc.EncodeString("NamedEmbeddings"); err != nil {
		return err
	}
	if err := enc.Encode(node.NamedEmbeddings); err != nil {
		return err
	}
	if err := enc.EncodeString("ChunkEmbeddings"); err != nil {
		return err
	}
	if err := enc.Encode(node.ChunkEmbeddings); err != nil {
		return err
	}
	if err := enc.EncodeString("EmbedMeta"); err != nil {
		return err
	}
	if err := enc.Encode(transportPropertyMap(node.EmbedMeta)); err != nil {
		return err
	}
	if err := enc.EncodeString("EmbeddingsStoredSeparately"); err != nil {
		return err
	}
	if err := enc.Encode(metadata.EmbeddingsStoredSeparately); err != nil {
		return err
	}
	if err := enc.EncodeString("VisibilitySuppressed"); err != nil {
		return err
	}
	return enc.Encode(metadata.VisibilitySuppressed)
}

func (node *transportNodeImage) DecodeMsgpack(dec *msgpack.Decoder) error {
	count, err := dec.DecodeMapLen()
	if err != nil {
		return err
	}
	if count == -1 {
		*node = transportNodeImage{}
		return nil
	}
	metadata := (*Node)(node).RuntimeMetadata()
	defer func() { (*Node)(node).SetRuntimeMetadata(metadata) }()
	for i := 0; i < count; i++ {
		key, err := dec.DecodeString()
		if err != nil {
			return err
		}
		switch key {
		case "Database":
			err = dec.Decode(&metadata.Database)
		case "ID":
			err = dec.Decode((*NodeID)(&node.ID))
		case "Labels":
			err = dec.Decode(&node.Labels)
		case "Properties":
			err = dec.Decode((*transportPropertyMap)(&node.Properties))
		case "CreatedAt":
			err = dec.Decode((*transportTime)(&node.CreatedAt))
		case "UpdatedAt":
			err = dec.Decode((*transportTime)(&node.UpdatedAt))
		case "NamedEmbeddings":
			err = dec.Decode(&node.NamedEmbeddings)
		case "ChunkEmbeddings":
			err = dec.Decode(&node.ChunkEmbeddings)
		case "EmbedMeta":
			err = dec.Decode((*transportPropertyMap)(&node.EmbedMeta))
		case "EmbeddingsStoredSeparately":
			err = dec.Decode(&metadata.EmbeddingsStoredSeparately)
		case "VisibilitySuppressed":
			err = dec.Decode(&metadata.VisibilitySuppressed)
		default:
			err = dec.Skip()
		}
		if err != nil {
			return err
		}
	}
	return nil
}

type transportEdgeImage struct {
	Database             string
	ID                   EdgeID
	StartNode            NodeID
	EndNode              NodeID
	Type                 string
	Properties           transportPropertyMap
	CreatedAt            transportTime
	UpdatedAt            transportTime
	Confidence           float64
	AutoGenerated        bool
	VisibilitySuppressed bool
}

func transportEdgeFrom(edge *Edge) *transportEdgeImage {
	if edge == nil {
		return nil
	}
	return &transportEdgeImage{
		Database: edge.Database, ID: edge.ID, StartNode: edge.StartNode, EndNode: edge.EndNode,
		Type: edge.Type, Properties: transportPropertyMap(edge.Properties),
		CreatedAt: transportTime(edge.CreatedAt), UpdatedAt: transportTime(edge.UpdatedAt),
		Confidence: edge.Confidence, AutoGenerated: edge.AutoGenerated, VisibilitySuppressed: edge.VisibilitySuppressed,
	}
}

func (edge *transportEdgeImage) storedEdge() *Edge {
	if edge == nil {
		return nil
	}
	return &Edge{
		Database: edge.Database, ID: edge.ID, StartNode: edge.StartNode, EndNode: edge.EndNode,
		Type: edge.Type, Properties: map[string]any(edge.Properties),
		CreatedAt: time.Time(edge.CreatedAt), UpdatedAt: time.Time(edge.UpdatedAt),
		Confidence: edge.Confidence, AutoGenerated: edge.AutoGenerated, VisibilitySuppressed: edge.VisibilitySuppressed,
	}
}

type replicationSnapshotWire struct {
	WALPosition uint64
	Nodes       []*transportNodeImage
	Edges       []*transportEdgeImage
}

func (snapshot ReplicationSnapshot) EncodeMsgpack(enc *msgpack.Encoder) error {
	wire := replicationSnapshotWire{WALPosition: snapshot.WALPosition}
	if snapshot.Nodes != nil {
		wire.Nodes = make([]*transportNodeImage, len(snapshot.Nodes))
		for i, node := range snapshot.Nodes {
			wire.Nodes[i] = (*transportNodeImage)(node)
		}
	}
	if snapshot.Edges != nil {
		wire.Edges = make([]*transportEdgeImage, len(snapshot.Edges))
		for i, edge := range snapshot.Edges {
			wire.Edges[i] = transportEdgeFrom(edge)
		}
	}
	return enc.Encode(wire)
}

func (snapshot *ReplicationSnapshot) DecodeMsgpack(dec *msgpack.Decoder) error {
	var wire replicationSnapshotWire
	if err := dec.Decode(&wire); err != nil {
		return err
	}
	result := ReplicationSnapshot{WALPosition: wire.WALPosition}
	if wire.Nodes != nil {
		result.Nodes = make([]*Node, len(wire.Nodes))
		for i, node := range wire.Nodes {
			result.Nodes[i] = (*Node)(node)
		}
	}
	if wire.Edges != nil {
		result.Edges = make([]*Edge, len(wire.Edges))
		for i, edge := range wire.Edges {
			result.Edges[i] = edge.storedEdge()
		}
	}
	*snapshot = result
	return nil
}
