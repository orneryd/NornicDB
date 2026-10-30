package storage

import (
	"bytes"
	"time"

	"github.com/orneryd/nornicdb/internal/gobcodec"
)

// storedNodeGob is the historical exported Node field contract consumed only
// by the existing storage migration reader. Runtime identity is never decoded.
// The current writer remains MessagePack; this schema does not create a new
// migration arm or change any old database payload.
type storedNodeGob struct {
	Database                   string
	ID                         NodeID
	Labels                     []string
	Properties                 map[string]any
	CreatedAt                  time.Time
	UpdatedAt                  time.Time
	NamedEmbeddings            map[string][]float32
	ChunkEmbeddings            [][]float32
	EmbedMeta                  map[string]any
	EmbeddingsStoredSeparately bool
	VisibilitySuppressed       bool
}

func decodeStoredGob(data []byte, value any) error {
	node, ok := value.(*Node)
	if !ok {
		return gobcodec.Decode(bytes.NewReader(data), value, gobcodec.AllowTrailing)
	}
	metadata := node.RuntimeMetadata()
	image := storedNodeGob{Database: metadata.Database, ID: node.ID, Labels: node.Labels,
		Properties: node.Properties, CreatedAt: node.CreatedAt, UpdatedAt: node.UpdatedAt,
		NamedEmbeddings: node.NamedEmbeddings, ChunkEmbeddings: node.ChunkEmbeddings, EmbedMeta: node.EmbedMeta,
		EmbeddingsStoredSeparately: metadata.EmbeddingsStoredSeparately, VisibilitySuppressed: metadata.VisibilitySuppressed}
	err := gobcodec.Decode(bytes.NewReader(data), &image, gobcodec.AllowTrailing)
	node.ID, node.Labels, node.Properties = image.ID, image.Labels, image.Properties
	node.CreatedAt, node.UpdatedAt = image.CreatedAt, image.UpdatedAt
	node.NamedEmbeddings, node.ChunkEmbeddings, node.EmbedMeta = image.NamedEmbeddings, image.ChunkEmbeddings, image.EmbedMeta
	node.SetRuntimeMetadata(NodeRuntimeMetadata{Database: image.Database, EmbeddingsStoredSeparately: image.EmbeddingsStoredSeparately, VisibilitySuppressed: image.VisibilitySuppressed})
	return err
}
