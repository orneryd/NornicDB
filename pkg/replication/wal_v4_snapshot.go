package replication

import (
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/orneryd/nornicdb/internal/gobcodec"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// v4GobSnapshotNode freezes the historical exported Node schema at the
// snapshot migration boundary. Runtime Go fields must not define old bytes.
type v4GobSnapshotNode struct {
	Database                   string
	ID                         storage.NodeID
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

type v4GobReplicationSnapshot struct {
	WALPosition uint64
	Nodes       []*v4GobSnapshotNode
	Edges       []*storage.Edge
}

// TranscodeV4ReplicationSnapshot converts a caller-owned snapshot written by
// the old replication adapter to the current MessagePack snapshot. The caller
// owns durable publication of the output and must retain the original until
// the converted snapshot has been verified. Current snapshot reads do not
// fall back to the historical gob format.
func TranscodeV4ReplicationSnapshot(source io.Reader, destination io.Writer) error {
	var historical v4GobReplicationSnapshot
	if err := gobcodec.Decode(source, &historical, gobcodec.RejectTrailing); err != nil {
		if errors.Is(err, gobcodec.ErrTrailing) {
			return fmt.Errorf("trailing historical replication snapshot data: %w", err)
		}
		return fmt.Errorf("decode historical replication snapshot: %w", err)
	}
	snapshot := storage.ReplicationSnapshot{WALPosition: historical.WALPosition, Edges: historical.Edges}
	if historical.Nodes != nil {
		snapshot.Nodes = make([]*storage.Node, len(historical.Nodes))
	}
	for i, image := range historical.Nodes {
		if image == nil {
			continue
		}
		node := &storage.Node{ID: image.ID, Labels: image.Labels, Properties: image.Properties,
			CreatedAt: image.CreatedAt, UpdatedAt: image.UpdatedAt, NamedEmbeddings: image.NamedEmbeddings,
			ChunkEmbeddings: image.ChunkEmbeddings, EmbedMeta: image.EmbedMeta}
		node.SetRuntimeMetadata(storage.NodeRuntimeMetadata{Database: image.Database,
			EmbeddingsStoredSeparately: image.EmbeddingsStoredSeparately, VisibilitySuppressed: image.VisibilitySuppressed})
		snapshot.Nodes[i] = node
	}
	data, err := storage.EncodeCommandPayload(snapshot)
	if err != nil {
		return fmt.Errorf("encode replication snapshot: %w", err)
	}
	written, err := destination.Write(data)
	if err != nil {
		return err
	}
	if written != len(data) {
		return io.ErrShortWrite
	}
	return nil
}
