package storage

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

var ErrStreamingSnapshotUnsupported = errors.New("streaming snapshots require storage.StreamingEngine")

const (
	streamSnapshotMagic       = "NDS2"
	streamSnapshotVersion     = byte(1)
	streamSnapshotNode        = byte(1)
	streamSnapshotEdge        = byte(2)
	streamSnapshotFooter      = byte(0xff)
	defaultSnapshotRecordSize = 16 << 20
)

type SnapshotOptions struct {
	Sequence       uint64
	MaxRecordBytes int64
}

type SnapshotMetadata struct {
	Sequence  uint64
	Timestamp time.Time
	NodeCount uint64
	EdgeCount uint64
}

type SnapshotVisitor interface {
	VisitNode(*Node) error
	VisitEdge(*Edge) error
}

func WriteSnapshot(ctx context.Context, engine Engine, writer io.Writer, options SnapshotOptions) error {
	streamer, ok := engine.(StreamingEngine)
	if !ok {
		return ErrStreamingSnapshotUnsupported
	}
	maxRecordBytes := options.MaxRecordBytes
	if maxRecordBytes == 0 {
		maxRecordBytes = defaultSnapshotRecordSize
	}
	if maxRecordBytes < 0 {
		return fmt.Errorf("snapshot max record bytes must be nonnegative")
	}

	buffered := bufio.NewWriter(writer)
	if _, err := buffered.WriteString(streamSnapshotMagic); err != nil {
		return err
	}
	if err := buffered.WriteByte(streamSnapshotVersion); err != nil {
		return err
	}
	if err := binary.Write(buffered, binary.LittleEndian, options.Sequence); err != nil {
		return err
	}
	if err := binary.Write(buffered, binary.LittleEndian, time.Now().UnixNano()); err != nil {
		return err
	}

	var nodeCount uint64
	if err := streamer.StreamNodes(ctx, func(node *Node) error {
		if err := writeSnapshotFrame(buffered, streamSnapshotNode, node, maxRecordBytes); err != nil {
			return err
		}
		nodeCount++
		return nil
	}); err != nil {
		return fmt.Errorf("stream snapshot nodes: %w", err)
	}
	var edgeCount uint64
	if err := streamer.StreamEdges(ctx, func(edge *Edge) error {
		if err := writeSnapshotFrame(buffered, streamSnapshotEdge, edge, maxRecordBytes); err != nil {
			return err
		}
		edgeCount++
		return nil
	}); err != nil {
		return fmt.Errorf("stream snapshot edges: %w", err)
	}
	footer := struct {
		Nodes uint64 `json:"nodes"`
		Edges uint64 `json:"edges"`
	}{Nodes: nodeCount, Edges: edgeCount}
	if err := writeSnapshotFrame(buffered, streamSnapshotFooter, footer, maxRecordBytes); err != nil {
		return err
	}
	return buffered.Flush()
}

// SaveStreamingSnapshot writes a framed snapshot using temp-file, sync, and
// atomic-rename durability semantics.
func SaveStreamingSnapshot(ctx context.Context, engine Engine, path string, options SnapshotOptions) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("wal: failed to create snapshot directory: %w", err)
	}
	tmpPath := path + ".tmp"
	file, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("wal: failed to create snapshot file: %w", err)
	}
	removeTemp := true
	defer func() {
		_ = file.Close()
		if removeTemp {
			_ = os.Remove(tmpPath)
		}
	}()
	if err := WriteSnapshot(ctx, engine, file, options); err != nil {
		return fmt.Errorf("wal: failed to encode snapshot: %w", err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("wal: failed to sync snapshot: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("wal: failed to close snapshot: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("wal: failed to rename snapshot: %w", err)
	}
	removeTemp = false
	if err := syncDir(dir); err != nil {
		return nil
	}
	return nil
}

func IsStreamingSnapshot(path string) (bool, error) {
	file, err := os.Open(path)
	if err != nil {
		return false, err
	}
	defer file.Close()
	header := make([]byte, len(streamSnapshotMagic))
	if _, err := io.ReadFull(file, header); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return false, nil
		}
		return false, err
	}
	return string(header) == streamSnapshotMagic, nil
}

func ReadSnapshot(reader io.Reader, visitor SnapshotVisitor) (SnapshotMetadata, error) {
	var metadata SnapshotMetadata
	buffered := bufio.NewReader(reader)
	magic := make([]byte, len(streamSnapshotMagic))
	if _, err := io.ReadFull(buffered, magic); err != nil || string(magic) != streamSnapshotMagic {
		return metadata, fmt.Errorf("invalid streaming snapshot header")
	}
	version, err := buffered.ReadByte()
	if err != nil || version != streamSnapshotVersion {
		return metadata, fmt.Errorf("unsupported streaming snapshot version")
	}
	var timestamp int64
	if err := binary.Read(buffered, binary.LittleEndian, &metadata.Sequence); err != nil {
		return metadata, err
	}
	if err := binary.Read(buffered, binary.LittleEndian, &timestamp); err != nil {
		return metadata, err
	}
	metadata.Timestamp = time.Unix(0, timestamp)

	for {
		frameType, payload, err := readSnapshotFrame(buffered)
		if err != nil {
			return metadata, err
		}
		switch frameType {
		case streamSnapshotNode:
			var node Node
			if err := json.Unmarshal(payload, &node); err != nil {
				return metadata, err
			}
			if err := visitor.VisitNode(&node); err != nil {
				return metadata, err
			}
			metadata.NodeCount++
		case streamSnapshotEdge:
			var edge Edge
			if err := json.Unmarshal(payload, &edge); err != nil {
				return metadata, err
			}
			if err := visitor.VisitEdge(&edge); err != nil {
				return metadata, err
			}
			metadata.EdgeCount++
		case streamSnapshotFooter:
			var footer struct {
				Nodes uint64 `json:"nodes"`
				Edges uint64 `json:"edges"`
			}
			if err := json.Unmarshal(payload, &footer); err != nil {
				return metadata, err
			}
			if footer.Nodes != metadata.NodeCount || footer.Edges != metadata.EdgeCount {
				return metadata, fmt.Errorf("streaming snapshot footer count mismatch")
			}
			if _, err := buffered.Peek(1); err != io.EOF {
				return metadata, fmt.Errorf("trailing data after streaming snapshot footer")
			}
			return metadata, nil
		default:
			return metadata, fmt.Errorf("unknown streaming snapshot frame type %d", frameType)
		}
	}
}

func writeSnapshotFrame(writer io.Writer, frameType byte, value any, maxRecordBytes int64) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if int64(len(payload)) > maxRecordBytes {
		return fmt.Errorf("snapshot record is %d bytes, exceeds limit %d", len(payload), maxRecordBytes)
	}
	if _, err := writer.Write([]byte{frameType}); err != nil {
		return err
	}
	if err := binary.Write(writer, binary.LittleEndian, uint32(len(payload))); err != nil {
		return err
	}
	if _, err := writer.Write(payload); err != nil {
		return err
	}
	return binary.Write(writer, binary.LittleEndian, crc32Checksum(payload))
}

func readSnapshotFrame(reader io.Reader) (byte, []byte, error) {
	var frameType [1]byte
	if _, err := io.ReadFull(reader, frameType[:]); err != nil {
		return 0, nil, fmt.Errorf("streaming snapshot missing completion footer: %w", err)
	}
	var length uint32
	if err := binary.Read(reader, binary.LittleEndian, &length); err != nil {
		return 0, nil, err
	}
	if length > defaultSnapshotRecordSize {
		return 0, nil, fmt.Errorf("streaming snapshot record exceeds reader limit")
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(reader, payload); err != nil {
		return 0, nil, err
	}
	var expectedCRC uint32
	if err := binary.Read(reader, binary.LittleEndian, &expectedCRC); err != nil {
		return 0, nil, err
	}
	if crc32Checksum(payload) != expectedCRC {
		return 0, nil, fmt.Errorf("streaming snapshot checksum mismatch")
	}
	return frameType[0], payload, nil
}
