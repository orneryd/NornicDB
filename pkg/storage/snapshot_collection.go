package storage

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"time"
)

const materializedSnapshotRecordSize int64 = 1<<32 - 1

type snapshotCollectionFields struct {
	Timestamp time.Time `json:"timestamp"`
	Version   string    `json:"version"`
}

type snapshotCompletion struct {
	Nodes      uint64                    `json:"nodes"`
	Edges      uint64                    `json:"edges"`
	Collection *snapshotCollectionFields `json:"collection,omitempty"`
}

func writeSnapshotHeader(writer *bufio.Writer, sequence uint64, timestamp time.Time) error {
	if _, err := writer.WriteString(streamSnapshotMagic); err != nil {
		return err
	}
	if err := writer.WriteByte(streamSnapshotVersion); err != nil {
		return err
	}
	if err := binary.Write(writer, binary.LittleEndian, sequence); err != nil {
		return err
	}
	return binary.Write(writer, binary.LittleEndian, timestamp.UnixNano())
}

// writeSnapshotRecords is the one snapshot lifecycle: it buffers writer, writes
// the stream header, and reuses one frame writer for the frames emit produces,
// counting the successful node and edge frames. The completion footer carries
// that count and, when fields supplies them, the materialized collection
// metadata. Collection is omitted otherwise, so a streaming footer is byte for
// byte the footer it always was. The flush writes buffered frames to the destination.
func writeSnapshotRecords(writer io.Writer, sequence uint64, timestamp time.Time, maxRecordBytes int64, fields *snapshotCollectionFields, emit func(write func(byte, any) error) error) error {
	buffered := bufio.NewWriter(writer)
	frames := snapshotFrameWriter{writer: buffered}
	if err := writeSnapshotHeader(buffered, sequence, timestamp); err != nil {
		return err
	}
	var nodeCount, edgeCount uint64
	if err := emit(func(frameType byte, value any) error {
		if err := frames.write(frameType, value, maxRecordBytes); err != nil {
			return err
		}
		switch frameType {
		case streamSnapshotNode:
			nodeCount++
		case streamSnapshotEdge:
			edgeCount++
		}
		return nil
	}); err != nil {
		return err
	}
	footer := snapshotCompletion{Nodes: nodeCount, Edges: edgeCount, Collection: fields}
	if err := frames.write(streamSnapshotFooter, footer, maxRecordBytes); err != nil {
		return err
	}
	return buffered.Flush()
}

// writeSnapshotCollection uses the framed transport for materialized backups
// too, so old binaries reject the new format rather than silently dropping
// fields. The exact collection metadata survives in the completion frame.
func writeSnapshotCollection(writer io.Writer, snapshot *Snapshot) error {
	if snapshot == nil {
		return fmt.Errorf("nil snapshot")
	}
	timestamp := snapshot.Timestamp
	if timestamp.IsZero() {
		timestamp = time.Now()
	}
	fields := &snapshotCollectionFields{Timestamp: snapshot.Timestamp, Version: snapshot.Version}
	return writeSnapshotRecords(writer, snapshot.Sequence, timestamp, materializedSnapshotRecordSize, fields,
		func(write func(byte, any) error) error {
			for _, node := range snapshot.Nodes {
				if node == nil {
					return fmt.Errorf("snapshot contains nil node")
				}
				if err := write(streamSnapshotNode, node); err != nil {
					return err
				}
			}
			for _, edge := range snapshot.Edges {
				if edge == nil {
					return fmt.Errorf("snapshot contains nil edge")
				}
				if err := write(streamSnapshotEdge, edge); err != nil {
					return err
				}
			}
			return nil
		})
}

type snapshotCollectionVisitor struct{ snapshot *Snapshot }

func (v snapshotCollectionVisitor) VisitNode(node *Node) error {
	v.snapshot.Nodes = append(v.snapshot.Nodes, node)
	return nil
}
func (v snapshotCollectionVisitor) VisitEdge(edge *Edge) error {
	v.snapshot.Edges = append(v.snapshot.Edges, edge)
	return nil
}

// Legacy materialized snapshots also accepted bare IDs. Keep that routing
// when SaveSnapshot now writes the framed transport, without changing Load's
// representation or already-qualified IDs from streaming engines.
func qualifyRecoverySnapshotNode(node *Node) *Node {
	if node == nil || node.ID == "" {
		return node
	}
	if _, _, ok := ParseDatabasePrefix(string(node.ID)); ok {
		return node
	}
	result := *node
	result.ID = NodeID(defaultRecoveryDatabase() + ":" + string(node.ID))
	return &result
}
func qualifyRecoverySnapshotEdge(edge *Edge) *Edge {
	if edge == nil {
		return nil
	}
	namespace := defaultRecoveryDatabase()
	if db, _, ok := ParseDatabasePrefix(string(edge.ID)); ok {
		namespace = db
	} else if db, _, ok := ParseDatabasePrefix(string(edge.StartNode)); ok {
		namespace = db
	}
	result := *edge
	if _, _, ok := ParseDatabasePrefix(string(result.ID)); result.ID != "" && !ok {
		result.ID = EdgeID(namespace + ":" + string(result.ID))
	}
	if _, _, ok := ParseDatabasePrefix(string(result.StartNode)); result.StartNode != "" && !ok {
		result.StartNode = NodeID(namespace + ":" + string(result.StartNode))
	}
	if _, _, ok := ParseDatabasePrefix(string(result.EndNode)); result.EndNode != "" && !ok {
		result.EndNode = NodeID(namespace + ":" + string(result.EndNode))
	}
	return &result
}
