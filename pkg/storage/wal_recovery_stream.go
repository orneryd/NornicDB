package storage

import (
	"errors"
	"fmt"
	"os"

	"github.com/orneryd/nornicdb/pkg/config"
)

// RecoveryStreamStatus describes whether recovery used bounded snapshot and
// WAL paths. Legacy JSON snapshots require compatibility materialization.
type RecoveryStreamStatus struct {
	SnapshotStreaming bool
	SnapshotNodes     uint64
	SnapshotEdges     uint64
	WALEntries        uint64
}

const recoveryBatchSize = 1000

// RecoverIntoEngine restores a snapshot and subsequent WAL entries directly
// into destination. Framed snapshots and WAL files are processed one record at
// a time, so retained recovery memory is bounded by the largest record.
func RecoverIntoEngine(destination Engine, walDir, snapshotPath string) (ReplayResult, RecoveryStreamStatus, error) {
	result := ReplayResult{Errors: make([]ReplayError, 0)}
	status := RecoveryStreamStatus{}
	var snapshotSeq uint64

	if snapshotPath != "" {
		streaming, err := IsStreamingSnapshot(snapshotPath)
		if err != nil && !os.IsNotExist(err) {
			return result, status, fmt.Errorf("wal: failed to inspect snapshot: %w", err)
		}
		status.SnapshotStreaming = streaming
		if streaming {
			file, err := os.Open(snapshotPath)
			if err != nil {
				return result, status, fmt.Errorf("wal: failed to open snapshot: %w", err)
			}
			visitor := newRecoverySnapshotVisitor(destination, recoveryBatchSize)
			metadata, readErr := ReadSnapshot(file, visitor)
			if readErr == nil {
				readErr = visitor.Flush()
			}
			closeErr := file.Close()
			if readErr != nil {
				return result, status, fmt.Errorf("wal: failed to load snapshot: %w", readErr)
			}
			if closeErr != nil {
				return result, status, fmt.Errorf("wal: failed to close snapshot: %w", closeErr)
			}
			snapshotSeq = metadata.Sequence
			status.SnapshotNodes = metadata.NodeCount
			status.SnapshotEdges = metadata.EdgeCount
		} else {
			snapshot, err := LoadSnapshot(snapshotPath)
			if err != nil {
				if !os.IsNotExist(err) {
					return result, status, fmt.Errorf("wal: failed to load snapshot: %w", err)
				}
			} else {
				snapshotSeq = snapshot.Sequence
				status.SnapshotNodes = uint64(len(snapshot.Nodes))
				status.SnapshotEdges = uint64(len(snapshot.Edges))
				if err := restoreLegacySnapshot(destination, snapshot); err != nil {
					return result, status, err
				}
			}
		}
	}

	activePath := walActivePath(walDir)
	_, _, _ = repairWALTailIfNeeded(activePath, defaultWALLogger{})
	err := VisitWALEntriesAfterFromDir(walDir, snapshotSeq, func(entry WALEntry) error {
		status.WALEntries++
		if entry.Operation == OpCheckpoint {
			result.Skipped++
			return nil
		}
		if err := ReplayWALEntry(destination, entry); err == nil {
			result.Applied++
		} else if errors.Is(err, ErrAlreadyExists) {
			result.Skipped++
		} else {
			result.Failed++
			result.Errors = append(result.Errors, ReplayError{Sequence: entry.Sequence, Operation: entry.Operation, Error: err})
		}
		return nil
	})
	if err != nil {
		return result, status, fmt.Errorf("wal: failed to read WAL: %w", err)
	}
	return result, status, nil
}

type recoverySnapshotVisitor struct {
	engine    Engine
	batchSize int
	nodes     []*Node
	edges     []*Edge
}

func newRecoverySnapshotVisitor(engine Engine, batchSize int) *recoverySnapshotVisitor {
	return &recoverySnapshotVisitor{
		engine:    engine,
		batchSize: batchSize,
		nodes:     make([]*Node, 0, batchSize),
		edges:     make([]*Edge, 0, batchSize),
	}
}

func (v *recoverySnapshotVisitor) VisitNode(node *Node) error {
	v.nodes = append(v.nodes, node)
	if len(v.nodes) < v.batchSize {
		return nil
	}
	return v.flushNodes()
}

func (v *recoverySnapshotVisitor) VisitEdge(edge *Edge) error {
	if err := v.flushNodes(); err != nil {
		return err
	}
	v.edges = append(v.edges, edge)
	if len(v.edges) < v.batchSize {
		return nil
	}
	return v.flushEdges()
}

func (v *recoverySnapshotVisitor) Flush() error {
	if err := v.flushNodes(); err != nil {
		return err
	}
	return v.flushEdges()
}

func (v *recoverySnapshotVisitor) flushNodes() error {
	if len(v.nodes) == 0 {
		return nil
	}
	if err := BulkCreateNodesForRecovery(v.engine, v.nodes); err != nil {
		return err
	}
	v.nodes = v.nodes[:0]
	return nil
}

func (v *recoverySnapshotVisitor) flushEdges() error {
	if len(v.edges) == 0 {
		return nil
	}
	if err := BulkCreateEdgesForRecovery(v.engine, v.edges); err != nil {
		return err
	}
	v.edges = v.edges[:0]
	return nil
}

func restoreLegacySnapshot(destination Engine, snapshot *Snapshot) error {
	dbName := defaultRecoveryDatabase()
	if len(snapshot.Nodes) > 0 {
		if parsed, _, ok := ParseDatabasePrefix(string(snapshot.Nodes[0].ID)); ok {
			dbName = parsed
		}
	} else if len(snapshot.Edges) > 0 {
		if parsed, _, ok := ParseDatabasePrefix(string(snapshot.Edges[0].ID)); ok {
			dbName = parsed
		}
	}
	for _, node := range snapshot.Nodes {
		node.ID = NodeID(StripDatabasePrefix(dbName, string(node.ID)))
	}
	for _, edge := range snapshot.Edges {
		edge.ID = EdgeID(StripDatabasePrefix(dbName, string(edge.ID)))
		edge.StartNode = NodeID(StripDatabasePrefix(dbName, string(edge.StartNode)))
		edge.EndNode = NodeID(StripDatabasePrefix(dbName, string(edge.EndNode)))
	}
	namespaced := NewNamespacedEngine(destination, dbName)
	if err := BulkCreateNodesForRecovery(namespaced, snapshot.Nodes); err != nil {
		return fmt.Errorf("wal: failed to restore nodes: %w", err)
	}
	if err := BulkCreateEdgesForRecovery(namespaced, snapshot.Edges); err != nil {
		return fmt.Errorf("wal: failed to restore edges: %w", err)
	}
	return nil
}

func defaultRecoveryDatabase() string {
	globalConfig := config.LoadFromEnv()
	if globalConfig.Database.DefaultDatabase != "" {
		return globalConfig.Database.DefaultDatabase
	}
	return "nornic"
}
