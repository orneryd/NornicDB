package storage

import (
	"context"
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
// a time. Physical bodies and name authority are retained one record at a time.
// The destination must be quiescent until recovery completes; it must not serve
// concurrent application writers while bodies and derived counts are restored.

func RecoverIntoEngine(ctx context.Context, destination Engine, walDir, snapshotPath string) (ReplayResult, RecoveryStreamStatus, error) {
	ctx = context.WithoutCancel(ctx)
	result := ReplayResult{Errors: make([]ReplayError, 0)}
	status := RecoveryStreamStatus{}
	if err := ValidateWALFormat(walDir, WALValidateCurrent); err != nil {
		return result, status, err
	}
	var snapshotSeq uint64
	if err := markRecoveryPropertyForeign(destination); err != nil {
		return result, status, err
	}

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
			if err := beginRecoveryPropertyRestore(destination); err != nil {
				file.Close()
				return result, status, err
			}
			visitor := newRecoverySnapshotVisitor(destination, recoveryBatchSize)
			metadata, readErr := readSnapshot(ctx, file, visitor, nil, materializedSnapshotRecordSize)
			if readErr == nil {
				readErr = visitor.Flush(ctx)
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
			snapshot, err := LoadSnapshot(ctx, snapshotPath)
			if err != nil {
				if !os.IsNotExist(err) {
					return result, status, fmt.Errorf("wal: failed to load snapshot: %w", err)
				}
			} else {
				if err := beginRecoveryPropertyRestore(destination); err != nil {
					return result, status, err
				}
				snapshotSeq = snapshot.Sequence
				status.SnapshotNodes = uint64(len(snapshot.Nodes))
				status.SnapshotEdges = uint64(len(snapshot.Edges))
				if err := restoreLegacySnapshot(ctx, destination, snapshot); err != nil {
					return result, status, err
				}
			}
		}
		// Snapshot batches may only be acknowledged into a write-behind
		// generation. Complete that phase before physical WAL replay: otherwise
		// its older overlay masks replayed bodies and a later drain overwrites
		// them. Recovery owns completion independently of a client request.
		if flusher, ok := UnwrapEngine(destination).(interface{ FlushWriteBehind(context.Context) error }); ok {
			if err := flusher.FlushWriteBehind(ctx); err != nil {
				return result, status, fmt.Errorf("wal: failed to complete snapshot restore: %w", err)
			}
		}
	}

	activePath := walActivePath(walDir)
	if _, _, err := repairWALTailIfNeeded(activePath, defaultWALLogger{}); err != nil {
		return result, status, fmt.Errorf("wal: failed to inspect recovery log: %w", err)
	}
	err := VisitWALEntriesAfterFromDir(ctx, walDir, snapshotSeq, func(entry WALEntry) error {
		status.WALEntries++
		if entry.Operation == OpCheckpoint {
			result.Skipped++
			return nil
		}
		if err := replayWALEntry(ctx, destination, entry); err == nil {
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
		err = fmt.Errorf("wal: failed to read WAL: %w", err)
	}
	if result.Applied > 0 {
		if native, ok := UnwrapEngine(destination).(*BadgerEngine); ok {
			err = native.completeRecoveredCounts(ctx, err)
		}
	}
	return result, status, err
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

func (v *recoverySnapshotVisitor) VisitNode(ctx context.Context, node *Node) error {
	node = qualifyRecoverySnapshotNode(node)
	if len(v.nodes) > 0 {
		previous, _, _ := ParseDatabasePrefix(string(v.nodes[0].ID))
		current, _, _ := ParseDatabasePrefix(string(node.ID))
		if previous != current {
			if err := v.flushNodes(ctx); err != nil {
				return err
			}
		}
	}
	v.nodes = append(v.nodes, node)
	if len(v.nodes) < v.batchSize {
		return nil
	}
	return v.flushNodes(ctx)
}

func (v *recoverySnapshotVisitor) VisitEdge(ctx context.Context, edge *Edge) error {
	edge = qualifyRecoverySnapshotEdge(edge)
	if err := v.flushNodes(ctx); err != nil {
		return err
	}
	if len(v.edges) > 0 {
		previous, _, _ := ParseDatabasePrefix(string(v.edges[0].ID))
		current, _, _ := ParseDatabasePrefix(string(edge.ID))
		if previous != current {
			if err := v.flushEdges(ctx); err != nil {
				return err
			}
		}
	}
	v.edges = append(v.edges, edge)
	if len(v.edges) < v.batchSize {
		return nil
	}
	return v.flushEdges(ctx)
}

func (v *recoverySnapshotVisitor) Flush(ctx context.Context) error {
	if err := v.flushNodes(ctx); err != nil {
		return err
	}
	return v.flushEdges(ctx)
}

func (v *recoverySnapshotVisitor) flushNodes(ctx context.Context) error {
	if len(v.nodes) == 0 {
		return nil
	}
	if err := v.engine.BulkCreateNodes(ctx, v.nodes); err != nil {
		return fmt.Errorf("wal: failed to restore nodes: %w", err)
	}
	clear(v.nodes)
	v.nodes = v.nodes[:0]
	return nil
}

func (v *recoverySnapshotVisitor) flushEdges(ctx context.Context) error {
	if len(v.edges) == 0 {
		return nil
	}
	if err := v.engine.BulkCreateEdges(ctx, v.edges); err != nil {
		return fmt.Errorf("wal: failed to restore edges: %w", err)
	}
	clear(v.edges)
	v.edges = v.edges[:0]
	return nil
}

func restoreLegacySnapshot(ctx context.Context, destination Engine, snapshot *Snapshot) error {
	ctx = context.WithoutCancel(ctx)
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
	if err := namespaced.BulkCreateNodes(ctx, snapshot.Nodes); err != nil {
		return fmt.Errorf("wal: failed to restore nodes: %w", err)
	}
	if err := namespaced.BulkCreateEdges(ctx, snapshot.Edges); err != nil {
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
