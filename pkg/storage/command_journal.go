package storage

import (
	"context"
	"github.com/google/uuid"
	"sync"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
)

// commandJournal separates an attempted write from its durable outcome. Only
// ordered successful publication writes the commit marker. Rejection and an
// interrupted attempt can never be interpreted as successful standalone redo.
type commandJournal struct {
	wal           *WAL
	namespace, id string
	operations    int
	once          sync.Once
	committed     bool
	err           error
}

func (j *commandJournal) publish() {
	if j == nil {
		return
	}
	j.once.Do(func() {
		j.committed = true
		_, j.err = j.wal.AppendTxCommit(j.namespace, j.id, j.operations)
	})
}

func (j *commandJournal) abort() {
	if j == nil {
		return
	}
	j.once.Do(func() { _, _ = j.wal.AppendTxAbort(j.namespace, j.id, "write rejected before publication") })
}

func (j *commandJournal) result(err error) error {
	if j != nil && j.committed && j.err != nil {
		return &AppliedMutationError{Cause: j.err}
	}
	return err
}

func (w *WALEngine) prepareCommandJournal(namespace string, ops []Operation, bulk bool, command ...[16]byte) (*commandJournal, func(), error) {
	ids := make([]string, 0, len(ops))
	for _, op := range ops {
		id := string(op.NodeID)
		if op.Node != nil {
			id = string(op.Node.ID)
		}
		if op.Edge != nil {
			id = string(op.Edge.ID)
		}
		if id == "" {
			id = string(op.EdgeID)
		}
		ids = append(ids, id)
	}
	release, err := w.admitJournalIDs(ids...)
	if err != nil {
		return nil, nil, err
	}
	w.mutationMu.RLock()
	cleanup := func() { w.mutationMu.RUnlock(); release() }
	if !config.IsWALEnabled() {
		return nil, cleanup, nil
	}
	j := &commandJournal{wal: w.wal, namespace: namespace, id: "atomic-" + generateTxID(), operations: len(ops)}
	finish := func() { j.abort(); cleanup() }
	metadata := map[string]string{"publication": "atomic"}
	if len(command) > 0 && command[0] != ([16]byte{}) {
		metadata["replication_id"] = uuid.UUID(command[0]).String()
	}
	if _, err := w.wal.AppendTxBegin(namespace, j.id, metadata); err != nil {
		cleanup()
		return nil, nil, err
	}
	appendOp := func(op Operation) error {
		// A committed physical replacement is an update during redo. Its
		// CREATE acceptance depended on live visibility, which recovery
		// deliberately does not rerun and may not have restored yet.
		if op.Type == OpCreateEdge && op.ReplacesExisting {
			op.Type = OpUpdateEdge
		}
		var data any
		switch op.Type {
		case OpCreateNode, OpUpdateNode:
			data = WALNodeData{Node: cloneNodeForWAL(namespace, op.Node), TxID: j.id}
		case OpCreateEdge, OpUpdateEdge:
			data = WALEdgeData{Edge: cloneEdgeForWAL(namespace, op.Edge), TxID: j.id}
		case OpDeleteNode:
			data = WALDeleteData{ID: StripDatabasePrefix(namespace, string(op.NodeID)), TxID: j.id}
		case OpDeleteEdge:
			data = WALDeleteData{ID: StripDatabasePrefix(namespace, string(op.EdgeID)), TxID: j.id}
		case OpSchemaDefinition:
			data = WALSchemaData{Schema: op.Schema, SchemaTransaction: op.SchemaTransaction, TxID: j.id}
		default:
			return ErrNotImplemented
		}
		return w.wal.AppendWithDatabase(op.Type, data, namespace)
	}
	for _, op := range ops {
		if op.ReplacesExisting {
			bulk = false
			break
		}
	}
	if bulk && len(ops) > 0 {
		var data any
		typ := OpBulkNodes
		if ops[0].Type == OpCreateNode {
			nodes := make([]*Node, len(ops))
			for i, op := range ops {
				nodes[i] = cloneNodeForWAL(namespace, op.Node)
			}
			data = WALBulkNodesData{Nodes: nodes, TxID: j.id}
		} else {
			edges := make([]*Edge, len(ops))
			for i, op := range ops {
				edges[i] = cloneEdgeForWAL(namespace, op.Edge)
			}
			typ, data = OpBulkEdges, WALBulkEdgesData{Edges: edges, TxID: j.id}
		}
		err = w.wal.AppendWithDatabase(typ, data, namespace)
	} else {
		for _, op := range ops {
			if err = appendOp(op); err != nil {
				break
			}
		}
	}
	if err != nil {
		finish()
		return nil, nil, err
	}
	return j, finish, nil
}

func (w *WALEngine) SupportsNativeCommand() bool { return SupportsNativeCommand(w.engine) }

func (w *WALEngine) NativeCommand(ctx context.Context, op Operation, published func()) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	switch op.Type {
	case OpCreateNode:
		if op.Node != nil {
			initializeCreationTimes(&op.Node.CreatedAt, &op.Node.UpdatedAt)
		}
	case OpCreateEdge:
		if op.Edge != nil {
			initializeCreationTimes(&op.Edge.CreatedAt, &op.Edge.UpdatedAt)
		}
	case OpUpdateNode:
		if op.Node != nil && op.Node.UpdatedAt.IsZero() {
			op.Node.UpdatedAt = time.Now()
		}
	}
	namespace := w.databaseFromNode(op.Node)
	if op.Edge != nil {
		var err error
		namespace, err = w.databaseFromEdge(op.Edge)
		if err != nil {
			return err
		}
	} else if op.Node == nil {
		id := string(op.NodeID)
		if id == "" {
			id = string(op.EdgeID)
		}
		if ns, _, ok := ParseDatabasePrefix(id); ok {
			namespace = ns
		}
	}
	j, release, err := w.prepareCommandJournal(namespace, []Operation{op}, false, CommandVersionFromContext(ctx))
	if err != nil {
		return err
	}
	defer release()
	err = ExecuteNativeCommand(ctx, w.engine, op, func() {
		j.publish()
		if published != nil {
			published()
		}
	})
	return j.result(err)
}
