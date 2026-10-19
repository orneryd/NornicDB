package replication

import (
	"context"
	"sync"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

var _ storage.ManagedWriteRouter = (*ReplicatedEngine)(nil)

// A consensus timeout can precede local apply. Retire the staged callback before
// its caller rolls back; later committed replay then uses the ordinary command
// path and registered accounting owner. An apply already running owns the
// callback through physical commit/publication, so retirement waits for it.
type managedCommandWrite struct {
	mu      sync.Mutex
	local   func() error
	applied bool
	err     error
}

func (m *managedCommandWrite) apply() (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.applied {
		return true, m.err
	}
	if m.local == nil {
		return false, nil
	}
	m.applied = true
	m.err = m.local()
	m.local = nil
	return true, m.err
}

func (m *managedCommandWrite) retire(err error) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.local = nil
	if err != nil && m.applied && m.err == nil && !storage.MutationApplied(err) {
		return &storage.AppliedMutationError{Cause: err}
	}
	return err
}

// CommitManagedWrite sends the ordinary direct-write command through the
// replicator. Local apply commits the transaction already admitted by the one
// accounting owner. Wire/WAL replay uses the ordinary engine write and the same
// registered stored-pair accounting owner, without serializing local closures.
func (e *ReplicatedEngine) CommitManagedWrite(ctx context.Context, ops []storage.Operation, bulk bool, local func() error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(ops) == 0 {
		return storage.ErrInvalidData
	}
	publication := &managedCommandWrite{local: local}
	cmd := &Command{Timestamp: time.Now(), managedWrite: publication}
	op := ops[0]
	var err error
	if bulk {
		payload := make([][]byte, len(ops))
		for i, item := range ops {
			if item.Type != op.Type {
				return storage.ErrInvalidData
			}
			switch item.Type {
			case storage.OpCreateNode:
				payload[i], err = encodeNodePayload(item.Node)
			case storage.OpCreateEdge:
				payload[i], err = encodeEdgePayload(item.Edge)
			default:
				return storage.ErrNotImplemented
			}
			if err != nil {
				return err
			}
		}
		cmd.Type = CmdBulkCreateNodes
		if op.Type == storage.OpCreateEdge {
			cmd.Type = CmdBulkCreateEdges
		}
		cmd.Data, err = encodeGob(payload)
	} else {
		switch op.Type {
		case storage.OpCreateNode:
			cmd.Type = CmdCreateNode
			cmd.Data, err = encodeNodePayload(op.Node)
		case storage.OpUpdateNode:
			cmd.Type = CmdUpdateNode
			cmd.Data, err = encodeNodePayload(op.Node)
		case storage.OpDeleteNode:
			cmd.Type, cmd.Data = CmdDeleteNode, []byte(op.NodeID)
		case storage.OpCreateEdge:
			cmd.Type = CmdCreateEdge
			cmd.Data, err = encodeEdgePayload(op.Edge)
		case storage.OpUpdateEdge:
			cmd.Type = CmdUpdateEdge
			cmd.Data, err = encodeEdgePayload(op.Edge)
		case storage.OpDeleteEdge:
			cmd.Type = CmdDeleteEdge
			cmd.Data, err = encodeGob(struct{ EdgeID string }{string(op.EdgeID)})
		default:
			return storage.ErrNotImplemented
		}
	}
	if err != nil {
		return err
	}
	// Dispatch through Apply so its leader rejection/forwarding and command
	// publication remain authoritative, exactly as ordinary direct CRUD.
	return publication.retire(e.replicator.Apply(cmd, e.timeout))
}
