package storage

import (
	"context"
	"errors"
	"fmt"

	"github.com/orneryd/nornicdb/pkg/localization"
)

type EntityCommandKind uint8

const (
	EntityUnknown EntityCommandKind = iota
	EntityCreateNode
	EntityUpdateNode
	EntityDeleteNode
	EntityCreateEdge
	EntityDeleteEdge
	EntityUpdateEdge
)

// EntityTransactionVersion identifies the current semantic transaction format.
const EntityTransactionVersion uint16 = 2

// EntityTransactionEnvelope identifies a decoded semantic transaction.
type EntityTransactionEnvelope struct {
	Mode        EntityCommitMode
	Version     uint16
	Namespace   string
	LogicalTxID string
}

type entityWriter interface {
	CreateNode(ctx context.Context, node *Node) (NodeID, error)
	CreateEdge(ctx context.Context, edge *Edge) error
	UpdateEdge(ctx context.Context, updateRequest EdgeUpdateRequest) error
	DeleteEdge(ctx context.Context, id EdgeID) error
}

// DecodeEntityChange decodes one current direct replication command. It never
// imports numeric dictionary IDs.
func DecodeEntityChange(kind EntityCommandKind, data []byte) (Operation, error) {
	var op Operation
	switch kind {
	case EntityCreateNode, EntityUpdateNode:
		node, err := DecodeNodePayload(data)
		if err != nil {
			return op, localizedError(localization.ReplicationStorageDecodeNodeFailed(err), err)
		}
		op.Node = node
		op.NodeID = node.ID
		op.Type = OpCreateNode
		if kind == EntityUpdateNode {
			op.Type = OpUpdateNode
		}
	case EntityCreateEdge, EntityUpdateEdge:
		edge, err := DecodeEdgePayload(data)
		if err != nil {
			return op, localizedError(localization.ReplicationStorageDecodeEdgeFailed(err), err)
		}
		op.Edge = edge
		op.EdgeID = edge.ID
		op.Type = OpCreateEdge
		if kind == EntityUpdateEdge {
			op.Type = OpUpdateEdge
		}
	case EntityDeleteNode:
		op.Type = OpDeleteNode
		op.NodeID = NodeID(data)
	case EntityDeleteEdge:
		var req struct{ EdgeID string }
		if err := DecodeCommandPayload(data, &req); err != nil {
			return op, localizedError(localization.ReplicationStorageDecodeDeleteEdgeFailed(err), err)
		}
		op.Type = OpDeleteEdge
		op.EdgeID = EdgeID(req.EdgeID)
	default:
		return op, fmt.Errorf("not an entity operation: %d", kind)
	}
	return op, nil
}
func ApplyEntityChange(ctx context.Context, target entityWriter, op Operation, updateNode func(context.Context, NodeUpdateRequest) error, deleteNode func(context.Context, NodeID) error) error {
	switch op.Type {
	case OpCreateNode:
		_, err := target.CreateNode(ctx, op.Node)
		return err
	case OpUpdateNode:
		return updateNode(ctx, op.NodeUpdate())
	case OpDeleteNode:
		err := deleteNode(ctx, op.NodeID)
		if op.skipMissingDelete && errors.Is(err, ErrNotFound) {
			return nil
		}
		return err
	case OpCreateEdge:
		return target.CreateEdge(ctx, op.Edge)
	case OpUpdateEdge:
		return target.UpdateEdge(ctx, op.EdgeUpdate())
	case OpDeleteEdge:
		err := target.DeleteEdge(ctx, op.EdgeID)
		if op.skipMissingDelete && errors.Is(err, ErrNotFound) {
			return nil
		}
		return err
	default:
		return fmt.Errorf("not an entity operation: %s", op.Type)
	}
}

// EntityImagePolicy selects the interpretation of one transaction node update.
type EntityImagePolicy uint8

const (
	// ClientNodeEdits reconciles client edits with stored embedding metadata.
	ClientNodeEdits EntityImagePolicy = iota
	// StoredNodeImages applies a complete image normalized by its coordinator.
	StoredNodeImages
)

// BeginEntityTransaction obtains one native frame through the actual wrapper
// chain and binds its durability mode before exposing it to a caller.
func BeginEntityTransaction(ctx context.Context, engine Engine, mode EntityCommitMode) (*BadgerTransaction, error) {
	if !mode.valid() {
		return nil, ErrInvalidData
	}
	visited := map[Engine]bool{}
	for engine != nil && !visited[engine] {
		visited[engine] = true
		if starter, ok := engine.(interface {
			BeginTransaction(context.Context) (*BadgerTransaction, error)
		}); ok {
			tx, err := starter.BeginTransaction(ctx)
			if err != nil {
				return nil, err
			}
			tx.mu.Lock()
			tx.commitMode = mode
			tx.mu.Unlock()
			return tx, nil
		}
		wrapper, ok := engine.(EngineUnwrapper)
		if !ok {
			break
		}
		engine = wrapper.GetInnerEngine()
	}
	return nil, errors.New("entity transaction needs a native transactional engine")
}

// ApplyAcceptedEntityTransaction waits for the full accepted apply, including
// notifications. State-machine drivers use Submit and its physical boundary.
func ApplyAcceptedEntityTransaction(engine Engine, data []byte) error {
	envelope, operations, err := DecodeEntityTransaction(data)
	if err != nil {
		return err
	}
	receipt, err := SubmitAcceptedEntityTransaction(context.Background(), engine, envelope, operations, nil)
	if err != nil {
		return err
	}
	return receipt.Wait(context.Background())
}

// SubmitAcceptedEntityTransaction applies an already-decoded transaction and
// returns the commit receipt. DecodeEntityTransaction validates the wire image
// before callers publish it to replication or recovery logs.
func SubmitAcceptedEntityTransaction(ctx context.Context, engine Engine, envelope EntityTransactionEnvelope, ops []Operation, settle func(CommitOutcome)) (*CommitReceipt, error) {
	tx, err := BeginEntityTransaction(ctx, engine, envelope.Mode)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if err = tx.SetNamespace(envelope.Namespace); err != nil {
		return nil, err
	}
	if err = tx.SetValidationSetting(DeferredConstraintValidation, true); err != nil {
		return nil, err
	}
	prepared, err := tx.PrepareEntityCommit(ctx, EntityCommitRequest{
		LogicalID:  envelope.LogicalTxID,
		Operations: ops,
	})
	if err != nil {
		return nil, err
	}
	route, found := EntityCommitSubmitterFor(engine)
	if !found {
		prepared.Abort()
		return nil, ErrInvalidData
	}
	receipt, err := route.Submitter.SubmitEntityCommit(ctx, prepared, settle)
	if err != nil {
		prepared.Abort()
	}
	return receipt, err
}
