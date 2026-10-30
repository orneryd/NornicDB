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

// EntityTransactionEnvelope identifies a decoded semantic transaction.
type EntityTransactionEnvelope struct {
	Mode        EntityCommitMode
	Version     uint16
	Namespace   string
	LogicalTxID string
}

type entityWriter interface {
	CreateNode(*Node) (NodeID, error)
	CreateEdge(*Edge) error
	UpdateEdge(*Edge) error
	DeleteEdge(EdgeID) error
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
func ApplyEntityChange(target entityWriter, op Operation, updateNode func(*Node) error, deleteNode func(NodeID) error) error {
	switch op.Type {
	case OpCreateNode:
		_, err := target.CreateNode(op.Node)
		return err
	case OpUpdateNode:
		return updateNode(op.Node)
	case OpDeleteNode:
		return deleteNode(op.NodeID)
	case OpCreateEdge:
		return target.CreateEdge(op.Edge)
	case OpUpdateEdge:
		return target.UpdateEdge(op.Edge)
	case OpDeleteEdge:
		return target.DeleteEdge(op.EdgeID)
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
func BeginEntityTransaction(engine Engine, mode EntityCommitMode) (*BadgerTransaction, error) {
	if !mode.valid() {
		return nil, ErrInvalidData
	}
	visited := map[Engine]bool{}
	for engine != nil && !visited[engine] {
		visited[engine] = true
		if starter, ok := engine.(interface {
			BeginTransaction() (*BadgerTransaction, error)
		}); ok {
			tx, err := starter.BeginTransaction()
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
	return receipt.WaitOutcome(context.Background()).Err
}

// SubmitAcceptedEntityTransaction applies an already-decoded durable command.
// The existing native owner settles it without accepting another log entry.
// Author: NOR-288 lane (Codex), 2026-10-29; preserves the merged decoded API.
func SubmitAcceptedEntityTransaction(ctx context.Context, engine Engine, envelope EntityTransactionEnvelope, ops []Operation, settle func(CommitOutcome)) (*CommitReceipt, error) {
	prepared, existing, err := prepareAcceptedEntityApplication(ctx, engine, envelope, ops, "")
	if err != nil {
		return nil, AcceptedOutcome(CommitRecoveryPending, envelope.LogicalTxID, err)
	}
	if existing != nil {
		existing.OnSettled(settle)
		return existing, nil
	}
	return prepared.engine.SubmitEntityCommit(context.Background(), prepared, settle)
}

// prepareAcceptedEntityApplication materializes the existing semantic command
// through the native staging owner, borrowing retained entity authority during
// an in-process retry. Empty authority means a cold replica/recovery application.
func prepareAcceptedEntityApplication(ctx context.Context, engine Engine, envelope EntityTransactionEnvelope, ops []Operation, authority string) (*PreparedEntityCommit, *CommitReceipt, error) {
	tx, err := BeginEntityTransaction(engine, envelope.Mode)
	if err != nil {
		return nil, nil, err
	}
	defer tx.Rollback()
	if authority == "" {
		tx.engine.mu.RLock()
		application := tx.engine.acceptedApplications[acceptedApplicationKey{envelope.Namespace, envelope.LogicalTxID}]
		tx.engine.mu.RUnlock()
		if application != nil {
			application.start()
			return nil, application.receipt, nil
		}
	} else {
		tx.ID = authority
	}
	tx.acceptedApplication = true
	if err = tx.SetNamespace(envelope.Namespace); err != nil {
		return nil, nil, err
	}
	if err = tx.SetValidationSetting(DeferredConstraintValidation, true); err != nil {
		return nil, nil, err
	}
	prepared, err := tx.PrepareEntityCommit(ctx, EntityCommitRequest{
		LogicalID:  envelope.LogicalTxID,
		Operations: ops,
	})
	if err != nil {
		return nil, nil, err
	}
	if authority == "" && len(prepared.operations) != 0 {
		if _, err := prepared.RetainAcceptedAuthority(); err != nil {
			prepared.Abort()
			return nil, nil, err
		}
	}
	return prepared, nil, nil
}
