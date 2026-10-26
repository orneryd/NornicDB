package storage

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"time"

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

const EntityTransactionVersion uint16 = 1

type EntityTransactionChange struct {
	Type                 EntityCommandKind
	Data                 []byte
	NamedProperties      []byte
	EmbedMeta            []byte
	UpdatedAtUnixNs      int64
	VisibilitySuppressed bool
}
type EntityTransactionEnvelope struct {
	Mode        EntityCommitMode
	Version     uint16
	Namespace   string
	LogicalTxID string
	Changes     []EntityTransactionChange
}

type entityWriter interface {
	CreateNode(*Node) (NodeID, error)
	CreateEdge(*Edge) error
	UpdateEdge(*Edge) error
	DeleteEdge(EdgeID) error
}

// DecodeEntityChange shares the existing semantic payload codecs and legacy
// direct-command deletion contract. It never imports numeric dictionary IDs.
func DecodeEntityChange(kind EntityCommandKind, data []byte) (Operation, error) {
	return decodeEntityChange(kind, data, true)
}
func decodeEntityChange(kind EntityCommandKind, data []byte, legacyAllowed bool) (Operation, error) {
	var op Operation
	switch kind {
	case EntityCreateNode, EntityUpdateNode:
		node, err := decodeNodePayload(data, legacyAllowed)
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
		edge, err := decodeEdgePayload(data, legacyAllowed)
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
		var req struct{ NodeID string }
		if err := decodeCommandPayload(data, &req, legacyAllowed); err != nil || req.NodeID == "" {
			req.NodeID = string(data)
		}
		op.Type = OpDeleteNode
		op.NodeID = NodeID(req.NodeID)
	case EntityDeleteEdge:
		var req struct{ EdgeID string }
		if err := decodeCommandPayload(data, &req, legacyAllowed); err != nil {
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

// This new command format has one strict version; only the existing leaf
// payload decoder retains historical direct-command compatibility.
func DecodeEntityTransaction(data []byte) (EntityTransactionEnvelope, []Operation, error) {
	var envelope EntityTransactionEnvelope
	if err := decodeCommandPayload(data, &envelope, false); err != nil {
		return envelope, nil, err
	}
	if !envelope.Mode.valid() {
		return envelope, nil, ErrInvalidData
	}
	if envelope.Version != EntityTransactionVersion {
		return envelope, nil, errors.New("unsupported entity transaction version")
	}
	if envelope.Namespace == "" || envelope.LogicalTxID == "" || len(envelope.Changes) == 0 {
		return envelope, nil, errors.New("entity transaction requires namespace, logical ID and changes")
	}
	ops := make([]Operation, len(envelope.Changes))
	sameNamespace := func(id string) bool {
		ns, _, ok := ParseDatabasePrefix(id)
		return ok && ns == envelope.Namespace
	}
	for i, change := range envelope.Changes {
		op, err := decodeEntityChange(change.Type, change.Data, false)
		if err != nil {
			return envelope, nil, err
		}
		if op.Node != nil || op.Edge != nil {
			properties, err := DecodeNamedProperties(change.NamedProperties)
			if err != nil {
				return envelope, nil, err
			}
			if op.Node != nil {
				op.Node.Properties = properties
				op.Node.EmbedMeta, err = DecodeNamedMetadata(change.EmbedMeta)
				if err != nil {
					return envelope, nil, err
				}
				op.Node.VisibilitySuppressed = change.VisibilitySuppressed
			} else {
				op.Edge.Properties = properties
				if change.UpdatedAtUnixNs != 0 {
					op.Edge.UpdatedAt = time.Unix(0, change.UpdatedAtUnixNs)
				}
				op.Edge.VisibilitySuppressed = change.VisibilitySuppressed
			}
		}

		if op.Node != nil {
			if !sameNamespace(string(op.Node.ID)) {
				return envelope, nil, ErrCrossNamespaceTransaction
			}
		}
		if op.Type == OpDeleteNode && !sameNamespace(string(op.NodeID)) {
			return envelope, nil, ErrCrossNamespaceTransaction
		}
		if op.Edge != nil {
			if !sameNamespace(string(op.Edge.ID)) || !sameNamespace(string(op.Edge.StartNode)) || !sameNamespace(string(op.Edge.EndNode)) {
				return envelope, nil, ErrCrossNamespaceTransaction
			}
		}
		if op.Type == OpDeleteEdge && !sameNamespace(string(op.EdgeID)) {
			return envelope, nil, ErrCrossNamespaceTransaction
		}
		ops[i] = op
	}
	return envelope, ops, nil
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
	receipt, err := SubmitAcceptedEntityTransaction(context.Background(), engine, data, nil)
	if err != nil {
		return err
	}
	return receipt.Wait(context.Background())
}

func SubmitAcceptedEntityTransaction(ctx context.Context, engine Engine, data []byte, settle func(CommitOutcome)) (*CommitReceipt, error) {
	envelope, ops, err := DecodeEntityTransaction(data)
	if err != nil {
		return nil, err
	}
	return submitDecodedEntityTransaction(ctx, engine, envelope, ops, settle)
}
func submitDecodedEntityTransaction(ctx context.Context, engine Engine, envelope EntityTransactionEnvelope, ops []Operation, settle func(CommitOutcome)) (*CommitReceipt, error) {
	tx, err := BeginEntityTransaction(engine, envelope.Mode)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if err = tx.SetNamespace(envelope.Namespace); err != nil {
		return nil, err
	}
	if err = tx.SetDeferredConstraintValidation(true); err != nil {
		return nil, err
	}
	applied, err := tx.PrepareLogicalCommitReceipt(envelope.LogicalTxID)
	if err != nil {
		return nil, err
	}
	if applied {
		prepared, err := tx.PrepareEntityCommit(ctx, envelope.LogicalTxID)
		if err != nil {
			return nil, err
		}
		route, found := EntityCommitSubmitterFor(engine)
		if !found {
			prepared.Abort()
			return nil, ErrInvalidData
		}
		return route.Submitter.SubmitEntityCommit(ctx, prepared, settle)
	}
	for _, op := range ops {
		if err = ApplyEntityChange(tx, op, func(node *Node) error { return tx.UpdateGraphNode(node, StoredNodeImages) }, func(id NodeID) error { return tx.DeleteGraphNode(id, CascadeEdges) }); err != nil {
			return nil, err
		}
	}
	prepared, err := tx.PrepareEntityCommit(ctx, envelope.LogicalTxID)
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

// EncodeEntityTransactionChange reuses the existing node/edge payload codecs
// for structural fields/vectors while the existing storage value codec owns
// dictionary-independent named properties. Legacy direct command bytes stay
// unchanged: only this new envelope carries the named-value field.
func EncodeEntityTransactionChange(op Operation) (EntityTransactionChange, error) {
	var change EntityTransactionChange
	var err error
	switch op.Type {
	case OpCreateNode, OpUpdateNode:
		if op.Node == nil {
			return change, ErrInvalidData
		}
		node := *op.Node
		change.NamedProperties, err = EncodeNamedProperties(node.Properties)
		if err != nil {
			return change, err
		}
		change.EmbedMeta, err = EncodeNamedProperties(node.EmbedMeta)
		if err != nil {
			return change, err
		}
		change.VisibilitySuppressed = node.VisibilitySuppressed
		node.Properties = nil
		change.Data, err = EncodeNodePayload(&node)
		change.Type = EntityCreateNode
		if op.Type == OpUpdateNode {
			change.Type = EntityUpdateNode
		}
	case OpCreateEdge, OpUpdateEdge:
		if op.Edge == nil {
			return change, ErrInvalidData
		}
		edge := *op.Edge
		change.NamedProperties, err = EncodeNamedProperties(edge.Properties)
		if err != nil {
			return change, err
		}
		if !edge.UpdatedAt.IsZero() {
			change.UpdatedAtUnixNs = edge.UpdatedAt.UnixNano()
		}
		change.VisibilitySuppressed = edge.VisibilitySuppressed
		edge.Properties = nil
		change.Data, err = EncodeEdgePayload(&edge)
		change.Type = EntityCreateEdge
		if op.Type == OpUpdateEdge {
			change.Type = EntityUpdateEdge
		}
	case OpDeleteNode:
		change.Type = EntityDeleteNode
		change.Data, err = EncodeCommandPayload(struct{ NodeID string }{string(op.NodeID)})
	case OpDeleteEdge:
		change.Type = EntityDeleteEdge
		change.Data, err = EncodeCommandPayload(struct{ EdgeID string }{string(op.EdgeID)})
	default:
		return change, fmt.Errorf("not an entity operation: %s", op.Type)
	}
	return change, err
}

// EncodeEntityTransaction creates the strict semantic record shared by WAL
// and peer application. Existing direct-command codecs remain unchanged.
func EncodeEntityTransaction(namespace, logicalID string, ops []Operation, modes ...EntityCommitMode) ([]byte, error) {
	envelope := EntityTransactionEnvelope{Version: EntityTransactionVersion, Namespace: namespace, LogicalTxID: logicalID}
	if len(modes) > 1 {
		return nil, ErrInvalidData
	}
	if len(modes) == 1 {
		if !modes[0].valid() {
			return nil, ErrInvalidData
		}
		envelope.Mode = modes[0]
	}
	for _, op := range ops {
		change, err := EncodeEntityTransactionChange(op)
		if err != nil {
			return nil, err
		}
		envelope.Changes = append(envelope.Changes, change)
	}
	var data bytes.Buffer
	if err := gob.NewEncoder(&data).Encode(envelope); err != nil {
		return nil, err
	}
	return data.Bytes(), nil
}
