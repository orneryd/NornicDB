package replication

import (
	"context"
	"encoding/json"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"

	"github.com/orneryd/nornicdb/pkg/storage"
)

type transactionOperation struct {
	Type             storage.OperationType
	NodeID           storage.NodeID
	EdgeID           storage.EdgeID
	Node, Edge       []byte
	ReplacesExisting bool
}
type transactionCommand struct {
	Namespace         string
	Operations        []transactionOperation
	Schema            []byte
	SchemaTransaction bool
	Implicit          bool
	Transaction, Bulk bool
	Conditions        *storage.ReplayConditions
}

func encodeManagedTransaction(replay storage.ManagedReplay) ([]byte, error) {
	payload := transactionCommand{Namespace: replay.Namespace, Operations: make([]transactionOperation, len(replay.Operations))}
	payload.Implicit = replay.Implicit
	payload.Transaction, payload.Bulk, payload.Conditions = replay.Transaction, replay.Bulk, replay.Conditions
	if replay.Schema != nil {
		var err error
		payload.Schema, err = json.Marshal(replay.Schema)
		if err != nil {
			return nil, err
		}
		payload.SchemaTransaction = replay.SchemaTransaction
	}
	for i, op := range replay.Operations {
		encoded := transactionOperation{Type: op.Type, NodeID: op.NodeID, EdgeID: op.EdgeID, ReplacesExisting: op.ReplacesExisting}
		var err error
		if op.Node != nil {
			encoded.Node, err = encodeNodePayload(op.Node)
			if err != nil {
				return nil, err
			}
		}
		if op.Edge != nil {
			encoded.Edge, err = encodeEdgePayload(op.Edge)
			if err != nil {
				return nil, err
			}
		}
		payload.Operations[i] = encoded
	}
	return encodeGob(payload)
}

func decodeManagedTransaction(data []byte) (storage.ManagedReplay, error) {
	var payload transactionCommand
	if err := decodeGob(data, &payload); err != nil {
		return storage.ManagedReplay{}, err
	}
	replay := storage.ManagedReplay{Namespace: payload.Namespace, Transaction: payload.Transaction, Bulk: payload.Bulk, Conditions: payload.Conditions, Operations: make([]storage.Operation, len(payload.Operations))}
	replay.Implicit = payload.Implicit
	if len(payload.Schema) != 0 {
		replay.Schema, replay.SchemaTransaction = &storage.SchemaDefinition{}, payload.SchemaTransaction
		if err := json.Unmarshal(payload.Schema, replay.Schema); err != nil {
			return replay, err
		}
	}
	for i, encoded := range payload.Operations {
		op := storage.Operation{Type: encoded.Type, NodeID: encoded.NodeID, EdgeID: encoded.EdgeID, ReplacesExisting: encoded.ReplacesExisting}
		var err error
		if len(encoded.Node) != 0 {
			op.Node, err = decodeNodePayload(encoded.Node)
			if err != nil {
				return replay, err
			}
		}
		if len(encoded.Edge) != 0 {
			op.Edge, err = decodeEdgePayload(encoded.Edge)
			if err != nil {
				return replay, err
			}
		}
		replay.Operations[i] = op
	}
	return replay, nil
}

func (e *ReplicatedEngine) CommitManagedTransaction(ctx context.Context, replay storage.ManagedReplay, local func(context.Context, func()) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	data, err := encodeManagedTransaction(replay)
	if err != nil {
		return nornicerrors.MarkCommitRolledBack(err)
	}
	return e.dispatchManaged(ctx, &Command{Type: CmdTransaction, Data: data}, local)
}
