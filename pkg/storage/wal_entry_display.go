package storage

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/orneryd/nornicdb/internal/jsontext"
)

// EntryPayloadJSON renders a WAL entry for db.txlog. Binary commit keys are
// base64-encoded byte strings so every physical key remains distinguishable.
// Invalid payloads are reported rather than returned as arbitrary text.
func EntryPayloadJSON(entry WALEntry) (string, error) {
	var value any
	switch entry.Operation {
	case OpCheckpoint, OpTxBegin, OpTxCommit, OpTxAbort:
		if !json.Valid(entry.Data) {
			return "", fmt.Errorf("wal: invalid JSON control payload for entry %d", entry.Sequence)
		}
		return string(entry.Data), nil
	case OpCreateNode, OpUpdateNode, OpUpdateEmbedding:
		value = &WALNodeData{}
	case OpCreateEdge, OpUpdateEdge:
		value = &WALEdgeData{}
	case OpDeleteNode, OpDeleteEdge:
		value = &WALDeleteData{}
	case OpBulkNodes:
		value = &WALBulkNodesData{}
	case OpBulkEdges:
		value = &WALBulkEdgesData{}
	case OpBulkDeleteNodes:
		value = &WALBulkDeleteNodesData{}
	case OpBulkDeleteEdges:
		value = &WALBulkDeleteEdgesData{}
	case OpEntityCommit:
		value = &walEntityCommitData{}
	case OpBufferedCommit:
		data, err := decodeBufferedWALRecord(entry)
		if err != nil {
			return "", err
		}
		value = bufferedWALDisplay(&data)
	default:
		return "", fmt.Errorf("wal: unsupported operation %q in entry %d", entry.Operation, entry.Sequence)
	}
	if entry.Operation != OpBufferedCommit {
		if err := decodeRecoveryPayload(entry.Data, value); err != nil {
			return "", fmt.Errorf("wal: failed to decode entry %d (%s) payload: %w", entry.Sequence, entry.Operation, err)
		}
		switch data := value.(type) {
		case *WALNodeData:
			if data.Node == nil && data.OldNode == nil {
				return "", fmt.Errorf("wal: entry %d has no node image", entry.Sequence)
			}
		case *WALEdgeData:
			if data.Edge == nil && data.OldEdge == nil {
				return "", fmt.Errorf("wal: entry %d has no edge image", entry.Sequence)
			}
		}
	}
	encoded, err := jsontext.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("wal: failed to render entry %d (%s) payload: %w", entry.Sequence, entry.Operation, err)
	}
	return string(encoded), nil
}

type bufferedWALDisplayEntry struct {
	Key   []byte `json:"key"`
	Value []byte `json:"value"`
}

type bufferedWALDisplayPayload struct {
	LogicalTxID          string                    `json:"logical_tx_id,omitempty"`
	TxID                 string                    `json:"tx_id"`
	Namespace            string                    `json:"namespace,omitempty"`
	Ops                  []Operation               `json:"ops"`
	Writes               []bufferedWALDisplayEntry `json:"writes,omitempty"`
	Deletes              [][]byte                  `json:"deletes,omitempty"`
	Version              MVCCVersion               `json:"version"`
	NodeMax              uint64                    `json:"node_max,omitempty"`
	EdgeMax              uint64                    `json:"edge_max,omitempty"`
	HasCounters          bool                      `json:"has_counters,omitempty"`
	NumericBindings      []byte                    `json:"numeric_bindings,omitempty"`
	PropertyBindings     []byte                    `json:"property_bindings,omitempty"`
	PropertyDictionaryID string                    `json:"property_dictionary_id,omitempty"`
	PropKeys             []WALPropKeyEntry         `json:"prop_keys,omitempty"`
	PropKeyCounters      map[string]uint64         `json:"prop_key_counters,omitempty"`
}

func bufferedWALDisplay(data *WALBufferedCommitData) *bufferedWALDisplayPayload {
	out := &bufferedWALDisplayPayload{
		LogicalTxID: data.LogicalTxID, TxID: data.TxID, Namespace: data.Namespace,
		Ops: data.Ops, Version: data.Version, NodeMax: data.NodeMax, EdgeMax: data.EdgeMax,
		HasCounters: data.HasCounters, NumericBindings: data.NumericBindings, PropertyBindings: data.PropertyBindings,
		PropertyDictionaryID: data.PropertyDictionaryID, PropKeys: data.PropKeys,
		PropKeyCounters: data.PropKeyCounters,
	}
	for key, value := range data.Writes {
		out.Writes = append(out.Writes, bufferedWALDisplayEntry{Key: []byte(key), Value: value})
	}
	sort.Slice(out.Writes, func(i, j int) bool { return bytes.Compare(out.Writes[i].Key, out.Writes[j].Key) < 0 })
	for _, key := range data.Deletes {
		out.Deletes = append(out.Deletes, []byte(key))
	}
	return out
}
