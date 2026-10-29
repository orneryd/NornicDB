package replication

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/orneryd/nornicdb/internal/gobcodec"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// TranscodeV4WALPayload converts historical replication commands while
// preserving the other JSON fields in the record. It is used only by the
// authorized V4 WAL upgrade.
func TranscodeV4WALPayload(_ uint8, operation storage.OperationType, payload []byte) ([]byte, error) {
	if operation != storage.OperationType("replication_command") {
		return payload, nil
	}
	var record map[string]json.RawMessage
	if err := json.Unmarshal(payload, &record); err != nil {
		return nil, fmt.Errorf("decode historical replication record: %w", err)
	}
	var command map[string]json.RawMessage
	if err := json.Unmarshal(record["cmd"], &command); err != nil || command == nil {
		return nil, fmt.Errorf("decode historical replication command")
	}
	var kind CommandType
	if err := json.Unmarshal(command["Type"], &kind); err != nil {
		return nil, fmt.Errorf("decode historical replication command type: %w", err)
	}
	switch kind {
	case CmdCommitTransaction, CmdCreateNode, CmdUpdateNode, CmdDeleteNode, CmdCreateEdge,
		CmdUpdateEdge, CmdDeleteEdge, CmdBulkCreateNodes, CmdBulkCreateEdges,
		CmdBulkDeleteNodes, CmdBulkDeleteEdges, CmdDeleteByPrefix:
	default:
		return nil, fmt.Errorf("unsupported historical replication command %d", kind)
	}
	var historical []byte
	if err := json.Unmarshal(command["Data"], &historical); err != nil {
		return nil, fmt.Errorf("decode historical replication command payload: %w", err)
	}
	var converted []byte
	var err error
	switch kind {
	case CmdCommitTransaction:
		converted, err = storage.TranscodeV4EntityTransaction(historical)
	case CmdCreateNode, CmdUpdateNode, CmdDeleteNode, CmdCreateEdge, CmdUpdateEdge, CmdDeleteEdge:
		converted, err = storage.TranscodeV4EntityChange(storage.EntityCommandKind(kind), historical)
	case CmdBulkCreateNodes, CmdBulkCreateEdges:
		var values [][]byte
		if err = decodeV4ReplicationValue(historical, &values); err != nil {
			break
		}
		leaf := storage.EntityCreateNode
		if kind == CmdBulkCreateEdges {
			leaf = storage.EntityCreateEdge
		}
		for i := range values {
			values[i], err = storage.TranscodeV4EntityChange(leaf, values[i])
			if err != nil {
				break
			}
		}
		if err == nil {
			converted, err = storage.EncodeCommandPayload(values)
		}
	case CmdBulkDeleteNodes:
		var values []storage.NodeID
		if err = decodeV4ReplicationValue(historical, &values); err == nil {
			converted, err = storage.EncodeCommandPayload(values)
		}
	case CmdBulkDeleteEdges:
		var values []storage.EdgeID
		if err = decodeV4ReplicationValue(historical, &values); err == nil {
			converted, err = storage.EncodeCommandPayload(values)
		}
	case CmdDeleteByPrefix:
		var value struct{ Prefix string }
		if err = decodeV4ReplicationValue(historical, &value); err == nil {
			converted, err = storage.EncodeCommandPayload(value)
		}
	default:
		return payload, nil
	}
	if err != nil {
		return nil, fmt.Errorf("convert historical replication command %d: %w", kind, err)
	}
	command["Data"], err = json.Marshal(converted)
	if err != nil {
		return nil, err
	}
	record["cmd"], err = json.Marshal(command)
	if err != nil {
		return nil, err
	}
	return json.Marshal(record)
}

// Historical replication commands could be gob or JSON. This decoder is
// never used by current replication traffic or current WAL replay.
func decodeV4ReplicationValue(data []byte, value any) error {
	err := gobcodec.Decode(bytes.NewReader(data), value, gobcodec.RejectTrailing)
	if err == nil {
		return nil
	}
	if errors.Is(err, gobcodec.ErrTrailing) {
		return fmt.Errorf("trailing historical replication payload: %w", err)
	}
	return json.Unmarshal(data, value)
}
