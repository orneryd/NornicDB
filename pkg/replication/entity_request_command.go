package replication

import (
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// entityRequestCommand is the second entity wire family. The blind direct kinds
// (CmdCreateNode, CmdUpdateNode, CmdCreateEdge, CmdUpdateEdge) keep their
// existing payload and numeric slots because published logs and V4 migration
// read them. A caller that plans against an observed preimage needs that image
// to reach its leader, so it appends one request kind per updated entity.
//
// The request payload is a storage.EncodeCommandPayload record whose Before and
// After are the unchanged EncodeNodePayload/EncodeEdgePayload images. A node
// request also carries the caller's EntityImagePolicy. Nothing persists in this
// shape: a leader normalizes a decoded request into the existing accepted
// transaction encoding, and followers forward the request unchanged.
const (
	nodeImagePolicyWireClient byte = iota
	nodeImagePolicyWireStored
)

// nodeRequestPayload is the node request record. A nil Before encodes as an
// absent field, preserving the deliberate blind image write.
type nodeRequestPayload struct {
	Before []byte
	After  []byte
	Policy byte
}

// edgeRequestPayload is the relationship request record.
type edgeRequestPayload struct {
	Before []byte
	After  []byte
}

// encodeEntityRequestCommandPayload owns request-kind construction. After is
// always required; the policy field exists only on the node request kind.
func encodeEntityRequestCommandPayload(kind CommandType, value any) ([]byte, error) {
	var payload any
	var err error
	switch kind {
	case CmdUpdateNodeRequest:
		request := value.(storage.NodeUpdateRequest)
		if request.After == nil {
			return nil, storage.ErrInvalidData
		}
		record := nodeRequestPayload{}
		if request.Before != nil {
			if record.Before, err = storage.EncodeNodePayload(request.Before); err != nil {
				return nil, entityCommandEncodeError(kind, err)
			}
		}
		if record.After, err = storage.EncodeNodePayload(request.After); err != nil {
			return nil, entityCommandEncodeError(kind, err)
		}
		if record.Policy, err = nodeImagePolicyToWire(request.Policy); err != nil {
			return nil, err
		}
		payload = record
	case CmdUpdateEdgeRequest:
		request := value.(storage.EdgeUpdateRequest)
		if request.After == nil {
			return nil, storage.ErrInvalidData
		}
		record := edgeRequestPayload{}
		if request.Before != nil {
			if record.Before, err = storage.EncodeEdgePayload(request.Before); err != nil {
				return nil, entityCommandEncodeError(kind, err)
			}
		}
		if record.After, err = storage.EncodeEdgePayload(request.After); err != nil {
			return nil, entityCommandEncodeError(kind, err)
		}
		payload = record
	default:
		return nil, storage.ErrInvalidData
	}
	data, err := storage.EncodeCommandPayload(payload)
	if err != nil {
		return nil, entityCommandEncodeError(kind, err)
	}
	return data, nil
}

// nodeImagePolicyToWire accepts only the policies this transport can carry.
// An unsupported value is rejected rather than substituted.
func nodeImagePolicyToWire(policy storage.EntityImagePolicy) (byte, error) {
	switch policy {
	case storage.ClientNodeEdits:
		return nodeImagePolicyWireClient, nil
	case storage.StoredNodeImages:
		return nodeImagePolicyWireStored, nil
	}
	return 0, storage.ErrInvalidData
}

// nodeImagePolicyFromWire rejects an unknown policy byte.
func nodeImagePolicyFromWire(policy byte) (storage.EntityImagePolicy, error) {
	switch policy {
	case nodeImagePolicyWireClient:
		return storage.ClientNodeEdits, nil
	case nodeImagePolicyWireStored:
		return storage.StoredNodeImages, nil
	}
	return storage.ClientNodeEdits, storage.ErrInvalidData
}

// decodeEntityRequestOperations reconstructs the typed storage request so the
// existing leader prepare path stages and validates it natively.
func decodeEntityRequestOperations(kind CommandType, data []byte) ([]storage.Operation, error) {
	switch kind {
	case CmdUpdateNodeRequest:
		var record nodeRequestPayload
		if err := storage.DecodeCommandPayload(data, &record); err != nil {
			return nil, localizedError(localization.ReplicationStorageDecodeNodeFailed(err), err)
		}
		if len(record.After) == 0 {
			return nil, storage.ErrInvalidData
		}
		after, err := storage.DecodeNodePayload(record.After)
		if err != nil {
			return nil, localizedError(localization.ReplicationStorageDecodeNodeFailed(err), err)
		}
		var before *storage.Node
		if len(record.Before) > 0 {
			if before, err = storage.DecodeNodePayload(record.Before); err != nil {
				return nil, localizedError(localization.ReplicationStorageDecodeNodeFailed(err), err)
			}
		}
		policy, err := nodeImagePolicyFromWire(record.Policy)
		if err != nil {
			return nil, err
		}
		return []storage.Operation{storage.NodeUpdateRequest{Before: before, After: after, Policy: policy}.Operation()}, nil
	case CmdUpdateEdgeRequest:
		var record edgeRequestPayload
		if err := storage.DecodeCommandPayload(data, &record); err != nil {
			return nil, localizedError(localization.ReplicationStorageDecodeEdgeFailed(err), err)
		}
		if len(record.After) == 0 {
			return nil, storage.ErrInvalidData
		}
		after, err := storage.DecodeEdgePayload(record.After)
		if err != nil {
			return nil, localizedError(localization.ReplicationStorageDecodeEdgeFailed(err), err)
		}
		var before *storage.Edge
		if len(record.Before) > 0 {
			if before, err = storage.DecodeEdgePayload(record.Before); err != nil {
				return nil, localizedError(localization.ReplicationStorageDecodeEdgeFailed(err), err)
			}
		}
		return []storage.Operation{storage.EdgeUpdateRequest{Before: before, After: after}.Operation()}, nil
	}
	return nil, storage.ErrInvalidData
}
