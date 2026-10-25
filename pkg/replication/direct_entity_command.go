package replication

import (
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

func decodeEntityCommandOperations(kind CommandType, data []byte) ([]storage.Operation, bool, error) {
	switch kind {
	case CmdCreateNode, CmdUpdateNode, CmdDeleteNode, CmdCreateEdge, CmdUpdateEdge, CmdDeleteEdge:
		op, err := storage.DecodeEntityChange(storage.EntityCommandKind(kind), data)
		if err != nil {
			return nil, true, err
		}
		return []storage.Operation{op}, true, nil
	case CmdBulkCreateNodes, CmdBulkCreateEdges:
		var encoded [][]byte
		if err := storage.DecodeCommandPayload(data, &encoded); err != nil {
			if kind == CmdBulkCreateNodes {
				return nil, true, localizedError(localization.ReplicationStorageDecodeBulkCreateNodesFailed(err), err)
			}
			return nil, true, localizedError(localization.ReplicationStorageDecodeBulkCreateEdgesFailed(err), err)
		}
		leaf := CmdCreateNode
		if kind == CmdBulkCreateEdges {
			leaf = CmdCreateEdge
		}
		ops := make([]storage.Operation, 0, len(encoded))
		for _, body := range encoded {
			op, err := storage.DecodeEntityChange(storage.EntityCommandKind(leaf), body)
			if err != nil {
				return nil, true, err
			}
			ops = append(ops, op)
		}
		return ops, true, nil
	case CmdBulkDeleteNodes:
		var ids []storage.NodeID
		if err := storage.DecodeCommandPayload(data, &ids); err != nil {
			return nil, true, localizedError(localization.ReplicationStorageDecodeBulkDeleteNodesFailed(err), err)
		}
		ops := make([]storage.Operation, len(ids))
		for i, id := range ids {
			ops[i] = storage.Operation{Type: storage.OpDeleteNode, NodeID: id}
		}
		return ops, true, nil
	case CmdBulkDeleteEdges:
		var ids []storage.EdgeID
		if err := storage.DecodeCommandPayload(data, &ids); err != nil {
			return nil, true, localizedError(localization.ReplicationStorageDecodeBulkDeleteEdgesFailed(err), err)
		}
		ops := make([]storage.Operation, len(ids))
		for i, id := range ids {
			ops[i] = storage.Operation{Type: storage.OpDeleteEdge, EdgeID: id}
		}
		return ops, true, nil
	}
	return nil, false, nil
}
func applyDirectEntityCommand(engine storage.Engine, kind CommandType, ops []storage.Operation, origin uint64) error {
	bound, err := storage.EngineWithVectorWorkOrigin(engine, origin)
	if err != nil {
		return err
	}
	engine = bound
	switch kind {
	case CmdBulkCreateNodes:
		nodes := make([]*storage.Node, len(ops))
		for i := range ops {
			nodes[i] = ops[i].Node
		}
		return engine.BulkCreateNodes(nodes)
	case CmdBulkCreateEdges:
		edges := make([]*storage.Edge, len(ops))
		for i := range ops {
			edges[i] = ops[i].Edge
		}
		return engine.BulkCreateEdges(edges)
	case CmdBulkDeleteNodes:
		ids := make([]storage.NodeID, len(ops))
		for i := range ops {
			ids[i] = ops[i].NodeID
		}
		return engine.BulkDeleteNodes(ids)
	case CmdBulkDeleteEdges:
		ids := make([]storage.EdgeID, len(ops))
		for i := range ops {
			ids[i] = ops[i].EdgeID
		}
		return engine.BulkDeleteEdges(ids)
	}
	if len(ops) != 1 {
		return storage.ErrInvalidData
	}
	return storage.ApplyEntityChange(engine, ops[0], engine.UpdateNode, engine.DeleteNode)
}

// encodeEntityCommandPayload is the legacy entity wire construction owner.
// Accepted transaction envelopes use their separate versioned semantic codec.
func encodeEntityCommandPayload(kind CommandType, value any) ([]byte, error) {
	var data []byte
	var err error
	switch kind {
	case CmdCreateNode, CmdUpdateNode:
		node := value.(*storage.Node)
		if node == nil {
			return nil, localizedError(localization.ReplicationEngineNodeRequired(), nil)
		}
		data, err = storage.EncodeNodePayload(node)
	case CmdCreateEdge, CmdUpdateEdge:
		edge := value.(*storage.Edge)
		if edge == nil {
			return nil, localizedError(localization.ReplicationEngineEdgeRequired(), nil)
		}
		data, err = storage.EncodeEdgePayload(edge)
	case CmdDeleteNode:
		return []byte(string(value.(storage.NodeID))), nil
	case CmdDeleteEdge:
		data, err = storage.EncodeCommandPayload(struct{ EdgeID string }{EdgeID: string(value.(storage.EdgeID))})
	case CmdBulkCreateNodes:
		return encodeBulkEntityPayload(value.([]*storage.Node), storage.EncodeNodePayload, CmdCreateNode, kind)
	case CmdBulkCreateEdges:
		return encodeBulkEntityPayload(value.([]*storage.Edge), storage.EncodeEdgePayload, CmdCreateEdge, kind)
	case CmdBulkDeleteNodes, CmdBulkDeleteEdges:
		data, err = storage.EncodeCommandPayload(value)
	default:
		return nil, storage.ErrInvalidData
	}
	if err != nil {
		return nil, entityCommandEncodeError(kind, err)
	}
	return data, nil
}
func encodeBulkEntityPayload[T any](values []T, encode func(T) ([]byte, error), leaf, kind CommandType) ([]byte, error) {
	encoded := make([][]byte, 0, len(values))
	for _, value := range values {
		data, err := encode(value)
		if err != nil {
			return nil, entityCommandEncodeError(leaf, err)
		}
		encoded = append(encoded, data)
	}
	data, err := storage.EncodeCommandPayload(encoded)
	if err != nil {
		return nil, entityCommandEncodeError(kind, err)
	}
	return data, nil
}
func entityCommandEncodeError(kind CommandType, err error) error {
	switch kind {
	case CmdCreateNode, CmdUpdateNode:
		return localizedError(localization.ReplicationEngineEncodeNodeFailed(err), err)
	case CmdCreateEdge, CmdUpdateEdge:
		return localizedError(localization.ReplicationEngineEncodeEdgeFailed(err), err)
	case CmdDeleteEdge:
		return localizedError(localization.ReplicationEngineEncodeDeleteEdgeFailed(err), err)
	case CmdBulkCreateNodes:
		return localizedError(localization.ReplicationEngineEncodeBulkCreateNodesFailed(err), err)
	case CmdBulkCreateEdges:
		return localizedError(localization.ReplicationEngineEncodeBulkCreateEdgesFailed(err), err)
	case CmdBulkDeleteNodes:
		return localizedError(localization.ReplicationEngineEncodeBulkDeleteNodesFailed(err), err)
	case CmdBulkDeleteEdges:
		return localizedError(localization.ReplicationEngineEncodeBulkDeleteEdgesFailed(err), err)
	}
	return err
}
