package storage

import "context"

// Redo retains the physical command identity recorded by the outcome owner.
// Restarts and repeated redo must not invent a different conflict history on
// a peer with the same accepted commands.
func replayVersionedEntry(ctx context.Context, engine Engine, entry WALEntry) error {
	var op Operation
	op.Type = entry.Operation
	switch entry.Operation {
	case OpCreateNode, OpUpdateNode:
		var data WALNodeData
		if err := decodeRecoveryPayload(entry.Data, &data); err != nil {
			return err
		}
		op.Node = data.Node
	case OpCreateEdge, OpUpdateEdge:
		var data WALEdgeData
		if err := decodeRecoveryPayload(entry.Data, &data); err != nil {
			return err
		}
		op.Edge = data.Edge
	case OpDeleteNode, OpDeleteEdge:
		var data WALDeleteData
		if err := decodeRecoveryPayload(entry.Data, &data); err != nil {
			return err
		}
		if entry.Operation == OpDeleteNode {
			op.NodeID = NodeID(data.ID)
		} else {
			op.EdgeID = EdgeID(data.ID)
		}
	case OpBulkNodes, OpBulkEdges:
		replay := ManagedReplay{Namespace: entry.Database, Bulk: true}
		if entry.Operation == OpBulkNodes {
			var data WALBulkNodesData
			if err := decodeRecoveryPayload(entry.Data, &data); err != nil {
				return err
			}
			for _, node := range data.Nodes {
				replay.Operations = append(replay.Operations, Operation{Type: OpCreateNode, Node: node})
			}
		} else {
			var data WALBulkEdgesData
			if err := decodeRecoveryPayload(entry.Data, &data); err != nil {
				return err
			}
			for _, edge := range data.Edges {
				replay.Operations = append(replay.Operations, Operation{Type: OpCreateEdge, Edge: edge})
			}
		}
		return ApplyManagedReplay(ctx, engine, replay)
	case OpSchemaDefinition:
		var data WALSchemaData
		if err := decodeRecoveryPayload(entry.Data, &data); err != nil {
			return err
		}
		return ApplyManagedReplay(ctx, engine, ManagedReplay{Namespace: entry.Database, Transaction: true, Schema: data.Schema, SchemaTransaction: data.SchemaTransaction})
	default:
		entry.ReplicationID = [16]byte{}
		return ReplayWALEntry(engine, entry)
	}
	if SupportsNativeCommand(engine) {
		return ExecuteNativeCommand(ctx, engine, op, nil)
	}
	entry.ReplicationID = [16]byte{}
	return ReplayWALEntry(engine, entry)
}
