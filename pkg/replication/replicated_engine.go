package replication

import (
	"time"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// ReplicatedEngine wraps a storage.Engine and routes write operations through a Replicator.
//
// Design:
//   - Reads are served locally from the embedded Engine.
//   - Writes are turned into replication Commands and applied via the Replicator.
//   - The embedded Engine is used only for reads; replicated writes are applied to the
//     *inner* engine by the StorageAdapter on each node.
//
// This wrapper intentionally operates on the *base* storage (the engine that stores
// fully-qualified IDs like "<db>:<id>") so multi-database isolation is preserved.
type ReplicatedEngine struct {
	storage.Engine

	replicator Replicator
	timeout    time.Duration
}

func NewReplicatedEngine(inner storage.Engine, replicator Replicator, timeout time.Duration) *ReplicatedEngine {
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	return &ReplicatedEngine{
		Engine:     inner,
		replicator: replicator,
		timeout:    timeout,
	}
}

// IsLeader reports whether this node can accept writes in the current replication mode.
// This is a convenience for higher-level components (e.g. multidb startup) that need
// to avoid performing metadata migrations on standby/followers.
func (e *ReplicatedEngine) IsLeader() bool {
	if e == nil || e.replicator == nil {
		return true
	}
	return e.replicator.IsLeader()
}

func (e *ReplicatedEngine) CreateNode(node *storage.Node) (storage.NodeID, error) {
	if node == nil {
		return "", localizedError(localization.ReplicationEngineNodeRequired(), nil)
	}
	data, err := encodeNodePayload(node)
	if err != nil {
		return "", localizedError(localization.ReplicationEngineEncodeNodeFailed(err), err)
	}
	if err := e.replicator.Apply(&Command{Type: CmdCreateNode, Data: data, Timestamp: time.Now()}, e.timeout); err != nil {
		return "", err
	}
	return node.ID, nil
}

func (e *ReplicatedEngine) UpdateNode(node *storage.Node) error {
	if node == nil {
		return localizedError(localization.ReplicationEngineNodeRequired(), nil)
	}
	data, err := encodeNodePayload(node)
	if err != nil {
		return localizedError(localization.ReplicationEngineEncodeNodeFailed(err), err)
	}
	return e.replicator.Apply(&Command{Type: CmdUpdateNode, Data: data, Timestamp: time.Now()}, e.timeout)
}

func (e *ReplicatedEngine) DeleteNode(id storage.NodeID) error {
	return e.replicator.Apply(&Command{Type: CmdDeleteNode, Data: []byte(string(id)), Timestamp: time.Now()}, e.timeout)
}

func (e *ReplicatedEngine) CreateEdge(edge *storage.Edge) error {
	if edge == nil {
		return localizedError(localization.ReplicationEngineEdgeRequired(), nil)
	}
	data, err := encodeEdgePayload(edge)
	if err != nil {
		return localizedError(localization.ReplicationEngineEncodeEdgeFailed(err), err)
	}
	return e.replicator.Apply(&Command{Type: CmdCreateEdge, Data: data, Timestamp: time.Now()}, e.timeout)
}

func (e *ReplicatedEngine) UpdateEdge(edge *storage.Edge) error {
	if edge == nil {
		return localizedError(localization.ReplicationEngineEdgeRequired(), nil)
	}
	data, err := encodeEdgePayload(edge)
	if err != nil {
		return localizedError(localization.ReplicationEngineEncodeEdgeFailed(err), err)
	}
	return e.replicator.Apply(&Command{Type: CmdUpdateEdge, Data: data, Timestamp: time.Now()}, e.timeout)
}

func (e *ReplicatedEngine) DeleteEdge(id storage.EdgeID) error {
	// Gob payload; JSON tags are unnecessary/confusing here.
	payload, err := encodeGob(struct {
		EdgeID string
	}{EdgeID: string(id)})
	if err != nil {
		return localizedError(localization.ReplicationEngineEncodeDeleteEdgeFailed(err), err)
	}
	return e.replicator.Apply(&Command{Type: CmdDeleteEdge, Data: payload, Timestamp: time.Now()}, e.timeout)
}

func (e *ReplicatedEngine) BulkCreateNodes(nodes []*storage.Node) error {
	encoded := make([][]byte, 0, len(nodes))
	for _, n := range nodes {
		data, err := encodeNodePayload(n)
		if err != nil {
			return localizedError(localization.ReplicationEngineEncodeNodeFailed(err), err)
		}
		encoded = append(encoded, data)
	}
	payload, err := encodeGob(encoded)
	if err != nil {
		return localizedError(localization.ReplicationEngineEncodeBulkCreateNodesFailed(err), err)
	}
	return e.replicator.Apply(&Command{Type: CmdBulkCreateNodes, Data: payload, Timestamp: time.Now()}, e.timeout)
}

func (e *ReplicatedEngine) BulkCreateEdges(edges []*storage.Edge) error {
	encoded := make([][]byte, 0, len(edges))
	for _, edge := range edges {
		data, err := encodeEdgePayload(edge)
		if err != nil {
			return localizedError(localization.ReplicationEngineEncodeEdgeFailed(err), err)
		}
		encoded = append(encoded, data)
	}
	payload, err := encodeGob(encoded)
	if err != nil {
		return localizedError(localization.ReplicationEngineEncodeBulkCreateEdgesFailed(err), err)
	}
	return e.replicator.Apply(&Command{Type: CmdBulkCreateEdges, Data: payload, Timestamp: time.Now()}, e.timeout)
}

func (e *ReplicatedEngine) BulkDeleteNodes(ids []storage.NodeID) error {
	payload, err := encodeGob(ids)
	if err != nil {
		return localizedError(localization.ReplicationEngineEncodeBulkDeleteNodesFailed(err), err)
	}
	return e.replicator.Apply(&Command{Type: CmdBulkDeleteNodes, Data: payload, Timestamp: time.Now()}, e.timeout)
}

func (e *ReplicatedEngine) BulkDeleteEdges(ids []storage.EdgeID) error {
	payload, err := encodeGob(ids)
	if err != nil {
		return localizedError(localization.ReplicationEngineEncodeBulkDeleteEdgesFailed(err), err)
	}
	return e.replicator.Apply(&Command{Type: CmdBulkDeleteEdges, Data: payload, Timestamp: time.Now()}, e.timeout)
}

func (e *ReplicatedEngine) DeleteByPrefix(prefix string) (nodesDeleted int64, edgesDeleted int64, err error) {
	payload, err := encodeGob(struct {
		Prefix string `json:"prefix"`
	}{Prefix: prefix})
	if err != nil {
		return 0, 0, localizedError(localization.ReplicationEngineEncodeDeletePrefixFailed(err), err)
	}
	if err := e.replicator.Apply(&Command{Type: CmdDeleteByPrefix, Data: payload, Timestamp: time.Now()}, e.timeout); err != nil {
		return 0, 0, err
	}
	// Best-effort counts are not returned by the replication protocol today.
	return 0, 0, nil
}
