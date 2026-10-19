package replication

import (
	"context"
	"sort"
	"strings"
	"sync"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/pkg/util"
)

// A command retains its publication leases from journal admission through local
// apply. DROP releases them at the physical owner's completion, before catalog
// persistence can publish a system-namespace command through this adapter.
type commandPublicationPayload struct {
	node *storage.Node
	edge *storage.Edge
}

func (a *StorageAdapter) admitCommand(ctx context.Context, cmd *Command, drop bool, payload ...*commandPublicationPayload) (func(), error) {
	namespaces, err := commandNamespaces(cmd, payload...)
	if err != nil {
		return nil, err
	}
	global := len(namespaces) == 0
	var releases []func()
	release := func() {
		for i := len(releases) - 1; i >= 0; i-- {
			releases[i]()
		}
	}
	if !drop {
		r, err := storage.BeginNamespaceJournalMutation(ctx, a.engine, namespaces)
		if err != nil {
			return nil, err
		}
		releases = append(releases, r)
	}
	lock, unlock := a.commandPublication.RLock, a.commandPublication.RUnlock
	if global {
		lock, unlock = a.commandPublication.Lock, a.commandPublication.Unlock
	}
	r, err := util.LockContext(ctx, lock, unlock)
	if err != nil {
		release()
		return nil, err
	}
	releases = append(releases, r)
	sort.Strings(namespaces)
	previous := ""
	for _, namespace := range namespaces {
		if namespace == previous {
			continue
		}
		previous = namespace
		value, ok := a.namespacePublication.Load(namespace)
		if !ok {
			value, _ = a.namespacePublication.LoadOrStore(namespace, new(sync.RWMutex))
		}
		gate := value.(*sync.RWMutex)
		lock, unlock := gate.RLock, gate.RUnlock
		if drop {
			lock, unlock = gate.Lock, gate.Unlock
		}
		r, err := util.LockContext(ctx, lock, unlock)
		if err != nil {
			release()
			return nil, err
		}
		releases = append(releases, r)
	}
	var once sync.Once
	if !drop {
		if err := storage.ValidateNamespaceCommandAdmission(a.engine, namespaces); err != nil {
			release()
			return nil, err
		}
	}
	return func() { once.Do(release) }, nil
}

func commandNamespaces(cmd *Command, payload ...*commandPublicationPayload) ([]string, error) {
	var names []string
	add := func(id string) {
		namespace, _, ok := strings.Cut(id, ":")
		if ok && namespace != "" {
			names = append(names, namespace)
		}
	}
	switch cmd.Type {
	case CmdCreateNode, CmdUpdateNode:
		node, err := decodeNodePayload(cmd.Data)
		if err != nil {
			return nil, localizedError(localization.ReplicationStorageDecodeNodeFailed(err), err)
		}
		add(string(node.ID))
		if len(payload) != 0 {
			payload[0].node = node
		}
	case CmdCreateEdge, CmdUpdateEdge:
		edge, err := decodeEdgePayload(cmd.Data)
		if err != nil {
			return nil, localizedError(localization.ReplicationStorageDecodeEdgeFailed(err), err)
		}
		add(string(edge.ID))
		if len(payload) != 0 {
			payload[0].edge = edge
		}
	case CmdDeleteNode, CmdSetProperty:
		var req struct{ NodeID string }
		if err := decodeGob(cmd.Data, &req); err != nil && cmd.Type != CmdDeleteNode {
			return nil, localizedError(localization.ReplicationStorageDecodeSetPropertyFailed(err), err)
		}
		if req.NodeID == "" {
			add(string(cmd.Data))
		} else {
			add(req.NodeID)
		}
	case CmdDeleteEdge:
		var req struct{ EdgeID string }
		if err := decodeGob(cmd.Data, &req); err != nil {
			return nil, localizedError(localization.ReplicationStorageDecodeDeleteEdgeFailed(err), err)
		}
		add(req.EdgeID)
	case CmdDeleteByPrefix, CmdNamespaceCatalog:
		var req struct{ Prefix string }
		if err := decodeGob(cmd.Data, &req); err != nil {
			return nil, localizedError(localization.ReplicationStorageDecodeDeletePrefixFailed(err), err)
		}
		add(req.Prefix)
	case CmdBulkCreateNodes, CmdBulkCreateEdges:
		var encoded [][]byte
		if err := decodeGob(cmd.Data, &encoded); err != nil {
			if cmd.Type == CmdBulkCreateNodes {
				return nil, localizedError(localization.ReplicationStorageDecodeBulkCreateNodesFailed(err), err)
			}
			return nil, localizedError(localization.ReplicationStorageDecodeBulkCreateEdgesFailed(err), err)
		}
		for _, data := range encoded {
			if cmd.Type == CmdBulkCreateNodes {
				node, err := decodeNodePayload(data)
				if err != nil {
					return nil, err
				}
				add(string(node.ID))
			} else {
				edge, err := decodeEdgePayload(data)
				if err != nil {
					return nil, err
				}
				add(string(edge.ID))
			}
		}
	case CmdBulkDeleteNodes:
		var ids []storage.NodeID
		if err := decodeGob(cmd.Data, &ids); err != nil {
			return nil, localizedError(localization.ReplicationStorageDecodeBulkDeleteNodesFailed(err), err)
		}
		for _, id := range ids {
			add(string(id))
		}
	case CmdBulkDeleteEdges:
		var ids []storage.EdgeID
		if err := decodeGob(cmd.Data, &ids); err != nil {
			return nil, localizedError(localization.ReplicationStorageDecodeBulkDeleteEdgesFailed(err), err)
		}
		for _, id := range ids {
			add(string(id))
		}
	case CmdTransaction:
		replay, err := decodeManagedTransaction(cmd.Data)
		if err != nil {
			return nil, err
		}
		add(replay.Namespace + ":")
	case CmdBatchWrite:
		var batch struct{ Nodes, Edges [][]byte }
		if err := decodeGob(cmd.Data, &batch); err != nil {
			return nil, localizedError(localization.ReplicationStorageDecodeBatchFailed(err), err)
		}
		for _, data := range batch.Nodes {
			node, err := decodeNodePayload(data)
			if err != nil {
				return nil, err
			}
			add(string(node.ID))
		}
		for _, data := range batch.Edges {
			edge, err := decodeEdgePayload(data)
			if err != nil {
				return nil, err
			}
			add(string(edge.ID))
		}
	}
	return names, nil
}
