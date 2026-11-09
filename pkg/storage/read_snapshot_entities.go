// Author: #1683 implementation lane (Codex), 2026-11-01; ported to release/v1.4.4.
package storage

import (
	"context"
	"strings"

	"github.com/dgraph-io/badger/v4"
)

// ReadNode hydrates exactly one row through the native stream's item reader.
// A corrupt/hidden/missing row is absent, just as it is in that stream. No
// physical body cache is consulted or filled with an old snapshot's image.
func (s *badgerReadSnapshot) ReadNode(ctx context.Context, id NodeID) (*Node, error) {
	return snapshotEntity(ctx, s.view, id, s.prefix, s.nodesTouched, s.overlayNodes,
		func(node *Node) NodeID { return node.ID }, copyNode,
		func(node *Node) bool { return s.b.filterEntityByDecay(visibleRead, node, DecayScoringTime()) },
		func(txn *badger.Txn) (*Node, error) {
			item, err := txn.Get(nodeKey(id))
			if err != nil {
				return nil, err
			}
			node, err := s.b.readStreamNodeItem(ctx, txn, item, fullNodeStreamOptions(""), nil, DecayScoringTime(), false)
			if err != nil {
				return nil, err
			}
			return node, nil
		})
}

// ReadEdge uses the relationship stream's item reader at the capture timestamp.
func (s *badgerReadSnapshot) ReadEdge(ctx context.Context, id EdgeID) (*Edge, error) {
	return snapshotEntity(ctx, s.view, id, s.prefix, s.edgesTouched, s.overlayEdges,
		func(edge *Edge) EdgeID { return edge.ID }, copyEdge,
		func(edge *Edge) bool { return s.b.filterEntityByDecay(visibleRead, edge, DecayScoringTime()) },
		func(txn *badger.Txn) (*Edge, error) {
			item, err := txn.Get(edgeKey(id))
			if err != nil {
				return nil, err
			}
			edge, err := s.b.readStreamEdgeItem(txn, item)
			if edge != nil && s.b.filterEntityByDecay(visibleRead, edge, DecayScoringTime()) {
				edge = nil
			}
			return edge, err
		})
}

// snapshotEntity owns point admission for either kind. Even overlay reads go
// through withView so closure, retirement conflicts and engine lifetime have
// the same admission as the captured scans.
func snapshotEntity[ID ~string, T any](ctx context.Context, view *badgerReadView, id ID, prefix string, touched map[ID]bool, overlay []*T, key func(*T) ID, clone func(*T) *T, hidden func(*T) bool, load func(*badger.Txn) (*T, error)) (*T, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if id == "" || !strings.HasPrefix(string(id), prefix) {
		return nil, ErrNotFound
	}
	var value *T
	err := view.withView(ctx, func(txn *badger.Txn) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if touched[id] {
			for _, candidate := range overlay {
				if key(candidate) == id && !hidden(candidate) {
					value = clone(candidate)
					return nil
				}
			}
			return ErrNotFound
		}
		var err error
		value, err = load(txn)
		if err == badger.ErrKeyNotFound || (err == nil && value == nil) {
			return ErrNotFound
		}
		return err
	})
	return value, err
}

type namespacedEntityReadSnapshot struct {
	*namespacedReadSnapshot
	reader EntityReadSnapshot
}

func (s *namespacedEntityReadSnapshot) ReadNode(ctx context.Context, id NodeID) (*Node, error) {
	node, err := s.reader.ReadNode(ctx, s.n.PrefixNodeID(id))
	if err != nil || node == nil {
		return nil, err
	}
	return s.n.toUserNode(node, nodeUserView{}), nil
}

func (s *namespacedEntityReadSnapshot) ReadEdge(ctx context.Context, id EdgeID) (*Edge, error) {
	edge, err := s.reader.ReadEdge(ctx, s.n.PrefixEdgeID(id))
	if err != nil || edge == nil {
		return nil, err
	}
	return s.n.toUserEdge(edge), nil
}
