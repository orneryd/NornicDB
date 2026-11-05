package search

import (
	"context"
	"sync"
)

// vectorMemberIndex publishes immutable member lists when node indexing changes.
// A scoring pass borrows the complete map under one read lock; it neither
// reconstructs membership nor allocates a query-local membership cache.
// Writers hold indexMu and publish after vector/graph mutation locks are released.
// Readers never acquire Service locks while borrowing this map.
type vectorMemberIndex struct {
	mu    sync.RWMutex
	nodes map[string][]string
	// Only nodes that need more than their own main vector. Published by the
	// same writer as nodes, so weak main vectors need no full membership lookup.
	expanded map[string]struct{}
}

type vectorMemberIndexKey struct{}

func withVectorMemberIndex(ctx context.Context, index *vectorMemberIndex) context.Context {
	return context.WithValue(ctx, vectorMemberIndexKey{}, index)
}

// indexedNodeMembersLocked combines the representation-specific write metadata
// once, at mutation/build time. Published chunk lists are immutable and can be
// shared when the node has no named or property vectors.
func (s *Service) indexedNodeMembersLocked(nodeID string) []string {
	chunks, named, props := s.nodeChunkVectors[nodeID], s.nodeNamedVector[nodeID], s.nodePropVector[nodeID]
	if len(named)+len(props) == 0 {
		return chunks
	}
	ids := make([]string, 0, len(chunks)+len(named)+len(props))
	ids = append(ids, chunks...)
	for _, id := range named {
		ids = append(ids, id)
	}
	for _, id := range props {
		ids = append(ids, id)
	}
	return ids
}

func (s *Service) publishNodeMembersLocked(nodeID string) {
	ids := s.indexedNodeMembersLocked(nodeID)
	index := &s.vectorMembers
	index.mu.Lock()
	defer index.mu.Unlock()
	if len(ids) == 0 {
		delete(index.nodes, nodeID)
		delete(index.expanded, nodeID)
		return
	}
	if index.nodes == nil {
		index.nodes = make(map[string][]string)
	}
	index.nodes[nodeID] = ids
	index.setExpandedLocked(nodeID, ids)
}

func (s *Service) rebuildNodeMembersLocked() {
	index := &s.vectorMembers
	index.mu.Lock()
	defer index.mu.Unlock()
	index.nodes = make(map[string][]string, len(s.nodeChunkVectors))
	clear(index.expanded)
	for id := range s.nodeChunkVectors {
		index.nodes[id] = s.indexedNodeMembersLocked(id)
	}
	for id := range s.nodeNamedVector {
		if _, ok := index.nodes[id]; !ok {
			index.nodes[id] = s.indexedNodeMembersLocked(id)
		}
	}
	for id := range s.nodePropVector {
		if _, ok := index.nodes[id]; !ok {
			index.nodes[id] = s.indexedNodeMembersLocked(id)
		}
	}
	for id, ids := range index.nodes {
		index.setExpandedLocked(id, ids)
	}
}

func (index *vectorMemberIndex) setExpandedLocked(id string, ids []string) {
	if len(ids) == 1 && ids[0] == nodeMainVectorKey(id) {
		delete(index.expanded, id)
		return
	}
	if index.expanded == nil {
		index.expanded = make(map[string]struct{})
	}
	index.expanded[id] = struct{}{}
}

func (index *vectorMemberIndex) clear() {
	index.mu.Lock()
	clear(index.nodes)
	clear(index.expanded)
	index.mu.Unlock()
}
