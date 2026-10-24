package storage

import (
	"encoding/binary"
	"errors"

	"github.com/dgraph-io/badger/v4"
)

type relationshipCacheEndpoint struct {
	id       NodeID
	outgoing bool
}

// Prepared once at write entry, before staging can replace its maps. This is
// an inventory of the write, never a search through the cache population.
type relationshipCacheInvalidations struct {
	edges     []*Edge
	bodies    []EdgeID
	endpoints []relationshipCacheEndpoint
	types     []string
}

// One key classifier serves both the allocation-free empty-scope check and
// normalization. Adding a relationship key kind cannot bypass preparation.
func relationshipCacheRawKeyKind(key string) byte {
	if len(key) < 2 {
		return 0
	}
	switch key[0] {
	case prefixEdge, prefixOutgoingIndex, prefixIncomingIndex, prefixEdgeTypeIndex:
		return key[0]
	}
	return 0
}

func rawScopeHasRelationshipKeys(writes map[string][]byte, deletes map[string]bool) bool {
	for key := range writes {
		if relationshipCacheRawKeyKind(key) != 0 {
			return true
		}
	}
	for key, deleting := range deletes {
		if deleting && relationshipCacheRawKeyKind(key) != 0 {
			return true
		}
	}
	return false
}

func operationsHaveRelationshipKeys(ops []Operation) bool {
	for _, op := range ops {
		if op.Edge != nil || op.OldEdge != nil || op.EdgeID != "" || len(op.DeletedEdgeIDs) > 0 {
			return true
		}
	}
	return false
}

func scopeHasRelationshipCacheWrites(scope nodeCacheWriteScope) bool {
	if scope.relationships != nil {
		return true
	}
	if scope.transaction != nil && operationsHaveRelationshipKeys(scope.transaction.operations) {
		return true
	}
	if rawScopeHasRelationshipKeys(scope.writes, scope.deletes) {
		return true
	}
	for _, commit := range scope.buffered {
		if operationsHaveRelationshipKeys(commit.ops) || rawScopeHasRelationshipKeys(commit.writes, commit.deletes) {
			return true
		}
	}
	return false
}

func (b *BadgerEngine) collectRelationshipCacheWrites(scope nodeCacheWriteScope, txn *badger.Txn) (*relationshipCacheInvalidations, error) {
	if !scope.known {
		return nil, nil
	}
	// Node/metadata commits do not construct visitors, maps or slices for the
	// relationship cache. This check precedes every potentially escaping
	// builder closure, preserving the base's allocation-free empty scope.
	if !scopeHasRelationshipCacheWrites(scope) {
		return nil, nil
	}
	var keys *relationshipCacheInvalidations
	ensure := func() {
		if keys == nil {
			keys = &relationshipCacheInvalidations{}
		}
	}
	// Every body key is classified at most once, regardless of how many raw
	// representations accompany its authoritative before/after operation.
	var covered map[EdgeID]uint8
	cover := func(id EdgeID, metadata bool) {
		if id == "" {
			return
		}
		if covered == nil {
			covered = make(map[EdgeID]uint8)
		}
		covered[id] |= 1
		if metadata {
			covered[id] |= 2
		}
	}
	visit := func(edge *Edge) {
		if edge != nil {
			ensure()
			keys.edges = append(keys.edges, edge)
			cover(edge.ID, true)
		}
	}
	if scope.relationships != nil {
		scope.relationships(visit)
	}
	operations := func(ops []Operation) {
		for _, op := range ops {
			visit(op.Edge)
			visit(op.OldEdge)
			if op.EdgeID != "" {
				ensure()
				keys.bodies = append(keys.bodies, op.EdgeID)
				cover(op.EdgeID, false)
			}
			for _, id := range op.DeletedEdgeIDs {
				ensure()
				keys.bodies = append(keys.bodies, id)
				cover(id, false)
			}
		}
	}
	if scope.transaction != nil {
		operations(scope.transaction.operations)
	}
	for _, commit := range scope.buffered {
		operations(commit.ops)
	}
	// A recovered generation can carry dictionary pairs which are not in the
	// engine yet. Resolve their numeric keys from this same write inventory;
	// never depend on replay having already populated an in-memory dictionary.
	var pendingNodes map[uint64]NodeID
	var pendingEdges map[uint64]EdgeID
	dictionary := func(writes map[string][]byte) {
		for key, value := range writes {
			if len(key) != 9 {
				continue
			}
			switch key[0] {
			case prefixIDDictNodeReverse:
				if pendingNodes == nil {
					pendingNodes = make(map[uint64]NodeID)
				}
				pendingNodes[binary.BigEndian.Uint64([]byte(key[1:]))] = NodeID(value)
			case prefixIDDictEdgeReverse:
				if pendingEdges == nil {
					pendingEdges = make(map[uint64]EdgeID)
				}
				pendingEdges[binary.BigEndian.Uint64([]byte(key[1:]))] = EdgeID(value)
			}
		}
	}
	dictionary(scope.writes)
	for _, commit := range scope.buffered {
		dictionary(commit.writes)
	}
	nodeID := func(num uint64) (NodeID, bool) {
		if id, ok := pendingNodes[num]; ok {
			return id, id != ""
		}
		return b.idDict.lookupNodeIDByNum(num)
	}
	operationCoversIndex := func(key string) bool {
		if len(key) < 9 {
			return false
		}
		num := binary.BigEndian.Uint64([]byte(key[len(key)-8:]))
		id, ok := pendingEdges[num]
		if !ok {
			id, ok = b.idDict.lookupEdgeIDByNum(num)
		}
		return ok && covered[id]&2 != 0
	}
	header := func(id EdgeID, data []byte) error {
		if len(data) == 0 {
			return ErrInvalidData
		}
		edge, start, end, _, err := decodeEdgeCompactHeader(data, data[0])
		if err != nil {
			return err
		}
		var ok bool
		edge.ID = id
		edge.StartNode, ok = nodeID(start)
		if !ok {
			return ErrInvalidData
		}
		edge.EndNode, ok = nodeID(end)
		if !ok {
			return ErrInvalidData
		}
		ensure()
		keys.edges = append(keys.edges, edge)
		return nil
	}
	body := func(id EdgeID, data []byte, deleting bool) error {
		if covered[id]&1 != 0 {
			return nil
		}
		foundOld := false
		// This read is BEFORE buffered physical writes. It enrolls the before
		// image in the transaction's conflict set, so a raw writer cannot silently
		// overwrite a peer's newer endpoints/type. No cache lock is held here.
		if txn != nil {
			item, err := txn.Get(edgeKey(id))
			if err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
				return err
			}
			if err == nil {
				if err := item.Value(func(old []byte) error { return header(id, old) }); err != nil {
					return err
				}
				foundOld = true
			}
		}
		if !deleting {
			if err := header(id, data); err != nil {
				return err
			}
		} else if txn == nil && !foundOld {
			return ErrInvalidData
		}
		// Do not mark raw data operation-covered: another buffered raw write of
		// this ID may supply a different type/endpoint and must contribute too.
		ensure()
		keys.bodies = append(keys.bodies, id)
		return nil
	}
	rawKey := func(key string, data []byte, deleting bool) error {
		switch relationshipCacheRawKeyKind(key) {
		case prefixEdge:
			return body(EdgeID(key[1:]), data, deleting)
		case prefixOutgoingIndex, prefixIncomingIndex:
			if len(key) == 17 {
				if operationCoversIndex(key) {
					return nil
				}
				id, ok := nodeID(binary.BigEndian.Uint64([]byte(key[1:9])))
				if !ok {
					return ErrInvalidData
				}
				ensure()
				keys.endpoints = append(keys.endpoints, relationshipCacheEndpoint{id, key[0] == prefixOutgoingIndex})
			}
		case prefixEdgeTypeIndex:
			if len(key) >= 10 && key[len(key)-9] == 0 {
				if operationCoversIndex(key) {
					return nil
				}
				ensure()
				keys.types = append(keys.types, key[1:len(key)-9])
			}
		}
		return nil
	}
	raw := func(writes map[string][]byte, deletes map[string]bool) error {
		for key, value := range writes {
			if err := rawKey(key, value, false); err != nil {
				return err
			}
		}
		for key, deleting := range deletes {
			if deleting {
				if err := rawKey(key, nil, true); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if err := raw(scope.writes, scope.deletes); err != nil {
		return nil, err
	}
	for _, commit := range scope.buffered {
		if err := raw(commit.writes, commit.deletes); err != nil {
			return nil, err
		}
	}
	return keys, nil
}

// Issuance only applies prepared keys. No cache enumeration, decoding, storage
// read or dictionary lookup occurs under these locks. Node is held by issuer;
// adjacency -> body -> type is the one lock order.
func (b *BadgerEngine) invalidateRelationshipCacheScope(scope nodeCacheWriteScope, ts uint64) {
	keys := scope.relationshipKeys
	if scope.known && keys == nil {
		return
	}
	b.adjCacheMu.Lock()
	defer b.adjCacheMu.Unlock()
	b.edgeCacheMu.Lock()
	defer b.edgeCacheMu.Unlock()
	b.edgeTypeCacheMu.Lock()
	defer b.edgeTypeCacheMu.Unlock()
	if !scope.known {
		b.revokeRelationshipCachesLocked(ts)
	} else {
		for _, edge := range keys.edges {
			b.revokeEdgeLocked(edge.ID, ts)
			b.revokeAdjacencyLocked(edge.StartNode, true, ts)
			b.revokeAdjacencyLocked(edge.EndNode, false, ts)
			b.revokeEdgeTypeLocked(edge.Type, ts)
		}
		for _, id := range keys.bodies {
			b.revokeEdgeLocked(id, ts)
		}
		for _, endpoint := range keys.endpoints {
			b.revokeAdjacencyLocked(endpoint.id, endpoint.outgoing, ts)
		}
		for _, kind := range keys.types {
			b.revokeEdgeTypeLocked(kind, ts)
		}
	}
	b.edgeCacheGen.advance()
	b.adjCacheGen.advance()
	b.edgeTypeCacheGen.advance()
	if b.edgeCacheMaxItems > 0 && len(b.edgeCache) > b.edgeCacheMaxItems {
		b.resetEdgeCacheLocked()
	}
	if b.adjCacheMaxNodes > 0 && (len(b.outgoingAdjCache) > b.adjCacheMaxNodes || len(b.incomingAdjCache) > b.adjCacheMaxNodes) {
		b.resetAdjacencyCacheLocked()
	}
	if b.edgeTypeCacheMaxTypes > 0 && len(b.edgeTypeCache) > b.edgeTypeCacheMaxTypes {
		b.resetEdgeTypeCacheLocked()
	}
}
