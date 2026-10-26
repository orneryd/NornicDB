// MERGE clause implementation for NornicDB.
// This file contains MERGE execution, compound queries, and context-aware operations.

package cypher

import (
	"context"
	"errors"
	"sort"

	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func mergeNodeHasLabels(node *storage.Node, labels []string) bool {
	for _, label := range labels {
		found := false
		for _, nodeLabel := range node.Labels {
			if nodeLabel == label {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func mergeNodeMatches(node *storage.Node, labels []string, props map[string]interface{}) bool {
	if node == nil {
		return false
	}
	if len(labels) > 0 && !mergeNodeHasLabels(node, labels) {
		return false
	}
	return nodePropertiesMatch(node, props)
}

func mergeCreateConflict(err error) bool {
	if errors.Is(err, storage.ErrAlreadyExists) {
		return true
	}
	return strings.Contains(lowerASCII(err.Error()), "already exists")
}

func mergeLookupCacheKey(labels []string, prop string, val interface{}) string {
	return unwindMergeKey(unwindMergeLabelsKey(labels), map[string]interface{}{prop: val})
}

func cloneNodePropertiesMap(in map[string]interface{}) map[string]interface{} {
	out := make(map[string]interface{}, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func (e *StorageExecutor) evictMergeNodeCacheEntries(labels []string, props map[string]interface{}, nodeID storage.NodeID) {
	if len(labels) == 0 || len(props) == 0 {
		return
	}
	e.ensureNodeLookupCache()

	cacheMu := e.nodeLookupCacheLock()
	cacheMu.Lock()
	defer cacheMu.Unlock()
	for prop, val := range props {
		key := mergeLookupCacheKey(labels, prop, val)
		cached, ok := e.nodeLookupCache[key]
		if !ok || cached == nil {
			continue
		}
		if nodeID == "" || cached.ID == nodeID {
			delete(e.nodeLookupCache, key)
		}
	}
}

func (e *StorageExecutor) findMergeNodeInCache(store storage.Engine, labels []string, props map[string]interface{}) *storage.Node {
	if len(labels) == 0 || len(props) == 0 {
		return nil
	}
	e.ensureNodeLookupCache()

	var cachedNode *storage.Node
	cacheMu := e.nodeLookupCacheLock()
	cacheMu.RLock()
	for prop, val := range props {
		if cached, ok := e.nodeLookupCache[mergeLookupCacheKey(labels, prop, val)]; ok {
			if mergeNodeMatches(cached, labels, props) {
				cachedNode = cached
				break
			}
		}
	}
	cacheMu.RUnlock()

	if cachedNode == nil {
		return nil
	}
	if store == nil {
		return cachedNode
	}

	liveNode, err := e.getQueryNode(store, cachedNode.ID)
	if err == nil && mergeNodeMatches(liveNode, labels, props) {
		return liveNode
	}

	e.evictMergeNodeCacheEntries(labels, props, cachedNode.ID)
	return nil
}

func (e *StorageExecutor) cacheMergeNode(labels []string, props map[string]interface{}, node *storage.Node) {
	if node == nil || len(labels) == 0 || len(props) == 0 {
		return
	}
	cacheMu := e.nodeLookupCacheLock()
	cacheMu.Lock()
	defer cacheMu.Unlock()
	for prop, val := range props {
		e.nodeLookupCache[mergeLookupCacheKey(labels, prop, val)] = node
	}
}

// prepareMergeKeys readies a transaction for a MERGE lookup of a node with
// labels and props, so concurrent MERGEs of a uniquely constrained key behave
// as in Neo4j: one creates the node and the others wait for it and match it
// (#961). It uses the storage.MergeNodePreparer capability of the statement's
// storage, so every transactional view admits keys the same way. A MERGE whose
// statement writes the node it matches (write) also waits for a concurrent
// writer of the node, then writes on top of its commit
// (PrepareMergeNodeForWrite, NOR-117). Storage without a transaction admits
// nothing.
func prepareMergeKeys(ctx context.Context, store storage.Engine, labels []string, props map[string]interface{}, write bool) error {
	preparer, ok := storage.OwnedCapability[storage.MergeNodePreparer](store)
	if !ok || len(labels) == 0 || len(props) == 0 {
		return nil
	}
	if write {
		return preparer.PrepareMergeNodeForWrite(ctx, labels, props)
	}
	return preparer.PrepareMergeNode(ctx, labels, props)
}

func (e *StorageExecutor) loadMergeCandidateNodes(ctx context.Context, store storage.Engine, ids []storage.NodeID) ([]*storage.Node, error) {
	if len(ids) == 0 {
		return nil, ctx.Err()
	}
	out := make([]*storage.Node, 0, len(ids))
	seen := make(map[storage.NodeID]struct{}, len(ids))
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if _, exists := seen[id]; exists {
			continue
		}
		seen[id] = struct{}{}
		n, err := e.getQueryNode(store, id)
		if err != nil && !errors.Is(err, storage.ErrNotFound) {
			return nil, err
		}
		if n == nil {
			continue
		}
		out = append(out, n)
	}
	return out, ctx.Err()
}

func mergePropertyNamesSorted(props map[string]interface{}) []string {
	names := make([]string, 0, len(props))
	for prop := range props {
		names = append(names, prop)
	}
	sort.Strings(names)
	return names
}

func mergeIndexMatchesAllProperties(idx *storage.CompositeIndex, props map[string]interface{}) bool {
	if idx == nil || len(idx.Properties) != len(props) {
		return false
	}
	for _, prop := range idx.Properties {
		if _, ok := props[prop]; !ok {
			return false
		}
	}
	return true
}

func compositeLookupValues(idx *storage.CompositeIndex, props map[string]interface{}) []interface{} {
	values := make([]interface{}, 0, len(idx.Properties))
	for _, prop := range idx.Properties {
		values = append(values, props[prop])
	}
	return values
}

func (e *StorageExecutor) findMergeNode(ctx context.Context, store storage.Engine, labels []string, props map[string]interface{}) (*storage.Node, error) {
	if len(props) == 0 {
		var candidates []*storage.Node
		var err error
		if len(labels) > 0 {
			candidates, err = storage.GetNodesByLabelWithContext(ctx, store, labels[0])
		} else {
			candidates, err = storage.AllNodesWithContext(ctx, store)
		}
		if err != nil {
			return nil, err
		}
		for _, node := range candidates {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if mergeNodeMatches(node, labels, props) {
				return node, nil
			}
		}
		return nil, nil
	}
	if len(labels) == 0 {
		candidates, err := storage.AllNodesWithContext(ctx, store)
		if err != nil {
			return nil, err
		}
		for _, node := range candidates {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if mergeNodeMatches(node, labels, props) {
				return node, nil
			}
		}
		return nil, nil
	}

	if cached := e.findMergeNodeInCache(store, labels, props); cached != nil {
		e.markMergeSchemaLookupUsed()
		return cached, nil
	}

	// Schema hot path: prefer exact composite lookups for full-key MERGE patterns,
	// then fall back to the smallest single-property candidate set.
	schema := store.GetSchema()
	schemaLookupUsed := false
	if schema != nil {
		label := labels[0]

		indexes, selectionErr := schema.SeekableCompositeIndexesForLabelWithContext(ctx, label)
		if selectionErr != nil {
			return nil, selectionErr
		}
		for _, idx := range indexes {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if !mergeIndexMatchesAllProperties(idx, props) {
				continue
			}
			schemaLookupUsed = true
			e.markMergeSchemaLookupUsed()
			ids, lookupErr := e.compositeIndexLookup(ctx, store, idx, compositeLookupValues(idx, props), true)
			if lookupErr != nil {
				return nil, lookupErr
			}
			candidateNodes, loadErr := e.loadMergeCandidateNodes(ctx, store, ids)
			if loadErr != nil {
				return nil, loadErr
			}
			for _, n := range candidateNodes {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
				if mergeNodeMatches(n, labels, props) {
					e.cacheMergeNode(labels, props, n)
					return n, nil
				}
			}
		}

		for _, prop := range mergePropertyNamesSorted(props) {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			val := props[prop]
			nodeID, valueFound, constraintExists, cacheComplete, lookupErr := schema.LookupUniqueConstraintValueForPlanningWithContext(ctx, label, prop, val)
			if lookupErr != nil {
				return nil, lookupErr
			}
			if !constraintExists {
				continue
			}
			e.markMergeSchemaLookupUsed()
			if !valueFound {
				if cacheComplete {
					schemaLookupUsed = true
				}
				continue
			}
			candidateNodes, loadErr := e.loadMergeCandidateNodes(ctx, store, []storage.NodeID{nodeID})
			if loadErr != nil {
				return nil, loadErr
			}
			for _, n := range candidateNodes {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
				if mergeNodeMatches(n, labels, props) {
					e.cacheMergeNode(labels, props, n)
					return n, nil
				}
			}
			if cacheComplete {
				schemaLookupUsed = true
			}
		}

		bestIDs := []storage.NodeID(nil)
		bestCount := -1
		for _, prop := range mergePropertyNamesSorted(props) {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			val := props[prop]
			if _, ok := schema.GetPropertyIndex(label, prop); !ok {
				continue
			}
			schemaLookupUsed = true
			e.markMergeSchemaLookupUsed()
			ids, lookupErr := e.propertyIndexLookup(ctx, store, schema, label, prop, val)
			if lookupErr != nil {
				return nil, lookupErr
			}
			count := len(ids)
			if bestCount == -1 || count < bestCount {
				bestIDs = ids
				bestCount = count
				if count <= 1 {
					break
				}
			}
		}
		candidateNodes, loadErr := e.loadMergeCandidateNodes(ctx, store, bestIDs)
		if loadErr != nil {
			return nil, loadErr
		}
		for _, n := range candidateNodes {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if mergeNodeMatches(n, labels, props) {
				e.cacheMergeNode(labels, props, n)
				return n, nil
			}
		}
	}
	if schemaLookupUsed {
		return nil, nil
	}
	e.markMergeScanFallbackUsed()

	nodes, err := storage.GetNodesByLabelWithContext(ctx, store, labels[0])
	if err != nil {
		return nil, err
	}
	for _, node := range nodes {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if mergeNodeMatches(node, labels, props) {
			e.cacheMergeNode(labels, props, node)
			return node, nil
		}
	}

	// No global AllNodes() fallback (#640, #694): the label index and schema
	// lookups are transactionally maintained and authoritative. Scanning every
	// node in the database for each creating MERGE row made bulk MERGE
	// ingestion quadratic (85 ms per row at 20k nodes) while Neo4j bounds the
	// lookup to the label's own scan. A missed label means no match, exactly
	// as in Neo4j with an out-of-date index.
	return nil, nil
}

// findMergeNodesScanned is findMergeNodes, also reporting whether it read
// the pattern's whole label (or its complete pinned equality population). Only such an
// answer is complete: a node any other lookup (the MERGE cache, a schema
// index) could return carries the label, so a scan that found none means
// there is none. An index answer is not; findMergeNode tries further
// sources after it.
func (e *StorageExecutor) findMergeNodesScanned(ctx context.Context, store storage.Engine, labels []string, props map[string]interface{}) (matches []*storage.Node, scanned bool, err error) {
	ids, indexed, lookupErr := e.mergeNodeIndexedCandidateIDs(ctx, store, labels, props)
	if lookupErr != nil {
		return nil, false, lookupErr
	}
	if indexed {
		matches = make([]*storage.Node, 0, len(ids))
		candidateNodes, loadErr := e.loadMergeCandidateNodes(ctx, store, ids)
		if loadErr != nil {
			return nil, false, loadErr
		}
		for _, n := range candidateNodes {
			if err := ctx.Err(); err != nil {
				return nil, false, err
			}
			if mergeNodeMatches(n, labels, props) {
				matches = append(matches, n)
			}
		}
		return matches, false, nil
	}
	if len(labels) > 0 && len(props) > 0 {
		candidates, complete, lookupErr := storage.LookupNodesByEqualityWithContext(ctx, store, labels[0], props)
		if lookupErr != nil {
			return nil, false, lookupErr
		}
		if complete {
			// The selected capability owns the issued view and its namespace projection.
			e.markMergeScanFallbackUsed()
			for _, candidate := range candidates {
				if err := ctx.Err(); err != nil {
					return nil, false, err
				}
				if mergeNodeMatches(candidate, labels, props) {
					matches = append(matches, candidate)
				}
			}
			return matches, true, nil
		}
	}
	var candidates []*storage.Node
	if len(labels) > 0 && len(props) > 0 {
		e.markMergeScanFallbackUsed()
	}
	if len(labels) > 0 {
		candidates, err = storage.GetNodesByLabelWithContext(ctx, store, labels[0])
	} else {
		candidates, err = storage.AllNodesWithContext(ctx, store)
	}
	if err != nil {
		return nil, false, err
	}

	matches = make([]*storage.Node, 0, len(candidates))
	for _, candidate := range candidates {
		if err := ctx.Err(); err != nil {
			return nil, false, err
		}
		if mergeNodeMatches(candidate, labels, props) {
			matches = append(matches, candidate)
		}
	}
	return matches, true, nil
}

// mergeNodeIndexedCandidateIDs returns the candidate node IDs for a MERGE node
// pattern from the schema, and whether the schema answered: a unique
// constraint on one of the pattern's properties (at most one node holds a
// value), a composite index covering every pattern property, or the smallest
// single-property index result. When it answers, every node matching the
// pattern is among the candidates, so findMergeNodes needs no label scan
// (#640, #694). It answers nothing for a pattern without labels or properties.
func (e *StorageExecutor) mergeNodeIndexedCandidateIDs(ctx context.Context, store storage.Engine, labels []string, props map[string]interface{}) ([]storage.NodeID, bool, error) {
	if len(labels) == 0 || len(props) == 0 {
		return nil, false, nil
	}
	schema := store.GetSchema()
	if schema == nil {
		return nil, false, nil
	}
	label := labels[0]
	for _, prop := range mergePropertyNamesSorted(props) {
		nodeID, valueFound, constraintExists, cacheComplete, lookupErr := schema.LookupUniqueConstraintValueForPlanningWithContext(ctx, label, prop, props[prop])
		if lookupErr != nil {
			return nil, false, lookupErr
		}
		if !constraintExists {
			continue
		}
		if valueFound {
			e.markMergeSchemaLookupUsed()
			return []storage.NodeID{nodeID}, true, nil
		}
		if cacheComplete {
			e.markMergeSchemaLookupUsed()
			return nil, true, nil
		}
	}
	indexes, selectionErr := schema.SeekableCompositeIndexesForLabelWithContext(ctx, label)
	if selectionErr != nil {
		return nil, false, selectionErr
	}
	for _, idx := range indexes {
		if mergeIndexMatchesAllProperties(idx, props) {
			e.markMergeSchemaLookupUsed()
			ids, err := e.compositeIndexLookup(ctx, store, idx, compositeLookupValues(idx, props), true)
			return ids, true, err
		}
	}
	var best []storage.NodeID
	found := false
	for _, prop := range mergePropertyNamesSorted(props) {
		if _, ok := schema.GetPropertyIndex(label, prop); !ok {
			continue
		}
		ids, err := e.propertyIndexLookup(ctx, store, schema, label, prop, props[prop])
		if err != nil {
			return nil, false, err
		}
		if !found || len(ids) < len(best) {
			best, found = ids, true
		}
	}
	if found {
		e.markMergeSchemaLookupUsed()
	}
	return best, found, nil
}

// parseMergePattern parses a MERGE pattern like "(n:Label {prop: value})"
