// MERGE clause implementation for NornicDB.
// This file contains MERGE execution, compound queries, and context-aware operations.

package cypher

import (
	"context"
	"errors"
	"github.com/orneryd/nornicdb/internal/queryprofile"
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

func (e *StorageExecutor) findMergeNodeInCache(ctx context.Context, store storage.Engine, labels []string, props map[string]interface{}) *storage.Node {
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

	liveNode, err := e.getQueryNode(ctx, store, cachedNode.ID)
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
	preparer, ok := store.(storage.MergeNodePreparer)
	if !ok || len(labels) == 0 || len(props) == 0 {
		return nil
	}
	if write {
		return preparer.PrepareMergeNodeForWrite(ctx, labels, props)
	}
	return preparer.PrepareMergeNode(ctx, labels, props)
}

func (e *StorageExecutor) loadMergeCandidateNodes(ctx context.Context, store storage.Engine, ids []storage.NodeID) ([]*storage.Node, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return nil, nil
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
		n, err := e.getQueryNode(ctx, store, id)
		if err != nil || n == nil {
			continue
		}
		out = append(out, n)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return out, nil
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

// mergeCandidatePopulation reads the node population a MERGE compares its
// pattern against when no index answers for it - the smallest written label's
// nodes, or every node for a pattern without labels - and reports it before the
// residual pattern matching narrows it. The record is the canonical scan it is,
// under the same kind and label the clause scanner reports, so a MERGE that
// admitted one node of five reports the five it read.
func mergeCandidatePopulation(ctx context.Context, store storage.Engine, labels []string) ([]*storage.Node, error) {
	kind, detail := profileKindAllNodesScan, ""
	var (
		candidates []*storage.Node
		err        error
	)
	if len(labels) > 0 {
		kind, detail = profileKindNodeByLabelScan, nodeScanLabels(ctx, store, labels)[0]
		candidates, err = store.GetNodesByLabel(ctx, detail)
	} else {
		candidates, err = store.AllNodes(ctx)
	}
	if err != nil {
		return nil, err
	}
	queryprofile.EntityPopulation(ctx, kind, detail, candidates)
	return candidates, nil
}

func (e *StorageExecutor) findMergeNode(ctx context.Context, store storage.Engine, labels []string, props map[string]interface{}) (*storage.Node, error) {
	if len(props) == 0 {
		candidates, err := mergeCandidatePopulation(ctx, store, labels)
		if err != nil {
			return nil, err
		}
		for _, node := range candidates {
			if mergeNodeMatches(node, labels, props) {
				return node, nil
			}
		}
		return nil, nil
	}
	if len(labels) == 0 {
		candidates, err := mergeCandidatePopulation(ctx, store, labels)
		if err != nil {
			return nil, err
		}
		for _, node := range candidates {
			if mergeNodeMatches(node, labels, props) {
				return node, nil
			}
		}
		return nil, nil
	}

	if cached := e.findMergeNodeInCache(ctx, store, labels, props); cached != nil {
		e.markMergeSchemaLookupUsed()
		return cached, nil
	}

	// Schema hot path: prefer exact composite lookups for full-key MERGE patterns,
	// then fall back to the smallest single-property candidate set.
	schema := store.GetSchema()
	schemaLookupUsed := false
	if schema != nil {
		label := labels[0]

		indexes, err := schema.SeekableCompositeIndexesForLabel(ctx, label)
		if err != nil {
			return nil, err
		}
		for _, idx := range indexes {
			if !mergeIndexMatchesAllProperties(idx, props) {
				continue
			}
			schemaLookupUsed = true
			e.markMergeSchemaLookupUsed()
			ids, err := compositeIndexLookup(ctx, store, schema, idx, compositeLookupValues(idx, props), true)
			if err != nil {
				return nil, err
			}
			candidateNodes, err := e.loadMergeCandidateNodes(ctx, store, ids)
			if err != nil {
				return nil, err
			}
			for _, n := range candidateNodes {
				if mergeNodeMatches(n, labels, props) {
					e.cacheMergeNode(labels, props, n)
					return n, nil
				}
			}
		}

		for _, prop := range mergePropertyNamesSorted(props) {
			val := props[prop]
			nodeID, valueFound, constraintExists, cacheComplete, err := schema.LookupUniqueConstraintValue(ctx, label, prop, val)
			if err != nil {
				return nil, err
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
			candidates, err := e.loadMergeCandidateNodes(ctx, store, []storage.NodeID{nodeID})
			if err != nil {
				return nil, err
			}
			for _, n := range candidates {
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
			val := props[prop]
			indexed, err := schema.HasPropertyIndex(ctx, label, prop)
			if err != nil {
				return nil, err
			}
			if !indexed {
				continue
			}
			schemaLookupUsed = true
			e.markMergeSchemaLookupUsed()
			ids, err := propertyIndexLookup(ctx, store, schema, label, prop, val)
			if err != nil {
				return nil, err
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
		candidates, err := e.loadMergeCandidateNodes(ctx, store, bestIDs)
		if err != nil {
			return nil, err
		}
		for _, n := range candidates {
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

	// The release scanned the first written label; any pattern label lists
	// every match, so the one with the fewest nodes is scanned (NOR-414).
	nodes, err := mergeCandidatePopulation(ctx, store, labels)
	if err != nil {
		return nil, err
	}
	for _, node := range nodes {
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
	ids, indexed, err := e.mergeNodeIndexedCandidateIDs(ctx, store, labels, props)
	if err != nil {
		return nil, false, err
	}
	if indexed {
		matches = make([]*storage.Node, 0, len(ids))
		candidates, err := e.loadMergeCandidateNodes(ctx, store, ids)
		if err != nil {
			return nil, false, err
		}
		for _, n := range candidates {
			if mergeNodeMatches(n, labels, props) {
				matches = append(matches, n)
			}
		}
		return matches, false, nil
	}
	lookup, hasEqualityLookup := store.(storage.TransactionEqualityLookup)
	wrapper, transactional := store.(*transactionStorageWrapper)
	if transactional {
		lookup, hasEqualityLookup = wrapper.tx, true
	}
	if hasEqualityLookup && len(labels) > 0 && len(props) > 0 {
		// Resolve the same native transaction owner used by property/composite
		// lookups; namespace binding and user projection belong to this query.
		if transactional && wrapper.namespace != "" {
			if err := wrapper.tx.SetNamespace(wrapper.namespace); err != nil {
				return nil, false, err
			}
		}
		// The read's population is the pattern label's own: the same label the
		// scan below would read.
		label := nodeScanLabels(ctx, store, labels)[0]
		candidates, complete, lookupErr := lookup.LookupNodesByEquality(ctx, label, props)
		if lookupErr != nil {
			return nil, false, lookupErr
		}
		if complete {
			if transactional && wrapper.namespace != "" {
				candidates, lookupErr = projectTransactionRead(ctx, wrapper, candidates, nil, readScanned)
				if lookupErr != nil {
					return nil, false, lookupErr
				}
			}
			e.markMergeScanFallbackUsed()
			// This equality lookup returned candidates, not the whole label.
			// Count the delivered population before the language predicate;
			// do not describe the selective lookup as a label scan.
			queryprofile.EntityPopulation(ctx, "NodeEqualityLookup", label, candidates)
			for _, candidate := range candidates {
				if mergeNodeMatches(candidate, labels, props) {
					matches = append(matches, candidate)
				}
			}
			return matches, true, nil
		}
	}
	if len(labels) > 0 && len(props) > 0 {
		e.markMergeScanFallbackUsed()
	}
	candidates, err := mergeCandidatePopulation(ctx, store, labels)
	if err != nil {
		return nil, false, err
	}

	matches = make([]*storage.Node, 0, len(candidates))
	for _, candidate := range candidates {
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
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if len(labels) == 0 || len(props) == 0 {
		return nil, false, nil
	}
	schema := store.GetSchema()
	if schema == nil {
		return nil, false, nil
	}
	label := labels[0]
	for _, prop := range mergePropertyNamesSorted(props) {
		nodeID, valueFound, constraintExists, cacheComplete, err := schema.LookupUniqueConstraintValue(ctx, label, prop, props[prop])
		if err != nil {
			return nil, false, err
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
	indexes, err := schema.SeekableCompositeIndexesForLabel(ctx, label)
	if err != nil {
		return nil, false, err
	}
	for _, idx := range indexes {
		if mergeIndexMatchesAllProperties(idx, props) {
			e.markMergeSchemaLookupUsed()
			ids, err := compositeIndexLookup(ctx, store, schema, idx, compositeLookupValues(idx, props), true)
			return ids, true, err
		}
	}
	var best []storage.NodeID
	found := false
	for _, prop := range mergePropertyNamesSorted(props) {
		indexed, err := schema.HasPropertyIndex(ctx, label, prop)
		if err != nil {
			return nil, false, err
		}
		if !indexed {
			continue
		}
		ids, err := propertyIndexLookup(ctx, store, schema, label, prop, props[prop])
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
