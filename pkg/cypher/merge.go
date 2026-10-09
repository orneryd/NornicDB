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

func mergeNodeHasAnyLabel(node *storage.Node, labels []string) bool {
	for _, label := range labels {
		for _, nodeLabel := range node.Labels {
			if nodeLabel == label {
				return true
			}
		}
	}
	return false
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

func mergeNodeMatchesAnyLabel(node *storage.Node, labels []string, props map[string]interface{}) bool {
	if node == nil {
		return false
	}
	if len(labels) > 0 && !mergeNodeHasAnyLabel(node, labels) {
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

func mergePropsContainUnresolvedParamLiteral(props map[string]interface{}) bool {
	for _, val := range props {
		s, ok := val.(string)
		if !ok {
			continue
		}
		if strings.HasPrefix(strings.TrimSpace(s), "$") {
			return true
		}
	}
	return false
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

func cloneNodeForMergeMutation(in *storage.Node) *storage.Node {
	if in == nil {
		return nil
	}
	out := *in
	if in.Labels != nil {
		out.Labels = append([]string(nil), in.Labels...)
	}
	if in.Properties != nil {
		out.Properties = cloneNodePropertiesMap(in.Properties)
	}
	if in.NamedEmbeddings != nil {
		out.NamedEmbeddings = make(map[string][]float32, len(in.NamedEmbeddings))
		for k, v := range in.NamedEmbeddings {
			out.NamedEmbeddings[k] = append([]float32(nil), v...)
		}
	}
	if in.ChunkEmbeddings != nil {
		out.ChunkEmbeddings = make([][]float32, len(in.ChunkEmbeddings))
		for i, v := range in.ChunkEmbeddings {
			out.ChunkEmbeddings[i] = append([]float32(nil), v...)
		}
	}
	if in.EmbedMeta != nil {
		out.EmbedMeta = make(map[string]any, len(in.EmbedMeta))
		for k, v := range in.EmbedMeta {
			out.EmbedMeta[k] = v
		}
	}
	return &out
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

	liveNode, err := store.GetNode(cachedNode.ID)
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
// (storage.BadgerTransaction.PrepareMergeKey, #961). Outside a transaction it
// does nothing.
func prepareMergeKeys(ctx context.Context, store storage.Engine, labels []string, props map[string]interface{}) error {
	wrapper, ok := store.(*transactionStorageWrapper)
	if !ok || wrapper.tx == nil || len(labels) == 0 || len(props) == 0 {
		return nil
	}
	for _, label := range labels {
		for _, property := range mergePropertyNamesSorted(props) {
			if err := wrapper.tx.PrepareMergeKey(ctx, label, property, props[property]); err != nil {
				return err
			}
		}
	}
	return nil
}

func (e *StorageExecutor) loadMergeCandidateNodes(store storage.Engine, ids []storage.NodeID) []*storage.Node {
	if len(ids) == 0 {
		return nil
	}
	out := make([]*storage.Node, 0, len(ids))
	seen := make(map[storage.NodeID]struct{}, len(ids))
	for _, id := range ids {
		if _, exists := seen[id]; exists {
			continue
		}
		seen[id] = struct{}{}
		n, err := store.GetNode(id)
		if err != nil || n == nil {
			continue
		}
		out = append(out, n)
	}
	return out
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

func (e *StorageExecutor) findMergeNode(store storage.Engine, labels []string, props map[string]interface{}) (*storage.Node, error) {
	if len(props) == 0 {
		var candidates []*storage.Node
		var err error
		if len(labels) > 0 {
			candidates, err = store.GetNodesByLabel(labels[0])
		} else {
			candidates, err = store.AllNodes()
		}
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
		candidates, err := store.AllNodes()
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

		for _, idx := range schema.SeekableCompositeIndexesForLabel(label) {
			if !mergeIndexMatchesAllProperties(idx, props) {
				continue
			}
			schemaLookupUsed = true
			e.markMergeSchemaLookupUsed()
			candidateNodes := e.loadMergeCandidateNodes(store, compositeIndexLookup(store, idx, compositeLookupValues(idx, props), true))
			for _, n := range candidateNodes {
				if mergeNodeMatches(n, labels, props) {
					e.cacheMergeNode(labels, props, n)
					return n, nil
				}
			}
		}

		for _, prop := range mergePropertyNamesSorted(props) {
			val := props[prop]
			nodeID, valueFound, constraintExists, cacheComplete := schema.LookupUniqueConstraintValueForPlanning(label, prop, val)
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
			for _, n := range e.loadMergeCandidateNodes(store, []storage.NodeID{nodeID}) {
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
			if _, ok := schema.GetPropertyIndex(label, prop); !ok {
				continue
			}
			schemaLookupUsed = true
			e.markMergeSchemaLookupUsed()
			ids := propertyIndexLookup(store, schema, label, prop, val)
			count := len(ids)
			if bestCount == -1 || count < bestCount {
				bestIDs = ids
				bestCount = count
				if count <= 1 {
					break
				}
			}
		}
		for _, n := range e.loadMergeCandidateNodes(store, bestIDs) {
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

	nodes, err := store.GetNodesByLabel(labels[0])
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

// findMergeNodes returns every node that satisfies a MERGE node pattern.
// MERGE is a row-producing clause: when an unbound pattern has multiple
// existing matches, every match must continue through the remaining clauses.
// Callers that only need existence may continue to use findMergeNode.
func (e *StorageExecutor) findMergeNodes(store storage.Engine, labels []string, props map[string]interface{}) ([]*storage.Node, error) {
	matches, _, err := e.findMergeNodesScanned(store, labels, props)
	return matches, err
}

// findMergeNodesScanned is findMergeNodes, also reporting whether it read
// the pattern's whole label (or every node, without a label). Only such an
// answer is complete: a node any other lookup (the MERGE cache, a schema
// index) could return carries the label, so a scan that found none means
// there is none. An index answer is not; findMergeNode tries further
// sources after it.
func (e *StorageExecutor) findMergeNodesScanned(store storage.Engine, labels []string, props map[string]interface{}) (matches []*storage.Node, scanned bool, err error) {
	if ids, indexed := e.mergeNodeIndexedCandidateIDs(store, labels, props); indexed {
		matches = make([]*storage.Node, 0, len(ids))
		for _, n := range e.loadMergeCandidateNodes(store, ids) {
			if mergeNodeMatches(n, labels, props) {
				matches = append(matches, n)
			}
		}
		return matches, false, nil
	}
	var candidates []*storage.Node
	if len(labels) > 0 && len(props) > 0 {
		e.markMergeScanFallbackUsed()
	}
	if len(labels) > 0 {
		candidates, err = store.GetNodesByLabel(labels[0])
	} else {
		candidates, err = store.AllNodes()
	}
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
func (e *StorageExecutor) mergeNodeIndexedCandidateIDs(store storage.Engine, labels []string, props map[string]interface{}) ([]storage.NodeID, bool) {
	if len(labels) == 0 || len(props) == 0 {
		return nil, false
	}
	schema := store.GetSchema()
	if schema == nil {
		return nil, false
	}
	label := labels[0]
	for _, prop := range mergePropertyNamesSorted(props) {
		nodeID, valueFound, constraintExists, cacheComplete := schema.LookupUniqueConstraintValueForPlanning(label, prop, props[prop])
		if !constraintExists {
			continue
		}
		if valueFound {
			e.markMergeSchemaLookupUsed()
			return []storage.NodeID{nodeID}, true
		}
		if cacheComplete {
			e.markMergeSchemaLookupUsed()
			return nil, true
		}
	}
	for _, idx := range schema.SeekableCompositeIndexesForLabel(label) {
		if mergeIndexMatchesAllProperties(idx, props) {
			e.markMergeSchemaLookupUsed()
			return compositeIndexLookup(store, idx, compositeLookupValues(idx, props), true), true
		}
	}
	var best []storage.NodeID
	found := false
	for _, prop := range mergePropertyNamesSorted(props) {
		if _, ok := schema.GetPropertyIndex(label, prop); !ok {
			continue
		}
		ids := propertyIndexLookup(store, schema, label, prop, props[prop])
		if !found || len(ids) < len(best) {
			best, found = ids, true
		}
	}
	if found {
		e.markMergeSchemaLookupUsed()
	}
	return best, found
}

func (e *StorageExecutor) findMergeNodeAnyLabel(store storage.Engine, labels []string, props map[string]interface{}) (*storage.Node, error) {
	if len(labels) == 0 || len(props) == 0 {
		return nil, nil
	}

	schema := store.GetSchema()
	if schema != nil {
		for _, label := range labels {
			for _, prop := range mergePropertyNamesSorted(props) {
				val := props[prop]
				nodeID, valueFound, constraintExists := schema.LookupUniqueConstraintValue(label, prop, val)
				if !constraintExists {
					continue
				}
				e.markMergeSchemaLookupUsed()
				if !valueFound {
					continue
				}
				for _, n := range e.loadMergeCandidateNodes(store, []storage.NodeID{nodeID}) {
					if mergeNodeMatchesAnyLabel(n, labels, props) {
						return n, nil
					}
				}
			}
		}

		bestIDs := []storage.NodeID(nil)
		bestCount := -1
		for _, label := range labels {
			for _, prop := range mergePropertyNamesSorted(props) {
				val := props[prop]
				if _, ok := schema.GetPropertyIndex(label, prop); !ok {
					continue
				}
				e.markMergeSchemaLookupUsed()
				ids := propertyIndexLookup(store, schema, label, prop, val)
				count := len(ids)
				if bestCount == -1 || count < bestCount {
					bestIDs = ids
					bestCount = count
					if count <= 1 {
						break
					}
				}
			}
			if bestCount == 1 {
				break
			}
		}
		for _, n := range e.loadMergeCandidateNodes(store, bestIDs) {
			if mergeNodeMatchesAnyLabel(n, labels, props) {
				return n, nil
			}
		}
	}
	e.markMergeScanFallbackUsed()

	seen := make(map[storage.NodeID]struct{})
	for _, label := range labels {
		nodes, err := store.GetNodesByLabel(label)
		if err != nil {
			return nil, err
		}
		for _, node := range nodes {
			if _, ok := seen[node.ID]; ok {
				continue
			}
			seen[node.ID] = struct{}{}
			if mergeNodeMatchesAnyLabel(node, labels, props) {
				return node, nil
			}
		}
	}

	return nil, nil
}

func (e *StorageExecutor) evaluateWhereForMergeContext(ctx context.Context, whereClause string, nodeCtx map[string]*storage.Node, relCtx map[string]*storage.Edge) bool {
	whereClause = strings.TrimSpace(whereClause)
	if whereClause == "" {
		return true
	}

	if v, ok := e.evaluateExpressionWithContext(ctx, whereClause, nodeCtx, relCtx).(bool); ok {
		return v
	}
	return e.evaluateWhereForContext(ctx, whereClause, nodeCtx)
}

// parseMergePattern parses a MERGE pattern like "(n:Label {prop: value})"
