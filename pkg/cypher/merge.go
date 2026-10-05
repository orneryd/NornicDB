// MERGE clause implementation for NornicDB.
// This file contains MERGE execution, compound queries, and context-aware operations.

package cypher

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"strings"

	nerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/localization"
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

		for _, idx := range schema.GetCompositeIndexesForLabel(label) {
			if !mergeIndexMatchesAllProperties(idx, props) {
				continue
			}
			schemaLookupUsed = true
			e.markMergeSchemaLookupUsed()
			candidateNodes := e.loadMergeCandidateNodes(store, idx.LookupFull(compositeLookupValues(idx, props)...))
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
	for _, idx := range schema.GetCompositeIndexesForLabel(label) {
		if mergeIndexMatchesAllProperties(idx, props) {
			e.markMergeSchemaLookupUsed()
			return idx.LookupFull(compositeLookupValues(idx, props)...), true
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

func (e *StorageExecutor) executeMerge(ctx context.Context, cypher string) (*ExecuteResult, error) {
	// Substitute parameters AFTER routing to avoid keyword detection issues
	if params := getParamsFromContext(ctx); params != nil {
		cypher = e.substituteParams(cypher, params)
	}

	result := &ExecuteResult{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Stats:   &QueryStats{},
	}
	store := e.getStorage(ctx)

	// Extract the main MERGE pattern - use word boundary detection
	mergeIdx := findKeywordIndex(cypher, "MERGE")
	if mergeIdx == -1 {
		return nil, localizedError(localization.CypherMergeClauseNotFound(truncateQuery(cypher, 80)), nil)
	}

	// Find ON CREATE SET, ON MATCH SET, standalone SET, and RETURN clauses
	// Use word boundary detection to avoid matching substrings
	onCreateIdx := findKeywordIndex(cypher, "ON CREATE SET")
	onMatchIdx := findKeywordIndex(cypher, "ON MATCH SET")
	returnIdx := findKeywordIndex(cypher, "RETURN")
	withIdx := findKeywordIndex(cypher, "WITH")

	// Find standalone SET clause (after ON CREATE SET / ON MATCH SET)
	// Must handle SET preceded by space, tab, or newline
	setIdx := -1
	searchStart := 0
	if onCreateIdx > 0 {
		searchStart = onCreateIdx + 13 // After "ON CREATE SET"
	}
	if onMatchIdx > 0 && onMatchIdx > searchStart {
		searchStart = onMatchIdx + 12 // After "ON MATCH SET"
	}

	// Helper function to find SET with any whitespace before it
	findStandaloneSet := func(s string, start int) int {
		upperS := upperASCII(s)
		for i := start; i <= len(upperS)-3; i++ {
			if strings.HasPrefix(upperS[i:], "SET") {
				// Check for whitespace before SET
				if i > 0 {
					prevChar := upperS[i-1]
					if prevChar != ' ' && prevChar != '\n' && prevChar != '\t' && prevChar != '\r' {
						continue // Not a word boundary
					}
				}
				// Check for whitespace/end after SET
				endPos := i + 3
				if endPos < len(upperS) {
					nextChar := upperS[endPos]
					if nextChar != ' ' && nextChar != '\n' && nextChar != '\t' && nextChar != '\r' {
						continue // Not a word boundary
					}
				}
				// Make sure this isn't part of ON CREATE SET or ON MATCH SET
				if i >= 10 && strings.HasPrefix(upperS[i-10:], "ON CREATE ") {
					continue
				}
				if i >= 9 && strings.HasPrefix(upperS[i-9:], "ON MATCH ") {
					continue
				}
				return i
			}
		}
		return -1
	}

	if searchStart > 0 {
		setIdx = findStandaloneSet(cypher, searchStart)
	} else {
		setIdx = findStandaloneSet(cypher, 0)
	}

	// Determine where the MERGE pattern ends
	patternEnd := len(cypher)
	for _, idx := range []int{onCreateIdx, onMatchIdx, setIdx, returnIdx} {
		if idx > 0 && idx < patternEnd {
			patternEnd = idx
		}
	}

	// Extract MERGE pattern (e.g., "(n:Label {prop: value})")
	mergePattern := strings.TrimSpace(cypher[mergeIdx+5 : patternEnd])
	if patternHasRelationship(mergePattern) {
		return e.executeMergeWithContext(ctx, cypher, make(map[string]*storage.Node), make(map[string]*storage.Edge))
	}

	// Parse the pattern to extract labels and properties for matching
	// Note: Parameters ($param) should already be substituted by substituteParams()
	varName, labels, matchProps, err := e.parseMergePattern(ctx, mergePattern)

	// If pattern properties still contain unresolved param literals (like
	// $path), handle gracefully. Resolved dotted param paths such as $node.url
	// must keep their parsed match props.
	if mergePropsContainUnresolvedParamLiteral(matchProps) {
		// Extract what we can from the pattern
		varName = e.extractVarName(mergePattern)
		labels = e.extractLabels(mergePattern)
		matchProps = make(map[string]interface{})
		err = nil // Continue with partial info
	}

	if err != nil {
		// If we truly can't parse, create a basic node
		node := &storage.Node{
			ID:         storage.NodeID(e.generateID()),
			Labels:     labels,
			Properties: matchProps,
		}
		if err := validateMergePatternProperties(node.Properties, "node"); err != nil {
			return nil, err
		}
		actualID, err := store.CreateNode(node)
		if err != nil {
			return nil, localizedError(localization.CypherMergeCreateNodeFailed(err), err)
		}
		node.ID = actualID
		e.notifyNodeMutated(string(node.ID))
		result.Stats.NodesCreated = 1
		countCreatedEntity(result.Stats, node.Labels, node.Properties)

		if varName == "" {
			varName = "n"
		}
		result.Columns = []string{varName}
		result.Rows = append(result.Rows, []interface{}{node})
		return result, nil
	}
	if err := validateMergePatternProperties(matchProps, "node"); err != nil {
		return nil, err
	}

	// Try to find existing node
	existingNode, err := e.findMergeNode(store, labels, matchProps)
	if err != nil {
		return nil, err
	}

	var node *storage.Node
	if existingNode != nil {
		// Node exists - apply ON MATCH SET if present
		node = cloneNodeForMergeMutation(existingNode)
		if onMatchIdx > 0 {
			setEnd := len(cypher)
			for _, idx := range []int{onCreateIdx, returnIdx} {
				if idx > onMatchIdx && idx < setEnd {
					setEnd = idx
				}
			}
			setClause := strings.TrimSpace(cypher[onMatchIdx+13 : setEnd])
			if _, err := e.applyCountedNodeSet(ctx, node, varName, setClause, nil, nil, result.Stats); err != nil {
				return nil, err
			}
			if err := store.UpdateNode(node); err != nil {
				return nil, localizedError(localization.CypherMutationsUpdateNodeFailed(err), err)
			}
			e.notifyNodeMutated(string(node.ID))
		}
	} else {
		// Node doesn't exist - create it
		node = &storage.Node{
			ID:         storage.NodeID(e.generateID()),
			Labels:     labels,
			Properties: matchProps,
		}
		if err := validatePropertyValues(node.Properties); err != nil {
			return nil, err
		}
		actualID, err := store.CreateNode(node)
		if err != nil {
			if mergeCreateConflict(err) {
				recoveredNode, findErr := e.findMergeNode(store, labels, matchProps)
				if findErr != nil {
					return nil, findErr
				}
				if recoveredNode != nil {
					existingNode = recoveredNode
					node = cloneNodeForMergeMutation(recoveredNode)
				} else {
					return nil, localizedError(localization.CypherMergeCreateNodeFailed(err), err)
				}
			} else {
				return nil, localizedError(localization.CypherMergeCreateNodeFailed(err), err)
			}
		}
		if existingNode == nil {
			node.ID = actualID
			e.notifyNodeMutated(string(node.ID))
			result.Stats.NodesCreated = 1
			countCreatedEntity(result.Stats, node.Labels, node.Properties)

			// Apply ON CREATE SET if present
			if onCreateIdx > 0 {
				setEnd := len(cypher)
				// Stop at: standalone SET, ON MATCH SET, WITH, or RETURN
				for _, idx := range []int{setIdx, onMatchIdx, withIdx, returnIdx} {
					if idx > onCreateIdx && idx < setEnd {
						setEnd = idx
					}
				}
				setClause := strings.TrimSpace(cypher[onCreateIdx+13 : setEnd])
				if _, err := e.applyCountedNodeSet(ctx, node, varName, setClause, nil, nil, result.Stats); err != nil {
					return nil, err
				}
			}
		}
	}

	// Apply standalone SET clause (runs for both create and match)
	if setIdx > 0 {
		setEnd := len(cypher)
		for _, idx := range []int{withIdx, returnIdx} {
			if idx > setIdx && idx < setEnd {
				setEnd = idx
			}
		}
		setClause := strings.TrimSpace(cypher[setIdx+3 : setEnd]) // +3 to skip "SET"
		if _, err := e.applyCountedNodeSet(ctx, node, varName, setClause, nil, nil, result.Stats); err != nil {
			return nil, err
		}
	}

	// Persist updates
	if existingNode != nil || setIdx > 0 || onCreateIdx > 0 {
		if err := store.UpdateNode(node); err != nil {
			return nil, localizedError(localization.CypherMutationsUpdateNodeFailed(err), err)
		}
		e.notifyNodeMutated(string(node.ID))
	}
	e.cacheMergeNode(labels, matchProps, node)

	// Handle RETURN clause
	if returnIdx > 0 {
		projected, err := e.projectMergeReturn(ctx, []pipelineRow{e.mergeBindingRow(ctx, map[string]*storage.Node{varName: node}, nil)}, cypher[returnIdx:])
		if err != nil {
			return nil, err
		}
		result.Columns, result.Rows = projected.Columns, projected.Rows
	}

	return result, nil
}

// executeMergeWithChain handles MERGE ... WITH ... MATCH ... MERGE chain patterns.
// This is the pattern used in import scripts:
//
//	MERGE (e:Entry {key: $key})
//	ON CREATE SET e.value = $value
//	WITH e
//	MATCH (c:Category {name: $category})
//	MERGE (e)-[:IN_CATEGORY]->(c)
//	WITH e
//	MATCH (t:Team {name: $team})
//	MERGE (e)-[:MANAGED_BY]->(t)
//	RETURN e.key
//
// In Neo4j Cypher, if any MATCH in the chain fails to find a node,
// the query returns 0 rows (the chain is broken). The MERGE still executes
// for nodes found before the break.
func (e *StorageExecutor) executeMergeWithChain(ctx context.Context, cypher string) (*ExecuteResult, error) {
	ctx = withExpressionFailureSlot(ctx)
	originalFabricBindings := e.fabricRecordBindings
	defer func() {
		e.fabricRecordBindings = originalFabricBindings
	}()
	if strings.TrimSpace(cypher) == "" {
		return nil, nerrors.ErrInvalidMergeChainQuery
	}

	// Substitute parameters
	if params := getParamsFromContext(ctx); params != nil {
		cypher = e.substituteParams(cypher, params)
	}
	// Normalization: collapse duplicated consecutive WITH projections.
	// Some generated MERGE+CALL query shapes emit the same WITH line twice
	// inside subqueries, which is a semantic no-op but adds parse/execution overhead.
	cypher = collapseConsecutiveDuplicateWithClauses(cypher)

	result := &ExecuteResult{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Stats:   &QueryStats{},
	}

	// Split the query into segments at each WITH clause
	// Each segment is: [initial MERGE] or [MATCH ... MERGE relationship]
	segments := e.splitMergeChainSegments(cypher)
	if len(segments) == 0 {
		return nil, nerrors.ErrInvalidMergeChainQuery
	}

	// Context to track bound variables (node variable -> *storage.Node)
	nodeContext := make(map[string]*storage.Node)
	relContext := make(map[string]*storage.Edge)
	scalarContext := cloneStringAnyMap(e.fabricRecordBindings)

	// Track if chain is broken (a MATCH returned 0 rows)
	chainBroken := false

	// Process each segment
	for i, segment := range segments {
		segment = strings.TrimSpace(segment)
		if segment == "" {
			continue
		}

		upperSeg := upperASCII(segment)

		if i == 0 {
			// First segment may contain multiple setup MERGEs before the first WITH.
			initialClauses := e.splitMultipleMerges(segment)
			for _, initialClause := range initialClauses {
				initialClause = strings.TrimSpace(initialClause)
				if initialClause == "" {
					continue
				}
				upperInitial := upperASCII(initialClause)
				if strings.HasPrefix(upperInitial, "MERGE") {
					mergeContent := strings.TrimSpace(initialClause[5:])
					if strings.Contains(mergeContent, "-[") || strings.Contains(mergeContent, "]-") {
						if err := e.executeMergeRelSegment(ctx, mergeContent, nodeContext, result.Stats); err != nil {
							return nil, localizedError(localization.CypherMergeInitialMergeFailed(err), err)
						}
						continue
					}
					mergedNode, varName, err := e.executeMergeNodeSegment(ctx, initialClause, result.Stats)
					if err != nil {
						return nil, localizedError(localization.CypherMergeInitialMergeFailed(err), err)
					}
					if mergedNode != nil && varName != "" {
						nodeContext[varName] = mergedNode
					}
				}
			}
		} else if strings.HasPrefix(upperSeg, "FOREACH") {
			if chainBroken {
				continue
			}
			if _, err := e.executeForeachWithContext(ctx, segment, nodeContext, relContext); err != nil {
				return nil, localizedError(localization.CypherMergeForeachFailed(err), err)
			}
		} else if strings.HasPrefix(upperSeg, "RETURN") {
			rows := []pipelineRow{}
			if !chainBroken {
				row := e.mergeBindingRow(ctx, nodeContext, relContext)
				for name, value := range scalarContext {
					row[name] = value
				}
				rows = append(rows, row)
			}
			projected, err := e.projectMergeReturn(ctx, rows, segment)
			if err != nil {
				return nil, err
			}
			result.Columns, result.Rows = projected.Columns, projected.Rows
		} else {
			// Segment after WITH: starts with a WITH projection (e.g., "e") followed by one or more clauses.
			// Example:
			//   WITH e
			//   OPTIONAL MATCH (a:TypeA {name: 'A1'})
			//   FOREACH (...)
			//
			// We apply WITH semantics by filtering context to only passed variables, then execute clauses in order.

			segmentNodeCtx := nodeContext
			segmentRelCtx := relContext
			segmentScalarCtx := scalarContext

			remaining, newNodeCtx, newRelCtx, newScalarCtx := e.applyWithProjection(ctx, segment, segmentNodeCtx, segmentRelCtx, segmentScalarCtx)
			if failure := getExpressionFailure(ctx); failure != nil {
				return nil, failure
			}
			if remaining == "" {
				chainBroken = true
			}
			segmentNodeCtx = newNodeCtx
			segmentRelCtx = newRelCtx
			segmentScalarCtx = newScalarCtx
			e.fabricRecordBindings = segmentScalarCtx

			clauses := splitMergeChainClauseBlock(remaining)
			for _, clause := range clauses {
				if strings.TrimSpace(clause) == "" {
					continue
				}
				upperClause := upperASCII(strings.TrimSpace(clause))

				// If chain is broken, we must still allow the final RETURN segment to produce 0 rows
				// (handled above), but all intermediate updates/clauses are skipped.
				if chainBroken {
					continue
				}

				switch {
				case strings.HasPrefix(upperClause, "OPTIONAL MATCH"):
					matchedNode, matchVarName, err := e.executeMatchSegment(ctx, clause, segmentNodeCtx)
					if err != nil {
						// OPTIONAL MATCH errors still break execution (Neo4j would error)
						return nil, err
					}
					if matchVarName != "" {
						segmentNodeCtx[matchVarName] = matchedNode // may be nil
					}
				case strings.HasPrefix(upperClause, "MATCH"):
					matchedNode, matchVarName, err := e.executeMatchSegment(ctx, clause, segmentNodeCtx)
					if err != nil {
						chainBroken = true
						continue
					}
					if matchedNode == nil {
						chainBroken = true
						continue
					}
					if matchVarName != "" {
						segmentNodeCtx[matchVarName] = matchedNode
					}

					// Check for MERGE relationship in this clause (MATCH ... MERGE ...)
					mergeIdx := findKeywordIndex(clause, "MERGE")
					if mergeIdx > 0 {
						mergePart := strings.TrimSpace(clause[mergeIdx+5:])
						if strings.Contains(mergePart, "-[") || strings.Contains(mergePart, "]-") {
							_ = e.executeMergeRelSegment(ctx, mergePart, segmentNodeCtx, result.Stats)
						}
					}
				case strings.HasPrefix(upperClause, "MERGE"):
					mergePart := strings.TrimSpace(clause[5:])
					if strings.Contains(mergePart, "-[") || strings.Contains(mergePart, "]-") {
						if err := e.executeMergeRelSegment(ctx, mergePart, segmentNodeCtx, result.Stats); err != nil {
							return nil, err
						}
					} else {
						mergedNode, mergeVarName, err := e.executeMergeNodeSegment(ctx, clause, result.Stats)
						if err != nil {
							return nil, err
						}
						if mergedNode != nil && mergeVarName != "" {
							segmentNodeCtx[mergeVarName] = mergedNode
						}
					}
				case strings.HasPrefix(upperClause, "FOREACH"):
					_, err := e.executeForeachWithContext(ctx, clause, segmentNodeCtx, segmentRelCtx)
					if err != nil {
						return nil, err
					}
				}
			}

			// Persist segment context back to main context for subsequent segments.
			nodeContext = segmentNodeCtx
			relContext = segmentRelCtx
			scalarContext = segmentScalarCtx
			e.fabricRecordBindings = scalarContext
		}
	}

	return result, nil
}

func collapseConsecutiveDuplicateWithClauses(cypher string) string {
	lines := strings.Split(cypher, "\n")
	if len(lines) < 2 {
		return cypher
	}
	out := make([]string, 0, len(lines))
	var prevTrim string
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		upperTrim := upperASCII(trimmed)
		if strings.HasPrefix(upperTrim, "WITH ") && prevTrim == trimmed {
			continue
		}
		out = append(out, line)
		prevTrim = trimmed
	}
	return strings.Join(out, "\n")
}

// applyWithProjection applies WITH semantics to a MERGE chain segment.
//
// The input segment is the text between "WITH" and the next "WITH"/"RETURN",
// i.e. it starts with a projection list (e.g., "e") followed by the next clause.
// It returns the remaining clause block plus filtered contexts.
func (e *StorageExecutor) applyWithProjection(ctx context.Context, segment string, nodeCtx map[string]*storage.Node, relCtx map[string]*storage.Edge, scalarCtx map[string]interface{}) (remaining string, newNodeCtx map[string]*storage.Node, newRelCtx map[string]*storage.Edge, newScalarCtx map[string]interface{}) {
	segment = strings.TrimSpace(segment)
	if segment == "" {
		return "", nodeCtx, relCtx, scalarCtx
	}

	keywords := []string{"OPTIONAL MATCH", "MATCH", "MERGE", "FOREACH", "CREATE", "SET", "DELETE", "REMOVE", "CALL", "UNWIND", "RETURN"}
	nextClausePos := -1
	for _, kw := range keywords {
		if idx := topLevelKeywordIndex(segment, kw); idx >= 0 {
			if nextClausePos == -1 || idx < nextClausePos {
				nextClausePos = idx
			}
		}
	}
	if nextClausePos == -1 {
		// No further clause keywords - treat entire segment as projection list.
		nextClausePos = len(segment)
	}

	withPart := strings.TrimSpace(segment[:nextClausePos])
	remaining = strings.TrimSpace(segment[nextClausePos:])

	row := e.mergeBindingRow(ctx, nodeCtx, relCtx)
	for name, value := range scalarCtx {
		row[name] = value
	}
	rows, ok := e.pipelineApplyWith(ctx, []pipelineRow{row}, "WITH "+withPart)
	newNodeCtx = make(map[string]*storage.Node)
	newRelCtx = make(map[string]*storage.Edge)
	newScalarCtx = make(map[string]interface{})
	if !ok {
		pipelineItemUnevaluable(ctx, withPart)
		return remaining, newNodeCtx, newRelCtx, newScalarCtx
	}
	if len(rows) == 0 {
		return "", newNodeCtx, newRelCtx, newScalarCtx
	}
	for name, value := range rows[0] {
		switch bound := value.(type) {
		case *storage.Node:
			newNodeCtx[name] = bound
		case *storage.Edge:
			newRelCtx[name] = bound
		default:
			newScalarCtx[name] = value
		}
	}

	return remaining, newNodeCtx, newRelCtx, newScalarCtx
}

func splitMergeChainClauseBlock(block string) []string {
	block = strings.TrimSpace(block)
	if block == "" {
		return nil
	}

	keywords := []string{"OPTIONAL MATCH", "MATCH", "MERGE", "FOREACH", "RETURN"}

	// Find the first clause start at top level.
	start := -1
	for _, kw := range keywords {
		if idx := findKeywordIndex(block, kw); idx >= 0 {
			if start == -1 || idx < start {
				start = idx
			}
		}
	}
	if start == -1 {
		return []string{block}
	}
	if start > 0 {
		block = strings.TrimSpace(block[start:])
	}

	var clauses []string
	pos := 0
	for pos < len(block) {
		// Identify which keyword starts here.
		var currentKw string
		for _, kw := range keywords {
			if findKeywordIndex(block[pos:], kw) == 0 {
				currentKw = kw
				break
			}
		}
		if currentKw == "" {
			// Defensive fallback: once aligned to the first keyword, subsequent clause
			// boundaries should always start on a recognized keyword.
			clauses = append(clauses, strings.TrimSpace(block[pos:]))
			continue
		}

		// Find the next clause start.
		searchFrom := pos + len(currentKw)
		nextStart := -1
		for _, kw := range keywords {
			if idx := findKeywordIndex(block[searchFrom:], kw); idx >= 0 {
				abs := searchFrom + idx
				if abs > pos && (nextStart == -1 || abs < nextStart) {
					nextStart = abs
				}
			}
		}

		if nextStart == -1 {
			clauses = append(clauses, strings.TrimSpace(block[pos:]))
			break
		}

		clauses = append(clauses, strings.TrimSpace(block[pos:nextStart]))
		pos = nextStart
	}

	return clauses
}

// splitMergeChainSegments splits a MERGE...WITH...MATCH chain into segments.
// Returns segments like: ["MERGE (e:Entry...) ON CREATE SET...", "MATCH (c:Cat...) MERGE (e)-[:REL]->(c)", "RETURN..."]
func (e *StorageExecutor) splitMergeChainSegments(cypher string) []string {
	var segments []string

	// Find all WITH positions
	var withPositions []int
	searchPos := 0
	for {
		idx := findKeywordIndex(cypher[searchPos:], "WITH")
		if idx == -1 {
			break
		}
		// Check it's not "STARTS WITH" or "ENDS WITH"
		actualPos := searchPos + idx
		if actualPos > 6 {
			before := upperASCII(cypher[actualPos-6 : actualPos])
			if strings.HasSuffix(strings.TrimSpace(before), "STARTS") || strings.HasSuffix(strings.TrimSpace(before), "ENDS") {
				searchPos = actualPos + 4
				continue
			}
		}
		withPositions = append(withPositions, actualPos)
		searchPos = actualPos + 4
	}

	// Find RETURN position
	returnIdx := findKeywordIndex(cypher, "RETURN")

	if len(withPositions) == 0 {
		// No WITH clauses - return whole query
		return []string{cypher}
	}

	// First segment: from start to first WITH
	segments = append(segments, strings.TrimSpace(cypher[:withPositions[0]]))

	// Middle segments: between WITH clauses
	for i := 0; i < len(withPositions); i++ {
		// Skip the WITH keyword and find the content after it
		startPos := withPositions[i] + 4 // Skip "WITH"

		// Find where this segment ends
		var endPos int
		if i+1 < len(withPositions) {
			endPos = withPositions[i+1]
		} else if returnIdx > startPos {
			endPos = returnIdx
		} else {
			endPos = len(cypher)
		}

		// Preserve everything after WITH so we can apply WITH semantics and execute
		// OPTIONAL MATCH/FOREACH patterns inside the segment.
		segmentContent := strings.TrimSpace(cypher[startPos:endPos])
		if segmentContent != "" {
			segments = append(segments, segmentContent)
		}
	}

	// Add RETURN segment if present
	if returnIdx > 0 {
		segments = append(segments, strings.TrimSpace(cypher[returnIdx:]))
	}

	return segments
}

// executeMergeNodeSegment executes the initial MERGE (node) part and returns
// the node and variable name. It adds what it writes (a created node and its
// properties and labels, and its ON CREATE SET / ON MATCH SET / SET) to stats,
// which may be nil.
func (e *StorageExecutor) executeMergeNodeSegment(ctx context.Context, segment string, stats *QueryStats, boundNodes ...map[string]*storage.Node) (*storage.Node, string, error) {
	store := e.getStorage(ctx)
	// Parse: MERGE (varName:Label {props}) [ON CREATE SET ...] [ON MATCH SET ...]
	mergeIdx := findKeywordIndex(segment, "MERGE")
	if mergeIdx == -1 {
		return nil, "", localizedError(localization.CypherMergeSegmentClauseNotFound(), nil)
	}

	// Find the pattern end (ON CREATE SET / ON MATCH SET / standalone SET / end of segment)
	patternEnd := len(segment)
	onCreateIdx := findKeywordIndex(segment, "ON CREATE SET")
	onMatchIdx := findKeywordIndex(segment, "ON MATCH SET")
	setIdx := findStandaloneSetInMergeSegment(segment)
	for _, idx := range []int{onCreateIdx, onMatchIdx, setIdx} {
		if idx > 0 && idx < patternEnd {
			patternEnd = idx
		}
	}
	for _, keyword := range []string{"WITH", "RETURN"} {
		idx := findKeywordIndex(segment, keyword)
		if idx > 0 && idx < patternEnd {
			patternEnd = idx
		}
	}

	pattern := strings.TrimSpace(segment[mergeIdx+5 : patternEnd])

	// Parse the pattern
	var varName string
	var labels []string
	var props map[string]interface{}
	var err error
	if len(boundNodes) > 0 {
		varName, labels, props, err = e.parseMergeNodePattern(ctx, pattern, boundNodes[0], nil)
	} else {
		varName, labels, props, err = e.parseMergePattern(ctx, pattern)
	}
	if err != nil {
		return nil, "", err
	}
	if err := validateMergePatternProperties(props, "node"); err != nil {
		return nil, "", err
	}

	// Try to find existing node
	var existingNode *storage.Node
	if len(labels) > 0 && len(props) > 0 {
		existingNode, err = e.findMergeNode(store, labels, props)
		if err != nil {
			return nil, "", err
		}
	}

	var node *storage.Node
	if existingNode != nil {
		node = existingNode
		e.cacheMergeNode(labels, props, node)
		// Apply ON MATCH SET if present
		if onMatchIdx > 0 {
			setEnd := len(segment)
			if onCreateIdx > onMatchIdx {
				setEnd = onCreateIdx
			}
			if setIdx > onMatchIdx && setIdx < setEnd {
				setEnd = setIdx
			}
			setClause := strings.TrimSpace(segment[onMatchIdx+12 : setEnd])
			changed, err := e.applyCountedNodeSet(ctx, node, varName, setClause, nil, nil, stats)
			if err != nil {
				return nil, "", err
			}
			if changed {
				if err := store.UpdateNode(node); err != nil {
					return nil, "", localizedError(localization.CypherMutationsUpdateNodeFailed(err), err)
				}
				e.notifyNodeMutated(string(node.ID))
			}
		}
	} else {
		// Create new node
		node = &storage.Node{
			ID:         storage.NodeID(e.generateID()),
			Labels:     labels,
			Properties: props,
		}
		if err := validatePropertyValues(node.Properties); err != nil {
			return nil, "", err
		}
		actualID, err := store.CreateNode(node)
		if err != nil {
			if mergeCreateConflict(err) {
				recoveredNode, findErr := e.findMergeNode(store, labels, props)
				if findErr != nil {
					return nil, "", findErr
				}
				if recoveredNode != nil {
					existingNode = recoveredNode
					node = recoveredNode
					e.cacheMergeNode(labels, props, node)
				} else {
					return nil, "", localizedError(localization.CypherMergeCreateNodeSegmentFailed(err), err)
				}
			} else {
				return nil, "", localizedError(localization.CypherMergeCreateNodeSegmentFailed(err), err)
			}
		}
		if existingNode == nil {
			node.ID = actualID
			e.notifyNodeMutated(string(node.ID))
			e.cacheMergeNode(labels, props, node)
			if stats != nil {
				stats.NodesCreated++
			}
			countCreatedEntity(stats, node.Labels, node.Properties)

			// Apply ON CREATE SET if present
			if onCreateIdx > 0 {
				setEnd := len(segment)
				if onMatchIdx > onCreateIdx {
					setEnd = onMatchIdx
				}
				if setIdx > onCreateIdx && setIdx < setEnd {
					setEnd = setIdx
				}
				setClause := strings.TrimSpace(segment[onCreateIdx+13 : setEnd])
				changed, err := e.applyCountedNodeSet(ctx, node, varName, setClause, nil, nil, stats)
				if err != nil {
					return nil, "", err
				}
				if changed {
					if err := store.UpdateNode(node); err != nil {
						return nil, "", localizedError(localization.CypherMutationsUpdateNodeFailed(err), err)
					}
					e.notifyNodeMutated(string(node.ID))
				}
			}
		}
	}

	// Apply standalone SET (outside ON CREATE/ON MATCH), e.g.:
	// MERGE (n:Label {k:'v'}) SET n.prop = 1
	if setIdx > 0 {
		setEnd := len(segment)
		for _, idx := range []int{findKeywordIndex(segment, "WITH"), findKeywordIndex(segment, "RETURN")} {
			if idx > setIdx && idx < setEnd {
				setEnd = idx
			}
		}
		setClause := strings.TrimSpace(segment[setIdx+3 : setEnd])
		changed, err := e.applyCountedNodeSet(ctx, node, varName, setClause, nil, nil, stats)
		if err != nil {
			return nil, "", err
		}
		if changed {
			if err := store.UpdateNode(node); err != nil {
				return nil, "", localizedError(localization.CypherMutationsUpdateNodeFailed(err), err)
			}
			e.notifyNodeMutated(string(node.ID))
			e.cacheMergeNode(labels, props, node)
		}
	}

	return node, varName, nil
}

func findStandaloneSetInMergeSegment(segment string) int {
	return findStandaloneSetInMergeSegmentFrom(segment, 0)
}

func findStandaloneSetInMergeSegmentFrom(segment string, start int) int {
	if start < 0 {
		start = 0
	}
	searchFrom := 0
	if start > searchFrom {
		searchFrom = start
	}
	for searchFrom < len(segment) {
		idx := keywordIndexFrom(segment, "SET", searchFrom, defaultKeywordScanOpts())
		if idx < 0 {
			return -1
		}
		if idx > 0 && !isASCIISpace(segment[idx-1]) {
			searchFrom = idx + 3
			continue
		}
		end := idx + 3
		if end < len(segment) && !isASCIISpace(segment[end]) {
			searchFrom = idx + 3
			continue
		}
		prefix := upperASCII(strings.TrimSpace(segment[:idx]))
		if strings.HasSuffix(prefix, "ON CREATE") || strings.HasSuffix(prefix, "ON MATCH") {
			searchFrom = idx + 3
			continue
		}
		return idx
	}
	return -1
}

// executeMatchSegment executes a MATCH segment and returns the matched node.
func (e *StorageExecutor) executeMatchSegment(ctx context.Context, segment string, nodeContext map[string]*storage.Node) (*storage.Node, string, error) {
	store := e.getStorage(ctx)
	// Parse: MATCH (varName:Label {props}) [MERGE ...]
	matchIdx := findKeywordIndex(segment, "MATCH")
	if matchIdx == -1 {
		return nil, "", localizedError(localization.CypherMergeMatchSegmentClauseNotFound(), nil)
	}

	// Find the pattern end (MERGE or end of segment)
	patternEnd := len(segment)
	mergeIdx := findKeywordIndex(segment, "MERGE")
	if mergeIdx > 0 {
		patternEnd = mergeIdx
	}

	pattern := strings.TrimSpace(segment[matchIdx+5 : patternEnd])

	// Parse the node pattern
	nodePattern := e.parseNodePattern(ctx, pattern)
	if nodePattern.variable == "" && len(nodePattern.labels) == 0 {
		return nil, "", localizedError(localization.CypherMergeNodePatternParseFailed(pattern), nil)
	}
	for key, raw := range nodePattern.properties {
		ident, ok := raw.(string)
		if !ok || !isSimpleIdentifier(ident) {
			continue
		}
		if bound, exists := e.fabricRecordBindings[ident]; exists {
			nodePattern.properties[key] = bound
		}
	}
	// Check if variable is already bound
	if boundNode, exists := nodeContext[nodePattern.variable]; exists {
		return boundNode, nodePattern.variable, nil
	}

	if cached := e.findMergeNodeInCache(store, nodePattern.labels, nodePattern.properties); cached != nil {
		return cached, nodePattern.variable, nil
	}

	// Find matching node
	var nodes []*storage.Node
	var err error
	if len(nodePattern.labels) > 0 {
		nodes, err = store.GetNodesByLabel(nodePattern.labels[0])
	} else {
		nodes, err = store.AllNodes()
	}
	if err != nil {
		return nil, "", err
	}

	// Filter by properties
	for _, n := range nodes {
		matches := true
		for key, val := range nodePattern.properties {
			if nodeVal, ok := n.Properties[key]; !ok || fmt.Sprintf("%v", nodeVal) != fmt.Sprintf("%v", val) {
				matches = false
				break
			}
		}
		if matches {
			return n, nodePattern.variable, nil
		}
	}

	// No match found
	return nil, nodePattern.variable, nil
}

// executeMergeRelSegment executes a MERGE relationship segment like (e)-[:REL]->(c)
func (e *StorageExecutor) executeMergeRelSegment(ctx context.Context, pattern string, nodeContext map[string]*storage.Node, stats *QueryStats) error {
	store := e.getStorage(ctx)
	// Parse relationship pattern: (startVar)-[:TYPE]->(endVar) or (startVar)-[:TYPE {props}]->(endVar)
	pattern = strings.TrimSpace(pattern)

	// Extract start node variable
	startParen := strings.Index(pattern, "(")
	if startParen == -1 {
		return localizedError(localization.CypherMergeRelationshipStartMissing(pattern), nil)
	}

	endStartParen := strings.Index(pattern[startParen+1:], ")")
	if endStartParen == -1 {
		return localizedError(localization.CypherMergeRelationshipStartParenMissing(pattern), nil)
	}
	startVar := strings.TrimSpace(pattern[startParen+1 : startParen+1+endStartParen])

	// Find the relationship part -[...]->
	relStart := strings.Index(pattern, "-[")
	relEnd := strings.Index(pattern, "]->")
	if relEnd == -1 {
		relEnd = strings.Index(pattern, "]-")
	}
	if relStart == -1 || relEnd == -1 {
		return localizedError(localization.CypherMergeRelationshipBracketsMissing(pattern), nil)
	}

	relContent := pattern[relStart+2 : relEnd]

	// Parse relationship type and properties
	var relType string
	relProps := make(map[string]interface{})

	if colonIdx := strings.Index(relContent, ":"); colonIdx >= 0 {
		afterColon := relContent[colonIdx+1:]
		if braceIdx := strings.Index(afterColon, "{"); braceIdx > 0 {
			relType = strings.TrimSpace(afterColon[:braceIdx])
			if braceEnd := strings.LastIndex(afterColon, "}"); braceEnd > braceIdx {
				relProps = e.parseProperties(ctx, afterColon[braceIdx:braceEnd+1])
			}
		} else {
			relType = strings.TrimSpace(afterColon)
		}
	}

	// Extract end node variable
	// Find the last (var) pattern
	lastParenStart := strings.LastIndex(pattern, "(")
	lastParenEnd := strings.LastIndex(pattern, ")")
	if lastParenStart == -1 || lastParenEnd == -1 || lastParenEnd < lastParenStart {
		return localizedError(localization.CypherMergeRelationshipEndMissing(pattern), nil)
	}
	endVar := strings.TrimSpace(pattern[lastParenStart+1 : lastParenEnd])

	// Look up nodes in context
	startNode, startExists := nodeContext[startVar]
	endNode, endExists := nodeContext[endVar]

	if !startExists {
		return localizedError(localization.CypherMergeStartVariableNotBound(startVar, getKeys(nodeContext)), nil)
	}
	if !endExists {
		return localizedError(localization.CypherMergeEndVariableNotBound(endVar, getKeys(nodeContext)), nil)
	}

	// Check the complete relationship pattern, including its identity
	// properties, rather than collapsing every same-pair/type relationship.
	existing, err := findRelationshipForMerge(store, startNode.ID, endNode.ID, relType, relProps)
	if err != nil {
		return localizedError(localization.CypherMergeFindRelationshipSegmentFailed(err), err)
	}
	if existing != nil {
		return nil
	}

	// Create the relationship
	edge := &storage.Edge{
		ID:         e.newRelationshipMergeEdgeID(startNode.ID, endNode.ID, relType, relProps),
		Type:       relType,
		StartNode:  startNode.ID,
		EndNode:    endNode.ID,
		Properties: relProps,
	}

	createdEdge, created, err := createRelationshipForMerge(e, store, edge, relProps)
	if err != nil {
		return err
	}
	if !created {
		return nil
	}
	// Only a relationship this MERGE created is a write; a matched one is not.
	if stats != nil {
		stats.RelationshipsCreated++
	}
	countCreatedEntity(stats, nil, createdEdge.Properties)
	e.notifyEdgeMutated(string(createdEdge.ID))
	return nil
}

// executeMultipleMerges handles MERGE-led queries with multiple mutation clauses:
//
//	MERGE (e:Entry {key: 'x'})
//	MERGE (f:Category {name: 'y'})
//	MERGE (e)-[:REL]->(f)
//	RETURN e.key, f.name
//
// Each MERGE is executed in sequence, building a context of bound variables.
// Relationship MERGEs use variables from previous node MERGEs.
func (e *StorageExecutor) executeMultipleMerges(ctx context.Context, cypher string) (*ExecuteResult, error) {
	originalFabricBindings := e.fabricRecordBindings
	defer func() {
		e.fabricRecordBindings = originalFabricBindings
	}()

	// Substitute parameters
	if params := getParamsFromContext(ctx); params != nil {
		cypher = e.substituteParams(cypher, params)
	}

	result := &ExecuteResult{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Stats:   &QueryStats{},
	}

	// Context to track bound variables
	nodeContext := make(map[string]*storage.Node)
	relContext := make(map[string]*storage.Edge)
	scalarContext := cloneStringAnyMap(e.fabricRecordBindings)

	// Split into MERGE segments
	segments := e.splitMultipleMerges(cypher)

	// Process each MERGE segment
	chainBroken := false
	for _, segment := range segments {
		segment = strings.TrimSpace(segment)
		if segment == "" {
			continue
		}
		upperSeg := upperASCII(segment)

		if strings.HasPrefix(upperSeg, "MERGE") {
			if chainBroken {
				continue
			}
			mergeContent := strings.TrimSpace(segment[5:])

			// Check if this is a relationship MERGE
			if strings.Contains(mergeContent, "-[") || strings.Contains(mergeContent, "]-") {
				mergeResult, err := e.executeMergeWithContext(ctx, segment, nodeContext, relContext)
				if err != nil {
					return nil, localizedError(localization.CypherMergeRelationshipFailed(err), err)
				}
				if mergeResult != nil && mergeResult.Stats != nil {
					addQueryStats(result.Stats, mergeResult.Stats)
				}
			} else {
				// Node MERGE
				node, varName, err := e.executeMergeNodeSegment(ctx, segment, result.Stats, nodeContext)
				if err != nil {
					return nil, localizedError(localization.CypherMergeNodeFailed(err), err)
				}
				if node != nil && varName != "" {
					nodeContext[varName] = node
				}
			}
		} else if strings.HasPrefix(upperSeg, "CREATE") {
			if chainBroken {
				continue
			}
			if _, err := e.createPatternsInScope(ctx, strings.TrimSpace(segment[6:]), nodeContext, relContext, result); err != nil {
				return nil, localizedError(localization.CypherMutationsNodeCreateFailed(err), err)
			}
		} else if strings.HasPrefix(upperSeg, "OPTIONAL MATCH") {
			if chainBroken {
				continue
			}
			node, varName, err := e.executeMatchSegment(ctx, segment, nodeContext)
			if err != nil {
				return nil, localizedError(localization.CypherMergeOptionalMatchFailed(err), err)
			}
			if varName != "" {
				// Preserve variable even when nil (OPTIONAL semantics).
				nodeContext[varName] = node
			}
		} else if strings.HasPrefix(upperSeg, "MATCH") {
			if chainBroken {
				continue
			}
			node, varName, err := e.executeMatchSegment(ctx, segment, nodeContext)
			if err != nil {
				return nil, localizedError(localization.CypherMergeMatchFailed(err), err)
			}
			if node == nil {
				chainBroken = true
				continue
			}
			if varName != "" {
				nodeContext[varName] = node
			}
		} else if strings.HasPrefix(upperSeg, "WITH") {
			if chainBroken {
				continue
			}
			newNodeCtx, newRelCtx, newScalarCtx := e.projectWithContext(ctx, strings.TrimSpace(segment[4:]), nodeContext, relContext, scalarContext)
			nodeContext = newNodeCtx
			relContext = newRelCtx
			scalarContext = newScalarCtx
			e.fabricRecordBindings = scalarContext
		} else if strings.HasPrefix(upperSeg, "WHERE") {
			if chainBroken {
				continue
			}
			whereClause := strings.TrimSpace(segment[5:])
			if !e.evaluateWhereForMergeContext(ctx, whereClause, nodeContext, relContext) {
				chainBroken = true
			}
		} else if strings.HasPrefix(upperSeg, "FOREACH") {
			if chainBroken {
				continue
			}
			if _, err := e.executeForeachWithContext(ctx, segment, nodeContext, relContext); err != nil {
				return nil, localizedError(localization.CypherMergeForeachFailed(err), err)
			}
		} else if strings.HasPrefix(upperSeg, "RETURN") {
			// Build result from context
			if chainBroken {
				returnClause := strings.TrimSpace(segment[6:])
				items := e.parseReturnItems(returnClause)
				for _, item := range items {
					if item.alias != "" {
						result.Columns = append(result.Columns, item.alias)
					} else {
						result.Columns = append(result.Columns, item.expr)
					}
				}
				return result, nil
			}
			returnClause := strings.TrimSpace(segment[6:])
			items := e.parseReturnItems(returnClause)

			row := make([]interface{}, len(items))
			for i, item := range items {
				if item.alias != "" {
					result.Columns = append(result.Columns, item.alias)
				} else {
					result.Columns = append(result.Columns, item.expr)
				}
				row[i] = e.evaluateExpressionWithContext(ctx, item.expr, nodeContext, relContext)
			}
			result.Rows = append(result.Rows, row)
		}
	}

	return result, nil
}

// splitMultipleMerges splits a query into mutation and row-processing clauses.
func (e *StorageExecutor) splitMultipleMerges(cypher string) []string {
	var segments []string
	boundaries := collectTopLevelMergeClauseBoundaries(cypher, []string{
		"OPTIONAL MATCH",
		"FOREACH",
		"MERGE",
		"MATCH",
		"CREATE",
		"WITH",
		"WHERE",
		"RETURN",
	})
	if len(boundaries) == 0 {
		return []string{strings.TrimSpace(cypher)}
	}
	sort.Slice(boundaries, func(i, j int) bool { return boundaries[i].pos < boundaries[j].pos })

	lastPos := -1
	for i, b := range boundaries {
		if b.pos == lastPos {
			continue
		}
		end := len(cypher)
		for j := i + 1; j < len(boundaries); j++ {
			if boundaries[j].pos > b.pos {
				end = boundaries[j].pos
				break
			}
		}
		seg := strings.TrimSpace(cypher[b.pos:end])
		if seg != "" {
			segments = append(segments, seg)
		}
		lastPos = b.pos
	}

	return segments
}

type mergeClauseBoundary struct {
	pos int
	kw  string
}

func collectTopLevelMergeClauseBoundaries(cypher string, keywords []string) []mergeClauseBoundary {
	out := make([]mergeClauseBoundary, 0)
	if strings.TrimSpace(cypher) == "" {
		return out
	}

	// Prefer longer keywords first so OPTIONAL MATCH wins over MATCH.
	sort.SliceStable(keywords, func(i, j int) bool { return len(keywords[i]) > len(keywords[j]) })

	upper := upperASCII(cypher)
	inSingle := false
	inDouble := false
	inBacktick := false
	depthParen := 0
	depthBracket := 0
	depthBrace := 0

	for i := 0; i < len(upper); i++ {
		ch := upper[i]
		if inSingle {
			if ch == '\'' {
				inSingle = false
			}
			continue
		}
		if inDouble {
			if ch == '"' {
				inDouble = false
			}
			continue
		}
		if inBacktick {
			if ch == '`' {
				inBacktick = false
			}
			continue
		}

		switch ch {
		case '\'':
			inSingle = true
			continue
		case '"':
			inDouble = true
			continue
		case '`':
			inBacktick = true
			continue
		case '(':
			depthParen++
			continue
		case ')':
			if depthParen > 0 {
				depthParen--
			}
			continue
		case '[':
			depthBracket++
			continue
		case ']':
			if depthBracket > 0 {
				depthBracket--
			}
			continue
		case '{':
			depthBrace++
			continue
		case '}':
			if depthBrace > 0 {
				depthBrace--
			}
			continue
		}

		if depthParen != 0 || depthBracket != 0 || depthBrace != 0 {
			continue
		}

		for _, kw := range keywords {
			if !strings.HasPrefix(upper[i:], kw) {
				continue
			}
			end := i + len(kw)
			if (i == 0 || !isIdentByte(upper[i-1])) && (end >= len(upper) || !isIdentByte(upper[end])) {
				// A CREATE pattern starts with '('. This excludes ON CREATE SET
				// modifiers and identifiers such as n.create.
				if kw == "CREATE" && !isCreatePatternClause(cypher, end) {
					break
				}
				// Skip ON MATCH SET modifier inside MERGE clauses.
				if kw == "MATCH" && (isOnMatchModifier(cypher, i) || isOptionalMatchModifier(cypher, i)) {
					break
				}
				out = append(out, mergeClauseBoundary{pos: i, kw: kw})
				i = end - 1
				break
			}
		}
	}
	return out
}

func isCreatePatternClause(cypher string, afterCreate int) bool {
	return strings.HasPrefix(strings.TrimSpace(cypher[afterCreate:]), "(")
}

func isOnMatchModifier(cypher string, matchPos int) bool {
	prefix := strings.TrimSpace(cypher[:matchPos])
	return strings.HasSuffix(upperASCII(prefix), "ON")
}

func isOptionalMatchModifier(cypher string, matchPos int) bool {
	prefix := strings.TrimSpace(cypher[:matchPos])
	return strings.HasSuffix(upperASCII(prefix), "OPTIONAL")
}

func (e *StorageExecutor) projectWithContext(ctx context.Context, withClause string, nodeCtx map[string]*storage.Node, relCtx map[string]*storage.Edge, scalarCtx map[string]interface{}) (map[string]*storage.Node, map[string]*storage.Edge, map[string]interface{}) {
	withClause = strings.TrimSpace(withClause)
	if withClause == "" {
		return nodeCtx, relCtx, scalarCtx
	}
	if withClause == "*" {
		return nodeCtx, relCtx, scalarCtx
	}

	items := e.parseReturnItems(withClause)
	if len(items) == 0 {
		return nodeCtx, relCtx, scalarCtx
	}

	newNodeCtx := make(map[string]*storage.Node)
	newRelCtx := make(map[string]*storage.Edge)
	newScalarCtx := make(map[string]interface{})

	for _, item := range items {
		alias := strings.TrimSpace(item.alias)
		expr := strings.TrimSpace(item.expr)
		if alias == "" {
			alias = expr
		}
		if alias == "" {
			continue
		}
		val := e.evaluateExpressionWithContext(ctx, expr, nodeCtx, relCtx)
		switch v := val.(type) {
		case *storage.Node:
			newNodeCtx[alias] = v
		case *storage.Edge:
			newRelCtx[alias] = v
		default:
			if scalarCtx != nil {
				if existing, ok := scalarCtx[expr]; ok {
					newScalarCtx[alias] = existing
					continue
				}
			}
			if item.alias != "" && val != nil {
				if literal, ok := val.(string); !ok || literal != expr {
					newScalarCtx[alias] = val
					continue
				}
			}
			if n, ok := nodeCtx[expr]; ok {
				newNodeCtx[alias] = n
			}
			if r, ok := relCtx[expr]; ok {
				newRelCtx[alias] = r
			}
		}
	}

	return newNodeCtx, newRelCtx, newScalarCtx
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
