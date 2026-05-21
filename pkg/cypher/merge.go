// MERGE clause implementation for NornicDB.
// This file contains MERGE execution, compound queries, and context-aware operations.

package cypher

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strconv"
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
	for key, val := range props {
		nodeVal, ok := node.Properties[key]
		if !ok || !reflect.DeepEqual(canonicalUnwindMergeValue(nodeVal), canonicalUnwindMergeValue(val)) {
			return false
		}
	}
	return true
}

func mergeNodeMatchesAnyLabel(node *storage.Node, labels []string, props map[string]interface{}) bool {
	if node == nil {
		return false
	}
	if len(labels) > 0 && !mergeNodeHasAnyLabel(node, labels) {
		return false
	}
	for key, val := range props {
		nodeVal, ok := node.Properties[key]
		if !ok || !reflect.DeepEqual(canonicalUnwindMergeValue(nodeVal), canonicalUnwindMergeValue(val)) {
			return false
		}
	}
	return true
}

func mergeCreateConflict(err error) bool {
	if errors.Is(err, storage.ErrAlreadyExists) {
		return true
	}
	return strings.Contains(strings.ToLower(err.Error()), "already exists")
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
	if len(labels) == 0 || len(props) == 0 {
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
			ids := schema.PropertyIndexLookup(label, prop, val)
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

	allNodes, err := store.AllNodes()
	if err != nil {
		return nil, err
	}
	for _, node := range allNodes {
		if mergeNodeMatches(node, labels, props) {
			e.cacheMergeNode(labels, props, node)
			return node, nil
		}
	}

	return nil, nil
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
				ids := schema.PropertyIndexLookup(label, prop, val)
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
		return nil, fmt.Errorf("MERGE clause not found in query: %q", truncateQuery(cypher, 80))
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
		upperS := strings.ToUpper(s)
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

	// Parse the pattern to extract labels and properties for matching
	// Note: Parameters ($param) should already be substituted by substituteParams()
	varName, labels, matchProps, err := e.parseMergePattern(ctx, mergePattern)

	// If pattern contains unsubstituted params (like $path), handle gracefully
	if strings.Contains(mergePattern, "$") {
		// Extract what we can from the pattern
		varName = e.extractVarName(mergePattern)
		labels = e.extractLabels(mergePattern)
		matchProps = make(map[string]interface{})
		err = nil // Continue with partial info
	}

	if err != nil || (len(labels) == 0 && len(matchProps) == 0) {
		// If we truly can't parse, create a basic node
		node := &storage.Node{
			ID:         storage.NodeID(e.generateID()),
			Labels:     labels,
			Properties: matchProps,
		}
		actualID, err := store.CreateNode(node)
		if err != nil {
			return nil, fmt.Errorf("failed to create node in MERGE: %w", err)
		}
		node.ID = actualID
		e.notifyNodeMutated(string(node.ID))
		result.Stats.NodesCreated = 1

		if varName == "" {
			varName = "n"
		}
		result.Columns = []string{varName}
		result.Rows = append(result.Rows, []interface{}{node})
		return result, nil
	}

	// Try to find existing node
	var existingNode *storage.Node
	if len(labels) > 0 && len(matchProps) > 0 {
		existingNode, err = e.findMergeNode(store, labels, matchProps)
		if err != nil {
			return nil, err
		}
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
			e.applySetToNode(ctx, node, varName, setClause)
			store.UpdateNode(node)
			e.notifyNodeMutated(string(node.ID))
		}
	} else {
		// Node doesn't exist - create it
		node = &storage.Node{
			ID:         storage.NodeID(e.generateID()),
			Labels:     labels,
			Properties: matchProps,
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
					return nil, fmt.Errorf("failed to create node in MERGE: %w", err)
				}
			} else {
				return nil, fmt.Errorf("failed to create node in MERGE: %w", err)
			}
		}
		if existingNode == nil {
			node.ID = actualID
			e.notifyNodeMutated(string(node.ID))
			result.Stats.NodesCreated = 1

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
				e.applySetToNode(ctx, node, varName, setClause)
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
		e.applySetToNode(ctx, node, varName, setClause)
	}

	// Persist updates
	if existingNode != nil || setIdx > 0 || onCreateIdx > 0 {
		store.UpdateNode(node)
		e.notifyNodeMutated(string(node.ID))
	}
	e.cacheMergeNode(labels, matchProps, node)

	// Handle RETURN clause
	if returnIdx > 0 {
		returnClause := strings.TrimSpace(cypher[returnIdx+6:])
		columns, values := e.parseReturnClause(ctx, returnClause, varName, node)
		result.Columns = columns
		if len(values) > 0 {
			result.Rows = append(result.Rows, values)
		}
	}

	return result, nil
}

// executeCompoundMatchMerge handles MATCH ... MERGE ... queries where MERGE references matched nodes.
// This is the Neo4j pattern: MATCH (a) ... MERGE (b) ... SET b.prop = a.prop, etc.
func (e *StorageExecutor) executeCompoundMatchMerge(ctx context.Context, cypher string) (*ExecuteResult, error) {
	// Substitute parameters AFTER routing to avoid keyword detection issues
	if params := getParamsFromContext(ctx); params != nil {
		cypher = e.substituteParams(cypher, params)
	}

	result := &ExecuteResult{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Stats:   &QueryStats{},
	}

	// Use word boundary detection to avoid matching substrings
	matchIdx := findKeywordIndex(cypher, "MATCH")
	mergeIdx := findKeywordIndex(cypher, "MERGE")

	// If MERGE appears at the start, find the second one (after MATCH)
	if mergeIdx <= matchIdx && mergeIdx != -1 {
		// Search for MERGE after MATCH
		afterMatch := cypher[matchIdx+5:]
		secondMergeIdx := findKeywordIndex(afterMatch, "MERGE")
		if secondMergeIdx != -1 {
			mergeIdx = matchIdx + 5 + secondMergeIdx
		}
	}

	if matchIdx == -1 || mergeIdx == -1 {
		return nil, fmt.Errorf("invalid MATCH ... MERGE query")
	}

	// Extract MATCH clause
	matchClause := strings.TrimSpace(cypher[matchIdx:mergeIdx])
	mergeClause := strings.TrimSpace(cypher[mergeIdx:])
	windowVar, windowSkip, windowLimit, hasWindow := parseTrailingWithWindow(matchClause)
	if hasWindow {
		withIdx := findKeywordIndexInContext(matchClause, "WITH")
		if withIdx > 0 {
			matchClause = strings.TrimSpace(matchClause[:withIdx])
		}
	}
	returnIdxInMerge := findKeywordIndex(mergeClause, "RETURN")
	aggregateCountOnly := false
	aggregateCountAlias := "count(*)"
	if returnIdxInMerge > 0 {
		returnPart := strings.TrimSpace(mergeClause[returnIdxInMerge+len("RETURN"):])
		items := e.parseReturnItems(returnPart)
		if len(items) == 1 {
			expr := strings.ToUpper(strings.ReplaceAll(strings.TrimSpace(items[0].expr), " ", ""))
			if expr == "COUNT(*)" {
				aggregateCountOnly = true
				if items[0].alias != "" {
					aggregateCountAlias = items[0].alias
				}
			}
		}
	}

	// Execute MATCH to get context
	matchedNodes, matchedRels, err := e.executeMatchForContext(ctx, matchClause)
	if err != nil {
		return nil, fmt.Errorf("failed to execute MATCH: %v", err)
	}
	if hasWindow {
		matchedNodes = applyContextWindow(matchedNodes, windowVar, windowSkip, windowLimit)
	}

	// If no matches found and not OPTIONAL MATCH, return empty
	if len(matchedNodes) == 0 && findKeywordIndex(cypher, "OPTIONAL MATCH") == -1 {
		if aggregateCountOnly {
			return &ExecuteResult{
				Columns: []string{aggregateCountAlias},
				Rows:    [][]interface{}{{int64(0)}},
				Stats:   result.Stats,
			}, nil
		}
		return result, nil
	}

	// For each set of matched nodes, execute the MERGE with context
	for _, nodeContext := range matchedNodes {
		mergeResult, err := e.executeMergeWithContext(ctx, mergeClause, nodeContext, matchedRels)
		if err != nil {
			return nil, err
		}

		// Combine results
		if mergeResult.Stats != nil {
			result.Stats.NodesCreated += mergeResult.Stats.NodesCreated
			result.Stats.RelationshipsCreated += mergeResult.Stats.RelationshipsCreated
			result.Stats.PropertiesSet += mergeResult.Stats.PropertiesSet
		}

		// Add rows from merge result
		if len(mergeResult.Columns) > 0 && len(result.Columns) == 0 {
			result.Columns = mergeResult.Columns
		}
		if !aggregateCountOnly {
			result.Rows = append(result.Rows, mergeResult.Rows...)
		}
	}

	// If no matched nodes but had OPTIONAL MATCH, still try to execute MERGE
	if len(matchedNodes) == 0 {
		mergeResult, err := e.executeMergeWithContext(ctx, mergeClause, make(map[string]*storage.Node), make(map[string]*storage.Edge))
		if err != nil {
			return nil, err
		}
		result = mergeResult
	}
	if aggregateCountOnly {
		countValue := int64(len(matchedNodes))
		countQuery := strings.TrimSpace(matchClause) + " RETURN count(*) AS " + aggregateCountAlias
		if countRes, err := e.executeMatch(ctx, countQuery); err == nil {
			if len(countRes.Rows) > 0 && len(countRes.Rows[0]) > 0 {
				switch v := countRes.Rows[0][0].(type) {
				case int:
					countValue = int64(v)
				case int32:
					countValue = int64(v)
				case int64:
					countValue = v
				case float64:
					countValue = int64(v)
				}
			}
		}
		result.Columns = []string{aggregateCountAlias}
		result.Rows = [][]interface{}{{countValue}}
	}

	return result, nil
}

func parseTrailingWithWindow(matchClause string) (varName string, skip int, limit int, ok bool) {
	withIdx := findKeywordIndexInContext(matchClause, "WITH")
	if withIdx <= 0 {
		return "", 0, 0, false
	}
	tail := strings.TrimSpace(matchClause[withIdx+len("WITH"):])
	if tail == "" {
		return "", 0, 0, false
	}
	tokens := strings.Fields(tail)
	if len(tokens) == 0 || !isValidIdentifier(tokens[0]) {
		return "", 0, 0, false
	}

	varName = tokens[0]
	skip = 0
	limit = -1
	for i := 1; i < len(tokens); {
		switch strings.ToUpper(tokens[i]) {
		case "SKIP":
			if i+1 >= len(tokens) {
				return "", 0, 0, false
			}
			n, err := strconv.Atoi(tokens[i+1])
			if err != nil || n < 0 {
				return "", 0, 0, false
			}
			skip = n
			i += 2
		case "LIMIT":
			if i+1 >= len(tokens) {
				return "", 0, 0, false
			}
			n, err := strconv.Atoi(tokens[i+1])
			if err != nil || n < 0 {
				return "", 0, 0, false
			}
			limit = n
			i += 2
		default:
			return "", 0, 0, false
		}
	}
	if limit < 0 {
		return "", 0, 0, false
	}
	return varName, skip, limit, true
}

func applyContextWindow(contexts []map[string]*storage.Node, variable string, skip int, limit int) []map[string]*storage.Node {
	if len(contexts) == 0 || limit == 0 {
		return nil
	}
	filtered := make([]map[string]*storage.Node, 0, len(contexts))
	for _, ctx := range contexts {
		if _, ok := ctx[variable]; ok {
			filtered = append(filtered, ctx)
		}
	}
	if skip >= len(filtered) {
		return nil
	}
	end := skip + limit
	if end > len(filtered) {
		end = len(filtered)
	}
	return filtered[skip:end]
}

// executeMatchForContext executes a MATCH clause and returns matched nodes by variable name.
// Handles both simple node patterns like (a:Label), (b:Label2) and relationship patterns
// like (a)<-[:REL]-(b)-[:REL]->(c).
func (e *StorageExecutor) executeMatchForContext(ctx context.Context, matchClause string) ([]map[string]*storage.Node, map[string]*storage.Edge, error) {
	relMatches := make(map[string]*storage.Edge)
	store := e.getStorage(ctx)

	// Find WHERE clause if present (newline/tab tolerant).
	whereIdx := findKeywordIndex(matchClause, "WHERE")
	var patternPart string

	if whereIdx > 0 {
		patternPart = matchClause[5:whereIdx]
	} else {
		patternPart = matchClause[5:]
	}

	patternPart = strings.TrimSpace(patternPart)

	// Check if this is a relationship pattern (contains ->, <-, or ]-)
	// If so, we need to use proper path matching, not cartesian product
	hasRelationship := strings.Contains(patternPart, "->") ||
		strings.Contains(patternPart, "<-") ||
		strings.Contains(patternPart, "]-")

	if hasRelationship {
		// Use executeMatch to properly find paths, then extract variable bindings
		return e.executeMatchForContextWithRelationships(ctx, matchClause, patternPart)
	}

	// Simple node patterns only - use cartesian product approach
	// Split multiple node patterns: (a:Label), (b:Label2)
	nodePatterns := e.splitNodePatterns(patternPart)

	// If no patterns found, try parsing as single pattern
	if len(nodePatterns) == 0 {
		nodePatterns = []string{patternPart}
	}

	// For each node pattern, find matching nodes
	patternMatches := make([]struct {
		variable string
		nodes    []*storage.Node
	}, len(nodePatterns))

	for i, np := range nodePatterns {
		nodeInfo := e.parseNodePattern(ctx, np)

		var candidates []*storage.Node
		// Single-pattern WHERE fast-path: reduce candidates via property index lookup
		// when possible (e.g., o.textKey128='x' OR ('y' IS NOT NULL AND o.textKey='y')).
		if whereIdx > 0 && len(nodePatterns) == 1 {
			wherePart := strings.TrimSpace(matchClause[whereIdx+len("WHERE"):])
			if indexed, ok := e.lookupWhereCandidatesUsingPropertyIndex(nodeInfo, wherePart, store); ok {
				candidates = indexed
			}
		}
		if candidates == nil {
			if len(nodeInfo.labels) > 0 {
				candidates, _ = store.GetNodesByLabel(nodeInfo.labels[0])
			} else {
				candidates = store.GetAllNodes()
			}
		}

		// Filter by properties
		var filtered []*storage.Node
		for _, node := range candidates {
			if e.nodeMatchesProps(node, nodeInfo.properties) {
				filtered = append(filtered, node)
			}
		}

		patternMatches[i] = struct {
			variable string
			nodes    []*storage.Node
		}{
			variable: nodeInfo.variable,
			nodes:    filtered,
		}
	}

	// Build cartesian product of all pattern matches
	allMatches := e.buildCartesianProduct(patternMatches)

	// Apply WHERE clause to each combination
	if whereIdx > 0 {
		wherePart := strings.TrimSpace(matchClause[whereIdx+len("WHERE"):])
		var filtered []map[string]*storage.Node
		for _, nodeMap := range allMatches {
			if e.evaluateWhereForNodeMap(ctx, nodeMap, wherePart) {
				filtered = append(filtered, nodeMap)
			}
		}
		allMatches = filtered
	}

	return allMatches, relMatches, nil
}

func (e *StorageExecutor) evaluateWhereForNodeMap(ctx context.Context, nodeMap map[string]*storage.Node, wherePart string) bool {
	wherePart = strings.Join(strings.Fields(strings.TrimSpace(wherePart)), " ")
	if wherePart == "" {
		return true
	}
	if andIdx := findTopLevelKeyword(wherePart, " AND "); andIdx > 0 {
		left := strings.TrimSpace(wherePart[:andIdx])
		right := strings.TrimSpace(wherePart[andIdx+5:])
		return e.evaluateWhereForNodeMap(ctx, nodeMap, left) && e.evaluateWhereForNodeMap(ctx, nodeMap, right)
	}
	if handled, ok := e.evaluateSimpleWhereClauseForNodeMap(ctx, nodeMap, wherePart); handled {
		return ok
	}
	for varName, node := range nodeMap {
		if node == nil {
			continue
		}
		if !e.evaluateWhere(ctx, node, varName, wherePart) {
			lowerWhere := strings.ToLower(wherePart)
			refsVar := strings.Contains(wherePart, varName+".") ||
				strings.Contains(wherePart, varName+" ") ||
				strings.Contains(lowerWhere, "id("+varName+")") ||
				strings.Contains(lowerWhere, "elementid("+varName+")")
			if refsVar {
				return false
			}
		}
	}
	return true
}

func (e *StorageExecutor) lookupWhereCandidatesUsingPropertyIndex(nodeInfo nodePatternInfo, wherePart string, store storage.Engine) ([]*storage.Node, bool) {
	if len(nodeInfo.labels) == 0 {
		return nil, false
	}
	// Safety: don't index-narrow top-level OR expressions.
	// A single property index lookup can under-approximate OR semantics when one
	// branch is non-selective or when an index backend is non-unique for a value.
	// Fall back to normal filtering to preserve openCypher correctness.
	if findTopLevelKeyword(wherePart, " OR ") >= 0 {
		return nil, false
	}
	schema := store.GetSchema()
	if schema == nil {
		return nil, false
	}
	label := nodeInfo.labels[0]
	terms := splitTopLevelOrTerms(wherePart)
	if len(terms) == 0 {
		terms = []string{wherePart}
	}

	idSet := make(map[storage.NodeID]struct{}, 8)
	for _, term := range terms {
		term = strings.TrimSpace(term)
		if term == "" {
			continue
		}
		prop, lit, ok := e.extractIndexedEqualityFromWhereTerm(nodeInfo.variable, term)
		if !ok {
			continue
		}
		ids := schema.PropertyIndexLookup(label, prop, lit)
		for _, id := range ids {
			idSet[id] = struct{}{}
		}
	}
	if len(idSet) == 0 {
		return nil, false
	}
	out := make([]*storage.Node, 0, len(idSet))
	for id := range idSet {
		n, err := store.GetNode(id)
		if err == nil && n != nil {
			out = append(out, n)
		}
	}
	return out, true
}

func splitTopLevelOrTerms(expr string) []string {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return nil
	}
	orIdx := findTopLevelKeyword(expr, " OR ")
	if orIdx < 0 {
		return []string{expr}
	}
	left := strings.TrimSpace(expr[:orIdx])
	right := strings.TrimSpace(expr[orIdx+4:])
	out := make([]string, 0, 4)
	out = append(out, splitTopLevelOrTerms(left)...)
	out = append(out, splitTopLevelOrTerms(right)...)
	return out
}

func (e *StorageExecutor) extractIndexedEqualityFromWhereTerm(variable, term string) (string, interface{}, bool) {
	term = strings.TrimSpace(term)
	if term == "" {
		return "", nil, false
	}
	if strings.HasPrefix(term, "(") && strings.HasSuffix(term, ")") {
		inner := strings.TrimSpace(term[1 : len(term)-1])
		if inner != "" {
			term = inner
		}
	}

	if prop, lit, ok := e.parseVarPropEqualsLiteral(variable, term); ok {
		return prop, lit, true
	}

	parts := splitTopLevelAndCartesian(term)
	if len(parts) < 2 {
		return "", nil, false
	}
	var (
		prop   string
		lit    interface{}
		haveEq bool
	)
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		if eqProp, eqLit, ok := e.parseVarPropEqualsLiteral(variable, p); ok {
			prop, lit, haveEq = eqProp, eqLit, true
			continue
		}
		if !e.isLiteralIsNotNullExpr(p) {
			return "", nil, false
		}
	}
	if haveEq {
		return prop, lit, true
	}
	return "", nil, false
}

func (e *StorageExecutor) parseVarPropEqualsLiteral(variable, expr string) (string, interface{}, bool) {
	expr = strings.TrimSpace(expr)
	if strings.Count(expr, "=") != 1 ||
		strings.Contains(expr, ">=") || strings.Contains(expr, "<=") ||
		strings.Contains(expr, "!=") || strings.Contains(expr, "<>") {
		return "", nil, false
	}
	parts := strings.SplitN(expr, "=", 2)
	if len(parts) != 2 {
		return "", nil, false
	}
	left := strings.TrimSpace(parts[0])
	right := strings.TrimSpace(parts[1])
	prefix := variable + "."
	if !strings.HasPrefix(left, prefix) {
		return "", nil, false
	}
	prop := strings.TrimSpace(left[len(prefix):])
	if prop == "" {
		return "", nil, false
	}
	lit, ok := parseWhereLiteral(right)
	if !ok {
		return "", nil, false
	}
	return prop, lit, true
}

func (e *StorageExecutor) isLiteralIsNotNullExpr(expr string) bool {
	expr = strings.TrimSpace(expr)
	up := strings.ToUpper(expr)
	needle := " IS NOT NULL"
	idx := strings.Index(up, needle)
	if idx <= 0 {
		return false
	}
	lit := strings.TrimSpace(expr[:idx])
	v, ok := parseWhereLiteral(lit)
	if !ok {
		return false
	}
	return v != nil
}

func parseWhereLiteral(token string) (interface{}, bool) {
	token = strings.TrimSpace(token)
	if token == "" {
		return nil, false
	}
	if strings.EqualFold(token, "null") {
		return nil, true
	}
	if strings.EqualFold(token, "true") {
		return true, true
	}
	if strings.EqualFold(token, "false") {
		return false, true
	}
	if len(token) >= 2 && token[0] == '\'' && token[len(token)-1] == '\'' {
		s := token[1 : len(token)-1]
		s = strings.ReplaceAll(s, "''", "'")
		s = strings.ReplaceAll(s, "\\\\", "\\")
		return s, true
	}
	if i, err := strconv.ParseInt(token, 10, 64); err == nil {
		return i, true
	}
	if f, err := strconv.ParseFloat(token, 64); err == nil {
		return f, true
	}
	return nil, false
}

func (e *StorageExecutor) evaluateSimpleWhereClauseForNodeMap(ctx context.Context, nodeMap map[string]*storage.Node, clause string) (bool, bool) {
	clause = strings.TrimSpace(clause)
	if clause == "" {
		return true, true
	}
	upper := strings.ToUpper(clause)
	if inIdx := strings.Index(upper, " IN "); inIdx > 0 {
		left := strings.TrimSpace(clause[:inIdx])
		right := strings.TrimSpace(clause[inIdx+4:])
		lv, lok := lookupNodeMapProperty(nodeMap, left)
		if !lok {
			return false, false
		}
		rv := e.evaluateExpressionWithContext(ctx, right, nodeMap, make(map[string]*storage.Edge))
		items, ok := normalizeWhereList(rv)
		if !ok {
			return true, false
		}
		for _, it := range items {
			if e.compareEqual(lv, it) {
				return true, true
			}
		}
		return true, false
	}
	if eqIdx := strings.Index(clause, "="); eqIdx > 0 &&
		!strings.Contains(clause, ">=") &&
		!strings.Contains(clause, "<=") &&
		!strings.Contains(clause, "!=") &&
		!strings.Contains(clause, "<>") {
		left := strings.TrimSpace(clause[:eqIdx])
		right := strings.TrimSpace(clause[eqIdx+1:])
		lv, lok := lookupNodeMapProperty(nodeMap, left)
		rv, rok := lookupNodeMapProperty(nodeMap, right)
		switch {
		case lok && rok:
			return true, e.compareEqual(lv, rv)
		case lok:
			rvExpr := e.evaluateExpressionWithContext(ctx, right, nodeMap, make(map[string]*storage.Edge))
			return true, e.compareEqual(lv, rvExpr)
		case rok:
			lvExpr := e.evaluateExpressionWithContext(ctx, left, nodeMap, make(map[string]*storage.Edge))
			return true, e.compareEqual(lvExpr, rv)
		default:
			return false, false
		}
	}
	return false, false
}

func lookupNodeMapProperty(nodeMap map[string]*storage.Node, expr string) (interface{}, bool) {
	parts := strings.SplitN(strings.TrimSpace(expr), ".", 2)
	if len(parts) != 2 {
		return nil, false
	}
	v := strings.TrimSpace(parts[0])
	p := strings.TrimSpace(parts[1])
	n, ok := nodeMap[v]
	if !ok || n == nil {
		return nil, false
	}
	val, exists := n.Properties[p]
	if !exists {
		return nil, false
	}
	return val, true
}

func normalizeWhereList(v interface{}) ([]interface{}, bool) {
	switch t := v.(type) {
	case []interface{}:
		return t, true
	case []string:
		out := make([]interface{}, 0, len(t))
		for _, x := range t {
			out = append(out, x)
		}
		return out, true
	case []int:
		out := make([]interface{}, 0, len(t))
		for _, x := range t {
			out = append(out, x)
		}
		return out, true
	case []int64:
		out := make([]interface{}, 0, len(t))
		for _, x := range t {
			out = append(out, x)
		}
		return out, true
	case []float64:
		out := make([]interface{}, 0, len(t))
		for _, x := range t {
			out = append(out, x)
		}
		return out, true
	default:
		return nil, false
	}
}

// executeMatchForContextWithRelationships handles MATCH patterns that include relationships.
// It executes the MATCH query and extracts variable bindings from the results.
func (e *StorageExecutor) executeMatchForContextWithRelationships(ctx context.Context, matchClause, patternPart string) ([]map[string]*storage.Node, map[string]*storage.Edge, error) {
	relMatches := make(map[string]*storage.Edge)
	store := e.getStorage(ctx)

	// Fail fast on malformed relationship patterns instead of returning
	// an implicit empty context. This keeps behavior strict and predictable.
	if strings.Count(patternPart, "(") != strings.Count(patternPart, ")") ||
		strings.Count(patternPart, "[") != strings.Count(patternPart, "]") {
		return nil, relMatches, fmt.Errorf("malformed relationship pattern: %s", patternPart)
	}

	// Extract all variable names from the pattern
	varNames := e.extractVariableNamesFromPattern(patternPart)
	if len(varNames) == 0 {
		return nil, relMatches, nil
	}

	// Build a synthetic RETURN clause to get all node variables
	// Filter to only include node variables (not relationship variables)
	nodeVarNames := make([]string, 0)
	for _, v := range varNames {
		// Skip relationship variables (they appear after [ and before ])
		// Node variables appear after ( and before )
		nodeVarNames = append(nodeVarNames, v)
	}

	if len(nodeVarNames) == 0 {
		return nil, relMatches, nil
	}

	// Build RETURN clause with all variables
	returnClause := "RETURN " + strings.Join(nodeVarNames, ", ")
	fullQuery := matchClause + " " + returnClause

	// Execute the match
	result, err := e.executeMatch(ctx, fullQuery)
	if err != nil {
		return nil, relMatches, err
	}

	// Convert results to node context maps
	var allMatches []map[string]*storage.Node

	for _, row := range result.Rows {
		nodeMap := make(map[string]*storage.Node)
		for i, col := range result.Columns {
			if i >= len(row) {
				continue
			}

			// Get the node from storage based on the returned value
			val := row[i]
			if val == nil {
				continue
			}

			// The returned value might be a map or a node representation
			var node *storage.Node

			switch v := val.(type) {
			case map[string]interface{}:
				// It's a map representation - find the actual node
				// Look for an ID property or _id
				if id, ok := v["_id"]; ok {
					if nodeID, ok := id.(string); ok {
						node, _ = store.GetNode(storage.NodeID(nodeID))
					}
				} else if id, ok := v["id"]; ok {
					if nodeID, ok := id.(string); ok {
						node, _ = store.GetNode(storage.NodeID(nodeID))
					}
				}
				// If we still don't have a node, try to find by properties
				if node == nil {
					// Try to find by matching properties
					node = e.findNodeByProperties(v)
				}
			case *storage.Node:
				node = v
			case storage.Node:
				node = &v
			}

			if node != nil {
				nodeMap[col] = node
			}
		}

		if len(nodeMap) > 0 {
			allMatches = append(allMatches, nodeMap)
		}
	}

	return allMatches, relMatches, nil
}

// extractVariableNamesFromPattern extracts variable names from a Cypher pattern.
// e.g., "(p:Person)<-[:REL]-(poc:POC)-[:BELONGS_TO]->(a:Area)" returns ["p", "poc", "a"]
func (e *StorageExecutor) extractVariableNamesFromPattern(pattern string) []string {
	var varNames []string
	seen := make(map[string]bool)

	// Find all node patterns (...)
	inParen := false
	inBracket := false
	var current strings.Builder

	for _, c := range pattern {
		switch c {
		case '(':
			inParen = true
			current.Reset()
		case ')':
			if inParen {
				nodeContent := current.String()
				// Extract variable name (before : or end)
				varName := strings.Split(nodeContent, ":")[0]
				varName = strings.TrimSpace(varName)
				// Remove any property part
				if idx := strings.Index(varName, "{"); idx > 0 {
					varName = strings.TrimSpace(varName[:idx])
				}
				if varName != "" && !seen[varName] {
					varNames = append(varNames, varName)
					seen[varName] = true
				}
			}
			inParen = false
		case '[':
			inBracket = true
		case ']':
			inBracket = false
		default:
			if inParen && !inBracket {
				current.WriteRune(c)
			}
		}
	}

	return varNames
}

// findNodeByProperties finds a node by matching its properties.
func (e *StorageExecutor) findNodeByProperties(props map[string]interface{}) *storage.Node {
	// Get all nodes and try to match
	allNodes := e.storage.GetAllNodes()
	for _, node := range allNodes {
		// Check if name property matches (common identifier)
		if name, ok := props["name"]; ok {
			if nodeName, ok := node.Properties["name"]; ok && nodeName == name {
				return node
			}
		}
		// Check if all provided properties match
		matches := true
		for k, v := range props {
			if k == "_labels" || k == "_id" {
				continue
			}
			if nodeVal, ok := node.Properties[k]; !ok || nodeVal != v {
				matches = false
				break
			}
		}
		if matches && len(props) > 0 {
			return node
		}
	}
	return nil
}

// buildCartesianProduct creates all combinations of node matches
func (e *StorageExecutor) buildCartesianProduct(patternMatches []struct {
	variable string
	nodes    []*storage.Node
}) []map[string]*storage.Node {
	if len(patternMatches) == 0 {
		return nil
	}

	// Start with first pattern's nodes
	var result []map[string]*storage.Node
	for _, node := range patternMatches[0].nodes {
		result = append(result, map[string]*storage.Node{
			patternMatches[0].variable: node,
		})
	}

	// For each subsequent pattern, expand the combinations
	for i := 1; i < len(patternMatches); i++ {
		pm := patternMatches[i]
		var expanded []map[string]*storage.Node

		for _, existing := range result {
			for _, node := range pm.nodes {
				// Copy existing map and add new variable
				newMap := make(map[string]*storage.Node)
				for k, v := range existing {
					newMap[k] = v
				}
				newMap[pm.variable] = node
				expanded = append(expanded, newMap)
			}
		}

		result = expanded
	}

	return result
}

// executeMergeWithContext executes a MERGE clause with context from a prior MATCH.
func (e *StorageExecutor) executeMergeWithContext(ctx context.Context, cypher string, nodeContext map[string]*storage.Node, relContext map[string]*storage.Edge) (*ExecuteResult, error) {
	result := &ExecuteResult{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Stats:   &QueryStats{},
	}
	store := e.getStorage(ctx)

	// Find clauses - use word boundary detection
	mergeIdx := findKeywordIndex(cypher, "MERGE")
	if mergeIdx == -1 {
		mergeIdx = 0 // Already stripped
	}

	onCreateIdx := findKeywordIndex(cypher, "ON CREATE SET")
	onMatchIdx := findKeywordIndex(cypher, "ON MATCH SET")
	// Use quote-aware search for RETURN and WITH since text content may contain these keywords
	returnIdx := findKeywordIndexInContext(cypher, "RETURN")
	withIdx := findKeywordIndexInContext(cypher, "WITH")

	setIdx := findStandaloneSetInMergeSegment(cypher)

	// Find MERGE pattern end
	patternEnd := len(cypher)
	for _, idx := range []int{onCreateIdx, onMatchIdx, setIdx, returnIdx, withIdx} {
		if idx > 0 && idx < patternEnd {
			patternEnd = idx
		}
	}

	// Handle second MERGE in compound query (handle any whitespace before MERGE)
	// Use quote-aware search since text content may contain "MERGE" keyword
	secondMergeIdx := findKeywordIndexInContext(cypher[mergeIdx+5:], "MERGE")
	if secondMergeIdx > 0 {
		// There's a second MERGE clause - this is for relationships
		// Handle the first MERGE, then process second
		firstMergeEnd := mergeIdx + 5 + secondMergeIdx
		if firstMergeEnd < patternEnd {
			patternEnd = firstMergeEnd
		}
	}

	// Extract and parse MERGE pattern
	mergePattern := strings.TrimSpace(cypher[mergeIdx+5 : patternEnd])

	// Check if this is a relationship pattern: (a)-[r:TYPE]->(b)
	if strings.Contains(mergePattern, "->") || strings.Contains(mergePattern, "<-") || strings.Contains(mergePattern, "]-") {
		// Relationship MERGE - create relationship, then continue processing chained MERGE clauses.
		relationshipResult, err := e.executeMergeRelationshipWithContext(ctx, cypher, mergePattern, nodeContext, relContext)
		if err != nil {
			return nil, err
		}
		if secondMergeIdx > 0 {
			secondMergePart := strings.TrimSpace(cypher[mergeIdx+5+secondMergeIdx:])
			if !strings.HasPrefix(strings.ToUpper(secondMergePart), "MERGE ") {
				trimmed := strings.TrimSpace(secondMergePart)
				if strings.HasPrefix(trimmed, "(") {
					secondMergePart = "MERGE " + trimmed
				}
			}
			nextResult, err := e.executeMergeWithContext(ctx, secondMergePart, nodeContext, relContext)
			if err != nil {
				return nil, err
			}
			if relationshipResult != nil && relationshipResult.Stats != nil && nextResult != nil && nextResult.Stats != nil {
				nextResult.Stats.RelationshipsCreated += relationshipResult.Stats.RelationshipsCreated
				nextResult.Stats.NodesCreated += relationshipResult.Stats.NodesCreated
				nextResult.Stats.PropertiesSet += relationshipResult.Stats.PropertiesSet
			}
			return nextResult, nil
		}
		return relationshipResult, nil
	}

	// Parse node pattern
	varName, labels, matchProps, err := e.parseMergePattern(ctx, mergePattern)
	if err != nil || varName == "" {
		varName = e.extractVarName(mergePattern)
		labels = e.extractLabels(mergePattern)
		matchProps = make(map[string]interface{})
	}
	matchProps = e.resolveMergePropsWithContext(ctx, matchProps, nodeContext, relContext)

	// Try to find existing node
	var existingNode *storage.Node
	if len(labels) > 0 && len(matchProps) > 0 {
		existingNode, err = e.findMergeNode(store, labels, matchProps)
		if err != nil {
			return nil, err
		}
	}

	var node *storage.Node
	if existingNode != nil {
		node = existingNode
		e.cacheMergeNode(labels, matchProps, node)
		if onMatchIdx > 0 {
			setEnd := len(cypher)
			for _, idx := range []int{onCreateIdx, returnIdx, withIdx, setIdx} {
				if idx > onMatchIdx && idx < setEnd {
					setEnd = idx
				}
			}
			setClause := strings.TrimSpace(cypher[onMatchIdx+13 : setEnd])
			e.applySetToNodeWithContext(ctx, node, varName, setClause, nodeContext, relContext)
			store.UpdateNode(node)
			e.notifyNodeMutated(string(node.ID))
		}
	} else {
		node = &storage.Node{
			ID:         storage.NodeID(e.generateID()),
			Labels:     labels,
			Properties: matchProps,
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
					node = recoveredNode
					e.cacheMergeNode(labels, matchProps, node)
				} else {
					return nil, fmt.Errorf("failed to create node in MERGE: %w", err)
				}
			} else {
				return nil, fmt.Errorf("failed to create node in MERGE: %w", err)
			}
		}
		if existingNode == nil {
			node.ID = actualID
			e.notifyNodeMutated(string(node.ID))
			result.Stats.NodesCreated = 1
			e.cacheMergeNode(labels, matchProps, node)

			if onCreateIdx > 0 {
				setEnd := len(cypher)
				for _, idx := range []int{setIdx, onMatchIdx, withIdx, returnIdx} {
					if idx > onCreateIdx && idx < setEnd {
						setEnd = idx
					}
				}
				setClause := strings.TrimSpace(cypher[onCreateIdx+13 : setEnd])
				e.applySetToNodeWithContext(ctx, node, varName, setClause, nodeContext, relContext)
			}
		}
	}

	// Apply standalone SET
	if setIdx > 0 {
		setEnd := len(cypher)
		// Also check for second MERGE - SET clause ends there too
		secondMergeAbsIdx := -1
		if secondMergeIdx > 0 {
			secondMergeAbsIdx = mergeIdx + 5 + secondMergeIdx
		}
		for _, idx := range []int{withIdx, returnIdx, secondMergeAbsIdx} {
			if idx > setIdx && idx < setEnd {
				setEnd = idx
			}
		}
		setClause := strings.TrimSpace(cypher[setIdx+3 : setEnd])
		e.applySetToNodeWithContext(ctx, node, varName, setClause, nodeContext, relContext)
	}

	// Save updates
	store.UpdateNode(node)
	e.notifyNodeMutated(string(node.ID))
	e.cacheMergeNode(labels, matchProps, node)

	// Add this node to context for subsequent MERGEs
	nodeContext[varName] = node

	// Handle second MERGE (usually relationship creation)
	if secondMergeIdx > 0 {
		secondMergePart := strings.TrimSpace(cypher[mergeIdx+5+secondMergeIdx:])
		if !strings.HasPrefix(strings.ToUpper(secondMergePart), "MERGE ") {
			trimmed := strings.TrimSpace(secondMergePart)
			if strings.HasPrefix(trimmed, "(") {
				secondMergePart = "MERGE " + trimmed
			}
		}
		_, err := e.executeMergeWithContext(ctx, secondMergePart, nodeContext, relContext)
		if err != nil {
			return nil, err
		}
	}

	// Handle RETURN clause
	if returnIdx > 0 {
		returnClause := strings.TrimSpace(cypher[returnIdx+6:])
		columns, values := e.parseReturnClauseWithContext(ctx, returnClause, nodeContext, relContext)
		result.Columns = columns
		if len(values) > 0 {
			result.Rows = append(result.Rows, values)
		}
	}

	return result, nil
}

func (e *StorageExecutor) resolveMergePropsWithContext(ctx context.Context, props map[string]interface{}, nodeContext map[string]*storage.Node, relContext map[string]*storage.Edge) map[string]interface{} {
	if len(props) == 0 {
		return props
	}
	resolved := make(map[string]interface{}, len(props))
	for k, v := range props {
		s, ok := v.(string)
		if !ok {
			resolved[k] = v
			continue
		}
		expr := strings.TrimSpace(s)
		if expr == "" {
			resolved[k] = v
			continue
		}
		dot := strings.Index(expr, ".")
		if dot <= 0 {
			resolved[k] = v
			continue
		}
		root := strings.TrimSpace(expr[:dot])
		if _, ok := nodeContext[root]; !ok {
			if _, ok := relContext[root]; !ok {
				resolved[k] = v
				continue
			}
		}
		evaluated := e.evaluateExpressionWithContext(ctx, expr, nodeContext, relContext)
		resolved[k] = evaluated
	}
	return resolved
}

// executeMergeRelationshipWithContext handles MERGE for relationship patterns.
func (e *StorageExecutor) executeMergeRelationshipWithContext(ctx context.Context, cypher string, pattern string, nodeContext map[string]*storage.Node, relContext map[string]*storage.Edge) (*ExecuteResult, error) {
	result := &ExecuteResult{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Stats:   &QueryStats{},
	}
	store := e.getStorage(ctx)

	returnIdx := findKeywordIndex(cypher, "RETURN")

	// Parse relationship pattern: (a)-[r:TYPE {props}]->(b)
	// Extract start node, relationship, end node

	// Find the relationship part
	relStart := strings.Index(pattern, "[")
	relEnd := strings.Index(pattern, "]")

	if relStart == -1 || relEnd == -1 {
		return result, nil // Not a valid relationship pattern
	}

	setSearchStart := 0
	if patternIdx := strings.Index(cypher, pattern); patternIdx >= 0 {
		setSearchStart = patternIdx + len(pattern)
	}
	setIdx := findStandaloneSetInMergeSegmentFrom(cypher, setSearchStart)

	// Get start and end node variables
	startPart := strings.TrimSpace(pattern[:relStart])
	endPart := strings.TrimSpace(pattern[relEnd+1:])
	relPart := pattern[relStart+1 : relEnd]

	// Remove direction markers and parens
	startPart = strings.Trim(startPart, "()-")
	endPart = strings.Trim(endPart, "()<>-")

	// Extract start/end variable names
	startVar := strings.Split(startPart, ":")[0]
	endVar := strings.Split(endPart, ":")[0]

	// Parse relationship type and variable
	relVar := ""
	relType := ""
	relProps := make(map[string]interface{})

	relPart = strings.TrimSpace(relPart)
	propsStart := strings.Index(relPart, "{")
	if propsStart > 0 {
		propsEnd := strings.LastIndex(relPart, "}")
		if propsEnd > propsStart {
			relProps = e.parseProperties(ctx, relPart[propsStart:propsEnd+1])
		}
		relPart = relPart[:propsStart]
	}

	relParts := strings.Split(relPart, ":")
	if len(relParts) > 0 {
		relVar = strings.TrimSpace(relParts[0])
	}
	if len(relParts) > 1 {
		relType = strings.TrimSpace(relParts[1])
	}

	// Get start and end nodes from context
	startNode := nodeContext[startVar]
	endNode := nodeContext[endVar]

	if startNode == nil || endNode == nil {
		// Nodes not in context - can't create relationship
		return result, nil
	}

	// Check if relationship exists
	existingEdge := store.GetEdgeBetween(startNode.ID, endNode.ID, relType)

	var edge *storage.Edge
	if existingEdge != nil {
		edge = existingEdge
	} else {
		// Create new relationship
		edge = &storage.Edge{
			ID:         storage.EdgeID(e.generateID()),
			Type:       relType,
			StartNode:  startNode.ID,
			EndNode:    endNode.ID,
			Properties: relProps,
		}
		err := store.CreateEdge(edge)
		if err != nil {
			// If already exists error, ignore it (MERGE semantics)
			if err == storage.ErrAlreadyExists {
				// Try to find the existing edge again
				existingEdge = store.GetEdgeBetween(startNode.ID, endNode.ID, relType)
				if existingEdge != nil {
					edge = existingEdge
				}
			} else {
				return nil, fmt.Errorf("failed to create relationship: %w", err)
			}
		} else {
			result.Stats.RelationshipsCreated = 1
		}
	}

	// Store in context
	if relVar != "" {
		relContext[relVar] = edge
	}

	if setIdx > 0 && relVar != "" {
		setEnd := len(cypher)
		if returnIdx > setIdx && returnIdx < setEnd {
			setEnd = returnIdx
		}
		setClause := strings.TrimSpace(cypher[setIdx+3 : setEnd])
		if propertiesSet := e.applySetToRelationshipWithContext(ctx, edge, relVar, setClause, nodeContext, relContext); propertiesSet > 0 {
			if err := store.UpdateEdge(edge); err != nil {
				return nil, fmt.Errorf("failed to update edge property: %w", err)
			}
			result.Stats.PropertiesSet += propertiesSet
		}
	}

	// Handle RETURN
	if returnIdx > 0 {
		returnClause := strings.TrimSpace(cypher[returnIdx+6:])
		columns, values := e.parseReturnClauseWithContext(ctx, returnClause, nodeContext, relContext)
		result.Columns = columns
		if len(values) > 0 {
			result.Rows = append(result.Rows, values)
		}
	}

	return result, nil
}

func (e *StorageExecutor) applySetToRelationshipWithContext(ctx context.Context, edge *storage.Edge, varName string, setClause string, nodeContext map[string]*storage.Node, relContext map[string]*storage.Edge) int {
	if edge == nil || varName == "" {
		return 0
	}
	fullRelContext := make(map[string]*storage.Edge)
	for k, v := range relContext {
		fullRelContext[k] = v
	}
	fullRelContext[varName] = edge

	propertiesSet := 0
	assignments := e.splitSetAssignments(setClause)
	for _, assignment := range assignments {
		assignment = strings.TrimSpace(assignment)
		if assignment == "" {
			continue
		}

		if plusEqIdx := strings.Index(assignment, "+="); plusEqIdx > 0 {
			left := strings.TrimSpace(assignment[:plusEqIdx])
			right := strings.TrimSpace(assignment[plusEqIdx+2:])
			if left != varName {
				continue
			}
			evaluated := e.evaluateExpressionWithContext(ctx, right, nodeContext, fullRelContext)
			if s, ok := evaluated.(string); ok && strings.TrimSpace(s) == strings.TrimSpace(right) {
				evaluated = e.parseValue(ctx, strings.TrimSpace(right))
			}
			if evaluated == nil {
				evaluated = e.parseValue(ctx, strings.TrimSpace(right))
			}
			if props, ok := toStringAnyMap(evaluated); ok {
				if edge.Properties == nil {
					edge.Properties = make(map[string]interface{})
				}
				for k, v := range props {
					edge.Properties[k] = v
					propertiesSet++
				}
			}
			continue
		}

		if !strings.HasPrefix(assignment, varName+".") {
			continue
		}
		eqIdx := strings.Index(assignment, "=")
		if eqIdx <= 0 {
			continue
		}
		propName := strings.TrimSpace(assignment[len(varName)+1 : eqIdx])
		propValue := strings.TrimSpace(assignment[eqIdx+1:])
		if propName == "" {
			continue
		}
		if edge.Properties == nil {
			edge.Properties = make(map[string]interface{})
		}
		// Direct $param resolution preserves declared types (e.g. []string,
		// []float64) end-to-end. Without this branch, substituteParams's
		// type-preserving short-circuit leaves the literal text "$name"
		// here, and the generic expression evaluator would only return
		// the unresolved string.
		if v, ok := resolveDirectParamRef(ctx, propValue); ok {
			edge.Properties[propName] = normalizePropValue(v)
			propertiesSet++
			continue
		}
		edge.Properties[propName] = e.evaluateSetExpressionWithContext(ctx, propValue, nodeContext, fullRelContext)
		propertiesSet++
	}
	return propertiesSet
}

// applySetToNodeWithContext applies SET clauses with access to matched context.
func (e *StorageExecutor) applySetToNodeWithContext(ctx context.Context, node *storage.Node, varName string, setClause string, nodeContext map[string]*storage.Node, relContext map[string]*storage.Edge) {
	// Add current node to context for self-references
	fullContext := make(map[string]*storage.Node)
	for k, v := range nodeContext {
		fullContext[k] = v
	}
	fullContext[varName] = node

	// Split SET clause into individual assignments
	assignments := e.splitSetAssignments(setClause)

	for _, assignment := range assignments {
		assignment = strings.TrimSpace(assignment)

		// Support map-merge in MERGE-with-context flows:
		// SET n += {...} / SET n += row.props
		if plusEqIdx := strings.Index(assignment, "+="); plusEqIdx > 0 {
			left := strings.TrimSpace(assignment[:plusEqIdx])
			right := strings.TrimSpace(assignment[plusEqIdx+2:])
			if left == varName {
				e.applySetMapMergeToNode(ctx, node, varName, right, fullContext, relContext)
			}
			continue
		}

		if !strings.HasPrefix(assignment, varName+".") {
			continue
		}

		eqIdx := strings.Index(assignment, "=")
		if eqIdx <= 0 {
			continue
		}

		propName := strings.TrimSpace(assignment[len(varName)+1 : eqIdx])
		propValue := strings.TrimSpace(assignment[eqIdx+1:])

		// Direct $param resolution preserves declared types end-to-end.
		// Without it, substituteParams's type-preserving short-circuit
		// leaves "$name" as a literal and the generic evaluator returns
		// the unresolved string.
		if v, ok := resolveDirectParamRef(ctx, propValue); ok {
			setNodeProperty(node, propName, normalizePropValue(v))
			continue
		}
		// Evaluate expression with full context
		setNodeProperty(node, propName, e.evaluateSetExpressionWithContext(ctx, propValue, fullContext, relContext))
	}
}

// evaluateSetExpressionWithContext evaluates SET clause expressions with context.
func (e *StorageExecutor) evaluateSetExpressionWithContext(ctx context.Context, expr string, nodes map[string]*storage.Node, rels map[string]*storage.Edge) interface{} {
	return e.evaluateExpressionWithContext(ctx, expr, nodes, rels)
}

// parseReturnClauseWithContext parses RETURN with context from MATCH.
func (e *StorageExecutor) parseReturnClauseWithContext(ctx context.Context, returnClause string, nodes map[string]*storage.Node, rels map[string]*storage.Edge) ([]string, []interface{}) {
	// Handle RETURN *
	if strings.TrimSpace(returnClause) == "*" {
		var columns []string
		var values []interface{}
		for name, node := range nodes {
			columns = append(columns, name)
			values = append(values, node)
		}
		return columns, values
	}

	var columns []string
	var values []interface{}

	parts := e.splitReturnExpressions(returnClause)

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		var expr, alias string
		asIdx := strings.LastIndex(strings.ToUpper(part), " AS ")
		if asIdx > 0 {
			expr = strings.TrimSpace(part[:asIdx])
			alias = strings.TrimSpace(part[asIdx+4:])
		} else {
			expr = part
			alias = e.expressionToAlias(expr)
		}

		value := e.evaluateExpressionWithContext(ctx, expr, nodes, rels)
		columns = append(columns, alias)
		values = append(values, value)
	}

	return columns, values
}

// parseReturnClause parses RETURN expressions and evaluates them against a node.
// Supports: n.prop, n.prop AS alias, id(n), *, literal values
func (e *StorageExecutor) parseReturnClause(ctx context.Context, returnClause string, varName string, node *storage.Node) ([]string, []interface{}) {
	// Handle RETURN *
	if strings.TrimSpace(returnClause) == "*" {
		return []string{varName}, []interface{}{node}
	}

	var columns []string
	var values []interface{}

	// Split by comma, but be careful with nested expressions
	parts := e.splitReturnExpressions(returnClause)

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		// Check for AS alias
		var expr, alias string
		asIdx := strings.LastIndex(strings.ToUpper(part), " AS ")
		if asIdx > 0 {
			expr = strings.TrimSpace(part[:asIdx])
			alias = strings.TrimSpace(part[asIdx+4:])
		} else {
			expr = part
			// Generate alias from expression
			alias = e.expressionToAlias(expr)
		}

		// Evaluate expression
		value := e.evaluateExpression(ctx, expr, varName, node)
		columns = append(columns, alias)
		values = append(values, value)
	}

	return columns, values
}

// splitReturnExpressions splits RETURN clause by commas, respecting parentheses.
func (e *StorageExecutor) splitReturnExpressions(clause string) []string {
	var result []string
	var current strings.Builder
	depth := 0

	for _, ch := range clause {
		switch ch {
		case '(':
			depth++
			current.WriteRune(ch)
		case ')':
			depth--
			current.WriteRune(ch)
		case ',':
			if depth == 0 {
				result = append(result, current.String())
				current.Reset()
			} else {
				current.WriteRune(ch)
			}
		default:
			current.WriteRune(ch)
		}
	}

	if current.Len() > 0 {
		result = append(result, current.String())
	}

	return result
}

// expressionToAlias converts an expression to a column alias.
func (e *StorageExecutor) expressionToAlias(expr string) string {
	expr = strings.TrimSpace(expr)

	// Function call: id(n) -> id(n)
	if strings.Contains(expr, "(") {
		return expr
	}

	// Property access: n.prop -> prop
	if dotIdx := strings.LastIndex(expr, "."); dotIdx > 0 {
		return expr[dotIdx+1:]
	}

	return expr
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
	originalFabricBindings := e.fabricRecordBindings
	defer func() {
		e.fabricRecordBindings = originalFabricBindings
	}()

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
		return nil, fmt.Errorf("invalid MERGE...WITH chain: no segments found")
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

		upperSeg := strings.ToUpper(segment)

		if i == 0 {
			// First segment may contain multiple setup MERGEs before the first WITH.
			initialClauses := e.splitMultipleMerges(segment)
			for _, initialClause := range initialClauses {
				initialClause = strings.TrimSpace(initialClause)
				if initialClause == "" {
					continue
				}
				upperInitial := strings.ToUpper(initialClause)
				if strings.HasPrefix(upperInitial, "MERGE") {
					mergeContent := strings.TrimSpace(initialClause[5:])
					if strings.Contains(mergeContent, "-[") || strings.Contains(mergeContent, "]-") {
						if err := e.executeMergeRelSegment(ctx, mergeContent, nodeContext); err != nil {
							return nil, fmt.Errorf("initial MERGE failed: %w", err)
						}
						result.Stats.RelationshipsCreated++
						continue
					}
					mergedNode, varName, err := e.executeMergeNodeSegment(ctx, initialClause)
					if err != nil {
						return nil, fmt.Errorf("initial MERGE failed: %w", err)
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
				return nil, fmt.Errorf("FOREACH failed: %w", err)
			}
		} else if strings.HasPrefix(upperSeg, "RETURN") {
			// RETURN segment: build final result
			if chainBroken {
				// Chain broken - return 0 rows
				returnClause := strings.TrimSpace(segment[6:])
				items := e.parseReturnItems(returnClause)
				for _, item := range items {
					if item.alias != "" {
						result.Columns = append(result.Columns, item.alias)
					} else {
						result.Columns = append(result.Columns, item.expr)
					}
				}
				// No rows - chain was broken
				return result, nil
			}

			// Build result from context
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
			segmentNodeCtx = newNodeCtx
			segmentRelCtx = newRelCtx
			segmentScalarCtx = newScalarCtx
			e.fabricRecordBindings = segmentScalarCtx

			clauses := splitMergeChainClauseBlock(remaining)
			for _, clause := range clauses {
				if strings.TrimSpace(clause) == "" {
					continue
				}
				upperClause := strings.ToUpper(strings.TrimSpace(clause))

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
							err := e.executeMergeRelSegment(ctx, mergePart, segmentNodeCtx)
							if err == nil {
								result.Stats.RelationshipsCreated++
							}
						}
					}
				case strings.HasPrefix(upperClause, "MERGE"):
					mergePart := strings.TrimSpace(clause[5:])
					if strings.Contains(mergePart, "-[") || strings.Contains(mergePart, "]-") {
						if err := e.executeMergeRelSegment(ctx, mergePart, segmentNodeCtx); err != nil {
							return nil, err
						}
						result.Stats.RelationshipsCreated++
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
		upperTrim := strings.ToUpper(trimmed)
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

	keywords := []string{"OPTIONAL MATCH", "MATCH", "MERGE", "FOREACH", "RETURN"}
	nextClausePos := -1
	for _, kw := range keywords {
		if idx := findKeywordIndex(segment, kw); idx >= 0 {
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

	// WITH * keeps everything.
	if strings.TrimSpace(withPart) == "*" {
		return remaining, nodeCtx, relCtx, scalarCtx
	}

	items := e.parseReturnItems(withPart)
	if len(items) == 0 {
		// If we can't parse, avoid dropping context.
		return remaining, nodeCtx, relCtx, scalarCtx
	}

	newNodeCtx = make(map[string]*storage.Node)
	newRelCtx = make(map[string]*storage.Edge)
	newScalarCtx = make(map[string]interface{})
	for _, item := range items {
		alias := strings.TrimSpace(item.alias)
		expr := strings.TrimSpace(item.expr)
		if alias == "" {
			alias = expr
		}
		if alias == "" {
			continue
		}
		if n, ok := nodeCtx[expr]; ok {
			newNodeCtx[alias] = n
			continue
		}
		if r, ok := relCtx[expr]; ok {
			newRelCtx[alias] = r
			continue
		}
		if scalarCtx != nil {
			if val, ok := scalarCtx[expr]; ok {
				newScalarCtx[alias] = val
				continue
			}
		}
		if item.alias != "" {
			if value := e.evaluateExpressionWithContext(ctx, expr, nodeCtx, relCtx); value != nil {
				if literal, ok := value.(string); ok && literal == expr {
					continue
				}
				newScalarCtx[alias] = value
				continue
			}
		}
	}

	return remaining, newNodeCtx, newRelCtx, newScalarCtx
}

func splitMergeChainClauseBlock(block string) []string {
	block = strings.TrimSpace(block)
	if block == "" {
		return nil
	}

	keywords := []string{"OPTIONAL MATCH", "MATCH", "FOREACH", "RETURN"}

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
			before := strings.ToUpper(cypher[actualPos-6 : actualPos])
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

// executeMergeNodeSegment executes the initial MERGE (node) part and returns the node and variable name.
func (e *StorageExecutor) executeMergeNodeSegment(ctx context.Context, segment string) (*storage.Node, string, error) {
	store := e.getStorage(ctx)
	// Parse: MERGE (varName:Label {props}) [ON CREATE SET ...] [ON MATCH SET ...]
	mergeIdx := findKeywordIndex(segment, "MERGE")
	if mergeIdx == -1 {
		return nil, "", fmt.Errorf("MERGE not found in segment")
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
	varName, labels, props, err := e.parseMergePattern(ctx, pattern)
	if err != nil {
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
			beforeProps := cloneNodePropertiesMap(node.Properties)
			e.applySetToNode(ctx, node, varName, setClause)
			if !reflect.DeepEqual(beforeProps, node.Properties) {
				store.UpdateNode(node)
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
					return nil, "", fmt.Errorf("failed to create node: %w", err)
				}
			} else {
				return nil, "", fmt.Errorf("failed to create node: %w", err)
			}
		}
		if existingNode == nil {
			node.ID = actualID
			e.notifyNodeMutated(string(node.ID))
			e.cacheMergeNode(labels, props, node)

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
				beforeProps := cloneNodePropertiesMap(node.Properties)
				e.applySetToNode(ctx, node, varName, setClause)
				if !reflect.DeepEqual(beforeProps, node.Properties) {
					store.UpdateNode(node)
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
		beforeProps := cloneNodePropertiesMap(node.Properties)
		e.applySetToNode(ctx, node, varName, setClause)
		if !reflect.DeepEqual(beforeProps, node.Properties) {
			store.UpdateNode(node)
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
		prefix := strings.ToUpper(strings.TrimSpace(segment[:idx]))
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
		return nil, "", fmt.Errorf("MATCH not found in segment")
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
		return nil, "", fmt.Errorf("could not parse node pattern: %s", pattern)
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
func (e *StorageExecutor) executeMergeRelSegment(ctx context.Context, pattern string, nodeContext map[string]*storage.Node) error {
	store := e.getStorage(ctx)
	// Parse relationship pattern: (startVar)-[:TYPE]->(endVar) or (startVar)-[:TYPE {props}]->(endVar)
	pattern = strings.TrimSpace(pattern)

	// Extract start node variable
	startParen := strings.Index(pattern, "(")
	if startParen == -1 {
		return fmt.Errorf("invalid relationship pattern: missing start node in %q", pattern)
	}

	endStartParen := strings.Index(pattern[startParen+1:], ")")
	if endStartParen == -1 {
		return fmt.Errorf("invalid relationship pattern: missing start node closing paren in %q", pattern)
	}
	startVar := strings.TrimSpace(pattern[startParen+1 : startParen+1+endStartParen])

	// Find the relationship part -[...]->
	relStart := strings.Index(pattern, "-[")
	relEnd := strings.Index(pattern, "]->")
	if relEnd == -1 {
		relEnd = strings.Index(pattern, "]-")
	}
	if relStart == -1 || relEnd == -1 {
		return fmt.Errorf("invalid relationship pattern: missing relationship brackets (expected -[type]-> or -[type]-) in %q", pattern)
	}

	relContent := pattern[relStart+2 : relEnd]

	// Parse relationship type and properties
	var relType string
	relProps := make(map[string]interface{})

	if colonIdx := strings.Index(relContent, ":"); colonIdx >= 0 {
		afterColon := relContent[colonIdx+1:]
		if braceIdx := strings.Index(afterColon, "{"); braceIdx > 0 {
			relType = strings.TrimSpace(afterColon[:braceIdx])
			// Parse properties (simplified)
		} else {
			relType = strings.TrimSpace(afterColon)
		}
	}

	// Extract end node variable
	// Find the last (var) pattern
	lastParenStart := strings.LastIndex(pattern, "(")
	lastParenEnd := strings.LastIndex(pattern, ")")
	if lastParenStart == -1 || lastParenEnd == -1 || lastParenEnd < lastParenStart {
		return fmt.Errorf("invalid relationship pattern: missing end node in %q", pattern)
	}
	endVar := strings.TrimSpace(pattern[lastParenStart+1 : lastParenEnd])

	// Look up nodes in context
	startNode, startExists := nodeContext[startVar]
	endNode, endExists := nodeContext[endVar]

	if !startExists {
		return fmt.Errorf("start node variable '%s' not in context (available: %v)", startVar, getKeys(nodeContext))
	}
	if !endExists {
		return fmt.Errorf("end node variable '%s' not in context (available: %v)", endVar, getKeys(nodeContext))
	}

	// Check if relationship already exists
	edges, _ := store.GetOutgoingEdges(startNode.ID)
	for _, edge := range edges {
		if edge.Type == relType && edge.EndNode == endNode.ID {
			// Relationship already exists
			return nil
		}
	}

	// Create the relationship
	edge := &storage.Edge{
		ID:         storage.EdgeID(e.generateID()),
		Type:       relType,
		StartNode:  startNode.ID,
		EndNode:    endNode.ID,
		Properties: relProps,
	}

	return store.CreateEdge(edge)
}

// executeMultipleMerges handles queries with multiple MERGE statements without WITH:
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
		upperSeg := strings.ToUpper(segment)

		if strings.HasPrefix(upperSeg, "MERGE") {
			if chainBroken {
				continue
			}
			mergeContent := strings.TrimSpace(segment[5:])

			// Check if this is a relationship MERGE
			if strings.Contains(mergeContent, "-[") || strings.Contains(mergeContent, "]-") {
				mergeResult, err := e.executeMergeWithContext(ctx, segment, nodeContext, relContext)
				if err != nil {
					return nil, fmt.Errorf("relationship MERGE failed: %w", err)
				}
				if mergeResult != nil && mergeResult.Stats != nil {
					result.Stats.RelationshipsCreated += mergeResult.Stats.RelationshipsCreated
					result.Stats.PropertiesSet += mergeResult.Stats.PropertiesSet
				}
			} else {
				// Node MERGE
				node, varName, err := e.executeMergeNodeSegment(ctx, segment)
				if err != nil {
					return nil, fmt.Errorf("node MERGE failed: %w", err)
				}
				if node != nil && varName != "" {
					nodeContext[varName] = node
				}
			}
		} else if strings.HasPrefix(upperSeg, "OPTIONAL MATCH") {
			if chainBroken {
				continue
			}
			node, varName, err := e.executeMatchSegment(ctx, segment, nodeContext)
			if err != nil {
				return nil, fmt.Errorf("OPTIONAL MATCH failed: %w", err)
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
				return nil, fmt.Errorf("MATCH failed: %w", err)
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
				return nil, fmt.Errorf("FOREACH failed: %w", err)
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

// splitMultipleMerges splits a query into MERGE/MATCH/RETURN segments.
func (e *StorageExecutor) splitMultipleMerges(cypher string) []string {
	var segments []string
	boundaries := collectTopLevelMergeClauseBoundaries(cypher, []string{
		"OPTIONAL MATCH",
		"FOREACH",
		"MERGE",
		"MATCH",
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

	upper := strings.ToUpper(cypher)
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

func isOnMatchModifier(cypher string, matchPos int) bool {
	prefix := strings.TrimSpace(cypher[:matchPos])
	return strings.HasSuffix(strings.ToUpper(prefix), "ON")
}

func isOptionalMatchModifier(cypher string, matchPos int) bool {
	prefix := strings.TrimSpace(cypher[:matchPos])
	return strings.HasSuffix(strings.ToUpper(prefix), "OPTIONAL")
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
