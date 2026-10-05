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
