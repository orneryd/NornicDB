// MERGE clause implementation for NornicDB.
// This file contains MERGE execution, compound queries, and context-aware operations.

package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/orneryd/nornicdb/internal/queryprofile"

	"strings"

	valueconvert "github.com/orneryd/nornicdb/pkg/convert"
	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// patternPropertyRead is one candidate ID set the inline pattern-property seek
// can read: label's arity-1 index on property under value, the property's
// indexes on every one of labels (anyLabel, for a pattern without labels),
// or the composite index read of the pattern's values (props) for the
// composite's first covered properties.
type patternPropertyRead struct {
	label     string
	labels    []string
	property  string
	value     interface{}
	anyLabel  bool
	composite *storage.CompositeIndex
	props     map[string]interface{}
	covered   int
	full      bool

	// authoritative: a read that finds nothing proves no node holds the value
	// (#821); otherwise the read narrows nothing.
	authoritative bool
}

// count is the length of the read's posting in the index, or
// unknownIndexSeekEstimate when the index cannot count it.
func (r *patternPropertyRead) count(ctx context.Context, store storage.Engine, schema *storage.SchemaManager) (int64, error) {
	var count int
	var indexed bool
	var err error
	switch {
	case r.composite != nil:
		var buf [4]interface{}
		count, indexed, err = compositeIndexValueCount(ctx, store, schema, r.composite, r.compositeValues(buf[:0]), r.full)
	case r.anyLabel:
		total := int64(0)
		for _, label := range r.labels {
			count, indexed, err = propertyIndexValueCount(ctx, store, schema, label, r.property, r.value)
			if err != nil || !indexed {
				return unknownIndexSeekEstimate, err
			}
			total += int64(count)
		}
		return total, nil
	default:
		count, indexed, err = propertyIndexValueCount(ctx, store, schema, r.label, r.property, r.value)
	}
	if err != nil || !indexed {
		return unknownIndexSeekEstimate, err
	}
	return int64(count), nil
}

// compositeValues appends the composite read's values, the pattern's values
// for the index's covered leading properties, to out. They are built only
// when the read is counted or made.
func (r *patternPropertyRead) compositeValues(out []interface{}) []interface{} {
	for _, property := range r.composite.Properties[:r.covered] {
		out = append(out, r.props[property])
	}
	return out
}

// hashed reports whether the read is a composite index read: counting or
// reading it hashes its key (storage.NewCompositeKey), which costs more than
// a single-property count.
func (r *patternPropertyRead) hashed() bool {
	return r.composite != nil
}

// worthCounting reports whether a hashed read can still save more than it
// costs against incumbent, the fewest nodes another read or seek lists: once
// that is at most one node, the read could save at most that one node, which
// the caller's property check handles anyway, so the read is neither counted
// nor made. Single-property reads are always counted.
func (r *patternPropertyRead) worthCounting(incumbent int64) bool {
	return !r.hashed() || incumbent > 1
}

// patternPropertyReads collects the inline seek's reads: the first two in
// place, and a list only once there is a third.
type patternPropertyReads struct {
	n     int
	small [2]patternPropertyRead
	all   []patternPropertyRead
}

func (r *patternPropertyReads) add(read patternPropertyRead) {
	switch {
	case r.n < len(r.small):
		r.small[r.n] = read
	case r.n == len(r.small):
		r.all = append(r.all, r.small[0], r.small[1], read)
	default:
		r.all = append(r.all, read)
	}
	r.n++
}

// patternPropertyIntersectFactor bounds the inline seek's intersection: once
// it holds candidates, a further index read is made only when its posting is
// at most this many times longer than the candidates, since reading a long
// posting to drop a few candidates costs more than the residual property
// filter that drops them anyway.
const patternPropertyIntersectFactor = 16

// planPatternPropertySeek plans the pattern's inline seek: the reads the
// pattern's indexed inline properties are served by. planned is false when no
// index serves one.
func (e *StorageExecutor) planPatternPropertySeek(ctx context.Context, nodeInfo nodePatternInfo, store storage.Engine) (nodeIndexSeek, bool, error) {
	if err := ctx.Err(); err != nil {
		return nodeIndexSeek{}, false, err
	}
	if len(nodeInfo.properties) == 0 {
		return nodeIndexSeek{}, false, nil
	}
	schema := store.GetSchema()
	if schema == nil {
		return nodeIndexSeek{}, false, nil
	}

	// One read per inline equality that an index can serve; each narrows the
	// result by intersection. The first read is kept in place, so the usual
	// one-property lookup allocates no list (patternPropertyReads).
	var reads patternPropertyReads

	// Composite indexes answer a whole equality prefix in one lookup, which
	// is far narrower than intersecting one set per property. Use the longest
	// covered leading prefix: all index properties → LookupFull, otherwise
	// the leading k properties → LookupPrefix. The residual nodeMatchesProps
	// step still enforces every non-indexed predicate.
	for _, label := range nodeInfo.labels {
		if !schema.HasMultiPropertyIndexForLabel(label) {
			continue // no composite index of two or more properties on the label
		}
		indexes, err := schema.SeekableCompositeIndexesForLabel(ctx, label)
		if err != nil {
			return nodeIndexSeek{}, false, err
		}
		for _, idx := range indexes {
			if idx == nil || len(idx.Properties) < 2 {
				// Arity-1 indexes are served by the per-property loop below,
				// whose authoritative-miss logic handles values an index can't
				// key (lists, maps) without a false negative.
				continue
			}
			covered := 0
			for covered < len(idx.Properties) {
				if _, ok := nodeInfo.properties[idx.Properties[covered]]; !ok {
					break
				}
				covered++
			}
			if covered == 0 {
				continue
			}
			// Every covered value must be a scalar the index files under an
			// equality-exact key; otherwise a miss is not authoritative and
			// the caller's scan must decide.
			authoritative := true
			for i := 0; i < covered; i++ {
				if !propertyIndexMissIsAuthoritative(nodeInfo.properties[idx.Properties[i]]) {
					authoritative = false
					break
				}
			}
			if !authoritative {
				continue
			}
			reads.add(patternPropertyRead{composite: idx, props: nodeInfo.properties, covered: covered, full: covered == len(idx.Properties), authoritative: true})
		}
	}

	for prop, val := range nodeInfo.properties {
		authoritative := propertyIndexMissIsAuthoritative(val)
		if len(nodeInfo.labels) > 0 {
			// Labelled: probe a (label, prop) index on any pattern label.
			label, indexed, err := e.patternPropertyIndexLabel(ctx, store, schema, nodeInfo.labels, prop, val)
			if err != nil {
				return nodeIndexSeek{}, false, err
			}
			if indexed {
				reads.add(patternPropertyRead{label: label, property: prop, value: storage.IndexLookupValue(val), authoritative: authoritative})
			}
			continue
		}
		labels, labelErr := e.indexCandidateLabels(ctx, schema, nil, prop)
		if labelErr != nil {
			return nodeIndexSeek{}, false, labelErr
		}
		if labellessPropertyIndexUsable(ctx, store, labels...) {
			reads.add(patternPropertyRead{labels: labels, property: prop, value: val, anyLabel: true, authoritative: authoritative})
		}
	}
	if reads.n == 0 {
		return nodeIndexSeek{}, false, nil
	}
	seek := nodeIndexSeek{schema: schema, store: store, patternLabels: nodeInfo.labels}
	if reads.n <= len(reads.small) {
		seek.inlineSmall, seek.inlineN = reads.small, reads.n
	} else {
		seek.inline = reads.all
	}
	return seek, true, nil
}

// patternPropertyIndexLabel returns the pattern label whose index on property
// the inline seek reads for value: the only indexed one, or, when several
// pattern labels index property, the one listing the fewest nodes under value
// (smallestLabelPosting; the written order breaks ties and decides when no
// count is known).
func (e *StorageExecutor) patternPropertyIndexLabel(ctx context.Context, store storage.Engine, schema *storage.SchemaManager, labels []string, property string, value interface{}) (string, bool, error) {
	if len(labels) == 1 {
		indexed, err := schema.HasPropertyIndex(ctx, labels[0], property)
		return labels[0], indexed && err == nil, err
	}
	var indexed []string
	for _, label := range labels {
		exists, err := schema.HasPropertyIndex(ctx, label, property)
		if err != nil {
			return "", false, err
		}
		if exists {
			indexed = append(indexed, label)
		}
	}
	if len(indexed) == 0 {
		return "", false, nil
	}
	postings := indexSeekPostings{labels: indexed, property: property, value: value}
	chosen, err := smallestLabelPosting(ctx, store, schema, labels, &postings)
	if err != nil {
		return "", false, err
	}
	return chosen[0], true, nil
}

// resolvePatternPropertySeek lists the inline seek's candidate IDs: the
// reads in order of posting size, intersected, each further read made only
// while it is short enough to narrow the candidates
// (patternPropertyIntersectFactor), and a composite read only while it is worth
// its key hash (worthCounting). The single-property reads are counted first. No
// node body is read.
//
// A posting of several nodes is kept as a set, as the release read it, and so
// in an order that differs from one read to the next. Storage keeps a bounded
// number of nodes cached; a read of more nodes than that, repeated in the same
// order, never finds one still cached and decodes every node again (measured:
// 39% more allocations on a repeated 12,000-node read inside a transaction).
// One listed node needs no set.
func (e *StorageExecutor) resolvePatternPropertySeek(ctx context.Context, seek *nodeIndexSeek) (nodeIDResults, bool, error) {
	store, schema, reads := seek.store, seek.schema, seek.inlineReads()
	var countBuf [4]int64
	var orderBuf [4]int
	counts, order := countBuf[:0], orderBuf[:0]
	if len(reads) > len(countBuf) {
		counts, order = make([]int64, 0, len(reads)), make([]int, 0, len(reads))
	}
	counts, order = counts[:len(reads)], order[:len(reads)]
	shortest := unknownIndexSeekEstimate
	for pass := 0; pass < 2; pass++ {
		for i := range reads {
			if reads[i].hashed() != (pass == 1) {
				continue
			}
			order[i] = i
			counts[i] = unknownIndexSeekEstimate
			if len(reads) < 2 || (pass == 1 && !reads[i].worthCounting(shortest)) {
				continue
			}
			count, err := reads[i].count(ctx, store, schema)
			if err != nil {
				return nodeIDResults{}, false, err
			}
			counts[i] = count
			shortest = min(shortest, count)
		}
	}
	// Shortest posting first, ties in plan order (an insertion sort: a few
	// reads, and no closure for the buffers to escape through).
	for i := 1; i < len(order); i++ {
		for j := i; j > 0 && counts[order[j]] < counts[order[j-1]]; j-- {
			order[j], order[j-1] = order[j-1], order[j]
		}
	}

	// The candidates are the first read's IDs until a second read narrows
	// them (single); only then, or for a read across labels that can list a
	// node twice, are they kept as a set (candidates).
	var single []storage.NodeID
	var candidates map[storage.NodeID]struct{}
	made := false
	for _, at := range order {
		read := &reads[at]
		size := int64(len(single) + len(candidates))
		if made && counts[at] != unknownIndexSeekEstimate && counts[at] > size*patternPropertyIntersectFactor {
			continue
		}
		if made && read.hashed() && !read.worthCounting(size) {
			continue
		}
		var ids []storage.NodeID
		var err error
		switch {
		case read.composite != nil:
			var buf [4]interface{}
			ids, err = compositeIndexLookup(ctx, store, schema, read.composite, read.compositeValues(buf[:0]), read.full)
		case read.anyLabel:
			ids, err = schema.PropertyIndexLookupAnyLabel(ctx, read.property, read.value)
			// The any-label read is one lookup over the pattern's labels; the
			// label and composite reads below are counted by
			// propertyIndexLookup and compositeIndexLookup themselves, so no read
			// is counted twice.
			if err == nil && profileRuntimeFromContext(ctx) != nil {
				queryprofile.Access(ctx, profileKindNodeIndexSeek, strings.Join(read.labels, "|")+"("+read.property+")").Add(int64(len(ids)), 1+int64(len(ids)))
			}
		default:
			ids, err = propertyIndexLookup(ctx, store, schema, read.label, read.property, read.value)
		}
		if err != nil {
			return nodeIDResults{}, false, err
		}
		// A probe that finds nothing for a string, boolean or number means no
		// node holds the value (#821); for other values the scan decides, and
		// the residual `nodeMatchesProps` step still enforces the property.
		if ids == nil && !read.authoritative {
			continue
		}
		if !made && !read.anyLabel {
			single, made = ids, true
			continue
		}
		if candidates == nil && made {
			candidates = make(map[storage.NodeID]struct{}, len(single))
			for _, id := range single {
				candidates[id] = struct{}{}
			}
			single = nil
		}
		next := make(map[storage.NodeID]struct{}, len(ids))
		for _, id := range ids {
			if err := ctx.Err(); err != nil {
				return nodeIDResults{}, false, err
			}
			if made {
				if _, ok := candidates[id]; !ok {
					continue
				}
			}
			next[id] = struct{}{}
		}
		candidates, made = next, true
	}
	if !made {
		return nodeIDResults{}, false, nil
	}
	if len(single) > 1 {
		candidates = make(map[storage.NodeID]struct{}, len(single))
		for _, id := range single {
			candidates[id] = struct{}{}
		}
		single = nil
	}
	// An inline seek the index answered seeded this lookup.
	e.markMergeSchemaLookupUsed()
	if candidates == nil {
		// The seek's one read already listed the candidates in a slice this
		// call owns — the index lookup built it — so the result keeps that
		// listing instead of copying it into a second one.
		return nodeIDResults{overflow: single}, true, nil
	}
	ids := newIDResults(len(candidates))
	for id := range candidates {
		ids.add(id)
	}
	return ids, true, nil
}

// propertyIndexMissIsAuthoritative reports whether a property index lookup
// that finds no node for value proves that no node holds an equal value:
// strings, booleans and numbers are filed under keys that are equal exactly
// when the values are equal in Cypher (1 and 1.0 share a key).
func propertyIndexMissIsAuthoritative(value interface{}) bool {
	// Null is not an authoritative property-index miss: it still needs the
	// existing scan path. Primitive type recognition is shared with APOC search.
	return value != nil && valueconvert.NativeScalar(value)
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

// extractVariableNamesFromPattern returns the node variables pattern binds,
// each once in order of first appearance: e.g.,
// "(p:Person)<-[:REL]-(poc:POC)-[:BELONGS_TO]->(a:Area)" returns ["p", "poc", "a"].
// The shared node-binding reader reads them from the pattern's structure, so a
// dynamic label's $any(...), a property map, an element WHERE, a call such as
// shortestPath(...) and a parenthesized or quantified path are not nodes.
func (e *StorageExecutor) extractVariableNamesFromPattern(ctx context.Context, pattern string) []string {
	return canonicalPatternVariables(ctx, pattern)
}

// mergeNodeAbsentKey marks a context whose caller has just scanned the
// MERGE node pattern's whole label and found no matching node
// (pipelineApplyMerge, findMergeNodesScanned): the MERGE creates the node
// without scanning the label a second time (#823).
type mergeNodeAbsentKey struct{}

func withMergeNodeAbsent(ctx context.Context) context.Context {
	return context.WithValue(ctx, mergeNodeAbsentKey{}, true)
}

// executeMergeWithContext runs one MERGE pattern, the text "MERGE pattern"
// without ON CREATE / ON MATCH actions (pipelineApplyMerge splits them off and
// applies them), with the row's bound nodes and relationships: it binds what
// it matched or created into nodeContext / relContext. Its stats report what
// it created, which tells the caller whether ON CREATE or ON MATCH applies.
func (e *StorageExecutor) executeMergeWithContext(ctx context.Context, cypher string, nodeContext map[string]*storage.Node, relContext map[string]*storage.Edge) (*ExecuteResult, error) {
	// The caller's finding applies to this MERGE's node pattern only, not to
	// the clauses that follow it.
	nodeKnownAbsent, _ := ctx.Value(mergeNodeAbsentKey{}).(bool)
	if nodeKnownAbsent {
		ctx = context.WithValue(ctx, mergeNodeAbsentKey{}, false)
	}
	result := &ExecuteResult{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Stats:   &QueryStats{},
	}
	store := e.getStorage(ctx)

	mergePattern := strings.TrimSpace(cypher)
	if startsWithKeywordFold(mergePattern, "MERGE") {
		mergePattern = pipelineClauseBody(mergePattern, "MERGE")
	}

	// A relationship pattern: (a)-[r:TYPE]->(b)
	if containsRelationshipPattern(mergePattern) {
		parsedPattern, err := e.parseMergeRelationshipPattern(ctx, mergePattern, nodeContext, relContext)
		if err != nil {
			return nil, err
		}
		if err := validateMergePatternProperties(parsedPattern.startNodePattern.properties, "node"); err != nil {
			return nil, err
		}
		if err := validateMergePatternProperties(parsedPattern.endNodePattern.properties, "node"); err != nil {
			return nil, err
		}
		if err := validateMergePatternProperties(parsedPattern.properties, "relationship"); err != nil {
			return nil, err
		}

		// Both endpoints are bound by earlier clauses: a pattern with an endpoint
		// the row doesn't bind is a whole-path MERGE (pipelineMergePath), which
		// matches every existing path or creates the whole pattern (#509, #640).
		startNode := nodeContext[parsedPattern.startVariable]
		endNode := nodeContext[parsedPattern.endVariable]
		if startNode == nil {
			return nil, localizedError(localization.CypherMergeStartVariableNotBound(parsedPattern.startVariable, getKeys(nodeContext)), nil)
		}
		if endNode == nil {
			return nil, localizedError(localization.CypherMergeEndVariableNotBound(parsedPattern.endVariable, getKeys(nodeContext)), nil)
		}
		var existingEdge *storage.Edge
		mergeStartNode, mergeEndNode := startNode, endNode
		if parsedPattern.direction == mergeRelationshipIncoming {
			mergeStartNode, mergeEndNode = endNode, startNode
		}
		// Cypher relationship properties inside the MERGE pattern are identity
		// fields. Scan the bounded endpoint pair so same-type relationships with
		// different property identities remain distinct.
		if existingEdge == nil {
			existingEdge, err = findRelationshipForMerge(ctx, store, mergeStartNode.ID, mergeEndNode.ID, parsedPattern.relType, parsedPattern.properties)
			if err != nil {
				return nil, localizedError(localization.CypherMergeFindRelationshipFailed(err), err)
			}
			if existingEdge == nil && parsedPattern.direction == mergeRelationshipUndirected {
				existingEdge, err = findRelationshipForMerge(ctx, store, mergeEndNode.ID, mergeStartNode.ID, parsedPattern.relType, parsedPattern.properties)
				if err != nil {
					return nil, localizedError(localization.CypherMergeFindRelationshipFailed(err), err)
				}
			}
		}

		var edge *storage.Edge
		if existingEdge != nil {
			edge = existingEdge
		} else {
			// Create new relationship
			edge = &storage.Edge{
				Database:   storage.DatabaseOf(store),
				ID:         e.newRelationshipMergeEdgeID(mergeStartNode.ID, mergeEndNode.ID, parsedPattern.relType, parsedPattern.properties),
				Type:       parsedPattern.relType,
				StartNode:  mergeStartNode.ID,
				EndNode:    mergeEndNode.ID,
				Properties: parsedPattern.properties,
			}
			if err := edge.SetEndpoints(mergeStartNode, mergeEndNode); err != nil {
				return nil, err
			}
			createdEdge, created, createErr := createRelationshipForMerge(ctx, e, store, edge, parsedPattern.properties)
			if createErr != nil {
				return nil, localizedError(localization.CypherMergeCreateRelationshipFailed(createErr), createErr)
			}
			edge = createdEdge
			if !created {
				if reporter, ok := store.(interface {
					EdgeCreateWasNoOp(storage.EdgeID) bool
				}); ok && reporter.EdgeCreateWasNoOp(edge.ID) {
					result.Stats.relationshipMergeNoOp = true
				}
			}
			if created {
				result.Stats.RelationshipsCreated = 1
				countCreatedEntity(result.Stats, nil, edge.Properties)
				e.notifyEdgeMutated(ctx, string(edge.ID))
				e.notifyNodeMutated(ctx, string(edge.StartNode))
				if edge.EndNode != edge.StartNode {
					e.notifyNodeMutated(ctx, string(edge.EndNode))
				}
			}
		}

		// Store in context
		if parsedPattern.relVariable != "" {
			relContext[parsedPattern.relVariable] = edge
		}
		return result, nil

	}

	// Parse node pattern
	varName, labels, matchProps, err := e.parseMergeNodePattern(ctx, mergePattern, nodeContext, relContext)
	if err != nil {
		return nil, &classifiedCypherError{
			cause:  err,
			code:   "Neo.ClientError.Statement.SyntaxError",
			detail: "UnexpectedSyntax",
		}
	}
	if err := validateMergePatternProperties(matchProps, "node"); err != nil {
		return nil, err
	}

	// Try to find existing node
	var existingNode *storage.Node
	if candidate := nodeContext[varName]; candidate != nil && mergeNodeMatches(candidate, labels, matchProps) {
		existingNode = candidate
	}
	if existingNode == nil && !nodeKnownAbsent {
		if err := prepareMergeKeys(ctx, store, labels, matchProps, mergeNodeWritesFromContext(ctx).writes(varName)); err != nil {
			return nil, err
		}
		existingNode, err = e.findMergeNode(ctx, store, labels, matchProps)
		if err != nil {
			return nil, err
		}
	}

	var node *storage.Node
	if existingNode != nil {
		node = existingNode
	} else {
		node = &storage.Node{
			ID:         storage.NodeID(e.generateID()),
			Labels:     labels,
			Properties: matchProps,
		}
		node.SetRuntimeMetadata(storage.NodeRuntimeMetadata{Database: storage.DatabaseOf(store)})
		if err := validatePropertyValues(node.Properties); err != nil {
			return nil, err
		}
		actualID, err := createMintedNode(ctx, store, node)
		if err != nil {
			if !mergeCreateConflict(err) {
				return nil, localizedError(localization.CypherMergeCreateNodeFailed(err), err)
			}
			// A concurrent MERGE created it first: this MERGE matched it.
			recoveredNode, findErr := e.findMergeNode(ctx, store, labels, matchProps)
			if findErr != nil {
				return nil, findErr
			}
			if recoveredNode == nil {
				return nil, localizedError(localization.CypherMergeCreateNodeFailed(err), err)
			}
			node = recoveredNode
		} else {
			node.ID = actualID
			e.notifyNodeMutated(ctx, string(node.ID))
			result.Stats.NodesCreated = 1
			countCreatedEntity(result.Stats, node.Labels, node.Properties)
		}
	}
	e.cacheMergeNode(labels, matchProps, node)

	// Anonymous pattern nodes do not introduce a variable into scope.
	if varName != "" {
		nodeContext[varName] = node
	}
	return result, nil
}

// parseMergeProperties parses a MERGE pattern's property map text
// ("{k: v, …}") with the statement's bound nodes and relationships. A value
// the parser resolves (a literal, a parameter, an evaluated expression) is
// that value: a string literal is a string, whatever it reads. An expression
// the parser leaves as its own text and that reads a bound variable's
// property (a.id) is evaluated against the bound entities. Null values remain
// in the map so the shared MERGE validator rejects them before matching.
func (e *StorageExecutor) parseMergeProperties(ctx context.Context, propsText string, nodeContext map[string]*storage.Node, relContext map[string]*storage.Edge) (map[string]interface{}, error) {
	return e.materializePropertyMap(ctx, propsText, entityRow(ctx, nodeContext, relContext), true, false)
}

// parseMergeNodePattern is parseMergePattern with the property map read by
// parseMergeProperties.
func (e *StorageExecutor) parseMergeNodePattern(ctx context.Context, pattern string, nodeContext map[string]*storage.Node, relContext map[string]*storage.Edge) (string, []string, map[string]interface{}, error) {
	pattern = strings.TrimSpace(pattern)
	if !strings.HasPrefix(pattern, "(") || !strings.HasSuffix(pattern, ")") {
		return "", nil, nil, localizedError(localization.CypherResidualMergePatternInvalid(pattern), nil)
	}
	head, props := splitNodePatternProperties(pattern)
	variable, labels, labelErr := parseNodeHead(head)
	if labelErr != nil {
		return "", nil, nil, labelErr
	}
	properties, err := e.parseMergeProperties(ctx, props, nodeContext, relContext)
	return variable, labels, properties, err
}

// applySetToRelationshipWithContext is the single per-relationship SET
// applicator (pipeline, MERGE, CREATE ... SET). It applies every assignment in
// setClause that targets varName: r = map / entity (replace), r += map / entity
// (merge) and r.p = value. A null value removes the key. setClause may chain
// several SET clauses (a SET b). It returns the number of properties written
// by Neo4j's properties_set rule (setWrites), or an error for a value Cypher
// cannot store (replacing with a non-map, a map or entity as a property value).
func (e *StorageExecutor) applySetToRelationshipWithContext(ctx context.Context, edge *storage.Edge, varName string, setClause string, nodeContext map[string]*storage.Node, relContext map[string]*storage.Edge) (int, error) {
	if edge == nil || varName == "" {
		return 0, nil
	}
	fullRelContext := make(map[string]*storage.Edge)
	for k, v := range relContext {
		fullRelContext[k] = v
	}
	fullRelContext[varName] = edge

	writes := setWrites{known: propertyKeyLookup(e.getStorage(ctx))}
	var run setPropertyRun
	for segment, next, ok := nextChainedSetClause(ctx, setClause, 0); ok; segment, next, ok = nextChainedSetClause(ctx, setClause, next) {
		run.applyToRelationship(edge, &writes)
		writes.endRun()
		for _, assignment := range splitSetAssignments(segment) {
			assignment = strings.TrimSpace(assignment)
			if assignment == "" {
				continue
			}

			target, propName, operator, right := splitSetAssignment(assignment)
			if target != varName || propName == "" {
				run.applyToRelationship(edge, &writes)
				writes.endRun()
			}
			if target != varName {
				continue
			}
			switch {
			case operator == "=" && propName == "":
				props, err := e.setSourceMap(ctx, right, "=", nodeContext, fullRelContext)
				if err != nil {
					return writes.count, err
				}
				// A source without properties (an empty node) clears them.
				writes.mapEntries(edge.Properties, props, true)
				edge.Properties = setPropertyMap(props)
			case operator == "+=":
				props, err := e.setSourceMap(ctx, right, "+=", nodeContext, fullRelContext)
				if err != nil {
					return writes.count, err
				}
				writes.mapEntries(edge.Properties, props, false)
				for k, v := range props {
					setRelationshipProperty(edge, k, v)
				}
			case operator == "=":
				// Direct $param resolution (in setPropertyValue) preserves declared
				// types (e.g. []string, []float64) end-to-end. The run's values
				// are written together (setPropertyRun).
				value, err := e.setPropertyValue(ctx, right, nodeContext, fullRelContext)
				if err != nil {
					return writes.count, err
				}
				run.add(propName, value)
			case operator == "[]=":
				key, err := e.dynamicPropertyKeyOf(ctx, propName, nodeContext, fullRelContext, false)
				if err != nil {
					return writes.count, err
				}
				value, err := e.setPropertyValue(ctx, right, nodeContext, fullRelContext)
				if err != nil {
					return writes.count, err
				}
				run.add(key, value)
			case operator == ":":
				// Labels belong to nodes (validateSetClauseScope rejects a
				// relationship target it can see statically).
				return writes.count, labelTargetTypeError("Relationship")
			}
		}
	}
	run.applyToRelationship(edge, &writes)
	return writes.count, nil
}

// applySetToNodeWithContext is the single per-node SET applicator used by
// every SET route (MATCH / pipeline, MERGE in all its forms, CREATE ... SET).
// It applies every assignment in setClause that targets varName: n = map /
// entity (replace), n += map / entity (merge), n.p = value and n:L1:L2 labels.
// A null value removes the key. setClause may chain several SET clauses
// (a SET b). It returns the number of properties written by Neo4j's
// properties_set rule (setWrites), or an error for a value Cypher cannot
// store (replacing with a non-map, a map or entity as a property value);
// assignments are validated statically by validatePipelineSetAssignments.
func (e *StorageExecutor) applySetToNodeWithContext(ctx context.Context, node *storage.Node, varName string, setClause string, nodeContext map[string]*storage.Node, relContext map[string]*storage.Edge) (int, error) {
	// Add current node to context for self-references
	fullContext := make(map[string]*storage.Node)
	for k, v := range nodeContext {
		fullContext[k] = v
	}
	fullContext[varName] = node

	writes := setWrites{known: propertyKeyLookup(e.getStorage(ctx))}
	var run setPropertyRun
	for segment, next, ok := nextChainedSetClause(ctx, setClause, 0); ok; segment, next, ok = nextChainedSetClause(ctx, setClause, next) {
		writes.endRun()
		if err := e.applyNodeSetClause(ctx, node, varName, segment, fullContext, relContext, &writes, &run); err != nil {
			return writes.count, err
		}
	}
	return writes.count, nil
}

// applyNodeSetClause applies the assignments of one SET clause that target
// varName to node (applySetToNodeWithContext), recording what they write.
func (e *StorageExecutor) applyNodeSetClause(ctx context.Context, node *storage.Node, varName string, setClause string, fullContext map[string]*storage.Node, relContext map[string]*storage.Edge, writes *setWrites, run *setPropertyRun) error {
	defer run.applyToNode(node, writes)
	for _, assignment := range splitSetAssignments(setClause) {
		assignment = strings.TrimSpace(assignment)

		target, propName, operator, right := splitSetAssignment(assignment)
		if target != varName || propName == "" {
			run.applyToNode(node, writes)
			writes.endRun()
		}
		if target != varName {
			continue
		}
		switch {
		case operator == "+=":
			if err := e.applySetMapMergeToNode(ctx, node, varName, right, fullContext, relContext, writes); err != nil {
				return err
			}
		case operator == "=" && propName == "":
			props, err := e.setSourceMap(ctx, right, "=", fullContext, relContext)
			if err != nil {
				return err
			}
			// A source without properties (an empty node) clears them.
			writes.mapEntries(node.Properties, props, true)
			node.Properties = setPropertyMap(props)
		case operator == ":":
			items, err := setLabelChainItems(right)
			if err != nil {
				return err
			}
			labels, err := e.chainLabelNames(ctx, items, fullContext, relContext)
			if err != nil {
				return err
			}
			for _, label := range labels {
				if !containsString(node.Labels, label) {
					node.Labels = append(node.Labels, label)
				}
			}
		case operator == "[]=":
			key, err := e.dynamicPropertyKeyOf(ctx, propName, fullContext, relContext, false)
			if err != nil {
				return err
			}
			value, err := e.setPropertyValue(ctx, right, fullContext, relContext)
			if err != nil {
				return err
			}
			run.add(key, value)
		case operator == "=":
			// Direct $param resolution (in setPropertyValue) preserves declared
			// types end-to-end. The run's values are written together
			// (setPropertyRun).
			value, err := e.setPropertyValue(ctx, right, fullContext, relContext)
			if err != nil {
				return err
			}
			run.add(propName, value)
		}
	}
	return nil
}

// setPropertyValue evaluates the right-hand side of SET x.p = <expr>. A direct
// $param keeps its declared Go type. A map, entity or nested list is rejected
// with a TypeError, as in Neo4j (storablePropertyValue).
func (e *StorageExecutor) setPropertyValue(ctx context.Context, expr string, nodes map[string]*storage.Node, rels map[string]*storage.Edge) (interface{}, error) {
	value, err := e.evaluateSetExpressionWithContext(ctx, expr, nodes, rels)
	if err != nil {
		return nil, err
	}
	value, _, err = storablePropertyValue(value)
	return value, err
}

// setSourceMap evaluates the right-hand side of SET x = <expr> or
// SET x += <expr> (operator): a map, or a node / relationship whose
// properties are copied. Any other value, null included, is a TypeError
// (setPropertyMapValue, #907), as is a map holding a value a property cannot
// store. Both operators evaluate their source through the checked boundary;
// every SET route (pipeline, MERGE actions, FOREACH, the UNWIND
// batch routes) calls this.
func (e *StorageExecutor) setSourceMap(ctx context.Context, expr, operator string, nodes map[string]*storage.Node, rels map[string]*storage.Edge) (map[string]interface{}, error) {
	value, err := e.evaluateSetExpressionWithContext(ctx, expr, nodes, rels)
	if err != nil {
		return nil, err
	}
	return setPropertyMapValue(value, operator)
}

// splitSetAssignment splits one SET assignment into its target variable,
// property (for x.p = v), operator ("=", "+=" or ":" for labels) and
// right-hand side. The operator is the first top-level "=" (preceded by "+"
// for +=), so "=" or "+=" inside a value string, or inside a dynamic key's
// brackets, does not change the form. Targets may be parenthesized:
// SET (n).p = v.
//
// A dynamic property key, x[expr] = v (Neo4j 5.26), has operator "[]=" and
// its key expression in property; the key is a property name only once
// evaluated (dynamicPropertyKeyOf), so readers that need the name treat the
// item as writing any key. x[expr] += v is not Cypher. Unrecognized text
// yields an empty operator.
func splitSetAssignment(assignment string) (target, property, operator, right string) {
	assignment = strings.TrimSpace(assignment)
	eq := findTopLevelOperator(assignment, "=", false, true)
	if colon := findTopLevelOperator(assignment, ":", false, true); colon > 0 && (eq < 0 || colon < eq) {
		// Label form x:L1:L2 / x:$(expr); a dynamic label expression may
		// itself contain "=".
		if target := strings.TrimSpace(assignment[:colon]); cyphertext.IsIdentifier(target) {
			return target, "", ":", strings.TrimSpace(assignment[colon+1:])
		}
	}
	if eq < 0 {
		return "", "", "", ""
	}
	left := assignment[:eq]
	operator = "="
	if eq > 0 && assignment[eq-1] == '+' {
		left = assignment[:eq-1]
		operator = "+="
	}
	right = strings.TrimSpace(assignment[eq+1:])
	if receiver, key, subscript, ok := staticPostfixSplit(strings.TrimSpace(left)); ok && subscript {
		key = strings.TrimSpace(key)
		if operator == "+=" || key == "" {
			return "", "", "", ""
		}
		variable, _, _ := parseSetAssignmentTarget(receiver)
		return variable, key, "[]=", right
	}
	target, property, hasProperty := parseSetAssignmentTarget(left)
	if operator == "+=" && hasProperty {
		return "", "", "", ""
	}
	return target, property, operator, right
}

// requireSetParameter reports a SET value that is a bare $name parameter not
// supplied with the query, as Neo4j does, instead of letting the unresolved
// "$name" text be stored. Other expressions pass through unchanged.
func requireSetParameter(ctx context.Context, expr string) error {
	expr = strings.TrimSpace(expr)
	if !strings.HasPrefix(expr, "$") {
		return nil
	}
	name := expr[1:]
	if !cyphertext.IsIdentifier(name) {
		return nil
	}
	params := getParamsFromContext(ctx)
	if len(params) == 0 {
		return localizedError(localization.CypherMutationsSetAssignmentParametersRequired(name), nil)
	}
	if _, ok := params[name]; !ok {
		return localizedError(localization.CypherMutationsSetAssignmentParameterNotFound(name), nil)
	}
	return nil
}

// setPropertyMapValue converts a SET x = / x += source value into the property
// map to write: maps as-is, nodes and relationships by their properties
// (propertyMapForSetValue). Anything else is a TypeError, null included (Neo4j
// 5.26: "Expected Null() to be a map", #907), as is a map holding a value a
// property cannot store. The map's values are the values a property stores
// (storablePropertyValue), so a map entry is checked as SET x.p = v checks v;
// the source map is copied only when one of them was rewritten.
func setPropertyMapValue(value interface{}, operator string) (map[string]interface{}, error) {
	value = cypherfn.RuntimeValue(value)
	typeName := "NULL"
	if value != nil {
		typeName = cypherTypeName(value)
	}
	props, ok := propertyMapForSetValue(value)
	if value == nil || !ok {
		return nil, localizedStatusError("Neo.ClientError.Statement.TypeError", "InvalidArgumentType",
			localization.CypherMutationsSetSourceNotMap(operator, typeName))
	}
	var stored map[string]interface{}
	for key, v := range props {
		storedValue, rewritten, err := storablePropertyValue(v)
		if err != nil {
			return nil, err
		}
		if rewritten && stored == nil {
			stored = make(map[string]interface{}, len(props))
			for k, original := range props {
				stored[k] = original
			}
		}
		if stored != nil {
			stored[key] = storedValue
		}
	}
	if stored != nil {
		return stored, nil
	}
	return props, nil
}

// evaluateSetExpressionWithContext evaluates a SET (or MERGE ON CREATE / ON
// MATCH SET) value over the clause's nodes and relationships and the value
// scope of ctx, with the row evaluator RETURN and WITH use: a value written
// is the value the same expression projects ([1 > 2] is [false], n.l[0] + 1
// reads the list). An error the evaluation recorded (a temporal
// constructor's ArgumentError, a division by zero) is returned: it is the
// statement's error, which a check of the value it left (null is not a map,
// a list may not hold null) must not replace.
func (e *StorageExecutor) evaluateSetExpressionWithContext(ctx context.Context, expr string, nodes map[string]*storage.Node, rels map[string]*storage.Edge) (interface{}, error) {
	if err := requireSetParameter(ctx, expr); err != nil {
		return nil, err
	}
	if value, resolved := resolveValueReference(ctx, expr, parameterName); resolved {
		return checkedPropertyResult(ctx, expr, value, true, nil)
	}
	if value, resolved := resolveValueReference(ctx, expr, rowPath); resolved {
		return checkedPropertyResult(ctx, expr, value, true, nil)
	}
	return e.checkedPropertyExpression(ctx, expr, entityRow(ctx, nodes, rels))
}

// mergeBindingRow is the row a MERGE's RETURN reads: the values bound in
// ctx (fabric record and child-context bindings, such as an UNWIND variable)
// and the MERGE's node and relationship bindings.
func (e *StorageExecutor) mergeBindingRow(ctx context.Context, nodes map[string]*storage.Node, rels map[string]*storage.Edge) pipelineRow {
	bindings := valueBindingsFromContext(ctx)
	row := make(pipelineRow, len(e.fabricRecordBindings)+len(bindings)+len(nodes)+len(rels))
	for name, value := range e.fabricRecordBindings {
		row[name] = value
	}
	for name, value := range bindings {
		row[name] = value
	}
	// An unbound variable (an OPTIONAL MATCH without a match) is null.
	for name, node := range nodes {
		if name == "" {
			continue
		}
		if node == nil {
			row[name] = nil
		} else {
			row[name] = node
		}
	}
	for name, edge := range rels {
		if name == "" {
			continue
		}
		if edge == nil {
			row[name] = nil
		} else {
			row[name] = edge
		}
	}
	return row
}

// projectMergeReturn projects a MERGE statement's binding rows through the
// shared RETURN projection (pipelineApplyReturn), so DISTINCT, aggregation,
// ORDER BY, SKIP and LIMIT apply over all the statement's rows (#640).
// returnClause starts with RETURN.
func (e *StorageExecutor) projectMergeReturn(ctx context.Context, rows []pipelineRow, returnClause string) (*ExecuteResult, error) {
	return e.projectMergeReturnSource(ctx, rows, returnClause, pipelineRowsSource(rows))
}

func (e *StorageExecutor) projectMergeReturnSource(ctx context.Context, rows []pipelineRow, returnClause string, source pipelineRowSource, preparedGroups ...[]*pipelineAggregateGroup) (*ExecuteResult, error) {
	returnClause = strings.TrimSpace(returnClause)
	return e.projectMergeReturnPlan(ctx, rows, returnClause, returnProjectionPlanFor(ctx, returnClause), source, preparedGroups...)
}

func (e *StorageExecutor) projectMergeReturnPlan(ctx context.Context, rows []pipelineRow, returnClause string, plan *returnProjectionPlan, source pipelineRowSource, preparedGroups ...[]*pipelineAggregateGroup) (*ExecuteResult, error) {
	priorFailure := getExpressionFailure(ctx)
	result, handled := e.pipelineApplyReturnPlan(ctx, rows, plan, source, false, preparedGroups...)
	if failure := getExpressionFailure(ctx); failure != nil && (!handled || priorFailure == nil) {
		return nil, failure
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if handled {
		return result, nil
	}
	err := newSemanticError(
		"Neo.ClientError.Statement.SyntaxError",
		"UnexpectedSyntax",
		"could not parse RETURN expression: "+strings.TrimSpace(returnClause[len("RETURN"):]),
	)
	recordExpressionFailure(ctx, err)
	return nil, err
}
