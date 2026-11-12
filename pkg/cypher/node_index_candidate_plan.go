package cypher

// NOR-414 deferred node-index candidate planning.
//
// collectPipelineIndexedNodeCandidates is the one node-pattern access
// decision: it picks the index route that narrows a MATCH node pattern, in a
// fixed order of precedence, and reads that route's candidates. Not every
// caller needs the nodes. The traversal seed choice (chooseTraversalEndSeeds)
// needs only the size of the end anchor's candidate set to compare it with the
// start side, and discards the whole set when the start side is cheaper;
// reading every candidate body there materialised an unselected index
// candidate set (128 document bodies read to compare a cost, in the reported
// lookup).
//
// The dispatch is therefore one preparation and one read. Preparation
// (planPipelineIndexedNodeCandidates) keeps the dispatch's route order and its
// selection rules unchanged, and resolves the selected route to the node IDs it
// lists: property index postings, a resolved id()/elementId() reference, or the
// nodes the ordered window's ordered read already returns. Reading
// (readNodeIndexCandidatePlan) hydrates those IDs through the admission each
// route applied when it read nodes itself, and answers the residual WHERE flag.
// The hydration is one loop over a typed request (readNodeCandidates) that
// names the route's admission, so the routes cannot diverge by copies of one
// predicate.
// A caller that needs only the candidate count reads no node body, and no
// input is resolved twice or earlier than the old winning route resolved it.
//
// A plan is one MATCH invocation's private value: it holds no engine state, no
// cache and no candidate nodes of its own, and it is dropped with its caller.
// The ordered window is the one route that keeps nodes, because its ordered
// read is the window itself; the seed planner never supplies the hint that
// reaches it.

import (
	"context"
	"strings"

	"github.com/orneryd/nornicdb/internal/ctxsort"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// nodeCandidateRoute names the index route a node candidate plan prepared.
type nodeCandidateRoute uint8

const (
	// nodeCandidateNone is a plan that prepared no route.
	nodeCandidateNone nodeCandidateRoute = iota
	// nodeCandidateOrderWindow is the ordered index window. Its ordered read
	// is the window, so the plan keeps the nodes it returned.
	nodeCandidateOrderWindow
	// nodeCandidateIDEquality is an id()/elementId() equality, or an OR of
	// them, resolved to the node IDs it names.
	nodeCandidateIDEquality
	// nodeCandidateIDIn is an id()/elementId() IN list resolved to the node IDs
	// it names.
	nodeCandidateIDIn
	// nodeCandidateSeek is a WHERE point seek: the union of the postings of an
	// equality, IN or OR predicate.
	nodeCandidateSeek
	// nodeCandidateInline is the pattern's own inline property seek: the
	// intersection of the index reads its property values serve.
	nodeCandidateInline
	// nodeCandidateIndexedProperty is a property index range or IS NOT NULL
	// read: the entries of one indexed property on the pattern's labels.
	nodeCandidateIndexedProperty
)

// nodeIndexCandidatePlan is one node pattern's prepared index candidates: the
// route the canonical node candidate dispatch selected, the node IDs that route
// listed in the order it lists them, and the resolved inputs the read applies
// as residual admission. Preparing a plan reads schema metadata, index entries
// and node IDs; reading it reads node bodies.
type nodeIndexCandidatePlan struct {
	route nodeCandidateRoute

	// ids are the candidates the route listed, in the order it listed them:
	// the resolver's ID result, which keeps an ordinary lookup's few IDs in
	// its own array and only a longer listing in an owned slice.
	ids nodeIDResults

	// seek is the point or inline seek the ids were listed through.
	seek nodeIndexSeek

	// nodes is the ordered window's read (nodeCandidateOrderWindow).
	nodes []*storage.Node

	// whereApplied marks a route that already answered the whole WHERE: the id
	// IN list that is the entire predicate.
	whereApplied bool

	// inList marks the point seek that answers a WHERE that is exactly an IN
	// list, whose candidates are then the whole answer.
	inList bool

	// pattern, where and params are the resolved inputs the read's residual
	// admission uses: the pattern's labels and inline properties, the WHERE the
	// route narrowed, and the parameters it was planned with.
	pattern nodePatternInfo
	where   string
	params  map[string]interface{}
}

// candidateCount is the number of candidates the plan prepared, without reading
// a node body: the ordered window's nodes, otherwise the IDs the route listed.
// It guides orientation only and never decides that a query is empty by itself:
// an empty listing is the route's own answer, so a caller that reports it reads
// the plan (which reads no node) instead of inferring it from the count.
func (p nodeIndexCandidatePlan) candidateCount() int {
	if p.route == nodeCandidateOrderWindow {
		return len(p.nodes)
	}
	return p.ids.len()
}

// planPipelineIndexedNodeCandidates is the preparation half of
// collectPipelineIndexedNodeCandidates: the dispatch's access decision, made in
// the same order, resolved to the node IDs the selected route lists. used is
// false when no index route applies. A plan whose candidateCount is zero is the
// route's own empty answer, not an estimate: the caller still reads the plan to
// report it, and that read reads no node.
func (e *StorageExecutor) planPipelineIndexedNodeCandidates(ctx context.Context, nodePattern nodePatternInfo, whereClause string, hint pipelineMatchPhysicalHint) (nodeIndexCandidatePlan, bool, error) {
	params := getParamsFromContext(ctx)
	// The pattern, WHERE and parameters are the resolved inputs the read
	// applies; they are read once, here, as the dispatch read them.
	seed := nodeIndexCandidatePlan{pattern: nodePattern, where: whereClause, params: params}
	if hint.limit > 0 && hint.orderExpr != "" {
		orderedPlans := []func() ([]*storage.Node, bool, error){
			func() ([]*storage.Node, bool, error) {
				return e.tryCollectNodesFromPropertyIndexOrderLimit(ctx, nodePattern, whereClause, hint.orderExpr, hint.limit, indexOrderExplicitNotNull)
			},
			func() ([]*storage.Node, bool, error) {
				return e.tryCollectNodesFromPropertyIndexOrderLimit(ctx, nodePattern, whereClause, hint.orderExpr, hint.limit, indexOrderGeneral)
			},
		}
		for _, ordered := range orderedPlans {
			nodes, used, err := ordered()
			if err != nil {
				return nodeIndexCandidatePlan{}, false, err
			}
			if used {
				e.markOuterIndexTopKUsed()
				seed.route, seed.nodes = nodeCandidateOrderWindow, nodes
				return seed, true, nil
			}
		}
	}
	if ids, used, err := e.resolveIDEqualityCompoundCandidates(ctx, nodePattern, whereClause, params); err != nil || used {
		if err != nil {
			return nodeIndexCandidatePlan{}, false, err
		}
		seed.route, seed.ids = nodeCandidateIDEquality, ids
		return seed, true, nil
	}
	// When an id IN list predicate is the whole WHERE, its seek returns
	// exactly the nodes it selects: the caller need not
	// evaluate the WHERE again on each of them. Evaluating it per row checked
	// every node against the whole list: CALL { … } IN TRANSACTIONS batches,
	// which select their rows by id(n) IN $ids, were quadratic in the batch
	// size (#703).
	if ids, used, err := e.resolveIDInCandidates(ctx, nodePattern, whereClause, params); err != nil || used {
		if err != nil {
			return nodeIndexCandidatePlan{}, false, err
		}
		seed.route, seed.ids, seed.whereApplied = nodeCandidateIDIn, ids, true
		return seed, true, nil
	}
	// An id IN list among AND conditions seeks the same way; the caller
	// still evaluates the whole WHERE on the nodes it selects.
	if findTopLevelKeyword(whereClause, " AND ") > 0 {
		for _, conjunct := range splitTopLevelAndConjuncts(ctx, whereClause) {
			ids, used, err := e.resolveIDInCandidates(ctx, nodePattern, conjunct, params)
			if err != nil {
				return nodeIndexCandidatePlan{}, false, err
			}
			if used {
				seed.route, seed.ids, seed.whereApplied = nodeCandidateIDIn, ids, false
				return seed, true, nil
			}
		}
	}
	// The point seeks (equality, IN and OR from the WHERE, and the pattern's
	// inline properties) compete: the one that reads the fewest index entries
	// is read, whichever predicate or pattern label names it (NOR-414). A
	// non-selective WHERE equality used to win over a unique inline lookup
	// only because its plan was tried first. Range and IS NOT NULL seeks
	// remain the fallbacks they were.
	var seeks nodeIndexSeekSet
	if seek, planned, err := e.planPropertyIndexInOrParam(ctx, nodePattern, whereClause, params); err != nil {
		return nodeIndexCandidatePlan{}, false, err
	} else if planned {
		seeks.add(seek)
	}
	if seek, planned, err := e.planPropertyIndexOrEquality(ctx, nodePattern, whereClause); err != nil {
		return nodeIndexCandidatePlan{}, false, err
	} else if planned {
		seeks.add(seek)
	}
	if err := e.addPropertyIndexInCompoundSeeks(ctx, nodePattern, whereClause, params, &seeks); err != nil {
		return nodeIndexCandidatePlan{}, false, err
	}
	if err := e.addPropertyIndexEqualityCompoundSeeks(ctx, nodePattern, whereClause, &seeks); err != nil {
		return nodeIndexCandidatePlan{}, false, err
	}
	if seek, planned, err := e.planPatternPropertySeek(ctx, nodePattern, e.getStorage(ctx)); err != nil {
		return nodeIndexCandidatePlan{}, false, err
	} else if planned {
		seeks.add(seek)
	}
	plan, used, err := e.resolveRankedNodeIndexSeek(ctx, &seeks, seed)
	if err != nil {
		return nodeIndexCandidatePlan{}, false, err
	}
	if used {
		return plan, true, nil
	}
	ids, used, err := e.resolvePropertyIndexRangeCandidates(ctx, nodePattern, whereClause, params)
	if err != nil {
		return nodeIndexCandidatePlan{}, false, err
	}
	if !used {
		ids, used, err = e.resolvePropertyIndexNotNullCandidates(ctx, nodePattern, whereClause)
		if err != nil || !used {
			return nodeIndexCandidatePlan{}, false, err
		}
	}
	seed.route, seed.ids = nodeCandidateIndexedProperty, ids
	return seed, true, nil
}

// readNodeIndexCandidatePlan is the reading half of
// collectPipelineIndexedNodeCandidates: it hydrates the candidates the plan
// listed, applies the route's residual admission and reports what the dispatch
// reported. whereApplied is true when the route already answered the whole
// WHERE.
func (e *StorageExecutor) readNodeIndexCandidatePlan(ctx context.Context, plan nodeIndexCandidatePlan) (nodes []*storage.Node, whereApplied bool, used bool, err error) {
	switch plan.route {
	case nodeCandidateOrderWindow:
		// The ordered window read its window when it was prepared: its ordered
		// read is the window, so nothing is left to hydrate.
		return plan.nodes, false, true, nil
	case nodeCandidateIDEquality, nodeCandidateIDIn:
		// Both identity routes — an id()/elementId() equality and its IN
		// list — hydrate through the one identity admission, and a route that
		// already answered the whole WHERE answers with no residual.
		nodes, err = e.readNodeCandidates(ctx, e.storage, nodeCandidateRead{
			ids:        plan.ids.ids(),
			policy:     nodeCandidateReadIdentity,
			labels:     plan.pattern.labels,
			properties: plan.pattern.properties,
		})
		if err != nil {
			return nil, false, false, err
		}
		return nodes, plan.whereApplied, true, nil
	case nodeCandidateIndexedProperty:
		nodes, err = e.readNodeCandidates(ctx, e.storage, nodeCandidateRead{
			ids:    plan.ids.ids(),
			policy: nodeCandidateReadIndexedProperty,
			labels: plan.pattern.labels,
		})
		if err != nil {
			return nil, false, false, err
		}
	case nodeCandidateInline:
		// The inline seek reads through the engine it was planned with, and
		// admits the pattern's labels as it read them: every candidate carries
		// every pattern label.
		nodes, err = e.readNodeCandidates(ctx, plan.seek.store, nodeCandidateRead{
			ids:    plan.ids.ids(),
			policy: nodeCandidateReadInline,
			seek:   &plan.seek,
		})
		if err != nil {
			return nil, false, false, err
		}
	case nodeCandidateSeek:
		nodes, err = e.readNodeCandidates(ctx, e.storage, nodeCandidateRead{
			ids:    plan.ids.ids(),
			policy: nodeCandidateReadSeek,
			seek:   &plan.seek,
		})
		if err != nil {
			return nil, false, false, err
		}
	default:
		return nil, false, false, nil
	}
	// The routes above narrowed the candidates without answering the whole
	// WHERE, so the pattern's own inline properties are the residual filter
	// they were after the index read. A point seek (and the inline seek) builds
	// its own slice; a range or IS NOT NULL read fills its candidate slice.
	if len(plan.pattern.properties) > 0 {
		if plan.route == nodeCandidateSeek || plan.route == nodeCandidateInline {
			nodes = e.filterOwnedNodesByProperties(nodes, plan.pattern.properties)
		} else {
			nodes = e.filterNodesByProperties(nodes, plan.pattern.properties)
		}
	}
	// A WHERE that is exactly <variable>.<property> IN <list> on an indexed
	// property is answered by the index: one lookup per list value returns
	// exactly the nodes it selects, so the caller doesn't test each of them
	// against the whole list again (quadratic in the list for a list that
	// names every node). Checked only once the index answered, so a read that
	// takes another route doesn't parse its WHERE for it.
	exact := plan.inList && e.whereIsSimpleIndexedIn(ctx, plan.pattern.variable, plan.where, plan.params)
	return nodes, exact, true, nil
}

// nodeCandidateReadPolicy names the admission one candidate hydration request
// applies to the nodes its IDs name: the admission the route applied when it
// read its own nodes.
type nodeCandidateReadPolicy uint8

const (
	// nodeCandidateReadIdentity admits a node that still carries the pattern's
	// labels and its inline properties, the properties by the canonical MATCH
	// node test (nodeMatchesProps). Go value comparison was not that test: it
	// panics on two values of one uncomparable type, and it rejects an equal
	// list held in another dynamic representation — the reported public failure
	// was that empty row set, where the same pattern's IN route matched. A
	// candidate that is gone or no longer matches is not an error: the route
	// answers with no node.
	nodeCandidateReadIdentity nodeCandidateReadPolicy = iota
	// nodeCandidateReadSeek admits a node the WHERE point seek still sees: an
	// index can list a node that has since lost a label or value.
	nodeCandidateReadSeek
	// nodeCandidateReadIndexedProperty admits a node that still carries the
	// pattern's labels: an index can still list a node that lost one.
	nodeCandidateReadIndexedProperty
	// nodeCandidateReadInline admits a node the pattern's inline seek still
	// sees: a pattern of two or more labels keeps the nodes that carry all of
	// them.
	nodeCandidateReadInline
)

// nodeCandidateRead is one route's request to hydrate the candidates it listed:
// the IDs it listed in order, and the admission applied to each node they name.
type nodeCandidateRead struct {
	ids    []storage.NodeID
	policy nodeCandidateReadPolicy

	// labels and properties are the pattern's resolved labels and its inline
	// properties: the indexed-property route admits the labels alone, and with
	// the properties the identity route admits both.
	labels     []string
	properties map[string]interface{}

	// seek is the point or inline seek the IDs were listed through: the
	// admission of a WHERE point seek, and the pattern labels the inline route
	// keeps.
	seek *nodeIndexSeek
}

// readNodeCandidates hydrates one route's listed candidates: it reads each ID's
// node through store and keeps the nodes the request's policy
// admits, in the order the route listed them. It is the one ID-to-node read of
// the candidate routes, so no route can admit candidates by a copy of another
// route's predicate.
//
// A node that is missing, or whose read fails, is the route's answer without
// that node rather than the caller's error; a canceled context is the caller's
// error. The output slice is the read's own, and non-nil even when it admits no
// node; ids is borrowed for the length of the read.
func (e *StorageExecutor) readNodeCandidates(ctx context.Context, store storage.Engine, req nodeCandidateRead) ([]*storage.Node, error) {
	ids := req.ids
	if req.policy == nodeCandidateReadSeek && len(ids) == 0 {
		// No posting entry is the seek's own empty answer, and it reports it
		// without a read.
		return []*storage.Node{}, nil
	}
	nodes := make([]*storage.Node, 0, len(ids))
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		node, err := e.getQueryNode(ctx, store, id)
		if err != nil || node == nil {
			continue
		}
		switch req.policy {
		case nodeCandidateReadIdentity:
			if len(req.labels) > 0 && !mergeNodeHasLabels(node, req.labels) {
				continue
			}
			if len(req.properties) > 0 && !e.nodeMatchesProps(node, req.properties) {
				continue
			}
		case nodeCandidateReadSeek:
			if !req.seek.admits(node) {
				continue
			}
		case nodeCandidateReadIndexedProperty:
			if !mergeNodeHasLabels(node, req.labels) {
				continue
			}
		case nodeCandidateReadInline:
			if len(req.seek.patternLabels) >= 2 && !mergeNodeHasLabels(node, req.seek.patternLabels) {
				continue
			}
		}
		nodes = append(nodes, node)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return nodes, nil
}

// resolveRankedNodeIndexSeek prepares the best of seeks (bestNodeIndexSeek), or,
// when it cannot decide, the others in order: the first seek the index answers
// is the route. used is false when none answers.
func (e *StorageExecutor) resolveRankedNodeIndexSeek(ctx context.Context, seeks *nodeIndexSeekSet, seed nodeIndexCandidatePlan) (nodeIndexCandidatePlan, bool, error) {
	if seeks.n == 0 {
		return nodeIndexCandidatePlan{}, false, ctx.Err()
	}
	bestAt, err := bestNodeIndexSeek(ctx, seeks)
	if err != nil {
		return nodeIndexCandidatePlan{}, false, err
	}
	for i := -1; i < seeks.n; i++ {
		at := i
		if i < 0 {
			at = bestAt
		} else if i == bestAt {
			continue
		}
		plan, used, err := e.planNodeIndexSeek(ctx, seeks.at(at), seed)
		if err != nil {
			return nodeIndexCandidatePlan{}, false, err
		}
		if used {
			return plan, true, nil
		}
	}
	return nodeIndexCandidatePlan{}, false, nil
}

// planNodeIndexSeek prepares one planned seek's candidates: the IDs the seek
// lists, in the order it lists them, in seed's pattern, WHERE and parameters,
// which the read applies as residual admission.
func (e *StorageExecutor) planNodeIndexSeek(ctx context.Context, seek *nodeIndexSeek, seed nodeIndexCandidatePlan) (nodeIndexCandidatePlan, bool, error) {
	plan := seed
	plan.seek = *seek
	plan.inList = seek.inList
	var ids nodeIDResults
	var used bool
	var err error
	if seek.isInline() {
		plan.route = nodeCandidateInline
		ids, used, err = e.resolvePatternPropertySeek(ctx, seek)
	} else {
		plan.route = nodeCandidateSeek
		ids, used, err = e.resolveNodeIndexSeekCandidates(ctx, seek)
	}
	if err != nil || !used {
		return nodeIndexCandidatePlan{}, false, err
	}
	plan.ids = ids
	return plan, true, nil
}

// resolveNodeIndexSeekCandidates lists the IDs a WHERE point seek reads in ID
// order: the union of its postings. A posting set that lists nothing yields no
// candidate, which the read reports as the seek's empty answer. The listing is
// ordered in place — in the result's own array while it fits there — so
// ordering it needs no second listing.
func (e *StorageExecutor) resolveNodeIndexSeekCandidates(ctx context.Context, seek *nodeIndexSeek) (nodeIDResults, bool, error) {
	idSet := make(map[storage.NodeID]struct{})
	for g := range seek.postings {
		postings := &seek.postings[g]
		for _, label := range postings.labels {
			for i := 0; i < postings.valueCount(); i++ {
				lookupIDs, lookupErr := propertyIndexLookup(ctx, e.storage, seek.schema, label, postings.property, postings.valueAt(i))
				if lookupErr != nil {
					return nodeIDResults{}, false, lookupErr
				}
				for _, id := range lookupIDs {
					if err := ctx.Err(); err != nil {
						return nodeIDResults{}, false, err
					}
					idSet[id] = struct{}{}
				}
			}
		}
	}
	ids := newIDResults(len(idSet))
	for id := range idSet {
		if err := ctx.Err(); err != nil {
			return nodeIDResults{}, false, err
		}
		ids.add(id)
	}
	listed := ids.ids()
	if err := ctxsort.Sort(ctx, listed, func(a, b storage.NodeID) int { return strings.Compare(string(a), string(b)) }, false); err != nil {
		return nodeIDResults{}, false, err
	}
	if err := ctx.Err(); err != nil {
		return nodeIDResults{}, false, err
	}
	return ids, true, nil
}

// resolvePropertyIndexRangeCandidates prepares the range seek: the node IDs the
// pattern labels' index on the WHERE's bounded property lists. Only the bounds
// narrow the seed; the caller still evaluates the whole WHERE on every
// candidate (#820).
func (e *StorageExecutor) resolvePropertyIndexRangeCandidates(ctx context.Context, nodePattern nodePatternInfo, whereClause string, params map[string]interface{}) (nodeIDResults, bool, error) {
	if err := ctx.Err(); err != nil {
		return nodeIDResults{}, false, err
	}
	if len(nodePattern.labels) == 0 {
		return nodeIDResults{}, false, nil
	}
	property, bounds, ok := parseIndexedRangeBounds(ctx, nodePattern.variable, whereClause, params)
	if !ok {
		return nodeIDResults{}, false, nil
	}
	schema := orderedPropertyIndexSchema(e.storage)
	if schema == nil {
		return nodeIDResults{}, false, nil
	}
	return e.resolveIndexedPropertyCandidates(ctx, schema, nodePattern, property, func(label string) ([]storage.NodeID, bool, error) {
		return schema.PropertyIndexRange(ctx, label, property, bounds)
	})
}

// resolvePropertyIndexNotNullCandidates prepares the IS NOT NULL seek: the node
// IDs the pattern labels' index on the WHERE's property lists.
func (e *StorageExecutor) resolvePropertyIndexNotNullCandidates(ctx context.Context, nodePattern nodePatternInfo, whereClause string) (nodeIDResults, bool, error) {
	if err := ctx.Err(); err != nil {
		return nodeIDResults{}, false, err
	}
	if len(nodePattern.labels) == 0 {
		return nodeIDResults{}, false, nil
	}
	property, ok := e.parseSimpleIndexedIsNotNull(ctx, nodePattern.variable, whereClause)
	if !ok {
		return nodeIDResults{}, false, nil
	}
	schema := orderedPropertyIndexSchema(e.storage)
	if schema == nil {
		return nodeIDResults{}, false, nil
	}
	return e.resolveIndexedPropertyCandidates(ctx, schema, nodePattern, property, func(label string) ([]storage.NodeID, bool, error) {
		return schema.PropertyIndexNonNilCandidates(ctx, label, property)
	})
}

// resolveIndexedPropertyCandidates is the shared preparation of the index seeks
// that read one indexed property per candidate label: it gathers lookup(label)
// for every label of the pattern that indexes property and de-duplicates the
// IDs in lookup order. used is false when no candidate label indexes property,
// and the read keeps the nodes that carry all pattern labels.
func (e *StorageExecutor) resolveIndexedPropertyCandidates(ctx context.Context, schema *storage.SchemaManager, nodePattern nodePatternInfo, property string, lookup func(label string) ([]storage.NodeID, bool, error)) (nodeIDResults, bool, error) {
	if err := ctx.Err(); err != nil {
		return nodeIDResults{}, false, err
	}
	labels, err := e.indexCandidateLabels(ctx, schema, nodePattern.labels, property)
	if err != nil {
		return nodeIDResults{}, false, err
	}
	if len(labels) == 0 {
		return nodeIDResults{}, false, nil
	}
	idSet := make(map[storage.NodeID]struct{})
	var ordered nodeIDResults
	for _, label := range labels {
		ids, ok, err := lookup(label)
		if err != nil {
			return nodeIDResults{}, false, err
		}
		if !ok {
			return nodeIDResults{}, false, nil
		}
		for _, id := range ids {
			if err := ctx.Err(); err != nil {
				return nodeIDResults{}, false, err
			}
			if _, exists := idSet[id]; exists {
				continue
			}
			idSet[id] = struct{}{}
			ordered.add(id)
		}
	}
	return ordered, true, nil
}
