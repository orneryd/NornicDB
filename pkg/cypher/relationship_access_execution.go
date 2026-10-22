package cypher

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// relationshipAccessExecution is the only entry to the relationship read
// kernels. It obtains the same compiled record/decision that EXPLAIN projects;
// individual consumers cannot choose an independent scan or seek.
type relationshipAccessExecution struct {
	// Keep original and physical syntax by immutable identity. Traversal
	// callbacks copy this capability, so copying three strings per read
	// would duplicate source metadata in every closure.
	relationshipEntityExecution
	identity, physical *relationshipAccessIdentity
	access             *relationshipAccess
}

// statement projects the existing typed execution capability, never ambient context.
func (run relationshipAccessExecution) statement() admittedRelationshipStatement {
	return admittedRelationshipStatement{program: run.access.program}
}

func (run relationshipAccessExecution) pattern() string {
	return run.identity.key.pattern
}

func (run relationshipAccessExecution) where() string {
	return run.identity.key.where
}

func (run relationshipAccessExecution) physicalWhere() string {
	return run.physical.key.where
}

// Storage-only entity reads have no access choices. Reuse their immutable empty
// descriptor so hydration and degree APIs do not allocate a plan for each read.
var relationshipStorageAccess = &relationshipAccess{}
var relationshipStorageIdentity = &relationshipAccessIdentity{}

type relationshipVectorAccess struct {
	index, relType, property string
	estimatedRows            int64
	// route is a conditional plan choice or a completed physical access.
	route, reason           string
	failedIndex, indexError string
}

type relationshipIDAccess struct {
	kind, expression string
	estimatedRows    int64
}

// relationshipStorageExecution gives explicit-entity APIs the same owner's
// storage-only capability. There is no parsed access site to describe for a
// metadata, hydration or write-validation read. Parsed kernels instead carry
// the execution obtained for their registered statement template.
func relationshipStorageExecution() relationshipEntityExecution {
	return relationshipEntityExecution{}
}

func (run relationshipEntityExecution) inDegree(node storage.NodeID) int {
	return run.executor.storage.GetInDegree(node)
}

func (run relationshipEntityExecution) outDegree(node storage.NodeID) int {
	return run.executor.storage.GetOutDegree(node)
}

func (e *StorageExecutor) relationshipExecutionFor(statement admittedRelationshipStatement, ctx context.Context, pattern, where string) relationshipAccessExecution {
	physical := relationshipAccessIdentityFor(pattern, where)
	identity := relationshipAccessSourceIdentity(statement, ctx, physical)
	return relationshipAccessExecution{relationshipEntityExecution: relationshipEntityExecution{executor: e}, identity: identity, physical: physical, access: e.relationshipAccessForIdentity(statement, ctx, identity)}
}

// Path-search identity belongs to its admitted source, independently of any
// ordinary MATCH with the same pattern. Keep ordinary execution's lookup and
// capability size unchanged; the kind is determined once by the source compiler.
func (e *StorageExecutor) relationshipPathExecutionFor(statement admittedRelationshipStatement, ctx context.Context, pattern string) relationshipAccessExecution {
	physical := relationshipAccessIdentityFor(pattern, "")
	source := relationshipAccessSourceIdentity(statement, ctx, physical)
	template := statement.program.paths[source.id]
	if template == nil || statement.program.byID[template.id] != template {
		recordUnregisteredRelationshipAccess(ctx, source.key.pattern, source.key.where)
		identity := &relationshipAccessIdentity{key: source.key, id: relationshipAccessKindID(source.id, relationshipPathAccess)}
		return relationshipAccessExecution{relationshipEntityExecution: relationshipEntityExecution{executor: e}, identity: identity, physical: physical, access: &relationshipAccess{program: statement.program}}
	}
	return relationshipAccessExecution{relationshipEntityExecution: relationshipEntityExecution{executor: e}, identity: template.identity, physical: physical, access: e.relationshipTemplateAccess(statement, ctx, template)}
}

// Native pattern properties use private parameters after their values have
// been evaluated once. Resolve that physical spelling to its original source
// before the admitted program lookup, shared by execution and access planning.
// The physical identity remains on the frame for residual evaluation.
func relationshipAccessSourceIdentity(statement admittedRelationshipStatement, ctx context.Context, physical *relationshipAccessIdentity) *relationshipAccessIdentity {
	pattern := physical.key.pattern
	for prepared, _ := ctx.Value(preparedPatternRowKey{}).(*preparedPatternRowContext); prepared != nil; prepared, _ = prepared.Context.Value(preparedPatternRowKey{}).(*preparedPatternRowContext) {
		if prepared.statement.program == statement.program && prepared.plan.pattern == pattern {
			pattern = prepared.plan.source
		}
	}
	if original, ok := ctx.Value(relationshipRowIdentityKey{}).(*relationshipAccessIdentity); ok && original.key.pattern == pattern {
		return original
	}
	if pattern != physical.key.pattern {
		return relationshipAccessIdentityFor(pattern, physical.key.where)
	}
	return physical
}

func (e *StorageExecutor) relationshipExecutionForQuery(statement admittedRelationshipStatement, ctx context.Context, query string) (relationshipAccessExecution, bool) {
	program := statement.program
	shape := program
	if program.query != query {
		// A fragment may contain one vector site while its source has other
		// sites. Read its shape with the same compiler, but only the source's
		// registered template can authorize execution (including removals).
		shape = relationshipProgramFor(query)
	}
	if len(shape.sites) != 1 || len(shape.graphQueries) != 0 {
		var empty relationshipAccessExecution
		return empty, false
	}
	template := program.byID[shape.sites[0].id]
	if template == nil {
		var empty relationshipAccessExecution
		return empty, false
	}
	if template.kind == relationshipPathAccess {
		return e.relationshipPathExecutionFor(statement, ctx, template.pattern), true
	}
	return e.relationshipExecutionFor(statement, ctx, template.pattern, template.where), true
}

func (run relationshipAccessExecution) vectorPlan(ctx context.Context, query string) *relationshipVectorAccess {
	request, ok := run.vectorRequest(ctx, query)
	if !ok {
		return nil
	}
	return run.vectorChoice(request, run.executor.search())
}

func (run relationshipAccessExecution) vector(ctx context.Context, query string) (*ExecuteResult, bool) {
	request, ok := run.vectorRequest(ctx, query)
	if !ok {
		return nil, false
	}
	return run.executeVector(ctx, request)
}

func (run relationshipAccessExecution) count(ctx context.Context, match *TraversalMatch, item returnItem) (int64, bool, error) {
	if len(run.access.alternatives) != 0 {
		return 0, false, nil
	}
	n, ok, err := run.relationshipCountFromStore(ctx, match, item)
	if ok {
		recordRelationshipSeekFallback(ctx, run.identity.id, match.Relationship.Variable)
	}
	return n, ok, err
}

func (run relationshipAccessExecution) aggregate(ctx context.Context, match *TraversalMatch, items []returnItem) ([][]interface{}, bool, error) {
	if len(run.access.alternatives) != 0 {
		return nil, false, nil
	}
	rows, ok, err := run.tryFastRelationshipAggregations(match, items)
	if ok {
		recordRelationshipSeekFallback(ctx, run.identity.id, match.Relationship.Variable)
	}
	return rows, ok, err
}

func (run relationshipAccessExecution) degree(ctx context.Context, node *storage.Node, shape boundDegreeShape) int64 {
	recordRelationshipSeekFallback(ctx, run.identity.id, shape.relationship)
	return run.nodeDegreeCount(ctx, node, shape)
}

func (run relationshipAccessExecution) idSeedNodes(ctx context.Context) (nodes []*storage.Node, none, ok bool) {
	return run.scanRelationshipIDSeedNodes(ctx, run.pattern(), run.physicalWhere())
}

func (run relationshipAccessExecution) selectedPaths(ctx context.Context, plan *selectedPathPlan, automaton *pathSearchAutomaton, reversed bool, row pipelineRow, maxLength int, source, target *storage.Node, ends []*storage.Node, count int, groups bool) []pipelineRow {
	recordRelationshipSeekFallback(ctx, run.identity.id, run.access.variable)
	e := run.executor
	bind := func(path pathSearchPath) (pipelineRow, bool) {
		return e.bindSelectedPath(run.statement(), ctx, plan, automaton, reversed, row, path)
	}
	search := run.newPathSearch(ctx, automaton, row, maxLength, source)
	if target != nil {
		direct := len(plan.pathChecks) == 0 && len(plan.iterationChecks) == 0 && !plan.acyclic && !plan.repeated
		for _, variable := range plan.listVariables {
			if _, bound := row[variable]; bound {
				direct = false
			}
		}
		return search.pair(run.statement(), source, target, count, groups, direct, bind)
	}
	if ends != nil {
		search.ends = make(map[storage.NodeID]bool, len(ends))
		for _, node := range ends {
			search.ends[node.ID] = true
		}
	}
	return search.run(run.statement(), source, count, groups, bind)
}

func (run relationshipEntityExecution) incidentEdgeLists(ctx context.Context, store storage.Engine, node storage.NodeID, direction string) ([]*storage.Edge, []*storage.Edge, error) {
	return run.incidentEdgeListsContext(ctx, store, node, direction)
}

func (run relationshipEntityExecution) undirectedIncidentEdges(ctx context.Context, store storage.Engine, node storage.NodeID) ([]*storage.Edge, error) {
	outgoing, incoming, err := run.incidentEdgeLists(ctx, store, node, "both")
	if err != nil {
		return nil, err
	}
	return combineIncidentEdges(ctx, node, outgoing, incoming)
}

func (run relationshipAccessExecution) anchoredCount(ctx context.Context, match *TraversalMatch, seed *storage.Node, values map[string]interface{}, first bool) (int64, bool) {
	paths, sought, err := run.tryID(ctx, match, []*storage.Node{seed}, nil)
	if !sought && err == nil {
		paths, sought, err = run.trySeek(ctx, match, []*storage.Node{seed}, nil)
	}
	if err != nil {
		recordExpressionFailure(ctx, err)
		return 0, true
	}
	if sought {
		return run.executor.countRelationshipPaths(run.statement(), ctx, paths, match, run.where(), values, first), true
	}
	return run.scanRelationshipCount(ctx, match, seed, run.where(), values, first), true
}

// paths keeps the existing seed/streaming/temporal kernels below the one access
// decision. optimizedStart is a node-index pruning result; start/end are actual
// bound endpoints and therefore also constrain a relationship-index result.
func (run relationshipAccessExecution) paths(ctx context.Context, match *TraversalMatch, start, end, optimizedStart []*storage.Node) ([]PathResult, error) {
	if paths, sought, err := run.tryID(ctx, match, start, end); sought || err != nil {
		return paths, err
	}
	if paths, sought, err := run.trySeek(ctx, match, start, end); sought || err != nil {
		return paths, err
	}
	if optimizedStart == nil {
		optimizedStart = start
	}
	return run.scanRelationshipPaths(ctx, match, optimizedStart, end, run.physicalWhere())
}

func (run relationshipAccessExecution) scanRelationshipPaths(ctx context.Context, match *TraversalMatch, start, end []*storage.Node, where string) ([]PathResult, error) {
	e := run.executor
	var paths []PathResult
	if seeded, ok := run.traverseFromEndSeeds(ctx, match, start, end, where); ok {
		return seeded, nil
	}
	if len(start) > 0 {
		if match.IsChained && len(match.Segments) > 1 {
			paths = run.traverseChainedGraph(ctx, match, start)
		} else {
			viewport, _ := TemporalViewportFromContext(ctx)
			checker, _ := e.getStorage(ctx).(temporalCurrentNodeChecker)
			paths = run.traverseGraphSequential(ctx, match, start, viewport, checker)
		}
	} else if match.TraversalLimit > 0 && !match.IsChained && len(match.StartNode.properties) == 0 {
		viewport, _ := TemporalViewportFromContext(ctx)
		checker, _ := e.getStorage(ctx).(temporalCurrentNodeChecker)
		var streamed bool
		var err error
		paths, streamed, err = run.traverseGraphWithStreamingStartNodes(ctx, match, viewport, checker)
		if err != nil {
			return nil, err
		}
		if !streamed {
			paths = run.traverseGraph(ctx, match)
		}
	} else {
		paths = run.traverseGraph(ctx, match)
	}
	if where != "" {
		paths = e.filterPathsByWhere(run.statement(), ctx, paths, match, where, nil)
	}
	return paths, nil
}

func (e *StorageExecutor) countRelationshipPaths(admission admittedRelationshipStatement, ctx context.Context, paths []PathResult, matches *TraversalMatch, where string, values map[string]interface{}, first bool) int64 {
	var count int64
	for _, path := range paths {
		inner := e.buildPathContext(path, matches)
		correlated := true
		for name, node := range inner.nodes {
			if bound, isNode := values[name].(*storage.Node); isNode && (bound == nil || node == nil || bound.ID != node.ID) {
				correlated = false
				break
			}
		}
		for name, relationship := range inner.rels {
			if !correlated {
				break
			}
			if bound, isRel := values[name].(*storage.Edge); isRel && (bound == nil || relationship == nil || bound.ID != relationship.ID) {
				correlated = false
				break
			}
		}
		if !correlated {
			continue
		}
		if where != "" {
			// The WHERE sees the row's values with the path's bindings.
			row := make(map[string]interface{}, len(values)+len(inner.nodes)+len(inner.rels))
			for name, value := range values {
				row[name] = value
			}
			for name, node := range inner.nodes {
				row[name] = node
			}
			for name, relationship := range inner.rels {
				row[name] = relationship
			}
			if !e.evaluateRowPredicate(admission, ctx, where, row) {
				continue
			}
		}
		count++
		if first {
			return count
		}
	}
	return count
}
