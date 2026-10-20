package cypher

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// relationshipAccessExecution is the only entry to the relationship read
// kernels. It obtains the same compiled record/decision that EXPLAIN projects;
// individual consumers cannot choose an independent scan or seek.
type relationshipAccessExecution struct {
	executor                      *StorageExecutor
	pattern, where, physicalWhere string
	access                        *relationshipAccess
}

// Storage-only entity reads have no access choices. Reuse their immutable empty
// descriptor so hydration and degree APIs do not allocate a plan for each read.
var relationshipStorageAccess = &relationshipAccess{}

type relationshipVectorAccess struct {
	index, relType, property string
	estimatedRows            int64
}

type relationshipIDAccess struct {
	kind, expression string
	estimatedRows    int64
}

type relationshipVectorProbeKey struct{}
type relationshipVectorProbe struct {
	planOnly         bool
	access           *relationshipVectorAccess
	candidates, rows int64
}

// relationshipStorageExecution gives explicit-entity APIs the same owner's
// storage-only capability. There is no parsed access site to describe for a
// metadata, hydration or write-validation read. Parsed kernels instead carry
// the execution obtained for their registered statement template.
func relationshipStorageExecution() relationshipAccessExecution {
	var executor *StorageExecutor
	return executor.relationshipExecutionFor(nil, "", "")
}

func (run relationshipAccessExecution) inDegree(node storage.NodeID) int {
	return run.executor.storage.GetInDegree(node)
}

func (run relationshipAccessExecution) outDegree(node storage.NodeID) int {
	return run.executor.storage.GetOutDegree(node)
}

func (e *StorageExecutor) relationshipExecutionFor(ctx context.Context, pattern, where string) relationshipAccessExecution {
	if pattern == "" {
		return relationshipAccessExecution{executor: e, access: relationshipStorageAccess}
	}
	physicalWhere := where
	if original, ok := ctx.Value(relationshipSeekPredicateKey{}).(relationshipSeekPredicate); ok && original.pattern == pattern {
		where = original.where
	}
	return relationshipAccessExecution{executor: e, pattern: pattern, where: where, physicalWhere: physicalWhere, access: e.relationshipAccessFor(ctx, pattern, where)}
}

func (e *StorageExecutor) relationshipExecutionForQuery(ctx context.Context, query string) (relationshipAccessExecution, bool) {
	program := relationshipProgramFromContext(ctx, query)
	if len(program.sites) != 1 || len(program.graphQueries) != 0 {
		var empty relationshipAccessExecution
		return empty, false
	}
	template := program.sites[0]
	return e.relationshipExecutionFor(ctx, template.pattern, template.where), true
}

func (run relationshipAccessExecution) vectorPlan(ctx context.Context, query string) *relationshipVectorAccess {
	probe := &relationshipVectorProbe{planOnly: true}
	ctx = context.WithValue(ctx, relationshipVectorProbeKey{}, probe)
	if _, handled := run.tryRawMatchRelationshipVectorCosine(ctx, query); !handled {
		_, _ = run.tryRawMatchWithRelationshipVectorCosineProjection(ctx, query)
	}
	return probe.access
}

func (run relationshipAccessExecution) vector(ctx context.Context, query string) (*ExecuteResult, bool) {
	probe := &relationshipVectorProbe{}
	ctx = context.WithValue(ctx, relationshipVectorProbeKey{}, probe)
	result, handled := run.tryRawMatchRelationshipVectorCosine(ctx, query)
	if !handled {
		result, handled = run.tryRawMatchWithRelationshipVectorCosineProjection(ctx, query)
	}
	if handled && probe.access != nil {
		if result != nil {
			probe.rows = int64(len(result.Rows))
		}
		recordRelationshipVectorAccess(ctx, run.pattern, run.where, run.access.variable, probe.access, probe.candidates, probe.rows)
	}
	return result, handled
}

func (run relationshipAccessExecution) count(ctx context.Context, match *TraversalMatch, item returnItem) (int64, bool, error) {
	if len(run.access.alternatives) != 0 {
		return 0, false, nil
	}
	n, ok, err := run.relationshipCountFromStore(ctx, match, item)
	if ok {
		recordRelationshipSeekFallback(ctx, run.pattern, run.where, match.Relationship.Variable)
	}
	return n, ok, err
}

func (run relationshipAccessExecution) aggregate(ctx context.Context, match *TraversalMatch, items []returnItem) ([][]interface{}, bool, error) {
	if len(run.access.alternatives) != 0 {
		return nil, false, nil
	}
	rows, ok, err := run.tryFastRelationshipAggregations(match, items)
	if ok {
		recordRelationshipSeekFallback(ctx, run.pattern, run.where, match.Relationship.Variable)
	}
	return rows, ok, err
}

func (run relationshipAccessExecution) degree(ctx context.Context, node *storage.Node, shape boundDegreeShape) int64 {
	recordRelationshipSeekFallback(ctx, run.pattern, run.where, shape.relationship)
	return run.nodeDegreeCount(ctx, node, shape)
}

func (run relationshipAccessExecution) idSeedNodes(ctx context.Context) (nodes []*storage.Node, none, ok bool) {
	return run.scanRelationshipIDSeedNodes(ctx, run.pattern, run.physicalWhere)
}

func (run relationshipAccessExecution) selectedPaths(ctx context.Context, plan *selectedPathPlan, automaton *pathSearchAutomaton, reversed bool, row pipelineRow, maxLength int, source, target *storage.Node, ends []*storage.Node, count int, groups bool) []pipelineRow {
	recordRelationshipSeekFallback(ctx, run.pattern, run.where, run.access.variable)
	e := run.executor
	bind := func(path pathSearchPath) (pipelineRow, bool) {
		return e.bindSelectedPath(ctx, plan, automaton, reversed, row, path)
	}
	search := run.newPathSearch(ctx, automaton, row, maxLength, source)
	if target != nil {
		direct := len(plan.pathChecks) == 0 && len(plan.iterationChecks) == 0 && !plan.acyclic && !plan.repeated
		for _, variable := range plan.listVariables {
			if _, bound := row[variable]; bound {
				direct = false
			}
		}
		return search.pair(source, target, count, groups, direct, bind)
	}
	if ends != nil {
		search.ends = make(map[storage.NodeID]bool, len(ends))
		for _, node := range ends {
			search.ends[node.ID] = true
		}
	}
	return search.run(source, count, groups, bind)
}

func (run relationshipAccessExecution) incidentEdgeLists(ctx context.Context, store storage.Engine, node storage.NodeID, direction string) ([]*storage.Edge, []*storage.Edge, error) {
	return run.incidentEdgeListsContext(ctx, store, node, direction)
}

func (run relationshipAccessExecution) undirectedIncidentEdges(ctx context.Context, store storage.Engine, node storage.NodeID) ([]*storage.Edge, error) {
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
		return run.executor.countRelationshipPaths(ctx, paths, match, run.where, values, first), true
	}
	return run.scanRelationshipCount(ctx, match, seed, run.where, values, first), true
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
	return run.scanRelationshipPaths(ctx, match, optimizedStart, end, run.physicalWhere)
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
		paths = e.filterPathsByWhere(ctx, paths, match, where, nil)
	}
	return paths, nil
}

func (e *StorageExecutor) countRelationshipPaths(ctx context.Context, paths []PathResult, matches *TraversalMatch, where string, values map[string]interface{}, first bool) int64 {
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
			if !e.evaluateRowPredicate(ctx, where, row) {
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
