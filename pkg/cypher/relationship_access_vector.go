package cypher

import (
	"context"
	"sort"
	"strings"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/pkg/util"
)

// A request contains syntax, never a resolved query vector or an executed access.
// Both public vector shapes use this reader. Planning stops at choice(); only
// executeVector resolves expressions and publishes completed physical accesses.
type relationshipVectorRequest struct {
	pattern                               parsedSimpleMatchRelationshipPattern
	items                                 []returnItem
	query, property, score, before, after string
	cosine                                int
	projection, descending                bool
	limit                                 int
	index                                 string
}

func (run relationshipAccessExecution) vectorRequest(ctx context.Context, query string) (*relationshipVectorRequest, bool) {
	e := run.executor
	text := strings.TrimSpace(query)
	if !startsWithKeywordFold(text, "MATCH") {
		return nil, false
	}
	for _, keyword := range []string{"SKIP", "UNWIND", "OPTIONAL MATCH", "CALL", "CREATE", "MERGE", "DELETE", "DETACH DELETE", "SET", "REMOVE", "UNION"} {
		if findKeywordIndex(text, keyword) > 0 {
			return nil, false
		}
	}
	ret, order, limit := findKeywordIndex(text, "RETURN"), findKeywordIndex(text, "ORDER BY"), findKeywordIndex(text, "LIMIT")
	if ret <= 0 || order <= ret || limit <= order {
		return nil, false
	}
	with := findKeywordIndex(text, "WITH")
	request := &relationshipVectorRequest{cosine: -1, projection: with > 0}
	matchEnd := ret
	if request.projection {
		if with >= ret {
			return nil, false
		}
		matchEnd = with
	}
	pattern := strings.TrimSpace(text[len("MATCH"):matchEnd])
	if at := findKeywordIndex(pattern, "WHERE"); at > 0 {
		request.before = strings.TrimSpace(pattern[at+len("WHERE"):])
		pattern = strings.TrimSpace(pattern[:at])
		if request.before == "" {
			return nil, false
		}
	}
	var ok bool
	request.pattern, ok = e.parseSimpleMatchRelationshipPattern(pattern)
	if !ok || request.pattern.relVar == "" || request.pattern.relType == "" {
		return nil, false
	}
	request.items = e.parseReturnItems(strings.TrimSpace(text[ret+len("RETURN") : order]))
	if len(request.items) == 0 {
		return nil, false
	}
	cosineItems := request.items
	if request.projection {
		projection := strings.TrimSpace(text[with+len("WITH") : ret])
		if at := findKeywordIndex(projection, "WHERE"); at > 0 {
			request.after = strings.TrimSpace(projection[at+len("WHERE"):])
			projection = strings.TrimSpace(projection[:at])
			if request.after == "" {
				return nil, false
			}
		}
		projection, _ = cutDistinct(projection)
		cosineItems = e.parseReturnItems(projection)
		if len(cosineItems) < 2 || !withProjectionContainsVariable(cosineItems, request.pattern.relVar) {
			return nil, false
		}
	}
	request.cosine, request.property, request.query, request.score, ok = parseCosineReturnShape(cosineItems, request.pattern.relVar)
	if !ok || (request.projection && request.score == "") {
		return nil, false
	}
	if request.projection {
		// This route preserves the matched entities and their score. Extra
		// entity pass-throughs (including endpoints) remain index-capable;
		// renames and computed bindings belong to the projection owner.
		for index, item := range cosineItems {
			if index == request.cosine {
				continue
			}
			variable := strings.TrimSpace(item.expr)
			if item.column() != variable || (variable != request.pattern.relVar && variable != request.pattern.leftVar && variable != request.pattern.rightVar) {
				return nil, false
			}
		}
		for _, item := range request.items {
			if strings.TrimSpace(item.expr) != request.score && referencesVariable(item.expr, request.score) {
				return nil, false
			}
		}
	}
	request.descending, ok = parseOrderByScoreDirection(strings.TrimSpace(text[order+len("ORDER BY"):limit]), request.score)
	if !ok {
		return nil, false
	}
	request.limit, ok = parseFastPathLimit(ctx, strings.TrimSpace(text[limit+len("LIMIT"):]))
	if !ok || request.limit < 0 {
		return nil, false
	}
	request.index, ok = findCosineVectorIndexName(e.storage.GetSchema(), request.pattern.relType, request.property, storage.ConstraintEntityRelationship)
	return request, ok
}

// Availability is a live condition, not evidence of an index lookup. ASC needs
// exhaustive scoring; missing/unpopulated services use the same scan kernel.
func (run relationshipAccessExecution) vectorChoice(request *relationshipVectorRequest, service *search.Service) *relationshipVectorAccess {
	choice := &relationshipVectorAccess{relType: request.pattern.relType, property: request.property, estimatedRows: 100, route: "scan", reason: "search service unavailable"}
	switch {
	case request.limit == 0:
		choice.reason = "zero limit"
		choice.route = "empty"
		choice.estimatedRows = 0
	case !request.descending:
		choice.reason = "ascending order requires exhaustive scoring"
	case service == nil:
	case !service.HasRelationshipVectorEntries(choice.relType, choice.property):
		choice.reason = "relationship vector index unpopulated"
	default:
		choice.route = "index"
		choice.index = request.index
		choice.estimatedRows = 10
		choice.reason = "runtime vector resolution and successful index query required; otherwise scan continuation"
	}
	return choice
}

func (run relationshipAccessExecution) executeVector(ctx context.Context, request *relationshipVectorRequest) (*ExecuteResult, bool) {
	e := run.executor
	columns := make([]string, len(request.items))
	for i, item := range request.items {
		columns[i] = item.column()
	}
	// Resolve the database's current generation once for this physical attempt.
	// Availability and execution must use the same service, including managed
	// executors whose search service is replaced after a reset.
	service := e.search()
	choice := run.vectorChoice(request, service)
	if request.limit == 0 {
		recordRelationshipVectorAccess(ctx, run.pattern(), run.where(), run.access.variable, choice, 0, 0, 0)
		return &ExecuteResult{Columns: columns, Rows: [][]interface{}{}, Stats: &QueryStats{}}, true
	}
	// Neither query-vector resolution nor score-predicate evaluation is reachable
	// from vectorPlan. In particular, COLLECT bodies run only during execution.
	scoreOp, scoreValue := "", float64(0)
	if request.after != "" {
		var ok bool
		scoreOp, scoreValue, ok = e.parseScoreComparisonPredicate(run.statement(), ctx, request.after, request.score)
		if !ok {
			return nil, false
		}
	}
	vec, ok := e.cosineQueryVectorValue(ctx, e.evaluateExpressionWithContext(run.statement(), ctx, request.query, nil, nil))
	if !ok || len(vec) == 0 {
		return nil, false
	}
	limit := chooseVectorCandidateLimit(request.limit, request.before != "" || scorePredicateRejectsLeadingResults(request.descending, scoreOp))
	hits, completed, candidates, lookups := run.vectorScores(ctx, request, choice, vec, limit, service)
	if !completed {
		return nil, false
	}
	rows := make([][]interface{}, 0, util.SafePreallocCap(len(hits)))
	nodes := make(map[string]*storage.Node, 2)
	edges := make(map[string]*storage.Edge, 1)
	for _, hit := range hits {
		edge := hit.edge
		if edge == nil && hit.edgeID != "" {
			var err error
			edge, err = e.storage.GetEdge(hit.edgeID)
			if err != nil || edge == nil {
				continue
			}
		}
		if !e.buildRelationshipContextInto(request.pattern, edge, nodes, edges) {
			continue
		}
		if request.before != "" && !evaluateExpressionBoolWithContext(run.statement(), e, ctx, request.before, nodes, edges) {
			continue
		}
		if scoreOp != "" && !compareScore(hit.score, scoreOp, scoreValue) {
			continue
		}
		row := make([]interface{}, len(request.items))
		for i, item := range request.items {
			if (!request.projection && i == request.cosine) || (request.projection && strings.EqualFold(strings.TrimSpace(item.expr), request.score)) {
				row[i] = hit.score
			} else {
				row[i] = e.evaluateExpressionWithContext(run.statement(), ctx, item.expr, nodes, edges)
			}
		}
		rows = append(rows, row)
		if len(rows) >= request.limit {
			break
		}
	}
	// The descriptor is published only after the chosen physical read completed.
	recordRelationshipVectorAccess(ctx, run.pattern(), run.where(), run.access.variable, choice, lookups, candidates, int64(len(rows)))
	if choice.route == "index" {
		e.markCosineVectorIndexFastPathUsed()
	}
	return &ExecuteResult{Columns: columns, Rows: rows, Stats: &QueryStats{}}, true
}

func (run relationshipAccessExecution) vectorScores(ctx context.Context, request *relationshipVectorRequest, choice *relationshipVectorAccess, vec []float32, limit int, service *search.Service) ([]vectorEdgeScore, bool, int64, int64) {
	e := run.executor
	lookups := int64(0)
	if choice.route == "index" {
		lookups = 1
		hits, err := service.VectorQueryRelationships(ctx, vec, search.RelationshipVectorQuerySpec{IndexName: request.index, Type: request.pattern.relType, Property: request.property, Similarity: "cosine", Limit: limit})
		if err == nil {
			out := make([]vectorEdgeScore, 0, util.SafePreallocCap(len(hits)))
			for _, hit := range hits {
				out = append(out, vectorEdgeScore{edgeID: storage.EdgeID(hit.ID), score: hit.Score})
			}
			return out, true, int64(len(hits)), lookups
		}
		// Preserve exact-scoring continuation after a runtime index failure. Do
		// not retain its failed seek descriptor or hide the attempted lookup.
		choice.route = "scan"
		choice.index = ""
		choice.estimatedRows = 100
		choice.reason = "runtime vector index error; exact scan continuation"
		choice.failedIndex = request.index
		choice.indexError = err.Error()
	}
	edges, err := e.storage.AllEdges()
	if err != nil {
		return nil, false, 0, lookups
	}
	out := make([]vectorEdgeScore, 0, util.SafePreallocCap(len(edges)))
	for _, edge := range edges {
		if edge.Type != request.pattern.relType {
			continue
		}
		if score, ok := scoreVectorForFastPath(vec, toFloat32Slice(edge.Properties[request.property]), "cosine"); ok {
			out = append(out, vectorEdgeScore{edge: edge, score: score})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if request.descending {
			return out[i].score > out[j].score
		}
		return out[i].score < out[j].score
	})
	if len(out) > limit {
		out = out[:limit]
	}
	return out, true, int64(len(edges)), lookups
}

// Projection consumes the owner's choice/outcome. A possible seek in EXPLAIN
// is explicitly conditional and always shows its runtime scan continuation.
// PROFILE instead projects only the completed route and its measured counters.
func relationshipVectorOperator(access *relationshipVectorAccess, variable string, occurrences int64, stats *relationshipSeekStats, profile bool) *PlanOperator {
	operator := &PlanOperator{OperatorType: "Expand", Description: "Exact relationship vector score scan", Identifiers: []string{variable}, EstimatedRows: access.estimatedRows, Arguments: map[string]interface{}{"relationshipType": access.relType, "property": access.property, "accessReason": access.reason}}
	if access.route == "index" {
		operator.OperatorType = "RelationshipVectorIndexSeek"
		operator.Description = "Relationship vector index access"
		operator.Arguments["index"] = access.index
	} else if access.route == "empty" {
		operator.OperatorType = "EmptyResult"
		operator.Description = "Zero limit; no relationship access"
	}
	if profile {
		operator.Arguments["accessAttempts"] = occurrences
		if access.indexError != "" {
			operator.Arguments["failedIndex"] = access.failedIndex
			operator.Arguments["indexError"] = access.indexError
		}
		if stats != nil {
			operator.ActualRows = stats.rows
			operator.DBHits = stats.lookups + stats.candidates
			operator.Arguments["indexLookups"] = stats.lookups
			operator.Arguments["relationshipCandidates"] = stats.candidates
			if access.route == "scan" {
				operator.Arguments["scanRelationships"] = stats.candidates
			}
		}
		return operator
	}
	operator.Arguments["accessOccurrences"] = occurrences
	if access.route != "index" {
		return operator
	}
	operator.Arguments["conditional"] = true
	scan := &PlanOperator{OperatorType: "Expand", Description: "Runtime vector index failure or unsupported query vector: scan continuation", Identifiers: []string{variable}, EstimatedRows: 100, Arguments: map[string]interface{}{"conditional": true}}
	return &PlanOperator{OperatorType: "RelationshipAccessChoice", Description: access.reason, EstimatedRows: 100, Children: []*PlanOperator{operator, scan}}
}
