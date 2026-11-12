// Package cypher implements EXPLAIN and PROFILE query execution modes.
//
// EXPLAIN shows the query execution plan without executing the query.
// PROFILE executes the query and shows the plan with runtime statistics.
//
// # ELI12 (Explain Like I'm 12)
//
// Imagine you're planning a trip. EXPLAIN is like looking at the map and saying
// "I'll take this road, then that highway" without actually driving.
// PROFILE is like actually driving the route and noting "that road took 10 minutes,
// the highway took 20 minutes, I passed 50 cars."
//
// # Neo4j Compatibility
//
// This implementation matches Neo4j's execution modes:
// - EXPLAIN: Returns plan without execution
// - PROFILE: Returns plan with actual execution statistics
package cypher

import (
	"context"
	"fmt"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"sort"
	"strings"
	"time"

	"github.com/orneryd/nornicdb/pkg/localization"
)

// ExecutionMode represents how a query should be executed
type ExecutionMode string

const (
	ModeNormal  ExecutionMode = "normal"
	ModeExplain ExecutionMode = "EXPLAIN"
	ModeProfile ExecutionMode = "PROFILE"
)

// PlanOperator represents a single operator in the execution plan
type PlanOperator struct {
	// Operator type (e.g., "NodeByLabelScan", "Filter", "Expand")
	OperatorType string `json:"operatorType"`

	// Human-readable description
	Description string `json:"description"`

	// Operator-specific arguments
	Arguments map[string]interface{} `json:"arguments,omitempty"`

	// Variables introduced by this operator
	Identifiers []string `json:"identifiers,omitempty"`

	// Child operators (execution flows bottom-up)
	Children []*PlanOperator `json:"children,omitempty"`

	// Cost estimation (for EXPLAIN and PROFILE)
	EstimatedRows int64 `json:"estimatedRows"`

	// Actual statistics (only for PROFILE)
	ActualRows int64         `json:"rows,omitempty"`
	DBHits     int64         `json:"dbHits,omitempty"`
	Time       time.Duration `json:"time,omitempty"`
	// Observed flags distinguish a measured zero from an unavailable counter.
	RowsObserved   bool `json:"rowsObserved,omitempty"`
	DBHitsObserved bool `json:"dbHitsObserved,omitempty"`
}

// ExecutionPlan represents the complete query execution plan
type ExecutionPlan struct {
	// Root operator of the plan
	Root *PlanOperator `json:"root"`

	// Query being explained/profiled
	Query string `json:"query"`

	// Execution mode (EXPLAIN or PROFILE)
	Mode ExecutionMode `json:"mode"`

	// Total statistics (only for PROFILE)
	TotalDBHits int64         `json:"totalDbHits,omitempty"`
	TotalTime   time.Duration `json:"totalTime,omitempty"`
	TotalRows   int64         `json:"totalRows,omitempty"`
	// Unobserved identifies execution boundaries without captured measurements.
	// Such boundaries must never be populated with planner estimates.
	Unobserved []string `json:"unobserved,omitempty"`
	// TotalDBHitsObserved is false when an actual callback's accesses cannot
	// be fully measured, even if some of its nested readers were observed.
	TotalDBHitsObserved bool `json:"totalDbHitsObserved,omitempty"`
}

// parseExecutionMode extracts EXPLAIN or PROFILE prefix from a query
func parseExecutionMode(query string) (ExecutionMode, string) {
	mode, statement, _ := readExecutionModes(query, ModeNormal)
	return mode, statement
}

// executeExplain returns the execution plan without executing the query
func (e *StorageExecutor) executeExplain(ctx context.Context, query string) (*ExecuteResult, error) {
	query, err := executionModeStatement(ModeExplain, query)
	if err != nil {
		return nil, err
	}
	if endsInFinish(ctx, query) {
		query += " FINISH"
	}
	// Catalog commands are validated by their typed command preparation; graph
	// body composition has no clauses to validate in their native DDL syntax.
	if !isMetadataCommand(ctx, query, metadataCatalogExecution) {
		if err := validateQueryBodyComposition(ctx, query); err != nil {
			return nil, err
		}
	}
	columns, err := e.StatementResultColumnsContext(ctx, query, AcceptedSource)

	if err != nil {
		return nil, err
	}
	if indexASCIIFold(query, "shortestpath") >= 0 {
		if err := e.validateMatchSemanticScopes(ctx, query, cypherVersionFromContext(ctx) == "25"); err != nil {
			return nil, err
		}
	}
	plan, err := e.buildExecutionPlan(ctx, query)
	if err != nil {
		return nil, localizedError(localization.CypherCoreExecutionPlanBuildFailed(err), err)
	}
	plan.Mode = ModeExplain
	result := &ExecuteResult{
		Columns: columns,
		Rows:    [][]interface{}{},
		Stats:   &QueryStats{},
	}
	return e.attachPlanMetadata(result, plan), nil
}

// executeProfile executes the query and returns the plan with statistics
func (e *StorageExecutor) executeProfile(ctx context.Context, query string) (*ExecuteResult, error) {
	query, err := executionModeStatement(ModeProfile, query)
	if err != nil {
		return nil, err
	}
	if endsInFinish(ctx, query) {
		query += " FINISH"
	}
	// Resolve participant signatures through the same owner as EXPLAIN and
	// planning. Incomplete metadata cannot authorize execution effects.
	columns, err := e.StatementResultColumnsContext(ctx, query, AcceptedSource)
	if err != nil {
		return nil, err
	}
	// PROFILE's inner execution must bypass the result cache on both read and
	// write: Neo4j profiles actual execution, and a result object shared with
	// the cache would otherwise be mutated by attachPlanMetadata below,
	// leaking plan metadata into ordinary cached reads of the same query.
	ctx = context.WithValue(ctx, profileExecutionKey{}, true)
	collector := newProfileRuntimeCollector()
	ctx = withProfileRuntimeCollector(ctx, collector)
	contract, contractErr := e.analyzer.Analyze(ctx, query)
	if contractErr != nil {
		return nil, contractErr
	}
	// Execute the actual query to collect statistics
	startTime := time.Now()
	result, execErr := e.executeRouted(ctx, query, nil)
	totalTime := time.Since(startTime)

	// A statement that fails fails its PROFILE too, as in Neo4j: the error
	// is the statement's, not a plan row describing it.
	if execErr != nil {
		return nil, execErr
	}

	// A write whose route is outside the clause pipeline - ordinary schema
	// DDL, a catalog or administration command, a registered procedure -
	// completes its work where nothing reports reads or mutation requests, so
	// its storage accesses stay unknown as a whole. A statement the pipeline
	// executed by clauses reports both, so its own records certify it.
	if contract.Writes() && !profileExecutedClauses(collector) {
		collector.UnobservedRead(ctx, "write statement storage accesses", "")
	}

	plan := observedExecutionPlan(query, result, totalTime, collector)

	// TRC-16: emit per-operator spans with estimated/actual rows.
	emitOperatorSpans(ctx, plan.Root)

	if result == nil {
		result = &ExecuteResult{
			Columns: []string{},
			Rows:    [][]interface{}{},
			Stats:   &QueryStats{},
		}
	}
	if err := verifyResultColumns(result.Columns, columns); err != nil {
		return nil, err
	}

	return e.attachPlanMetadata(result, plan), nil
}

// profileExecutedClauses reports whether the statement ran clauses the
// collector observed. A route outside the clause pipeline runs none, so its
// accesses can never be certified by the statement's own records.
func profileExecutedClauses(collector *profileRuntimeCollector) bool {
	return len(collector.observation().Clauses) > 0
}

// buildExecutionPlan creates an execution plan for a query
func (e *StorageExecutor) buildExecutionPlan(ctx context.Context, query string) (*ExecutionPlan, error) {
	plan := &ExecutionPlan{
		Query: query,
		Mode:  ModeNormal,
	}

	// Analyze query and build operator tree
	root, err := e.analyzeQuery(ctx, query)
	if err != nil {
		return nil, err
	}
	plan.Root = root

	return plan, nil
}

// analyzeQuery analyzes a Cypher query and builds the operator tree
func (e *StorageExecutor) analyzeQuery(ctx context.Context, query string) (*PlanOperator, error) {
	// Build operators in query clause order (Neo4j-style plan chain).
	type plannedClause struct {
		idx int
		op  *PlanOperator
	}
	clauses := make([]plannedClause, 0, 12)

	add := func(idx int, op *PlanOperator) {
		if idx >= 0 && op != nil {
			clauses = append(clauses, plannedClause{idx: idx, op: op})
		}
	}

	if idx := findKeywordIndex(ctx, query, "MATCH"); idx >= 0 {
		op, err := e.analyzeMatchClause(ctx, query)
		if err != nil {
			return nil, err
		}
		add(idx, op)
	}
	if idx := findKeywordIndex(ctx, query, "CALL"); idx >= 0 {
		add(idx, e.analyzeCallClause(ctx, query))
	}
	if idx := findKeywordIndex(ctx, query, "WHERE"); idx >= 0 {
		add(idx, e.analyzeWhereClause(ctx, query))
	}
	if idx := findKeywordIndex(ctx, query, "WITH"); idx >= 0 {
		add(idx, &PlanOperator{
			OperatorType:  "Projection",
			Description:   "Project intermediate results",
			EstimatedRows: 100,
		})
	}
	if idx := findKeywordIndex(ctx, query, "CREATE"); idx >= 0 {
		add(idx, &PlanOperator{
			OperatorType:  "Create",
			Description:   "Create nodes and relationships",
			EstimatedRows: 1,
		})
	}
	if idx := findKeywordIndex(ctx, query, "MERGE"); idx >= 0 {
		add(idx, &PlanOperator{
			OperatorType:  "Merge",
			Description:   "Merge (create if not exists)",
			EstimatedRows: 1,
		})
	}
	if idx := findKeywordIndex(ctx, query, "SET"); idx >= 0 {
		add(idx, &PlanOperator{
			OperatorType:  "Set",
			Description:   "Set properties",
			EstimatedRows: 100,
		})
	}
	if idx := findKeywordIndex(ctx, query, "REMOVE"); idx >= 0 {
		add(idx, &PlanOperator{
			OperatorType:  "Remove",
			Description:   "Remove labels/properties",
			EstimatedRows: 100,
		})
	}
	if idx := findKeywordIndex(ctx, query, "DETACH DELETE"); idx >= 0 {
		add(idx, e.analyzeDeleteClause(ctx, query, true))
	} else if idx := findKeywordIndex(ctx, query, "DELETE"); idx >= 0 {
		add(idx, e.analyzeDeleteClause(ctx, query, false))
	}
	if idx := findKeywordIndex(ctx, query, "ORDER BY"); idx >= 0 {
		add(idx, &PlanOperator{
			OperatorType:  "Sort",
			Description:   "Sort results",
			EstimatedRows: 100,
		})
	}
	if idx := findKeywordIndex(ctx, query, "LIMIT"); idx >= 0 {
		op, err := e.analyzeLimitSkip(ctx, query)
		if err != nil {
			return nil, err
		}
		add(idx, op)
	} else if idx := findKeywordIndex(ctx, query, "SKIP"); idx >= 0 {
		op, err := e.analyzeLimitSkip(ctx, query)
		if err != nil {
			return nil, err
		}
		add(idx, op)
	}
	if idx := findKeywordIndex(ctx, query, "RETURN"); idx >= 0 {
		add(idx, e.analyzeReturnClause(ctx, query))
	}

	sort.SliceStable(clauses, func(i, j int) bool {
		return clauses[i].idx < clauses[j].idx
	})

	operators := make([]*PlanOperator, 0, len(clauses))
	for _, c := range clauses {
		operators = append(operators, c.op)
	}

	// Build the operator tree (chain operators bottom-up)
	if len(operators) == 0 {
		return &PlanOperator{
			OperatorType:  "EmptyResult",
			Description:   "No operations",
			EstimatedRows: 0,
		}, nil
	}

	// Chain operators: each operator is a child of the next
	for i := len(operators) - 1; i > 0; i-- {
		operators[i].Children = []*PlanOperator{operators[i-1]}
	}

	return operators[len(operators)-1], nil
}

// analyzeMatchClause analyzes a MATCH clause and returns appropriate operators
func (e *StorageExecutor) analyzeMatchClause(ctx context.Context, query string) (*PlanOperator, error) {
	upper := upperASCII(query)

	// Check for shortestPath
	if strings.Contains(upper, "SHORTESTPATH") {
		return &PlanOperator{
			OperatorType:  "ShortestPath",
			Description:   "Find shortest path using BFS",
			EstimatedRows: 1,
			Arguments: map[string]interface{}{
				"algorithm": "BFS",
			},
		}, nil
	}

	// Check for variable-length path
	if containsVariableLengthPathPattern(query) {
		return &PlanOperator{
			OperatorType:  "VarLengthExpand",
			Description:   "Variable length path expansion",
			EstimatedRows: 100,
		}, nil
	}

	// Check for relationship pattern
	if containsRelationshipPattern(query) {
		scan, err := e.analyzeNodeScan(ctx, query)
		if err != nil {
			return nil, err
		}
		return &PlanOperator{
			OperatorType:  "Expand",
			Description:   "Expand relationships",
			EstimatedRows: 100,
			Children: []*PlanOperator{
				scan,
			},
		}, nil
	}

	// Simple node scan
	return e.analyzeNodeScan(ctx, query)
}

// analyzeNodeScan determines the type of node scan needed and the index
// diagnostics that accompany it.
//
// An index seek is claimed only when a canonical index-seek planner plans one
// for the MATCH's first node pattern: parseNodePattern reads the pattern, and
// planPatternPropertySeek (inline equality) and
// addPropertyIndexEqualityCompoundSeeks (WHERE equality) decide whether a
// declared index answers it. Otherwise the plan scans and the arguments name
// the indexes that exist and why none applies. Reading the plan performs no
// statement read.
func (e *StorageExecutor) analyzeNodeScan(ctx context.Context, query string) (*PlanOperator, error) {
	pattern, whereClause, ok, err := e.explainNodePattern(ctx, query)
	if err != nil {
		return nil, err
	}
	if !ok || len(pattern.labels) == 0 {
		// No label - full scan
		return &PlanOperator{
			OperatorType:  "AllNodesScan",
			Description:   "Scan all nodes",
			EstimatedRows: 10000,
			Identifiers:   []string{"n"},
		}, nil
	}

	planned, err := e.explainPlansNodeIndexSeek(ctx, pattern, whereClause)
	if err != nil {
		return nil, err
	}

	label := pattern.labels[0]
	args := map[string]interface{}{
		"label": label,
	}
	e.annotateIndexDiagnostics(args, label, query, planned)
	if planned {
		return &PlanOperator{
			OperatorType:  "NodeIndexSeek",
			Description:   fmt.Sprintf("Index seek on :%s", label),
			EstimatedRows: 10,
			Arguments:     args,
			Identifiers:   []string{"n"},
		}, nil
	}

	return &PlanOperator{
		OperatorType:  "NodeByLabelScan",
		Description:   fmt.Sprintf("Scan all :%s nodes", label),
		EstimatedRows: 1000,
		Arguments:     args,
		Identifiers:   []string{"n"},
	}, nil
}

// explainNodePattern reads the MATCH pattern through its canonical syntax
// owner. Only literals and supplied parameters become planning values:
// EXPLAIN must never execute a user function to discover an access path.
// Expressions requiring execution remain runtime-dependent, not assumed values.
func (e *StorageExecutor) explainNodePattern(ctx context.Context, query string) (nodePatternInfo, string, bool, error) {
	matchClause := extractMatchClause(ctx, query)
	if matchClause == "" {
		return nodePatternInfo{}, "", false, nil
	}
	parts := e.splitNodePatterns(strings.TrimSpace(matchClause[len("MATCH"):]))
	if len(parts) == 0 {
		return nodePatternInfo{}, "", false, nil
	}
	pattern, err := readNodePattern(ctx, parts[0], func(ctx context.Context, source string, _ bool) (map[string]interface{}, error) {
		expressions, err := parsePropertyExpressions(source)
		if err != nil {
			return nil, err
		}
		values := make(map[string]interface{}, len(expressions))
		params := getParamsFromContext(ctx)
		for _, expression := range expressions {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if value, known := rangeBoundValue(expression.expr, params); known {
				values[expression.key] = value
			}
		}
		return values, nil
	})
	if err != nil {
		return nodePatternInfo{}, "", false, err
	}
	whereClause, _ := explainWhereClause(ctx, query)
	return pattern, whereClause, true, nil
}

// explainPlansNodeIndexSeek reports whether a canonical index-seek planner
// plans a point seek for pattern: its inline equality or the MATCH's WHERE
// equality on an indexed property. It plans without reading index postings,
// so EXPLAIN reports applicability without estimating or executing.
func (e *StorageExecutor) explainPlansNodeIndexSeek(ctx context.Context, pattern nodePatternInfo, whereClause string) (bool, error) {
	if _, planned, err := e.planPatternPropertySeek(ctx, pattern, e.storage); err != nil {
		return false, err
	} else if planned {
		return true, nil
	}
	var seeks nodeIndexSeekSet
	if err := e.addPropertyIndexEqualityCompoundSeeks(ctx, pattern, whereClause, &seeks); err != nil {
		return false, err
	}
	return seeks.n > 0, nil
}

// annotateIndexDiagnostics adds index usage/rejection diagnostics to the plan
// operator arguments. This makes tuning deterministic by exposing whether an
// index was available, which properties are indexed, and why it was rejected.
// seekPlanned reports whether a canonical index-seek planner planned a seek
// for the pattern; only then is the index status "available".
func (e *StorageExecutor) annotateIndexDiagnostics(args map[string]interface{}, label, query string, seekPlanned bool) {
	schema := e.storage.GetSchema()
	if schema == nil {
		args["indexStatus"] = "no_schema"
		return
	}

	// Collect all indexes for this label.
	indexes := schema.GetIndexes()
	var labelIndexes []string
	for _, raw := range indexes {
		idx, ok := raw.(map[string]interface{})
		if !ok {
			continue
		}
		idxLabel, _ := idx["label"].(string)
		if strings.TrimSpace(idxLabel) != label {
			continue
		}
		prop := ""
		if p, ok := idx["property"].(string); ok {
			prop = p
		}
		if prop == "" {
			if props, ok := idx["properties"].([]string); ok && len(props) > 0 {
				prop = strings.Join(props, ",")
			}
		}
		idxType, _ := idx["type"].(string)
		labelIndexes = append(labelIndexes, fmt.Sprintf("%s(%s)", idxType, prop))
	}

	if len(labelIndexes) == 0 {
		args["indexStatus"] = "no_index_for_label"
		return
	}

	if seekPlanned {
		args["indexStatus"] = "available"
	} else {
		args["indexStatus"] = "no_applicable_index"
	}
	args["availableIndexes"] = labelIndexes

	// Check WHERE clause for predicate shapes that prevent index use.
	upper := upperASCII(query)
	if strings.Contains(upper, "COALESCE(") {
		args["indexRejectionRisk"] = "function_wrapping (coalesce)"
	}
	if strings.Contains(upper, "TOLOWER(") || strings.Contains(upper, "TOUPPER(") {
		args["indexRejectionRisk"] = "function_wrapping (toLower/toUpper)"
	}
}

// explainWhereClause returns the statement's WHERE body and whether the
// statement has a WHERE clause.
func explainWhereClause(ctx context.Context, query string) (string, bool) {
	whereIdx := findKeywordIndex(ctx, query, "WHERE")
	if whereIdx < 0 {
		return "", false
	}

	// Find end of WHERE clause
	endIdx := len(query)
	for _, keyword := range []string{"RETURN", "ORDER", "LIMIT", "SKIP", "WITH", "CREATE", "SET", "REMOVE", "DETACH DELETE", "DELETE"} {
		if idx := findKeywordIndex(ctx, query[whereIdx:], keyword); idx > 0 {
			if whereIdx+idx < endIdx {
				endIdx = whereIdx + idx
			}
		}
	}

	return strings.TrimSpace(query[whereIdx+5 : endIdx]), true
}

// analyzeWhereClause analyzes WHERE conditions
func (e *StorageExecutor) analyzeWhereClause(ctx context.Context, query string) *PlanOperator {
	whereClause, ok := explainWhereClause(ctx, query)
	if !ok {
		return nil
	}

	return &PlanOperator{
		OperatorType:  "Filter",
		Description:   fmt.Sprintf("Filter: %s", truncate(whereClause, 50)),
		EstimatedRows: 100,
		Arguments: map[string]interface{}{
			"predicate": whereClause,
		},
	}
}

func (e *StorageExecutor) analyzeDeleteClause(ctx context.Context, query string, detach bool) *PlanOperator {
	startKeyword := "DELETE"
	operator := "Delete"
	if detach {
		startKeyword = "DETACH DELETE"
		operator = "DetachDelete"
	}
	deleteIdx := findKeywordIndex(ctx, query, startKeyword)
	if deleteIdx < 0 {
		return &PlanOperator{
			OperatorType:  operator,
			Description:   "Delete matched entities",
			EstimatedRows: 100,
		}
	}

	endIdx := len(query)
	for _, keyword := range []string{"RETURN", "WITH", "ORDER BY", "LIMIT", "SKIP"} {
		if idx := findKeywordIndex(ctx, query[deleteIdx:], keyword); idx > 0 {
			candidate := deleteIdx + idx
			if candidate < endIdx {
				endIdx = candidate
			}
		}
	}

	deleteClause := strings.TrimSpace(query[deleteIdx:endIdx])
	strip := startKeyword + " "
	if strings.HasPrefix(upperASCII(deleteClause), strip) {
		deleteClause = strings.TrimSpace(deleteClause[len(strip):])
	}
	desc := "Delete matched entities"
	if deleteClause != "" {
		desc = fmt.Sprintf("%s %s", operator, truncate(deleteClause, 50))
	}

	return &PlanOperator{
		OperatorType:  operator,
		Description:   desc,
		EstimatedRows: 100,
		Arguments: map[string]interface{}{
			"details": deleteClause,
		},
	}
}

// analyzeReturnClause analyzes the RETURN clause
func (e *StorageExecutor) analyzeReturnClause(ctx context.Context, query string) *PlanOperator {
	returnIdx := findKeywordIndex(ctx, query, "RETURN")
	if returnIdx < 0 {
		return nil
	}

	// Extract RETURN items
	returnClause := query[returnIdx+6:]
	// Trim ORDER BY, LIMIT, etc.
	for _, keyword := range []string{"ORDER BY", "LIMIT", "SKIP"} {
		if idx := findKeywordIndex(ctx, returnClause, keyword); idx > 0 {
			returnClause = returnClause[:idx]
		}
	}
	returnClause = strings.TrimSpace(returnClause)

	// Check for aggregations
	hasAggregation := containsAggregateFunc(returnClause)

	if hasAggregation {
		return &PlanOperator{
			OperatorType:  "EagerAggregation",
			Description:   "Aggregate results",
			EstimatedRows: 1,
			Arguments: map[string]interface{}{
				"expressions": returnClause,
			},
		}
	}

	// Check for DISTINCT
	if _, distinct := cutDistinct(returnClause); distinct {
		return &PlanOperator{
			OperatorType:  "Distinct",
			Description:   "Remove duplicates",
			EstimatedRows: 100,
		}
	}

	return &PlanOperator{
		OperatorType:  "ProduceResults",
		Description:   "Return results",
		EstimatedRows: 100,
		Arguments: map[string]interface{}{
			"columns": returnClause,
		},
	}
}

func containsVariableLengthPathPattern(query string) bool {
	for i := 0; i < len(query); i++ {
		if query[i] != '[' {
			continue
		}
		inside, _, ok := extractBracketSection(query[i:])
		if !ok {
			continue
		}
		inner := strings.TrimSpace(inside)
		if !strings.HasPrefix(inner, "*") {
			continue
		}
		rangeExpr := strings.TrimSpace(inner[1:])
		if rangeExpr == "" {
			return true // [*]
		}
		dotIdx := strings.Index(rangeExpr, "..")
		if dotIdx < 0 {
			continue
		}
		left := strings.TrimSpace(rangeExpr[:dotIdx])
		right := strings.TrimSpace(rangeExpr[dotIdx+2:])
		if (left == "" || isAllDigits(left)) && (right == "" || isAllDigits(right)) {
			return true // [*1..3], [*..5], [*1..]
		}
	}
	return false
}

func isAllDigits(s string) bool {
	token, ok := cyphertext.ScanNumber(s, 0)
	return ok && !token.Invalid && token.End == len(s) && token.Base == 10 && !token.Fraction && !token.Exponent && !token.Grouped && token.Suffix == 0
}

// analyzeLimitSkip analyzes LIMIT and SKIP clauses
func (e *StorageExecutor) analyzeLimitSkip(ctx context.Context, query string) (*PlanOperator, error) {
	upper := upperASCII(query)

	op := &PlanOperator{
		OperatorType: "Limit",
		Arguments:    make(map[string]interface{}),
	}

	// Extract LIMIT value
	limit, err := ExtractLimitString(ctx, query)
	if err != nil {
		return nil, err
	}
	if limit != "" {
		op.Arguments["limit"] = limit
		op.Description = fmt.Sprintf("Limit to %s rows", limit)
	}

	// Extract SKIP value
	skip, err := ExtractSkipString(ctx, query)
	if err != nil {
		return nil, err
	}
	if skip != "" {
		op.Arguments["skip"] = skip
		if op.Description != "" {
			op.Description += fmt.Sprintf(", skip %s", skip)
		} else {
			op.Description = fmt.Sprintf("Skip %s rows", skip)
		}
	}

	// Estimate rows based on limit
	if _, ok := op.Arguments["limit"]; ok {
		op.EstimatedRows = 10 // Use limit as estimate
	} else if strings.Contains(upper, "SKIP") {
		op.EstimatedRows = 100
	}

	return op, ctx.Err()
}

// analyzeCallClause analyzes CALL procedure invocations
func (e *StorageExecutor) analyzeCallClause(ctx context.Context, query string) *PlanOperator {
	procName := "unknown"
	if callIdx := keywordIndexFrom(ctx, query, "CALL", 0, defaultKeywordScanOpts()); callIdx >= 0 {
		callEnd, ok := keywordSpanAt(query, callIdx, "CALL")
		if ok {
			rest := strings.TrimSpace(query[callEnd:])
			if name, _, ok := parseProcedureNameToken(rest); ok {
				procName = name
			}
		}
	}

	return &PlanOperator{
		OperatorType:  "ProcedureCall",
		Description:   fmt.Sprintf("Call %s", procName),
		EstimatedRows: 100,
		Arguments: map[string]interface{}{
			"procedure": procName,
		},
	}
}

func parseProcedureNameToken(s string) (string, string, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return "", "", false
	}
	start := 0
	for start < len(s) {
		_, rest, ok := parseIdentifierToken(s[start:])
		if !ok {
			return "", "", false
		}
		start = len(s) - len(rest)
		if strings.HasPrefix(rest, ".") {
			start++
			continue
		}
		return s[:start], rest, true
	}
	return "", "", false
}

// planToResult converts an execution plan to an ExecuteResult
func (e *StorageExecutor) attachPlanMetadata(result *ExecuteResult, plan *ExecutionPlan) *ExecuteResult {
	// Add plan as metadata
	if result.Metadata == nil {
		result.Metadata = make(map[string]interface{})
	}
	result.Metadata["planString"] = formatPlan(plan)
	result.Metadata["plan"] = plan
	result.Metadata["planType"] = string(plan.Mode)

	return result
}

// Neo4jPlanMap renders the plan tree in the shape Neo4j puts in Bolt SUCCESS
// metadata and HTTP results: each node carries operatorType, identifiers,
// args (with EstimatedRows), and children; the profile form adds rows and
// dbHits. Bolt and HTTP both deliver this map so plans reach clients (#744).
func Neo4jPlanMap(plan *ExecutionPlan, profile bool) map[string]any {
	if plan == nil {
		return nil
	}
	return neo4jPlanOperatorMap(plan.Root, profile)
}

// profileExecutionKey marks PROFILE's inner execution so it neither serves nor
// populates the result cache (#744): a cached result object would otherwise be
// mutated by attachPlanMetadata and hand plan metadata to ordinary cached
// reads of the same query.
type profileExecutionKey struct{}

func profileExecutionBypassesCache(ctx context.Context) bool {
	bypass, _ := ctx.Value(profileExecutionKey{}).(bool)
	return bypass
}

func neo4jPlanOperatorMap(op *PlanOperator, profile bool) map[string]any {
	if op == nil {
		return nil
	}
	node := make(map[string]any, 4)
	node["operatorType"] = op.OperatorType
	if len(op.Identifiers) > 0 {
		node["identifiers"] = op.Identifiers
	}
	args := make(map[string]interface{}, len(op.Arguments)+1)
	for key, value := range op.Arguments {
		args[key] = value
	}
	if op.EstimatedRows > 0 {
		args["EstimatedRows"] = op.EstimatedRows
	}
	if len(args) > 0 {
		node["args"] = args
	}
	if len(op.Children) > 0 {
		children := make([]map[string]any, 0, len(op.Children))
		for _, child := range op.Children {
			if childMap := neo4jPlanOperatorMap(child, profile); childMap != nil {
				children = append(children, childMap)
			}
		}
		node["children"] = children
	}
	if profile {
		if op.RowsObserved {
			node["rows"] = op.ActualRows
		}
		if op.DBHitsObserved {
			node["dbHits"] = op.DBHits
		}
	}
	return node
}

// formatPlan formats the execution plan as a string (tree visualization)
func formatPlan(plan *ExecutionPlan) string {
	var sb strings.Builder

	sb.WriteString(fmt.Sprintf("+-%s-+\n", strings.Repeat("-", 60)))
	sb.WriteString(fmt.Sprintf("| %-60s |\n", fmt.Sprintf("%s %s", plan.Mode, "Query Plan")))
	sb.WriteString(fmt.Sprintf("+-%s-+\n", strings.Repeat("-", 60)))

	if plan.Mode == ModeProfile {
		sb.WriteString(fmt.Sprintf("| Total Time: %-47s |\n", plan.TotalTime.String()))
		sb.WriteString(fmt.Sprintf("| Total Rows: %-47d |\n", plan.TotalRows))
		if plan.TotalDBHitsObserved {
			sb.WriteString(fmt.Sprintf("| Total DB Hits: %-44d |\n", plan.TotalDBHits))
		} else {
			sb.WriteString(fmt.Sprintf("| Total DB Hits: unobserved (recorded: %d) |\n", plan.TotalDBHits))
		}
		sb.WriteString(fmt.Sprintf("+-%s-+\n", strings.Repeat("-", 60)))
	}

	for _, reason := range plan.Unobserved {
		sb.WriteString(fmt.Sprintf("| Unobserved: %s |\n", reason))
	}
	formatOperator(&sb, plan.Root, 0, plan.Mode == ModeProfile)

	sb.WriteString(fmt.Sprintf("+-%s-+\n", strings.Repeat("-", 60)))

	return sb.String()
}

// formatOperator formats a single operator in the plan tree
func formatOperator(sb *strings.Builder, op *PlanOperator, depth int, showStats bool) {
	if op == nil {
		return
	}

	indent := strings.Repeat("  ", depth)
	prefix := "+-"
	if depth > 0 {
		prefix = "|" + indent + "+-"
	}

	// Operator line
	line := fmt.Sprintf("%s %s", prefix, op.OperatorType)
	if op.Description != "" && op.Description != op.OperatorType {
		line += fmt.Sprintf(" (%s)", truncate(op.Description, 40))
	}

	sb.WriteString(fmt.Sprintf("| %-60s |\n", line))

	// Statistics line for PROFILE mode
	if showStats {
		rows, hits := "unobserved", "unobserved"
		estimate := "unobserved"
		if op.EstimatedRows > 0 {
			estimate = fmt.Sprint(op.EstimatedRows)
		}
		if op.RowsObserved {
			rows = fmt.Sprint(op.ActualRows)
		}
		if op.DBHitsObserved {
			hits = fmt.Sprint(op.DBHits)
		}
		statsLine := fmt.Sprintf("%s|   Est: %s, Actual: %s, Hits: %s", indent, estimate, rows, hits)
		sb.WriteString(fmt.Sprintf("| %-60s |\n", statsLine))
	} else {
		// Just show estimates for EXPLAIN
		statsLine := fmt.Sprintf("%s|   Estimated Rows: %d", indent, op.EstimatedRows)
		sb.WriteString(fmt.Sprintf("| %-60s |\n", statsLine))
	}

	// Format children
	for _, child := range op.Children {
		formatOperator(sb, child, depth+1, showStats)
	}
}

// truncate truncates a string to maxLen characters
func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen-3] + "..."
}
