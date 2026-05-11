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
	"sort"
	"strings"
	"time"
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
}

// parseExecutionMode extracts EXPLAIN or PROFILE prefix from a query
func parseExecutionMode(query string) (ExecutionMode, string) {
	trimmed := strings.TrimSpace(query)
	upper := strings.ToUpper(trimmed)

	if strings.HasPrefix(upper, "EXPLAIN ") {
		return ModeExplain, strings.TrimSpace(trimmed[8:])
	}
	if strings.HasPrefix(upper, "PROFILE ") {
		return ModeProfile, strings.TrimSpace(trimmed[8:])
	}
	return ModeNormal, trimmed
}

// executeExplain returns the execution plan without executing the query
func (e *StorageExecutor) executeExplain(ctx context.Context, query string) (*ExecuteResult, error) {
	plan, err := e.buildExecutionPlan(query)
	if err != nil {
		return nil, fmt.Errorf("failed to build execution plan: %w", err)
	}
	plan.Mode = ModeExplain
	result := &ExecuteResult{
		Columns: e.inferExplainColumns(query),
		Rows:    [][]interface{}{},
		Stats:   &QueryStats{},
	}
	return e.attachPlanMetadata(result, plan), nil
}

// executeProfile executes the query and returns the plan with statistics
func (e *StorageExecutor) executeProfile(ctx context.Context, query string) (*ExecuteResult, error) {
	// Build the plan first
	plan, err := e.buildExecutionPlan(query)
	if err != nil {
		return nil, fmt.Errorf("failed to build execution plan: %w", err)
	}
	plan.Mode = ModeProfile

	// Execute the actual query to collect statistics
	startTime := time.Now()
	result, execErr := e.Execute(ctx, query, nil)
	totalTime := time.Since(startTime)

	// Even if execution fails, we return what we have
	if execErr != nil {
		plan.Root.Description += fmt.Sprintf(" (ERROR: %s)", execErr.Error())
	}

	// Update plan with actual statistics
	plan.TotalTime = totalTime
	if result != nil {
		plan.TotalRows = int64(len(result.Rows))
		e.updatePlanWithStats(plan.Root, result)
	}

	// Calculate total DB hits (estimate based on operations)
	plan.TotalDBHits = e.estimateDBHits(plan.Root)

	// TRC-16: emit per-operator spans with estimated/actual rows.
	emitOperatorSpans(ctx, plan.Root)

	if result == nil {
		result = &ExecuteResult{
			Columns: []string{},
			Rows:    [][]interface{}{},
			Stats:   &QueryStats{},
		}
	}
	return e.attachPlanMetadata(result, plan), nil
}

// buildExecutionPlan creates an execution plan for a query
func (e *StorageExecutor) buildExecutionPlan(query string) (*ExecutionPlan, error) {
	plan := &ExecutionPlan{
		Query: query,
		Mode:  ModeNormal,
	}

	// Analyze query and build operator tree
	root, err := e.analyzeQuery(query)
	if err != nil {
		return nil, err
	}
	plan.Root = root

	return plan, nil
}

// analyzeQuery analyzes a Cypher query and builds the operator tree
func (e *StorageExecutor) analyzeQuery(query string) (*PlanOperator, error) {
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

	if idx := findKeywordIndex(query, "MATCH"); idx >= 0 {
		add(idx, e.analyzeMatchClause(query))
	}
	if idx := findKeywordIndex(query, "CALL"); idx >= 0 {
		add(idx, e.analyzeCallClause(query))
	}
	if idx := findKeywordIndex(query, "WHERE"); idx >= 0 {
		add(idx, e.analyzeWhereClause(query))
	}
	if idx := findKeywordIndex(query, "WITH"); idx >= 0 {
		add(idx, &PlanOperator{
			OperatorType:  "Projection",
			Description:   "Project intermediate results",
			EstimatedRows: 100,
		})
	}
	if idx := findKeywordIndex(query, "CREATE"); idx >= 0 {
		add(idx, &PlanOperator{
			OperatorType:  "Create",
			Description:   "Create nodes and relationships",
			EstimatedRows: 1,
		})
	}
	if idx := findKeywordIndex(query, "MERGE"); idx >= 0 {
		add(idx, &PlanOperator{
			OperatorType:  "Merge",
			Description:   "Merge (create if not exists)",
			EstimatedRows: 1,
		})
	}
	if idx := findKeywordIndex(query, "SET"); idx >= 0 {
		add(idx, &PlanOperator{
			OperatorType:  "Set",
			Description:   "Set properties",
			EstimatedRows: 100,
		})
	}
	if idx := findKeywordIndex(query, "REMOVE"); idx >= 0 {
		add(idx, &PlanOperator{
			OperatorType:  "Remove",
			Description:   "Remove labels/properties",
			EstimatedRows: 100,
		})
	}
	if idx := findKeywordIndex(query, "DETACH DELETE"); idx >= 0 {
		add(idx, e.analyzeDeleteClause(query, true))
	} else if idx := findKeywordIndex(query, "DELETE"); idx >= 0 {
		add(idx, e.analyzeDeleteClause(query, false))
	}
	if idx := findKeywordIndex(query, "ORDER BY"); idx >= 0 {
		add(idx, &PlanOperator{
			OperatorType:  "Sort",
			Description:   "Sort results",
			EstimatedRows: 100,
		})
	}
	if idx := findKeywordIndex(query, "LIMIT"); idx >= 0 {
		add(idx, e.analyzeLimitSkip(query))
	} else if idx := findKeywordIndex(query, "SKIP"); idx >= 0 {
		add(idx, e.analyzeLimitSkip(query))
	}
	if idx := findKeywordIndex(query, "RETURN"); idx >= 0 {
		add(idx, e.analyzeReturnClause(query))
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
func (e *StorageExecutor) analyzeMatchClause(query string) *PlanOperator {
	upper := strings.ToUpper(query)

	// Check for shortestPath
	if strings.Contains(upper, "SHORTESTPATH") {
		return &PlanOperator{
			OperatorType:  "ShortestPath",
			Description:   "Find shortest path using BFS",
			EstimatedRows: 1,
			Arguments: map[string]interface{}{
				"algorithm": "BFS",
			},
		}
	}

	// Check for variable-length path
	if varLengthPathPattern.MatchString(query) {
		return &PlanOperator{
			OperatorType:  "VarLengthExpand",
			Description:   "Variable length path expansion",
			EstimatedRows: 100,
		}
	}

	// Check for relationship pattern
	if strings.Contains(query, "->") || strings.Contains(query, "<-") || strings.Contains(query, "]-[") {
		return &PlanOperator{
			OperatorType:  "Expand",
			Description:   "Expand relationships",
			EstimatedRows: 100,
			Children: []*PlanOperator{
				e.analyzeNodeScan(query),
			},
		}
	}

	// Simple node scan
	return e.analyzeNodeScan(query)
}

// analyzeNodeScan determines the type of node scan needed.
// When the storage schema is available, it checks for actual index existence
// and reports index usage or rejection reasons in the plan arguments.
func (e *StorageExecutor) analyzeNodeScan(query string) *PlanOperator {
	// Check for label in pattern (n:Label)
	if matches := labelExtractPattern.FindStringSubmatch(query); matches != nil {
		label := matches[1]

		// Check for property filter (n:Label {prop: value})
		if strings.Contains(query, "{") {
			args := map[string]interface{}{
				"label": label,
			}
			// Check actual index availability from schema.
			e.annotateIndexDiagnostics(args, label, query)

			return &PlanOperator{
				OperatorType:  "NodeIndexSeek",
				Description:   fmt.Sprintf("Index seek on :%s", label),
				EstimatedRows: 10,
				Arguments:     args,
				Identifiers:   []string{"n"},
			}
		}

		args := map[string]interface{}{
			"label": label,
		}
		e.annotateIndexDiagnostics(args, label, query)

		return &PlanOperator{
			OperatorType:  "NodeByLabelScan",
			Description:   fmt.Sprintf("Scan all :%s nodes", label),
			EstimatedRows: 1000,
			Arguments:     args,
			Identifiers:   []string{"n"},
		}
	}

	// No label - full scan
	return &PlanOperator{
		OperatorType:  "AllNodesScan",
		Description:   "Scan all nodes",
		EstimatedRows: 10000,
		Identifiers:   []string{"n"},
	}
}

// annotateIndexDiagnostics adds index usage/rejection diagnostics to the plan
// operator arguments. This makes tuning deterministic by exposing whether an
// index was available, which properties are indexed, and why it was rejected.
func (e *StorageExecutor) annotateIndexDiagnostics(args map[string]interface{}, label, query string) {
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
		if !strings.EqualFold(strings.TrimSpace(idxLabel), label) {
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

	args["indexStatus"] = "available"
	args["availableIndexes"] = labelIndexes

	// Check WHERE clause for predicate shapes that prevent index use.
	upper := strings.ToUpper(query)
	if strings.Contains(upper, "COALESCE(") {
		args["indexRejectionRisk"] = "function_wrapping (coalesce)"
	}
	if strings.Contains(upper, "TOLOWER(") || strings.Contains(upper, "TOUPPER(") {
		args["indexRejectionRisk"] = "function_wrapping (toLower/toUpper)"
	}
}

// analyzeWhereClause analyzes WHERE conditions
func (e *StorageExecutor) analyzeWhereClause(query string) *PlanOperator {
	// Extract WHERE clause
	whereIdx := findKeywordIndex(query, "WHERE")
	if whereIdx < 0 {
		return nil
	}

	// Find end of WHERE clause
	endIdx := len(query)
	for _, keyword := range []string{"RETURN", "ORDER", "LIMIT", "SKIP", "WITH", "CREATE", "SET", "REMOVE", "DETACH DELETE", "DELETE"} {
		if idx := findKeywordIndex(query[whereIdx:], keyword); idx > 0 {
			if whereIdx+idx < endIdx {
				endIdx = whereIdx + idx
			}
		}
	}

	whereClause := strings.TrimSpace(query[whereIdx+5 : endIdx])

	return &PlanOperator{
		OperatorType:  "Filter",
		Description:   fmt.Sprintf("Filter: %s", truncate(whereClause, 50)),
		EstimatedRows: 100,
		Arguments: map[string]interface{}{
			"predicate": whereClause,
		},
	}
}

func (e *StorageExecutor) analyzeDeleteClause(query string, detach bool) *PlanOperator {
	startKeyword := "DELETE"
	operator := "Delete"
	if detach {
		startKeyword = "DETACH DELETE"
		operator = "DetachDelete"
	}
	deleteIdx := findKeywordIndex(query, startKeyword)
	if deleteIdx < 0 {
		return &PlanOperator{
			OperatorType:  operator,
			Description:   "Delete matched entities",
			EstimatedRows: 100,
		}
	}

	endIdx := len(query)
	for _, keyword := range []string{"RETURN", "WITH", "ORDER BY", "LIMIT", "SKIP"} {
		if idx := findKeywordIndex(query[deleteIdx:], keyword); idx > 0 {
			candidate := deleteIdx + idx
			if candidate < endIdx {
				endIdx = candidate
			}
		}
	}

	deleteClause := strings.TrimSpace(query[deleteIdx:endIdx])
	strip := startKeyword + " "
	if strings.HasPrefix(strings.ToUpper(deleteClause), strip) {
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
func (e *StorageExecutor) analyzeReturnClause(query string) *PlanOperator {
	returnIdx := findKeywordIndex(query, "RETURN")
	if returnIdx < 0 {
		return nil
	}

	// Extract RETURN items
	returnClause := query[returnIdx+6:]
	// Trim ORDER BY, LIMIT, etc.
	for _, keyword := range []string{"ORDER BY", "LIMIT", "SKIP"} {
		if idx := findKeywordIndex(returnClause, keyword); idx > 0 {
			returnClause = returnClause[:idx]
		}
	}
	returnClause = strings.TrimSpace(returnClause)

	// Check for aggregations
	hasAggregation := aggregationPattern.MatchString(returnClause)

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
	if len(returnClause) >= 8 && strings.EqualFold(strings.TrimSpace(returnClause)[:8], "DISTINCT") {
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

// analyzeLimitSkip analyzes LIMIT and SKIP clauses
func (e *StorageExecutor) analyzeLimitSkip(query string) *PlanOperator {
	upper := strings.ToUpper(query)

	op := &PlanOperator{
		OperatorType: "Limit",
		Arguments:    make(map[string]interface{}),
	}

	// Extract LIMIT value
	if matches := limitPattern.FindStringSubmatch(query); matches != nil {
		op.Arguments["limit"] = matches[1]
		op.Description = fmt.Sprintf("Limit to %s rows", matches[1])
	}

	// Extract SKIP value
	if matches := skipPattern.FindStringSubmatch(query); matches != nil {
		op.Arguments["skip"] = matches[1]
		if op.Description != "" {
			op.Description += fmt.Sprintf(", skip %s", matches[1])
		} else {
			op.Description = fmt.Sprintf("Skip %s rows", matches[1])
		}
	}

	// Estimate rows based on limit
	if _, ok := op.Arguments["limit"]; ok {
		op.EstimatedRows = 10 // Use limit as estimate
	} else if strings.Contains(upper, "SKIP") {
		op.EstimatedRows = 100
	}

	return op
}

// analyzeCallClause analyzes CALL procedure invocations
func (e *StorageExecutor) analyzeCallClause(query string) *PlanOperator {
	// Extract procedure name
	matches := callProcedurePattern.FindStringSubmatch(query)

	procName := "unknown"
	if matches != nil {
		procName = matches[1]
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

// updatePlanWithStats updates plan operators with actual execution statistics
func (e *StorageExecutor) updatePlanWithStats(op *PlanOperator, result *ExecuteResult) {
	if op == nil {
		return
	}

	// Update actual rows for leaf operators
	op.ActualRows = int64(len(result.Rows))

	// Recursively update children
	for _, child := range op.Children {
		e.updatePlanWithStats(child, result)
	}
}

// estimateDBHits estimates database hits based on the plan
func (e *StorageExecutor) estimateDBHits(op *PlanOperator) int64 {
	if op == nil {
		return 0
	}

	var hits int64

	// Estimate based on operator type
	switch op.OperatorType {
	case "AllNodesScan":
		hits = op.EstimatedRows * 2 // Read node + properties
	case "NodeByLabelScan":
		hits = op.EstimatedRows * 2
	case "NodeIndexSeek":
		hits = op.EstimatedRows + 1 // Index lookup + node reads
	case "Expand":
		hits = op.EstimatedRows * 3 // Node + relationships + target nodes
	case "Filter":
		hits = op.EstimatedRows // Property access for filter
	case "ShortestPath":
		hits = op.EstimatedRows * 10 // BFS traversal hits
	default:
		hits = op.EstimatedRows
	}

	// Add child hits
	for _, child := range op.Children {
		hits += e.estimateDBHits(child)
	}

	op.DBHits = hits
	return hits
}

// planToResult converts an execution plan to an ExecuteResult
func (e *StorageExecutor) attachPlanMetadata(result *ExecuteResult, plan *ExecutionPlan) *ExecuteResult {
	// Add plan as metadata
	if result.Metadata == nil {
		result.Metadata = make(map[string]interface{})
	}
	result.Metadata["planString"] = e.formatPlan(plan)
	result.Metadata["plan"] = plan
	result.Metadata["planType"] = string(plan.Mode)

	return result
}

func (e *StorageExecutor) inferExplainColumns(query string) []string {
	// Neo4j EXPLAIN returns the same columns as the underlying query but no rows.
	if y := parseYieldClause(query); y != nil {
		if y.hasReturn && strings.TrimSpace(y.returnExpr) != "" {
			items := e.parseReturnItems(y.returnExpr)
			// For RETURN * after explicit YIELD items, project yielded aliases/names.
			if len(items) == 1 && strings.TrimSpace(items[0].expr) == "*" && len(y.items) > 0 {
				cols := make([]string, 0, len(y.items))
				for _, item := range y.items {
					if item.alias != "" {
						cols = append(cols, item.alias)
					} else {
						cols = append(cols, item.name)
					}
				}
				return cols
			}
			cols := make([]string, 0, len(items))
			for _, item := range items {
				if item.alias != "" {
					cols = append(cols, item.alias)
				} else {
					cols = append(cols, item.expr)
				}
			}
			return cols
		}
		if !y.yieldAll {
			cols := make([]string, 0, len(y.items))
			for _, item := range y.items {
				if item.alias != "" {
					cols = append(cols, item.alias)
				} else {
					cols = append(cols, item.name)
				}
			}
			return cols
		}
	}

	if returnIdx := findKeywordIndex(query, "RETURN"); returnIdx >= 0 {
		returnClause := strings.TrimSpace(query[returnIdx+len("RETURN"):])
		items := e.parseReturnItems(returnClause)
		cols := make([]string, 0, len(items))
		for _, item := range items {
			if item.alias != "" {
				cols = append(cols, item.alias)
			} else {
				cols = append(cols, item.expr)
			}
		}
		return cols
	}

	return []string{}
}

// formatPlan formats the execution plan as a string (tree visualization)
func (e *StorageExecutor) formatPlan(plan *ExecutionPlan) string {
	var sb strings.Builder

	sb.WriteString(fmt.Sprintf("+-%s-+\n", strings.Repeat("-", 60)))
	sb.WriteString(fmt.Sprintf("| %-60s |\n", fmt.Sprintf("%s %s", plan.Mode, "Query Plan")))
	sb.WriteString(fmt.Sprintf("+-%s-+\n", strings.Repeat("-", 60)))

	if plan.Mode == ModeProfile {
		sb.WriteString(fmt.Sprintf("| Total Time: %-47s |\n", plan.TotalTime.String()))
		sb.WriteString(fmt.Sprintf("| Total Rows: %-47d |\n", plan.TotalRows))
		sb.WriteString(fmt.Sprintf("| Total DB Hits: %-44d |\n", plan.TotalDBHits))
		sb.WriteString(fmt.Sprintf("+-%s-+\n", strings.Repeat("-", 60)))
	}

	e.formatOperator(&sb, plan.Root, 0, plan.Mode == ModeProfile)

	sb.WriteString(fmt.Sprintf("+-%s-+\n", strings.Repeat("-", 60)))

	return sb.String()
}

// formatOperator formats a single operator in the plan tree
func (e *StorageExecutor) formatOperator(sb *strings.Builder, op *PlanOperator, depth int, showStats bool) {
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
		statsLine := fmt.Sprintf("%s|   Est: %d, Actual: %d, Hits: %d",
			indent, op.EstimatedRows, op.ActualRows, op.DBHits)
		sb.WriteString(fmt.Sprintf("| %-60s |\n", statsLine))
	} else {
		// Just show estimates for EXPLAIN
		statsLine := fmt.Sprintf("%s|   Estimated Rows: %d", indent, op.EstimatedRows)
		sb.WriteString(fmt.Sprintf("| %-60s |\n", statsLine))
	}

	// Format children
	for _, child := range op.Children {
		e.formatOperator(sb, child, depth+1, showStats)
	}
}

// truncate truncates a string to maxLen characters
func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen-3] + "..."
}
