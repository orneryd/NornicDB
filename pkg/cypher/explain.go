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

	return e.planToResult(plan), nil
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

	return e.planToResult(plan), nil
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
	upper := strings.ToUpper(query)

	// Build operator tree based on query structure
	// The tree is built bottom-up (data sources at leaves, results at root)

	var operators []*PlanOperator

	// Check for MATCH clause - the main data source
	if strings.Contains(upper, "MATCH") {
		matchOp := e.analyzeMatchClause(query)
		operators = append(operators, matchOp)
	}

	// Check for CREATE clause
	if strings.Contains(upper, "CREATE") {
		createOp := &PlanOperator{
			OperatorType:  "CreateNode",
			Description:   "Create nodes and relationships",
			EstimatedRows: 1,
		}
		operators = append(operators, createOp)
	}

	// Check for MERGE clause
	if strings.Contains(upper, "MERGE") {
		mergeOp := &PlanOperator{
			OperatorType:  "Merge",
			Description:   "Merge (create if not exists)",
			EstimatedRows: 1,
		}
		operators = append(operators, mergeOp)
	}

	// Check for WHERE clause - adds Filter operator
	if strings.Contains(upper, "WHERE") {
		filterOp := e.analyzeWhereClause(query)
		operators = append(operators, filterOp)
	}

	// Check for WITH clause - adds Projection
	if strings.Contains(upper, "WITH") {
		withOp := &PlanOperator{
			OperatorType:  "Projection",
			Description:   "Project intermediate results",
			EstimatedRows: 100,
		}
		operators = append(operators, withOp)
	}

	// Check for ORDER BY - adds Sort operator
	if strings.Contains(upper, "ORDER BY") {
		sortOp := &PlanOperator{
			OperatorType:  "Sort",
			Description:   "Sort results",
			EstimatedRows: 100,
		}
		operators = append(operators, sortOp)
	}

	// Check for LIMIT/SKIP - adds Limit operator
	if strings.Contains(upper, "LIMIT") || strings.Contains(upper, "SKIP") {
		limitOp := e.analyzeLimitSkip(query)
		operators = append(operators, limitOp)
	}

	// Check for RETURN clause - the final result projection
	if strings.Contains(upper, "RETURN") {
		returnOp := e.analyzeReturnClause(query)
		operators = append(operators, returnOp)
	}

	// Check for CALL procedure
	if strings.Contains(upper, "CALL") {
		callOp := e.analyzeCallClause(query)
		operators = append(operators, callOp)
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

// analyzeNodeScan determines the type of node scan needed
func (e *StorageExecutor) analyzeNodeScan(query string) *PlanOperator {
	// Check for label in pattern (n:Label)
	if matches := labelExtractPattern.FindStringSubmatch(query); matches != nil {
		label := matches[1]

		// Check for property filter (n:Label {prop: value})
		if strings.Contains(query, "{") {
			// Estimate rows based on filter selectivity
			return &PlanOperator{
				OperatorType:  "NodeIndexSeek",
				Description:   fmt.Sprintf("Index seek on :%s", label),
				EstimatedRows: 10,
				Arguments: map[string]interface{}{
					"label": label,
				},
				Identifiers: []string{"n"},
			}
		}

		return &PlanOperator{
			OperatorType:  "NodeByLabelScan",
			Description:   fmt.Sprintf("Scan all :%s nodes", label),
			EstimatedRows: 1000,
			Arguments: map[string]interface{}{
				"label": label,
			},
			Identifiers: []string{"n"},
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

// analyzeWhereClause analyzes WHERE conditions
func (e *StorageExecutor) analyzeWhereClause(query string) *PlanOperator {
	upper := strings.ToUpper(query)

	// Extract WHERE clause
	whereIdx := strings.Index(upper, "WHERE")
	if whereIdx < 0 {
		return nil
	}

	// Find end of WHERE clause
	endIdx := len(query)
	for _, keyword := range []string{"RETURN", "ORDER", "LIMIT", "SKIP", "WITH", "CREATE", "SET", "DELETE"} {
		if idx := strings.Index(upper[whereIdx:], keyword); idx > 0 {
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

// analyzeReturnClause analyzes the RETURN clause
func (e *StorageExecutor) analyzeReturnClause(query string) *PlanOperator {
	upper := strings.ToUpper(query)

	returnIdx := strings.Index(upper, "RETURN")
	if returnIdx < 0 {
		return nil
	}

	// Extract RETURN items
	returnClause := query[returnIdx+6:]
	// Trim ORDER BY, LIMIT, etc.
	for _, keyword := range []string{"ORDER BY", "LIMIT", "SKIP"} {
		if idx := strings.Index(strings.ToUpper(returnClause), keyword); idx > 0 {
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

	// Extract LIMIT value - using optimized string parsing (~6x faster than regex)
	if limitStr := ExtractLimitString(query); limitStr != "" {
		op.Arguments["limit"] = limitStr
		op.Description = fmt.Sprintf("Limit to %s rows", limitStr)
	}

	// Extract SKIP value - using optimized string parsing (~6x faster than regex)
	if skipStr := ExtractSkipString(query); skipStr != "" {
		op.Arguments["skip"] = skipStr
		if op.Description != "" {
			op.Description += fmt.Sprintf(", skip %s", skipStr)
		} else {
			op.Description = fmt.Sprintf("Skip %s rows", skipStr)
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
func (e *StorageExecutor) planToResult(plan *ExecutionPlan) *ExecuteResult {
	result := &ExecuteResult{
		Columns: []string{"Plan"},
		Rows:    [][]interface{}{},
		Stats:   &QueryStats{},
	}

	// Build plan representation
	planStr := e.formatPlan(plan)
	result.Rows = append(result.Rows, []interface{}{planStr})

	// Add plan as metadata
	if result.Metadata == nil {
		result.Metadata = make(map[string]interface{})
	}
	result.Metadata["plan"] = plan
	result.Metadata["planType"] = string(plan.Mode)

	return result
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
