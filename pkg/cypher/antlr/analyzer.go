// Package antlr provides ANTLR-based Cypher parsing for NornicDB.
package antlr

import (
	"sync"

	"github.com/antlr4-go/antlr/v4"
)

// ClauseType represents the type of a Cypher clause
type ClauseType int

const (
	ClauseUnknown ClauseType = iota
	ClauseMatch
	ClauseCreate
	ClauseMerge
	ClauseDelete
	ClauseSet
	ClauseRemove
	ClauseReturn
	ClauseWith
	ClauseUnwind
	ClauseCall
	ClauseForeach
	ClauseLoadCSV
	ClauseShow
	ClauseDrop
	ClauseOptionalMatch
)

// QueryInfo contains analyzed information about a Cypher query extracted from AST
type QueryInfo struct {
	// Query type flags
	HasMatch         bool
	HasOptionalMatch bool
	HasCreate        bool
	HasMerge         bool
	HasDelete        bool
	HasDetachDelete  bool
	HasSet           bool
	HasRemove        bool
	HasReturn        bool
	HasWith          bool
	HasUnwind        bool
	HasCall          bool
	HasExplain       bool
	HasProfile       bool
	HasShow          bool
	HasSchema        bool // DROP INDEX, CREATE CONSTRAINT, etc.
	HasUnion         bool
	HasUnionAll      bool
	HasForeach       bool
	HasLoadCSV       bool

	// FirstClause is the type of the first main clause in the query
	// Used for routing (e.g., "starts with MATCH" vs "starts with CREATE")
	FirstClause ClauseType

	// CallIsDbProcedure is true when the query calls a db.* procedure
	// These are metadata queries (db.labels, db.schema, etc.) that are safe to cache
	CallIsDbProcedure bool

	// HasShortestPath is true if query uses shortestPath() or allShortestPaths()
	HasShortestPath bool

	// MergeCount is the number of MERGE clauses (for detecting multiple MERGEs)
	MergeCount int

	// ClauseCount tracks number of main clause types (MATCH, CREATE, MERGE, DELETE, SET)
	// Incremented during AST walk - used for IsCompoundQuery derivation
	ClauseCount int

	// Derived properties
	IsReadOnly    bool
	IsWriteQuery  bool
	IsSchemaQuery bool

	// IsCompoundQuery is true if query has multiple clause types (e.g., MATCH...CREATE)
	IsCompoundQuery bool

	// Labels mentioned in the query (for cache invalidation)
	Labels []string

	// The inner query (without EXPLAIN/PROFILE prefix)
	InnerQuery string
}

// QueryAnalyzer walks ANTLR parse trees to extract query metadata
type QueryAnalyzer struct {
	cache   map[string]*QueryInfo
	cacheMu sync.RWMutex
}

// NewQueryAnalyzer creates a new query analyzer
func NewQueryAnalyzer() *QueryAnalyzer {
	return &QueryAnalyzer{
		cache: make(map[string]*QueryInfo),
	}
}

// Analyze extracts query information from a parse result
// Results are cached by query string for performance
func (a *QueryAnalyzer) Analyze(query string, parseResult *ParseResult) *QueryInfo {
	// Check cache first
	a.cacheMu.RLock()
	if info, ok := a.cache[query]; ok {
		a.cacheMu.RUnlock()
		return info
	}
	a.cacheMu.RUnlock()

	// Walk the AST to extract info
	info := &QueryInfo{
		InnerQuery: query,
	}

	if parseResult != nil && parseResult.Tree != nil {
		walker := &queryWalker{info: info, query: query}
		antlr.ParseTreeWalkerDefault.Walk(walker, parseResult.Tree)
	}

	// Derive properties
	info.IsWriteQuery = info.HasCreate || info.HasMerge || info.HasDelete || info.HasSet || info.HasRemove

	// Determine if query is read-only (safe to cache)
	// - Must not be a write query or schema change
	// - CALL procedures are NOT read-only UNLESS they're db.* metadata queries
	// - Pure read patterns (MATCH, RETURN without writes) are read-only
	callIsReadOnly := info.HasCall && info.CallIsDbProcedure
	hasReadPattern := info.HasMatch || info.HasReturn || info.HasWith || info.HasUnwind

	info.IsReadOnly = !info.IsWriteQuery && !info.HasSchema &&
		(callIsReadOnly || (!info.HasCall && hasReadPattern))

	info.IsSchemaQuery = info.HasSchema || info.HasShow

	// IsCompoundQuery derived from clause counts tracked during AST walk
	info.IsCompoundQuery = info.ClauseCount > 1 || info.MergeCount > 1

	// Cache the result
	a.cacheMu.Lock()
	a.cache[query] = info
	a.cacheMu.Unlock()

	return info
}

// ClearCache clears the analysis cache
func (a *QueryAnalyzer) ClearCache() {
	a.cacheMu.Lock()
	a.cache = make(map[string]*QueryInfo)
	a.cacheMu.Unlock()
}

// queryWalker implements antlr.ParseTreeListener to walk the AST
type queryWalker struct {
	*BaseCypherParserListener
	info           *QueryInfo
	query          string
	firstClauseSet bool // Track if we've seen the first clause
}

// setFirstClause sets the first clause type if not already set
func (w *queryWalker) setFirstClause(ct ClauseType) {
	if !w.firstClauseSet {
		w.info.FirstClause = ct
		w.firstClauseSet = true
	}
}

// EnterQueryPrefix detects EXPLAIN/PROFILE
func (w *queryWalker) EnterQueryPrefix(ctx *QueryPrefixContext) {
	text := ctx.GetText()
	if text == "EXPLAIN" {
		w.info.HasExplain = true
	} else if text == "PROFILE" {
		w.info.HasProfile = true
	}
}

// EnterMatchSt detects MATCH clauses (including OPTIONAL MATCH)
func (w *queryWalker) EnterMatchSt(ctx *MatchStContext) {
	// Check if this is an OPTIONAL MATCH
	if ctx.OPTIONAL() != nil {
		w.info.HasOptionalMatch = true
		w.setFirstClause(ClauseOptionalMatch)
		return
	}
	if !w.info.HasMatch {
		w.info.HasMatch = true
		w.info.ClauseCount++
	}
	w.setFirstClause(ClauseMatch)
}

// EnterCreateSt detects CREATE clauses
func (w *queryWalker) EnterCreateSt(ctx *CreateStContext) {
	if !w.info.HasCreate {
		w.info.HasCreate = true
		w.info.ClauseCount++
	}
	w.setFirstClause(ClauseCreate)
}

// EnterMergeSt detects MERGE clauses
func (w *queryWalker) EnterMergeSt(ctx *MergeStContext) {
	if !w.info.HasMerge {
		w.info.HasMerge = true
		w.info.ClauseCount++
	}
	w.info.MergeCount++
	w.setFirstClause(ClauseMerge)
}

// EnterDeleteSt detects DELETE clauses
func (w *queryWalker) EnterDeleteSt(ctx *DeleteStContext) {
	if !w.info.HasDelete {
		w.info.HasDelete = true
		w.info.ClauseCount++
	}
	if ctx.DETACH() != nil {
		w.info.HasDetachDelete = true
	}
	w.setFirstClause(ClauseDelete)
}

// EnterSetSt detects SET clauses
func (w *queryWalker) EnterSetSt(ctx *SetStContext) {
	if !w.info.HasSet {
		w.info.HasSet = true
		w.info.ClauseCount++
	}
	w.setFirstClause(ClauseSet)
}

// EnterRemoveSt detects REMOVE clauses
func (w *queryWalker) EnterRemoveSt(ctx *RemoveStContext) {
	w.info.HasRemove = true
	w.setFirstClause(ClauseRemove)
}

// EnterReturnSt detects RETURN clauses
func (w *queryWalker) EnterReturnSt(ctx *ReturnStContext) {
	w.info.HasReturn = true
	w.setFirstClause(ClauseReturn)
}

// EnterWithSt detects WITH clauses
func (w *queryWalker) EnterWithSt(ctx *WithStContext) {
	w.info.HasWith = true
	w.setFirstClause(ClauseWith)
}

// EnterUnwindSt detects UNWIND clauses
func (w *queryWalker) EnterUnwindSt(ctx *UnwindStContext) {
	w.info.HasUnwind = true
	w.setFirstClause(ClauseUnwind)
}

// EnterStandaloneCall detects standalone CALL statements (top-level)
func (w *queryWalker) EnterStandaloneCall(ctx *StandaloneCallContext) {
	w.info.HasCall = true
	w.setFirstClause(ClauseCall)
}

// EnterCallSubquery detects CALL {} subqueries
func (w *queryWalker) EnterCallSubquery(ctx *CallSubqueryContext) {
	w.info.HasCall = true
	w.setFirstClause(ClauseCall)
}

// EnterQueryCallSt detects CALL inside a regular query (e.g., CALL db.index...)
func (w *queryWalker) EnterQueryCallSt(ctx *QueryCallStContext) {
	w.info.HasCall = true
	w.setFirstClause(ClauseCall)
	// Check if this is a db.* procedure (safe to cache)
	if ctx.InvocationName() != nil {
		name := ctx.InvocationName().GetText()
		if len(name) >= 3 && (name[:3] == "db." || name[:3] == "DB.") {
			w.info.CallIsDbProcedure = true
		}
	}
}

// EnterShowCommand detects SHOW commands
func (w *queryWalker) EnterShowCommand(ctx *ShowCommandContext) {
	w.info.HasShow = true
	w.info.HasSchema = true
	w.setFirstClause(ClauseShow)
}

// EnterSchemaCommand detects schema commands (CREATE INDEX, etc.)
func (w *queryWalker) EnterSchemaCommand(ctx *SchemaCommandContext) {
	w.info.HasSchema = true
	// Check if it's a DROP or CREATE command
	if ctx.DROP() != nil {
		w.setFirstClause(ClauseDrop)
	} else if ctx.CREATE() != nil {
		// CREATE INDEX or CREATE CONSTRAINT - still routed via ClauseCreate
		w.setFirstClause(ClauseCreate)
	}
}

// EnterPathFunction detects shortestPath/allShortestPaths
func (w *queryWalker) EnterPathFunction(ctx *PathFunctionContext) {
	w.info.HasShortestPath = true
}

// EnterNodeLabels collects node labels for cache invalidation
func (w *queryWalker) EnterNodeLabels(ctx *NodeLabelsContext) {
	w.info.Labels = append(w.info.Labels, labelExpressionNames(ctx)...)
}

// EnterRelationshipTypes collects relationship types for cache invalidation
// This is critical - the old regex extracted both node labels AND relationship types
// for proper cache invalidation (e.g., :KNOWS, :BENCH_REL)
func (w *queryWalker) EnterRelationshipTypes(ctx *RelationshipTypesContext) {
	w.info.Labels = append(w.info.Labels, labelExpressionNames(ctx)...)
}

func labelExpressionNames(tree antlr.Tree) []string {
	if name, ok := tree.(INameContext); ok {
		return []string{name.GetText()}
	}
	var names []string
	for _, child := range tree.GetChildren() {
		names = append(names, labelExpressionNames(child)...)
	}
	return names
}
