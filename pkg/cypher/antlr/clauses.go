// Package antlr - Clause extraction from ANTLR AST.
//
// This file provides utilities to extract individual clause CONTENT directly
// from the AST structure, eliminating ALL string-based keyword parsing.
// The extracted content is ready to use - no HasPrefix stripping needed.
package antlr

import (
	"strings"

	"github.com/antlr4-go/antlr/v4"
	"github.com/orneryd/nornicdb/internal/cyphertext"
)

// ClauseInfo contains extracted clause CONTENT from AST (without keywords)
type ClauseInfo struct {
	// Content of each clause type - just the body, NOT the keyword
	// Example: for "RETURN x, y" -> ReturnContent = "x, y"
	MatchPattern    string   // Pattern from MATCH (n:Label) - just "(n:Label)"
	MatchFull       string   // Full MATCH clause including keyword (for executeMatch)
	OptionalMatches []string // Full OPTIONAL MATCH clauses
	WhereCondition  string   // Condition from WHERE - just the expression
	CreatePattern   string   // Pattern from CREATE
	CreateFull      string   // Full CREATE clause
	MergePattern    string   // Pattern from MERGE
	MergeFull       string   // Full MERGE clause
	MergePatterns   []string // Multiple MERGE patterns
	DeleteTargets   string   // Variables from DELETE - just "n, r"
	DetachDelete    bool     // Whether DETACH DELETE
	SetAssignments  string   // Assignments from SET - just "n.prop = value"
	RemoveItems     string   // Items from REMOVE - just "n.prop, n:Label"
	ReturnItems     string   // Items from RETURN - just "x, y AS alias"
	WithItems       string   // Items from WITH - just "x, y"
	WithItemsList   []string // Multiple WITH items
	UnwindExpr      string   // Expression from UNWIND
	UnwindAs        string   // Variable from UNWIND AS
	OrderByItems    string   // Items from ORDER BY - just "n.name DESC"
	LimitValue      string   // Value from LIMIT - just "10"
	SkipValue       string   // Value from SKIP - just "5"
	CallProcedure   string   // Procedure call - just "db.labels()"

	// ON CREATE SET / ON MATCH SET content (assignments only)
	OnCreateSet string
	OnMatchSet  string

	// Pattern parts extracted from MATCH
	Patterns []string

	// Variables defined in the query
	Variables []string
}

// ClauseExtractor extracts clause information from ANTLR parse trees
type ClauseExtractor struct {
	info *ClauseInfo
}

// NewClauseExtractor creates a new extractor
func NewClauseExtractor() *ClauseExtractor {
	return &ClauseExtractor{
		info: &ClauseInfo{},
	}
}

// Extract extracts clause information from a parse result
func (e *ClauseExtractor) Extract(parseResult *ParseResult) *ClauseInfo {
	if parseResult == nil || parseResult.Tree == nil {
		return e.info
	}

	// Walk the tree
	walker := &clauseWalker{info: e.info}
	antlr.ParseTreeWalkerDefault.Walk(walker, parseResult.Tree)

	return e.info
}

// ExtractClauses is a convenience function that parses and extracts in one call
func ExtractClauses(query string, parseResult *ParseResult) *ClauseInfo {
	extractor := NewClauseExtractor()
	return extractor.Extract(parseResult)
}

// clauseWalker walks the AST and extracts clause content
type clauseWalker struct {
	*BaseCypherParserListener
	info *ClauseInfo
}

// getChildText extracts text from a child context
func getChildText(ctx antlr.ParserRuleContext) string {
	if ctx == nil {
		return ""
	}
	start := ctx.GetStart()
	stop := ctx.GetStop()
	if start == nil || stop == nil {
		return strings.TrimSpace(ctx.GetText())
	}
	input := start.GetInputStream()
	if input == nil {
		return strings.TrimSpace(ctx.GetText())
	}
	if source, ok := input.(interface{ OriginalText(int, int) string }); ok {
		return strings.TrimSpace(source.OriginalText(start.GetStart(), stop.GetStop()))
	}
	return strings.TrimSpace(input.GetText(start.GetStart(), stop.GetStop()))
}

// getFullText extracts text including whitespace from token positions
func getFullText(ctx antlr.ParserRuleContext) string {
	if ctx == nil {
		return ""
	}
	start := ctx.GetStart()
	stop := ctx.GetStop()
	if start == nil || stop == nil {
		return strings.TrimSpace(ctx.GetText())
	}
	input := start.GetInputStream()
	if input == nil {
		return strings.TrimSpace(ctx.GetText())
	}
	if source, ok := input.(interface{ OriginalText(int, int) string }); ok {
		return source.OriginalText(start.GetStart(), stop.GetStop())
	}
	return input.GetText(start.GetStart(), stop.GetStop())
}

// EnterMatchSt captures MATCH clause - extracts pattern directly from AST
func (w *clauseWalker) EnterMatchSt(ctx *MatchStContext) {
	fullText := getFullText(ctx)

	if ctx.OPTIONAL() != nil {
		w.info.OptionalMatches = append(w.info.OptionalMatches, fullText)
	} else if w.info.MatchFull == "" {
		w.info.MatchFull = fullText
	}

	// Extract pattern content from PatternWhere child
	if pw := ctx.PatternWhere(); pw != nil {
		if pattern := pw.Pattern(); pattern != nil {
			patternText := getChildText(pattern)
			w.info.Patterns = append(w.info.Patterns, patternText)
			if w.info.MatchPattern == "" {
				w.info.MatchPattern = patternText
			}
		}
		if where := pw.Where(); where != nil {
			// WHERE child contains "WHERE condition" - extract just condition
			// The Where context has Expression() child
			if whereCtx, ok := where.(*WhereContext); ok {
				if expr := whereCtx.Expression(); expr != nil {
					w.info.WhereCondition = getChildText(expr)
				}
			}
		}
	}
}

// EnterCreateSt captures CREATE clause - extracts pattern from AST
func (w *clauseWalker) EnterCreateSt(ctx *CreateStContext) {
	if w.info.CreateFull == "" {
		w.info.CreateFull = getFullText(ctx)
		// CreateSt has Pattern() child containing just the pattern
		if pattern := ctx.Pattern(); pattern != nil {
			w.info.CreatePattern = getChildText(pattern)
		}
	}
}

// EnterMergeSt captures MERGE clause - extracts pattern from AST
func (w *clauseWalker) EnterMergeSt(ctx *MergeStContext) {
	fullText := getFullText(ctx)
	if w.info.MergeFull == "" {
		w.info.MergeFull = fullText
	}

	// MergeSt has PatternPart() child containing just the pattern
	if patternPart := ctx.PatternPart(); patternPart != nil {
		patternText := getChildText(patternPart)
		w.info.MergePatterns = append(w.info.MergePatterns, patternText)
		if w.info.MergePattern == "" {
			w.info.MergePattern = patternText
		}
	}

	// Extract ON CREATE SET / ON MATCH SET content from MergeAction children
	for _, action := range ctx.AllMergeAction() {
		if actionCtx, ok := action.(*MergeActionContext); ok {
			// MergeAction has SetSt() which contains SetItem children
			if setSt := actionCtx.SetSt(); setSt != nil {
				if setCtx, ok := setSt.(*SetStContext); ok {
					// Extract assignments directly from SetItem children
					var assignments []string
					for _, item := range setCtx.AllSetItem() {
						assignments = append(assignments, getChildText(item))
					}
					assignmentsText := strings.Join(assignments, ", ")

					// Determine if ON CREATE or ON MATCH using AST tokens
					if actionCtx.CREATE() != nil {
						w.info.OnCreateSet = assignmentsText
					} else if actionCtx.MATCH() != nil {
						w.info.OnMatchSet = assignmentsText
					}
				}
			}
		}
	}
}

// EnterDeleteSt captures DELETE clause - extracts targets from ExpressionChain
func (w *clauseWalker) EnterDeleteSt(ctx *DeleteStContext) {
	if ctx.DETACH() != nil {
		w.info.DetachDelete = true
	}

	// DeleteSt has ExpressionChain() containing the delete targets
	if exprChain := ctx.ExpressionChain(); exprChain != nil {
		w.info.DeleteTargets = getChildText(exprChain)
	}
}

// EnterSetSt captures SET clause - extracts assignments from SetItem children
func (w *clauseWalker) EnterSetSt(ctx *SetStContext) {
	if w.info.SetAssignments != "" {
		return
	}

	// SetSt has AllSetItem() containing the individual assignments
	var assignments []string
	for _, item := range ctx.AllSetItem() {
		assignments = append(assignments, getChildText(item))
	}
	w.info.SetAssignments = strings.Join(assignments, ", ")
}

// EnterRemoveSt captures REMOVE clause - extracts items from RemoveItem children
func (w *clauseWalker) EnterRemoveSt(ctx *RemoveStContext) {
	if w.info.RemoveItems != "" {
		return
	}

	// RemoveSt has AllRemoveItem() containing the individual items
	var items []string
	for _, item := range ctx.AllRemoveItem() {
		items = append(items, getChildText(item))
	}
	w.info.RemoveItems = strings.Join(items, ", ")
}

// EnterReturnSt captures RETURN clause - extracts items from ProjectionBody
func (w *clauseWalker) EnterReturnSt(ctx *ReturnStContext) {
	if w.info.ReturnItems != "" {
		return
	}

	// ReturnSt has ProjectionBody() containing projection items plus optional
	// ORDER BY / SKIP / LIMIT modifiers. ReturnItems should only capture the
	// projection items, not trailing modifiers.
	if projBody := ctx.ProjectionBody(); projBody != nil {
		if items := projBody.ProjectionItems(); items != nil {
			w.info.ReturnItems = getChildText(items)
		}
	}
}

// EnterWithSt captures WITH clause - extracts items from ProjectionBody
func (w *clauseWalker) EnterWithSt(ctx *WithStContext) {
	// WithSt has ProjectionBody() containing projection items plus optional
	// ORDER BY / SKIP / LIMIT modifiers. WithItems should only capture the
	// projection items, not trailing modifiers.
	if projBody := ctx.ProjectionBody(); projBody != nil {
		itemsText := ""
		if items := projBody.ProjectionItems(); items != nil {
			itemsText = getChildText(items)
		}
		w.info.WithItemsList = append(w.info.WithItemsList, itemsText)
		if w.info.WithItems == "" {
			w.info.WithItems = itemsText
		}
	}
}

// EnterUnwindSt captures UNWIND clause - extracts expression and variable
func (w *clauseWalker) EnterUnwindSt(ctx *UnwindStContext) {
	if w.info.UnwindExpr != "" {
		return
	}

	// UnwindSt has Expression() and Symbol() children
	if expr := ctx.Expression(); expr != nil {
		w.info.UnwindExpr = getChildText(expr)
	}
	if symbol := ctx.Symbol(); symbol != nil {
		w.info.UnwindAs = cyphertext.SymbolicNameValue(symbol.GetText())
	}
}

// EnterOrderSt captures ORDER BY clause - extracts items from OrderItem children
func (w *clauseWalker) EnterOrderSt(ctx *OrderStContext) {
	if w.info.OrderByItems != "" {
		return
	}

	// OrderSt has AllOrderItem() containing the individual items
	var items []string
	for _, item := range ctx.AllOrderItem() {
		items = append(items, getChildText(item))
	}
	w.info.OrderByItems = strings.Join(items, ", ")
}

// EnterLimitSt captures LIMIT clause - extracts value from Expression
func (w *clauseWalker) EnterLimitSt(ctx *LimitStContext) {
	if w.info.LimitValue != "" {
		return
	}

	// LimitSt has Expression() containing the limit value
	if expr := ctx.Expression(); expr != nil {
		w.info.LimitValue = getChildText(expr)
	}
}

// EnterSkipSt captures SKIP clause - extracts value from Expression
func (w *clauseWalker) EnterSkipSt(ctx *SkipStContext) {
	if w.info.SkipValue != "" {
		return
	}

	// SkipSt has Expression() containing the skip value
	if expr := ctx.Expression(); expr != nil {
		w.info.SkipValue = getChildText(expr)
	}
}

// EnterStandaloneCall captures CALL procedure - extracts procedure name and args from AST
func (w *clauseWalker) EnterStandaloneCall(ctx *StandaloneCallContext) {
	if w.info.CallProcedure != "" {
		return
	}

	// Build procedure call from InvocationName and ParenExpressionChain AST nodes
	var callText string
	if invName := ctx.InvocationName(); invName != nil {
		callText = getChildText(invName)
	}
	if parenExpr := ctx.ParenExpressionChain(); parenExpr != nil {
		callText += getChildText(parenExpr)
	}
	w.info.CallProcedure = callText
}

// EnterQueryCallSt captures CALL in query context - extracts from AST
func (w *clauseWalker) EnterQueryCallSt(ctx *QueryCallStContext) {
	if w.info.CallProcedure != "" {
		return
	}

	// Build procedure call from InvocationName and ParenExpressionChain AST nodes
	var callText string
	if invName := ctx.InvocationName(); invName != nil {
		callText = getChildText(invName)
	}
	if parenExpr := ctx.ParenExpressionChain(); parenExpr != nil {
		callText += getChildText(parenExpr)
	}
	w.info.CallProcedure = callText
}

// EnterSymbol captures variable names
func (w *clauseWalker) EnterSymbol(ctx *SymbolContext) {
	varName := cyphertext.SymbolicNameValue(strings.TrimSpace(ctx.GetText()))
	if varName != "" {
		// Deduplicate
		for _, v := range w.info.Variables {
			if v == varName {
				return
			}
		}
		w.info.Variables = append(w.info.Variables, varName)
	}
}
