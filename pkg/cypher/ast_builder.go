// Package cypher - AST builder for structured query representation.
package cypher

import (
	"regexp"
	"strconv"
	"strings"
)

// ASTBuilder builds Abstract Syntax Trees from Cypher queries.
// This is separate from QueryAnalyzer to allow lazy AST building only when needed.
//
// Usage:
//
//	builder := NewASTBuilder()
//	ast, err := builder.Build("MATCH (n:Person) WHERE n.age > 21 RETURN n.name")
//	if err != nil {
//	    // handle error
//	}
//	// ast.Clauses contains structured representation
type ASTBuilder struct {
	// Precompiled patterns for performance
	nodePattern     *regexp.Regexp
	relationPattern *regexp.Regexp
	propertyPattern *regexp.Regexp
}

// NewASTBuilder creates a new AST builder.
func NewASTBuilder() *ASTBuilder {
	return &ASTBuilder{
		nodePattern:     regexp.MustCompile(`\((\w*)?(?::(\w+(?::\w+)*))?(?:\s*\{([^}]*)\})?\)`),
		relationPattern: regexp.MustCompile(`-\[(\w*)?(?::(\w+))?\]->`),
		propertyPattern: regexp.MustCompile(`(\w+)\s*:\s*([^,}]+)`),
	}
}

// AST represents a complete parsed query.
type AST struct {
	Clauses    []ASTClause
	RawQuery   string
	QueryType  QueryType
	IsReadOnly bool
	IsCompound bool
}

// ASTClause represents a parsed clause with its content.
type ASTClause struct {
	Type     ASTClauseType
	RawText  string // Original text of the clause
	StartPos int    // Position in original query
	EndPos   int    // End position in original query

	// Parsed content (populated based on clause type)
	Match   *ASTMatch
	Create  *ASTCreate
	Merge   *ASTMerge
	Delete  *ASTDelete
	Set     *ASTSet
	Remove  *ASTRemove
	Return  *ASTReturn
	With    *ASTWith
	Where   *ASTWhere
	Unwind  *ASTUnwind
	OrderBy *ASTOrderBy
	Limit   *int64
	Skip    *int64
	Call    *ASTCall
}

// ASTClauseType identifies the clause type.
type ASTClauseType int

const (
	ASTClauseMatch ASTClauseType = iota
	ASTClauseOptionalMatch
	ASTClauseCreate
	ASTClauseMerge
	ASTClauseDelete
	ASTClauseDetachDelete
	ASTClauseSet
	ASTClauseRemove
	ASTClauseReturn
	ASTClauseWith
	ASTClauseWhere
	ASTClauseUnwind
	ASTClauseOrderBy
	ASTClauseLimit
	ASTClauseSkip
	ASTClauseCall
	ASTClauseUnion
	ASTClauseForeach
)

// ASTMatch represents a MATCH clause.
type ASTMatch struct {
	Patterns []ASTPattern
	Optional bool
}

// ASTCreate represents a CREATE clause.
type ASTCreate struct {
	Patterns []ASTPattern
}

// ASTMerge represents a MERGE clause.
type ASTMerge struct {
	Pattern  ASTPattern
	OnCreate []ASTSetItem
	OnMatch  []ASTSetItem
}

// ASTDelete represents a DELETE clause.
type ASTDelete struct {
	Variables []string
	Detach    bool
}

// ASTSet represents a SET clause.
type ASTSet struct {
	Items []ASTSetItem
}

// ASTSetItem represents a single SET assignment.
type ASTSetItem struct {
	Variable string
	Property string
	Value    ASTExpression
	RawValue string // Original value text for complex expressions
}

// ASTRemove represents a REMOVE clause.
type ASTRemove struct {
	Items []ASTRemoveItem
}

// ASTRemoveItem represents a property or label removal.
type ASTRemoveItem struct {
	Variable string
	Property string   // For property removal
	Labels   []string // For label removal
}

// ASTReturn represents a RETURN clause.
type ASTReturn struct {
	Items    []ASTReturnItem
	Distinct bool
}

// ASTReturnItem represents an item in RETURN.
type ASTReturnItem struct {
	Expression ASTExpression
	Alias      string
	RawText    string
}

// ASTWith represents a WITH clause.
type ASTWith struct {
	Items    []ASTReturnItem
	Distinct bool
}

// ASTWhere represents a WHERE clause.
type ASTWhere struct {
	Condition ASTExpression
	RawText   string
}

// ASTUnwind represents an UNWIND clause.
type ASTUnwind struct {
	Expression ASTExpression
	Variable   string
	RawExpr    string
}

// ASTOrderBy represents ORDER BY.
type ASTOrderBy struct {
	Items []ASTOrderItem
}

// ASTOrderItem represents a single ORDER BY item.
type ASTOrderItem struct {
	Expression ASTExpression
	Descending bool
	RawText    string
}

// ASTCall represents a CALL clause.
type ASTCall struct {
	Procedure string
	Arguments []ASTExpression
	RawArgs   string
	Yield     []string
}

// ASTPattern represents a graph pattern.
type ASTPattern struct {
	Nodes         []ASTNode
	Relationships []ASTRelationship
	RawText       string
}

// ASTNode represents a node in a pattern.
type ASTNode struct {
	Variable   string
	Labels     []string
	Properties map[string]ASTExpression
	RawProps   string
}

// ASTRelationship represents a relationship in a pattern.
type ASTRelationship struct {
	Variable   string
	Type       string
	Direction  EdgeDirection
	Properties map[string]ASTExpression
	MinHops    *int
	MaxHops    *int
}

// ASTExpression represents an expression.
type ASTExpression struct {
	Type    ASTExprType
	RawText string

	// Different expression types populate different fields
	Literal   interface{} // For literals
	Variable  string      // For variable references
	Property  *ASTPropertyAccess
	Function  *ASTFunctionCall
	Binary    *ASTBinaryExpr
	Unary     *ASTUnaryExpr
	List      []ASTExpression
	Map       map[string]ASTExpression
	Parameter string // For $param
	Case      *ASTCaseExpr
}

// ASTExprType identifies expression types.
type ASTExprType int

const (
	ASTExprLiteral ASTExprType = iota
	ASTExprVariable
	ASTExprProperty
	ASTExprFunction
	ASTExprBinary
	ASTExprUnary
	ASTExprList
	ASTExprMap
	ASTExprParameter
	ASTExprCase
)

// ASTPropertyAccess represents property access (n.name).
type ASTPropertyAccess struct {
	Variable string
	Property string
}

// ASTFunctionCall represents a function call.
type ASTFunctionCall struct {
	Name      string
	Arguments []ASTExpression
	Distinct  bool
}

// ASTBinaryExpr represents a binary operation.
type ASTBinaryExpr struct {
	Left     ASTExpression
	Operator string
	Right    ASTExpression
}

// ASTUnaryExpr represents a unary operation.
type ASTUnaryExpr struct {
	Operator string
	Operand  ASTExpression
}

// ASTCaseExpr represents a CASE expression.
type ASTCaseExpr struct {
	Input   *ASTExpression
	Whens   []ASTCaseWhen
	Default *ASTExpression
}

// ASTCaseWhen represents a WHEN clause in CASE.
type ASTCaseWhen struct {
	Condition ASTExpression
	Result    ASTExpression
}

// Build parses a Cypher query into an AST.
func (b *ASTBuilder) Build(cypher string) (*AST, error) {
	ast := &AST{
		RawQuery: cypher,
		Clauses:  make([]ASTClause, 0),
	}

	// Split into clauses
	clauses := b.splitIntoClauses(cypher)

	for _, clauseInfo := range clauses {
		clause, err := b.parseClause(clauseInfo.clauseType, clauseInfo.text, clauseInfo.startPos)
		if err != nil {
			// For robustness, store raw text even if parsing fails
			clause = &ASTClause{
				Type:     clauseInfo.clauseType,
				RawText:  clauseInfo.text,
				StartPos: clauseInfo.startPos,
				EndPos:   clauseInfo.startPos + len(clauseInfo.text),
			}
		}
		ast.Clauses = append(ast.Clauses, *clause)
	}

	// Determine query properties
	ast.IsReadOnly = b.determineReadOnly(ast)
	ast.IsCompound = len(ast.Clauses) > 1
	ast.QueryType = b.determineQueryType(ast)

	return ast, nil
}

type clauseInfo struct {
	clauseType ASTClauseType
	text       string
	startPos   int
}

// splitIntoClauses splits a query into individual clauses.
func (b *ASTBuilder) splitIntoClauses(cypher string) []clauseInfo {
	var clauses []clauseInfo
	upper := strings.ToUpper(cypher)

	// Keywords that start clauses
	// Order matters: longer phrases must come before shorter ones
	// to avoid partial matches (e.g., "OPTIONAL MATCH" before "MATCH")
	keywords := []struct {
		keyword    string
		clauseType ASTClauseType
	}{
		{"OPTIONAL MATCH", ASTClauseOptionalMatch},
		{"DETACH DELETE", ASTClauseDetachDelete},
		{"ORDER BY", ASTClauseOrderBy},
		{"MATCH", ASTClauseMatch},
		{"MERGE", ASTClauseMerge},
		{"DELETE", ASTClauseDelete},
		{"REMOVE", ASTClauseRemove},
		{"RETURN", ASTClauseReturn},
		{"UNWIND", ASTClauseUnwind},
		{"WHERE", ASTClauseWhere},
		{"LIMIT", ASTClauseLimit},
		{"SKIP", ASTClauseSkip},
		{"CALL", ASTClauseCall},
		{"UNION", ASTClauseUnion},
		{"FOREACH", ASTClauseForeach},
		{"WITH", ASTClauseWith},
		// CREATE and SET are special - they can appear inside MERGE (ON CREATE SET, ON MATCH SET)
		// We handle them last and filter out false positives
		{"CREATE", ASTClauseCreate},
		{"SET", ASTClauseSet},
	}

	// Find all clause boundaries
	type boundary struct {
		pos        int
		clauseType ASTClauseType
		keyword    string
	}
	var boundaries []boundary

	for _, kw := range keywords {
		pos := 0
		for {
			idx := findKeywordPosition(upper[pos:], kw.keyword)
			if idx < 0 {
				break
			}
			absolutePos := pos + idx

			// Skip CREATE and SET if they're part of "ON CREATE SET" or "ON MATCH SET"
			// These are MERGE clause modifiers, not standalone clauses
			if kw.keyword == "CREATE" || kw.keyword == "SET" {
				// Check if preceded by "ON " (with some flexibility for spacing)
				if absolutePos >= 3 {
					before := strings.TrimRight(upper[:absolutePos], " \t\n")
					if strings.HasSuffix(before, "ON") || strings.HasSuffix(before, "ON CREATE") || strings.HasSuffix(before, "ON MATCH") {
						pos = absolutePos + len(kw.keyword)
						continue
					}
				}
			}

			boundaries = append(boundaries, boundary{
				pos:        absolutePos,
				clauseType: kw.clauseType,
				keyword:    kw.keyword,
			})
			pos = absolutePos + len(kw.keyword)
		}
	}

	// Sort by position
	for i := 0; i < len(boundaries); i++ {
		for j := i + 1; j < len(boundaries); j++ {
			if boundaries[j].pos < boundaries[i].pos {
				boundaries[i], boundaries[j] = boundaries[j], boundaries[i]
			}
		}
	}

	// Remove overlapping boundaries (e.g., OPTIONAL MATCH vs MATCH)
	var filtered []boundary
	for i, b := range boundaries {
		if i == 0 {
			filtered = append(filtered, b)
			continue
		}
		prev := filtered[len(filtered)-1]
		// Skip if this boundary starts within the previous keyword
		if b.pos < prev.pos+len(prev.keyword) {
			continue
		}
		filtered = append(filtered, b)
	}

	// Extract clause text between boundaries
	for i, bound := range filtered {
		endPos := len(cypher)
		if i+1 < len(filtered) {
			endPos = filtered[i+1].pos
		}

		text := strings.TrimSpace(cypher[bound.pos:endPos])
		clauses = append(clauses, clauseInfo{
			clauseType: bound.clauseType,
			text:       text,
			startPos:   bound.pos,
		})
	}

	return clauses
}

// findKeywordPosition finds keyword position respecting word boundaries.
func findKeywordPosition(s, keyword string) int {
	idx := strings.Index(s, keyword)
	if idx < 0 {
		return -1
	}

	// Check word boundaries
	if idx > 0 {
		prev := s[idx-1]
		if (prev >= 'A' && prev <= 'Z') || (prev >= 'a' && prev <= 'z') || (prev >= '0' && prev <= '9') || prev == '_' {
			// Try to find next occurrence
			rest := findKeywordPosition(s[idx+1:], keyword)
			if rest < 0 {
				return -1
			}
			return idx + 1 + rest
		}
	}

	end := idx + len(keyword)
	if end < len(s) {
		next := s[end]
		if (next >= 'A' && next <= 'Z') || (next >= 'a' && next <= 'z') || (next >= '0' && next <= '9') || next == '_' {
			// Try to find next occurrence
			rest := findKeywordPosition(s[idx+1:], keyword)
			if rest < 0 {
				return -1
			}
			return idx + 1 + rest
		}
	}

	return idx
}

// parseClause parses a single clause into its structured form.
func (b *ASTBuilder) parseClause(clauseType ASTClauseType, text string, startPos int) (*ASTClause, error) {
	clause := &ASTClause{
		Type:     clauseType,
		RawText:  text,
		StartPos: startPos,
		EndPos:   startPos + len(text),
	}

	switch clauseType {
	case ASTClauseMatch, ASTClauseOptionalMatch:
		clause.Match = b.parseMatch(text, clauseType == ASTClauseOptionalMatch)
	case ASTClauseCreate:
		clause.Create = b.parseCreate(text)
	case ASTClauseMerge:
		clause.Merge = b.parseMerge(text)
	case ASTClauseDelete, ASTClauseDetachDelete:
		clause.Delete = b.parseDelete(text, clauseType == ASTClauseDetachDelete)
	case ASTClauseSet:
		clause.Set = b.parseSet(text)
	case ASTClauseRemove:
		clause.Remove = b.parseRemove(text)
	case ASTClauseReturn:
		clause.Return = b.parseReturn(text)
	case ASTClauseWith:
		clause.With = b.parseWith(text)
	case ASTClauseWhere:
		clause.Where = b.parseWhere(text)
	case ASTClauseUnwind:
		clause.Unwind = b.parseUnwind(text)
	case ASTClauseOrderBy:
		clause.OrderBy = b.parseOrderBy(text)
	case ASTClauseLimit:
		clause.Limit = b.parseLimit(text)
	case ASTClauseSkip:
		clause.Skip = b.parseSkip(text)
	case ASTClauseCall:
		clause.Call = b.parseCall(text)
	}

	return clause, nil
}

// parseMatch parses a MATCH clause.
func (b *ASTBuilder) parseMatch(text string, optional bool) *ASTMatch {
	match := &ASTMatch{Optional: optional}

	// Remove MATCH or OPTIONAL MATCH prefix
	patternText := text
	if optional {
		patternText = strings.TrimPrefix(strings.ToUpper(text), "OPTIONAL MATCH")
	} else {
		patternText = strings.TrimPrefix(strings.ToUpper(text), "MATCH")
	}
	patternText = strings.TrimSpace(text[len(text)-len(patternText):])

	// Parse patterns
	match.Patterns = b.parsePatterns(patternText)
	return match
}

// parseCreate parses a CREATE clause.
func (b *ASTBuilder) parseCreate(text string) *ASTCreate {
	create := &ASTCreate{}
	patternText := strings.TrimSpace(strings.TrimPrefix(strings.ToUpper(text), "CREATE"))
	patternText = strings.TrimSpace(text[len("CREATE"):])
	create.Patterns = b.parsePatterns(patternText)
	return create
}

// parseMerge parses a MERGE clause.
func (b *ASTBuilder) parseMerge(text string) *ASTMerge {
	merge := &ASTMerge{}

	// Find ON CREATE SET and ON MATCH SET
	upper := strings.ToUpper(text)
	onCreateIdx := strings.Index(upper, "ON CREATE SET")
	onMatchIdx := strings.Index(upper, "ON MATCH SET")

	// Find pattern end
	patternEnd := len(text)
	if onCreateIdx > 0 {
		patternEnd = onCreateIdx
	}
	if onMatchIdx > 0 && onMatchIdx < patternEnd {
		patternEnd = onMatchIdx
	}

	patternText := strings.TrimSpace(text[len("MERGE"):patternEnd])
	patterns := b.parsePatterns(patternText)
	if len(patterns) > 0 {
		merge.Pattern = patterns[0]
	}

	// Parse ON CREATE SET
	if onCreateIdx > 0 {
		endIdx := len(text)
		if onMatchIdx > onCreateIdx {
			endIdx = onMatchIdx
		}
		setItems := text[onCreateIdx+len("ON CREATE SET") : endIdx]
		merge.OnCreate = b.parseSetItems(setItems)
	}

	// Parse ON MATCH SET
	if onMatchIdx > 0 {
		setItems := text[onMatchIdx+len("ON MATCH SET"):]
		merge.OnMatch = b.parseSetItems(setItems)
	}

	return merge
}

// parseDelete parses a DELETE clause.
func (b *ASTBuilder) parseDelete(text string, detach bool) *ASTDelete {
	del := &ASTDelete{Detach: detach}

	// Remove prefix
	varText := text
	if detach {
		varText = strings.TrimPrefix(strings.ToUpper(text), "DETACH DELETE")
	} else {
		varText = strings.TrimPrefix(strings.ToUpper(text), "DELETE")
	}
	varText = strings.TrimSpace(text[len(text)-len(varText):])

	// Split by comma
	vars := strings.Split(varText, ",")
	for _, v := range vars {
		v = strings.TrimSpace(v)
		if v != "" {
			del.Variables = append(del.Variables, v)
		}
	}

	return del
}

// parseSet parses a SET clause.
func (b *ASTBuilder) parseSet(text string) *ASTSet {
	set := &ASTSet{}
	itemsText := strings.TrimSpace(strings.TrimPrefix(strings.ToUpper(text), "SET"))
	itemsText = strings.TrimSpace(text[len("SET"):])
	set.Items = b.parseSetItems(itemsText)
	return set
}

// parseSetItems parses SET assignments.
func (b *ASTBuilder) parseSetItems(text string) []ASTSetItem {
	var items []ASTSetItem

	// Split by comma (but not inside brackets/braces)
	parts := splitOutsideBrackets(text, ',')

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		item := ASTSetItem{RawValue: part}

		// Parse n.prop = value or n = value or n += value
		if eqIdx := strings.Index(part, "="); eqIdx > 0 {
			left := strings.TrimSpace(part[:eqIdx])
			right := strings.TrimSpace(part[eqIdx+1:])

			// Handle += operator
			if strings.HasSuffix(left, "+") {
				left = strings.TrimSuffix(left, "+")
			}

			// Parse left side (variable.property or just variable)
			if dotIdx := strings.LastIndex(left, "."); dotIdx > 0 {
				item.Variable = strings.TrimSpace(left[:dotIdx])
				item.Property = strings.TrimSpace(left[dotIdx+1:])
			} else {
				item.Variable = left
			}

			item.Value = b.parseExpression(right)
			item.RawValue = right
		}

		items = append(items, item)
	}

	return items
}

// parseRemove parses a REMOVE clause.
func (b *ASTBuilder) parseRemove(text string) *ASTRemove {
	rem := &ASTRemove{}
	itemsText := strings.TrimSpace(text[len("REMOVE"):])

	parts := strings.Split(itemsText, ",")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		item := ASTRemoveItem{}

		// Check for label removal (n:Label) or property removal (n.prop)
		if colonIdx := strings.Index(part, ":"); colonIdx > 0 {
			item.Variable = strings.TrimSpace(part[:colonIdx])
			labels := strings.Split(part[colonIdx+1:], ":")
			for _, l := range labels {
				l = strings.TrimSpace(l)
				if l != "" {
					item.Labels = append(item.Labels, l)
				}
			}
		} else if dotIdx := strings.Index(part, "."); dotIdx > 0 {
			item.Variable = strings.TrimSpace(part[:dotIdx])
			item.Property = strings.TrimSpace(part[dotIdx+1:])
		}

		rem.Items = append(rem.Items, item)
	}

	return rem
}

// parseReturn parses a RETURN clause.
func (b *ASTBuilder) parseReturn(text string) *ASTReturn {
	ret := &ASTReturn{}

	itemsText := strings.TrimSpace(text[len("RETURN"):])

	// Check for DISTINCT
	if strings.HasPrefix(strings.ToUpper(itemsText), "DISTINCT") {
		ret.Distinct = true
		itemsText = strings.TrimSpace(itemsText[len("DISTINCT"):])
	}

	// Parse items
	parts := splitOutsideBrackets(itemsText, ',')
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		item := ASTReturnItem{RawText: part}

		// Check for AS alias
		upper := strings.ToUpper(part)
		if asIdx := strings.LastIndex(upper, " AS "); asIdx > 0 {
			item.Alias = strings.TrimSpace(part[asIdx+4:])
			part = strings.TrimSpace(part[:asIdx])
		}

		item.Expression = b.parseExpression(part)
		ret.Items = append(ret.Items, item)
	}

	return ret
}

// parseWith parses a WITH clause.
func (b *ASTBuilder) parseWith(text string) *ASTWith {
	with := &ASTWith{}

	itemsText := strings.TrimSpace(text[len("WITH"):])

	// Check for DISTINCT
	if strings.HasPrefix(strings.ToUpper(itemsText), "DISTINCT") {
		with.Distinct = true
		itemsText = strings.TrimSpace(itemsText[len("DISTINCT"):])
	}

	// Parse items (same as RETURN)
	parts := splitOutsideBrackets(itemsText, ',')
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		item := ASTReturnItem{RawText: part}

		// Check for AS alias
		upper := strings.ToUpper(part)
		if asIdx := strings.LastIndex(upper, " AS "); asIdx > 0 {
			item.Alias = strings.TrimSpace(part[asIdx+4:])
			part = strings.TrimSpace(part[:asIdx])
		}

		item.Expression = b.parseExpression(part)
		with.Items = append(with.Items, item)
	}

	return with
}

// parseWhere parses a WHERE clause.
func (b *ASTBuilder) parseWhere(text string) *ASTWhere {
	condText := strings.TrimSpace(text[len("WHERE"):])
	return &ASTWhere{
		Condition: b.parseExpression(condText),
		RawText:   condText,
	}
}

// parseUnwind parses an UNWIND clause.
func (b *ASTBuilder) parseUnwind(text string) *ASTUnwind {
	unwind := &ASTUnwind{}
	content := strings.TrimSpace(text[len("UNWIND"):])

	// Find AS
	upper := strings.ToUpper(content)
	if asIdx := strings.Index(upper, " AS "); asIdx > 0 {
		unwind.RawExpr = strings.TrimSpace(content[:asIdx])
		unwind.Variable = strings.TrimSpace(content[asIdx+4:])
		unwind.Expression = b.parseExpression(unwind.RawExpr)
	}

	return unwind
}

// parseOrderBy parses an ORDER BY clause.
func (b *ASTBuilder) parseOrderBy(text string) *ASTOrderBy {
	orderBy := &ASTOrderBy{}
	content := strings.TrimSpace(text[len("ORDER BY"):])

	parts := splitOutsideBrackets(content, ',')
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		item := ASTOrderItem{RawText: part}

		// Check for DESC/ASC
		upper := strings.ToUpper(part)
		if strings.HasSuffix(upper, " DESC") {
			item.Descending = true
			part = strings.TrimSpace(part[:len(part)-5])
		} else if strings.HasSuffix(upper, " ASC") {
			part = strings.TrimSpace(part[:len(part)-4])
		}

		item.Expression = b.parseExpression(part)
		orderBy.Items = append(orderBy.Items, item)
	}

	return orderBy
}

// parseLimit parses a LIMIT clause.
func (b *ASTBuilder) parseLimit(text string) *int64 {
	content := strings.TrimSpace(text[len("LIMIT"):])
	if val, err := strconv.ParseInt(content, 10, 64); err == nil {
		return &val
	}
	return nil
}

// parseSkip parses a SKIP clause.
func (b *ASTBuilder) parseSkip(text string) *int64 {
	content := strings.TrimSpace(text[len("SKIP"):])
	if val, err := strconv.ParseInt(content, 10, 64); err == nil {
		return &val
	}
	return nil
}

// parseCall parses a CALL clause.
func (b *ASTBuilder) parseCall(text string) *ASTCall {
	call := &ASTCall{}
	content := strings.TrimSpace(text[len("CALL"):])

	// Find procedure name and arguments
	if parenIdx := strings.Index(content, "("); parenIdx > 0 {
		call.Procedure = strings.TrimSpace(content[:parenIdx])

		// Find closing paren
		closeIdx := strings.LastIndex(content, ")")
		if closeIdx > parenIdx {
			call.RawArgs = content[parenIdx+1 : closeIdx]

			// Parse YIELD if present
			rest := strings.TrimSpace(content[closeIdx+1:])
			if strings.HasPrefix(strings.ToUpper(rest), "YIELD") {
				yieldContent := strings.TrimSpace(rest[5:])
				yields := strings.Split(yieldContent, ",")
				for _, y := range yields {
					y = strings.TrimSpace(y)
					if y != "" {
						call.Yield = append(call.Yield, y)
					}
				}
			}
		}
	} else {
		call.Procedure = content
	}

	return call
}

// parsePatterns parses pattern text into ASTPattern structs.
func (b *ASTBuilder) parsePatterns(text string) []ASTPattern {
	var patterns []ASTPattern

	// Split by top-level commas
	parts := splitOutsideBrackets(text, ',')

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		pattern := ASTPattern{RawText: part}

		// Find all nodes in pattern
		for _, match := range b.nodePattern.FindAllStringSubmatch(part, -1) {
			node := ASTNode{
				Properties: make(map[string]ASTExpression),
			}
			if len(match) > 1 {
				node.Variable = match[1]
			}
			if len(match) > 2 && match[2] != "" {
				node.Labels = strings.Split(match[2], ":")
			}
			if len(match) > 3 && match[3] != "" {
				node.RawProps = match[3]
				// Parse properties
				for _, propMatch := range b.propertyPattern.FindAllStringSubmatch(match[3], -1) {
					if len(propMatch) > 2 {
						node.Properties[propMatch[1]] = b.parseExpression(propMatch[2])
					}
				}
			}
			pattern.Nodes = append(pattern.Nodes, node)
		}

		// Find relationships
		for _, match := range b.relationPattern.FindAllStringSubmatch(part, -1) {
			rel := ASTRelationship{
				Direction: EdgeOutgoing, // Default for ->
			}
			if len(match) > 1 {
				rel.Variable = match[1]
			}
			if len(match) > 2 {
				rel.Type = match[2]
			}
			pattern.Relationships = append(pattern.Relationships, rel)
		}

		patterns = append(patterns, pattern)
	}

	return patterns
}

// parseExpression parses an expression string.
func (b *ASTBuilder) parseExpression(text string) ASTExpression {
	text = strings.TrimSpace(text)

	expr := ASTExpression{RawText: text}

	// Parameter ($name)
	if strings.HasPrefix(text, "$") {
		expr.Type = ASTExprParameter
		expr.Parameter = text[1:]
		return expr
	}

	// String literal
	if (strings.HasPrefix(text, "'") && strings.HasSuffix(text, "'")) ||
		(strings.HasPrefix(text, "\"") && strings.HasSuffix(text, "\"")) {
		expr.Type = ASTExprLiteral
		expr.Literal = text[1 : len(text)-1]
		return expr
	}

	// Number literal
	if val, err := strconv.ParseInt(text, 10, 64); err == nil {
		expr.Type = ASTExprLiteral
		expr.Literal = val
		return expr
	}
	if val, err := strconv.ParseFloat(text, 64); err == nil {
		expr.Type = ASTExprLiteral
		expr.Literal = val
		return expr
	}

	// Boolean/null
	upper := strings.ToUpper(text)
	if upper == "TRUE" {
		expr.Type = ASTExprLiteral
		expr.Literal = true
		return expr
	}
	if upper == "FALSE" {
		expr.Type = ASTExprLiteral
		expr.Literal = false
		return expr
	}
	if upper == "NULL" {
		expr.Type = ASTExprLiteral
		expr.Literal = nil
		return expr
	}

	// List literal
	if strings.HasPrefix(text, "[") && strings.HasSuffix(text, "]") {
		expr.Type = ASTExprList
		inner := text[1 : len(text)-1]
		parts := splitOutsideBrackets(inner, ',')
		for _, p := range parts {
			p = strings.TrimSpace(p)
			if p != "" {
				expr.List = append(expr.List, b.parseExpression(p))
			}
		}
		return expr
	}

	// Property access (n.prop)
	if dotIdx := strings.Index(text, "."); dotIdx > 0 && !strings.Contains(text, "(") {
		expr.Type = ASTExprProperty
		expr.Property = &ASTPropertyAccess{
			Variable: text[:dotIdx],
			Property: text[dotIdx+1:],
		}
		return expr
	}

	// Function call
	if parenIdx := strings.Index(text, "("); parenIdx > 0 {
		closeParen := strings.LastIndex(text, ")")
		if closeParen > parenIdx {
			expr.Type = ASTExprFunction
			funcName := strings.TrimSpace(text[:parenIdx])
			argsText := text[parenIdx+1 : closeParen]

			fc := &ASTFunctionCall{Name: funcName}

			// Check for DISTINCT
			if strings.HasPrefix(strings.ToUpper(argsText), "DISTINCT ") {
				fc.Distinct = true
				argsText = strings.TrimSpace(argsText[9:])
			}

			args := splitOutsideBrackets(argsText, ',')
			for _, arg := range args {
				arg = strings.TrimSpace(arg)
				if arg != "" && arg != "*" {
					fc.Arguments = append(fc.Arguments, b.parseExpression(arg))
				}
			}

			expr.Function = fc
			return expr
		}
	}

	// Default: treat as variable
	expr.Type = ASTExprVariable
	expr.Variable = text
	return expr
}

// splitOutsideBrackets splits string by delimiter, ignoring delimiters inside brackets.
func splitOutsideBrackets(s string, delim rune) []string {
	var parts []string
	var current strings.Builder
	depth := 0
	inString := false
	stringChar := rune(0)

	for _, ch := range s {
		if inString {
			current.WriteRune(ch)
			if ch == stringChar {
				inString = false
			}
			continue
		}

		if ch == '\'' || ch == '"' {
			inString = true
			stringChar = ch
			current.WriteRune(ch)
			continue
		}

		if ch == '(' || ch == '[' || ch == '{' {
			depth++
			current.WriteRune(ch)
			continue
		}

		if ch == ')' || ch == ']' || ch == '}' {
			depth--
			current.WriteRune(ch)
			continue
		}

		if ch == delim && depth == 0 {
			parts = append(parts, current.String())
			current.Reset()
			continue
		}

		current.WriteRune(ch)
	}

	if current.Len() > 0 {
		parts = append(parts, current.String())
	}

	return parts
}

// determineReadOnly checks if the AST represents a read-only query.
func (b *ASTBuilder) determineReadOnly(ast *AST) bool {
	for _, clause := range ast.Clauses {
		switch clause.Type {
		case ASTClauseCreate, ASTClauseMerge, ASTClauseDelete, ASTClauseDetachDelete,
			ASTClauseSet, ASTClauseRemove:
			return false
		}
	}
	return true
}

// determineQueryType determines the primary query type.
func (b *ASTBuilder) determineQueryType(ast *AST) QueryType {
	if len(ast.Clauses) == 0 {
		return QueryMatch
	}

	switch ast.Clauses[0].Type {
	case ASTClauseCreate:
		return QueryCreate
	case ASTClauseMerge:
		return QueryMerge
	case ASTClauseDelete, ASTClauseDetachDelete:
		return QueryDelete
	case ASTClauseSet:
		return QuerySet
	default:
		return QueryMatch
	}
}
