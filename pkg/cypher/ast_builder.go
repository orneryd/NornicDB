// Package cypher - AST builder for structured query representation.
package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/internal/cyphertext"
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
type ASTBuilder struct{}

// NewASTBuilder creates a new AST builder. Lexical extents come from cyphertext;
// the builder retains its advisory pattern/clause structure.
func NewASTBuilder() *ASTBuilder { return &ASTBuilder{} }

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

// exprMarker lets lightweight Query consumers retain the complete canonical
// expression product instead of reconstructing or discarding its structure.
func (ASTExpression) exprMarker() {}

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

// astTrim and astCommaViews retain exact accepted offsets; no substring lookup
// is used to recover original spelling when equal expressions occur repeatedly.
func astTrim(source *cyphertext.Source) *cyphertext.Source {
	if source == nil {
		return nil
	}
	start, end := sourceWhitespaceExtent(source.Text)
	return source.Window(start, end)
}

func astRaw(source *cyphertext.Source) string {
	if source == nil {
		return ""
	}
	span, ok := source.OriginalSpan(cyphertext.Span{Start: 0, End: len(source.Text)}, cyphertext.SourceExtent)
	if !ok {
		return ""
	}
	return source.Original[span.Start:span.End]
}

func astCommaViews(source *cyphertext.Source) []*cyphertext.Source {
	if source == nil {
		return nil
	}
	var parts []*cyphertext.Source
	for start := 0; ; {
		comma := findTopLevelOperator(source.Text[start:], ",", false, true)
		end := len(source.Text)
		if comma >= 0 {
			end = start + comma
		}
		parts = append(parts, astTrim(source.Window(start, end)))
		if comma < 0 {
			return parts
		}
		start = end + 1
	}
}

func astWithoutComments(source *cyphertext.Source) *cyphertext.Source {
	var edits []cyphertext.SourceEdit
	text := stripCommentText(source.Text, &edits)
	if len(edits) == 0 {
		return source
	}
	return source.Rewrite(text, edits)
}

// Build parses a Cypher query into an AST.
func (b *ASTBuilder) Build(cypher string) (*AST, error) {
	source := cyphertext.NormalizeSource(cypher)
	if source.Err != nil {
		return nil, source.Err
	}
	ast := &AST{
		RawQuery: cypher,
		Clauses:  make([]ASTClause, 0),
	}

	// Preserve one accepted source and original spans while canonical trivia
	// removal lets compound grammar phrases share their normal keyword owner.
	source = astWithoutComments(source)
	// Split into clauses
	clauses := b.splitIntoClauses(source.Text)

	for _, clauseInfo := range clauses {
		clause := b.parseClause(context.Background(), clauseInfo.clauseType, source.Window(clauseInfo.startPos, clauseInfo.startPos+len(clauseInfo.text)))
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
	upper := upperASCII(cypher)

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
		end        int
		clauseType ASTClauseType
	}
	var boundaries []boundary

	options := defaultKeywordScanOpts()
	options.SkipBraces = true
	for _, kw := range keywords {
		pos := 0
		for {
			absolutePos := keywordIndexFrom(cypher, kw.keyword, pos, options)
			if absolutePos < 0 {
				break
			}

			// Skip action keywords within "ON CREATE SET" or "ON MATCH SET"
			// These are MERGE clause modifiers, not standalone clauses
			if kw.keyword == "CREATE" || kw.keyword == "MATCH" || kw.keyword == "SET" {
				// Check if preceded by the keyword ON, not a name ending in on
				if keywordsBefore(upper, absolutePos, "ON") >= 0 || keywordsBefore(upper, absolutePos, "ON", "CREATE") >= 0 || keywordsBefore(upper, absolutePos, "ON", "MATCH") >= 0 {
					pos = absolutePos + len(kw.keyword)
					continue
				}
			}

			boundaries = append(boundaries, boundary{
				pos:        absolutePos,
				clauseType: kw.clauseType,
				end:        keywordEndAt(cypher, absolutePos, kw.keyword),
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
		if b.pos < prev.end {
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

// parseClause parses a single clause into its structured form.
func (b *ASTBuilder) parseClause(ctx context.Context, clauseType ASTClauseType, source *cyphertext.Source) *ASTClause {
	text := source.Text
	span, _ := source.OriginalSpan(cyphertext.Span{Start: 0, End: len(text)}, cyphertext.SourceExtent)
	clause := &ASTClause{
		Type:     clauseType,
		RawText:  astRaw(source),
		StartPos: span.Start,
		EndPos:   span.End,
	}

	switch clauseType {
	case ASTClauseMatch, ASTClauseOptionalMatch:
		clause.Match = b.parseMatch(source, clauseType == ASTClauseOptionalMatch)
	case ASTClauseCreate, ASTClauseSet:
		keywordBytes := len("SET")
		if clauseType == ASTClauseCreate {
			keywordBytes = len("CREATE")
		}
		body := astTrim(source.Window(keywordBytes, len(text)))
		if clauseType == ASTClauseCreate {
			clause.Create = &ASTCreate{Patterns: b.parsePatterns(body)}
		} else {
			clause.Set = &ASTSet{Items: b.parseSetItems(body)}
		}
	case ASTClauseMerge:
		clause.Merge = b.parseMerge(source)
	case ASTClauseDelete, ASTClauseDetachDelete:
		clause.Delete = b.parseDelete(source, clauseType == ASTClauseDetachDelete)
	case ASTClauseRemove:
		clause.Remove = b.parseRemove(source)
	case ASTClauseReturn, ASTClauseWith:
		keywordBytes := len("WITH")
		if clauseType == ASTClauseReturn {
			keywordBytes = len("RETURN")
		}
		items, distinct := b.parseProjection(ctx, source, keywordBytes)
		if clauseType == ASTClauseReturn {
			clause.Return = &ASTReturn{Items: items, Distinct: distinct}
		} else {
			clause.With = &ASTWith{Items: items, Distinct: distinct}
		}
	case ASTClauseWhere:
		clause.Where = b.parseWhere(source)
	case ASTClauseUnwind:
		clause.Unwind = b.parseUnwind(source)
	case ASTClauseOrderBy:
		clause.OrderBy = b.parseOrderBy(source)
	case ASTClauseLimit:
		clause.Limit = b.parseRowCount(source, len("LIMIT"))
	case ASTClauseSkip:
		clause.Skip = b.parseRowCount(source, len("SKIP"))
	case ASTClauseCall:
		clause.Call = b.parseCall(source)
	}

	return clause
}

// parseMatch parses a MATCH clause.
func (b *ASTBuilder) parseMatch(source *cyphertext.Source, optional bool) *ASTMatch {
	text := source.Text
	match := &ASTMatch{Optional: optional}

	keyword := "MATCH"
	if optional {
		keyword = "OPTIONAL MATCH"
	}
	prefixEnd := keywordEndAt(text, 0, keyword)
	match.Patterns = b.parsePatterns(astTrim(source.Window(prefixEnd, len(text))))

	return match
}

// parseMerge parses a MERGE clause through the MERGE action parser
// (splitMergeClauseActions): OnCreate and OnMatch hold the items of every
// ON CREATE SET / ON MATCH SET clause, in order.
func (b *ASTBuilder) parseMerge(source *cyphertext.Source) *ASTMerge {
	text := source.Text
	merge := &ASTMerge{}
	body := astTrim(source.Window(queryGapEnd(text, keywordEndAt(text, 0, "MERGE")), len(text)))
	var spans mergeActionSpans
	parts := splitMergeClauseActions(body.Text, &spans)
	if patterns := b.parsePatterns(body.Window(spans.pattern.Start, spans.pattern.End)); len(patterns) > 0 {
		merge.Pattern = patterns[0]
	}
	for index := 0; index < parts.onCreate.len(); index++ {
		span := inlineOverflowAt(spans.onCreate.inline[:], spans.onCreate.overflow, index)
		merge.OnCreate = append(merge.OnCreate, b.parseSetItems(astTrim(body.Window(span.Start+len("SET"), span.End)))...)
	}
	for index := 0; index < parts.onMatch.len(); index++ {
		span := inlineOverflowAt(spans.onMatch.inline[:], spans.onMatch.overflow, index)
		merge.OnMatch = append(merge.OnMatch, b.parseSetItems(astTrim(body.Window(span.Start+len("SET"), span.End)))...)
	}
	return merge
}

// parseDelete parses a DELETE clause.
func (b *ASTBuilder) parseDelete(source *cyphertext.Source, detach bool) *ASTDelete {
	text := source.Text
	del := &ASTDelete{Detach: detach}

	keyword := "DELETE"
	if detach {
		keyword = "DETACH DELETE"
	}
	prefixEnd := keywordEndAt(text, 0, keyword)
	varText := strings.TrimSpace(text[prefixEnd:])

	// Split by comma
	vars := splitTopLevelComma(varText)
	for _, v := range vars {
		v = strings.TrimSpace(v)
		if v != "" {
			del.Variables = append(del.Variables, v)
		}
	}

	return del
}

// parseSetItems parses SET assignments.
func (b *ASTBuilder) parseSetItems(source *cyphertext.Source) []ASTSetItem {
	var items []ASTSetItem

	// Split by comma (but not inside brackets/braces)
	parts := astCommaViews(source)

	for _, partSource := range parts {
		partSource = astTrim(partSource)
		part := partSource.Text
		if part == "" {
			continue
		}

		item := ASTSetItem{RawValue: astRaw(partSource)}

		// Parse n.prop = value or n = value or n += value
		if eqIdx := findTopLevelOperator(part, "=", false, true); eqIdx > 0 {
			left := strings.TrimSpace(part[:eqIdx])

			// Handle += operator
			left = strings.TrimSuffix(left, "+")

			// The contextual SET target owner supplies the postfix boundary
			// and decoded property; quoted punctuation is part of the name.
			item.Variable, item.Property, _ = parseSetAssignmentTarget(left)
			if name, ok := cyphertext.ScanIdentifier(item.Variable, 0); ok && name.End == len(item.Variable) {
				item.Variable = name.Value
			}

			rightSource := astTrim(partSource.Window(eqIdx+1, len(part)))
			item.Value = b.parseExpression(rightSource)
			item.RawValue = astRaw(rightSource)
		}

		items = append(items, item)
	}

	return items
}

// parseRemove parses a REMOVE clause.
func (b *ASTBuilder) parseRemove(source *cyphertext.Source) *ASTRemove {
	text := source.Text
	rem := &ASTRemove{}
	itemsText := strings.TrimSpace(text[len("REMOVE"):])

	parts := astCommaViews(source.Window(len(text)-len(itemsText), len(text)))
	for _, partSource := range parts {
		partSource = astTrim(partSource)
		part := partSource.Text
		if part == "" {
			continue
		}

		item := ASTRemoveItem{}

		// Check for label removal (n:Label) or property removal (n.prop)
		if colonIdx := findTopLevelOperator(part, ":", false, true); colonIdx > 0 {
			item.Variable = strings.TrimSpace(part[:colonIdx])
			labels := labelChainNames(part[colonIdx+1:])
			for _, l := range labels {
				if l != "" {
					item.Labels = append(item.Labels, l)
				}
			}
		} else if variable, property, hasProperty := parseSetAssignmentTarget(part); hasProperty {
			item.Variable, item.Property = variable, property
		}
		if name, ok := cyphertext.ScanIdentifier(item.Variable, 0); ok && name.End == len(item.Variable) {
			item.Variable = name.Value
		}

		rem.Items = append(rem.Items, item)
	}

	return rem
}

// parseProjection owns RETURN/WITH item framing, DISTINCT and alias admission.
func (b *ASTBuilder) parseProjection(ctx context.Context, source *cyphertext.Source, keywordBytes int) (items []ASTReturnItem, distinct bool) {
	text := source.Text

	itemsText := strings.TrimSpace(text[keywordBytes:])

	// Check for DISTINCT
	itemsText, distinct = cutDistinct(itemsText)

	// Parse items
	parts := astCommaViews(source.Window(len(text)-len(itemsText), len(text)))
	for _, partSource := range parts {
		partSource = astTrim(partSource)
		part := partSource.Text
		if part == "" {
			continue
		}

		item := ASTReturnItem{RawText: astRaw(partSource)}

		// Check for AS alias
		projectionSource := cyphertext.NewSource(ctx, "RETURN "+part, cyphertext.AcceptedSource)
		itemSource := readProjectionItemSource(cyphertext.WithSourceView(ctx, projectionSource), part, projectionAdmitted)
		if itemSource.as > 0 {
			item.Alias = itemSource.alias
			partSource = astTrim(partSource.Window(0, itemSource.as))
			part = partSource.Text
		}

		item.Expression = b.parseExpression(partSource)
		items = append(items, item)
	}

	return items, distinct
}

// parseWhere parses a WHERE clause.
func (b *ASTBuilder) parseWhere(source *cyphertext.Source) *ASTWhere {
	text := source.Text
	condText := strings.TrimSpace(text[len("WHERE"):])
	return &ASTWhere{
		Condition: b.parseExpression(source.Window(len(text)-len(condText), len(text))),
		RawText:   astRaw(source.Window(len(text)-len(condText), len(text))),
	}
}

// parseUnwind parses an UNWIND clause.
func (b *ASTBuilder) parseUnwind(source *cyphertext.Source) *ASTUnwind {
	unwind := &ASTUnwind{}
	body := astTrim(source.Window(len("UNWIND"), len(source.Text)))
	content := body.Text

	if asIndex := topLevelKeywordIndex(content, "AS"); asIndex > 0 {
		// Map through the AS boundary before trimming accepted trivia: a
		// normalized comment may occupy that trivia in the original source.
		expressionSource := body.Window(0, asIndex)
		unwind.RawExpr = strings.TrimSpace(astRaw(expressionSource))
		unwind.Variable, _ = isOneSymbolicName(StripComments(content[asIndex+len("AS"):]))
		unwind.Expression = b.parseExpression(astTrim(expressionSource))
	}

	return unwind
}

// parseOrderBy parses an ORDER BY clause.
func (b *ASTBuilder) parseOrderBy(source *cyphertext.Source) *ASTOrderBy {
	text := source.Text
	orderBy := &ASTOrderBy{}
	content := strings.TrimSpace(text[keywordEndAt(text, 0, "ORDER BY"):])

	parts := astCommaViews(source.Window(len(text)-len(content), len(text)))
	for _, partSource := range parts {
		partSource = astTrim(partSource)
		part := partSource.Text
		if part == "" {
			continue
		}

		item := ASTOrderItem{RawText: astRaw(partSource)}

		// Check for DESC/ASC
		upper := upperASCII(part)
		if strings.HasSuffix(upper, " DESC") {
			item.Descending = true
			partSource = astTrim(partSource.Window(0, len(part)-5))
			part = partSource.Text
		} else if strings.HasSuffix(upper, " ASC") {
			partSource = astTrim(partSource.Window(0, len(part)-4))
			part = partSource.Text
		}

		item.Expression = b.parseExpression(partSource)
		orderBy.Items = append(orderBy.Items, item)
	}

	return orderBy
}

// parseRowCount admits the signed decimal count shared by LIMIT and SKIP.
func (b *ASTBuilder) parseRowCount(source *cyphertext.Source, keywordBytes int) *int64 {
	text := source.Text
	content := strings.TrimSpace(text[keywordBytes:])
	if val, err := strconv.ParseInt(content, 10, 64); err == nil {
		return &val
	}
	return nil
}

// parseCall parses a CALL clause.
func (b *ASTBuilder) parseCall(source *cyphertext.Source) *ASTCall {
	text := source.Text
	call := &ASTCall{}
	content := strings.TrimSpace(text[len("CALL"):])

	// Find procedure name and arguments
	parenIdx := -1
	for index := 0; index < len(content); index++ {
		if end, opaque := cyphertext.SkipToken(content, index); opaque {
			index = end - 1
			continue
		}
		if content[index] == '(' {
			parenIdx = index
			break
		}
	}
	if parenIdx > 0 {
		call.Procedure = strings.TrimSpace(content[:parenIdx])

		// Find closing paren
		closeIdx := cyphertext.FindMatchingDelimiter(content, parenIdx, '(', ')', cyphertext.OpeningDelimiter)
		if closeIdx > parenIdx {
			call.RawArgs = astRaw(source.Window(len(text)-len(content)+parenIdx+1, len(text)-len(content)+closeIdx))

			// Parse YIELD if present
			rest := strings.TrimSpace(content[closeIdx+1:])
			if strings.HasPrefix(upperASCII(rest), "YIELD") {
				yieldContent := strings.TrimSpace(rest[5:])
				yields := splitTopLevelComma(yieldContent)
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
func (b *ASTBuilder) parsePatterns(source *cyphertext.Source) []ASTPattern {
	var patterns []ASTPattern

	// Split by top-level commas
	parts := astCommaViews(source)

	for _, partSource := range parts {
		partSource = astTrim(partSource)
		part := partSource.Text
		if part == "" {
			continue
		}

		pattern := ASTPattern{RawText: astRaw(partSource)}

		for index := 0; index < len(part); {
			if end, opaque := cyphertext.SkipToken(part, index); opaque {
				index = end
				continue
			}
			opener := part[index]
			if opener != '(' && opener != '[' {
				index++
				continue
			}
			closer := byte(')')
			if opener == '[' {
				closer = ']'
			}
			end := cyphertext.FindMatchingDelimiter(part, index, rune(opener), rune(closer), cyphertext.OpeningDelimiter)
			if end < 0 {
				break
			}
			variable, labels, properties, ok := b.patternFields(partSource.Window(index+1, end))
			if ok && opener == '(' {
				node := ASTNode{Variable: variable, Labels: labels, Properties: map[string]ASTExpression{}, RawProps: astRaw(properties)}
				for _, fieldSource := range astCommaViews(properties) {
					fieldSource = astTrim(fieldSource)
					field := fieldSource.Text
					name, present := cyphertext.ScanIdentifier(field, 0)
					if !present {
						continue
					}
					colon, _ := cyphertext.GapEnd(field, name.End)
					if colon < len(field) && field[colon] == ':' {
						node.Properties[name.Value] = b.parseExpression(fieldSource.Window(colon+1, len(field)))
					}
				}
				pattern.Nodes = append(pattern.Nodes, node)
			} else if ok && opener == '[' && len(labels) <= 1 && (properties == nil || properties.Text == "") {
				open, close := readRelationshipBracket(part, index, requireClosedRelationship)
				left := len(strings.TrimRightFunc(part[:index], cyphertext.IsWhitespace)) - 1
				right, unclosed := cyphertext.GapEnd(part, end+1)
				if open == index && close == end && left >= 0 && part[left] == '-' && unclosed < 0 && right < len(part) && part[right] == '-' {
					start, finish := left, right+1
					if start > 0 && part[start-1] == '<' {
						start--
					}
					if finish < len(part) && part[finish] == '>' {
						finish++
					}
					arrowDirection, _, _ := relationshipArrows(part[start:finish])
					direction := EdgeBoth
					switch arrowDirection {
					case "incoming":
						direction = EdgeIncoming
					case "outgoing":
						direction = EdgeOutgoing
					}
					relationship := ASTRelationship{Variable: variable, Direction: direction}
					if len(labels) == 1 {
						relationship.Type = labels[0]
					}
					pattern.Relationships = append(pattern.Relationships, relationship)
				}
			}
			index = end + 1
		}

		patterns = append(patterns, pattern)
	}

	return patterns
}

// patternFields reads the builder's narrow node/relationship shape, delegating
// all names, trivia and property-map extents to the lexical foundation.
func (b *ASTBuilder) patternFields(source *cyphertext.Source) (variable string, labels []string, properties *cyphertext.Source, ok bool) {
	text := source.Text
	index, unclosed := cyphertext.GapEnd(text, 0)
	if unclosed >= 0 {
		return
	}
	if name, present := cyphertext.ScanIdentifier(text, index); present {
		variable, index = name.Value, name.End
	}
	for {
		index, unclosed = cyphertext.GapEnd(text, index)
		if unclosed >= 0 {
			return
		}
		if index >= len(text) || text[index] != ':' {
			break
		}
		index, unclosed = cyphertext.GapEnd(text, index+1)
		name, present := cyphertext.ScanIdentifier(text, index)
		if unclosed >= 0 || !present {
			return
		}
		labels, index = append(labels, name.Value), name.End
	}
	if index < len(text) && text[index] == '{' {
		end := cyphertext.FindMatchingDelimiter(text, index, '{', '}', cyphertext.OpeningDelimiter)
		if end < 0 {
			return
		}
		properties, index = source.Window(index+1, end), end+1
		index, unclosed = cyphertext.GapEnd(text, index)
	}
	ok = unclosed < 0 && index == len(text)
	return
}

// parseExpression parses an expression string.
func (b *ASTBuilder) parseExpression(source *cyphertext.Source) ASTExpression {
	text := source.Text
	source = astTrim(source)
	expr := ASTExpression{RawText: astRaw(source)}
	source = astTrim(astWithoutComments(source))
	text = source.Text

	// Parameter ($name)
	if strings.HasPrefix(text, "$") {
		expr.Type = ASTExprParameter
		expr.Parameter = symbolicNameValue(text[1:])
		return expr
	}

	// String literal
	if value, quoted := cyphertext.DecodeString(text, cyphertext.AcceptedSource); quoted {
		expr.Type = ASTExprLiteral
		expr.Literal = value
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
	upper := upperASCII(text)
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
	if isListLiteral(text) {
		expr.Type = ASTExprList
		inner := source.Window(1, len(text)-1)
		parts := astCommaViews(inner)
		for _, p := range parts {
			p = astTrim(p)
			if p.Text != "" {
				expr.List = append(expr.List, b.parseExpression(p))
			}
		}
		return expr
	}

	// Map literal
	if strings.HasPrefix(text, "{") && strings.HasSuffix(text, "}") {
		expr.Type = ASTExprMap
		expr.Map = make(map[string]ASTExpression)
		inner := astTrim(source.Window(1, len(text)-1))
		if inner.Text == "" {
			return expr
		}
		parts := astCommaViews(inner)
		for _, partSource := range parts {
			partSource = astTrim(partSource)
			part := partSource.Text
			if part == "" {
				continue
			}
			if colonIdx := findTopLevelMapKeyValueSeparator(part); colonIdx > 0 {
				key := normalizePropertyKey(strings.TrimSpace(part[:colonIdx]))
				expr.Map[key] = b.parseExpression(partSource.Window(colonIdx+1, len(part)))
			}
		}
		return expr
	}

	// Property access (n.prop)
	if variable, property, ok := splitPostfixPropertyAccess(text); ok {
		expr.Type = ASTExprProperty
		if decoded, named := isOneSymbolicName(variable); named {
			variable = decoded
		}
		expr.Property = &ASTPropertyAccess{
			Variable: variable,
			Property: property,
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

			// Keep the original function-argument source extent and modifier.
			argumentSource := cyphertext.ReadFunctionArgumentSource(argsText, cyphertext.DistinctArgumentModifier, distinctFollowsAsName)
			fc.Distinct = argumentSource.Modifier == cyphertext.DistinctArgumentModifier

			args := astCommaViews(source.Window(parenIdx+1+argumentSource.Arguments.Start, parenIdx+1+argumentSource.Arguments.End))
			for _, arg := range args {
				arg = astTrim(arg)
				if arg.Text != "" && arg.Text != "*" {
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
	if name, ok := isOneSymbolicName(text); ok {
		expr.Variable = name
	}
	return expr
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
