// Author of retained-input changes: Codex R17 implementation agent, 29 October 2026.
package cypher

import (
	"container/heap"
	"context"
	"fmt"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
	math "github.com/orneryd/nornicdb/pkg/math/libm"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/util"
)

// ===== CALL {} Subquery Support (Neo4j 4.0+) =====

// isCallSubquery detects if a query is a CALL {} subquery vs CALL procedure()
// CALL {} subqueries have "CALL" followed by optional whitespace and "{"
// CALL procedures have "CALL procedure.name()"
func isCallSubquery(ctx context.Context, cypher string) bool {
	// Use regex for flexible whitespace matching: CALL followed by optional whitespace and {
	return hasSubqueryPattern(ctx, cypher, callSubqueryRe)
}

// startsWithCallSubquery reports whether cypher begins with a CALL { } or
// CALL (vars) { } subquery clause (as opposed to a procedure CALL, or a
// subquery later in the statement).
func startsWithCallSubquery(ctx context.Context, cypher string) bool {
	return callSubqueryAt(ctx, strings.TrimSpace(cypher), 0)
}

// isCallSubqueryPureReturn checks whether innerBody is a pure RETURN projection
// with no MATCH/CALL/UNWIND/write clauses. Precomputed once per branch to avoid
// repeated keyword scans on every seed row.
func isCallSubqueryPureReturn(innerBody string) bool {
	trimmed := strings.TrimSpace(innerBody)
	// Accept any valid RETURN clause start (space/newline/tab after RETURN),
	// not just a single-space prefix.
	if findKeywordIndex(trimmed, "RETURN") != 0 {
		return false
	}
	return findKeywordIndex(trimmed, "MATCH") < 0 &&
		findKeywordIndex(trimmed, "CALL") < 0 &&
		findKeywordIndex(trimmed, "UNWIND") < 0 &&
		findKeywordIndex(trimmed, "MERGE") < 0 &&
		findKeywordIndex(trimmed, "CREATE") < 0 &&
		findKeywordIndex(trimmed, "DELETE") < 0 &&
		findKeywordIndex(trimmed, "SET") < 0 &&
		findKeywordIndex(trimmed, "REMOVE") < 0 &&
		findKeywordIndex(trimmed, "WITH") < 0 &&
		findKeywordIndex(trimmed, "UNION") < 0
}

// parseCallSubquery is the compatibility adapter for body/plan consumers. The
// shared grammar owns the body boundary and the complete transactional spec.
func (e *StorageExecutor) parseCallSubquery(ctx context.Context, clause pipelineClause) (queryBody, string, bool, *transactionBatchSpec) {
	if ctx.Err() != nil {
		return queryBody{}, "", false, nil
	}
	call := readCallClause(clause)
	body := readQueryBody(clause.syntax.ChildRule(cypherantlr.CypherParserRULE_subqueryBody))
	if stripped, finishes := stripUnionBranchFinishes(ctx, body.executionText); finishes && strings.TrimSpace(stripped) != "" {
		body.executionText = stripped
		for i := range body.branches {
			branch := &body.branches[i]
			if text, removed := stripUnionBranchFinishes(ctx, branch.executionText); removed {
				branch.executionText = text
			}
			branch.finished = false
		}
	}
	return body, call.After, call.Batch != nil, call.Batch
}

// callSubqueryHasScopeClause reports whether a CALL subquery declares its
// imports with a scope clause (CALL () { … }, CALL (x, y) { … }, CALL (*)
// { … }). Its body's leading WITH is then an ordinary projection, never an
// import list (parseLeadingWithImports): CALL () { WITH 1 AS x FINISH } is a
// unit subquery whose body is just that WITH (#907).
func callSubqueryHasScopeClause(ctx context.Context, cypher string) bool {
	return readCallSubqueryScopeText(ctx, cypher).scoped
}

// parseCallSubqueryImportVariables is the legacy names-only view. Scope and
// result-schema consumers use StatementFacts, which preserves syntax errors.
func parseCallSubqueryImportVariables(ctx context.Context, cypher string) ([]string, error) {
	scope := readCallSubqueryScopeText(ctx, cypher)
	if scope.invalid {
		return nil, newSemanticError("Neo.ClientError.Statement.SyntaxError", "InvalidVariableImport", "CALL imports must contain only variable names")
	}
	return scope.names, nil
}

// prefixUnionBranches puts prefix before body, or before each branch of a
// UNION / UNION ALL body: every branch of a CALL subquery imports the same
// outer variables.
func prefixUnionBranches(ctx context.Context, prefix, body string) string {
	branches, unionAll, mixed, ok := parseTopLevelUnionTextBranches(ctx, body)
	if !ok || mixed || len(branches) < 2 {
		return prefix + " " + body
	}
	separator := " UNION "
	if unionAll {
		separator = " UNION ALL "
	}
	for i, branch := range branches {
		branches[i] = prefix + " " + branch
	}
	return strings.Join(branches, separator)
}

func detectReferencedCallSubquerySeedColumns(seedResult *ExecuteResult, subqueryBody string) []string {
	if seedResult == nil || len(seedResult.Columns) == 0 {
		return nil
	}
	referenced := make([]string, 0, len(seedResult.Columns))
	for _, col := range seedResult.Columns {
		name := strings.TrimSpace(col)
		if name == "" {
			continue
		}
		if isIdentifierReferenced(subqueryBody, name) {
			referenced = append(referenced, name)
		}
	}
	return referenced
}

// withImportClauseKeywords are the clauses that can follow a subquery's
// importing WITH list (parseLeadingWithImports).
var withImportClauseKeywords = []string{"WHERE", "OPTIONAL MATCH", "MATCH", "UNWIND", "MERGE", "CREATE", "SET", "DETACH DELETE", "DELETE", "REMOVE", "CALL", "RETURN", "WITH"}

// unscopedCallImports reads the importing WITHs of an unscoped CALL
// subquery's body: whether any UNION branch (or the body, without UNION)
// starts with an importing WITH (branchLeadingWithImport), and the
// variables those WITHs list. A branch that imports nothing adds none,
// whatever the other branches import (#907); it still reads outer variables
// through NornicDB's implicit import (pipelineApplyCallSubqueryWithMetadata).
// A CALL with a scope clause has no importing WITH
// (callSubqueryHasScopeClause).
//
// names is filled only when collectNames.
func unscopedCallImports(ctx context.Context, body queryBody, outer func(string) bool, collectNames bool) (names []string, importing bool, err error) {
	for _, branch := range body.branches {
		next, imports, failure := branchSyntaxImports(branch, outer, collectNames)
		if failure != nil {
			return nil, false, failure
		}
		if imports {
			importing = true
			names = append(names, next...)
		}
	}
	return names, importing, nil
}

func branchLeadingWithImport(ctx context.Context, body queryBody, outer func(string) bool, collectNames bool) ([]string, bool, error) {
	if len(body.branches) == 0 {
		return nil, false, nil
	}
	return branchSyntaxImports(body.branches[0], outer, collectNames)
}

func branchSyntaxImports(branch queryBodyBranch, outer func(string) bool, collectNames bool) (names []string, importing bool, err error) {
	if len(branch.clauses) == 0 || branch.clauses[0].kind != pipelineClauseWith {
		return nil, false, nil
	}
	plan := projectionPlanOfNode(branch.clauses[0].syntax)
	importing = plan.star
	if plan.star {
		plan = plan.withStarExpanded(nil)
	}
	for _, item := range plan.projections {
		if collectNames {
			names = append(names, item.alias)
		}
		for _, name := range expressionFreeVariables(item.expr) {
			if outer(name) {
				importing = true
			}
		}
	}
	if !importing {
		return nil, false, nil
	}
	if len(branch.clauses) == 1 && branch.useStart < 0 && !branch.finished {
		return names, true, localizedError(localization.CypherSubqueriesWithQueryClauseRequired(), nil)
	}
	return names, true, nil
}

// unscopedBranchLocals returns the outer variables (names) that one branch
// of an unscoped CALL body declares itself (branchDeclaresVariable): the
// branch doesn't receive their outer values through NornicDB's implicit
// import, so they are new variables there, as in Cypher (#907).
func unscopedBranchLocals(ctx context.Context, branch queryBody, names []string) []string {
	var locals []string
	for _, name := range names {
		if branchDeclaresVariable(ctx, branch, name) {
			locals = append(locals, name)
		}
	}
	return locals
}

// branchDeclaresVariable reports whether a branch of an unscoped CALL body
// declares variable before it reads it: its first use is a node or
// relationship variable of a MATCH, OPTIONAL MATCH, CREATE or MERGE pattern
// (or the pattern's path name), or the name UNWIND … AS or WITH … AS binds.
// Cypher gives such a branch a new variable; NornicDB's implicit import of an
// outer variable applies only to a name the branch reads first (#907).
func branchDeclaresVariable(ctx context.Context, branch queryBody, variable string) bool {
	clauses, ok := splitPipelineClauses(ctx, branch)
	if !ok {
		return false
	}
	for _, clause := range clauses {
		index := firstVariableOccurrence(clause.text, variable)
		if index < 0 {
			continue
		}
		switch clause.kind {
		case pipelineClauseMatch, pipelineClauseOptionalMatch, pipelineClauseCreate, pipelineClauseMerge:
			return index < patternPartEnd(clause) && isPatternVariableAt(clause.text, index, len(variable))
		case pipelineClauseUnwind, pipelineClauseWith:
			return precededByAS(clause.text, index)
		}
		return false
	}
	return false
}

// patternPartEnd is where a MATCH, CREATE or MERGE clause's pattern ends:
// at its WHERE, or MERGE's ON CREATE / ON MATCH, else the clause's end.
func patternPartEnd(clause pipelineClause) int {
	end := len(clause.text)
	for _, keyword := range [...]string{"WHERE", "ON"} {
		if index := topLevelKeywordIndex(clause.text, keyword); index > 0 && index < end {
			end = index
		}
	}
	return end
}

// firstVariableOccurrence is the index of the first use of variable in text
// as a variable: a whole word outside string literals, not a property key
// (m.variable), a parameter ($variable) or a map key ({variable: …}).
// -1 when there is none.
func firstVariableOccurrence(text, variable string) int {
	for index := 0; index+len(variable) <= len(text); index++ {
		switch c := text[index]; cypherLexicalByte(c) {
		case cypherQuoteClass:
			index = skipCypherQuotedText(text, index, c) - 1
			continue
		}
		if text[index:index+len(variable)] != variable {
			continue
		}
		end := index + len(variable)
		if (index > 0 && cyphertext.NamePartAt(text, index-1)) || (end < len(text) && cyphertext.NamePartAt(text, end)) {
			continue
		}
		previous, next := previousNonSpace(text, index), nextNonSpace(text, end)
		if previous == '.' || previous == '$' || ((previous == '{' || previous == ',') && next == ':') {
			continue
		}
		return index
	}
	return -1
}

// isPatternVariableAt reports whether the word at index is a pattern's node
// or relationship variable ((n:L), (n), -[r:T]->) or its path name (p = …).
func isPatternVariableAt(text string, index, length int) bool {
	next := nextNonSpace(text, index+length)
	switch previousNonSpace(text, index) {
	case '(':
		// Not a function call's argument (size(n)): a name before the
		// parenthesis is a function, unless it is the clause keyword
		// (MATCH (n), OPTIONAL MATCH (n), CREATE (n), MERGE (n)).
		opening := strings.LastIndexByte(text[:index], '(')
		if word := rawWordBefore(text, opening); word != "" && !isPatternClauseKeyword(word) {
			return false
		}
		return next == ':' || next == ')' || next == '{'
	case '[':
		opening := strings.LastIndexByte(text[:index], '[')
		if before := previousNonSpace(text, opening); before != '-' && before != '<' {
			return false
		}
		return next == ':' || next == ']' || next == '{' || next == '*'
	}
	return next == '=' && nextNonSpace(text, strings.IndexByte(text[index:], '=')+index+1) == '('
}

// wordBefore is the identifier that ends right before index (spaces
// skipped), "" when another character comes first.
// isPatternClauseKeyword reports whether word is a keyword a pattern
// follows: MATCH, CREATE, MERGE.
func isPatternClauseKeyword(word string) bool {
	return equalFoldASCII(word, "MATCH") || equalFoldASCII(word, "CREATE") || equalFoldASCII(word, "MERGE")
}

// precededByAS reports whether the word at index follows the keyword AS.
func precededByAS(text string, index int) bool {
	end := index
	for end > 0 && cyphertext.IsWhitespaceAt(text, end-1) {
		end--
	}
	return end >= 3 && equalFoldASCII(text[end-2:end], "AS") && !cyphertext.NamePartAt(text, end-3)
}

// previousNonSpace is the last non-space byte before index, 0 at the start.
func previousNonSpace(text string, index int) byte {
	if index <= 0 {
		return 0
	}
	at, found := cyphertext.NeighborIndex(text, index, cyphertext.Before)
	if !found {
		return 0
	}
	return text[at]
}

// nextNonSpace is the first non-space byte at or after index, 0 at the end.
func nextNonSpace(text string, index int) byte {
	at, found := cyphertext.NeighborIndex(text, index, cyphertext.AtOrAfter)
	if !found {
		return 0
	}
	return text[at]
}

// splitLeadingWhereNullGuard detects a leading correlated guard shape:
//
//	WITH <imports> WHERE <expr> <write/query-clause...>
//
// and returns (<expr>, <rest-after-where-expr>, true).
// It is intentionally conservative and only used by the correlated write branch
// fallback path in executeMatchWithCallSubquery.
func splitLeadingWhereNullGuard(query string) (whereExpr string, rest string, ok bool) {
	trimmed := strings.TrimSpace(query)
	var afterWhere string
	if strings.HasPrefix(upperASCII(trimmed), "WITH ") {
		afterWith := strings.TrimSpace(trimmed[len("WITH "):])
		whereIdx := findKeywordIndex(afterWith, "WHERE")
		if whereIdx <= 0 {
			return "", "", false
		}
		afterWhere = strings.TrimSpace(afterWith[whereIdx+len("WHERE"):])
	} else if strings.HasPrefix(upperASCII(trimmed), "WHERE ") {
		afterWhere = strings.TrimSpace(trimmed[len("WHERE "):])
	} else {
		return "", "", false
	}
	if afterWhere == "" {
		return "", "", false
	}

	nextIdx := len(afterWhere)
	clauseStarts := []int{
		findKeywordIndex(afterWhere, "SET"),
		findKeywordIndex(afterWhere, "CREATE"),
		findKeywordIndex(afterWhere, "MERGE"),
		findKeywordIndex(afterWhere, "DELETE"),
		findKeywordIndex(afterWhere, "REMOVE"),
		findKeywordIndex(afterWhere, "MATCH"),
		findMultiWordKeywordIndex(afterWhere, "OPTIONAL", "MATCH"),
		findKeywordIndex(afterWhere, "UNWIND"),
		findKeywordIndex(afterWhere, "CALL"),
		findKeywordIndex(afterWhere, "RETURN"),
		findKeywordIndex(afterWhere, "WITH"),
	}
	for _, idx := range clauseStarts {
		if idx > 0 && idx < nextIdx {
			nextIdx = idx
		}
	}
	if nextIdx == len(afterWhere) {
		return "", "", false
	}

	whereExpr = strings.TrimSpace(afterWhere[:nextIdx])
	rest = strings.TrimSpace(afterWhere[nextIdx:])
	if whereExpr == "" || rest == "" {
		return "", "", false
	}
	return whereExpr, rest, true
}

// evalWhereNullGuard evaluates narrow guard expressions used in correlated UNION
// write branches:
//
//	<var> IS NULL
//	<var> IS NOT NULL
//
// Returns (pass, handled). handled=false means expression is outside this narrow
// supported subset and caller should use the generic path.
func evalWhereNullGuard(whereExpr string, vars map[string]interface{}) (pass bool, handled bool) {
	expr := strings.TrimSpace(whereExpr)
	// Accept optional wrapping parentheses.
	for strings.HasPrefix(expr, "(") && cyphertext.FindMatchingDelimiter(expr, 0, '(', ')', cyphertext.OpeningDelimiter) == len(expr)-1 {
		inner := strings.TrimSpace(expr[1 : len(expr)-1])
		if inner == expr {
			break
		}
		expr = inner
	}

	name, present := cyphertext.ScanIdentifier(expr, 0)
	if !present {
		return false, false
	}
	index, unclosed := cyphertext.GapEnd(expr, name.End)
	if unclosed >= 0 {
		return false, false
	}
	next := keywordEndAt(expr, index, "IS")
	if next == index {
		return false, false
	}
	index = next
	index, unclosed = cyphertext.GapEnd(expr, index)
	if unclosed >= 0 {
		return false, false
	}
	isNot := false
	if end := keywordEndAt(expr, index, "NOT"); end > index {
		isNot = true
		index, unclosed = cyphertext.GapEnd(expr, end)
	}
	if unclosed >= 0 {
		return false, false
	}
	next = keywordEndAt(expr, index, "NULL")
	if next == index {
		return false, false
	}
	index = next
	index, unclosed = cyphertext.GapEnd(expr, index)
	if unclosed >= 0 || index != len(expr) {
		return false, false
	}
	value, exists := vars[name.Value]
	isNull := !exists || value == nil

	if isNot {
		return !isNull, true
	}
	return isNull, true
}

// parseTopLevelUnionBranches preserves whether a composition mixes UNION and
// UNION ALL so callers can report the Cypher semantic error rather than
// treating the statement as an unsplittable query.
func parseTopLevelUnionBranches(ctx context.Context, body queryBody) (branches []queryBody, unionAllMode, mixed, ok bool) {
	if body.status == queryBodyDeclined || len(body.branches) < 2 {
		return nil, false, false, false
	}
	unionAllMode = true
	for i, branch := range body.branches {
		branches = append(branches, body.branch(i))
		if i > 0 && !branch.unionAll {
			unionAllMode = false
		}
	}
	return branches, unionAllMode, body.status == queryBodyMixedUnion, true
}

// parseTopLevelUnionTextBranches is the explicit source admission boundary.
func parseTopLevelUnionTextBranches(ctx context.Context, query string) (branches []string, unionAllMode, mixed, ok bool) {
	body := readQueryBodyText(ctx, query, 0, len(query))
	_, unionAllMode, mixed, ok = parseTopLevelUnionBranches(ctx, body)
	if !ok {
		return nil, unionAllMode, mixed, false
	}
	for _, branch := range body.branches {
		branches = append(branches, query[branch.start:branch.end])
	}
	return branches, unionAllMode, mixed, true
}
func queryStatsMayContainNodeMutation(stats *QueryStats) bool {
	return stats != nil && (stats.PropertiesSet > 0 || stats.LabelsAdded > 0 || stats.LabelsRemoved > 0)
}

func callSubqueryLookupKeyString(v interface{}) string {
	switch x := v.(type) {
	case nil:
		return "<nil>"
	case string:
		return "s:" + normalizeCallSubqueryLookupString(x)
	case []byte:
		return "s:" + normalizeCallSubqueryLookupString(string(x))
	case int:
		return fmt.Sprintf("i:%d", x)
	case int64:
		return fmt.Sprintf("i64:%d", x)
	case float64:
		return fmt.Sprintf("f:%g", x)
	case bool:
		if x {
			return "b:1"
		}
		return "b:0"
	default:
		return fmt.Sprintf("%T:%v", v, v)
	}
}

func normalizeCallSubqueryLookupString(s string) string {
	s = strings.TrimSpace(s)
	return strings.Trim(s, `"`)
}

func parseCallSubqueryVarProp(expr string) (string, string, bool) {
	expr = strings.TrimSpace(expr)
	dot := strings.Index(expr, ".")
	if dot <= 0 || dot >= len(expr)-1 {
		return "", "", false
	}
	v := strings.Trim(expr[:dot], "`")
	p := strings.Trim(expr[dot+1:], "`")
	if !isSimpleIdentifier(v) || !isSimpleIdentifier(p) {
		return "", "", false
	}
	return v, p, true
}

func splitTopLevelEqualityCallSubquery(expr string) (lhs, rhs string, ok bool) {
	paren, bracket, brace := 0, 0, 0
	for i := 0; i < len(expr); i++ {
		if end, quoted := skipCypherLexicalToken(expr, i); quoted {
			i = end - 1
			continue
		}
		ch := expr[i]
		switch ch {
		case '(':
			paren++
			continue
		case ')':
			if paren > 0 {
				paren--
			}
			continue
		case '[':
			bracket++
			continue
		case ']':
			if bracket > 0 {
				bracket--
			}
			continue
		case '{':
			brace++
			continue
		case '}':
			if brace > 0 {
				brace--
			}
			continue
		}
		if paren != 0 || bracket != 0 || brace != 0 {
			continue
		}
		if ch == '=' {
			left := strings.TrimSpace(expr[:i])
			right := strings.TrimSpace(expr[i+1:])
			if left == "" || right == "" {
				return "", "", false
			}
			return left, right, true
		}
	}
	return "", "", false
}

func indexTopLevelKeywordCallSubquery(s string, keyword string) int {
	paren, bracket, brace := 0, 0, 0
	for i := 0; i < len(s); i++ {
		if end, quoted := skipCypherLexicalToken(s, i); quoted {
			i = end - 1
			continue
		}
		ch := s[i]
		switch ch {
		case '(':
			paren++
		case ')':
			if paren > 0 {
				paren--
			}
		case '[':
			bracket++
		case ']':
			if bracket > 0 {
				bracket--
			}
		case '{':
			brace++
		case '}':
			if brace > 0 {
				brace--
			}
		}
		if paren != 0 || bracket != 0 || brace != 0 {
			continue
		}
		if findKeywordIndex(s[i:], keyword) == 0 {
			return i
		}
	}
	return -1
}

func splitTopLevelResultModifiersCallSubquery(returnPart string) (projection string, modifiers string) {
	idx := indexTopLevelKeywordCallSubquery(returnPart, "ORDER BY")
	if idx < 0 {
		idx = indexTopLevelKeywordCallSubquery(returnPart, "SKIP")
	}
	if idx < 0 {
		idx = indexTopLevelKeywordCallSubquery(returnPart, "LIMIT")
	}
	if idx < 0 {
		return strings.TrimSpace(returnPart), ""
	}
	return strings.TrimSpace(returnPart[:idx]), strings.TrimSpace(returnPart[idx:])
}

func callSubqueryQueryIsWrite(query string) bool {
	// Single-pass: check each write keyword once with findKeywordIndex from position 0.
	// Previous implementation was O(n*m) — called findKeywordIndex from every byte offset.
	return findKeywordIndex(query, "CREATE") >= 0 ||
		findKeywordIndex(query, "MERGE") >= 0 ||
		findKeywordIndex(query, "DELETE") >= 0 ||
		findKeywordIndex(query, "SET") >= 0 ||
		findKeywordIndex(query, "REMOVE") >= 0
}

func isSimpleIdentifier(s string) bool {
	return cyphertext.IsIdentifier(strings.TrimSpace(strings.Trim(s, "`")))
}

func crossJoinCallResults(left, right *ExecuteResult) *ExecuteResult {
	if left == nil {
		return right
	}
	if right == nil {
		return left
	}

	colMap := make(map[string]struct{}, len(left.Columns))
	for _, col := range left.Columns {
		colMap[col] = struct{}{}
	}
	combinedCols := append([]string{}, left.Columns...)
	innerUniqueIdx := make([]int, 0, len(right.Columns))
	for i, col := range right.Columns {
		if _, exists := colMap[col]; !exists {
			combinedCols = append(combinedCols, col)
			innerUniqueIdx = append(innerUniqueIdx, i)
		}
	}

	rows := make([][]interface{}, 0, util.SafePreallocProduct(len(left.Rows), len(right.Rows)))
	for _, lrow := range left.Rows {
		for _, rrow := range right.Rows {
			joined := append([]interface{}{}, lrow...)
			for _, idx := range innerUniqueIdx {
				if idx >= 0 && idx < len(rrow) {
					joined = append(joined, rrow[idx])
				} else {
					joined = append(joined, nil)
				}
			}
			rows = append(rows, joined)
		}
	}

	return &ExecuteResult{Columns: combinedCols, Rows: rows}
}

// applyResultModifiers applies ORDER BY, LIMIT, SKIP to a result
func (e *StorageExecutor) applyResultModifiers(ctx context.Context, result *ExecuteResult, modifiers string) (*ExecuteResult, error) {
	skip := 0
	if value, ok := e.parseIntModifier(ctx, modifiers, "SKIP"); ok {
		skip = value
	}
	limit := -1
	if value, ok := e.parseIntModifier(ctx, modifiers, "LIMIT"); ok {
		limit = value
	}
	return e.applyCallResultOrderWindow(ctx, result, parseOrderByTerms(modifiers), skip, limit)
}

// applyOrderByToResult applies ORDER BY to a result set
func (e *StorageExecutor) applyOrderByToResult(result *ExecuteResult, orderByClause string) *ExecuteResult {
	terms := parseOrderByTerms(orderByClause)
	if len(terms) == 0 {
		terms = parseOrderByClause(orderByClause)
	}
	_, _ = e.applyCallResultOrderWindow(context.Background(), result, terms, 0, -1)
	return result
}

func (e *StorageExecutor) applyCallResultOrderWindow(ctx context.Context, result *ExecuteResult, terms []orderByTerm, skip, limit int) (*ExecuteResult, error) {
	rows := make([]pipelineRow, 0, len(result.Rows))
	for _, values := range result.Rows {
		row := make(pipelineRow, len(result.Columns))
		for index, column := range result.Columns {
			if index < len(values) {
				row[column] = values[index]
			} else {
				row[column] = nil
			}
		}
		for _, term := range terms {
			parts := strings.SplitN(term.column, ".", 2)
			if len(parts) != 2 {
				continue
			}
			if value, ok := row[parts[0]].(map[string]interface{}); ok {
				if _, nested := value["properties"].(map[string]interface{}); nested {
					row[term.column] = extractPropertyFromValue(value, parts[1])
				}
			}
		}
		rows = append(rows, row)
	}
	if !e.orderPipelineRows(ctx, rows, terms) {
		if err := getExpressionFailure(ctx); err != nil {
			return nil, err
		}
	}
	rows = applyPipelineWindow(rows, skip, limit)
	result.Rows = make([][]interface{}, 0, len(rows))
	for _, row := range rows {
		values := make([]interface{}, len(result.Columns))
		for index, column := range result.Columns {
			values[index] = row[column]
		}
		result.Rows = append(result.Rows, values)
	}
	return result, nil
}

type orderByTerm struct {
	column     string
	descending bool
}

// parseOrderByTerms parses the ORDER BY of a projection's modifiers. Keywords
// inside braces (COLLECT { … ORDER BY … }, map literals) belong to nested
// expressions, never to the projection (#652, #547).
func parseOrderByTerms(modifiers string) []orderByTerm {
	orderByIndex := topLevelKeywordIndex(modifiers, "ORDER BY")
	if orderByIndex < 0 {
		return nil
	}
	return parseOrderByClause(modifiers[keywordEndAt(modifiers, orderByIndex, "ORDER BY"):])
}

// parseOrderByClause parses the sort items that follow ORDER BY. The list ends
// at SKIP / LIMIT, and at the WHERE that may follow ORDER BY in a WITH clause
// (WITH n ORDER BY n.x WHERE n.y > 0): that WHERE filters the sorted rows and
// is not part of the last sort item. A word at the start is the first sort
// item, even one named like those keywords (ORDER BY skip{.a}).
func parseOrderByClause(clause string) []orderByTerm {
	clause = strings.TrimSpace(clause)
	end := len(clause)
	keywordAfterFirstItem := func(keyword string) int {
		index := topLevelKeywordIndex(clause, keyword)
		if index == 0 {
			opts := defaultKeywordScanOpts()
			opts.SkipBraces = true
			index = keywordIndexFrom(clause, keyword, len(keyword), opts)
		}
		return index
	}
	for _, keyword := range []string{"LIMIT", "SKIP"} {
		if index := keywordAfterFirstItem(keyword); index >= 0 && index < end {
			end = index
		}
	}
	if index := keywordAfterFirstItem("WHERE"); index >= 0 && index < end {
		end = index
	}
	clause = strings.TrimSpace(clause[:end])
	parts := splitTopLevelComma(clause)
	terms := make([]orderByTerm, 0, len(parts))
	for _, part := range parts {
		expression := strings.TrimSpace(part)
		fields := strings.Fields(expression)
		if len(fields) == 0 {
			continue
		}
		term := orderByTerm{column: expression}
		if len(fields) > 1 {
			direction := upperASCII(fields[len(fields)-1])
			switch direction {
			case "DESC", "DESCENDING":
				term.descending = true
				term.column = strings.TrimSpace(expression[:strings.LastIndex(expression, fields[len(fields)-1])])
			case "ASC", "ASCENDING":
				term.column = strings.TrimSpace(expression[:strings.LastIndex(expression, fields[len(fields)-1])])
			}
		}
		terms = append(terms, term)
	}
	return terms
}

// compareValuesForSort compares two values for sorting, returns -1, 0, or 1
// extractPropertyFromValue extracts a named property from a value that may be a node map.
func extractPropertyFromValue(val interface{}, propName string) interface{} {
	property, _ := rowPropertyValue(val, propName)
	return property
}

func compareValuesForSort(a, b interface{}) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		if cypherValueKindOf(b) == valueKindNull {
			return 0
		}
		return 1
	}
	if b == nil {
		if cypherValueKindOf(a) == valueKindNull {
			return 0
		}
		return -1
	}
	if comparison, temporal := compareTemporalOrdering(a, b); temporal {
		return comparison
	}
	if comparison, points := comparePointOrdering(a, b); points {
		return comparison
	}

	aRank := cypherSortRank(a)
	bRank := cypherSortRank(b)
	if aRank != bRank {
		if aRank < bRank {
			return -1
		}
		return 1
	}
	if left, isDuration := asCypherDuration(a); isDuration {
		if right, bothDurations := asCypherDuration(b); bothDurations {
			return compareDurationOrdering(left, right)
		}
	}

	if aList, ok := cypherSortList(a); ok {
		bList, _ := cypherSortList(b)
		for index := 0; index < len(aList) && index < len(bList); index++ {
			if comparison := compareValuesForSort(aList[index], bList[index]); comparison != 0 {
				return comparison
			}
		}
		return compareOrderedInts(len(aList), len(bList))
	}
	if comparison, exact := compareCypherNumbersExactly(a, b); exact {
		return comparison
	}
	if aNumber, ok := strictNumericValue(a); ok {
		bNumber, _ := strictNumericValue(b)
		if math.IsNaN(aNumber) && math.IsNaN(bNumber) {
			return 0
		}
		if aNumber < bNumber {
			return -1
		}
		if aNumber > bNumber {
			return 1
		}
		// -0.0 orders before 0.0 (Neo4j orders floats as Java's
		// Double.compare does), so max picks 0.0 and min -0.0.
		if _, leftFloat := a.(float64); leftFloat && aNumber == 0 {
			if _, rightFloat := b.(float64); rightFloat && math.Signbit(aNumber) != math.Signbit(bNumber) {
				if math.Signbit(aNumber) {
					return -1
				}
				return 1
			}
		}
		return 0
	}
	switch left := a.(type) {
	case CypherVector:
		return compareVectors(left, b.(CypherVector))
	case CypherUUID:
		return compareUUIDs(left, b.(CypherUUID))
	case string:
		right := b.(string)
		if left < right {
			return -1
		}
		if left > right {
			return 1
		}
		return 0
	case bool:
		right := b.(bool)
		if left == right {
			return 0
		}
		if !left {
			return -1
		}
		return 1
	}

	if comparison, _, structured := compareCypherStructuredValues(a, b, compareSortableValues); structured {
		return comparison
	}
	sa := fmt.Sprintf("%v", a)
	sb := fmt.Sprintf("%v", b)
	if sa < sb {
		return -1
	} else if sa > sb {
		return 1
	}
	return 0
}

func compareOrderedInts(left, right int) int {
	if left < right {
		return -1
	}
	if left > right {
		return 1
	}
	return 0
}

func parseOrderByModifier(modifiers string) (column string, descending bool, ok bool) {
	terms := parseOrderByTerms(modifiers)
	if len(terms) == 0 {
		return "", false, false
	}
	return terms[0].column, terms[0].descending, terms[0].column != ""
}

// parseIntModifier returns the SKIP or LIMIT value in a RETURN/WITH modifier
// tail. A plain integer is read directly; anything else (1 + 1, $s + 1 after
// parameter substitution, toInteger('2')) is evaluated as a constant
// expression through evaluatePipelinePagination, the same evaluation WITH uses,
// so every route applies SKIP/LIMIT expressions instead of ignoring them.
// ok is false when the keyword is absent or the value is not a non-negative
// integer.
func (e *StorageExecutor) parseIntModifier(ctx context.Context, modifiers, keyword string) (value int, ok bool) {
	idx := topLevelKeywordIndex(modifiers, keyword)
	if idx == -1 {
		return 0, false
	}
	kwPart := strings.TrimSpace(modifiers[idx+len(keyword):])
	nextKw := len(kwPart)
	for _, otherKeyword := range []string{"LIMIT", "SKIP", "ORDER BY"} {
		if otherKeyword == keyword {
			continue
		}
		if kidx := topLevelKeywordIndex(kwPart, otherKeyword); kidx != -1 && kidx < nextKw {
			nextKw = kidx
		}
	}
	vs := strings.TrimSpace(kwPart[:nextKw])
	if v, err := strconv.Atoi(vs); err == nil {
		return v, true
	}
	if vs == "" {
		return 0, false
	}
	return e.evaluatePipelinePagination(ctx, vs, nil)
}

func findColumnIndexByName(cols []string, name string) int {
	for i, col := range cols {
		if col == name {
			return i
		}
	}
	return -1
}

type rowRefForOrder struct {
	row []interface{}
	idx int
}

func compareRowRefsForOrder(a, b rowRefForOrder, colIdx int, descending bool) int {
	var av, bv interface{}
	if colIdx >= 0 && colIdx < len(a.row) {
		av = a.row[colIdx]
	}
	if colIdx >= 0 && colIdx < len(b.row) {
		bv = b.row[colIdx]
	}
	cmp := compareValuesForSort(av, bv)
	if descending {
		cmp = -cmp
	}
	if cmp != 0 {
		return cmp
	}
	// Stable tie-breaker by original position.
	if a.idx < b.idx {
		return -1
	}
	if a.idx > b.idx {
		return 1
	}
	return 0
}

type topKRowsHeap struct {
	items      []rowRefForOrder
	colIdx     int
	descending bool
}

func (h topKRowsHeap) Len() int { return len(h.items) }
func (h topKRowsHeap) Less(i, j int) bool {
	// Keep worst row at heap top.
	return compareRowRefsForOrder(h.items[i], h.items[j], h.colIdx, h.descending) > 0
}
func (h topKRowsHeap) Swap(i, j int) { h.items[i], h.items[j] = h.items[j], h.items[i] }
func (h *topKRowsHeap) Push(x interface{}) {
	h.items = append(h.items, x.(rowRefForOrder))
}
func (h *topKRowsHeap) Pop() interface{} {
	n := len(h.items)
	it := h.items[n-1]
	h.items = h.items[:n-1]
	return it
}

func selectTopKRowsForOrder(rows [][]interface{}, colIdx int, descending bool, k int) [][]interface{} {
	if k <= 0 || len(rows) == 0 {
		return [][]interface{}{}
	}
	if k >= len(rows) {
		out := make([][]interface{}, len(rows))
		copy(out, rows)
		sort.SliceStable(out, func(i, j int) bool {
			ri := rowRefForOrder{row: out[i], idx: i}
			rj := rowRefForOrder{row: out[j], idx: j}
			return compareRowRefsForOrder(ri, rj, colIdx, descending) < 0
		})
		return out
	}

	h := &topKRowsHeap{
		items:      make([]rowRefForOrder, 0, k),
		colIdx:     colIdx,
		descending: descending,
	}
	for i, row := range rows {
		ref := rowRefForOrder{row: row, idx: i}
		if h.Len() < k {
			heap.Push(h, ref)
			continue
		}
		// Replace current worst when new row is better.
		if compareRowRefsForOrder(ref, h.items[0], colIdx, descending) < 0 {
			h.items[0] = ref
			heap.Fix(h, 0)
		}
	}

	selected := make([]rowRefForOrder, len(h.items))
	copy(selected, h.items)
	sort.SliceStable(selected, func(i, j int) bool {
		return compareRowRefsForOrder(selected[i], selected[j], colIdx, descending) < 0
	})

	out := make([][]interface{}, 0, len(selected))
	for _, it := range selected {
		out = append(out, it.row)
	}
	return out
}

// Patterns compiled once (#591).
var (
	// callPropertyAccessPattern is a variable.property reference in a CALL.
	callPropertyAccessPattern = regexp.MustCompile(`(\w+)\.(\w+)`)
)
