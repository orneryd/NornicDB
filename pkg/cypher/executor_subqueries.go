package cypher

import (
	"context"
	"errors"
	"fmt"
	math "github.com/orneryd/nornicdb/pkg/math/libm"
	"regexp"
	"strconv"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/pkg/util"
)

// ===== CALL {} Subquery Support (Neo4j 4.0+) =====

// startsWithCallSubquery reports whether cypher begins with a CALL { } or
// CALL (vars) { } subquery clause (as opposed to a procedure CALL, or a
// subquery later in the statement).
func startsWithCallSubquery(cypher string) bool {
	return callSubqueryAt(strings.TrimSpace(cypher), 0)
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

// parseCallSubquery extracts the body from CALL { ... } and any trailing clauses
// Returns: body, afterCall, inTransactions bool, batchSize int
func (e *StorageExecutor) parseCallSubquery(cypher string) (body, afterCall string, inTransactions bool, batchSize int) {
	batchSize = 1000 // Default batch size

	trimmed := strings.TrimSpace(cypher)

	// Find the opening brace
	braceStart := strings.Index(trimmed, "{")
	if braceStart == -1 {
		return "", "", false, batchSize
	}

	// The matching closing brace; a brace in quoted text isn't one
	// (CALL () { RETURN '}' AS y } …).
	braceEnd := findMatchingDelimiter(trimmed, braceStart, '{', '}')
	if braceEnd < 0 {
		return "", "", false, batchSize
	}

	// Extract body (between braces)
	body = strings.TrimSpace(trimmed[braceStart+1 : braceEnd])
	if stripped, finishes := stripUnionBranchFinishes(body); finishes && strings.TrimSpace(stripped) != "" {
		body = stripped
	}

	// Get what's after the closing brace
	afterCall = strings.TrimSpace(trimmed[braceEnd+1:])

	// Check for IN TRANSACTIONS
	upperAfter := upperASCII(afterCall)
	if strings.HasPrefix(upperAfter, "IN TRANSACTIONS") {
		inTransactions = true
		afterTx := strings.TrimSpace(afterCall[15:])
		upperAfterTx := upperASCII(afterTx)

		// Check for OF n ROWS
		if strings.HasPrefix(upperAfterTx, "OF ") {
			// Parse batch size
			ofPart := afterTx[3:]
			// Find ROWS keyword
			rowsIdx := strings.Index(upperASCII(ofPart), " ROWS")
			if rowsIdx > 0 {
				sizeStr := strings.TrimSpace(ofPart[:rowsIdx])
				if size, err := strconv.Atoi(sizeStr); err == nil && size > 0 {
					batchSize = size
				}
				afterCall = strings.TrimSpace(ofPart[rowsIdx+5:])
			} else {
				afterCall = ""
			}
		} else {
			afterCall = afterTx
		}
	}

	return body, afterCall, inTransactions, batchSize
}

type callSubqueryScopeClause struct {
	names      []string
	scoped     bool
	text       string
	bodyOffset int
	invalid    bool
}

// readCallSubqueryScope is the shared static/runtime owner of CALL's scope
// clause. scoped distinguishes CALL (*) from legacy CALL { ... }; a nil name
// list means all outer variables, and a non-nil empty list means CALL ().
// text and bodyOffset let validation use the same parsed clause. Legacy
// importing-WITH and implicit imports are resolved by their existing owners.
func readCallSubqueryScope(cypher string) callSubqueryScopeClause {
	trimmed := strings.TrimSpace(cypher)
	if findKeywordIndex(trimmed, "CALL") != 0 {
		return callSubqueryScopeClause{}
	}
	idx := skipSpaces(trimmed, len("CALL"))
	offset := strings.Index(cypher, trimmed)
	scope := callSubqueryScopeClause{bodyOffset: offset + idx}
	if idx >= len(trimmed) || trimmed[idx] != '(' {
		return scope
	}
	close := findMatchingCallParen(trimmed, idx)
	if close < 0 {
		return scope
	}
	scope.scoped = true
	scope.text = strings.TrimSpace(trimmed[idx+1 : close])
	scope.bodyOffset = offset + skipSpaces(trimmed, close+1)
	if scope.text == "*" {
		return scope
	}
	parts := splitProcedureTopLevelComma(scope.text)
	scope.names = make([]string, 0, len(parts))
	for _, part := range parts {
		if simpleSemanticIdentifier(part) == "" && scope.text != "" {
			scope.invalid = true
		}
		name := strings.Trim(strings.TrimSpace(part), "`")
		if isSimpleIdentifier(name) {
			scope.names = append(scope.names, name)
		}
	}
	return scope
}

// executeCallInTransactions executes a CALL {} IN TRANSACTIONS query
// This batches operations for large datasets by processing results in separate transactions.
//
// The subquery is executed in batches where each batch is processed in its own transaction.
// This is useful for large imports/updates to avoid memory issues and provide transaction boundaries.
//
// Example:
//
//	CALL {
//	  MATCH (p:Person)
//	  SET p.processed = true
//	  RETURN p.name AS name
//	} IN TRANSACTIONS OF 2 ROWS
//
// This will process Person nodes in batches of 2, each batch in a separate transaction.
//
// Strategy:
//  1. First execute the subquery to determine the total number of rows (read-only)
//  2. If it contains write operations, process in batches by adding LIMIT/SKIP to the MATCH
//  3. Each batch is executed in its own transaction via executeWithImplicitTransaction
//
// rejectCallInTransactionsInExplicitTx enforces the Neo4j rule that
// CALL { ... } IN TRANSACTIONS cannot run inside an explicit transaction:
// its batches would execute inside the caller's transaction and survive a
// ROLLBACK (#648).
func (e *StorageExecutor) rejectCallInTransactionsInExplicitTx() error {
	if e.txContext != nil && e.txContext.active {
		if tx, ok := e.txContext.tx.(*storage.BadgerTransaction); ok && tx.OperationCount() > 0 {
			return &classifiedCypherError{
				cause:  errors.New("Expected transaction state to be empty when calling transactional subquery. (Transactions committed: 0)"),
				code:   "Neo.DatabaseError.Statement.ExecutionFailed",
				detail: "InvalidCallInTransactions",
			}
		}
		return newSemanticError("Neo.DatabaseError.Transaction.TransactionStartFailed", "InvalidCallInTransactions",
			"CALL { ... } IN TRANSACTIONS is not allowed inside an explicit transaction")
	}
	return nil
}

// leadingWithClause returns the leading WITH clause of text, which starts
// with WITH (its WHERE, ORDER BY, SKIP and LIMIT included), and whether a
// clause follows it. Unlike splitPipelineClauses it reads no further than
// the next clause, in one pass, and it takes texts with procedure calls
// (WITH p CALL db.x() …).
func leadingWithClause(text string) (clause string, followed bool) {
	depth := 0
	for index := len("WITH"); index < len(text); index++ {
		switch c := text[index]; c {
		case '\'', '"', '`':
			index = skipCypherQuotedText(text, index, c) - 1
			continue
		case '(', '[', '{':
			depth++
			continue
		case ')', ']', '}':
			if depth > 0 {
				depth--
			}
			continue
		}
		if depth > 0 || !isIdentStartByte(text[index]) || isIdentByte(text[index-1]) {
			continue
		}
		end := index + 1
		for end < len(text) && isIdentByte(text[end]) {
			end++
		}
		previous := text[index-1]
		if previous != '.' && previous != '$' && previous != ':' && isLeadingWithFollower(text[index:end]) &&
			!(equalFoldASCII(text[index:end], "with") && isOperatorWith(text, index)) {
			return text[:index], true
		}
		index = end - 1
	}
	return text, false
}

// isLeadingWithFollower reports whether word starts a clause that can follow
// a leading WITH.
func isLeadingWithFollower(word string) bool {
	switch len(word) {
	case 3:
		return equalFoldASCII(word, "set") || equalFoldASCII(word, "use")
	case 4:
		return equalFoldASCII(word, "call") || equalFoldASCII(word, "with") || equalFoldASCII(word, "load")
	case 5:
		return equalFoldASCII(word, "match") || equalFoldASCII(word, "merge") || equalFoldASCII(word, "union")
	case 6:
		return equalFoldASCII(word, "unwind") || equalFoldASCII(word, "create") || equalFoldASCII(word, "insert") ||
			equalFoldASCII(word, "delete") || equalFoldASCII(word, "detach") || equalFoldASCII(word, "remove") ||
			equalFoldASCII(word, "return") || equalFoldASCII(word, "finish")
	case 7:
		return equalFoldASCII(word, "foreach")
	case 8:
		return equalFoldASCII(word, "optional") || equalFoldASCII(word, "nodetach")
	}
	return false
}

// unscopedCallImports reads the importing WITHs of an unscoped CALL
// subquery's body: whether any UNION branch (or the body, without UNION)
// starts with an importing WITH (branchLeadingWithImport), and the
// variables those WITHs list. A branch that imports nothing adds none,
// whatever the other branches import (#907); it still reads outer variables
// through NornicDB's implicit import (pipelineApplyCallSubqueryWithMetadata).
// A CALL with a scope clause has no importing WITH; its caller reads the
// scope clause instead (parsed.scoped).
//
// names is filled only when collectNames.
func unscopedCallImports(subqueryBody string, outer func(string) bool, collectNames bool) (names []string, importing bool, err error) {
	trimmed := strings.TrimSpace(subqueryBody)
	if indexASCIIFold(trimmed, "with") < 0 {
		return nil, false, nil
	}
	if indexASCIIFold(trimmed, "union") >= 0 {
		if branches, _, _, union := parseTopLevelUnionBranches(trimmed); union {
			for _, branch := range branches {
				branchNames, branchImports, err := branchLeadingWithImport(branch, outer, collectNames)
				if err != nil {
					return nil, false, err
				}
				if branchImports {
					importing = true
					names = append(names, branchNames...)
				}
			}
			return names, importing, nil
		}
	}
	return branchLeadingWithImport(trimmed, outer, collectNames)
}

// branchLeadingWithImport reports whether one branch of an unscoped CALL
// body starts with an importing WITH: WITH * or a WITH that reads a
// variable from outside (outer), and the variables it lists (not *). Any
// other leading WITH (WITH 1 AS x, WITH 2 AS i over an outer i) is an
// ordinary projection inside the subquery (#907). An importing WITH must
// be followed by a query clause: CALL { WITH x } imports and is an error.
func branchLeadingWithImport(branch string, outer func(string) bool, collectNames bool) (names []string, importing bool, err error) {
	trimmed := strings.TrimSpace(branch)
	if !startsWithKeywordFold(trimmed, "WITH") {
		return nil, false, nil
	}
	clause, followed := leadingWithClause(trimmed)
	projection, _ := projectionSemanticBodyAndTail(clause, "WITH")
	var buffer [8]string
	for _, item := range appendTopLevelComma(buffer[:0], projection) {
		expression, alias := parseProjectionExprAlias(item)
		expression = strings.TrimSpace(expression)
		if expression == "*" {
			importing = true
			continue
		}
		if collectNames {
			// The item's name: the variable itself for a plain reference.
			names = append(names, alias)
		} else if importing {
			break
		}
		if importing || expression == "" {
			continue
		}
		// A plain reference (the usual import) needs no expression scan.
		if expression[0] != '`' && isSimpleIdentifier(expression) {
			importing = outer(expression)
			continue
		}
		for _, variable := range expressionFreeVariables(expression) {
			if outer(variable) {
				importing = true
				break
			}
		}
	}
	if !importing {
		return nil, false, nil
	}
	if !followed {
		return names, true, localizedError(localization.CypherSubqueriesWithQueryClauseRequired(), nil)
	}
	return names, true, nil
}

// unscopedBranchLocals returns the outer variables (names) that one branch
// of an unscoped CALL body declares itself (branchDeclaresVariable): the
// branch doesn't receive their outer values through NornicDB's implicit
// import, so they are new variables there, as in Cypher (#907).
func unscopedBranchLocals(branch string, names []string) []string {
	var locals []string
	for _, name := range names {
		if branchDeclaresVariable(branch, name) {
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
func branchDeclaresVariable(branch, variable string) bool {
	clauses, ok := splitPipelineClauses(strings.TrimSpace(branch))
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
		switch c := text[index]; c {
		case '\'', '"', '`':
			index = skipCypherQuotedText(text, index, c) - 1
			continue
		}
		if text[index:index+len(variable)] != variable {
			continue
		}
		end := index + len(variable)
		if (index > 0 && isIdentByte(text[index-1])) || (end < len(text) && isIdentByte(text[end])) {
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
		if word := wordBefore(text, opening); word != "" && !isPatternClauseKeyword(word) {
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
func wordBefore(text string, index int) string {
	end := index
	for end > 0 && isASCIISpace(text[end-1]) {
		end--
	}
	start := end
	for start > 0 && isIdentByte(text[start-1]) {
		start--
	}
	return text[start:end]
}

// isPatternClauseKeyword reports whether word is a keyword a pattern
// follows: MATCH, CREATE, MERGE.
func isPatternClauseKeyword(word string) bool {
	return equalFoldASCII(word, "MATCH") || equalFoldASCII(word, "CREATE") || equalFoldASCII(word, "MERGE")
}

// precededByAS reports whether the word at index follows the keyword AS.
func precededByAS(text string, index int) bool {
	end := index
	for end > 0 && isASCIISpace(text[end-1]) {
		end--
	}
	return end >= 3 && equalFoldASCII(text[end-2:end], "AS") && !isIdentByte(text[end-3])
}

// previousNonSpace is the last non-space byte before index, 0 at the start.
func previousNonSpace(text string, index int) byte {
	for index > 0 {
		index--
		if !isASCIISpace(text[index]) {
			return text[index]
		}
	}
	return 0
}

// nextNonSpace is the first non-space byte at or after index, 0 at the end.
func nextNonSpace(text string, index int) byte {
	for ; index < len(text); index++ {
		if !isASCIISpace(text[index]) {
			return text[index]
		}
	}
	return 0
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

// parseTopLevelUnionBranches preserves whether a composition mixes UNION and
// UNION ALL so callers can report the Cypher semantic error rather than
// treating the statement as an unsplittable query.
func parseTopLevelUnionBranches(query string) (branches []string, unionAllMode, mixed, ok bool) {
	trimmed := strings.TrimSpace(query)
	if trimmed == "" {
		return nil, false, false, false
	}

	type sep struct {
		pos int
		end int
		all bool
	}
	seps := make([]sep, 0)
	depthParen := 0
	depthBracket := 0
	depthBrace := 0

	for i := 0; i < len(trimmed); i++ {
		ch := trimmed[i]
		switch ch {
		case '\'', '"', '`':
			// A string or a backticked name (`union`) holds no UNION.
			i = skipCypherQuotedText(trimmed, i, ch) - 1
			continue
		case '(':
			depthParen++
			continue
		case ')':
			if depthParen > 0 {
				depthParen--
			}
			continue
		case '[':
			depthBracket++
			continue
		case ']':
			if depthBracket > 0 {
				depthBracket--
			}
			continue
		case '{':
			depthBrace++
			continue
		case '}':
			if depthBrace > 0 {
				depthBrace--
			}
			continue
		}

		if depthParen != 0 || depthBracket != 0 || depthBrace != 0 {
			continue
		}
		// A variable named union is a name, not the operator (#894).
		if !matchKeywordAt(trimmed, i, "UNION") || clauseKeywordUsedAsName(trimmed, i, i+len("UNION"), "UNION") {
			continue
		}
		j := skipSpaces(trimmed, i+len("UNION"))
		all := false
		if matchKeywordAt(trimmed, j, "ALL") {
			all = true
			j = skipSpaces(trimmed, j+len("ALL"))
		}
		seps = append(seps, sep{pos: i, end: j, all: all})
		i = j - 1
	}

	if len(seps) == 0 {
		return nil, false, false, false
	}

	unionAllMode = true
	hasDistinct := false
	hasAll := false
	for _, s := range seps {
		if s.all {
			hasAll = true
		} else {
			hasDistinct = true
			unionAllMode = false
		}
	}
	mixed = hasDistinct && hasAll

	branches = make([]string, 0, util.SafePreallocSum(len(seps), 1))
	start := 0
	for _, s := range seps {
		part := strings.TrimSpace(trimmed[start:s.pos])
		if part == "" {
			return nil, false, false, false
		}
		branches = append(branches, part)
		start = s.end
	}
	last := strings.TrimSpace(trimmed[start:])
	if last == "" {
		return nil, false, false, false
	}
	branches = append(branches, last)
	return branches, unionAllMode, mixed, true
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
	s = strings.TrimSpace(strings.Trim(s, "`"))
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		ch := s[i]
		if i == 0 {
			if !isIdentStartByte(ch) {
				return false
			}
			continue
		}
		if !isIdentByte(ch) {
			return false
		}
	}
	return true
}

// processCallSubqueryReturn processes the RETURN clause after CALL {}
func (e *StorageExecutor) processCallSubqueryReturn(ctx context.Context, innerResult *ExecuteResult, afterCall string) (*ExecuteResult, error) {
	if findKeywordIndex(afterCall, "RETURN") == -1 {
		return innerResult, nil
	}
	rows := make([]pipelineRow, 0, len(innerResult.Rows))
	scope := make(map[string]struct{}, len(innerResult.Columns))
	for _, column := range innerResult.Columns {
		scope[column] = struct{}{}
	}
	for _, values := range innerResult.Rows {
		row := make(pipelineRow, len(innerResult.Columns))
		for index, column := range innerResult.Columns {
			if index < len(values) {
				row[column] = values[index]
			} else {
				row[column] = nil
			}
		}
		rows = append(rows, row)
	}
	clauses := []pipelineClause{{kind: pipelineClauseReturn, text: afterCall}}
	result, handled, err := e.runPipelineClauses(ctx, rows, scope, clauses, clauses)
	if err != nil {
		return nil, err
	}
	if !handled || result == nil {
		return nil, localizedError(localization.CypherSubqueriesAfterCallClauseUnsupported("RETURN"), nil)
	}
	result.Stats = innerResult.Stats
	return result, nil
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

// Patterns compiled once (#591).
var (
	// callPropertyAccessPattern is a variable.property reference in a CALL.
	callPropertyAccessPattern = regexp.MustCompile(`(\w+)\.(\w+)`)
	// variableNullCheckPattern is "variable IS [NOT] NULL".
	variableNullCheckPattern = regexp.MustCompile(`(?i)^([A-Za-z_][A-Za-z0-9_]*)\s+IS\s+(NOT\s+)?NULL$`)
)
