package cypher

import "strings"

// This file is the one reader of query-body structure: the container kinds,
// every braced body in a text, a body's UNION branches with their leading USE
// and trailing FINISH, their clauses, and a CALL clause's body, each with its
// byte span in the text read. It reads structure only, no scope, so a rule
// that needs no scope (how a body concludes, what an expression subquery may
// hold) runs even where a statement's scope can't be resolved; scope and
// result-schema facts are built on it. It is built on execution's own
// readers (parseTopLevelUnionBranches, splitPipelineClausesAllowingProcedureCalls,
// stripTrailingFinish, parseUseClause, callSubqueryBraces), so execution,
// validation and result metadata read a body alike.

// queryBodyContainerKeywords are the keywords that open a braced query body,
// the container kinds of both parsers: CypherParser.g4's callSubquery,
// existsSubquery, countSubquery and collectSubquery
// (TestQueryBodyContainersMatchGrammar). The brace scanners and the query
// body traversal (forEachQueryBody) read this one list.
var queryBodyContainerKeywords = [...]string{"CALL", "EXISTS", "COUNT", "COLLECT"}

// queryBodyContainerKeyword reports whether word is a container keyword.
func queryBodyContainerKeyword(word string) bool {
	for _, keyword := range queryBodyContainerKeywords {
		if strings.EqualFold(word, keyword) {
			return true
		}
	}
	return false
}

// forEachQueryBody calls visit with every braced query body in text, as its
// span text[start:end], and the keyword of its container (one of
// containers), at any depth: the scan reads every brace, so a body nested in
// another body, in an expression, a comprehension, a CASE or a branch that
// ends with FINISH is visited like a top-level one, and CALL in every form
// (scoped, OPTIONAL, IN TRANSACTIONS).
func forEachQueryBody(text string, containers []string, visit func(container string, start, end int) error) error {
	if strings.IndexByte(text, '{') < 0 {
		return nil
	}
	for index := 0; index < len(text); index++ {
		switch character := text[index]; {
		case character == '\'' || character == '"' || character == '`':
			index = skipCypherQuotedText(text, index, character) - 1
			continue
		case character == '/':
			if end := queryCommentEnd(text, index); end >= 0 {
				index = end - 1
			}
			continue
		case character != '{':
			continue
		}
		container := queryBodyContainerBefore(text, index)
		if container == "" || !containsString(containers, container) || !precedingSubqueryExpressionKeyword(text, index) {
			continue
		}
		end := findMatchingDelimiter(text, index, '{', '}')
		if end < 0 {
			return nil
		}
		if err := visit(container, index+1, end); err != nil {
			return err
		}
	}
	return nil
}

// queryBodyContainerBefore is the container keyword before a body's brace,
// upper-cased (CALL for CALL (a, b) { … }), or "".
func queryBodyContainerBefore(text string, brace int) string {
	index := brace - 1
	for index >= 0 && isCypherWhitespace(text[index]) {
		index--
	}
	if index >= 0 && text[index] == ')' {
		depth := 0
		for ; index >= 0; index-- {
			if text[index] == ')' {
				depth++
			} else if text[index] == '(' {
				depth--
				if depth == 0 {
					break
				}
			}
		}
		index--
		for index >= 0 && isCypherWhitespace(text[index]) {
			index--
		}
	}
	end := index + 1
	for index >= 0 && isIdentByte(text[index]) {
		index--
	}
	if end <= index+1 {
		return ""
	}
	if word := text[index+1 : end]; queryBodyContainerKeyword(word) {
		return upperASCII(word)
	}
	return ""
}

// queryBodyStatus is the typed outcome of reading a query body: a body the
// reader can't split is never read as an empty one.
type queryBodyStatus uint8

const (
	// queryBodyRead: every branch split, every span located.
	queryBodyRead queryBodyStatus = iota
	// queryBodyDeclined: the clause splitter declines a branch, or rewrote
	// its text so its spans can't be located (the branch is declined).
	queryBodyDeclined
	// queryBodyMixedUnion: UNION mixed with UNION ALL, Neo4j's semantic
	// error.
	queryBodyMixedUnion
)

// queryBody is a read query body: its UNION branches and how it was read.
type queryBody struct {
	branches []queryBodyBranch
	status   queryBodyStatus
}

// queryBodyBranch is one UNION branch of a query body: its span (UNION
// excluded), whether UNION ALL joins it to the branch before, its USE
// clause's span (-1, -1 when none: a leading USE, or one after a legacy
// importing WITH, CALL { WITH x USE g … }), its clauses excluding the USE
// (nil when declined; the importing WITH is kept), the trailing FINISH that
// execution's rule strips (stripTrailingFinish: a FINISH after RETURN isn't
// stripped and stays in the RETURN clause's text) and its span (-1, -1 when
// none), and whether the reader declined it.
type queryBodyBranch struct {
	start, end             int
	unionAll               bool
	useStart, useEnd       int
	clauses                []queryBodyClause
	finished               bool
	finishStart, finishEnd int
	declined               bool
}

// queryBodyClause is one clause of a body branch and its span in the source
// read: text is source[start:end].
type queryBodyClause struct {
	pipelineClause
	start, end int
}

// readQueryBody reads source[start:end] (a whole statement or a braced
// body) into its branches; every span is absolute in source, so a nested
// body is read as a window of the same source.
func readQueryBody(source string, start, end int) queryBody {
	body := source[start:end]
	texts, unionAll := []string{body}, false
	read := queryBody{status: queryBodyRead}
	if parts, all, mixed, union := parseTopLevelUnionBranches(body); union && len(parts) > 0 {
		texts, unionAll = parts, all
		if mixed {
			read.status = queryBodyMixedUnion
		}
	}
	read.branches = make([]queryBodyBranch, 0, len(texts))
	cursor := start
	for index, part := range texts {
		branch := readQueryBodyBranch(source, cursor, end, part)
		branch.unionAll = index > 0 && unionAll
		if branch.end >= 0 {
			cursor = branch.end
		}
		if branch.declined && read.status == queryBodyRead {
			read.status = queryBodyDeclined
		}
		read.branches = append(read.branches, branch)
	}
	return read
}

// readQueryBodyBranch reads one branch, whose text part stands in
// source[from:to]. Its clauses are split from the source's own windows on
// each side of the USE clause, never from rewritten text.
func readQueryBodyBranch(source string, from, to int, part string) queryBodyBranch {
	branch := queryBodyBranch{start: -1, end: -1, useStart: -1, useEnd: -1, finishStart: -1, finishEnd: -1, declined: true}
	trimmed := strings.TrimSpace(part)
	found := strings.Index(source[from:to], trimmed)
	if trimmed == "" || found < 0 {
		return branch
	}
	branch.start, branch.end = from+found, from+found+len(trimmed)
	// The windows of source holding the branch's clauses.
	windows := [][2]int{{branch.start, branch.end}}
	if useStart, useEnd, rest, ok := queryBodyUseClause(trimmed); ok {
		branch.useStart, branch.useEnd = branch.start+useStart, branch.start+useEnd
		windows = [][2]int{{branch.start, branch.start + useStart}, {branch.start + rest, branch.end}}
		if useStart == 0 {
			windows = windows[1:]
		}
	} else if useStart >= 0 {
		return branch // a USE clause the USE grammar rejects
	}
	last := &windows[len(windows)-1]
	tail := source[last[0]:last[1]]
	if remainder, finished := stripTrailingFinish(tail); finished {
		end := last[0] + lastLiveByte(tail, len(tail)) + 1
		branch.finished, branch.finishStart, branch.finishEnd = true, end-len("FINISH"), end
		at := strings.Index(tail, remainder)
		if at < 0 {
			return branch
		}
		last[1] = last[0] + at + len(remainder)
	}
	for _, window := range windows {
		clauses, ok := queryBodyWindowClauses(source, window[0], window[1])
		if !ok {
			return branch
		}
		branch.clauses = append(branch.clauses, clauses...)
	}
	branch.declined = false
	return branch
}

// queryBodyUseClause finds a branch's USE clause in text (a trimmed branch):
// a leading one, or one after the branch's preceding clauses, read
// with the one USE grammar (parseUseClause). It reports the clause's span,
// where the clauses after it start, and ok; start is 0 with ok false for a
// USE clause the grammar rejects, -1 when there is none.
func queryBodyUseClause(text string) (start, end, rest int, ok bool) {
	candidates := findAllTopLevelPipelineKeywordPositions(text, "USE")
	for _, position := range candidates {
		if position > queryGapEnd(text, 0) {
			// Locate graph selection against the original execution clauses,
			// including projections that precede a participant fragment.
			if _, split := splitPipelineClausesAllowingProcedureCalls(text[:position]); !split {
				continue
			}
		}
		_, remaining, hasUse, err := parseUseClause(text[position:], true)
		switch {
		case !hasUse:
			continue
		case err != nil:
			if position == 0 {
				return 0, 0, 0, false
			}
			continue
		case !strings.HasSuffix(text, remaining):
			return 0, 0, 0, false
		}
		rest = len(text) - len(remaining)
		return position, position + len(strings.TrimSpace(text[position:rest])), rest, true
	}
	return -1, -1, -1, false
}

// queryBodyWindowClauses splits source[start:end] into clauses with their
// spans; ok is false when the splitter declines it or its text can't be
// located in the window. An OPTIONAL CALL's clause spans its CALL (the
// splitter's text) and is marked optional.
func queryBodyWindowClauses(source string, start, end int) ([]queryBodyClause, bool) {
	text := source[start:end]
	if strings.TrimSpace(text) == "" {
		return nil, true // CALL { FINISH }
	}
	clauses, ok := splitPipelineClausesAllowingProcedureCalls(strings.TrimSpace(text))
	if !ok {
		return nil, false
	}
	within := 0
	located := make([]queryBodyClause, len(clauses))
	for position, clause := range clauses {
		found := strings.Index(text[within:], clause.text)
		if found < 0 {
			return nil, false
		}
		at := start + within + found
		located[position] = queryBodyClause{pipelineClause: clause, start: at, end: at + len(clause.text)}
		within += found + len(clause.text)
	}
	return located, true
}

// queryStatementWindow is the window of source that holds a statement's
// query: past leading whitespace and comments (queryGapEnd), the CYPHER
// preamble and EXPLAIN / PROFILE (stripCypherPreamble, parseExecutionMode),
// before an optional trailing ';' (trimTrailingStatementDelimiters); ok is
// false when those owners leave a text that isn't the end of source's.
func queryStatementWindow(source string) (start, end int, mode ExecutionMode, ok bool) {
	start = queryGapEnd(source, 0)
	trimmed := trimTrailingStatementDelimiters(source[start:])
	if !strings.HasPrefix(source[start:], trimmed) {
		return -1, -1, ModeNormal, false
	}
	end = start + len(trimmed)
	rest, _ := stripCypherPreamble(trimmed)
	mode, rest = parseExecutionMode(rest)
	if !strings.HasSuffix(trimmed, rest) {
		return -1, -1, mode, false
	}
	return end - len(rest), end, mode, true
}

// callSubqueryBraces are the positions of a CALL subquery clause's opening
// brace and its matching closing brace in clause; ok is false when it has
// no body. A brace in quoted text isn't one (CALL () { RETURN '}' AS y }).
// Execution (parseCallSubquery) and the body reader both read it here.
func callSubqueryBraces(clause string) (open, end int, ok bool) {
	if !startsWithKeywordFold(clause[queryGapEnd(clause, 0):], "CALL") {
		return -1, -1, false
	}
	// The scope reader owns CALL's frame. A procedure's next token is its
	// name, so a brace in its arguments, predicate or a later CALL is not
	// this CALL's body.
	open = readCallSubqueryScope(clause).bodyOffset
	if open >= len(clause) || clause[open] != '{' {
		return -1, -1, false
	}
	end = findMatchingDelimiter(clause, open, '{', '}')
	if end < 0 {
		return -1, -1, false
	}
	return open, end, true
}

// callClauseBody is the body window of a CALL subquery clause (CALL (a) {
// … } IN TRANSACTIONS) in the source its clause was read from; ok is false
// for any other clause.
func callClauseBody(clause queryBodyClause) (start, end int, ok bool) {
	if clause.kind != pipelineClauseCallSubquery {
		return -1, -1, false
	}
	open, close, ok := callSubqueryBraces(clause.text)
	if !ok {
		return -1, -1, false
	}
	return clause.start + open + 1, clause.start + close, true
}
