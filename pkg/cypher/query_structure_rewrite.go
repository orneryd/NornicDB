package cypher

import (
	"strconv"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
)

// Cypher 25 query composition (Neo4j 2025.06 to 2026.02), as the forms every
// route already runs, rewritten once where a statement enters the executor:
//
//   - q1 NEXT q2: q1's rows are q2's input, as CALL (*) { q1 } q2 (left to
//     right: q1 NEXT q2 NEXT q3 is CALL (*) { CALL (*) { q1 } q2 } q3);
//   - WHEN c1 THEN q1 [WHEN c2 THEN q2 …] [ELSE qn]: the first branch whose
//     condition is true runs. The conditions are evaluated once, before any
//     branch runs, as UNWIND [CASE WHEN c1 THEN 1 … END] AS b CALL (*) {
//     FILTER b = 1 q1 UNION ALL FILTER b = 2 q2 … } RETURN columns;
//   - { q } as a query part: the braces group q's UNIONs. They are dropped
//     where that changes nothing, and a UNION inside a UNION ALL becomes
//     CALL (*) { q } RETURN columns.
//
// The same holds inside a subquery body (CALL, EXISTS, COUNT, COLLECT). The
// rewrite's edits are kept (queryRewrite), so columns and messages show the
// client's text.

// whenBranchVariable holds the number of the conditional branch that runs; a
// * projection doesn't list it (isGeneratedVariable).
const whenBranchVariable = generatedVariablePrefix + "when"

type structureRewriter struct {
	query   string
	edits   []labelRewriteEdit
	columns func(query string) []string
}

// desugarQueryStructure returns query with NEXT, WHEN and braced query parts
// rewritten (see above) and the rewrite that maps the result back, or query
// and nil when it has none. columns names a query's result columns.
func desugarQueryStructure(query string, columns func(string) []string) (string, *queryRewrite, error) {
	if !mayUseQueryStructure(query) {
		return query, nil, nil
	}
	r := &structureRewriter{query: query, columns: columns}
	if err := r.region(0, len(query)); err != nil {
		return query, nil, err
	}
	if len(r.edits) == 0 {
		return query, nil, nil
	}
	rewritten, rewrite := applyLabelRewriteEdits(query, r.edits, true)
	return rewritten, rewrite, nil
}

// mayUseQueryStructure is desugarQueryStructure's quick check: NEXT or WHEN
// appear, or a brace starts the statement or follows UNION.
func mayUseQueryStructure(query string) bool {
	return containsFold(query, "NEXT") || containsFold(query, "WHEN") || strings.HasPrefix(strings.TrimSpace(query), "{") ||
		containsFold(query, "UNION")
}

func (r *structureRewriter) edit(start, end int, text string) {
	r.edits = append(r.edits, labelRewriteEdit{start: start, end: end, text: text})
}

// queryStructureClauseStarts are the words that start a query part after
// NEXT (a NEXT followed by anything else is a name).
var queryStructureClauseStarts = []string{
	"MATCH", "OPTIONAL", "WITH", "RETURN", "UNWIND", "CALL", "CREATE", "MERGE", "DELETE", "DETACH", "SET", "REMOVE",
	"FOREACH", "LOAD", "FILTER", "LET", "FOR", "FINISH", "USE", "WHEN", "INSERT", "ORDER", "SKIP", "LIMIT", "OFFSET",
}

// topLevelWord is a word at a region's top level.
type topLevelWord struct {
	start, end int
	upper      string
}

// topLevelWords lists the words outside quotes, parentheses, brackets and
// braces in q[start:end].
func topLevelWords(q string, start, end int) []topLevelWord {
	var words []topLevelWord
	for i := start; i < end; i++ {
		c := q[i]
		switch {
		case c == '\'' || c == '"' || c == '`':
			i = skipCypherQuotedText(q[:end], i, c) - 1
		case c == '/':
			if commentEnd := queryCommentEnd(q[:end], i); commentEnd >= 0 {
				i = commentEnd - 1
			}
		case c == '(' || c == '[' || c == '{':
			closer := map[byte]rune{'(': ')', '[': ']', '{': '}'}[c]
			if close := findMatchingDelimiter(q[:end], i, rune(c), closer); close >= 0 {
				i = close
			}
		case isIdentByte(c) && !isDigitByte(c):
			if i > start && (isIdentByte(q[i-1]) || q[i-1] == '$' || q[i-1] == '.') {
				continue
			}
			j := i
			for j < end && isIdentByte(q[j]) {
				j++
			}
			words = append(words, topLevelWord{start: i, end: j, upper: strings.ToUpper(q[i:j])})
			i = j - 1
		}
	}
	return words
}

// region rewrites q[start:end], a statement or a subquery body.
func (r *structureRewriter) region(start, end int) error {
	words := topLevelWords(r.query, start, end)
	var nexts []topLevelWord
	for index, word := range words {
		if word.upper != "NEXT" || index > 0 && words[index-1].upper == "AS" && words[index-1].end == skipBackSpaces(r.query, word.start) {
			continue
		}
		if before := skipBackSpaces(r.query, word.start); before > start && strings.IndexByte(".:$", r.query[before-1]) >= 0 {
			continue
		}
		if r.startsQueryPart(word.end, end) {
			nexts = append(nexts, word)
		}
	}
	if len(nexts) > 0 {
		first := skipASCIISpaces(r.query, start, end)
		r.edit(first, first, strings.Repeat("CALL (*) { ", len(nexts)))
	}
	segmentStart := start
	for _, next := range nexts {
		if err := r.segment(segmentStart, next.start); err != nil {
			return err
		}
		r.edit(next.start, next.end, "}")
		segmentStart = next.end
	}
	return r.segment(segmentStart, end)
}

// startsQueryPart reports whether a query part (a clause or a brace) starts
// after q[from] in q[:end].
func (r *structureRewriter) startsQueryPart(from, end int) bool {
	at := skipASCIISpaces(r.query, from, end)
	if at >= end {
		return false
	}
	if r.query[at] == '{' {
		return true
	}
	for _, keyword := range queryStructureClauseStarts {
		if matchKeywordAt(r.query[:end], at, keyword) {
			return true
		}
	}
	return false
}

func skipBackSpaces(q string, end int) int {
	for end > 0 && isASCIISpace(q[end-1]) {
		end--
	}
	return end
}

// segment rewrites a query part between NEXTs: a conditional or a union.
func (r *structureRewriter) segment(start, end int) error {
	at := skipASCIISpaces(r.query, start, end)
	if matchKeywordAt(r.query[:end], at, "WHEN") {
		return r.conditional(at, end)
	}
	return r.unionParts(at, end)
}

// unionParts rewrites q[start:end], parts joined by UNION [ALL]: a braced
// part loses its braces, or, when it holds a UNION inside a UNION ALL,
// becomes CALL (*) { … } RETURN columns.
func (r *structureRewriter) unionParts(start, end int) error {
	words := topLevelWords(r.query, start, end)
	type part struct{ start, end int }
	var parts []part
	outerAll, partStart := false, start
	for index, word := range words {
		if word.upper != "UNION" {
			continue
		}
		parts = append(parts, part{partStart, word.start})
		partStart = word.end
		if index+1 < len(words) && words[index+1].upper == "ALL" && skipASCIISpaces(r.query, word.end, end) == words[index+1].start {
			outerAll, partStart = true, words[index+1].end
		}
	}
	parts = append(parts, part{partStart, end})
	for _, p := range parts {
		open := skipASCIISpaces(r.query, p.start, p.end)
		close := skipBackSpaces(r.query, p.end) - 1
		if open < p.end && r.query[open] == '{' && findMatchingDelimiter(r.query[:p.end], open, '{', '}') == close {
			if err := r.region(open+1, close); err != nil {
				return err
			}
			if outerAll && r.hasDistinctUnion(open+1, close) {
				r.edit(open, open+1, "CALL (*) {")
				r.edit(close+1, close+1, returnColumnsText(r.columns(r.query[open+1:close])))
			} else {
				r.edit(open, open+1, "")
				r.edit(close, close+1, "")
			}
			continue
		}
		if err := r.subqueries(p.start, p.end); err != nil {
			return err
		}
	}
	return nil
}

// hasDistinctUnion reports whether q[start:end] has a UNION without ALL at
// its top level.
func (r *structureRewriter) hasDistinctUnion(start, end int) bool {
	words := topLevelWords(r.query, start, end)
	for index, word := range words {
		if word.upper == "UNION" && (index+1 >= len(words) || words[index+1].upper != "ALL") {
			return true
		}
	}
	return false
}

// subqueries rewrites the subquery bodies in q[start:end].
func (r *structureRewriter) subqueries(start, end int) error {
	opener := &labelExpressionRewriter{query: r.query}
	for i := start; i < end; i++ {
		switch c := r.query[i]; c {
		case '\'', '"', '`':
			i = skipCypherQuotedText(r.query[:end], i, c) - 1
		case '{':
			if !opener.opensSubquery(i) {
				continue
			}
			close := findMatchingDelimiter(r.query[:end], i, '{', '}')
			if close < 0 {
				return nil
			}
			if err := r.region(i+1, close); err != nil {
				return err
			}
			i = close
		}
	}
	return nil
}

// whenBranch is one branch of a conditional query: its condition (empty for
// ELSE) and its query.
type whenBranch struct {
	conditionStart, conditionEnd int
	bodyStart, bodyEnd           int
	keywordStart                 int
}

// conditional rewrites WHEN … THEN … [ELSE …] at q[start:end] (see above).
func (r *structureRewriter) conditional(start, end int) error {
	branches, ok := r.whenBranches(start, end)
	if !ok {
		return nil
	}
	var columns []string
	var cases strings.Builder
	cases.WriteString("UNWIND [CASE")
	for index, branch := range branches {
		bodyColumns := r.columns(r.unbraced(branch.bodyStart, branch.bodyEnd))
		if index == 0 {
			columns = bodyColumns
		} else if len(bodyColumns) != len(columns) {
			return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "InvalidSyntax", localization.CypherCoreConditionalColumnCount())
		} else {
			for column := range columns {
				if bodyColumns[column] != columns[column] {
					return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "InvalidSyntax", localization.CypherCoreConditionalColumnNames())
				}
			}
		}
		number := strconv.Itoa(index + 1)
		if branch.conditionEnd > branch.conditionStart {
			cases.WriteString(" WHEN " + r.query[branch.conditionStart:branch.conditionEnd] + " THEN " + number)
		} else {
			cases.WriteString(" ELSE " + number)
		}
	}
	cases.WriteString(" END] AS " + whenBranchVariable + " CALL (*) { ")
	for index, branch := range branches {
		guard := "FILTER " + whenBranchVariable + " = " + strconv.Itoa(index+1) + " "
		if index == 0 {
			r.edit(branch.keywordStart, branch.bodyStart, cases.String()+guard)
		} else {
			r.edit(branches[index-1].bodyEnd, branch.bodyStart, " UNION ALL "+guard)
		}
		if err := r.unionParts(branch.bodyStart, branch.bodyEnd); err != nil {
			return err
		}
	}
	last := branches[len(branches)-1].bodyEnd
	r.edit(last, last, " }"+returnColumnsText(columns))
	return nil
}

// whenBranches splits WHEN c THEN q … [ELSE q] at q[start:end]; ok is false
// when the text doesn't have that form. A CASE expression's WHEN, THEN and
// ELSE belong to it.
func (r *structureRewriter) whenBranches(start, end int) ([]whenBranch, bool) {
	var branches []whenBranch
	cases := 0
	for _, word := range topLevelWords(r.query, start, end) {
		switch word.upper {
		case "CASE":
			cases++
		case "END":
			if cases > 0 {
				cases--
			}
		case "WHEN", "ELSE":
			if cases > 0 {
				continue
			}
			if len(branches) > 0 {
				previous := &branches[len(branches)-1]
				if previous.bodyStart == 0 {
					return nil, false
				}
				previous.bodyEnd = skipBackSpaces(r.query, word.start)
			}
			branch := whenBranch{keywordStart: word.start}
			if word.upper == "WHEN" {
				branch.conditionStart = skipASCIISpaces(r.query, word.end, end)
			} else {
				branch.bodyStart = skipASCIISpaces(r.query, word.end, end)
			}
			branches = append(branches, branch)
		case "THEN":
			if cases > 0 || len(branches) == 0 {
				continue
			}
			branch := &branches[len(branches)-1]
			if branch.bodyStart != 0 || branch.conditionStart == 0 {
				return nil, false
			}
			branch.conditionEnd = skipBackSpaces(r.query, word.start)
			branch.bodyStart = skipASCIISpaces(r.query, word.end, end)
		}
	}
	if len(branches) == 0 || branches[len(branches)-1].bodyStart == 0 {
		return nil, false
	}
	branches[len(branches)-1].bodyEnd = skipBackSpaces(r.query, end)
	for index, branch := range branches {
		if branch.bodyStart >= branch.bodyEnd || index < len(branches)-1 && branch.conditionStart == 0 {
			return nil, false // an ELSE that isn't last, or an empty branch
		}
	}
	return branches, true
}

// unbraced is q[start:end] without enclosing braces.
func (r *structureRewriter) unbraced(start, end int) string {
	text := strings.TrimSpace(r.query[start:end])
	for strings.HasPrefix(text, "{") && findMatchingDelimiter(text, 0, '{', '}') == len(text)-1 {
		text = strings.TrimSpace(text[1 : len(text)-1])
	}
	return text
}

// returnColumnsText is " RETURN c1, c2" for columns (each quoted when it
// isn't a plain name), empty for none.
func returnColumnsText(columns []string) string {
	if len(columns) == 0 {
		return ""
	}
	quoted := make([]string, len(columns))
	for index, column := range columns {
		quoted[index] = labelExpressionNameText(column)
	}
	return " RETURN " + strings.Join(quoted, ", ")
}
