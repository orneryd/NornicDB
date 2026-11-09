package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/internal/cyphertext"
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
//     where that changes nothing; a UNION inside a UNION ALL becomes
//     CALL (*) { q } RETURN columns, and a UNION ALL inside a UNION becomes
//     a UNION (the outer UNION removes the duplicates either way).
//
// The same holds inside a subquery body (CALL, EXISTS, COUNT, COLLECT). The
// rewrite's edits are kept (queryRewrite), so columns and messages show the
// client's text.

// whenBranchVariable holds the number of the conditional branch that runs; a
// * projection doesn't list it (isGeneratedVariable).
const whenBranchVariable = generatedVariablePrefix + "when"

type structureRewriter struct {
	ctx     context.Context
	query   string
	edits   []labelRewriteEdit
	columns statementColumner
	// selected is the source offset of the arm a compile-only candidate view
	// keeps; every other conditional in that view keeps its first arm. It is
	// -1 in execution lowering, which keeps every arm behind a CASE guard.
	selected int
}

// statementColumner names a statement's result columns
// (StorageExecutor.StatementResultColumnsContext). An interface, not a method value, so
// Execute passes the executor to desugarQueryStructure without allocating.
type statementColumner interface {
	StatementResultColumnsContext(ctx context.Context, query string, stage SourceStage) ([]string, error)
}

// regionalColumner is the columns owner's compile-only regional read
// (StorageExecutor.regionalStatementColumns): the columns the clause ending a
// region declares in the admitted statement program of the accepted text it is
// given. A viewing caller never supplies row values, and an incomplete program
// is an error rather than an empty header.
type regionalColumner interface {
	regionalStatementColumns(ctx context.Context, query string, region cyphertext.Span) ([]string, error)
}

// desugarQueryStructure returns query with NEXT, WHEN and braced query parts
// rewritten (see above) and the rewrite that maps the result back, or query
// and nil when it has none. columns names a query's result columns.
func desugarQueryStructure(ctx context.Context, query string, columns statementColumner) (string, *queryRewrite, error) {
	if !mayUseQueryStructure(ctx, query) {
		return query, nil, nil
	}
	r := &structureRewriter{ctx: ctx, query: query, columns: columns, selected: -1}
	if err := r.region(0, len(query), false); err != nil {
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
func mayUseQueryStructure(ctx context.Context, query string) bool {
	if strings.Contains(query, "{") {
		opener := &labelExpressionRewriter{ctx: ctx, query: query}
		for index := 0; index < len(query); index++ {
			if isCypherQuoteByte(query[index]) {
				index = skipCypherQuotedText(query, index, query[index]) - 1
				continue
			}
			if query[index] == '{' && opener.opensSubquery(index) {
				if next := skipSpaces(query, index+1); next < len(query) && query[next] == '{' {
					return true
				}
			}
		}
	}
	return indexASCIIFold(query, "next") >= 0 || indexASCIIFold(query, "when") >= 0 || strings.HasPrefix(strings.TrimSpace(query), "{") ||
		indexASCIIFold(query, "union") >= 0
}

func (r *structureRewriter) edit(start, end int, text string) {
	r.edits = append(r.edits, labelRewriteEdit{start: start, end: end, text: text})
}

// rewrittenRegionText materializes the exact execution edits for a region
// before asking the schema owner about it. Query composition is lowered by this
// rewriter once; the schema owner never receives half-lowered conditional text.
func (r *structureRewriter) rewrittenRegionText(start, end, firstEdit int) string {
	edits := make([]labelRewriteEdit, len(r.edits)-firstEdit)
	for index, edit := range r.edits[firstEdit:] {
		edit.start -= start
		edit.end -= start
		edits[index] = edit
	}
	text, _ := applyLabelRewriteEdits(r.query[start:end], edits, true)
	return text
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
		case isCypherQuoteByte(c):
			i = skipCypherQuotedText(q[:end], i, c) - 1
		case c == '/':
			if commentEnd := queryCommentEnd(q[:end], i); commentEnd >= 0 {
				i = commentEnd - 1
			}
		case c == '(' || c == '[' || c == '{':
			closer := map[byte]rune{'(': ')', '[': ']', '{': '}'}[c]
			if close := cyphertext.FindMatchingDelimiter(q[:end], i, rune(c), closer, cyphertext.OpeningDelimiter); close >= 0 {
				i = close
			}
		case cyphertext.NamePartAt(q, i) && !isDigitByte(c):
			if i > start && (cyphertext.NamePartAt(q, i-1) || q[i-1] == '$' || q[i-1] == '.') {
				continue
			}
			j := i
			for j < end && cyphertext.NamePartAt(q, j) {
				j++
			}
			words = append(words, topLevelWord{start: i, end: j, upper: strings.ToUpper(q[i:j])})
			i = j - 1
		}
	}
	return words
}

// region rewrites q[start:end], a statement or a subquery body. distinct is
// true when q[start:end] is a braced part whose braces are dropped inside a
// UNION: its UNION ALLs may become UNIONs.
func (r *structureRewriter) region(start, end int, distinct bool) error {
	// Modes belong to the statement, outside the CALLs introduced by NEXT.
	// Read the existing framing owner and retain its byte prefix unchanged.
	if start == 0 && end == len(r.query) {
		if mode, statement := parseExecutionMode(r.query); mode != ModeNormal {
			start = len(strings.TrimRightFunc(r.query, cyphertext.IsWhitespace)) - len(statement)
		}
	}
	words := topLevelWords(r.query, start, end)
	var nexts []topLevelWord
	for index, word := range words {
		if word.upper != "NEXT" {
			continue
		}
		if r.startsQueryPart(word.end, end) {
			before := start
			if index > 0 {
				before = words[max(0, index-2)].start
			}
			if !projectionOperandBefore(r.query, before, word.start) {
				nexts = append(nexts, word)
			}
		}
	}
	if len(nexts) > 0 {
		first := skipASCIISpaces(r.query, start, end)
		r.edit(first, first, strings.Repeat("CALL (*) { ", len(nexts)))
	}
	segmentStart := start
	for _, next := range nexts {
		if err := r.segment(segmentStart, next.start, false); err != nil {
			return err
		}
		r.edit(next.start, next.end, "}")
		segmentStart = next.end
	}
	// The parts before a NEXT run in CALLs; the last stays at this level.
	return r.segment(segmentStart, end, distinct)
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
	for end > 0 && cyphertext.IsWhitespaceAt(q, end-1) {
		end--
	}
	return end
}

// segment rewrites a query part between NEXTs: a conditional or a union.
func (r *structureRewriter) segment(start, end int, distinct bool) error {
	at := skipASCIISpaces(r.query, start, end)
	if matchKeywordAt(r.query[:end], at, "WHEN") {
		return r.conditional(at, end)
	}
	return r.unionParts(at, end, distinct)
}

// unionParts rewrites q[start:end], parts joined by UNION [ALL]: a braced
// part loses its braces, or, when it holds a UNION inside a UNION ALL,
// becomes CALL (*) { … } RETURN columns. distinct is true when q[start:end]
// is itself a braced part inside a UNION: its UNION ALLs become UNIONs, which
// gives the same rows, unless it mixes the two (Neo4j's error).
func (r *structureRewriter) unionParts(start, end int, distinct bool) error {
	words := topLevelWords(r.query, start, end)
	type part struct{ start, end int }
	var parts []part
	var alls []topLevelWord
	partStart := start
	for index, word := range words {
		if word.upper != "UNION" {
			continue
		}
		parts = append(parts, part{partStart, word.start})
		partStart = word.end
		if index+1 < len(words) && words[index+1].upper == "ALL" && skipASCIISpaces(r.query, word.end, end) == words[index+1].start {
			alls = append(alls, words[index+1])
			partStart = words[index+1].end
		}
	}
	parts = append(parts, part{partStart, end})
	outerAll := len(alls) > 0
	if distinct && len(alls) == len(parts)-1 {
		for _, all := range alls {
			r.edit(all.start, all.end, "")
		}
		outerAll = false
	}
	for _, p := range parts {
		open := skipASCIISpaces(r.query, p.start, p.end)
		close := skipBackSpaces(r.query, p.end) - 1
		if open < p.end && r.query[open] == '{' && cyphertext.FindMatchingDelimiter(r.query[:p.end], open, '{', '}', cyphertext.OpeningDelimiter) == close {
			wrap := outerAll && r.hasDistinctUnion(open+1, close)
			// Dropped braces put the part's UNIONs in this union; braces
			// around a single query hold no UNION to change.
			firstEdit := len(r.edits)
			if err := r.region(open+1, close, !wrap && !outerAll && len(parts) > 1); err != nil {
				return err
			}
			if wrap {
				columns, err := r.columns.StatementResultColumnsContext(r.ctx, r.rewrittenRegionText(open+1, close, firstEdit), AcceptedSource)
				if err != nil {
					return err
				}
				r.edit(open, open+1, "CALL (*) {")
				r.edit(close+1, close+1, returnColumnsText(columns))
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
	opener := &labelExpressionRewriter{ctx: r.ctx, query: r.query}
	for i := start; i < end; i++ {
		switch c := r.query[i]; cypherLexicalByte(c) {
		case cypherQuoteClass:
			i = skipCypherQuotedText(r.query[:end], i, c) - 1
		case '{':
			if !opener.opensSubquery(i) {
				continue
			}
			close := cyphertext.FindMatchingDelimiter(r.query[:end], i, '{', '}', cyphertext.OpeningDelimiter)
			if close < 0 {
				return nil
			}
			if err := r.region(i+1, close, false); err != nil {
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

// conditional rewrites WHEN … THEN … [ELSE …] at q[start:end] (see above). A
// compile-only candidate view keeps one arm instead (selectArm).
func (r *structureRewriter) conditional(start, end int) error {
	branches, ok := r.whenBranches(start, end)
	if !ok {
		return nil
	}
	if r.selected >= 0 {
		return r.selectArm(branches)
	}
	var columns []string
	var cases strings.Builder
	cases.WriteString("UNWIND [CASE")
	for index, branch := range branches {
		if err := r.region(branch.bodyStart, branch.bodyEnd, false); err != nil {
			return err
		}
		bodyColumns, err := r.armColumns(branch)
		if err != nil {
			return err
		}
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
			r.edit(branch.keywordStart, branch.bodyStart, cases.String()+guard+"CALL (*) { ")
		} else {
			r.edit(branches[index-1].bodyEnd, branch.bodyStart, " }"+returnColumnsText(columns)+" UNION ALL "+guard+"CALL (*) { ")
		}
	}
	last := branches[len(branches)-1].bodyEnd
	r.edit(last, last, " }"+returnColumnsText(columns)+" }"+returnColumnsText(columns))
	return nil
}

// selectArm keeps one arm of a conditional in a compile-only candidate view:
// the arm whose body holds r.selected, and the first arm of every other
// conditional. Removing the WHEN, THEN and ELSE keywords together with the
// arms that are not kept leaves the kept arm's clauses exactly where they
// stand, so the view is the whole statement in the frame that reaches the arm.
func (r *structureRewriter) selectArm(branches []whenBranch) error {
	kept := 0
	for index, branch := range branches {
		if r.selected >= branch.bodyStart && r.selected < branch.bodyEnd {
			kept = index
			break
		}
	}
	for index, branch := range branches {
		if index == kept {
			r.edit(branch.keywordStart, branch.bodyStart, "")
			continue
		}
		r.edit(branch.keywordStart, branch.bodyEnd, "")
	}
	return r.region(branches[kept].bodyStart, branches[kept].bodyEnd, false)
}

// armColumns is a conditional arm's declared result columns, read from the
// admitted statement program of a compile-only candidate view in which that
// arm is selected (armCandidate). The arm is compiled in its own frame: the
// clauses before the conditional, the enclosing subquery's imports and its
// sibling arms' absence all come from the source itself, not from a
// reconstructed fragment, which would lose the scope the arm reads.
func (r *structureRewriter) armColumns(branch whenBranch) ([]string, error) {
	regional, ok := r.columns.(regionalColumner)
	if !ok {
		return nil, unresolvedStatementError()
	}
	candidate, rewrite, err := r.armCandidate(branch.bodyStart)
	if err != nil {
		return nil, err
	}
	region, ok := candidateSpan(rewrite, cyphertext.Span{Start: branch.bodyStart, End: branch.bodyEnd})
	if !ok {
		return nil, unresolvedStatementError()
	}
	columns, err := regional.regionalStatementColumns(r.ctx, candidate, region)
	if err == nil {
		return columns, nil
	}
	// The candidate's byte offsets are not the source's: its edits are the
	// deleted arms and keywords. Map a diagnostic back before it leaves here.
	if mapped, changed := rewrite.restoreSourceError(err); changed {
		return nil, mapped
	}
	return nil, err
}

// armCandidate returns this rewriter's whole source as a compile-only view in
// which the arm holding selected is kept, and the rewrite that maps the view's
// byte spans and diagnostics back to the source.
func (r *structureRewriter) armCandidate(selected int) (string, *queryRewrite, error) {
	selection := &structureRewriter{ctx: r.ctx, query: r.query, columns: r.columns, selected: selected}
	if err := selection.region(0, len(r.query), false); err != nil {
		return "", nil, err
	}
	candidate, rewrite := applyLabelRewriteEdits(r.query, selection.edits, true)
	return candidate, rewrite, nil
}

// candidateSpan maps a source span into the candidate a rewrite produced.
// Bytes before the span shift by the edits applied before it; a boundary
// inside an edit's replaced extent takes that edge, because the selected arm
// is kept and only the conditional text around it is removed.
func candidateSpan(rewrite *queryRewrite, span cyphertext.Span) (cyphertext.Span, bool) {
	if rewrite == nil || span.Start < 0 || span.End < span.Start || span.End > len(rewrite.Original) {
		return cyphertext.Span{}, false
	}
	start := candidateOffset(rewrite.Edits, span.Start, true)
	end := candidateOffset(rewrite.Edits, span.End, false)
	if start < 0 || end < start || end > len(rewrite.Text) {
		return cyphertext.Span{}, false
	}
	return cyphertext.Span{Start: start, End: end}, true
}

// candidateOffset maps one source byte boundary through rewrite's edits.
func candidateOffset(edits []cyphertext.SourceEdit, offset int, start bool) int {
	shift := 0
	for _, edit := range edits {
		if offset <= edit.Original.Start {
			break
		}
		if offset < edit.Original.End {
			if start {
				return edit.Accepted.Start
			}
			return edit.Accepted.End
		}
		shift += (edit.Accepted.End - edit.Accepted.Start) - (edit.Original.End - edit.Original.Start)
	}
	return offset + shift
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
