package cypher

import (
	"context"
	"fmt"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"strings"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/localization"
)

func (e *StorageExecutor) validateStatementFraming(ctx context.Context, cypher string) error {
	view := statementBraceView(ctx, cypher)
	multiple, unclosed := inspectOutsideStrings(cypher, ";")
	if unclosed >= 0 {
		return unclosedCommentError(cypher, unclosed, ModeNormal)
	}
	if multiple {
		statements := e.splitBySemicolon(cypher)
		return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax",
			localization.CypherCommandRoutingMultipleStatements(len(statements)))
	}
	mode, body, conflict := statementExecutionModes(cypher)
	if mode != ModeNormal && queryGapEnd(body, 0) == len(body) {
		return emptyStatementError()
	}
	if conflict {
		return executionModeConflictError()
	}
	// Only a statement with the word UNION in it can have UNION branches.
	if findKeywordIndex(view, "UNION") < 0 || findKeywordIndex(view, "FINISH") < 0 {
		return nil
	}
	if branches, _, _, ok := parseTopLevelUnionBranches(ctx, view); ok && len(branches) > 1 {
		hasFinish, hasColumns := false, false
		for _, branch := range branches {
			_, finishes := readTrailingFinish(branch, stripPermittedFinish)
			if finishes {
				hasFinish = true
			} else if e.statementProducesColumns(withBraceView(ctx, branch), branch.text()) {
				hasColumns = true
			}
		}
		if hasFinish && hasColumns {
			return newSemanticError("Neo.ClientError.Statement.SyntaxError", "InvalidProjection",
				"All sub queries in an UNION must have the same return column names")
		}
	}
	return nil
}

// Statement framing helpers: Neo4j 5 statement preamble (CYPHER … groups),
// the FINISH clause terminator, and the EXPLAIN/PROFILE exclusivity rule.

// cypherClauseStarts are the keywords that end a CYPHER preamble group and
// start the actual statement. Matched case-insensitively at a token boundary.
var cypherClauseStarts = [...]string{
	"MATCH", "CREATE", "MERGE", "DELETE", "DETACH", "CALL", "RETURN", "WITH",
	"UNWIND", "OPTIONAL", "DROP", "SHOW", "FOREACH", "LOAD", "EXPLAIN",
	"PROFILE", "ALTER", "USE", "BEGIN", "COMMIT", "ROLLBACK", "TERMINATE",
	"UNION", "CYPHER", "FINISH", "LET", "FILTER", "FOR", "WHEN",
}

// startsWithClauseKeyword ends the preamble before a clause token or opening
// brace. This delimiter test makes no brace-role or admission decision; the
// retained statement product owns those decisions after the prefix.
func startsWithClauseKeyword(s string) bool {
	if strings.HasPrefix(s, "{") {
		return true
	}
	for _, keyword := range cypherClauseStarts {
		if matchKeywordAt(s, 0, keyword) {
			return true
		}
	}
	return false
}

// stripCypherPreamble removes leading CYPHER groups: the keyword CYPHER, an
// optional version number, and option tokens (ident, ident=value, or a quoted
// string) until a clause keyword starts the statement. Neo4j accepts any
// number of such groups (`CYPHER 5 runtime=slotted CYPHER RETURN 1`).
var cypherPreambleOptionValues = map[string]string{
	"runtime":                  "interpreted, legacy, parallel, pipelined, slotted",
	"planner":                  "cost, dp, idp",
	"expressionengine":         "compiled, interpreted, only_when_hot",
	"operatorengine":           "compiled, interpreted",
	"interpretedpipesfallback": "all, disabled, whitelisted_plans_only",
	"replan":                   "force, skip",
	"updatestrategy":           "default, eager",
	"connectcomponentsplanner": "greedy, idp",
	"eageranalyzer":            "ir, lp",
	"inferschemaparts":         "most_selective_label, off",
	"parallelruntimesupport":   "all, disabled",
	"debug":                    "ast, disableexistssubquerycaching, disablepropertycaching, generate_java_source, graphviz, inverse_cost, logcostcomparisons, logicalplan, logicalplanbuilder, mermaid, printcostcomparisons, querygraph, rawcardinalities, renderdistinctness, semanticstate, show_bytecode, show_java_source, tostring, verboseeagernessreasons, visualizepipelines, warnoncompilationerrors",
}

func validateCypherPreamble(query string) error {
	rest := strings.TrimSpace(query)
	restOffset := strings.Index(query, rest)
	argumentError := func(message string) error {
		return nornicerrors.MarkCompileTime(newSemanticError("Neo.ClientError.Statement.ArgumentError", "InvalidArgument", message))
	}
	consumeGap := func(index int) error {
		end, unclosed := readQueryGap(rest, index)
		if unclosed >= 0 {
			return unclosedCommentError(query, restOffset+unclosed, ModeNormal)
		}
		restOffset += end
		rest = rest[end:]
		return nil
	}
	var seenVersion string
	for rest != "" {
		if mode, inner := executionModePrefix(rest); mode != ModeNormal {
			restOffset += len(rest) - len(inner)
			rest = inner
			continue
		}
		if !matchKeywordAt(rest, 0, "CYPHER") {
			return nil
		}
		if err := consumeGap(len("CYPHER")); err != nil {
			return err
		}
		if token, ok := cyphertext.ScanNumber(rest, 0); ok {
			end := token.TailEnd
			version := rest[:end]
			if version != "5" && version != "25" {
				return argumentError(fmt.Sprintf("%s is not a valid option for cypher version. Valid options are: 5, 25", version))
			}
			if seenVersion != "" && seenVersion != version {
				return argumentError("Can't specify multiple conflicting values for cypher version")
			}
			seenVersion = version
			if err := consumeGap(end); err != nil {
				return err
			}
		}
		// Comments between the preamble and its body are still source text.
		// Admit their terminator while consuming this existing prefix gap,
		// before option parsing can misreport an unfinished comment as syntax.
		if err := consumeGap(0); err != nil {
			return err
		}
		for rest != "" && !startsWithClauseKeyword(rest) {
			end := 0
			end = cyphertext.NamePartEnd(rest, end)
			name := strings.ToLower(rest[:end])
			if err := consumeGap(end); err != nil {
				return err
			}
			if end == 0 || rest == "" || rest[0] != '=' {
				return newSemanticError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax", "invalid CYPHER option: expected name=value")
			}
			if err := consumeGap(1); err != nil {
				return err
			}
			end = 0
			end = cyphertext.NamePartEnd(rest, end)
			if end == 0 {
				return newSemanticError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax", "invalid CYPHER option: expected a value")
			}
			value := strings.ToLower(rest[:end])
			if err := consumeGap(end); err != nil {
				return err
			}
			allowed, known := cypherPreambleOptionValues[name]
			if !known {
				return argumentError("Unsupported options: " + name)
			}
			valid := false
			for _, candidate := range strings.Split(allowed, ", ") {
				if value == candidate {
					valid = true
					break
				}
			}
			if !valid {
				return argumentError(fmt.Sprintf("%s is not a valid option for %s. Valid options are: %s", value, name, allowed))
			}
		}
	}
	return nil
}

func stripCypherPreamble(query string) (string, bool) {
	text, changed, _ := readCypherPreamble(query)
	return text, changed
}

func readCypherPreamble(query string) (string, bool, *queryRewrite) {
	rest := query
	offset := 0
	changed := false
	var modes []string
	trim := func() {
		left := strings.TrimLeftFunc(rest, cyphertext.IsWhitespace)
		offset += len(rest) - len(left)
		rest = strings.TrimRightFunc(left, cyphertext.IsWhitespace)
	}
	consume := func(count int) { offset += count; rest = rest[count:]; trim() }
	for {
		trim()
		if mode, inner := executionModePrefix(rest); mode != ModeNormal {
			modes = append(modes, string(mode))
			offset += len(rest) - len(inner)
			rest = inner
			continue
		}
		if !matchKeywordAt(rest, 0, "CYPHER") {
			break
		}
		changed = true
		consume(queryGapEnd(rest, len("CYPHER")))
		if token, ok := cyphertext.ScanNumber(rest, 0); ok {
			consume(queryGapEnd(rest, token.TailEnd))
		}
		for rest != "" && !startsWithClauseKeyword(rest) {
			if isCypherStringQuoteByte(rest[0]) {
				end := skipCypherQuotedText(rest, 0, rest[0])
				consume(queryGapEnd(rest, end))
				continue
			}
			end := 0
			for end < len(rest) && !cyphertext.IsWhitespaceAt(rest, end) && queryCommentEnd(rest, end) < 0 {
				end++
			}
			consume(queryGapEnd(rest, end))
		}
		if rest == "" {
			break
		}
	}
	if !changed {
		return query, false, nil
	}
	prefix := ""
	if len(modes) > 0 {
		prefix = strings.Join(modes, " ") + " "
	}
	canonical := prefix + rest
	rewrite := &queryRewrite{Source: cyphertext.Source{Original: query, Text: canonical}}
	rewrite.Edits = append(rewrite.Edits, cyphertext.SourceEdit{Original: cyphertext.Span{Start: 0, End: offset}, Accepted: cyphertext.Span{Start: 0, End: len(prefix)}})
	if end := offset + len(rest); end < len(query) {
		rewrite.Edits = append(rewrite.Edits, cyphertext.SourceEdit{Original: cyphertext.Span{Start: end, End: len(query)}, Accepted: cyphertext.Span{Start: len(canonical), End: len(canonical)}})
	}
	return canonical, true, rewrite
}

// finishProhibitedClauses are the clause keywords a statement may not end
// with before FINISH. Neo4j 5.26 rejects FINISH only after RETURN ("RETURN can
// only be used at the end of the query"); after WITH or YIELD it ends the
// statement like after any other clause and returns no rows (#907). A
// prohibited trailing FINISH stays in the text so validation rejects it.
var finishProhibitedClauses = map[string]bool{
	"RETURN": true,
}

type finishSuffixPolicy uint8

const (
	detectFinishSuffix finishSuffixPolicy = iota
	stripPermittedFinish
)

// readTrailingFinish recognizes one name-aware FINISH suffix. Validation can
// detect every suffix; execution strips only those allowed after the preceding
// top-level clause, leaving prohibited RETURN/FINISH forms for validation.
func readTrailingFinish(view braceView, policy finishSuffixPolicy) (braceView, bool) {
	cypher := view.text()
	last := lastLiveByte(cypher, len(cypher))
	if last < 0 || !cyphertext.NamePartAt(cypher, last) {
		return view, false
	}
	end := last + 1
	start := end
	for start > 0 && cyphertext.NamePartAt(cypher, start-1) {
		start--
	}
	if !strings.EqualFold(cypher[start:end], "FINISH") {
		return view, false
	}
	remainder := view.window(0, start).trim()
	// A FINISH with nothing before it is the whole statement, the clause.
	if !remainder.empty() && clauseKeywordUsedAsName(view, start, end, "FINISH") {
		return view, false
	}
	if policy == stripPermittedFinish && finishProhibitedClauses[lastTopLevelClauseWord(remainder)] {
		return view, false
	}
	return remainder, true
}

type finishedStatementKey struct{}

// withFinishedStatement records that statement (the text left after its
// trailing FINISH was stripped) ended in FINISH. A CALL … YIELD with WHERE,
// ORDER BY, SKIP or LIMIT that is the whole statement is then not a
// standalone call: FINISH is the clause after it, as in Neo4j (#907).
func withFinishedStatement(ctx context.Context, statement string) context.Context {
	return context.WithValue(ctx, finishedStatementKey{}, strings.Join(strings.Fields(statement), " "))
}

// endsInFinish reports whether statement is the one withFinishedStatement
// recorded, compared with whitespace normalized: a CALL a subquery or a
// procedure runs inside the statement is not.
func endsInFinish(ctx context.Context, statement string) bool {
	finished, _ := ctx.Value(finishedStatementKey{}).(string)
	return finished != "" && finished == strings.Join(strings.Fields(statement), " ")
}

// topLevelUnionCut returns the offset of the first top-level UNION keyword
// (outside quotes, comments and nested delimiters), or -1.
func topLevelUnionCut(view braceView) int { return topLevelKeywordIndex(view, "UNION") }

// stripUnionBranchFinishes strips a trailing FINISH from each UNION branch of
// the statement. If no branch ends in FINISH the text is unchanged.
func stripUnionBranchFinishes(ctx context.Context, view braceView) (braceView, bool) {
	view = view.trim()
	cypher := view.text()
	cut := topLevelUnionCut(view)
	if cut < 0 {
		return readTrailingFinish(view, stripPermittedFinish)
	}
	before := view.window(0, cut).trim()
	after := view.window(cut, len(cypher)).trim()
	wordEnd := 0
	for wordEnd < len(after.text()) && !cyphertext.IsWhitespaceAt(after.text(), wordEnd) {
		wordEnd++
	}
	unionWord := after.text()[:wordEnd]
	tail := after.window(wordEnd, len(after.text())).trim()
	if nextWord, ok := firstWordUpper(tail.text()); ok && nextWord == "ALL" {
		unionWord += " ALL"
		tail = tail.window(len("ALL"), len(tail.text())).trim()
	}
	right, rightStripped := stripUnionBranchFinishes(ctx, tail)
	left, leftStripped := readTrailingFinish(before, stripPermittedFinish)
	if !leftStripped && !rightStripped {
		return view, false
	}
	if leftStripped && rightStripped && left.empty() && right.empty() {
		if _, _, mixed, _ := parseTopLevelUnionBranches(ctx, view); !mixed {
			return view.window(0, 0), true
		}
		return view, false
	}
	return view.assembleCode([]braceView{left, view.generated(" " + unionWord + " "), right}, "").trim(), true
}
func firstWordUpper(s string) (string, bool) {
	i := 0
	for i < len(s) && !cyphertext.IsWhitespaceAt(s, i) {
		i++
	}
	if i == 0 {
		return "", false
	}
	return upperASCII(s[:i]), true
}

func explainProfileConflict(cypher string) bool {
	_, statement, conflict := statementExecutionModes(cypher)
	return conflict && queryGapEnd(statement, 0) < len(statement)
}

// completeFinishedResult applies FINISH before the declared schema is checked.
// Internal and public execution share this terminal projection.
func completeFinishedResult(result *ExecuteResult) {
	if result != nil {
		result.Columns = nil
		result.Rows = nil
	}
}
