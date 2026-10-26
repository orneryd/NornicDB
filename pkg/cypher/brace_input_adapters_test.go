package cypher

import (
	"context"
	"strings"
	"unicode"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
)

// Author: #1663 implementation lane. Test-only adapters create a new original
// input; production consumers take their producer's retained view.

// acceptedBraceView is a new original input already in accepted form, so its
// text is exactly query (keyword-scan fixtures compare offsets in query).
func acceptedBraceView(query string) braceView {
	return inputBraceView(context.Background(), query, cyphertext.AcceptedSource)
}

// originalBraceViews adapts each fixture text as its own new original input.
func originalBraceViews(texts ...string) []braceView {
	views := make([]braceView, len(texts))
	for index, text := range texts {
		views[index] = originalBraceView(text)
	}
	return views
}

// testBodyWindow is a body view's extent in its original fixture text.
func testBodyWindow(body braceView) (start, end int) {
	span := body.source.Origin(cyphertext.Span{End: len(body.text())}).Original
	return span.Start, span.End
}

// nextShellCommand reads input's leading shell command through the shell
// producer, framing map commands with the brace owner.
func nextShellCommand(input string) (command, rest string, ok bool) {
	view := acceptedBraceView(input)
	commandSpan, restSpan, ok := shellCommandSpans(input, view.close)
	if !ok {
		return "", input, false
	}
	return input[commandSpan.Start:commandSpan.End], input[restSpan.Start:restSpan.End], true
}

// consumeShellMapCommand is nextShellCommand for a map-form :param(s)
// command; consumed is where the following statement starts.
func consumeShellMapCommand(input string) (command string, consumed int, ok bool) {
	view := acceptedBraceView(input)
	commandSpan, restSpan, ok := shellCommandSpans(input, view.close)
	if !ok {
		return "", 0, false
	}
	command = input[commandSpan.Start:commandSpan.End]
	name := strings.IndexFunc(command, unicode.IsSpace)
	if name < 0 || !strings.HasPrefix(strings.TrimSpace(command[name:]), "{") {
		return "", 0, false
	}
	return command, restSpan.Start, true
}

// expressionBraceView admits a standalone expression fragment (a map literal,
// a projection) through the expression-input owner, as a statement window
// reaches these consumers; originalBraceView frames whole statements.
func expressionBraceView(text string) braceView {
	return expressionInputView(text, cyphertext.RawSource)
}

// canonicalFragmentView is the window execution hands a consumer for fragment
// in the statement prefix+fragment+suffix: a window of the statement's
// canonical view (canonicalBraceView), in which comments and whitespace runs
// are single spaces and the brace roles are the statement's. prefix and suffix
// must already be canonical.
func canonicalFragmentView(prefix, fragment, suffix string) braceView {
	_, view := canonicalBraceView(context.Background(), prefix+fragment+suffix)
	return view.window(len(prefix), len(view.text())-len(suffix)).trim()
}

func originalBraceView(query string) braceView {
	source := cyphertext.NormalizeSource(query)
	return braceView{syntax: readBraceSyntax(source), source: source}
}

// Author: #1663 lane. Legacy direct OPTIONAL MATCH fixtures are new accepted
// inputs. Construct one original statement for their two fields, preserving
// the fixture's spelling and field evaluation order.
type optionalMatchFixture struct{ pattern, where string }

func (fixture optionalMatchFixture) retained() optionalMatchClause {
	const prefix = "OPTIONAL MATCH "
	text := prefix + fixture.pattern
	whereStart := len(text)
	if fixture.where != "" {
		text += " WHERE "
		whereStart = len(text)
		text += fixture.where
	}
	source := cyphertext.NewSource(context.Background(), text, cyphertext.AcceptedSource)
	view := braceView{syntax: readBraceSyntax(source), source: source}
	where := view.window(whereStart, whereStart+len(fixture.where))
	return optionalMatchClause{pattern: view.window(len(prefix), len(prefix)+len(fixture.pattern)), where: where}
}

// Author: #1663 lane. Existing direct builtin unit fixtures supply mock values
// through the exported Context callback. Retain each fixture input once and
// bind its position for the internal builtin; callback text stays byte-identical.
// This adapter exists only in tests and supplies no production string path.
func invokeFunctionFixture(function cypherfn.InvocationFunc, fixture cypherfn.Context, arguments []string) (interface{}, error) {
	views := make([]braceView, len(arguments))
	for index, argument := range arguments {
		source := cyphertext.NewSource(context.Background(), argument, cyphertext.AcceptedSource)
		views[index] = braceView{source: source, syntax: readOriginalBraceSyntax(source, true)}
	}
	call := cypherfn.InvocationContext{Context: fixture}
	if fixture.Eval != nil {
		call.Eval = func(index int) (interface{}, error) { return fixture.Eval(views[index].text()) }
		call.EvalSpan = func(index int, span cyphertext.Span) (interface{}, error) {
			return fixture.Eval(views[index].window(span.Start, span.End).text())
		}
	}
	return function(call, arguments)
}

func statementAccessesGraphText(query string) bool {
	return statementAccessesGraphView(originalBraceView(query))
}

func statementReadsGraphText(query string) bool {
	return statementReadsGraphView(originalBraceView(query))
}

func expressionReadsGraphText(query string) bool {
	return graphReadingExpressionView(originalBraceView(query)) != ""
}

func graphReadingExpression(query string) string {
	return graphReadingExpressionView(originalBraceView(query))
}

func desugarLabelExpressions(query string, params map[string]interface{}, cypher25 bool) (string, *queryRewrite, error) {
	return desugarLabelExpressionView(originalBraceView(query), params, cypher25)
}

func hasNotExistsFollowedByBrace(query string) bool {
	return hasNotExistsFollowedByBraceView(originalBraceView(query))
}

func hasKeywordFollowedByBrace(query, keyword string) bool {
	return hasKeywordFollowedByBraceView(originalBraceView(query), keyword)
}

func desugarStandaloneOrderClauses(query string) (string, *queryRewrite) {
	return desugarStandaloneOrderClauseView(originalBraceView(query))
}

func desugarCypher25Expressions(query string, cypher25 bool) (string, []*queryRewrite, error) {
	return desugarCypher25ExpressionView(originalBraceView(query), cypher25)
}

// These retired string-taking test entrances are genuine standalone fixtures.
// Their only decisions come from the retained owner; production cannot link
// these _test definitions and has no corresponding string fallback.
func validateStaticMapKeys(query string) error { return originalBraceView(query).admission() }

func mapBraceAt(query string, opening int) mapBraceKind {
	if fact, found := originalBraceView(query).at(opening); found {
		return fact.kind
	}
	return literalBrace
}

func precedingSubqueryExpressionKeyword(query string, opening int) bool {
	fact, found := originalBraceView(query).at(opening)
	return found && fact.kind == subqueryBrace
}

func newSubqueryBraceScanner(query string, start int) braceFactScanner {
	return newBraceFactScanner(originalBraceView(query), start)
}

type originalPatternContext struct{ subqueries, patterns map[int]bool }

func newMapPatternContext(query string) originalPatternContext {
	context := originalPatternContext{subqueries: map[int]bool{}, patterns: map[int]bool{}}
	for _, fact := range originalBraceView(query).syntax.braceFacts() {
		context.subqueries[fact.span.Start] = fact.kind == subqueryBrace
		context.patterns[fact.span.Start] = fact.kind == patternPropertyBrace
	}
	return context
}
func (context originalPatternContext) insidePatternDelimiter(_ string, opening int) bool {
	return context.patterns[opening]
}

func forEachMapEntry(query string, visit func(string, string)) error {
	view := originalBraceView(query)
	entries, valid := view.keyedEntries()
	if !valid {
		return newSemanticError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax", mapLiteralEntryMessage)
	}
	for _, entry := range entries {
		visit(entry.key(), entry.value.text())
	}
	return nil
}

// Author: #1663 lane. Metadata unit fixtures are new original statements.
func analyzeQuery(query string) *QueryInfo { return analyzeQueryView(originalBraceView(query)) }
