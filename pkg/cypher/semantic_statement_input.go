package cypher

import (
	"github.com/orneryd/nornicdb/pkg/fabric"
	"github.com/orneryd/nornicdb/pkg/localization"
	"strings"
)

// semanticStatementInput is owned by one admission. Native acquisition and
// clause projection happen at most once, independently of precheck count.
// It is not retained with a cached type verdict, parameter set or row frame.
type semanticStatementInput struct {
	parameters map[string]interface{}
	outerTypes map[string]string
	source     string
	names      *quotedVariableNames
	native     *fabric.QueryStructure
	clauses    []pipelineClause
	parsed, ok bool
	examined   bool
	logical    bool
}

func newSemanticStatementInput(source string, native *fabric.QueryStructure) *semanticStatementInput {
	return &semanticStatementInput{source: source, native: native}
}

func (input *semanticStatementInput) syntax() *fabric.QueryStructure {
	if input.native != nil || input.examined {
		return input.native
	}
	input.examined = true
	var split *pipelineClauseSplit
	if input.logical {
		// CALL bodies, metadata and Fabric explicitly demand native facts.
		// The main physical/native decision has already been made by their
		// enclosing statement; do not replay the physical descriptor pass.
		input.native = fabric.ReadQueryStructure(input.source)
		return input.native
	} else {
		split = pipelineStatementFor(input.source)
	}
	input.native, input.clauses, input.ok = split.native, split.clauses, split.ok
	input.parsed = true
	return input.native
}

func (input *semanticStatementInput) pipeline(allowCalls bool) ([]pipelineClause, bool) {
	syntax := input.syntax()
	if !input.parsed {
		input.parsed = true
		if syntax != nil && syntax.Err == nil {
			input.clauses, input.ok, _ = nativePipelineClauses(syntax, true)
		} else {
			input.clauses, input.ok, _ = legacyPipelineClauses(input.source)
			for index := range input.clauses {
				input.clauses[index] = physicalClauseFacts(input.clauses[index])
			}
		}
	}
	if !input.ok {
		return nil, false
	}
	for _, clause := range input.clauses {
		if !allowCalls && (clause.kind == pipelineClauseCall || clause.kind == pipelineClauseCallSubquery) {
			return nil, false
		}
	}
	return input.clauses, true
}

// matchingSemanticInput accepts only the exact source owned by this phase.
// Rewrites start another admission; this is provenance, not a text-keyed cache.
func matchingSemanticInput(source string, inputs ...*semanticStatementInput) *semanticStatementInput {
	if len(inputs) > 0 && inputs[0] != nil && inputs[0].source == source {
		return inputs[0]
	}
	return newSemanticStatementInput(source, nil)
}

func sharedSemanticClauses(text string, inputs ...*semanticStatementInput) ([]pipelineClause, bool) {
	if len(inputs) > 0 && inputs[0] != nil {
		return inputs[0].pipeline(false)
	}
	return splitPipelineClauses(text)
}

// describePipelineStatement is the single physical/native descriptor producer.
// The existing physical-plan cache retains immutable syntax only; each semantic
// admission creates its own type/binding scope from this descriptor.
func describePipelineStatement(source string, logical bool) *pipelineClauseSplit {
	split := &pipelineClauseSplit{}
	// Schema/admin routing already owns these statements. A graph parser
	// must not reinterpret their FOR/name slots as iteration operators.
	if !logical && (isSchemaCommandStatement(source) || isSystemCommandNoGraph(source)) {
		return split
	}
	// The existing physical clause descriptor decides which statement owns
	// logical CALL/USE facts. Ordinary graph statements retain that parser;
	// they do not acquire a native tree because CREATE, WITH or a relationship
	// bracket happens to appear in their source.
	var call bool
	split.clauses, split.ok, call = legacyPipelineClauses(source, true)
	demand := logical || call
	if !demand && !split.ok {
		analysis := analyzeQuery(classificationText(source))
		demand = analysis.HasCall || analysis.HasUse
	}
	local := false
	for index := range split.clauses {
		if demand {
			break
		}
		local = local || split.clauses[index].kind == pipelineClauseForeach || split.clauses[index].kind == pipelineClauseLet || split.clauses[index].kind == pipelineClauseFilter || split.clauses[index].kind == pipelineClauseUnwind && startsWithKeywordFold(split.clauses[index].text, "FOR")
		if local {
			break
		}
		if !fabric.QuerySyntaxDemand(split.clauses[index].text).Has(fabric.NativeLocalScopes) {
			continue
		}
		_ = forEachClauseOperatorExpression(split.clauses[index], func(expression string, _ bool) error {
			local = local || fabric.QuerySyntaxDemand(expression).Has(fabric.NativeLocalScopes)
			return nil
		}, true)
	}
	// Local-binding operands use the existing native expression owner. This
	// is separate from logical CALL/Fabric admission and includes standalone
	// list/map comprehensions, CASE subqueries and REDUCE.
	demand = demand || local
	if !demand {
		for index := range split.clauses {
			split.clauses[index].noLocalScopes = true
		}
	}
	if demand {
		split.native = fabric.ReadQueryStructure(source)
	}
	if split.native != nil && split.native.Err == nil {
		split.clauses, split.ok, call = nativePipelineClauses(split.native, true)
	}
	split.topLevelCall = call
	split.physical = split.ok
	for index, clause := range split.clauses {
		clause = physicalClauseFacts(clause)
		split.clauses[index] = clause
		if clause.kind == pipelineClauseWith {
			split.clauses[index].projection = admittedProjection(clause)
			if index > 0 && index+1 < len(split.clauses) {
				split.clauses[index].embeddingCandidate = fabric.EmbeddingProjectionCandidate(pipelineClauseKeyword(split.clauses[index-1].kind), pipelineClauseKeyword(split.clauses[index+1].kind), split.clauses[index].projection, "")
			}
		}
		if clause.kind >= pipelineClauseUse {
			split.physical = false
		}
	}
	return split
}

// physicalClauseFacts completes the existing physical parser's descriptor.
// Native=nil is authoritative: it never causes this producer to acquire a tree.
func physicalClauseFacts(clause pipelineClause) pipelineClause {
	if clause.syntax != nil {
		clause.projection = clause.syntax.Projection
		clause.iterationExpression, clause.iterationAlias = clause.syntax.Expression, clause.syntax.Name
		return clause
	}
	switch clause.kind {
	case pipelineClauseUnwind:
		body := strings.TrimSpace(clause.text)
		if startsWithKeywordFold(body, "UNWIND") {
			clause.iterationExpression, clause.iterationAlias, _ = splitUnwindBody(strings.TrimSpace(body[len("UNWIND"):]))
			clause.iterationAlias = symbolicNameValue(clause.iterationAlias)
		}
	case pipelineClauseWith, pipelineClauseReturn:
		if clause.projectionBody != "" || clause.projectionTail != "" {
			return clause
		}
		keyword := "WITH"
		if clause.kind == pipelineClauseReturn {
			keyword = "RETURN"
		}
		clause.projectionBody, clause.projectionTail = physicalProjectionBodyAndTail(clause.text, keyword)
	}
	return clause
}

// parsePipelineIteration reads the retained statement descriptor. String-only
// compatibility callers enter the same statement owner, not a native clause
// reparse. Pipeline consumers pass their existing clause directly.
func parsePipelineIteration(text string, clauses ...pipelineClause) (expression, alias string, ok bool) {
	var clause pipelineClause
	if len(clauses) > 0 {
		clause = clauses[0]
	} else {
		if startsWithKeywordFold(strings.TrimSpace(text), "FOR") {
			split := pipelineStatementFor(text)
			if split == nil || len(split.clauses) == 0 {
				return "", "", false
			}
			clause = split.clauses[0]
		} else {
			clause = physicalClauseFacts(pipelineClause{kind: pipelineClauseUnwind, text: text})
		}
	}
	if clause.kind != pipelineClauseUnwind {
		return "", "", false
	}
	return clause.iterationExpression, clause.iterationAlias, clause.iterationExpression != "" && clause.iterationAlias != ""
}

// physicalProjectionBodyAndTail is the existing physical projection reader;
// all consumers borrow this owner rather than invoking native demand again.
func physicalProjectionBodyAndTail(clause, keyword string) (string, string) {
	end := len(clause)
	for _, suffix := range []string{"WHERE", "ORDER BY", "SKIP", "LIMIT"} {
		if index := topLevelKeywordIndex(clause, suffix); index >= len(keyword) && index < end {
			end = index
		}
	}
	if len(clause) < len(keyword) {
		return "", ""
	}
	body, _ := cutDistinct(clause[len(keyword):end])
	return body, clause[end:]
}

// validateUnwindAlias rejects an UNWIND clause whose alias isn't exactly one
// variable, as Neo4j does: UNWIND takes no WHERE, so "UNWIND l AS x WHERE …"
// and "UNWIND l AS x y" are SyntaxErrors (Invalid input 'WHERE').
func validateUnwindAlias(clause string, syntax ...*fabric.QueryClause) error {
	var native *fabric.QueryClause
	if len(syntax) > 0 {
		native = syntax[0]
	}
	if native == nil {
		expression, alias, ok := splitUnwindBody(strings.TrimSpace(strings.TrimSpace(clause)[len("UNWIND"):]))
		if !ok || expression == "" {
			return sharedClauseSyntaxError("iteration requires a variable and an expression")
		}
		_, next, valid := scanSymbolicName(alias, 0)
		if !valid || queryGapEnd(alias, next) != len(alias) {
			token := strings.TrimSpace(alias[next:])
			if fields := strings.Fields(token); len(fields) > 0 {
				token = fields[0]
			}
			return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax", localization.CypherCoreInvalidInput(token))
		}
		return nil
	}
	if native.Kind == "UNWIND" && native.Predicate != "" {
		return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax", localization.CypherCoreInvalidInput("WHERE"))
	}
	return nil
}

func statementNativeClause(source, kind string, supplied ...*fabric.QueryClause) (*fabric.QueryClause, error) {
	if len(supplied) > 0 {
		return supplied[0], nil
	}
	split := pipelineStatementFor(source)
	if split.native == nil {
		return nil, nil
	}
	if split.native.Err != nil {
		return nil, split.native.Err
	}
	return split.native.First(kind), nil
}

// projectionAliasError is Neo4j's SyntaxError for a projection item whose
// alias, as written, is not one name (WITH n AS return n: the n after the
// alias is "Invalid input"). A backtick-quoted alias is one name.
func projectionAliasError(item string, syntax ...*fabric.QueryClause) error {
	if len(syntax) > 0 && syntax[0] != nil {
		return nil // the retained native projection already admitted its names
	}
	index := physicalProjectionAliasIndex(item)
	if index < 0 {
		return nil
	}
	alias := strings.TrimSpace(item[index+len("AS"):])
	_, end, valid := scanSymbolicName(alias, 0)
	if valid && queryGapEnd(alias, end) == len(alias) {
		return nil
	}
	token := strings.TrimSpace(alias[end:])
	if fields := strings.Fields(token); len(fields) > 0 {
		token = fields[0]
	}
	return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax", localization.CypherCoreInvalidInput(token))
}

func statementProjectionItem(item string, fact *fabric.QueryProjectionItem) (string, string) {
	if fabric.QuerySyntaxDemand(item).Has(fabric.NativeLocalScopes) {
		if p := fabric.ProjectionSyntax(item); p != nil && !p.Star && len(p.Items) == 1 {
			value := p.Items[0]
			if fact != nil {
				*fact = value
				fact.Input = fabric.ExpressionInput{}
				fact.Expression = lexicalExpressionText(value.Expression)
			}
			return lexicalExpressionText(value.Expression), value.Column
		}
	}
	trimmed := strings.TrimSpace(item)
	if trimmed == "" {
		return "", ""
	}
	at := physicalProjectionAliasIndex(trimmed)
	expression, column := lexicalExpressionText(trimmed), trimmed
	hasAlias := at >= 0
	if hasAlias {
		expression = lexicalExpressionText(trimmed[:at])
		column = normalizeProjectionColumnName(trimmed[at+len("AS"):])
	} else if name := simpleSemanticIdentifier(expression); name != "" {
		column = name
	}
	if fact != nil {
		*fact = fabric.QueryProjectionItem{Text: trimmed, Expression: expression, Column: column, Name: simpleSemanticIdentifier(expression), HasAlias: hasAlias}
		if hasAlias {
			fact.Alias = column
			fact.AliasStart = at
		}
	}
	return expression, column
}

func physicalProjectionAliasIndex(item string) int {
	opts := defaultKeywordScanOpts()
	opts.SkipBraces = true
	for from := 0; from < len(item); {
		index := keywordIndexFrom(item, "AS", from, opts)
		if index < 0 {
			return -1
		}
		end := index + len("AS")
		if index > 0 && isWhitespace(item[index-1]) && (end == len(item) || isWhitespace(item[end])) && !insideCaseBlock(item, index) {
			return index
		}
		from = end
	}
	return -1
}

// statementProjectionAliasIndex shares the physical/native demand decision
// with projection compilation, including string-only compatibility readers.
func statementProjectionAliasIndex(item string) int {
	if fabric.QuerySyntaxDemand(item).Has(fabric.NativeLocalScopes) {
		if projection := fabric.ProjectionSyntax(item); projection != nil && len(projection.Items) == 1 && projection.Items[0].HasAlias {
			return projection.Items[0].AliasStart - len("RETURN ")
		}
		return -1
	}
	return physicalProjectionAliasIndex(item)
}

// logicalStatementSyntax is the statement owner's explicit logical-demand
// entrance. CALL-relative bodies and declaration metadata need grammar facts
// even when their isolated text has no CALL token. It performs no semantic
// admission; invocation types and acceptance remain admission-lifetime facts.
func logicalStatementSyntax(source string) *fabric.QueryStructure {
	input := newSemanticStatementInput(source, nil)
	input.logical = true
	return input.syntax()
}

// logicalProjectionSyntax is the same owner's expression-declaration entry.
// Physical projection consumers use retained facts instead of this entrance.
func logicalProjectionSyntax(source string) *fabric.QueryProjection {
	return fabric.ProjectionSyntax(source)
}

// statementPhysicalClauses borrows the immutable descriptor retained by the
// statement owner. Adapters do not rebuild QueryStructure/QueryClause values.
func statementPhysicalClauses(source string) ([]pipelineClause, error) {
	split := pipelineStatementFor(source)
	if split.native != nil && split.native.Err != nil {
		return nil, split.native.Err
	}
	return split.clauses, nil
}

func statementFirstPhysicalClause(source string, kind pipelineClauseKind) *pipelineClause {
	clauses, err := statementPhysicalClauses(source)
	if err != nil || len(clauses) == 0 || clauses[0].kind != kind {
		return nil
	}
	return &clauses[0]
}

func statementPhysicalReturnProjection(source string) *fabric.QueryProjection {
	split := pipelineStatementFor(source)
	if split.native != nil {
		if split.native.Err != nil {
			return nil
		}
		if clause := split.native.ResultClause(); clause != nil {
			return clause.Projection
		}
		return nil
	}
	for index := len(split.clauses) - 1; index >= 0; index-- {
		if split.clauses[index].kind == pipelineClauseReturn {
			return admittedProjection(split.clauses[index])
		}
	}
	return nil
}

// statementClauseRange is a source view of the physical parser's retained
// clause, not another parse or a reconstruction of any binding/name fact.
func statementClauseRange(source string, clause *pipelineClause, from int) (int, int) {
	if clause.syntax != nil {
		return clause.syntax.Start, clause.syntax.End
	}
	if from < 0 || from > len(source) {
		return -1, -1
	}
	start := strings.Index(source[from:], clause.text)
	if start < 0 {
		return -1, -1
	}
	start += from
	return start, start + len(clause.text)
}

// statementReturnProjectionFacts is shared by retained physical clauses and
// string-only compatibility/precheck callers. An isolated projection is never
// installed as another statement descriptor just to compile its return plan.
func statementReturnProjectionFacts(source string, inputs ...pipelineClause) (*fabric.QueryProjection, string, string, bool) {
	if len(inputs) > 0 {
		clause := inputs[0]
		if clause.projection != nil {
			return clause.projection, "", "", false
		}
		if clause.syntax != nil && clause.syntax.Projection != nil {
			return clause.syntax.Projection, "", "", false
		}
		clause = physicalClauseFacts(clause)
		keyword := "RETURN"
		if clause.kind == pipelineClauseWith {
			keyword = "WITH"
		}
		_, distinct := cutDistinct(pipelineClauseBody(clause.text, keyword))
		return nil, clause.projectionBody, strings.TrimSpace(clause.projectionTail), distinct
	}
	if fabric.QuerySyntaxDemand(source).Has(fabric.NativeLocalScopes) {
		// The native projection entrance owns a projection BODY. The RETURN
		// plan entrance owns a whole clause, including its keyword. Retained
		// clauses above already carry this distinction; adapt it once here
		// for string-only admission and execution consumers as well.
		if projection := logicalProjectionSyntax(pipelineClauseBody(source, "RETURN")); projection != nil {
			return projection, "", "", false
		}
	}
	body, modifiers := physicalProjectionBodyAndTail(source, "RETURN")
	_, distinct := cutDistinct(pipelineClauseBody(source, "RETURN"))
	return nil, body, strings.TrimSpace(modifiers), distinct
}

// pipelineStatementFor is the existing statement-plan cache's canonical
// descriptor entry. Its native roles, physical clauses and operand read facts
// have one producer; admission and execution borrow the same immutable facts.
func pipelineStatementFor(cypher string) *pipelineClauseSplit {
	pipelineClauseSplits.RLock()
	split, cached := pipelineClauseSplits.splits[cypher]
	pipelineClauseSplits.RUnlock()
	if cached {
		return split
	}
	split = describePipelineStatement(cypher, false)
	pipelineClauseSplits.Lock()
	if existing, found := pipelineClauseSplits.splits[cypher]; found {
		split = existing
	} else {
		if len(pipelineClauseSplits.splits) >= pipelineClauseSplitLimit {
			pipelineClauseSplits.splits = make(map[string]*pipelineClauseSplit)
		}
		pipelineClauseSplits.splits[cypher] = split
	}
	pipelineClauseSplits.Unlock()
	return split
}

func cachedPipelineClauses(cypher string, procedureCalls bool) ([]pipelineClause, bool) {
	split := pipelineStatementFor(cypher)
	if !split.physical || split.topLevelCall && !procedureCalls {
		return nil, false
	}
	if split.clauses == nil {
		return nil, split.physical
	}
	// Execution keeps its own clause identity for result-stream binding.
	// Semantic consumers borrow the descriptor and do not copy these clauses.
	return append([]pipelineClause(nil), split.clauses...), split.physical
}

// parsePipelineClauses walks the query from left to right and slices it on
// top-level MATCH/CREATE/WITH/UNWIND/RETURN keywords, with top-level
// procedure calls (CALL proc(args) YIELD …) as clauses. Returns (clauses,
// true) on success. On anything unsupported (e.g. nested MERGE or CALL
// subquery) returns (nil, false) so the caller falls back. topLevelCall
// reports whether the query has a CALL of its own (cachedPipelineClauses).
func parsePipelineClauses(cypher string, logical ...bool) (clauses []pipelineClause, ok bool, topLevelCall bool) {
	var split *pipelineClauseSplit
	if len(logical) > 0 && logical[0] {
		split = describePipelineStatement(cypher, true)
	} else {
		split = pipelineStatementFor(cypher)
	}
	ok = split.physical
	if len(logical) > 0 && logical[0] {
		ok = split.ok
	}
	return split.clauses, ok, split.topLevelCall
}
