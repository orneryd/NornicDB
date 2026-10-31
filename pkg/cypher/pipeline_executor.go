package cypher

// Pipeline executor for composite queries of the form
//
//	MATCH ... [WHERE ...]
//	CREATE ... (any number)
//	WITH ... (projection / pass-through)
//	UNWIND <list-expression> AS <var>
//	MATCH ...
//	CREATE ...
//	[RETURN ...]
//
// The existing executeMatchWithClause / executeMatchWithUnwind handlers assume
// the segment between MATCH and WITH is a single node pattern. That makes
// them corrupt any query where CREATE clauses live between MATCH and WITH
// (the classic "invalid property value" error where a generated property key
// swallows the rest of the query, e.g. key="{}UNWIND[{productID").
//
// This file walks the clauses in order and threads a binding context through
// each step so arbitrary compositions work. It reuses the existing primitives
// (executeMatchForContext, executeCreateWithRefs, executeInternal) rather
// than reparsing patterns from scratch.

import (
	"context"
	"github.com/orneryd/nornicdb/internal/contextslot"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
	"reflect"
	"slices"
	"sort"
	"strings"
	"sync"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/pkg/util"
)

// pipelineClauseKind enumerates the clause types the pipeline executor
// understands. Anything else causes us to bail and return false so callers
// delegate to specialized executors. OPTIONAL MATCH is supported for bounded,
// single-hop clauses after a WITH horizon.
type pipelineClauseKind int

const (
	pipelineClauseMatch pipelineClauseKind = iota
	pipelineClauseOptionalMatch
	pipelineClauseCreate
	pipelineClauseMerge
	pipelineClauseDelete
	pipelineClauseSet
	pipelineClauseRemove
	pipelineClauseWith
	pipelineClauseUnwind
	pipelineClauseReturn
	pipelineClauseForeach
	pipelineClauseCallSubquery
	// pipelineClauseCall is a procedure call, CALL proc(args) YIELD … [WHERE …].
	pipelineClauseCall
	pipelineClauseLet
	pipelineClauseFilter
)

// pipelineClause is one segment of the pipeline. `text` includes the leading
// keyword (MATCH/CREATE/WITH/UNWIND/RETURN) and the clause body — exactly
// what you would pass to the clause implementation.
type pipelineClause struct {
	kind               pipelineClauseKind
	syntax             cypherantlr.SyntaxNode
	embeddingCandidate string
	text               string
	// optional marks OPTIONAL CALL (kinds pipelineClauseCall and
	// pipelineClauseCallSubquery; text is the CALL itself): an input row the
	// call produces no row for is kept, with null for the call's columns
	// (#907).
	optional bool
}

// pipelineMatchPhysicalHint describes downstream row requirements that a
// MATCH operator may safely push into candidate collection or traversal. The
// logical pipeline remains N-ary; this is an operator property derived by
// walking every remaining clause, not a separate query-shape handler.
type pipelineMatchPhysicalHint struct {
	orderExpr  string
	limit      int
	earlyLimit int
	// readTail holds the clauses after the MATCH when every one of them only
	// reads; nil otherwise. A label scan uses it to read only the properties
	// those clauses use (pipelineLabelScanProjection).
	readTail []string
	// streamScan lets the MATCH hand its candidates on as the scan reads
	// them: the statement only reads and streams its result to the client,
	// and the MATCH runs once (#939).
	streamScan bool
}

// pipelineReadOnlyTail returns the texts of remaining when every clause only
// reads and the last one is a RETURN, and nil otherwise. Clauses that don't
// end in RETURN may be one part of a larger statement whose later clauses can
// use any property. RETURN * and WITH * carry every variable on whole: a CALL
// subquery runs its outer MATCH as MATCH ... RETURN *.
func pipelineReadOnlyTail(remaining []pipelineClause) []string {
	if len(remaining) == 0 || remaining[len(remaining)-1].kind != pipelineClauseReturn {
		return nil
	}
	tail := make([]string, 0, len(remaining))
	for _, clause := range remaining {
		switch clause.kind {
		case pipelineClauseWith, pipelineClauseReturn:
			keyword := "RETURN"
			if clause.kind == pipelineClauseWith {
				keyword = "WITH"
			}
			body, _ := cutDistinct(pipelineClauseBody(clause.text, keyword))
			if strings.HasPrefix(strings.TrimSpace(body), "*") {
				return nil
			}
			tail = append(tail, clause.text)
		case pipelineClauseMatch, pipelineClauseOptionalMatch, pipelineClauseUnwind:
			tail = append(tail, clause.text)
		default:
			return nil
		}
	}
	return tail
}

// pipelineLabelScanProjection returns the properties a label scan for
// nodePattern must read: those its inline map, its WHERE and the read-only
// clauses after it use through the pattern's variable. It reports false, and
// the scan reads whole nodes, when a later clause can write, when the variable
// is used other than as variable.property (returned, passed on, compared,
// used in a later pattern), or when a temporal viewport needs the node's own
// validity properties.
func pipelineLabelScanProjection(ctx context.Context, nodePattern nodePatternInfo, whereClause string, readTail []string) ([]string, bool) {
	if len(nodePattern.labels) == 0 || readTail == nil {
		return nil, false
	}
	if _, viewport := TemporalViewportFromContext(ctx); viewport {
		return nil, false
	}
	properties := pipelineNodePredicateProperties(nodePattern.variable, whereClause, nodePattern.properties)
	if properties == nil {
		return nil, false
	}
	for _, text := range readTail {
		used := pipelineNodePredicateProperties(nodePattern.variable, text, nil)
		if used == nil {
			return nil, false
		}
		properties = append(properties, used...)
	}
	sort.Strings(properties)
	return slices.Compact(properties), true
}

// pipelineRow carries bindings across clauses. Values may be *storage.Node,
// *storage.Edge, or scalars (for WITH projections and UNWIND variables).
type pipelineRow map[string]interface{}

// canExecuteAsPipeline returns true when the query is decomposable into the
// clause kinds this executor understands. Any unsupported clause (CALL
// subquery, etc.) causes a false return so the
// caller can select a specialized physical plan.
func canExecuteAsPipeline(ctx context.Context, cypher string) ([]pipelineClause, bool) {
	clauses, ok := pipelineClausesFor(ctx, cypher)
	if !ok {
		return nil, false
	}
	if len(clauses) > 0 && clauses[0].kind == pipelineClauseCreate {
		body := pipelineClauseBody(clauses[0].text, "CREATE")
		if !strings.HasPrefix(body, "(") && !namedPathAssignmentPrefix(body) {
			return nil, false
		}
	}
	// Single graph reads, writes and CALL subqueries own their seed row. A
	// lone WITH or UNWIND is a whole statement only before FINISH (#907),
	// and runs on the seed row too.
	if len(clauses) < 2 {
		if len(clauses) == 0 {
			return nil, false
		}
		switch clauses[0].kind {
		case pipelineClauseMatch, pipelineClauseCreate, pipelineClauseMerge, pipelineClauseForeach, pipelineClauseCallSubquery,
			pipelineClauseSet, pipelineClauseRemove, pipelineClauseDelete, pipelineClauseWith, pipelineClauseUnwind, pipelineClauseLet, pipelineClauseFilter:
		default:
			return nil, false
		}
	}
	return clauses, true
}

// pipelineClausesFor splits a statement, or the clauses after a CALL …
// YIELD, into pipeline clauses when every clause is one the pipeline runs.
func pipelineClausesFor(ctx context.Context, cypher string) ([]pipelineClause, bool) {
	return splitPipelineClausesAllowingProcedureCalls(ctx, cypher)
}

// unwindBodySource keeps the admitted source views needed by iteration and
// top-level execution. aliasTail stays whole so semantic validation rejects
// trailing tokens; variableWritten and remainder share its canonical name extent.
type unwindBodySource struct {
	expression      string
	aliasTail       string
	variableWritten string
	remainder       string
}

// splitUnwindBody reads a full original UNWIND clause with the existing native
// scanner. AS in an operand or identifier remains part of the expression.
func splitUnwindBody(source string) (unwindBodySource, bool) {
	head := queryGapEnd(source, 0)
	if !startsWithKeywordFold(source[head:], "UNWIND") {
		return unwindBodySource{}, false
	}
	exprStart := queryGapEnd(source, keywordEndAt(source, head, "UNWIND"))
	opts := defaultKeywordScanOpts()
	opts.SkipBraces = true
	for from := exprStart; ; {
		at := keywordIndexFrom(source, "AS", from, opts)
		if at < 0 {
			return unwindBodySource{}, false
		}
		if wordIsName(source, at) || insideCaseBlock(source, at) {
			from = at + len("AS")
			continue
		}
		expression := strings.TrimSpace(source[exprStart:at])
		aliasStart := queryGapEnd(source, at+len("AS"))
		token, _, found := cyphertext.ScanIdentifierExtent(source, aliasStart)
		if expression == "" || !found {
			return unwindBodySource{}, false
		}
		return unwindBodySource{
			expression:      expression,
			aliasTail:       strings.TrimSpace(source[aliasStart:]),
			variableWritten: source[aliasStart:token.End],
			remainder:       strings.TrimSpace(source[token.End:]),
		}, true
	}
}

// pipelineClauseBody is a clause's text after its keyword, which the
// clause splitter matched in any letter case (Return, rEtUrN).
func pipelineClauseBody(text, keyword string) string {
	text = strings.TrimSpace(text)
	if end := keywordEndAt(text, 0, keyword); end > 0 {
		text = text[queryGapEnd(text, end):]
	}
	return strings.TrimSpace(text)
}

// pipelineClauseSplits caches parsePipelineClauses by statement text: one
// parse serves both splits (with and without procedure calls as clauses). It
// is cleared when it reaches pipelineClauseSplitLimit entries, which bounds
// it for workloads with unbounded distinct texts.
var pipelineClauseSplits = struct {
	sync.RWMutex
	splits map[string]pipelineClauseSplit
}{splits: make(map[string]pipelineClauseSplit)}

// pipelineClauseSplit is a text's split with procedure calls as clauses;
// topLevelCall records whether the text has a CALL of its own, which makes
// the split without procedure calls unsupported.
type pipelineClauseSplit struct {
	clauses      []pipelineClause
	ok           bool
	topLevelCall bool
	firstReturn  string
}

const pipelineClauseSplitLimit = 4096

// splitPipelineClauses cuts a statement into its pipeline clauses (see
// parsePipelineClauses); a CALL makes it unsupported.
func splitPipelineClauses(ctx context.Context, cypher string) ([]pipelineClause, bool) {
	split := cachedPipelineClauses(ctx, cypher, false)
	return split.clauses, split.ok
}

// splitPipelineClausesAllowingProcedureCalls is splitPipelineClauses that also
// accepts top-level procedure calls (CALL proc(args) YIELD …) as clauses of
// kind pipelineClauseCall, for the pipeline executor and call-aware semantic
// validation. splitPipelineClauses rejects statements with procedure calls.
func splitPipelineClausesAllowingProcedureCalls(ctx context.Context, cypher string) ([]pipelineClause, bool) {
	split := cachedPipelineClauses(ctx, cypher, true)
	return split.clauses, split.ok
}

// cachedPipelineClauses computes the split of a text once, for both
// callers: without procedure calls as clauses, a text with a CALL of its own
// is unsupported, and any other text splits the same way. Callers get their
// own copy of the clause list. Cancellation is recorded in the statement's
// existing failure slot so dispatch can distinguish it from unsupported text.
func cachedPipelineClauses(ctx context.Context, cypher string, procedureCalls bool) pipelineClauseSplit {
	if err := ctx.Err(); err != nil {
		recordExpressionFailure(ctx, err)
		return pipelineClauseSplit{}
	}
	key := semanticCacheKey(cypher, cypherVersionFromContext(ctx) == "25")
	pipelineClauseSplits.RLock()
	split, cached := pipelineClauseSplits.splits[key]
	pipelineClauseSplits.RUnlock()
	if !cached {
		split = parsePipelineClauses(ctx, cypher)
		if err := ctx.Err(); err != nil {
			recordExpressionFailure(ctx, err)
			return pipelineClauseSplit{}
		}
		pipelineClauseSplits.Lock()
		if err := ctx.Err(); err != nil {
			pipelineClauseSplits.Unlock()
			recordExpressionFailure(ctx, err)
			return pipelineClauseSplit{}
		}
		if len(pipelineClauseSplits.splits) >= pipelineClauseSplitLimit {
			pipelineClauseSplits.splits = make(map[string]pipelineClauseSplit)
		}
		pipelineClauseSplits.splits[key] = split
		pipelineClauseSplits.Unlock()
	}
	if split.topLevelCall && !procedureCalls {
		return pipelineClauseSplit{}
	}
	if split.clauses != nil {
		split.clauses = append([]pipelineClause(nil), split.clauses...)
	}
	return split
}

// parsePipelineClauses projects the retained query-body product once. A final
// RETURN remains available for semantic column export when USE or FINISH makes
// the body unsupported by execution's pipeline; unsupported clauses stay nil.
func parsePipelineClauses(ctx context.Context, source string) pipelineClauseSplit {
	body := readQueryBody(ctx, source, 0, len(source))
	if body.status != queryBodyRead || len(body.branches) != 1 {
		return pipelineClauseSplit{}
	}
	branch := body.branches[0]
	split := pipelineClauseSplit{}
	if len(branch.clauses) > 0 {
		last := branch.clauses[len(branch.clauses)-1]
		if last.kind == pipelineClauseReturn {
			split.firstReturn = strings.TrimSpace(last.text)
		}
	}
	if branch.useStart >= 0 || branch.finished || len(branch.clauses) == 0 {
		return split
	}
	// Inline embedding candidates keep the bound syntax of their WITH; the
	// pipeline frame resolves them like the scope compiler does.
	for index, clause := range branch.clauses {
		if clause.kind == pipelineClauseCall || clause.kind == pipelineClauseCallSubquery {
			split.topLevelCall = true
		}
		item := clause.pipelineClause
		item.syntax = clause.syntax
		item.embeddingCandidate = embeddingCandidate(branch.clauses, index)
		split.clauses = append(split.clauses, item)
	}
	split.ok = true
	return split
}

// pipelineApplyOptionally runs an OPTIONAL CALL (pipelineClause.optional)
// over rows one row at a time through apply, which returns the call's rows
// for one input row and the call's column names. An input row the call
// produces no row for is kept, with null for each of the call's columns, as
// Neo4j's OPTIONAL CALL does (#907). *ok reports false when apply can't run
// the call.
func pipelineApplyOptionally(rows []pipelineRow, apply func(row []pipelineRow) ([]pipelineRow, []string, bool, error), ok *bool) ([]pipelineRow, error) {
	out := make([]pipelineRow, 0, len(rows))
	for index := range rows {
		produced, columns, handled, err := apply(rows[index : index+1])
		if err != nil {
			return nil, err
		}
		if !handled {
			*ok = false
			return nil, nil
		}
		if len(produced) > 0 {
			out = append(out, produced...)
			continue
		}
		kept := make(pipelineRow, len(rows[index])+len(columns))
		for name, value := range rows[index] {
			kept[name] = value
		}
		for _, name := range columns {
			if _, bound := kept[name]; !bound {
				kept[name] = nil
			}
		}
		out = append(out, kept)
	}
	return out, nil
}

// findAllTopLevelPipelineKeywordPositions returns clause boundaries outside
// strings and every bracketed construct. In particular, MATCH inside
// EXISTS { MATCH ... } belongs to the predicate and must never become a new
// outer pipeline clause.
func findAllTopLevelPipelineKeywordPositions(query, keyword string) []int {
	// Allocated on the first match: most scans find nothing.
	var positions []int
	parenDepth, bracketDepth, braceDepth := 0, 0, 0
	withSearch := isWithKeyword(keyword)
	if keyword == "" {
		return positions
	}
	// Compare the first byte before the case-insensitive keyword match, so
	// most positions cost one byte comparison instead of an EqualFold call.
	first := asciiUpper(keyword[0])
	for i := 0; i < len(query); i++ {
		character := query[i]
		switch cypherLexicalByte(character) {
		case cypherQuoteClass:
			i = skipCypherQuotedText(query, i, character) - 1
			continue
		case '/':
			if end := queryCommentEnd(query, i); end >= 0 {
				i = end - 1
				continue
			}
		}
		if parenDepth == 0 && bracketDepth == 0 && braceDepth == 0 && asciiUpper(character) == first &&
			i+len(keyword) <= len(query) && strings.EqualFold(query[i:i+len(keyword)], keyword) &&
			(i == 0 || !cyphertext.NamePartAt(query, i-1)) &&
			(i+len(keyword) == len(query) || !cyphertext.NamePartAt(query, i+len(keyword))) {
			if (!withSearch || !isOperatorWith(query, i)) && !clauseKeywordUsedAsName(query, i, i+len(keyword), keyword) {
				if positions == nil {
					positions = make([]int, 0, 4)
				}
				positions = append(positions, i)
			}
			i += len(keyword) - 1
			continue
		}
		switch character {
		case '(':
			parenDepth++
		case ')':
			if parenDepth > 0 {
				parenDepth--
			}
		case '[':
			bracketDepth++
		case ']':
			if bracketDepth > 0 {
				bracketDepth--
			}
		case '{':
			braceDepth++
		case '}':
			if braceDepth > 0 {
				braceDepth--
			}
		}
	}
	return positions
}

func (e *StorageExecutor) executePipeline(ctx context.Context, cypher string) (outcome pipelineDispatchOutcome) {
	statement := cypher
	cypher = e.substituteParams(cypher, getParamsFromContext(ctx))
	ctx = withExpressionFailureSlot(ctx)
	defer func() {
		if recorded := getExpressionFailure(ctx); recorded != nil && outcome.err == nil {
			outcome = newPipelineDispatchOutcome(nil, true, recorded)
		}
	}()
	hasCallSubquery := firstTopLevelCallSubquery(ctx, cypher) >= 0
	if !hasCallSubquery && startsWithKeywordFold(strings.TrimSpace(cypher), "UNWIND") {
		if !pipelineUnwindUsesRange(cypher) {
			plan, err := e.prepareTopLevelUnwind(ctx, cypher)
			if err != nil {
				return newPipelineDispatchOutcome(nil, true, err)
			}
			if result, handled, err := e.executeUnwindBatchOperator(ctx, plan); handled || err != nil {
				return newPipelineDispatchOutcome(result, handled, err)
			}
		}
	}
	clauses, ok := canExecuteAsPipeline(ctx, cypher)
	if !ok {
		return newPipelineDispatchOutcome(nil, false, nil)
	}
	originalClauses := clauses
	if pipelineHasClauseKind(clauses, pipelineClauseSet) {
		cypher = normalizePipelineWhitespace(cypher)
		clauses, ok = canExecuteAsPipeline(ctx, cypher)
		if !ok {
			return newPipelineDispatchOutcome(nil, false, nil)
		}
	}

	// Parameters remain typed bindings through every clause.
	params := getParamsFromContext(ctx)
	// The statement shortcuts read the store as if every variable were
	// unbound, so a correlated body (EXISTS / COUNT / COLLECT over an
	// imported variable) takes the row pipeline, which is seeded with the
	// imports below.
	if !hasCallSubquery && !e.pipelineReadsImportedBinding(ctx, clauses) {
		if result, handled, err := e.tryExecutePipelineSimpleNodeReadPlan(ctx, clauses, params); handled || err != nil {
			return newPipelineDispatchOutcome(result, handled, err)
		}
		if result, handled, err := e.tryExecutePipelineSimpleRelationshipCountPlan(ctx, clauses, params); handled || err != nil {
			return newPipelineDispatchOutcome(result, handled, err)
		}
	}
	if result, handled, err := e.tryExecutePipelineCreatePlan(ctx, clauses, originalClauses); handled || err != nil {
		return newPipelineDispatchOutcome(result, handled, err)
	}
	// Start with one binding row. Parameters retain their typed values under
	// their `$name` expression keys so list/map inputs are not stringified while
	// crossing WITH and UNWIND horizons.
	initialRow := pipelineRow{}
	for name, value := range e.fabricRecordBindings {
		initialRow[name] = value
	}
	if len(clauses) == 0 || clauses[0].kind != pipelineClauseCallSubquery {
		for name, value := range valueBindingsFromContext(ctx) {
			initialRow[name] = value
		}
	}
	bindParameterRow(ctx, initialRow)
	rows := []pipelineRow{initialRow}
	scope := make(map[string]struct{})
	for name := range initialRow {
		if strings.HasPrefix(name, "$") {
			continue
		}
		scope[name] = struct{}{}
	}

	stream, _ := ctx.Value(armedResultStreamKey{}).(*ResultStream)
	stream.bind(statement, clauses)
	result, handled, err := e.runPipelineClauseRows(ctx, rows, scope, clauses, originalClauses, nil)
	return newPipelineDispatchOutcome(result, handled, err)
}

type pipelineRowOutput struct {
	rows  []pipelineRow
	scope map[string]struct{}
	wrote bool
}

func (e *StorageExecutor) runPipelineClauseRows(ctx context.Context, rows []pipelineRow, scope map[string]struct{}, clauses, originalClauses []pipelineClause, output *pipelineRowOutput) (*ExecuteResult, bool, error) {
	stream, _ := ctx.Value(armedResultStreamKey{}).(*ResultStream)
	programClauses := originalClauses
	// Projections and effects are looked up by the executing clause's syntax.
	// When whitespace normalization re-read the statement, bind that source.
	if len(programClauses) == 0 || len(clauses) > 0 && clauses[0].syntax.Valid() && programClauses[0].syntax.Valid() &&
		clauses[0].syntax.Source() != programClauses[0].syntax.Source() {
		programClauses = clauses
	}
	var programErr error
	ctx, programErr = e.bindPipelineProgram(ctx, programClauses, scope)
	if programErr != nil {
		return nil, true, programErr
	}
	result := &ExecuteResult{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Stats:   &QueryStats{},
	}
	wrote := false
	if output != nil {
		wrote = output.wrote
	}
	if pipelineClausesMayDelete(clauses) {
		ctx = contextslot.Ensure[deletedEntities](ctx)
	}
	// A statement's MERGEs learn which of their nodes it writes once, from
	// all its clauses (a row-ordered stretch below sees only part of them);
	// a subquery or FOREACH body adds what its own clauses write.
	if output == nil && (pipelineHasClauseKind(clauses, pipelineClauseMerge) ||
		pipelineHasClauseKind(clauses, pipelineClauseCallSubquery) || pipelineHasClauseKind(clauses, pipelineClauseForeach)) {
		ctx = withMergeNodeWrites(ctx, clauses)
	}
	independentCreate, _ := ctx.Value(pipelineIndependentCreateBatchKey{}).(bool)
	if len(rows) > 1 && !independentCreate {
		prefix, writes := 0, false
		for _, clause := range clauses {
			if clause.kind == pipelineClauseReturn {
				break
			}
			if clause.kind == pipelineClauseWith {
				if _, local := parsePipelineRowWith(ctx, clause.text); !local {
					break
				}
			}
			switch clause.kind {
			case pipelineClauseCreate, pipelineClauseMerge, pipelineClauseSet, pipelineClauseRemove, pipelineClauseDelete, pipelineClauseForeach:
				writes = true
			case pipelineClauseCall:
				if procedure, found := globalProcedureRegistry.Get(extractProcedureName(ctx, clause.text)); found && procedure.Spec.writes() {
					writes = true
				}
			}
			prefix++
		}
		// Row by row only while no clause of the stretch reads what another
		// one writes (pipelineWriteStretchConflicts); otherwise clause by
		// clause, as Neo4j does.
		if writes && !pipelineWriteStretchConflicts(ctx, clauses[:prefix], clauses[prefix:]) {
			var retained []pipelineRow
			for _, row := range rows {
				rowScope := make(map[string]struct{}, len(scope))
				for name := range scope {
					rowScope[name] = struct{}{}
				}
				state := &pipelineRowOutput{wrote: wrote}
				partial, handled, err := e.runPipelineClauseRows(ctx, []pipelineRow{row}, rowScope, clauses[:prefix], originalClauses[:prefix], state)
				if !handled || err != nil {
					return partial, handled, err
				}
				addQueryStats(result.Stats, partial.Stats)
				retained = append(retained, state.rows...)
				wrote = state.wrote
				scope = state.scope
			}
			rows = retained
			clauses = clauses[prefix:]
			originalClauses = originalClauses[prefix:]
		}
	}
	var source pipelineRowSource
	var terminalCallColumns []string
	// The rows a MATCH, OPTIONAL MATCH or WITH builds count against the
	// statement's memory limit (#1343).
	memory := memoryScopeFromContext(ctx)
	var callChain boundCallChain
	callChainEnd := 0
	for idx := 0; idx < len(clauses); idx++ {
		clause := clauses[idx]
		if (clause.kind == pipelineClauseWith || clause.kind == pipelineClauseReturn) && !callChain.pagination[idx] {
			if err := e.validateRuntimePaginationClause(ctx, clause.text); err != nil {
				return result, true, err
			}
		}
		var callBatch *boundTransactionBatch
		if clause.kind == pipelineClauseCallSubquery {
			_, _, inTransactions, _ := e.parseCallSubquery(ctx, clause.text)
			if inTransactions {
				if idx >= callChainEnd {
					maximumRows := len(rows)
					if source != nil {
						maximumRows = -1
					}
					plan, err := e.planPipelineCallChain(ctx, clauses, idx, maximumRows)
					if err != nil {
						return result, true, err
					}
					callChain, err = e.bindTransactionalCallChain(ctx, plan)
					if err != nil {
						return result, true, err
					}
					callChainEnd = plan.end
				}
				callBatch = callChain.batches[idx]
			}
		}
		if source != nil && clause.kind != pipelineClauseMatch && clause.kind != pipelineClauseWith &&
			clause.kind != pipelineClauseLet && clause.kind != pipelineClauseFilter &&
			!(clause.kind == pipelineClauseCreate && independentCreate) &&
			!(clause.kind == pipelineClauseReturn && (pipelineClauseAggregates(ctx, clause) || stream.forClause(ctx, &clauses[idx]) != nil)) {
			var completed bool
			rows, completed = materializePipelineSource(ctx, source)
			source = nil
			if !completed {
				return pipelineDecline(ctx, wrote, clause.text)
			}
		}
		if deleted := contextslot.Get[deletedEntities](ctx); !deleted.empty() {
			if err := validateDeletedEntityReads(rows, deletedEntityReadText(clause), deleted); err != nil {
				return nil, true, err
			}
		}
		switch clause.kind {
		case pipelineClauseLet, pipelineClauseFilter:
			input := source
			if input == nil {
				input = pipelineRowsSource(rows)
			}
			transformed, err := e.pipelineSharedClauseSource(ctx, input, clause)
			if err != nil {
				return nil, true, err
			}
			if clause.kind == pipelineClauseLet {
				projections, err := parsePipelineLet(clause.text)
				if err != nil {
					return nil, true, err
				}
				for _, projection := range projections {
					scope[projection.alias] = struct{}{}
				}
			}
			source, rows = transformed, nil
			continue
		case pipelineClauseMatch:
			hint := e.pipelineMatchHint(ctx, clauses[idx+1:])
			hint.streamScan = source == nil && len(rows) == 1 && len(scope) == 0 && stream.streamsScan(clauses)
			input := source
			if input == nil {
				input = pipelineRowsSource(rows)
			}
			matched, supported, err := e.pipelineNodeMatchSourceWithHint(ctx, input, clause.text, hint)
			if err != nil {
				return nil, true, err
			}
			if supported {
				source, rows = matched, nil
				addPipelinePatternBindings(ctx, e, scope, clause.text, "MATCH")
				continue
			}
			if source != nil {
				var completed bool
				rows, completed = materializePipelineSource(ctx, source)
				source = nil
				if !completed {
					return pipelineDecline(ctx, wrote, clause.text)
				}
			}
			newRows, ok, err := e.pipelineApplyMatchWithHint(ctx, rows, clause.text, hint)
			if err != nil {
				return nil, true, err
			}
			if !ok {
				return pipelineDecline(ctx, wrote, clause.text)
			}
			if err := memory.growRows(newRows); err != nil {
				return nil, true, err
			}
			rows = newRows
			addPipelinePatternBindings(ctx, e, scope, clause.text, "MATCH")
		case pipelineClauseOptionalMatch:
			newRows, err := e.pipelineApplyOptionalMatch(ctx, rows, clause.text)
			if err != nil {
				return nil, true, err
			}
			if err := memory.growRows(newRows); err != nil {
				return nil, true, err
			}
			rows = newRows
			addPipelinePatternBindings(ctx, e, scope, clause.text, "OPTIONAL MATCH")
		case pipelineClauseCreate:
			end := idx + 1
			for end < len(clauses) && clauses[end].kind == pipelineClauseCreate {
				// Publish every earlier CREATE row before a later CREATE
				// reads those writes. Only independent clauses share a plan.
				if pipelineWriteStretchConflicts(ctx, clauses[idx:end+1], nil) {
					break
				}
				end++
			}
			var newRows []pipelineRow
			var created *ExecuteResult
			var ok bool
			var err error
			if source != nil && independentCreate {
				newRows, created, ok, err = e.pipelineCreateSource(ctx, source, clauses[idx:end])
				source = nil
			} else {
				newRows, created, ok, err = e.pipelineApplyCreateClauses(ctx, rows, clauses[idx:end])
			}
			if err != nil {
				return nil, true, err
			}
			if !ok {
				return pipelineDecline(ctx, wrote, clause.text)
			}
			rows = newRows
			for _, createClause := range clauses[idx:end] {
				addPipelinePatternBindings(ctx, e, scope, createClause.text, "CREATE")
			}
			addQueryStats(result.Stats, created.Stats)
			if created.Metadata != nil {
				result.Metadata = created.Metadata
			}
			idx = end - 1
			wrote = true
		case pipelineClauseMerge:
			newRows, stats, err := e.pipelineApplyMerge(ctx, rows, clause.text)
			if err != nil {
				return nil, true, err
			}
			rows = newRows
			addPipelinePatternBindings(ctx, e, scope, clause.text, "MERGE")
			if stats != nil {
				addQueryStats(result.Stats, stats)
			}
			wrote = true
			if idx+1 < len(clauses) {
				e.refreshRowEntities(ctx, rows)
			}
		case pipelineClauseDelete:
			stats, ok, err := e.pipelineApplyDelete(ctx, rows, scope, clause.text)
			if err != nil {
				return nil, true, err
			}
			if !ok {
				return pipelineDecline(ctx, wrote, clause.text)
			}
			addQueryStats(result.Stats, stats)
			wrote = true
		case pipelineClauseSet:
			assignment := clause.text
			for idx+1 < len(clauses) && clauses[idx+1].kind == pipelineClauseSet {
				idx++
				assignment += " " + clauses[idx].text
			}
			// Rows that bind one entity through different copies write one
			// entity: each row reads what the rows before it wrote.
			e.refreshRowEntities(ctx, rows)
			stats, ok, err := e.pipelineApplySet(ctx, rows, assignment)
			if err != nil {
				return nil, true, err
			}
			if !ok {
				return pipelineDecline(ctx, wrote, clause.text)
			}
			addQueryStats(result.Stats, stats)
			wrote = true
			if idx+1 < len(clauses) {
				e.refreshRowEntities(ctx, rows)
			}
		case pipelineClauseRemove:
			e.refreshRowEntities(ctx, rows)
			if err := e.pipelineApplyRemove(ctx, rows, clause.text, result); err != nil {
				return nil, true, err
			}
			wrote = true
			if idx+1 < len(clauses) {
				e.refreshRowEntities(ctx, rows)
			}
		case pipelineClauseWith:
			// The execution frame is the authoritative variable owner. It
			// already includes wildcard exports, scoped imports and UNION
			// branches; no syntax reader reconstructs those scopes.
			if name := clause.embeddingCandidate; name != "" {
				if binding, bound := pipelineFrameBinding(ctx, scope, name); !bound {
					if wrapper, ok := ctx.Value(ctxKeyTxStorage).(*transactionStorageWrapper); ok {
						mutated := wrapper.snapshotMutatedNodeIDs()
						if err := e.applyInlineEmbeddingMutations(ctx, mutated); err != nil {
							return nil, true, err
						}
						wrapper.clearMutatedNodeIDs(mutated)
					} else {
						return nil, true, localizedError(localization.CypherCoreEmbeddingTransactionStorageRequired(), nil)
					}
					continue
				} else {
					clause.text = "WITH " + binding
				}
			}
			// Carry imports after an aggregating projection so they do not add
			// grouping or DISTINCT keys. Other WITH clauses project imports in
			// pipelineClausesCarrying.
			carried := pipelineImports(ctx)
			carryAfter := len(carried) > 0 && pipelineClauseAggregates(ctx, clause)
			if source != nil && !carryAfter {
				if windowed, supported := e.pipelineWithWindowSource(ctx, source, rows, clause.text); supported {
					source = windowed
					scope = pipelineProjectionScope(ctx, scope, clause.text, pipelineImports(ctx))
					continue
				}
				if plan, local := parsePipelineRowWith(ctx, clause.text); local {
					source = e.pipelineWithRowSource(ctx, source, plan)
					scope = pipelineProjectionScope(ctx, scope, clause.text, pipelineImports(ctx))
					continue
				}
			}
			if source != nil {
				if !pipelineClauseAggregates(ctx, clause) {
					var completed bool
					rows, completed = materializePipelineSource(ctx, source)
					source = nil
					if !completed {
						return pipelineDecline(ctx, wrote, clause.text)
					}
				}
			}
			if err := e.validatePipelineWithRows(ctx, rows, clause.text); err != nil {
				return nil, true, err
			}
			// The rows just validated are the clause's input when no
			// stream feeds it.
			rowsValidated := source == nil
			if source == nil {
				source = pipelineRowsSource(rows)
			}
			newRows, ok := e.pipelineApplyWith(ctx, rows, clause.text, source, rowsValidated)
			source = nil
			if !ok {
				return pipelineDecline(ctx, wrote, clause.text)
			}
			if err := memory.growRows(newRows); err != nil {
				return nil, true, err
			}
			rows = newRows
			scope = pipelineProjectionScope(ctx, scope, clause.text, pipelineImports(ctx))
			if carryAfter {
				carryPipelineImports(rows, scope, carried)
			}
		case pipelineClauseUnwind:
			if err := e.validatePipelineRangeArguments(ctx, rows, clause.text, "UNWIND"); err != nil {
				return nil, true, err
			}
			unwound, consumed, ok := e.pipelineUnwindSource(ctx, rows, clauses[idx:])
			if !ok {
				return pipelineDecline(ctx, wrote, clause.text)
			}
			if alias := pipelineUnwindAlias(clause.text); alias != "" {
				scope[alias] = struct{}{}
			}
			for offset := 1; offset <= consumed; offset++ {
				scope = pipelineProjectionScope(ctx, scope, clauses[idx+offset].text, pipelineImports(ctx))
			}
			idx += consumed
			if idx+1 < len(clauses) && (pipelineClauseAggregates(ctx, clauses[idx+1]) || stream.forClause(ctx, &clauses[idx+1]) != nil) {
				source = unwound
				rows = nil
				continue
			}
			rows, ok = materializePipelineSource(ctx, unwound)
			if !ok {
				return pipelineDecline(ctx, wrote, clause.text)
			}
			if len(rows) > 1 && idx+1 < len(clauses) {
				state := &pipelineRowOutput{wrote: wrote}
				if output != nil {
					state = output
					state.wrote = wrote
				}
				remaining, handled, err := e.runPipelineClauseRows(ctx, rows, scope, clauses[idx+1:], originalClauses[idx+1:], state)
				if remaining != nil {
					addQueryStats(remaining.Stats, result.Stats)
				}
				return remaining, handled, err
			}
		case pipelineClauseCall:
			var newRows []pipelineRow
			var yielded []string
			ok := true
			var err error
			if clause.optional {
				newRows, err = pipelineApplyOptionally(rows, func(row []pipelineRow) ([]pipelineRow, []string, bool, error) {
					rowsOut, names, handled, err := e.pipelineApplyProcedureCall(ctx, row, clause.text, result.Stats)
					yielded = names
					return rowsOut, names, handled, err
				}, &ok)
			} else {
				newRows, yielded, ok, err = e.pipelineApplyProcedureCall(ctx, rows, clause.text, result.Stats)
			}
			if err != nil {
				return nil, true, err
			}
			if !ok {
				return pipelineDecline(ctx, wrote, clause.text)
			}
			rows = newRows
			for _, name := range yielded {
				scope[name] = struct{}{}
			}
			if procedure, found := globalProcedureRegistry.Get(extractProcedureName(ctx, clause.text)); found && procedure.Spec.writes() {
				wrote = true
			}
		case pipelineClauseCallSubquery:
			callMetadata := &pipelineCallMetadata{scope: scope, batch: callBatch}
			var newRows []pipelineRow
			var stats *QueryStats
			ok := true
			var err error
			if clause.optional && len(rows) > 0 {
				stats = &QueryStats{}
				newRows, err = pipelineApplyOptionally(rows, func(row []pipelineRow) ([]pipelineRow, []string, bool, error) {
					rowsOut, rowStats, handled, err := e.pipelineApplyCallSubqueryWithMetadata(ctx, row, clause.text, callMetadata)
					addQueryStats(stats, rowStats)
					return rowsOut, callMetadata.columns, handled, err
				}, &ok)
			} else {
				newRows, stats, ok, err = e.pipelineApplyCallSubqueryWithMetadata(ctx, rows, clause.text, callMetadata)
			}
			addQueryStats(result.Stats, stats)
			if err != nil {
				return result, true, err
			}
			if !ok {
				return pipelineDecline(ctx, wrote, clause.text)
			}
			rows = newRows
			terminalCallColumns = append(terminalCallColumns[:0], callMetadata.columns...)
			for _, name := range callMetadata.columns {
				scope[name] = struct{}{}
			}
			if callSubqueryQueryIsWrite(clause.text) || callMetadata.batch != nil {
				wrote = true
			}
		case pipelineClauseForeach:
			e.refreshRowEntities(ctx, rows)
			stats, err := e.pipelineApplyForeach(ctx, rows, clause)
			if err != nil {
				return nil, true, err
			}
			addQueryStats(result.Stats, stats)
			wrote = true
			if idx+1 < len(clauses) {
				e.refreshRowEntities(ctx, rows)
			}
		case pipelineClauseReturn:
			if stream.forClause(ctx, &clauses[idx]) != nil {
				columns, streamedRows, handled, err := e.pipelineStreamReturn(ctx, stream.forClause(ctx, &clauses[idx]), rows, source, clauses, originalClauses, idx, scope, wrote)
				if !handled || err != nil {
					return nil, handled, err
				}
				result.Columns, result.Rows = columns, streamedRows
				return result, true, nil
			}
			if err := e.validatePipelinePercentileArguments(ctx, rows, clause.text, "RETURN"); err != nil {
				return nil, true, err
			}
			if err := e.validatePipelineProjectionValues(ctx, rows, clause.text, "RETURN"); err != nil {
				return nil, true, err
			}
			// The rows just validated are the clause's input when no
			// stream feeds it.
			rowsValidated := source == nil
			if source == nil {
				source = pipelineRowsSource(rows)
			}
			// Fused input operators can change clause indexes; each retained
			// syntax node still names its own canonical projection.
			original := clause
			plan := returnProjectionPlanFor(ctx, original.text, original)
			if plan == nil {
				return nil, true, unresolvedStatementError()
			}
			final, ok := e.pipelineApplyReturnPlan(ctx, rows, plan, source, rowsValidated)
			source = nil
			if !ok {
				if failure := getExpressionFailure(ctx); failure != nil && final != nil {

					return final, true, failure
				}
				return pipelineDecline(ctx, wrote, clause.text)
			}

			result.Columns = final.Columns
			result.Rows = final.Rows
			// RETURN is always last.
			return result, true, nil
		}
		_ = idx
	}
	if source != nil {
		var completed bool
		rows, completed = materializePipelineSource(ctx, source)
		if !completed {
			return pipelineDecline(ctx, wrote, "end of pipeline")
		}
	}
	// A statement ending with a write has no columns and no rows, as in
	// Neo4j (#676).
	if output != nil {
		output.rows = rows
		output.scope = scope
		output.wrote = wrote
	}
	if output == nil && len(clauses) > 0 && clauses[len(clauses)-1].kind == pipelineClauseCallSubquery {
		body, _, _, _ := e.parseCallSubquery(ctx, clauses[len(clauses)-1].text)
		if topLevelKeywordIndex(body, "RETURN") >= 0 {
			result = callPipelineResultFromRows(rows, nil, result.Stats, terminalCallColumns)
		}
	}
	return result, true, nil
}

// pipelineItemUnevaluable records that a RETURN, WITH or UNWIND item can't
// be evaluated (neither the row evaluator nor the shared evaluator handles
// it), unless an expression error is already recorded. The statement fails:
// another route would not evaluate the item either, and the older UNWIND and
// WITH routes run only part of a statement the pipeline started. Subquery
// expressions (EXISTS / COUNT / COLLECT { … }) are row values like any other
// (evaluateRowExpressionWithContext), so they have no route of their own.
func pipelineItemUnevaluable(ctx context.Context, expr string) {
	if getExpressionFailure(ctx) != nil {
		return
	}
	recordExpressionFailure(ctx, localizedStatusError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax",
		localization.CypherCoreExpressionUnevaluable(strings.TrimSpace(expr))))
}

// pipelineDecline is the pipeline's "shape unsupported" answer. Before any
// clause has written it hands the statement to the other routes; once a
// clause has written (CREATE, MERGE, DELETE, SET, REMOVE, FOREACH) the
// statement cannot be run again by another route - that would repeat the
// writes - so it is an error instead.
func pipelineDecline(ctx context.Context, wrote bool, clause string) (*ExecuteResult, bool, error) {
	// A clause that stopped on an expression error doesn't decline: the error
	// is the statement's, and no other route runs the statement instead.
	if failure := getExpressionFailure(ctx); failure != nil {
		return nil, true, failure
	}
	if wrote {
		return nil, true, localizedError(localization.CypherInvariantsPipelineDeclinedAfterWrite(clause), nil)
	}
	return nil, false, nil
}

// pipelineReadsImportedBinding reports whether a clause mentions a variable
// the pipeline's first row imports: a correlated subquery body's outer row
// (fabricRecordBindings) or an enclosing value scope. Parameters are not
// imports. It uses isIdentifierReferenced, the reader unscoped CALL imports
// use, so a body that only names its own variables keeps the shortcuts.
func (e *StorageExecutor) pipelineReadsImportedBinding(ctx context.Context, clauses []pipelineClause) bool {
	references := func(name string) bool {
		if strings.HasPrefix(name, "$") {
			return false
		}
		for _, clause := range clauses {
			if isIdentifierReferenced(clause.text, name) {
				return true
			}
		}
		return false
	}
	for name := range e.fabricRecordBindings {
		if references(name) {
			return true
		}
	}
	if len(clauses) == 0 || clauses[0].kind != pipelineClauseCallSubquery {
		for name := range valueBindingsFromContext(ctx) {
			if references(name) {
				return true
			}
		}
	}
	return false
}

// tryExecutePipelineSimpleRelationshipCountPlan is the fused physical form of
// the pipeline's MATCH -> count reduction for relationship patterns (issue
// #638). Without it, the row pipeline binds the relationship variable for
// every match and counts the materialized rows — so
//
//	MATCH ()-[r:T]->() RETURN count(r)
//
// costs O(store size) instead of an O(1) counter read, even though the
// traversal-layer fast path exists. It delegates to relationshipCountFromStore,
// the single owner of counts answered from the counters (count_store.go).
func (e *StorageExecutor) tryExecutePipelineSimpleRelationshipCountPlan(ctx context.Context, clauses []pipelineClause, params map[string]interface{}) (*ExecuteResult, bool, error) {
	if len(clauses) != 2 || clauses[0].kind != pipelineClauseMatch || clauses[1].kind != pipelineClauseReturn {
		return nil, false, nil
	}
	matchBody := pipelineClauseBody(clauses[0].text, "MATCH")
	if topLevelKeywordIndex(matchBody, "WHERE") >= 0 {
		return nil, false, nil
	}
	// This counter plan owns a relationship pattern, never a path assignment
	// or shortestPath call. Decline their syntax before reading any values.
	if !strings.HasPrefix(matchBody, "(") || !containsRelationshipPattern(matchBody) {
		return nil, false, nil
	}
	if len(splitTopLevelComma(matchBody)) != 1 {
		return nil, false, nil
	}
	// A counter owns one projected item. Decline other projections before
	// reading a traversal that this plan cannot consume.
	items := e.parseReturnItems(ctx, strings.TrimSpace(clauses[1].text[len("RETURN"):]))
	if len(items) != 1 {
		return nil, false, nil
	}
	if !patternMapsAreConstant(matchBody) {
		return nil, false, nil
	}
	matches := e.parseTraversalPatternStructure(ctx, matchBody)
	if matches == nil {
		return nil, false, nil
	}
	count, ok, err := e.relationshipCountFromStore(ctx, matches, items[0])
	if err != nil {
		return nil, true, err
	}
	if !ok {
		return nil, false, nil
	}
	column := items[0].column()
	return &ExecuteResult{Columns: []string{column}, Rows: [][]interface{}{{count}}, Stats: &QueryStats{}}, true, nil
}

// tryExecutePipelineSimpleNodeReadPlan applies cardinality and property-index
// operators before row materialization for a single node MATCH. The result is
// still projected by the pipeline, so indexed and scanned inputs share the
// same expression semantics.
func (e *StorageExecutor) tryExecutePipelineSimpleNodeReadPlan(ctx context.Context, clauses []pipelineClause, params map[string]interface{}) (*ExecuteResult, bool, error) {
	stream, _ := ctx.Value(armedResultStreamKey{}).(*ResultStream)
	if len(clauses) != 2 || clauses[0].kind != pipelineClauseMatch || clauses[1].kind != pipelineClauseReturn {
		return nil, false, nil
	}
	matchBody := pipelineClauseBody(clauses[0].text, "MATCH")
	whereClause := ""
	if whereIndex := topLevelKeywordIndex(matchBody, "WHERE"); whereIndex >= 0 {
		whereClause = strings.TrimSpace(matchBody[whereIndex+len("WHERE"):])
		matchBody = strings.TrimSpace(matchBody[:whereIndex])
	}
	// A path assignment (p = (n:L)) is the general route's, which binds the
	// path: read as a node, its variable would be "p = (n".
	if !e.isSingleNodePattern(matchBody) || extractPathAssignmentVariable(matchBody) != "" {
		return nil, false, nil
	}
	if !patternMapsAreConstant(matchBody) || !patternMapsAreConstant(whereClause) || stream.forClause(ctx, &clauses[1]) != nil {
		return nil, false, nil
	}
	nodePattern, parseErr := e.parseNodePattern(ctx, matchBody)
	if parseErr != nil {
		return nil, true, parseErr
	}
	if nodePattern.variable == "" {
		return nil, false, nil
	}

	items := e.parseReturnItems(ctx, strings.TrimSpace(clauses[1].text[len("RETURN"):]))
	if len(items) > 1 && items[0].expr == "*" {
		// RETURN *, items: the general projection writes the * out (#883).
		return nil, false, nil
	}
	hint := e.pipelineMatchHint(ctx, clauses[1:])
	boundedSimpleProjection := whereClause == "" && hint.earlyLimit > 0 && pipelineSimpleNodeProjections(items, nodePattern.variable)
	countColumn, filteredCount := pipelineSingleNodeCountProjection(items, nodePattern.variable)
	if whereClause == "" && len(items) == 1 {
		if count, ok, err := e.nodeCountFromStore(ctx, nodePattern, items[0]); ok {
			if err != nil {
				return nil, true, localizedError(localization.CypherMatchingStorageFailed(err), err)
			}
			return &ExecuteResult{Columns: []string{items[0].column()}, Rows: [][]interface{}{{count}}, Stats: &QueryStats{}}, true, nil
		}
	}

	candidates, usedIndex, err := e.tryCollectNodesFromPropertyIndexInOrParam(nodePattern, whereClause, params)
	if err != nil {
		return nil, true, err
	}
	if !usedIndex {
		if filteredCount && (whereClause != "" || len(nodePattern.properties) > 0) {
			if result, streamed, streamErr := e.tryStreamPipelineFilteredNodeCount(ctx, nodePattern, whereClause, countColumn); streamed || streamErr != nil {
				return result, true, streamErr
			}
		}
		if !boundedSimpleProjection {
			return nil, false, nil
		}
		// boundedSimpleProjection requires an empty WHERE, so no seek can
		// have applied one here.
		candidates, _, err = e.collectPipelineInitialNodeCandidates(ctx, nodePattern, whereClause, hint)
		if err != nil {
			return nil, true, err
		}
	}
	if boundedSimpleProjection {
		if len(candidates) > hint.earlyLimit {
			candidates = candidates[:hint.earlyLimit]
		}
		return e.projectVisiblePipelineNodeRead(candidates, items, nodePattern.variable), true, nil
	}
	rows := make([]pipelineRow, 0, len(candidates))
	for _, node := range candidates {
		row := pipelineNodeRow(ctx, nodePattern.variable, node)
		if whereClause == "" || e.evaluateWithWhereCondition(ctx, whereClause, map[string]interface{}(row)) {
			rows = append(rows, row)
		}
	}
	result, projected := e.pipelineApplyReturn(ctx, rows, clauses[1].text)
	if !projected {
		return nil, false, nil
	}
	return result, true, nil
}

// pipelineNodeRow is the binding row of a single-node pipeline read: the node
// under its variable and every parameter under "$name", as row values
// (parameterRowValues). The row loop and the fused filtered count evaluate
// WHERE on it.
func pipelineNodeRow(ctx context.Context, variable string, node *storage.Node) pipelineRow {
	paramRows := parameterRowValues(ctx)
	row := make(pipelineRow, len(paramRows)+1)
	for name, value := range paramRows {
		row[name] = value
	}
	row[variable] = node
	return row
}

func pipelineSingleNodeCountProjection(items []returnItem, variable string) (string, bool) {
	if len(items) != 1 || variable == "" || !countsRows(items[0], variable) {
		return "", false
	}
	return items[0].column(), true
}

// tryStreamPipelineFilteredNodeCount is the fused physical form of the
// pipeline's MATCH -> filter -> count reduction. It consumes the same
// snapshot-aware label stream as the general row operator but reduces each
// qualifying binding immediately, avoiding a node slice and one map-backed
// pipelineRow per match.
func (e *StorageExecutor) tryStreamPipelineFilteredNodeCount(
	ctx context.Context,
	nodePattern nodePatternInfo,
	whereClause string,
	column string,
) (*ExecuteResult, bool, error) {
	if len(nodePattern.labels) == 0 {
		return nil, false, nil
	}
	store := e.getStorage(ctx)
	reader, ok := store.(storage.ProjectedLabelNodeReader)
	if !ok {
		return nil, false, nil
	}

	predicateRow := pipelineNodeRow(ctx, nodePattern.variable, nil)
	predicatePlan := planRowPredicate(whereClause)
	hasWhere := strings.TrimSpace(whereClause) != ""
	whereFilter := func(node *storage.Node) bool {
		predicateRow[nodePattern.variable] = node
		if !hasWhere {
			return true
		}
		if predicatePlan != nil && predicatePlan.complete {
			return e.evaluateRowPredicatePlan(ctx, predicatePlan, predicateRow)
		}
		return e.evaluateRowPredicate(ctx, whereClause, predicateRow)
	}
	hideSystemNodes := shouldHideSystemNodes(store)
	viewport, hasViewport := TemporalViewportFromContext(ctx)
	checker, canCheckViewport := store.(temporalCurrentNodeChecker)
	var count int64
	countNode := func(node *storage.Node, whereApplied bool) error {
		if node == nil || (hideSystemNodes && isSystemNode(node)) {
			return nil
		}
		if !mergeNodeHasLabels(node, nodePattern.labels) || !e.nodeMatchesProps(node, nodePattern.properties) || (!whereApplied && !whereFilter(node)) {
			return nil
		}
		if hasViewport && canCheckViewport {
			visible, visibleErr := checker.IsCurrentTemporalNode(node, viewport.AsOf)
			if visibleErr != nil {
				return visibleErr
			}
			if !visible {
				return nil
			}
		}
		count++
		return nil
	}
	// A lookup an index can narrow is counted from the same seed the row
	// read uses (#820); only otherwise is the label streamed.
	candidates, whereApplied, indexed, err := e.collectPipelineIndexedNodeCandidates(ctx, nodePattern, whereClause, pipelineMatchPhysicalHint{})
	if err != nil {
		return nil, true, err
	}
	if indexed {
		for _, node := range candidates {
			if err := countNode(node, whereApplied); err != nil {
				return nil, true, localizedError(localization.CypherMatchingStorageFailed(err), err)
			}
		}
		return &ExecuteResult{Columns: []string{column}, Rows: [][]interface{}{{count}}, Stats: &QueryStats{}}, true, nil
	}
	projectedProperties := pipelineNodePredicateProperties(nodePattern.variable, whereClause, nodePattern.properties)
	// A count only reads its candidates and keeps none.
	stream := reader.StreamNodesByLabelProjected
	if readOnly, ok := store.(storage.ReadOnlyLabelNodeReader); ok {
		stream = readOnly.VisitReadOnlyNodesByLabel
	}
	err = stream(nodePattern.labels[0], projectedProperties, func(node *storage.Node) error {
		return countNode(node, false)
	})
	if err != nil {
		return nil, true, localizedError(localization.CypherMatchingStorageFailed(err), err)
	}
	return &ExecuteResult{
		Columns: []string{column},
		Rows:    [][]interface{}{{count}},
		Stats:   &QueryStats{},
	}, true, nil
}

func pipelineNodePredicateProperties(variable, expression string, inline map[string]interface{}) []string {
	properties := make([]string, 0, len(inline)+2)
	seen := make(map[string]struct{}, len(inline)+2)
	for property := range inline {
		seen[property] = struct{}{}
		properties = append(properties, property)
	}
	prefix := normalizeProjectionColumnName(variable) + "."
	for _, reference := range semanticExpressionReferences(expression) {
		if reference == normalizeProjectionColumnName(variable) {
			return nil
		}
		if !strings.HasPrefix(reference, prefix) {
			continue
		}
		property := strings.TrimPrefix(reference, prefix)
		if nested := strings.IndexByte(property, '.'); nested >= 0 {
			property = property[:nested]
		}
		if property == "" {
			return nil
		}
		if _, exists := seen[property]; exists {
			continue
		}
		seen[property] = struct{}{}
		properties = append(properties, property)
	}
	sort.Strings(properties)
	return properties
}

func projectPipelineSimpleNodeRead(nodes []*storage.Node, items []returnItem, variable string) *ExecuteResult {
	result := &ExecuteResult{
		Columns: make([]string, len(items)),
		Rows:    make([][]interface{}, 0, len(nodes)),
		Stats:   &QueryStats{},
	}
	for index, item := range items {
		result.Columns[index] = item.column()
	}
	prefix := variable + "."
	for _, node := range nodes {
		row := make([]interface{}, len(items))
		for index, item := range items {
			expression := strings.TrimSpace(item.expr)
			if expression == variable {
				row[index] = node
				continue
			}
			property := normalizeProjectionColumnName(strings.TrimSpace(expression[len(prefix):]))
			row[index] = node.Properties[property]
		}
		result.Rows = append(result.Rows, row)
	}
	return result
}

func pipelineSimpleNodeProjections(items []returnItem, variable string) bool {
	if len(items) == 0 || variable == "" {
		return false
	}
	for _, item := range items {
		expression := strings.TrimSpace(item.expr)
		if expression == variable {
			continue
		}
		prefix := variable + "."
		if !strings.HasPrefix(expression, prefix) {
			return false
		}
		property := strings.TrimSpace(expression[len(prefix):])
		if property == "" || strings.ContainsAny(property, ".()[]{}+-*/%^<>=, ") {
			return false
		}
	}
	return true
}

func pipelineHasClauseKind(clauses []pipelineClause, kind pipelineClauseKind) bool {
	for _, clause := range clauses {
		if clause.kind == kind {
			return true
		}
	}
	return false
}

func normalizePipelineWhitespace(query string) string {
	if strings.IndexFunc(query, cyphertext.NonCanonicalWhitespace) < 0 {
		return strings.TrimSpace(query)
	}
	var normalized strings.Builder
	normalized.Grow(len(query))
	spacePending := false
	for index := 0; index < len(query); index++ {
		character := query[index]
		if isCypherQuoteByte(character) {
			if spacePending && normalized.Len() > 0 {
				normalized.WriteByte(' ')
			}
			spacePending = false
			end := skipCypherQuotedText(query, index, character)
			normalized.WriteString(query[index:end])
			index = end - 1
			continue
		}
		if cyphertext.IsWhitespaceAt(query, index) {
			spacePending = normalized.Len() > 0
			continue
		}
		if spacePending {
			normalized.WriteByte(' ')
			spacePending = false
		}
		normalized.WriteByte(character)
	}
	return strings.TrimSpace(normalized.String())
}

// ---- clause appliers ----

func (e *StorageExecutor) pipelineApplyOptionalMatch(ctx context.Context, rows []pipelineRow, clause string) ([]pipelineRow, error) {
	body := pipelineClauseBody(clause, "OPTIONAL MATCH")
	if pattern, repeatable := stripRepeatableElements(body); repeatable {
		ctx, body = withRepeatableElements(ctx), pattern
		clause = "OPTIONAL MATCH " + body
	}
	if shortest, ok, err := e.parseShortestPathMatch(ctx, body); ok || err != nil {
		if err != nil {
			return nil, err
		}
		return e.pipelineApplyShortestPathMatch(ctx, rows, shortest, true)
	}
	if err := validateBoundPatternNodes(rows, body); err != nil {
		return nil, err
	}
	optionalClause := splitOptionalMatchClauses(body)
	if len(optionalClause) != 1 {
		return nil, localizedError(localization.CypherCoreOptionalMatchRequired(), nil)
	}
	nodeVariables := make(map[string]struct{})
	for _, variable := range extractNodeVariables(matchPatternStructure(clause)) {
		nodeVariables[variable] = struct{}{}
	}
	relationshipVariables := make(map[string]struct{})
	for _, variable := range extractRelationshipVariables(matchPatternStructure(clause)) {
		relationshipVariables[variable] = struct{}{}
	}

	// Convert every row, then left-outer-join the clause in one call: each
	// output row starts from a full copy of its input row, in input order.
	traversalRows := make([]traversalOptRow, 0, len(rows))
	for _, row := range rows {
		traversalRow := traversalOptRow{
			nodes: make(map[string]*storage.Node),
			rels:  make(map[string]*storage.Edge),
		}
		for name, value := range row {
			switch entity := value.(type) {
			case *storage.Node:
				traversalRow.nodes[name] = entity
			case *storage.Edge:
				traversalRow.rels[name] = entity
			default:
				if value == nil {
					if _, isNodeBinding := nodeVariables[name]; isNodeBinding {
						traversalRow.nodes[name] = nil
						continue
					}
					if _, isRelationshipBinding := relationshipVariables[name]; isRelationshipBinding {
						traversalRow.rels[name] = nil
						continue
					}
				}
				if traversalRow.values == nil {
					traversalRow.values = make(map[string]interface{})
				}
				traversalRow.values[name] = value
			}
		}
		traversalRows = append(traversalRows, traversalRow)
	}

	expanded, err := e.applyTraversalOptionalClause(ctx, traversalRows, optionalClause[0])
	if err != nil {
		return nil, err
	}
	out := make([]pipelineRow, 0, len(expanded))
	for _, expandedRow := range expanded {
		joined := make(pipelineRow, len(expandedRow.nodes)+len(expandedRow.rels)+len(expandedRow.values))
		for name, node := range expandedRow.nodes {
			if node == nil {
				joined[name] = nil
			} else {
				joined[name] = node
			}
		}
		for name, relationship := range expandedRow.rels {
			if relationship == nil {
				joined[name] = nil
			} else {
				joined[name] = relationship
			}
		}
		for name, value := range expandedRow.values {
			joined[name] = value
		}
		out = append(out, joined)
	}
	return out, nil
}

// pipelineApplyMatch runs MATCH for each current binding row and expands rows
// by the matched combinations. Returns (newRows, true, nil) on success. If a
// MATCH in the middle of a pipeline binds zero rows, it does NOT fail — it
// just zeros out the pipeline (matches Neo4j semantics for chained MATCH).
func (e *StorageExecutor) pipelineApplyMatch(ctx context.Context, rows []pipelineRow, clause string) ([]pipelineRow, bool, error) {
	return e.pipelineApplyMatchWithHint(ctx, rows, clause, pipelineMatchPhysicalHint{limit: -1, earlyLimit: -1})
}

func (e *StorageExecutor) pipelineApplyMatchWithHint(ctx context.Context, rows []pipelineRow, clause string, hint pipelineMatchPhysicalHint) ([]pipelineRow, bool, error) {
	body := pipelineClauseBody(clause, "MATCH")
	if stripped, repeatable := stripRepeatableElements(body); repeatable {
		ctx, body, clause = withRepeatableElements(ctx), stripped, "MATCH "+stripped
	}
	if err := validateBoundPatternNodes(rows, body); err != nil {
		return nil, true, err
	}
	if shortest, ok, err := e.parseShortestPathMatch(ctx, body); ok || err != nil {
		if err != nil {
			return nil, true, err
		}
		expanded, err := e.pipelineApplyShortestPathMatch(ctx, rows, shortest, false)
		return expanded, true, err
	}
	if quantified, ok, err := parseQuantifiedPathMatch(body); ok || err != nil {
		if err != nil {
			return nil, true, err
		}
		expanded, err := e.pipelineApplyQuantifiedPathMatch(ctx, rows, quantified)
		return expanded, true, err
	}
	patternEnd := len(body)
	if where := topLevelKeywordIndex(body, "WHERE"); where >= 0 {
		patternEnd = where
	}
	parts := splitTopLevelComma(body[:patternEnd])
	if len(parts) > 1 && extractPathAssignmentVariable(parts[len(parts)-1]) != "" {
		nodePrefix := true
		for _, part := range parts[:len(parts)-1] {
			if !strings.HasPrefix(strings.TrimSpace(part), "(") || containsRelationshipPattern(part) {
				nodePrefix = false
				break
			}
		}
		if nodePrefix {
			for index, part := range parts {
				if index == len(parts)-1 {
					part += " " + body[patternEnd:]
				}
				expanded, handled, err := e.pipelineApplyMatch(ctx, rows, "MATCH "+part)
				if !handled || err != nil {
					return expanded, handled, err
				}
				rows = expanded
			}
			return rows, true, nil
		}
	}
	if product, supported, err := e.pipelineNodeProductSource(ctx, rows, clause); supported || err != nil {
		if err != nil {
			return nil, true, err
		}
		expanded, resolved := materializePipelineSource(ctx, product)
		if !resolved {
			return nil, true, getExpressionFailure(ctx)
		}
		return expanded, true, nil
	}
	if len(parts) > 1 || len(parts) == 1 && strings.HasPrefix(strings.TrimSpace(parts[0]), "(") &&
		!containsRelationshipPattern(parts[0]) && nodePatternStructure(parts[0]).variable == "" {
		where := ""
		if patternEnd < len(body) {
			where = strings.TrimSpace(body[patternEnd+len("WHERE"):])
		}
		return e.pipelineApplyMatchProduct(ctx, rows, parts, where)
	}
	if expanded, ok, err := e.pipelineApplyBoundRelationshipListMatch(ctx, rows, clause); ok || err != nil {
		return expanded, ok, err
	}
	if expanded, ok, err := e.pipelineApplyInitialTraversalMatch(ctx, rows, clause, hint); ok || err != nil {
		return expanded, ok, err
	}
	if expanded, ok, err := e.pipelineApplyInitialNodeMatch(ctx, rows, clause, hint); ok || err != nil {
		return expanded, ok, err
	}
	if expanded, ok := e.pipelineApplyChainedMatch(ctx, rows, clause); ok {
		return expanded, true, nil
	}

	return nil, false, nil
}

func pipelineBindingValuesEqual(left, right interface{}) bool {
	switch typed := left.(type) {
	case *storage.Node:
		other, ok := right.(*storage.Node)
		return ok && typed != nil && other != nil && typed.Identity() == other.Identity()
	case *storage.Edge:
		other, ok := right.(*storage.Edge)
		return ok && typed != nil && other != nil && typed.Identity() == other.Identity()
	default:
		return reflect.DeepEqual(left, right)
	}
}

// pipelineReadOnlyCandidatesKey asks a label scan for read-only candidates
// (storage.ReadOnlyLabelNodeReader); the scan sets the *bool when it uses them.
type pipelineReadOnlyCandidatesKey struct{}

func (e *StorageExecutor) pipelineApplyInitialNodeMatch(ctx context.Context, rows []pipelineRow, clause string, hint pipelineMatchPhysicalHint) ([]pipelineRow, bool, error) {
	if len(rows) == 0 {
		return rows, true, nil
	}
	pattern := pipelineClauseBody(clause, "MATCH")
	whereClause := ""
	if whereIndex := topLevelKeywordIndex(pattern, "WHERE"); whereIndex >= 0 {
		whereClause = normalizePipelineWhitespace(pattern[whereIndex+len("WHERE"):])
		pattern = strings.TrimSpace(pattern[:whereIndex])
	}
	if !e.isSingleNodePattern(pattern) {
		return nil, false, nil
	}
	pathVariable := extractPathAssignmentVariable(pattern)
	if pathVariable != "" {
		// p = (n:L): the node pattern is what follows the assignment. Parsed
		// with the assignment, its variable was "p = (n" and it matched no
		// node.
		_, pattern, _ = splitPathAssignment(pattern)
	}
	basePattern := nodePatternStructure(pattern)
	if basePattern.variable == "" {
		return nil, false, nil
	}
	type initialCandidates struct {
		nodes        []*storage.Node
		whereApplied bool
		readOnly     bool
	}
	candidateCache := make(map[string]initialCandidates)
	template := e.pipelineNodeMatchTemplateFor(clause)
	out := make([]pipelineRow, 0, len(rows))
	for _, row := range rows {
		materializedWhere := whereClause
		// The pattern parsed once and its properties evaluated for the row;
		// the text route for a row the template can't evaluate.
		nodePattern, templated, parseErr := template.node(ctx, e, row)
		if parseErr != nil {
			return nil, true, parseErr
		}
		var cacheKey string
		if templated {
			if propsKey, keyed := pipelinePropertiesKey(nodePattern.properties); keyed {
				cacheKey = "\x03" + propsKey + "\x00" + materializedWhere
			}
		} else {
			rowCtx := withValueBindings(ctx, row)
			materializedPattern := pattern
			nodePattern, parseErr = e.parseNodePattern(rowCtx, materializedPattern)
			if parseErr != nil {
				return nil, true, parseErr
			}
			cacheKey = ""
		}
		if bound, exists := row[nodePattern.variable]; exists {
			node, isNode := bound.(*storage.Node)
			if !isNode || node == nil || !e.pipelineNodeMatchesPattern(node, nodePattern) {
				continue
			}
			// The WHERE sees the path too (WHERE length(p) = 0).
			bound := e.pipelineBindZeroLengthPath(row, pathVariable, node)
			if materializedWhere == "" || e.evaluateMatchWhereCondition(ctx, materializedWhere, map[string]interface{}(bound)) {
				out = append(out, bound)
			}
			continue
		}
		candidateHint := hint
		for name := range row {
			if name != nodePattern.variable && referencesVariable(whereClause, name) {
				cacheKey = ""
				candidateHint.earlyLimit = -1
				break
			}
		}
		// A WHERE that reads the path is tested below, where the path is
		// bound; the candidates are collected without it.
		candidateWhere := materializedWhere
		if pathVariable != "" && referencesVariable(whereClause, pathVariable) {
			candidateWhere = ""
			cacheKey = ""
		}
		candidates, cached := candidateCache[cacheKey]
		if cacheKey == "" || !cached {
			var err error
			scanCtx := withValueBindings(ctx, row)
			// The predicate below reads the scan's candidates as they are; a
			// row that passes takes its copy. A path binds the node itself,
			// so it reads ordinary results.
			candidates.readOnly = false
			if pathVariable == "" {
				scanCtx = context.WithValue(scanCtx, pipelineReadOnlyCandidatesKey{}, &candidates.readOnly)
			}
			candidates.nodes, candidates.whereApplied, err = e.collectPipelineInitialNodeCandidates(scanCtx, nodePattern, candidateWhere, candidateHint)
			if err != nil {
				return nil, true, err
			}
			if cacheKey != "" {
				candidateCache[cacheKey] = candidates
			}
		}
		if candidates.whereApplied {
			materializedWhere = ""
		}
		// The predicate is tested on one row reused for every candidate; a
		// row is built only for a candidate that passes. A predicate with a
		// complete plan is planned once for the row's candidates.
		var probe pipelineRow
		var plan *rowPredicatePlan
		if materializedWhere != "" {
			if planned := planRowPredicate(materializedWhere); planned != nil && planned.complete {
				plan = planned
			}
		}
		for _, node := range candidates.nodes {
			var path interface{}
			if pathVariable != "" {
				path = e.pathToMap(PathResult{Nodes: []*storage.Node{node}})
			}
			if materializedWhere != "" {
				if probe == nil {
					probe = make(pipelineRow, len(row)+2)
					for name, value := range row {
						probe[name] = value
					}
				}
				probe[nodePattern.variable] = node
				if pathVariable != "" {
					probe[pathVariable] = path
				}
				if plan != nil {
					if !e.evaluateRowPredicatePlan(ctx, plan, map[string]interface{}(probe)) {
						continue
					}
				} else if !e.evaluateMatchWhereCondition(ctx, materializedWhere, map[string]interface{}(probe)) {
					continue
				}
			}
			if candidates.readOnly {
				node = storage.CopyNode(node)
			}
			joined := make(pipelineRow, len(row)+2)
			for name, value := range row {
				joined[name] = value
			}
			joined[nodePattern.variable] = node
			if pathVariable != "" {
				joined[pathVariable] = path
			}
			out = append(out, joined)
		}
	}
	return out, true, nil
}

func (e *StorageExecutor) pipelineBindZeroLengthPath(row pipelineRow, variable string, node *storage.Node) pipelineRow {
	if variable == "" {
		return row
	}
	joined := make(pipelineRow, len(row)+1)
	for name, value := range row {
		joined[name] = value
	}
	joined[variable] = e.pathToMap(PathResult{Nodes: []*storage.Node{node}})
	return joined
}

// replaceQualifiedReferenceOutsideQuotes replaces a complete dotted row
// reference without rewriting string literals, longer identifiers, or a
// property access rooted at the reference. replaceIdentifierOutsideQuotes is
// intentionally token-oriented and therefore cannot match a dotted name.
func replaceQualifiedReferenceOutsideQuotes(input, reference, replacement string) string {
	return replaceQualifiedReferenceOutsideQuotesUnless(input, reference, replacement, nil, "")
}

// replaceQualifiedReferenceOutsideQuotesUnless is
// replaceQualifiedReferenceOutsideQuotes that leaves the occurrences that
// read a local variable named variable, the reference's root (locals.keeps).
func replaceQualifiedReferenceOutsideQuotesUnless(input, reference, replacement string, locals *localOccurrences, variable string) string {
	if reference == "" || !strings.Contains(input, reference) {
		return input
	}
	if locals != nil && !locals.binds(variable) {
		locals = nil
	}
	var output strings.Builder
	output.Grow(len(input) + len(replacement))
	for index := 0; index < len(input); {
		if end, quoted := skipCypherLexicalToken(input, index); quoted {
			output.WriteString(input[index:end])
			index = end
			continue
		}
		character := input[index]
		end := index + len(reference)
		if end <= len(input) && input[index:end] == reference &&
			(index == 0 || (!cyphertext.NamePartAt(input, index-1) && input[index-1] != '.')) &&
			(end == len(input) || (!cyphertext.NamePartAt(input, end) && input[end] != '.')) &&
			(locals == nil || !locals.keeps(input, variable, index)) {
			output.WriteString(replacement)
			index = end
			continue
		}
		output.WriteByte(character)
		index++
	}
	return output.String()
}

// whereIsSimpleIndexedIn reports whether a WHERE is exactly
// <variable>.<property> IN <parameter or literal list>, the form the IN-list
// plan answers completely.
func (e *StorageExecutor) whereIsSimpleIndexedIn(ctx context.Context, variable, whereClause string, params map[string]interface{}) bool {
	if _, _, ok := e.parseSimpleIndexedInParam(variable, whereClause, params); ok {
		return true
	}
	_, _, ok := e.parseSimpleIndexedInLiteral(ctx, variable, whereClause)
	return ok
}

// collectPipelineInitialNodeCandidates chooses an indexed seed whenever one
// of the shared property-index operators can safely narrow the MATCH, and
// streams the label otherwise. The complete predicate is still evaluated
// after the join, so these operators only affect the physical seed source and
// never the logical result.
type pipelinePrefetchedNodeCandidatesKey struct{}

func (e *StorageExecutor) collectPipelineInitialNodeCandidates(ctx context.Context, nodePattern nodePatternInfo, whereClause string, hint pipelineMatchPhysicalHint) (nodes []*storage.Node, whereApplied bool, err error) {
	if node := e.prefetchedPipelineCandidate(ctx, nodePattern); node != nil {
		return []*storage.Node{node}, false, nil
	}
	// A property equal to null matches no node ({id: null}, nothing equals
	// null): no index seek or label scan can find one.
	for _, value := range nodePattern.properties {
		if value == nil {
			return nil, false, nil
		}
	}
	nodes, whereApplied, used, err := e.collectPipelineIndexedNodeCandidates(ctx, nodePattern, whereClause, hint)
	if err != nil || used {
		return nodes, whereApplied, err
	}
	properties, streamingWhere, projection := e.pipelineLabelScanArguments(ctx, nodePattern, whereClause, hint)
	nodes, err = e.collectNodesWithStreamingProjection(ctx, nodePattern.labels, properties, nodePattern.variable, streamingWhere, hint.earlyLimit, projection)
	if len(nodePattern.properties) > 0 {
		e.markMergeScanFallbackUsed()
	}
	return nodes, false, err
}

// visitPipelineInitialNodeCandidates is collectPipelineInitialNodeCandidates
// for a MATCH whose rows are consumed as they are made (#939): the
// candidates go to visit as the scan reads them, so the scan advances only
// as far as the rows consumed. An indexed seed is read first, as there.
// visit returning storage.ErrIterationStopped ends the scan without error.
func (e *StorageExecutor) visitPipelineInitialNodeCandidates(ctx context.Context, nodePattern nodePatternInfo, hint pipelineMatchPhysicalHint, visit func(*storage.Node) error) error {
	// A streamed MATCH is the statement's first clause, over one row, so
	// no batch prefetch applies to it (prefetchedPipelineCandidate).
	nodes, _, used, err := e.collectPipelineIndexedNodeCandidates(ctx, nodePattern, "", hint)
	if err != nil {
		return err
	}
	if used {
		return visitNodeList(nodes, visit)
	}
	properties, streamingWhere, projection := e.pipelineLabelScanArguments(ctx, nodePattern, "", hint)
	err = e.visitNodesWithStreamingProjection(ctx, nodePattern.labels, properties, nodePattern.variable, streamingWhere, hint.earlyLimit, projection, nil, visit)
	if len(nodePattern.properties) > 0 {
		e.markMergeScanFallbackUsed()
	}
	return err
}

// prefetchedPipelineCandidate is the node a batch prefetch already found for
// a one-label, one-property pattern, or nil.
func (e *StorageExecutor) prefetchedPipelineCandidate(ctx context.Context, nodePattern nodePatternInfo) *storage.Node {
	prefetched, ok := ctx.Value(pipelinePrefetchedNodeCandidatesKey{}).(map[nodeBatchMatchKey]map[string]*storage.Node)
	if !ok || len(nodePattern.labels) != 1 || len(nodePattern.properties) != 1 {
		return nil
	}
	for property, value := range nodePattern.properties {
		key := nodeBatchMatchKey{label: nodePattern.labels[0], prop: property}
		if node := prefetched[key][propEqKeyBatch(value)]; node != nil && e.pipelineNodeMatchesPattern(node, nodePattern) {
			return node
		}
	}
	return nil
}

// pipelineLabelScanArguments are the filters and projection of the label
// (or label-less) scan that seeds a MATCH no index narrows.
func (e *StorageExecutor) pipelineLabelScanArguments(ctx context.Context, nodePattern nodePatternInfo, whereClause string, hint pipelineMatchPhysicalHint) (properties map[string]interface{}, streamingWhere string, projection []string) {
	if hint.earlyLimit > 0 {
		streamingWhere = whereClause
	}
	properties = nodePattern.properties
	if len(nodePattern.labels) == 0 && streamingWhere == "" {
		// The rows are filtered by the WHERE afterwards; its top-level
		// equalities still let a label-less scan skip the nodes that can't
		// match before decoding them (#857).
		properties = e.labellessScanRequiredProperties(ctx, properties, nodePattern.variable, whereClause)
	}
	projection, projected := pipelineLabelScanProjection(ctx, nodePattern, whereClause, hint.readTail)
	if !projected {
		projection = nil
	}
	return properties, streamingWhere, projection
}

// pipelineApplyChainedMatch expands a MATCH against graph bindings already in
// each row. It is the pipeline adapter around the shared traversal operator:
// node and relationship identity conflicts are rejected during the join, and
// WHERE is evaluated once against the complete joined row.
func (e *StorageExecutor) pipelineApplyChainedMatch(ctx context.Context, rows []pipelineRow, clause string) ([]pipelineRow, bool) {
	if len(rows) == 0 || extractPathAssignmentVariable(clause) != "" {
		return nil, false
	}
	patternBindings := make(map[string]struct{})
	for _, variable := range extractNodeVariables(matchPatternStructure(clause)) {
		patternBindings[variable] = struct{}{}
	}
	for _, variable := range extractRelationshipVariables(matchPatternStructure(clause)) {
		patternBindings[variable] = struct{}{}
	}
	hasGraphBinding := false
	for _, row := range rows {
		for name, value := range row {
			if _, referenced := patternBindings[name]; !referenced {
				continue
			}
			switch value.(type) {
			case *storage.Node, *storage.Edge:
				hasGraphBinding = true
			}
		}
	}
	if !hasGraphBinding {
		return nil, false
	}

	pattern := pipelineClauseBody(clause, "MATCH")
	whereClause := ""
	if whereIndex := topLevelKeywordIndex(pattern, "WHERE"); whereIndex >= 0 {
		whereClause = normalizePipelineWhitespace(pattern[whereIndex+len("WHERE"):])
		pattern = strings.TrimSpace(pattern[:whereIndex])
	}
	// executeChainedMatch runs one pattern part. A MATCH of several parts
	// ((a)-->(x), (b)-->(x)) needs its relationships distinct across the
	// parts, which the MATCH executor below enforces.
	if len(splitTopLevelComma(pattern)) != 1 || !containsRelationshipPattern(pattern) {
		return nil, false
	}
	out := make([]pipelineRow, 0, len(rows))
	for _, row := range rows {
		nodes := make(binding)
		relationships := make(relationshipBinding)
		for name, value := range row {
			switch entity := value.(type) {
			case *storage.Node:
				if entity != nil {
					nodes[name] = entity
				}
			case *storage.Edge:
				if entity != nil {
					relationships[name] = entity
				}
			}
		}
		joinedNodes, joinedRelationships := e.executeChainedMatch(withValueBindings(ctx, row), pattern, []binding{nodes}, []relationshipBinding{relationships})
		for index, nodeBindings := range joinedNodes {
			joined := make(pipelineRow, util.SafePreallocSum(len(row), len(nodeBindings)))
			for name, value := range row {
				joined[name] = value
			}
			for name, node := range nodeBindings {
				joined[name] = node
			}
			if index < len(joinedRelationships) {
				for name, relationship := range joinedRelationships[index] {
					joined[name] = relationship
				}
			}
			if whereClause == "" || e.evaluateWithWhereCondition(ctx, whereClause, map[string]interface{}(joined)) {
				out = append(out, joined)
			}
		}
	}
	return out, true
}

func (e *StorageExecutor) pipelineApplyBoundRelationshipListMatch(ctx context.Context, rows []pipelineRow, clause string) ([]pipelineRow, bool, error) {
	pattern := pipelineClauseBody(clause, "MATCH")
	whereClause := ""
	if whereIndex := topLevelKeywordIndex(pattern, "WHERE"); whereIndex >= 0 {
		whereClause = normalizePipelineWhitespace(pattern[whereIndex+len("WHERE"):])
		pattern = strings.TrimSpace(pattern[:whereIndex])
	}
	open, close := readRelationshipBracket(pattern, 0, requireClosedRelationship)
	if open < 0 || close < 0 {
		return nil, false, nil
	}
	// Admission needs the variable and length, not a type list or normalized
	// traversal. The execution reader checks the complete accepted pattern.
	declaration := relationshipDeclarationHead(pattern[open+1 : close])
	if !declaration.hasLength || declaration.variable == "" {
		return nil, false, nil
	}
	for _, row := range rows {
		if _, exists := row[declaration.variable]; !exists {
			return nil, false, nil
		}
	}
	if hasQuantifiedGroup(pattern) || e.isChainedPattern(normalizeAnonymousTraversalRelationships(pattern)) {
		return nil, false, nil
	}

	store := e.getStorage(ctx)
	out := make([]pipelineRow, 0, len(rows))
	for _, row := range rows {

		relationships, ok := pipelineRelationshipList(row[declaration.variable])
		if !ok || len(relationships) == 0 || !repeatableElements(ctx) && relationshipListReusesEdge(relationships) {
			continue
		}
		rowCtx, prepared, err := e.bindPatternProperties(ctx, pattern, row)
		if err != nil {
			return nil, true, err
		}
		match := e.parseTraversalPattern(rowCtx, prepared)
		if match == nil {
			return nil, true, getExpressionFailure(ctx)
		}

		for _, endpoints := range traceRelationshipList(relationships, match.Relationship.Direction) {
			startID, startValid := endpoints[0].NodeLookupID(store)
			endID, endValid := endpoints[1].NodeLookupID(store)
			if !startValid || !endValid {
				continue
			}
			start, startErr := e.getQueryNode(store, startID)
			end, endErr := e.getQueryNode(store, endID)
			if startErr != nil || endErr != nil || start == nil || end == nil ||
				!e.pipelineNodeMatchesPattern(start, match.StartNode) || !e.pipelineNodeMatchesPattern(end, match.EndNode) {
				continue
			}
			if bound, exists := row[match.StartNode.variable]; exists {
				boundNode, isNode := bound.(*storage.Node)
				if !isNode || boundNode == nil || boundNode.Identity() != start.Identity() {
					continue
				}
			}
			if bound, exists := row[match.EndNode.variable]; exists {
				boundNode, isNode := bound.(*storage.Node)
				if !isNode || boundNode == nil || boundNode.Identity() != end.Identity() {
					continue
				}
			}
			expanded := make(pipelineRow, util.SafePreallocSum(len(row), 2))
			for name, value := range row {
				expanded[name] = value
			}
			if match.StartNode.variable != "" {
				expanded[match.StartNode.variable] = start
			}
			if match.EndNode.variable != "" {
				expanded[match.EndNode.variable] = end
			}
			if whereClause != "" && !e.evaluateMatchWhereCondition(ctx, whereClause, map[string]interface{}(expanded)) {
				continue
			}
			out = append(out, expanded)
		}
	}
	if failure := getExpressionFailure(ctx); failure != nil {
		return nil, true, failure
	}
	return out, true, nil
}

func pipelineRelationshipList(value interface{}) ([]*storage.Edge, bool) {
	switch relationships := value.(type) {
	case []*storage.Edge:
		return relationships, true
	case []interface{}:
		result := make([]*storage.Edge, len(relationships))
		for index, value := range relationships {
			relationship, ok := value.(*storage.Edge)
			if !ok || relationship == nil {
				return nil, false
			}
			result[index] = relationship
		}
		return result, true
	default:
		return nil, false
	}
}

func relationshipListReusesEdge(relationships []*storage.Edge) bool {
	seen := make(map[storage.EntityIdentity]struct{}, len(relationships))
	for _, relationship := range relationships {
		if relationship == nil {
			return true
		}
		if _, exists := seen[relationship.Identity()]; exists {
			return true
		}
		seen[relationship.Identity()] = struct{}{}
	}
	return false
}

func traceRelationshipList(relationships []*storage.Edge, direction string) [][2]storage.EntityIdentity {
	if len(relationships) == 0 {
		return nil
	}
	starts := [][2]storage.EntityIdentity{{relationships[0].StartIdentity(), relationships[0].EndIdentity()}}
	if direction == "incoming" {
		starts[0] = [2]storage.EntityIdentity{relationships[0].EndIdentity(), relationships[0].StartIdentity()}
	} else if direction == "both" && relationships[0].StartIdentity() != relationships[0].EndIdentity() {
		starts = append(starts, [2]storage.EntityIdentity{relationships[0].EndIdentity(), relationships[0].StartIdentity()})
	}
	for _, relationship := range relationships[1:] {
		next := starts[:0]
		for _, endpoints := range starts {
			switch direction {
			case "outgoing":
				if endpoints[1] == relationship.StartIdentity() {
					next = append(next, [2]storage.EntityIdentity{endpoints[0], relationship.EndIdentity()})
				}
			case "incoming":
				if endpoints[1] == relationship.EndIdentity() {
					next = append(next, [2]storage.EntityIdentity{endpoints[0], relationship.StartIdentity()})
				}
			default:
				if endpoints[1] == relationship.StartIdentity() {
					next = append(next, [2]storage.EntityIdentity{endpoints[0], relationship.EndIdentity()})
				}
				if endpoints[1] == relationship.EndIdentity() && relationship.StartIdentity() != relationship.EndIdentity() {
					next = append(next, [2]storage.EntityIdentity{endpoints[0], relationship.StartIdentity()})
				}
			}
		}
		starts = next
	}
	return starts
}

func (e *StorageExecutor) pipelineNodeMatchesPattern(node *storage.Node, pattern nodePatternInfo) bool {
	if !mergeNodeHasLabels(node, pattern.labels) {
		return false
	}
	return e.nodeMatchesProps(node, pattern.properties)
}

// precededByOptionalKeyword reports whether the clause keyword at position in
// cypher follows the keyword OPTIONAL (OPTIONAL MATCH, OPTIONAL CALL), not a
// variable named optional. It doesn't allocate.
func precededByOptionalKeyword(cypher string, position int) bool {
	start := keywordsBefore(cypher, position, "OPTIONAL")
	return start >= 0 && !clauseKeywordUsedAsName(cypher, start, start+len("OPTIONAL"), "OPTIONAL")
}

// keywordsBefore returns the index of words' first word when the words, in
// order, end s before position as whole words (case-insensitive; whitespace
// before position and between them is skipped), or -1: MERGE's ON before
// CREATE, MATCH or SET, OPTIONAL before MATCH, DETACH before DELETE. A name
// ending in a word (reason, season, friends, autocommit) doesn't end with
// it. It doesn't allocate.
func keywordsBefore(s string, position int, words ...string) int {
	end := position
	for w := len(words) - 1; w >= 0; w-- {
		end = len(strings.TrimRightFunc(s[:end], cyphertext.IsWhitespace))
		start := end - len(words[w])
		if start < 0 || !equalFoldASCII(s[start:end], words[w]) || start > 0 && cyphertext.NamePartAt(s, start-1) {
			return -1
		}
		end = start
	}
	return end
}

// pipelineFrameBinding uses the execution frame and its existing identifier
// translation. It does not reconstruct CALL, UNION or wildcard scopes.
func pipelineFrameBinding(ctx context.Context, scope map[string]struct{}, name string) (string, bool) {
	if _, bound := scope[name]; bound {
		return name, true
	}
	if names, _ := ctx.Value(quotedVariablesKey{}).(*quotedVariableNames); names != nil {
		for internal, original := range names.internal {
			if original.name == name {
				if _, bound := scope[internal]; bound {
					return internal, true
				}
			}
		}
	}
	return name, false
}
