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
	"errors"
	"fmt"
	math "github.com/orneryd/nornicdb/pkg/math/libm"
	"reflect"
	"slices"
	"sort"
	"strconv"
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
	kind pipelineClauseKind
	text string
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
func canExecuteAsPipeline(cypher string) ([]pipelineClause, bool) {
	clauses, ok := pipelineClausesFor(cypher)
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
func pipelineClausesFor(cypher string) ([]pipelineClause, bool) {
	return splitPipelineClausesAllowingProcedureCalls(cypher)
}

// splitUnwindBody splits an UNWIND clause's body (after UNWIND) at its
// top-level AS into the list expression and the alias; an AS inside a
// string, list or map belongs to the expression.
func splitUnwindBody(body string) (list, alias string, ok bool) {
	index := topLevelKeywordIndex(body, "AS")
	if index <= 0 {
		return "", "", false
	}
	return strings.TrimSpace(body[:index]), strings.TrimSpace(body[index+len("AS"):]), true
}

// pipelineClauseBody is a clause's text after its keyword, which the
// clause splitter matched in any letter case (Return, rEtUrN).
func pipelineClauseBody(text, keyword string) string {
	text = strings.TrimSpace(text)
	if len(text) >= len(keyword) && strings.EqualFold(text[:len(keyword)], keyword) {
		text = text[len(keyword):]
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
}

const pipelineClauseSplitLimit = 4096

// splitPipelineClauses cuts a statement into its pipeline clauses (see
// parsePipelineClauses); a CALL makes it unsupported.
func splitPipelineClauses(cypher string) ([]pipelineClause, bool) {
	return cachedPipelineClauses(cypher, false)
}

// splitPipelineClausesAllowingProcedureCalls is splitPipelineClauses that also
// accepts top-level procedure calls (CALL proc(args) YIELD …) as clauses of
// kind pipelineClauseCall, for the pipeline executor and call-aware semantic
// validation. splitPipelineClauses rejects statements with procedure calls.
func splitPipelineClausesAllowingProcedureCalls(cypher string) ([]pipelineClause, bool) {
	return cachedPipelineClauses(cypher, true)
}

// cachedPipelineClauses computes the split of a text once, for both
// callers: without procedure calls as clauses, a text with a CALL of its own
// is unsupported, and any other text splits the same way. Callers get their
// own copy of the clause list.
func cachedPipelineClauses(cypher string, procedureCalls bool) ([]pipelineClause, bool) {
	pipelineClauseSplits.RLock()
	split, cached := pipelineClauseSplits.splits[cypher]
	pipelineClauseSplits.RUnlock()
	if !cached {
		split.clauses, split.ok, split.topLevelCall = parsePipelineClauses(cypher)
		pipelineClauseSplits.Lock()
		if len(pipelineClauseSplits.splits) >= pipelineClauseSplitLimit {
			pipelineClauseSplits.splits = make(map[string]pipelineClauseSplit)
		}
		pipelineClauseSplits.splits[cypher] = split
		pipelineClauseSplits.Unlock()
	}
	if split.topLevelCall && !procedureCalls {
		return nil, false
	}
	if split.clauses == nil {
		return nil, split.ok
	}
	return append([]pipelineClause(nil), split.clauses...), split.ok
}

// parsePipelineClauses walks the query from left to right and slices it on
// top-level MATCH/CREATE/WITH/UNWIND/RETURN keywords, with top-level
// procedure calls (CALL proc(args) YIELD …) as clauses. Returns (clauses,
// true) on success. On anything unsupported (e.g. nested MERGE or CALL
// subquery) returns (nil, false) so the caller falls back. topLevelCall
// reports whether the query has a CALL of its own (cachedPipelineClauses).
func parsePipelineClauses(cypher string) (clauses []pipelineClause, ok bool, topLevelCall bool) {
	type kw struct {
		name string
		kind pipelineClauseKind
	}
	// Order matters for multi-word lookups but we only care about single-word
	// keywords here; OPTIONAL MATCH and MERGE kick us out via detection below.
	keywords := []kw{
		{"OPTIONAL MATCH", pipelineClauseOptionalMatch},
		{"MATCH", pipelineClauseMatch},
		{"CREATE", pipelineClauseCreate},
		{"MERGE", pipelineClauseMerge},
		{"DETACH DELETE", pipelineClauseDelete},
		{"DELETE", pipelineClauseDelete},
		{"SET", pipelineClauseSet},
		{"REMOVE", pipelineClauseRemove},
		{"WITH", pipelineClauseWith},
		{"UNWIND", pipelineClauseUnwind},
		{"FOR", pipelineClauseUnwind},
		{"LET", pipelineClauseLet},
		{"FILTER", pipelineClauseFilter},
		{"FOREACH", pipelineClauseForeach},
		{"RETURN", pipelineClauseReturn},
	}
	// Clauses we don't yet model as their own kind force a fallback: a CALL
	// clause of the statement itself. A CALL inside a subquery expression's
	// body belongs to that body, which the subquery evaluator runs. Anything
	// else — including $param references and arbitrary WHERE on bindings —
	// is handled by the per-clause appliers below, which substitute params
	// from context and respect node bindings supplied by the caller.
	if topLevelKeywordIndex(cypher, "CALL") >= 0 {
		topLevelCall = true
		keywords = append(keywords, kw{"OPTIONAL CALL", pipelineClauseCall}, kw{"CALL", pipelineClauseCall})
	}

	// Collect boundary positions for each supported keyword.
	var boundaries []pipelineBoundary
	for _, k := range keywords {
		for _, p := range findAllTopLevelPipelineKeywordPositions(cypher, k.name) {
			// OPTIONAL CALL and OPTIONAL MATCH are one clause, found by their
			// own keyword, unless optional is a variable (WITH x, optional
			// MATCH (n), #894).
			if k.name == "CALL" && precededByOptionalKeyword(cypher, p) {
				continue
			}
			if k.kind == pipelineClauseMatch {
				if precededByOptionalKeyword(cypher, p) {
					continue
				}
				preceding := strings.TrimSpace(upperASCII(cypher[:p]))
				if strings.HasSuffix(preceding, "ON") {
					continue
				}
			}
			if k.kind == pipelineClauseSet {
				preceding := strings.TrimSpace(upperASCII(cypher[:p]))
				if strings.HasSuffix(preceding, "ON CREATE") || strings.HasSuffix(preceding, "ON MATCH") {
					continue
				}
			}
			if k.kind == pipelineClauseReturn {
				preceding := strings.TrimRight(cypher[:p], " \t\n\r")
				if strings.HasSuffix(preceding, ":") {
					continue
				}
			}
			if k.name == "DELETE" {
				preceding := strings.TrimSpace(upperASCII(cypher[:p]))
				if strings.HasSuffix(preceding, "DETACH") {
					continue
				}
			}
			// FOR is only an iteration clause in the shared grammar. In
			// schema and alias DDL it is a specifier (CREATE INDEX … FOR
			// (n:Label), CREATE ALIAS … FOR DATABASE …): not a clause.
			if k.name == "FOR" {
				if _, _, iteration := parsePipelineIteration(strings.TrimSpace(cypher[p:])); !iteration {
					continue
				}
			}
			if k.kind == pipelineClauseCreate {
				preceding := strings.TrimSpace(upperASCII(cypher[:p]))
				if strings.HasSuffix(preceding, "ON") {
					continue
				}
			}
			boundaries = append(boundaries, pipelineBoundary{pos: p, kind: k.kind, name: k.name})
		}
	}
	if len(boundaries) == 0 {
		return nil, false, topLevelCall
	}
	// Sort ascending by pos.
	sortBoundariesByPos(boundaries)

	// Cut the query on each boundary. The first clause must begin at the
	// first boundary (i.e. the query should begin with one of these keywords
	// after trimming).
	trimmedLeft := len(cypher) - len(strings.TrimLeft(cypher, " \t\n\r"))
	if boundaries[0].pos != trimmedLeft {
		return nil, false, topLevelCall
	}

	var out []pipelineClause
	for i, b := range boundaries {
		end := len(cypher)
		if i+1 < len(boundaries) {
			end = boundaries[i+1].pos
		}
		text := strings.TrimSpace(cypher[b.pos:end])
		if text == "" {
			continue
		}
		kind := b.kind
		optional := b.name == "OPTIONAL CALL"
		if optional {
			text = strings.TrimSpace(text[len("OPTIONAL"):])
		}
		if b.name == "CALL" || optional {
			if startsWithCallSubquery(text) {
				kind = pipelineClauseCallSubquery
			} else if !pipelineProcedureCallsAreClauses(text) {
				return nil, false, topLevelCall
			}
		}
		out = append(out, pipelineClause{kind: kind, text: text, optional: optional})
	}
	return out, true, topLevelCall
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
		switch character {
		case '\'', '"', '`':
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
			(i == 0 || !isAlphaNumericByte(query[i-1])) &&
			(i+len(keyword) == len(query) || !isAlphaNumericByte(query[i+len(keyword)])) {
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

type pipelineBoundary struct {
	pos  int
	kind pipelineClauseKind
	name string
}

func sortBoundariesByPos(bs []pipelineBoundary) {
	// Insertion sort — boundary count is small (usually < 20).
	for i := 1; i < len(bs); i++ {
		j := i
		for j > 0 && bs[j-1].pos > bs[j].pos {
			bs[j-1], bs[j] = bs[j], bs[j-1]
			j--
		}
	}
}

// executePipeline walks the clauses, threading the binding rows through each
// step. Its outcome distinguishes a safe decline from a parse rejection or a
// runtime failure, so callers only retry the NotApplicable state.
func (e *StorageExecutor) executePipeline(ctx context.Context, cypher string) (outcome pipelineDispatchOutcome) {
	statement := cypher
	ctx = withExpressionFailureSlot(ctx)
	defer func() {
		if recorded := getExpressionFailure(ctx); recorded != nil && outcome.err == nil {
			outcome = newPipelineDispatchOutcome(nil, true, recorded)
		}
	}()
	hasCallSubquery := firstTopLevelCallSubquery(cypher) >= 0
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
	clauses, ok := canExecuteAsPipeline(cypher)
	if !ok {
		return newPipelineDispatchOutcome(nil, false, nil)
	}
	originalClauses := clauses
	if pipelineHasClauseKind(clauses, pipelineClauseSet) {
		cypher = normalizePipelineWhitespace(cypher)
		clauses, ok = canExecuteAsPipeline(cypher)
		if !ok {
			return newPipelineDispatchOutcome(nil, false, nil)
		}
	}

	// Substitute $param placeholders up-front — this is the same pass the
	// other top-level handlers perform. After this step the clause texts are
	// self-contained and our per-clause appliers only have to worry about
	// pipeline-bound names (from WITH/UNWIND/MATCH), not caller parameters.
	params := getParamsFromContext(ctx)
	if !hasCallSubquery {
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
	if params != nil {
		cypher = e.substituteParams(cypher, params)
		clauses, ok = canExecuteAsPipeline(cypher)
		if !ok {
			return newPipelineDispatchOutcome(nil, false, nil)
		}
		for index := range clauses {
			if clauses[index].kind == pipelineClauseUnwind && index < len(originalClauses) {
				clauses[index].text = originalClauses[index].text
			}
		}
	}
	if result, handled, err := e.tryExecutePipelineOptionalMatchPlan(ctx, cypher, clauses); handled || err != nil {
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

	bindResultStream(ctx, statement, clauses)
	result, handled, err := e.runPipelineClauses(ctx, rows, scope, clauses, originalClauses)
	return newPipelineDispatchOutcome(result, handled, err)
}

// runPipelineClauses threads the binding rows through the clauses, starting
// from rows (one empty row for a whole statement, or the rows a procedure
// yielded for the clauses after CALL … YIELD). originalClauses are the clause
// texts before parameter substitution, which name RETURN columns. It returns
// (result, true, nil), (nil, false, nil) when a clause shape is unsupported
// before anything was written, or (nil, true, err).
func (e *StorageExecutor) runPipelineClauses(ctx context.Context, rows []pipelineRow, scope map[string]struct{}, clauses, originalClauses []pipelineClause) (*ExecuteResult, bool, error) {
	return e.runPipelineClauseRows(ctx, rows, scope, clauses, originalClauses, nil)
}

type pipelineRowOutput struct {
	rows  []pipelineRow
	scope map[string]struct{}
	wrote bool
}

func (e *StorageExecutor) runPipelineClauseRows(ctx context.Context, rows []pipelineRow, scope map[string]struct{}, clauses, originalClauses []pipelineClause, output *pipelineRowOutput) (*ExecuteResult, bool, error) {
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
		ctx = withDeletedEntities(ctx)
	}
	independentCreate, _ := ctx.Value(pipelineIndependentCreateBatchKey{}).(bool)
	if len(rows) > 1 && !independentCreate {
		prefix, writes := 0, false
		for _, clause := range clauses {
			if clause.kind == pipelineClauseReturn {
				break
			}
			if clause.kind == pipelineClauseWith {
				if _, local := parsePipelineRowWith(clause.text); !local {
					break
				}
			}
			switch clause.kind {
			case pipelineClauseCreate, pipelineClauseMerge, pipelineClauseSet, pipelineClauseRemove, pipelineClauseDelete, pipelineClauseForeach:
				writes = true
			case pipelineClauseCall:
				if procedure, found := globalProcedureRegistry.Get(extractProcedureName(clause.text)); found && procedure.Spec.Mode == ProcedureModeWrite {
					writes = true
				}
			}
			prefix++
		}
		if writes {
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
	for idx := 0; idx < len(clauses); idx++ {
		clause := clauses[idx]
		if source != nil && clause.kind != pipelineClauseMatch && clause.kind != pipelineClauseWith &&
			clause.kind != pipelineClauseLet && clause.kind != pipelineClauseFilter &&
			!(clause.kind == pipelineClauseCreate && independentCreate) &&
			!(clause.kind == pipelineClauseReturn && (pipelineClauseAggregates(clause) || streamsReturn(ctx, clauses, idx))) {
			var completed bool
			rows, completed = materializePipelineSource(source)
			source = nil
			if !completed {
				return pipelineDecline(ctx, wrote, clause.text)
			}
		}
		if deleted := deletedEntitiesOf(ctx); !deleted.empty() {
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
			hint := e.pipelineMatchHint(clauses[idx+1:])
			hint.streamScan = source == nil && len(rows) == 1 && len(scope) == 0 && streamsScan(ctx, clauses)
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
				addPipelinePatternBindings(e, scope, clause.text, "MATCH")
				continue
			}
			if source != nil {
				var completed bool
				rows, completed = materializePipelineSource(source)
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
			rows = newRows
			addPipelinePatternBindings(e, scope, clause.text, "MATCH")
		case pipelineClauseOptionalMatch:
			newRows, err := e.pipelineApplyOptionalMatch(ctx, rows, clause.text)
			if err != nil {
				return nil, true, err
			}
			rows = newRows
			addPipelinePatternBindings(e, scope, clause.text, "OPTIONAL MATCH")
		case pipelineClauseCreate:
			end := idx + 1
			for end < len(clauses) && clauses[end].kind == pipelineClauseCreate {
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
				addPipelinePatternBindings(e, scope, createClause.text, "CREATE")
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
			addPipelinePatternBindings(e, scope, clause.text, "MERGE")
			if stats != nil {
				addQueryStats(result.Stats, stats)
			}
			wrote = true
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
			stats, ok, err := e.pipelineApplySet(ctx, rows, assignment)
			if err != nil {
				return nil, true, err
			}
			if !ok {
				return pipelineDecline(ctx, wrote, clause.text)
			}
			addQueryStats(result.Stats, stats)
			wrote = true
		case pipelineClauseRemove:
			if err := e.pipelineApplyRemove(ctx, rows, clause.text, result); err != nil {
				return nil, true, err
			}
			wrote = true
		case pipelineClauseWith:
			if source != nil {
				if windowed, supported := e.pipelineWithWindowSource(ctx, source, rows, clause.text); supported {
					source = windowed
					scope = pipelineProjectionScope(scope, clause.text)
					continue
				}
				if plan, local := parsePipelineRowWith(clause.text); local {
					source = e.pipelineWithRowSource(ctx, source, plan)
					scope = pipelineProjectionScope(scope, clause.text)
					continue
				}
				if !pipelineClauseAggregates(clause) {
					var completed bool
					rows, completed = materializePipelineSource(source)
					source = nil
					if !completed {
						return pipelineDecline(ctx, wrote, clause.text)
					}
				}
			}
			if err := e.validatePipelineWithRows(rows, clause.text); err != nil {
				return nil, true, err
			}
			// The rows just validated are the clause's input when no
			// stream feeds it.
			rowsValidated := source == nil
			if source == nil {
				source = pipelineRowsSource(rows)
			}
			newRows, ok := e.pipelineApplyWithSource(ctx, rows, clause.text, source, rowsValidated)
			source = nil
			if !ok {
				return pipelineDecline(ctx, wrote, clause.text)
			}
			rows = newRows
			scope = pipelineProjectionScope(scope, clause.text)
		case pipelineClauseUnwind:
			if err := e.validatePipelineRangeArguments(rows, clause.text, "UNWIND"); err != nil {
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
				scope = pipelineProjectionScope(scope, clauses[idx+offset].text)
			}
			idx += consumed
			if idx+1 < len(clauses) && (pipelineClauseAggregates(clauses[idx+1]) || streamsReturn(ctx, clauses, idx+1)) {
				source = unwound
				rows = nil
				continue
			}
			rows, ok = materializePipelineSource(unwound)
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
					rowsOut, names, handled, err := e.pipelineApplyProcedureCall(ctx, row, clause.text)
					yielded = names
					return rowsOut, names, handled, err
				}, &ok)
			} else {
				newRows, yielded, ok, err = e.pipelineApplyProcedureCall(ctx, rows, clause.text)
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
			if procedure, found := globalProcedureRegistry.Get(extractProcedureName(clause.text)); found && procedure.Spec.Mode == ProcedureModeWrite {
				wrote = true
			}
		case pipelineClauseCallSubquery:
			callMetadata := &pipelineCallMetadata{scope: scope}
			var newRows []pipelineRow
			var stats *QueryStats
			ok := true
			var err error
			if clause.optional {
				body, _, _, _ := e.parseCallSubquery(clause.text)
				columns := e.inferTopLevelReturnColumns(body)
				stats = &QueryStats{}
				newRows, err = pipelineApplyOptionally(rows, func(row []pipelineRow) ([]pipelineRow, []string, bool, error) {
					rowsOut, rowStats, handled, err := e.pipelineApplyCallSubqueryWithMetadata(ctx, row, clause.text, callMetadata)
					addQueryStats(stats, rowStats)
					return rowsOut, columns, handled, err
				}, &ok)
				callMetadata.columns = columns
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
			for _, row := range rows {
				for name := range row {
					if !strings.HasPrefix(name, "$") {
						scope[name] = struct{}{}
					}
				}
			}
			if callSubqueryQueryIsWrite(clause.text) || strings.Contains(upperASCII(clause.text), "IN TRANSACTIONS") {
				wrote = true
			}
		case pipelineClauseForeach:
			stats, err := e.pipelineApplyForeach(ctx, rows, clause.text)
			if err != nil {
				return nil, true, err
			}
			addQueryStats(result.Stats, stats)
			wrote = true
		case pipelineClauseReturn:
			if streamsReturn(ctx, clauses, idx) {
				columns, streamedRows, handled, err := e.pipelineStreamReturn(ctx, boundResultStream(ctx, &clauses[idx]), rows, source, clauses, originalClauses, idx, scope, wrote)
				if !handled || err != nil {
					return nil, handled, err
				}
				result.Columns, result.Rows = columns, streamedRows
				return result, true, nil
			}
			if err := e.validatePipelinePercentileArguments(rows, clause.text, "RETURN"); err != nil {
				return nil, true, err
			}
			if err := e.validatePipelineProjectionValues(rows, clause.text, "RETURN"); err != nil {
				return nil, true, err
			}
			// The rows just validated are the clause's input when no
			// stream feeds it.
			rowsValidated := source == nil
			if source == nil {
				source = pipelineRowsSource(rows)
			}
			plan := returnProjectionPlanFor(clause.text)
			if plan.star && len(rows) == 0 {
				plan = plan.withStarExpanded(pipelineScopeColumns(scope))
			}
			final, ok := e.pipelineApplyReturnPlan(ctx, rows, plan, source, rowsValidated)
			source = nil
			if !ok {
				if failure := getExpressionFailure(ctx); failure != nil && final != nil {
					pipelineNameReturnColumns(final, clause.text, pipelineOriginalReturnText(originalClauses, idx), scope)
					return final, true, failure
				}
				return pipelineDecline(ctx, wrote, clause.text)
			}
			pipelineNameReturnColumns(final, clause.text, pipelineOriginalReturnText(originalClauses, idx), scope)
			result.Columns = final.Columns
			result.Rows = final.Rows
			// RETURN is always last.
			return result, true, nil
		}
		_ = idx
	}
	if source != nil {
		var completed bool
		rows, completed = materializePipelineSource(source)
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
		body, _, _, _ := e.parseCallSubquery(clauses[len(clauses)-1].text)
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

// tryExecutePipelineSimpleRelationshipCountPlan is the fused physical form of
// the pipeline's MATCH -> count reduction for relationship patterns (issue
// #638). Without it, the row pipeline binds the relationship variable for
// every match and counts the materialized rows — so
//
//	MATCH ()-[r:T]->() RETURN count(r)
//
// costs O(store size) instead of an O(1) counter read, even though the
// traversal-layer fast path exists. It delegates to tryFastRelationshipCount,
// the single implementation of the typed/untyped count semantics, so no
// second count algorithm exists in the codebase.
func (e *StorageExecutor) tryExecutePipelineSimpleRelationshipCountPlan(ctx context.Context, clauses []pipelineClause, params map[string]interface{}) (*ExecuteResult, bool, error) {
	if len(clauses) != 2 || clauses[0].kind != pipelineClauseMatch || clauses[1].kind != pipelineClauseReturn {
		return nil, false, nil
	}
	matchBody := strings.TrimSpace(clauses[0].text[len("MATCH"):])
	if topLevelKeywordIndex(matchBody, "WHERE") >= 0 {
		return nil, false, nil
	}
	if len(splitTopLevelComma(matchBody)) != 1 {
		return nil, false, nil
	}
	matches := e.parseTraversalPattern(ctx, matchBody)
	if matches == nil || matches.IsChained {
		return nil, false, nil
	}
	if matches.Relationship.MinHops != 1 || matches.Relationship.MaxHops != 1 {
		return nil, false, nil
	}
	// Endpoint shapes the O(1) counters can answer: anonymous endpoints
	// (per-type tier) or at most one endpoint with a single label
	// (positional (label, type) tier, like Neo4j's count store).
	// tryFastRelationshipCount declines anything else (properties, both
	// labeled, direction both), and the general path handles it.
	if len(matches.StartNode.labels) > 1 || len(matches.EndNode.labels) > 1 ||
		len(matches.Relationship.Properties) > 0 {
		return nil, false, nil
	}
	items := e.parseReturnItems(strings.TrimSpace(clauses[1].text[len("RETURN"):]))
	if len(items) > 1 && items[0].expr == "*" {
		// RETURN *, items: the general projection writes the * out (#883).
		return nil, false, nil
	}
	if len(items) != 1 {
		return nil, false, nil
	}
	// Counters can't reflect decay filtering or temporal viewports; those
	// stores keep the materializing general path (same guard as the node
	// label-count fast path).
	if storageHasDecayFiltering(e.getStorage(ctx)) {
		return nil, false, nil
	}
	if viewport, ok := TemporalViewportFromContext(ctx); ok && viewport.Enabled() {
		return nil, false, nil
	}
	count, ok, err := e.tryFastRelationshipCount(matches, items[0])
	if err != nil {
		return nil, true, err
	}
	if !ok {
		return nil, false, nil
	}
	column := items[0].expr
	if items[0].alias != "" {
		column = items[0].alias
	}
	return &ExecuteResult{Columns: []string{column}, Rows: [][]interface{}{{count}}, Stats: &QueryStats{}}, true, nil
}

// tryExecutePipelineSimpleNodeReadPlan applies cardinality and property-index
// operators before row materialization for a single node MATCH. The result is
// still projected by the pipeline, so indexed and scanned inputs share the
// same expression semantics.
func (e *StorageExecutor) tryExecutePipelineSimpleNodeReadPlan(ctx context.Context, clauses []pipelineClause, params map[string]interface{}) (*ExecuteResult, bool, error) {
	if len(clauses) != 2 || clauses[0].kind != pipelineClauseMatch || clauses[1].kind != pipelineClauseReturn {
		return nil, false, nil
	}
	matchBody := strings.TrimSpace(clauses[0].text[len("MATCH"):])
	whereClause := ""
	if whereIndex := topLevelKeywordIndex(matchBody, "WHERE"); whereIndex >= 0 {
		whereClause = strings.TrimSpace(matchBody[whereIndex+len("WHERE"):])
		matchBody = strings.TrimSpace(matchBody[:whereIndex])
	}
	if strings.Contains(matchBody, "-[") || strings.Contains(matchBody, "]-") || len(e.splitNodePatterns(matchBody)) != 1 {
		return nil, false, nil
	}
	nodePattern := e.parseNodePattern(ctx, matchBody)
	if nodePattern.variable == "" {
		return nil, false, nil
	}

	items := e.parseReturnItems(strings.TrimSpace(clauses[1].text[len("RETURN"):]))
	if len(items) > 1 && items[0].expr == "*" {
		// RETURN *, items: the general projection writes the * out (#883).
		return nil, false, nil
	}
	hint := e.pipelineMatchHint(clauses[1:])
	boundedSimpleProjection := whereClause == "" && hint.earlyLimit > 0 && pipelineSimpleNodeProjections(items, nodePattern.variable)
	countColumn, filteredCount := pipelineSingleNodeCountProjection(items, nodePattern.variable)
	if whereClause == "" && len(items) == 1 && len(nodePattern.properties) == 0 && isAggregateFuncName(items[0].expr, "count") {
		inner := strings.TrimSpace(extractFuncInner(items[0].expr))
		if inner == "*" || inner == nodePattern.variable {
			store := e.getStorage(ctx)
			var count int64
			var err error
			if len(nodePattern.labels) == 1 && !storageHasDecayFiltering(store) {
				if viewport, ok := TemporalViewportFromContext(ctx); !ok || !viewport.Enabled() {
					if counter, ok := store.(interface{ NodeCountByLabel(string) (int64, error) }); ok {
						count, err = counter.NodeCountByLabel(nodePattern.labels[0])
						if err != nil {
							return nil, true, localizedError(localization.CypherMatchingStorageFailed(err), err)
						}
						column := items[0].expr
						if items[0].alias != "" {
							column = items[0].alias
						}
						return &ExecuteResult{Columns: []string{column}, Rows: [][]interface{}{{count}}, Stats: &QueryStats{}}, true, nil
					}
				}
			}
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
		return projectPipelineSimpleNodeRead(candidates, items, nodePattern.variable), true, nil
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
	if len(items) != 1 || variable == "" || !isAggregateFuncName(items[0].expr, "count") {
		return "", false
	}
	inner := strings.TrimSpace(extractFuncInner(items[0].expr))
	if inner != "*" && inner != variable {
		return "", false
	}
	column := items[0].expr
	if items[0].alias != "" {
		column = items[0].alias
	}
	return column, true
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
	err = reader.StreamNodesByLabelProjected(nodePattern.labels[0], projectedProperties, func(node *storage.Node) error {
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
		result.Columns[index] = item.expr
		if item.alias != "" {
			result.Columns[index] = item.alias
		}
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

// tryExecutePipelineOptionalMatchPlan selects the optimized physical operator
// for a read-only MATCH followed by one or more OPTIONAL MATCH clauses. The
// query still enters through the row pipeline; clause count never changes the
// logical handler. The traversal operator performs the same N-ary left-outer
// join while avoiding repeated row materialization for graph-only queries.
func (e *StorageExecutor) tryExecutePipelineOptionalMatchPlan(ctx context.Context, cypher string, clauses []pipelineClause) (*ExecuteResult, bool, error) {
	if len(clauses) < 3 || clauses[0].kind != pipelineClauseMatch || clauses[len(clauses)-1].kind != pipelineClauseReturn {
		return nil, false, nil
	}
	seenOptional := false
	for index, clause := range clauses {
		switch clause.kind {
		case pipelineClauseMatch:
			if seenOptional {
				return nil, false, nil
			}
		case pipelineClauseOptionalMatch:
			seenOptional = true
		case pipelineClauseReturn:
			if index != len(clauses)-1 {
				return nil, false, nil
			}
		default:
			return nil, false, nil
		}
	}
	if !seenOptional || indexASCIIFold(cypher, "shortestpath") >= 0 {
		return nil, false, nil
	}

	optionalIndex := findMultiWordKeywordIndex(cypher, "OPTIONAL", "MATCH")
	returnIndex := topLevelKeywordIndex(cypher, "RETURN")
	if optionalIndex <= len("MATCH") || returnIndex <= optionalIndex {
		return nil, false, nil
	}
	initialSection := strings.TrimSpace(cypher[len("MATCH"):optionalIndex])
	optionalSection := strings.TrimSpace(cypher[optionalIndex+len("OPTIONAL MATCH") : returnIndex])
	restOfQuery := strings.TrimSpace(cypher[returnIndex:])
	if initialSection == "" || optionalSection == "" {
		return nil, false, nil
	}

	result, err := e.executeTraversalSeededOptionalMatch(ctx, initialSection, optionalSection, restOfQuery)
	return result, true, err
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
	if !strings.ContainsAny(query, "\t\n\r") {
		return strings.TrimSpace(query)
	}
	var normalized strings.Builder
	normalized.Grow(len(query))
	spacePending := false
	for index := 0; index < len(query); index++ {
		character := query[index]
		if character == '\'' || character == '"' || character == '`' {
			if spacePending && normalized.Len() > 0 {
				normalized.WriteByte(' ')
			}
			spacePending = false
			end := skipCypherQuotedText(query, index, character)
			normalized.WriteString(query[index:end])
			index = end - 1
			continue
		}
		if isWhitespace(character) {
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

// pipelineApplyDelete collects every entity target before validation and
// mutation, preserving statement atomicity while retaining input rows for
// subsequent WITH and RETURN clauses.
func (e *StorageExecutor) pipelineApplyDelete(ctx context.Context, rows []pipelineRow, scope map[string]struct{}, clause string) (*QueryStats, bool, error) {
	targets, detach, ok := deleteClauseTargets(clause)
	if !ok || len(targets) == 0 {
		return nil, false, nil
	}
	for _, expression := range targets {
		if hasTopLevelDeleteLabelQualifier(expression) {
			return nil, true, newSemanticError(
				"Neo.ClientError.Statement.SyntaxError",
				"InvalidDelete",
				"DELETE accepts nodes, relationships, and paths, not labels or relationship types",
			)
		}
		if root := deleteExpressionRootIdentifier(expression); root != "" {
			if _, bound := scope[root]; !bound {
				return nil, true, deleteUndefinedVariableError(expression)
			}
		} else if value, evaluated, err := e.evaluateRowValue(strings.TrimSpace(expression), pipelineRow{}); err != nil {
			return nil, true, err
		} else if evaluated && !isDeleteTargetValue(value) {
			return nil, true, newSemanticError(
				"Neo.ClientError.Statement.SyntaxError",
				"InvalidArgumentType",
				fmt.Sprintf("DELETE expression %q does not evaluate to a node, relationship, or path", strings.TrimSpace(expression)),
			)
		}
	}
	projected := &ExecuteResult{Columns: targets, Rows: make([][]interface{}, 0, len(rows))}
	for _, row := range rows {
		if err := ctx.Err(); err != nil {
			return nil, true, err
		}
		values := make([]interface{}, 0, len(targets))
		for _, expression := range targets {
			value, ok, err := e.evaluateRowValue(strings.TrimSpace(expression), row)
			if err != nil {
				return nil, true, err
			}
			if !ok {
				return nil, true, deleteUndefinedVariableError(expression)
			}
			if !isDeleteTargetValue(value) {
				return nil, true, newSemanticError(
					"Neo.ClientError.Statement.SyntaxError",
					"InvalidArgumentType",
					fmt.Sprintf("DELETE expression %q does not evaluate to a node, relationship, or path", strings.TrimSpace(expression)),
				)
			}
			values = append(values, value)
		}
		projected.Rows = append(projected.Rows, values)
	}
	nodeIDs, edgeIDs := collectDeleteMutationTargets(projected)
	store := e.getStorage(ctx)
	// A node relationships still connect is deleted for now and checked at
	// commit, when the transaction can hold it (connectedNodeDeletes); a
	// store without transactions checks here.
	var connected map[storage.NodeID]struct{}
	if !detach {
		if _, deferred := store.(storage.ConnectedNodeDeleter); deferred {
			var err error
			if connected, err = connectedDeleteTargets(store, nodeIDs, edgeIDs); err != nil {
				return nil, true, err
			}
		} else if err := validateNoResidualRelationships(store, nodeIDs, edgeIDs); err != nil {
			return nil, true, err
		}
	}

	// Rows are deleted as they stream, so an entity an earlier row of the
	// statement deleted reaches storage again: it is already gone
	// (storage.ErrNotFound) and counts once, as in Neo4j (#827).
	deletedEdges := make(map[storage.EdgeID]struct{}, len(edgeIDs))
	for _, edgeID := range edgeIDs {
		if err := store.DeleteEdge(edgeID); err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				continue
			}
			return nil, true, err
		}
		deletedEdges[edgeID] = struct{}{}
	}
	if detach {
		for _, nodeID := range nodeIDs {
			incident, err := undirectedIncidentEdges(store, nodeID)
			if err != nil {
				return nil, true, err
			}
			for _, edge := range incident {
				deletedEdges[edge.ID] = struct{}{}
			}
		}
	}
	stats := &QueryStats{RelationshipsDeleted: len(deletedEdges)}
	for _, nodeID := range nodeIDs {
		deleteNode := store.DeleteNode
		if _, stillConnected := connected[nodeID]; stillConnected {
			deleteNode = store.(storage.ConnectedNodeDeleter).DeleteConnectedNode
		}
		if err := deleteNode(nodeID); err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				continue
			}
			return nil, true, err
		}
		stats.NodesDeleted++
		e.removeNodeFromSearch(string(nodeID))
	}
	deleted := deletedEntitiesOf(ctx)
	if deleted == nil {
		deleted = &deletedEntities{}
	}
	deleted.add(nodeIDs, deletedEdges)
	e.replaceDeletedEntityViews(rows, deleted)
	return stats, true, nil
}

func (e *StorageExecutor) pipelineApplyRemove(ctx context.Context, rows []pipelineRow, clause string, result *ExecuteResult) error {
	body := strings.TrimSpace(clause[len("REMOVE"):])
	store := e.getStorage(ctx)
	for _, bindings := range rows {
		if err := ctx.Err(); err != nil {
			return err
		}
		columns := make([]string, 0, len(bindings))
		row := make([]interface{}, 0, len(bindings))
		for name, value := range bindings {
			columns = append(columns, name)
			row = append(row, value)
		}
		matched := &ExecuteResult{Columns: columns, Rows: [][]interface{}{row}}
		if err := e.applyRemoveToMatchedRows(store, matched, body, result); err != nil {
			return err
		}
	}
	return nil
}

// pipelineApplySet mutates entities already bound in each pipeline row. Scalar
// and map bindings are attached as typed context values so assignments such as
// SET target = row retain their original Go/Cypher types.
func (e *StorageExecutor) pipelineApplySet(ctx context.Context, rows []pipelineRow, clause string) (*QueryStats, bool, error) {
	// body may chain SET clauses (SET a SET b); the applicators keep their
	// boundaries, which properties_set depends on (setWrites).
	body := strings.TrimSpace(clause[len("SET"):])
	assignments := splitSetAssignments(collapseChainedSetClauses(body))
	if body == "" || len(assignments) == 0 {
		return nil, false, nil
	}

	store := e.getStorage(ctx)
	stats := &QueryStats{}
	if err := validatePipelineSetAssignments(assignments); err != nil {
		return nil, true, err
	}
	simpleTarget, simpleProperty, simpleExpression, simplePropertyAssignment := pipelineSimplePropertyAssignment(assignments)
	targets := pipelineSetTargetVariables(assignments)
	var runBuffer [4]setRun
	runs := appendSetClauseRuns(runBuffer[:0], body, assignments)
	var stateBuffer [4]setEntityState
	wrapSetError := func(state setEntityState, err error) error {
		return fmt.Errorf("SET %s: %w", pipelineSetOperation(state.variable, assignments), err)
	}
	if len(targets) == 0 {
		return nil, false, nil
	}
	for _, row := range rows {
		if err := ctx.Err(); err != nil {
			return nil, true, err
		}
		for _, target := range targets {
			value, bound := row[target]
			if !bound || value == nil {
				continue
			}
			switch value.(type) {
			case *storage.Node, *storage.Edge:
			default:
				return nil, true, newSemanticError("Neo.ClientError.Statement.TypeError", "InvalidType", fmt.Sprintf("Type mismatch: expected Node or Relationship but was %s", cypherTypeName(value)))
			}
		}
	}
	for _, row := range rows {
		if err := ctx.Err(); err != nil {
			return nil, true, err
		}
		nodes := make(map[string]*storage.Node)
		evalNodes := nodes
		rels := make(map[string]*storage.Edge)
		params := make(map[string]interface{})
		for name, value := range getParamsFromContext(ctx) {
			params[name] = value
		}
		var values map[string]interface{}
		for name, value := range row {
			switch entity := value.(type) {
			case *storage.Node:
				nodes[name] = entity
			case *storage.Edge:
				rels[name] = entity
			default:
				// Row values (UNWIND / WITH maps, lists, scalars) are variables in
				// the value scope; they also stay reachable as parameters for
				// resolveContextPathRef.
				params[name] = value
				if values == nil {
					values = valueBindingsLayer(ctx, len(row))
				}
				values[name] = value
			}
		}
		rowCtx := withParams(ctx, params)
		if values != nil {
			rowCtx = withValueBindings(rowCtx, values)
		}
		if simplePropertyAssignment {
			if node := nodes[simpleTarget]; node != nil {
				value, err := e.setPropertyValue(rowCtx, simpleExpression, evalNodes, rels)
				if err != nil {
					return nil, true, err
				}
				before := cloneStringAnyMap(node.Properties)
				written := simplePropertyWrites(node.Properties, simpleProperty, value)
				setNodeProperty(node, simpleProperty, value)
				if err := e.persistSetEntities(store, []setEntityState{{variable: simpleTarget, node: node, properties: before, labels: node.Labels, written: written}}, stats, wrapSetError); err != nil {
					return nil, true, err
				}
				continue
			}
			if relationship := rels[simpleTarget]; relationship != nil {
				value, err := e.setPropertyValue(rowCtx, simpleExpression, evalNodes, rels)
				if err != nil {
					return nil, true, err
				}
				before := cloneStringAnyMap(relationship.Properties)
				written := simplePropertyWrites(relationship.Properties, simpleProperty, value)
				setRelationshipProperty(relationship, simpleProperty, value)
				if err := e.persistSetEntities(store, []setEntityState{{variable: simpleTarget, relationship: relationship, properties: before, written: written}}, stats, wrapSetError); err != nil {
					return nil, true, err
				}
				continue
			}
		}
		states, handled, err := e.applySetRuns(rowCtx, runs, evalNodes, rels, func(variable string) bool {
			_, bound := row[variable]
			return bound
		}, stateBuffer[:0])
		if !handled || err != nil {
			return nil, handled, err
		}
		if err := e.persistSetEntities(store, states, stats, wrapSetError); err != nil {
			return nil, true, err
		}
	}
	return stats, true, nil
}

// pipelineSimplePropertyAssignment recognizes the single x.p = <expr> SET
// (the common case) so pipelineApplySet can skip re-splitting per row.
func pipelineSimplePropertyAssignment(assignments []string) (target, property, expression string, ok bool) {
	if len(assignments) != 1 {
		return "", "", "", false
	}
	target, property, operator, expression := splitSetAssignment(assignments[0])
	if operator != "=" || property == "" || expression == "" {
		return "", "", "", false
	}
	return target, property, expression, true
}

// validatePipelineSetAssignments statically checks SET assignment shapes for
// every route (it runs from validateSetClauseScope before execution): x = v,
// x.p = v, x += map and x:L1:L2 with a bound-identifier target, a non-empty
// right-hand side, a parseable inline map for += and a valid label chain
// (setLabelChain). The source type of x = and x += is checked after the
// variables (setSourceLiteralTypeError, validateSetClauseScope). Forms are split by
// splitSetAssignment, the splitter the applicators use.
func validatePipelineSetAssignments(assignments []string) error {
	for _, raw := range assignments {
		assignment := strings.TrimSpace(raw)
		if assignment == "" {
			return localizedError(localization.CypherMutationsSetAssignmentRequired(), nil)
		}
		target, _, operator, right := splitSetAssignment(assignment)
		if operator == "" || !isValidIdentifier(target) || right == "" {
			return newSemanticError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax", "invalid SET assignment: "+assignment)
		}
		if right == "$" {
			return localizedError(localization.CypherMutationsSetAssignmentParameterNameRequired(), nil)
		}
		switch operator {
		case "+=":
			if strings.HasPrefix(right, "{") {
				if _, err := parseSetMergeMapExpressionsStrict(right); err != nil {
					return localizedError(localization.CypherMutationsSetMergeParseFailed(err), err)
				}
			}
		case ":":
			if strings.HasPrefix(right, "$(") {
				continue // dynamic labels are resolved at run time
			}
			if _, err := setLabelChain(right); err != nil {
				return err
			}
		}
	}
	return nil
}

func pipelineSetOperation(variable string, assignments []string) string {
	for _, assignment := range assignments {
		assignment = strings.TrimSpace(assignment)
		if strings.HasPrefix(assignment, variable+" +=") || strings.HasPrefix(assignment, variable+"+=") {
			return variable + " +="
		}
		if strings.HasPrefix(assignment, variable+" =") || strings.HasPrefix(assignment, variable+"=") {
			return variable + " ="
		}
		if strings.HasPrefix(assignment, variable+".") {
			return variable + ".property ="
		}
	}
	return variable
}

func addedLabelCount(before, after []string) int {
	known := make(map[string]struct{}, len(before))
	for _, label := range before {
		known[label] = struct{}{}
	}
	added := 0
	for _, label := range after {
		if _, exists := known[label]; !exists {
			added++
		}
	}
	return added
}

// pipelineSetTargetVariables lists the variables a SET clause writes, in
// first-appearance order, using the shared splitSetAssignment.
func pipelineSetTargetVariables(assignments []string) []string {
	seen := make(map[string]struct{})
	var targets []string
	for _, assignment := range assignments {
		target, _, operator, _ := splitSetAssignment(assignment)
		if operator == "" || !isValidIdentifier(target) {
			continue
		}
		if _, exists := seen[target]; exists {
			continue
		}
		seen[target] = struct{}{}
		targets = append(targets, target)
	}
	return targets
}

// changedPropertyCount is the SET counting rule: the number of properties
// added, replaced with a different value or removed between before and after.
func changedPropertyCount(before, after map[string]interface{}) int {
	changed := 0
	for key, value := range after {
		if old, exists := before[key]; !exists || !samePropertyValue(old, value) {
			changed++
		}
	}
	for key := range before {
		if _, exists := after[key]; !exists {
			changed++
		}
	}
	return changed
}

// samePropertyValue is reflect.DeepEqual for stored property values, with the
// common scalar types compared directly.
func samePropertyValue(a, b interface{}) bool {
	switch av := a.(type) {
	case string:
		bv, ok := b.(string)
		return ok && av == bv
	case int64:
		bv, ok := b.(int64)
		return ok && av == bv
	case float64:
		bv, ok := b.(float64)
		return ok && av == bv
	case bool:
		bv, ok := b.(bool)
		return ok && av == bv
	}
	return reflect.DeepEqual(a, b)
}

// ---- clause appliers ----

func (e *StorageExecutor) pipelineApplyOptionalMatch(ctx context.Context, rows []pipelineRow, clause string) ([]pipelineRow, error) {
	if shortest, ok, err := e.parseShortestPathMatch(ctx, strings.TrimSpace(clause[len("OPTIONAL MATCH"):])); ok || err != nil {
		if err != nil {
			return nil, err
		}
		return e.pipelineApplyShortestPathMatch(ctx, rows, shortest, true)
	}
	optionalClause := splitOptionalMatchClauses(strings.TrimSpace(clause[len("OPTIONAL MATCH"):]))
	if len(optionalClause) != 1 {
		return nil, localizedError(localization.CypherCoreOptionalMatchRequired(), nil)
	}
	nodeVariables := make(map[string]struct{})
	for _, variable := range extractNodeVariables(clause) {
		nodeVariables[variable] = struct{}{}
	}
	relationshipVariables := make(map[string]struct{})
	for _, variable := range extractRelationshipVariables(clause) {
		relationshipVariables[variable] = struct{}{}
	}

	out := make([]pipelineRow, 0, len(rows))
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

		expanded, err := e.applyTraversalOptionalClause(ctx, []traversalOptRow{traversalRow}, optionalClause[0])
		if err != nil {
			return nil, err
		}
		for _, expandedRow := range expanded {
			joined := make(pipelineRow, util.SafePreallocSum(len(row), len(expandedRow.nodes)+len(expandedRow.rels)))
			for name, value := range row {
				joined[name] = value
			}
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
	if shortest, ok, err := e.parseShortestPathMatch(ctx, body); ok || err != nil {
		if err != nil {
			return nil, true, err
		}
		expanded, err := e.pipelineApplyShortestPathMatch(ctx, rows, shortest, false)
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
			if !strings.HasPrefix(strings.TrimSpace(part), "(") || containsRelExistencePattern(part) {
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
		expanded, resolved := materializePipelineSource(product)
		if !resolved {
			return nil, true, getExpressionFailure(ctx)
		}
		return expanded, true, nil
	}
	if len(parts) > 1 || len(parts) == 1 && strings.HasPrefix(strings.TrimSpace(parts[0]), "(") &&
		!containsRelExistencePattern(parts[0]) && e.parseNodePattern(ctx, parts[0]).variable == "" {
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

func (e *StorageExecutor) pipelineMatchHint(remaining []pipelineClause) pipelineMatchPhysicalHint {
	hint := pipelineMatchPhysicalHint{limit: -1, earlyLimit: -1, readTail: pipelineReadOnlyTail(remaining)}
	if len(remaining) > 0 && remaining[0].kind == pipelineClauseWith {
		clause := remaining[0].text
		if topLevelKeywordIndex(clause, "WHERE") >= 0 {
			return hint
		}
		end := len(clause)
		for _, keyword := range []string{"SKIP", "LIMIT"} {
			if index := topLevelKeywordIndex(clause, keyword); index >= 0 && index < end {
				end = index
			}
		}
		if _, local := parsePipelineRowWith(clause[:end]); !local {
			return hint
		}
		limit, hasLimit := e.parseIntModifier(context.Background(), clause, "LIMIT")
		skip, _ := e.parseIntModifier(context.Background(), clause, "SKIP")
		if !hasLimit || limit <= 0 || skip < 0 || skip > int(^uint(0)>>1)-limit {
			return hint
		}
		hint.limit, hint.earlyLimit = skip+limit, skip+limit
		return hint
	}
	var terminalReturn string
	for index, clause := range remaining {
		if clause.kind != pipelineClauseReturn || index != len(remaining)-1 {
			return hint
		}
		terminalReturn = clause.text
	}
	if terminalReturn == "" {
		return hint
	}
	body := strings.TrimSpace(terminalReturn[len("RETURN"):])
	if _, distinct := cutDistinct(body); distinct {
		return hint
	}
	for _, item := range e.parseReturnItems(body) {
		if pipelineExpressionContainsAggregate(item.expr) {
			return hint
		}
	}
	skip, hasSkip := e.parseIntModifier(context.Background(), body, "SKIP")
	if hasSkip && skip != 0 {
		return hint
	}
	limit, hasLimit := e.parseIntModifier(context.Background(), body, "LIMIT")
	if !hasLimit || limit < 0 {
		return hint
	}
	hint.limit = limit
	if orderIndex := topLevelKeywordIndex(body, "ORDER BY"); orderIndex >= 0 {
		orderExpr := strings.TrimSpace(body[orderIndex+len("ORDER BY"):])
		end := len(orderExpr)
		for _, keyword := range []string{"SKIP", "LIMIT"} {
			if index := topLevelKeywordIndex(orderExpr, keyword); index >= 0 && index < end {
				end = index
			}
		}
		hint.orderExpr = strings.TrimSpace(orderExpr[:end])
	} else {
		hint.earlyLimit = limit
	}
	return hint
}

// pipelineApplyInitialTraversalMatch adapts the shared streaming traversal
// operators to pipeline rows. It returns graph entities as bindings so every
// later WITH, mutation, and RETURN clause continues through the same executor.
func (e *StorageExecutor) pipelineApplyInitialTraversalMatch(ctx context.Context, rows []pipelineRow, clause string, hint pipelineMatchPhysicalHint) ([]pipelineRow, bool, error) {
	if len(rows) == 0 {
		return rows, true, nil
	}
	pattern := strings.TrimSpace(clause[len("MATCH"):])
	whereClause := ""
	if whereIndex := topLevelKeywordIndex(pattern, "WHERE"); whereIndex >= 0 {
		whereClause = normalizePipelineWhitespace(pattern[whereIndex+len("WHERE"):])
		pattern = strings.TrimSpace(pattern[:whereIndex])
	}
	if !containsRelExistencePattern(pattern) {
		return nil, false, nil
	}
	// A mixed comma-separated MATCH is a product of independent pattern
	// components. The single traversal operator cannot consume only one
	// component without losing rows; leave the complete product to the shared
	// multi-pattern operator.
	if len(splitTopLevelComma(pattern)) != 1 {
		return nil, false, nil
	}

	variables := make([]string, 0)
	for _, variable := range extractNodeVariables(pattern) {
		variables = appendUniquePipelineBinding(variables, variable)
	}
	for _, variable := range extractRelationshipVariables(pattern) {
		variables = appendUniquePipelineBinding(variables, variable)
	}
	pathVariable := extractPathAssignmentVariable(pattern)
	if pathVariable != "" {
		variables = appendUniquePipelineBinding(variables, pathVariable)
	}
	const anonymousBinding = "__nornic_pipeline_traversal"
	returnItems := make([]returnItem, 0, len(variables)+1)
	for _, variable := range variables {
		returnItems = append(returnItems, returnItem{expr: variable, alias: variable})
	}
	if len(returnItems) == 0 {
		returnItems = append(returnItems, returnItem{expr: "1", alias: anonymousBinding})
	}

	store := e.getStorage(ctx)
	out := make([]pipelineRow, 0, len(rows))
	for _, row := range rows {
		materializedPattern := e.materializePipelinePropertyExpressions(ctx, pattern, row)
		materializedWhere := e.materializePipelinePredicateExpressions(whereClause, row)
		physicalWhere := pipelineTraversalPushdownPredicate(materializedWhere, row, variables)
		rowHint := hint
		if physicalWhere != materializedWhere || pipelinePatternJoinsOuterBinding(row, variables) {
			rowHint = pipelineMatchPhysicalHint{limit: -1, earlyLimit: -1}
		}
		var result *ExecuteResult
		var handled bool
		var err error
		if rowHint.limit > 0 && rowHint.orderExpr != "" {
			result, handled, err = e.tryExecuteTraversalStartSeedOrderLimit(ctx, materializedPattern, physicalWhere, returnItems, pathVariable, rowHint.orderExpr, rowHint.limit)
			if err == nil && !handled {
				result, handled, err = e.tryExecuteTraversalEndSeedOrderLimit(ctx, materializedPattern, physicalWhere, returnItems, pathVariable, rowHint.orderExpr, rowHint.limit)
			}
		}
		if err != nil {
			return nil, true, err
		}
		if !handled {
			startSeedNodes, endSeedNodes, seedRejected := e.pipelineTraversalSeedNodes(ctx, materializedPattern, row)
			if seedRejected {
				// The bound node fails the pattern's own labels or inline
				// properties on that endpoint, so this row matches nothing.
				continue
			}
			result, err = e.executeMatchWithRelationshipsWithPathSeeded(ctx, materializedPattern, physicalWhere, returnItems, startSeedNodes, endSeedNodes, pathVariable, rowHint.earlyLimit)
			if err != nil {
				return nil, true, err
			}
		}
		e.normalizeSetMatchRowsToNodes(result, store)
		e.normalizeSetMatchRowsToEdges(result, store)
		for _, resultRow := range result.Rows {
			joined := make(pipelineRow, util.SafePreallocSum(len(row), len(result.Columns)))
			for name, value := range row {
				joined[name] = value
			}
			compatible := true
			for index, column := range result.Columns {
				if column == anonymousBinding || index >= len(resultRow) {
					continue
				}
				value := resultRow[index]
				if existing, bound := joined[column]; bound && !pipelineBindingValuesEqual(existing, value) {
					compatible = false
					break
				}
				joined[column] = value
			}
			if compatible && (materializedWhere == "" || e.evaluateWithWhereCondition(ctx, materializedWhere, map[string]interface{}(joined))) {
				out = append(out, joined)
			}
		}
	}
	return out, true, nil
}

func pipelineTraversalPushdownPredicate(whereClause string, row pipelineRow, localVariables []string) string {
	if strings.TrimSpace(whereClause) == "" {
		return ""
	}
	local := make(map[string]struct{}, len(localVariables))
	for _, variable := range localVariables {
		local[variable] = struct{}{}
	}
	terms := splitTopLevelAndConjuncts(whereClause)
	pushable := make([]string, 0, len(terms))
	for _, term := range terms {
		term = strings.TrimSpace(term)
		if term == "" {
			continue
		}
		dependsOnOuterBinding := false
		for name := range row {
			if _, isLocal := local[name]; isLocal || strings.HasPrefix(name, "$") {
				continue
			}
			if referencesVariable(term, name) {
				dependsOnOuterBinding = true
				break
			}
		}
		if !dependsOnOuterBinding {
			pushable = append(pushable, term)
		}
	}
	return strings.Join(pushable, " AND ")
}

func pipelinePatternJoinsOuterBinding(row pipelineRow, variables []string) bool {
	for _, variable := range variables {
		if _, bound := row[variable]; bound {
			return true
		}
	}
	return false
}

// pipelineTraversalSeedNodes looks up whether the traversal pattern's
// start-node or end-node variable is already bound to a node by an earlier
// clause in this row. When it is, the caller can seed
// executeMatchWithRelationshipsWithPathSeeded from that single node instead
// of expanding the whole pattern over the store and joining the bound value
// afterward. Start-node binding takes priority; end-node binding is only
// honored for non-chained patterns, since reversing a multi-segment chain
// is not supported.
//
// The seeded endpoint skips the scan that would otherwise apply the pattern's
// labels and inline properties for that endpoint, so they are checked here:
// every label must be present and every inline property must match. When the
// bound node fails them, rejected is true and the row matches nothing.
func (e *StorageExecutor) pipelineTraversalSeedNodes(ctx context.Context, pattern string, row pipelineRow) (startSeedNodes, endSeedNodes []*storage.Node, rejected bool) {
	matches := e.parseTraversalPattern(ctx, pattern)
	if matches == nil {
		return nil, nil, false
	}
	if node, ok := pipelineBoundNode(row, matches.StartNode.variable); ok {
		if !e.matchesEndPattern(node, &matches.StartNode) {
			return nil, nil, true
		}
		return []*storage.Node{node}, nil, false
	}
	if !matches.IsChained {
		if node, ok := pipelineBoundNode(row, matches.EndNode.variable); ok {
			if !e.matchesEndPattern(node, &matches.EndNode) {
				return nil, nil, true
			}
			return nil, []*storage.Node{node}, false
		}
	}
	return nil, nil, false
}

func pipelineBoundNode(row pipelineRow, variable string) (*storage.Node, bool) {
	if variable == "" {
		return nil, false
	}
	value, bound := row[variable]
	if !bound {
		return nil, false
	}
	node, ok := value.(*storage.Node)
	if !ok || node == nil {
		return nil, false
	}
	return node, true
}

func pipelineBindingValuesEqual(left, right interface{}) bool {
	switch typed := left.(type) {
	case *storage.Node:
		other, ok := right.(*storage.Node)
		return ok && typed != nil && other != nil && typed.ID == other.ID
	case *storage.Edge:
		other, ok := right.(*storage.Edge)
		return ok && typed != nil && other != nil && typed.ID == other.ID
	default:
		return reflect.DeepEqual(left, right)
	}
}

func (e *StorageExecutor) pipelineApplyInitialNodeMatch(ctx context.Context, rows []pipelineRow, clause string, hint pipelineMatchPhysicalHint) ([]pipelineRow, bool, error) {
	if len(rows) == 0 {
		return rows, true, nil
	}
	pattern := strings.TrimSpace(clause[len("MATCH"):])
	whereClause := ""
	if whereIndex := topLevelKeywordIndex(pattern, "WHERE"); whereIndex >= 0 {
		whereClause = normalizePipelineWhitespace(pattern[whereIndex+len("WHERE"):])
		pattern = strings.TrimSpace(pattern[:whereIndex])
	}
	if strings.Contains(pattern, "-[") || strings.Contains(pattern, "]-") || len(e.splitNodePatterns(pattern)) != 1 {
		return nil, false, nil
	}
	pathVariable := extractPathAssignmentVariable(pattern)
	if pathVariable != "" {
		// p = (n:L): the node pattern is what follows the assignment. Parsed
		// with the assignment, its variable was "p = (n" and it matched no
		// node.
		_, pattern, _ = splitPathAssignment(pattern)
	}
	basePattern := e.parseNodePattern(ctx, pattern)
	if basePattern.variable == "" {
		return nil, false, nil
	}
	type initialCandidates struct {
		nodes        []*storage.Node
		whereApplied bool
	}
	candidateCache := make(map[string]initialCandidates)
	template := e.pipelineNodeMatchTemplateFor(clause)
	out := make([]pipelineRow, 0, len(rows))
	for _, row := range rows {
		materializedWhere := whereClause
		// The pattern parsed once and its properties evaluated for the row;
		// the text route for a row the template can't evaluate.
		nodePattern, templated := template.node(ctx, e, row)
		var cacheKey string
		if templated {
			if propsKey, keyed := pipelinePropertiesKey(nodePattern.properties); keyed {
				cacheKey = "\x03" + propsKey + "\x00" + materializedWhere
			}
		} else {
			materializedPattern := e.materializePipelinePropertyExpressions(ctx, pattern, row)
			nodePattern = e.parseNodePattern(ctx, materializedPattern)
			cacheKey = materializedPattern + "\x00" + materializedWhere
		}
		if bound, exists := row[nodePattern.variable]; exists {
			node, isNode := bound.(*storage.Node)
			if !isNode || node == nil || !pipelineNodeMatchesPattern(node, nodePattern) {
				continue
			}
			if materializedWhere == "" || e.evaluateMatchWhereCondition(ctx, materializedWhere, map[string]interface{}(row)) {
				out = append(out, e.pipelineBindZeroLengthPath(row, pathVariable, node))
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
		candidates, cached := candidateCache[cacheKey]
		if cacheKey == "" || !cached {
			var err error
			candidates.nodes, candidates.whereApplied, err = e.collectPipelineInitialNodeCandidates(withValueBindings(ctx, row), nodePattern, materializedWhere, candidateHint)
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

func (e *StorageExecutor) materializePipelinePredicateExpressions(expression string, row pipelineRow) string {
	if expression == "" {
		return expression
	}
	materialized := expression
	for name, value := range row {
		if strings.HasPrefix(name, "$") {
			continue
		}
		if object, ok := toStringAnyMap(value); ok {
			for property, propertyValue := range object {
				reference := name + "." + property
				if strings.Contains(materialized, reference) {
					materialized = replaceQualifiedReferenceOutsideQuotes(materialized, reference, e.valueToLiteral(propertyValue))
				}
			}
			continue
		}
		switch entity := value.(type) {
		case *storage.Node:
			if entity != nil {
				for property, propertyValue := range entity.Properties {
					reference := name + "." + property
					if strings.Contains(materialized, reference) {
						materialized = replaceQualifiedReferenceOutsideQuotes(materialized, reference, e.valueToLiteral(propertyValue))
					}
				}
			}
			continue
		case *storage.Edge:
			if entity != nil {
				for property, propertyValue := range entity.Properties {
					reference := name + "." + property
					if strings.Contains(materialized, reference) {
						materialized = replaceQualifiedReferenceOutsideQuotes(materialized, reference, e.valueToLiteral(propertyValue))
					}
				}
			}
			continue
		}
		if referencesVariable(materialized, name) {
			materialized = replaceIdentifierOutsideQuotes(materialized, name, e.valueToLiteral(value))
		}
	}
	return materialized
}

// replaceQualifiedReferenceOutsideQuotes replaces a complete dotted row
// reference without rewriting string literals, longer identifiers, or a
// property access rooted at the reference. replaceIdentifierOutsideQuotes is
// intentionally token-oriented and therefore cannot match a dotted name.
func replaceQualifiedReferenceOutsideQuotes(input, reference, replacement string) string {
	if reference == "" || !strings.Contains(input, reference) {
		return input
	}
	var output strings.Builder
	output.Grow(len(input) + len(replacement))
	quote := byte(0)
	for index := 0; index < len(input); {
		character := input[index]
		if quote != 0 {
			output.WriteByte(character)
			index++
			if character == '\\' && quote != '`' && index < len(input) {
				output.WriteByte(input[index])
				index++
				continue
			}
			if character == quote {
				quote = 0
			}
			continue
		}
		if character == '\'' || character == '"' || character == '`' {
			quote = character
			output.WriteByte(character)
			index++
			continue
		}
		end := index + len(reference)
		if end <= len(input) && input[index:end] == reference &&
			(index == 0 || (!isIdentByte(input[index-1]) && input[index-1] != '.')) &&
			(end == len(input) || (!isIdentByte(input[end]) && input[end] != '.')) {
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
	if node := prefetchedPipelineCandidate(ctx, nodePattern); node != nil {
		return []*storage.Node{node}, false, nil
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
func prefetchedPipelineCandidate(ctx context.Context, nodePattern nodePatternInfo) *storage.Node {
	prefetched, ok := ctx.Value(pipelinePrefetchedNodeCandidatesKey{}).(map[nodeBatchMatchKey]map[string]*storage.Node)
	if !ok || len(nodePattern.labels) != 1 || len(nodePattern.properties) != 1 {
		return nil
	}
	for property, value := range nodePattern.properties {
		key := nodeBatchMatchKey{label: nodePattern.labels[0], prop: property}
		if node := prefetched[key][propEqKeyBatch(value)]; node != nil && pipelineNodeMatchesPattern(node, nodePattern) {
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
	for _, variable := range extractNodeVariables(clause) {
		patternBindings[variable] = struct{}{}
	}
	for _, variable := range extractRelationshipVariables(clause) {
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

	pattern := strings.TrimSpace(clause[len("MATCH"):])
	whereClause := ""
	if whereIndex := topLevelKeywordIndex(pattern, "WHERE"); whereIndex >= 0 {
		whereClause = normalizePipelineWhitespace(pattern[whereIndex+len("WHERE"):])
		pattern = strings.TrimSpace(pattern[:whereIndex])
	}
	// executeChainedMatch runs one pattern part. A MATCH of several parts
	// ((a)-->(x), (b)-->(x)) needs its relationships distinct across the
	// parts, which the MATCH executor below enforces.
	if len(splitTopLevelComma(pattern)) != 1 || !containsRelExistencePattern(pattern) {
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
		joinedNodes, joinedRelationships := e.executeChainedMatch(ctx, pattern, []binding{nodes}, []relationshipBinding{relationships})
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
	pattern := strings.TrimSpace(clause[len("MATCH"):])
	match := e.parseTraversalPattern(ctx, pattern)
	if match == nil || match.IsChained || !match.Relationship.VariableLength || match.Relationship.Variable == "" {
		return nil, false, nil
	}
	for _, row := range rows {
		if _, exists := row[match.Relationship.Variable]; !exists {
			return nil, false, nil
		}
	}

	store := e.getStorage(ctx)
	out := make([]pipelineRow, 0, len(rows))
	for _, row := range rows {
		relationships, ok := pipelineRelationshipList(row[match.Relationship.Variable])
		if !ok || len(relationships) == 0 || relationshipListReusesEdge(relationships) {
			continue
		}
		for _, endpoints := range traceRelationshipList(relationships, match.Relationship.Direction) {
			start, startErr := store.GetNode(endpoints[0])
			end, endErr := store.GetNode(endpoints[1])
			if startErr != nil || endErr != nil || start == nil || end == nil ||
				!pipelineNodeMatchesPattern(start, match.StartNode) || !pipelineNodeMatchesPattern(end, match.EndNode) {
				continue
			}
			if bound, exists := row[match.StartNode.variable]; exists {
				boundNode, isNode := bound.(*storage.Node)
				if !isNode || boundNode == nil || boundNode.ID != start.ID {
					continue
				}
			}
			if bound, exists := row[match.EndNode.variable]; exists {
				boundNode, isNode := bound.(*storage.Node)
				if !isNode || boundNode == nil || boundNode.ID != end.ID {
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
			out = append(out, expanded)
		}
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
	seen := make(map[storage.EdgeID]struct{}, len(relationships))
	for _, relationship := range relationships {
		if relationship == nil {
			return true
		}
		if _, exists := seen[relationship.ID]; exists {
			return true
		}
		seen[relationship.ID] = struct{}{}
	}
	return false
}

func traceRelationshipList(relationships []*storage.Edge, direction string) [][2]storage.NodeID {
	if len(relationships) == 0 {
		return nil
	}
	starts := [][2]storage.NodeID{{relationships[0].StartNode, relationships[0].EndNode}}
	if direction == "incoming" {
		starts[0] = [2]storage.NodeID{relationships[0].EndNode, relationships[0].StartNode}
	} else if direction == "both" && relationships[0].StartNode != relationships[0].EndNode {
		starts = append(starts, [2]storage.NodeID{relationships[0].EndNode, relationships[0].StartNode})
	}
	for _, relationship := range relationships[1:] {
		next := starts[:0]
		for _, endpoints := range starts {
			switch direction {
			case "outgoing":
				if endpoints[1] == relationship.StartNode {
					next = append(next, [2]storage.NodeID{endpoints[0], relationship.EndNode})
				}
			case "incoming":
				if endpoints[1] == relationship.EndNode {
					next = append(next, [2]storage.NodeID{endpoints[0], relationship.StartNode})
				}
			default:
				if endpoints[1] == relationship.StartNode {
					next = append(next, [2]storage.NodeID{endpoints[0], relationship.EndNode})
				}
				if endpoints[1] == relationship.EndNode && relationship.StartNode != relationship.EndNode {
					next = append(next, [2]storage.NodeID{endpoints[0], relationship.StartNode})
				}
			}
		}
		starts = next
	}
	return starts
}

func pipelineNodeMatchesPattern(node *storage.Node, pattern nodePatternInfo) bool {
	if !mergeNodeHasLabels(node, pattern.labels) {
		return false
	}
	return nodePropertiesMatch(node, pattern.properties)
}

func (e *StorageExecutor) tryExecutePipelineCreatePlan(ctx context.Context, clauses, originalClauses []pipelineClause) (*ExecuteResult, bool, error) {
	end := 0
	for end < len(clauses) && clauses[end].kind == pipelineClauseCreate {
		end++
	}
	if end == 0 {
		return nil, false, nil
	}
	for index := end; index < len(clauses); index++ {
		if clauses[index].kind != pipelineClauseSet &&
			!(clauses[index].kind == pipelineClauseReturn && index == len(clauses)-1) {
			return nil, false, nil
		}
	}
	initial := pipelineRow{}
	for name, value := range e.fabricRecordBindings {
		initial[name] = value
	}
	for name, value := range valueBindingsFromContext(ctx) {
		initial[name] = value
	}
	bindParameterRow(ctx, initial)
	rows, result, _, err := e.pipelineApplyCreateClauses(ctx, []pipelineRow{initial}, clauses[:end])
	if err != nil {
		return nil, true, err
	}
	for index := end; index < len(clauses); index++ {
		if clauses[index].kind == pipelineClauseSet {
			stats, _, err := e.pipelineApplySet(ctx, rows, clauses[index].text)
			if err != nil {
				return nil, true, err
			}
			addQueryStats(result.Stats, stats)
			continue
		}
		projected, err := e.projectMergeReturn(ctx, rows, originalClauses[index].text)
		if err != nil {
			return nil, true, err
		}
		result.Columns, result.Rows = projected.Columns, projected.Rows
	}
	return result, true, nil
}

type pipelineIndependentCreateBatchKey struct{}

func (e *StorageExecutor) pipelineApplyCreateClauses(ctx context.Context, rows []pipelineRow, clauses []pipelineClause) ([]pipelineRow, *ExecuteResult, bool, error) {
	created := &ExecuteResult{Stats: &QueryStats{}}
	if independent, _ := ctx.Value(pipelineIndependentCreateBatchKey{}).(bool); independent && len(rows) > 1 {
		return e.pipelineCreateSource(ctx, pipelineRowsSource(rows), clauses)
	}
	var out []pipelineRow
	for _, row := range rows {
		if err := ctx.Err(); err != nil {
			return nil, nil, false, err
		}
		newRow, err := e.pipelineCreateRow(ctx, row, clauses, created)
		if err != nil {
			return nil, nil, true, err
		}
		out = append(out, newRow)
	}
	return out, created, true, nil
}

func (e *StorageExecutor) pipelineCreateRow(ctx context.Context, row pipelineRow, clauses []pipelineClause, created *ExecuteResult) (pipelineRow, error) {
	plan := acquireCreatePlan()
	defer plan.release()
	newRow, err := e.pipelinePlanCreateRow(ctx, row, clauses, plan)
	if err != nil {
		return nil, err
	}
	if err := e.applyCreatePlan(ctx, plan, created); err != nil {
		return nil, localizedError(localization.CypherInvariantsPipelineCreateFailed(err), err)
	}
	return newRow, nil
}

func (e *StorageExecutor) pipelinePlanCreateRow(ctx context.Context, row pipelineRow, clauses []pipelineClause, plan *createPlan) (pipelineRow, error) {
	nodes, edges := plan.bindings()
	newRow := make(pipelineRow, len(row))
	for name, value := range row {
		newRow[name] = value
		switch typed := value.(type) {
		case *storage.Node:
			if typed != nil {
				nodes[name] = typed
			}
		case *storage.Edge:
			if typed != nil {
				edges[name] = typed
			}
		}
	}
	for _, clause := range clauses {
		rowCtx := withValueBindings(ctx, newRow)
		paths, err := e.planCreatePatterns(rowCtx, pipelineClauseBody(clause.text, "CREATE"), nodes, edges, plan)
		if err != nil {
			if failure := getExpressionFailure(rowCtx); failure != nil {
				return nil, failure
			}
			return nil, localizedError(localization.CypherInvariantsPipelineCreateFailed(err), err)
		}
		for name, node := range nodes {
			newRow[name] = node
		}
		for name, edge := range edges {
			newRow[name] = edge
		}
		for name, path := range paths {
			newRow[name] = e.pathToMap(path)
		}
	}
	return newRow, nil
}

// pipelineApplyMerge executes one MERGE per input row while retaining the row
// bindings for subsequent clauses. This preserves Cypher's row-at-a-time
// mutation semantics after UNWIND/WITH without duplicating MERGE behavior.
func (e *StorageExecutor) pipelineApplyMerge(ctx context.Context, rows []pipelineRow, clause string) ([]pipelineRow, *QueryStats, error) {
	stats := &QueryStats{}
	out := make([]pipelineRow, 0, len(rows))
	// The relationship lookups of this MERGE over its rows share one read
	// per endpoint pair (relationshipMergeIdentityCache); one row looks up
	// directly.
	var identities *relationshipMergeIdentityCache
	if len(rows) > 1 {
		identities = newRelationshipMergeIdentityCache()
	}
	template := e.pipelineMergeTemplateFor(clause)
	// The clause's pattern and actions (each action clause a SET of its own,
	// in order) are the same for every row.
	clauseBody := strings.TrimSpace(clause)
	isMerge := startsWithKeywordFold(clauseBody, "MERGE")
	var clausePattern, onCreateSet, onMatchSet string
	multiRelationship := false
	if isMerge {
		parts := splitMergeClauseActions(strings.TrimSpace(clauseBody[len("MERGE"):]))
		clausePattern, onCreateSet, onMatchSet = parts.pattern, parts.onCreate.setText(), parts.onMatch.setText()
		multiRelationship = e.isMultiRelationshipPattern(clausePattern)
	}
	for _, row := range rows {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		ctx := withValueBindings(ctx, row)
		// The clause parsed once: a row whose relationship already exists
		// is its matches, without rendering and parsing the row's text.
		if pattern, startNode, endNode, templated := template.pattern(ctx, e, row); templated {
			matches, findErr := findParsedMergeRelationships(e.getStorage(ctx), identities, pattern, startNode, endNode)
			if findErr != nil {
				return nil, nil, findErr
			}
			if len(matches) > 0 {
				out = append(out, e.mergeRelationshipRows(row, nil, nil, pattern, startNode, endNode, matches)...)
				continue
			}
		}
		nodeContext := make(map[string]*storage.Node)
		relContext := make(map[string]*storage.Edge)
		for name, value := range row {
			switch typed := value.(type) {
			case *storage.Node:
				nodeContext[name] = typed
			case *storage.Edge:
				relContext[name] = typed
			}
		}
		var relationshipPattern *mergeRelationshipPattern
		var nodePathVariable string
		var nodePathBinding string
		mergePattern := clauseBody
		if isMerge {
			mergePattern = clausePattern
			if multiRelationship {
				produced, pathStats, created, err := e.pipelineMergePath(ctx, row, mergePattern, nodeContext, relContext)
				if err != nil {
					return nil, nil, err
				}
				addQueryStats(stats, pathStats)
				actions := onMatchSet
				if created {
					actions = onCreateSet
				}
				if err := e.applyMergeActions(ctx, produced, actions, stats); err != nil {
					return nil, nil, err
				}
				// It may have written between any pair of nodes.
				identities.reset()
				out = append(out, produced...)
				continue
			}
			if open, _ := firstRelationshipBracket(mergePattern); open >= 0 {
				var parseErr error
				relationshipPattern, parseErr = e.parseMergeRelationshipPattern(ctx, mergePattern, nodeContext, relContext)
				if parseErr != nil {
					return nil, nil, parseErr
				}
				for _, variable := range [...]string{relationshipPattern.startVariable, relationshipPattern.endVariable} {
					if missingRelationshipEndpoint(row, variable) {
						return nil, nil, relationshipEndpointMissingError(variable)
					}
				}
			} else if nodePathVariable = extractPathAssignmentVariable(mergePattern); nodePathVariable != "" {
				mergePattern = strings.TrimSpace(mergePattern[strings.Index(mergePattern, "=")+1:])
				nodePathBinding = e.extractVarName(mergePattern)
			}
		}
		if relationshipPattern == nil {
			nodePattern := mergePattern
			variable, labels, properties, parseErr := e.parseMergeNodePattern(ctx, nodePattern, nodeContext, relContext)
			if parseErr == nil {
				_, alreadyBound := nodeContext[variable]
				if variable == "" || !alreadyBound {
					if err := prepareMergeKeys(ctx, e.getStorage(ctx), labels, properties); err != nil {
						return nil, nil, err
					}
					matches, scanned, findErr := e.findMergeNodesScanned(e.getStorage(ctx), labels, properties)
					if findErr != nil {
						return nil, nil, findErr
					}
					if len(matches) > 0 {
						matchedRows := make([]pipelineRow, 0, len(matches))
						for _, node := range matches {
							expanded := make(pipelineRow, util.SafePreallocSum(len(row), 2))
							for name, value := range row {
								expanded[name] = value
							}
							if variable != "" {
								expanded[variable] = node
							}
							if nodePathVariable != "" {
								path := PathResult{Nodes: []*storage.Node{node}}
								expanded[nodePathVariable] = e.pathToMap(path)
							}
							matchedRows = append(matchedRows, expanded)
						}
						// ON MATCH SET applies to every matched row, through
						// the shared SET applicator.
						if err := e.applyMergeActions(ctx, matchedRows, onMatchSet, stats); err != nil {
							return nil, nil, err
						}
						out = append(out, matchedRows...)
						continue
					}
					if scanned {
						ctx = withMergeNodeAbsent(ctx)
					}
				}
			}
		}
		if relationshipPattern != nil {
			// Bound endpoints: every existing relationship that matches the
			// pattern is a row, and ON MATCH SET applies to each, as in
			// Neo4j. One lookup decides; only a pattern with no match reaches
			// the create path below.
			startNode := nodeContext[relationshipPattern.startVariable]
			endNode := nodeContext[relationshipPattern.endVariable]
			if startNode != nil && endNode != nil {
				matches, findErr := findParsedMergeRelationships(e.getStorage(ctx), identities, relationshipPattern, startNode, endNode)
				if findErr != nil {
					return nil, nil, findErr
				}
				if len(matches) > 0 {
					matchedRows := e.mergeRelationshipRows(row, nodeContext, relContext, relationshipPattern, startNode, endNode, matches)
					if onMatchSet != "" {
						// It can change identity values.
						identities.reset()
					}
					if err := e.applyMergeActions(ctx, matchedRows, onMatchSet, stats); err != nil {
						return nil, nil, err
					}
					out = append(out, matchedRows...)
					continue
				}
			}
		}
		boundEndpoints := relationshipPattern != nil &&
			nodeContext[relationshipPattern.startVariable] != nil && nodeContext[relationshipPattern.endVariable] != nil
		merged, err := e.executeMergeWithContext(ctx, "MERGE "+mergePattern, nodeContext, relContext)
		if err != nil {
			return nil, nil, err
		}
		if relationshipPattern != nil && !boundEndpoints {
			// The MERGE bound or created endpoints itself: it may have
			// written between any pair.
			identities.reset()
		}
		patternCreated := false
		if merged != nil && merged.Stats != nil {
			addQueryStats(stats, merged.Stats)
			patternCreated = merged.Stats.NodesCreated > 0 || merged.Stats.RelationshipsCreated > 0
		}
		// The MERGE created its pattern (ON CREATE) or found it (ON MATCH).
		actions := onMatchSet
		if patternCreated {
			actions = onCreateSet
		}
		var produced []pipelineRow
		newRow := make(pipelineRow, util.SafePreallocSum(len(row), len(nodeContext), len(relContext)))
		for name, value := range row {
			newRow[name] = value
		}
		for name, node := range nodeContext {
			newRow[name] = node
		}
		for name, relationship := range relContext {
			newRow[name] = relationship
		}
		if nodePathVariable != "" {
			if node := nodeContext[nodePathBinding]; node != nil {
				path := PathResult{Nodes: []*storage.Node{node}}
				newRow[nodePathVariable] = e.pathToMap(path)
			}
		}
		if relationshipPattern != nil {
			// The create path made the relationship (and any endpoint the
			// pattern didn't bind): its row carries it.
			startNode := nodeContext[relationshipPattern.startVariable]
			endNode := nodeContext[relationshipPattern.endVariable]
			if startNode != nil && endNode != nil {
				created := relContext[relationshipPattern.relVariable]
				var matches []*storage.Edge
				if created != nil && relationshipPattern.relVariable != "" {
					matches = []*storage.Edge{created}
					if onCreateSet == "" {
						identities.created(created)
					} else {
						identities.reset()
					}
				} else {
					identities.reset()
					found, findErr := findParsedMergeRelationships(e.getStorage(ctx), identities, relationshipPattern, startNode, endNode)
					if findErr != nil {
						return nil, nil, findErr
					}
					matches = found
				}
				if len(matches) > 0 {
					produced = e.mergeRelationshipRows(newRow, nil, nil, relationshipPattern, startNode, endNode, matches)
				}
			}
		}
		if produced == nil {
			produced = []pipelineRow{newRow}
		}
		if actions != "" {
			// It can change identity values.
			identities.reset()
		}
		if err := e.applyMergeActions(ctx, produced, actions, stats); err != nil {
			return nil, nil, err
		}
		out = append(out, produced...)
	}
	return out, stats, nil
}

// mergeRelationshipRows returns one row per relationship a MERGE matched or
// created: row with the MERGE's node and relationship bindings, the pattern's
// relationship variable and its path variable.
func (e *StorageExecutor) mergeRelationshipRows(row pipelineRow, nodeContext map[string]*storage.Node, relContext map[string]*storage.Edge, pattern *mergeRelationshipPattern, startNode, endNode *storage.Node, relationships []*storage.Edge) []pipelineRow {
	rows := make([]pipelineRow, 0, len(relationships))
	for _, relationship := range relationships {
		expanded := make(pipelineRow, util.SafePreallocSum(len(row), len(nodeContext), len(relContext), 2))
		for name, value := range row {
			expanded[name] = value
		}
		for name, node := range nodeContext {
			expanded[name] = node
		}
		for name, edge := range relContext {
			expanded[name] = edge
		}
		if pattern.relVariable != "" {
			expanded[pattern.relVariable] = relationship
		}
		if pattern.pathVariable != "" {
			path := PathResult{Nodes: []*storage.Node{startNode, endNode}, Relationships: []*storage.Edge{relationship}, Length: 1}
			expanded[pattern.pathVariable] = e.pathToMap(path)
		}
		rows = append(rows, expanded)
	}
	return rows
}

// containsRemoveClauseAnywhere reports a REMOVE keyword outside strings,
// including one nested in a FOREACH body or subquery.
func containsRemoveClauseAnywhere(cypher string) bool {
	opts := defaultKeywordScanOpts()
	opts.SkipParens = false
	opts.SkipBrackets = false
	return keywordIndexFrom(cypher, "REMOVE", 0, opts) >= 0
}

// materializePipelinePropertyExpressions evaluates property-map values using
// the current row before the CREATE/MERGE parsers consume them. Substituting a
// variable token alone is insufficient for expressions such as row.parts[0]
// or row.value + '!': it can turn valid expressions into quoted source text.
func (e *StorageExecutor) materializePipelinePropertyExpressions(ctx context.Context, clause string, row pipelineRow) string {
	var output strings.Builder
	output.Grow(len(clause))
	for cursor := 0; cursor < len(clause); {
		if clause[cursor] != '{' {
			output.WriteByte(clause[cursor])
			cursor++
			continue
		}
		end := e.findMatchingBrace(clause, cursor)
		if end < 0 {
			output.WriteString(clause[cursor:])
			break
		}
		body := clause[cursor+1 : end]
		pairs := e.splitPropertyPairs(body)
		materialized := make([]string, 0, len(pairs))
		for _, pair := range pairs {
			colon := findTopLevelMapKeyValueSeparator(pair)
			if colon <= 0 {
				materialized = append(materialized, pair)
				continue
			}
			key := strings.TrimSpace(pair[:colon])
			expression := strings.TrimSpace(pair[colon+1:])
			if value, ok := e.evaluateRowExpressionWithContext(ctx, expression, row); ok {
				expression = e.valueToLiteral(value)
			}
			materialized = append(materialized, key+": "+expression)
		}
		output.WriteByte('{')
		output.WriteString(strings.Join(materialized, ", "))
		output.WriteByte('}')
		cursor = end + 1
	}
	return output.String()
}

// pipelineApplyWith drops / renames binding keys according to a WITH clause.
// Supports:
//   - plain variables:     `WITH a, b`            carries each forward
//   - map placeholder:     `WITH o, {}`           drops the {} projection
//   - variable → alias:    `WITH a AS b`          renames
//   - expression → alias:  `WITH a.name AS n`     evaluates a row expression
//   - aggregate pass-thru: `WITH count(*) AS c`   counts current rows
//
// Projection expressions use the same row-expression operator as WHERE,
// RETURN, and ORDER BY so list, map, property, and postfix operations cannot
// diverge between pipeline clauses.
func (e *StorageExecutor) pipelineApplyWith(ctx context.Context, rows []pipelineRow, clause string) ([]pipelineRow, bool) {
	return e.pipelineApplyWithSource(ctx, rows, clause, pipelineRowsSource(rows), false)
}

func (e *StorageExecutor) pipelineApplyWithSource(ctx context.Context, rows []pipelineRow, clause string, source pipelineRowSource, rowsValidated bool) ([]pipelineRow, bool) {
	if plan, ok := parsePipelineRowWith(clause); ok {
		out := make([]pipelineRow, 0, len(rows))
		for _, row := range rows {
			projected, accepted, resolved := e.pipelineProjectWithRow(ctx, row, plan, nil, nil)
			if !resolved {
				return nil, false
			}
			if accepted {
				out = append(out, projected)
			}
		}
		return out, true
	}
	// The clause is scanned with its keyword, which tells a keyword-named
	// first item from a clause (WITH with WHERE with = 3, #894).
	body := strings.TrimSpace(clause)
	orderTerms := parseOrderByTerms(body)
	withSkip, withLimit := 0, -1
	if skipIndex := topLevelKeywordIndex(body, "SKIP"); skipIndex >= 0 {
		value, ok := e.evaluatePipelinePagination(ctx, pipelinePaginationExpression(body, "SKIP"), rows)
		if !ok {
			return nil, false
		}
		withSkip = value
	}
	if limitIndex := topLevelKeywordIndex(body, "LIMIT"); limitIndex >= 0 {
		value, ok := e.evaluatePipelinePagination(ctx, pipelinePaginationExpression(body, "LIMIT"), rows)
		if !ok {
			return nil, false
		}
		withLimit = value
	}
	postWithWhere := ""
	if whereIdx := topLevelKeywordIndex(body, "WHERE"); whereIdx >= 0 {
		postWithWhere = strings.TrimSpace(body[whereIdx+len("WHERE"):])
		body = strings.TrimSpace(body[:whereIdx])
	}
	// WITH … [ORDER BY …] SKIP / LIMIT n WHERE p filters the rows SKIP /
	// LIMIT keep, as Neo4j does ("take the first n, then filter"). Without
	// SKIP / LIMIT the filter can run per row, before ordering: the result is
	// the same.
	windowedWhere := ""
	if postWithWhere != "" && (withSkip > 0 || withLimit >= 0) {
		windowedWhere, postWithWhere = postWithWhere, ""
	}
	for _, keyword := range []string{"ORDER BY", "SKIP", "LIMIT"} {
		if index := topLevelKeywordIndex(body, keyword); index >= 0 {
			body = strings.TrimSpace(body[:index])
		}
	}
	withDistinct := false
	body, withDistinct = cutDistinct(pipelineClauseBody(body, "WITH"))
	if strings.TrimSpace(body) == "*" {
		out := make([]pipelineRow, 0, len(rows))
		for _, row := range rows {
			projected := make(pipelineRow, len(row))
			for name, value := range row {
				projected[name] = value
			}
			out = append(out, projected)
		}
		if withDistinct {
			// WITH DISTINCT * keeps one row per distinct set of the
			// variables in scope (#883).
			out = deduplicatePipelineRows(out, pipelineRowColumns(out))
		}
		out = e.filterPipelineRows(ctx, out, postWithWhere)
		if !e.orderPipelineRows(ctx, out, orderTerms) {
			return nil, false
		}
		return e.filterPipelineRows(ctx, applyPipelineWindow(out, withSkip, withLimit), windowedWhere), true
	}
	items := splitTopLevelComma(body)
	if len(items) == 0 {
		return rows, true
	}
	if strings.TrimSpace(items[0]) == "*" {
		// WITH *, items: the * is every variable in scope (#883).
		items = starProjectionItems(pipelineWildcardColumns(rows), items[1:])
	}

	type withProjection struct {
		expr          string
		alias         string
		aggregate     bool
		aggregateName string
		aggregateExpr string
		distinct      bool
	}
	projections := make([]withProjection, 0, len(items))
	projectionAliases := make([]string, 0, len(items))
	hasAggregate := false
	for _, rawItem := range items {
		item := strings.TrimSpace(rawItem)
		if item == "" || item == "{}" {
			continue
		}
		expr, alias := parseProjectionExprAlias(item)
		if expr == "" || alias == "" {
			return nil, false
		}
		projection := withProjection{expr: expr, alias: alias}
		if aggregateName, aggregateExpr, distinct, aggregate := parsePipelineAggregate(expr); aggregate {
			if !strings.Contains(upperASCII(item), " AS ") {
				return nil, false
			}
			projection.aggregate = true
			projection.aggregateName = aggregateName
			projection.aggregateExpr = aggregateExpr
			projection.distinct = distinct
			hasAggregate = true
		} else if pipelineExpressionContainsAggregate(expr) {
			if !strings.Contains(upperASCII(item), " AS ") {
				return nil, false
			}
			projection.aggregate = true
			projection.aggregateExpr = expr
			hasAggregate = true
		}
		projections = append(projections, projection)
		projectionAliases = append(projectionAliases, alias)
	}
	if hasAggregate {
		aggregates := make([]returnProjection, len(projections))
		for index, projection := range projections {
			aggregates[index] = returnProjection{expr: projection.expr, alias: projection.alias, isAggr: projection.aggregate}
		}
		groups, ok := e.pipelineAggregateGroups(ctx, source, aggregates, rowsValidated)
		if !ok {
			return nil, false
		}

		out := make([]pipelineRow, 0, len(groups))
		needsOrderScopes := len(orderTerms) > 0 || postWithWhere != "" || withDistinct || windowedWhere != ""
		var orderScopes []pipelineRow
		if needsOrderScopes {
			orderScopes = make([]pipelineRow, 0, len(groups))
		}
		for _, group := range groups {
			newRow := pipelineRow{}
			var projectedExpressions pipelineRow
			if needsOrderScopes {
				projectedExpressions = make(pipelineRow, len(projections))
			}
			for name, value := range group.first {
				if strings.HasPrefix(name, "$") {
					newRow[name] = value
				}
			}
			for index, projection := range projections {
				value, ok := group.value(ctx, e, index)
				if !ok {
					pipelineItemUnevaluable(ctx, projection.expr)
					return nil, false
				}
				newRow[projection.alias] = value
				if needsOrderScopes {
					projectedExpressions[projection.expr] = value
				}
			}
			if !needsOrderScopes {
				out = append(out, newRow)
				continue
			}
			orderScope := make(pipelineRow, len(group.first)+len(projectedExpressions)+len(newRow))
			for name, value := range group.first {
				orderScope[name] = value
			}
			for expression, value := range projectedExpressions {
				orderScope[expression] = value
			}
			for name, value := range newRow {
				orderScope[name] = value
			}
			if postWithWhere != "" && !e.evaluateWithWhereCondition(ctx, postWithWhere, orderScope) {
				continue
			}
			out = append(out, newRow)
			orderScopes = append(orderScopes, orderScope)
		}
		if withDistinct {
			out, orderScopes = deduplicatePipelineRowsWithScopes(out, orderScopes, projectionAliases)
		}
		if !e.orderPipelineRowsWithScopes(ctx, out, orderScopes, orderTerms) {
			return nil, false
		}
		return e.filterPipelineWindow(ctx, out, orderScopes, withSkip, withLimit, windowedWhere), true
	}

	orderedExpressions := orderedProjectionExpressions(orderTerms, len(projections), func(index int) (string, string) {
		return projections[index].expr, projections[index].alias
	})
	out := make([]pipelineRow, 0, len(rows))
	needsOrderScopes := len(orderTerms) > 0 || withDistinct || windowedWhere != ""
	var orderScopes []pipelineRow
	if needsOrderScopes {
		orderScopes = make([]pipelineRow, 0, len(rows))
	}
	rowPlan := pipelineRowWith{where: postWithWhere}
	for _, projection := range projections {
		rowPlan.projections = append(rowPlan.projections, pipelineRowProjection{projection.expr, projection.alias})
	}
	for _, row := range rows {
		var scope pipelineRow
		if postWithWhere != "" || needsOrderScopes {
			scope = make(pipelineRow, len(row)+len(projections))
		}
		newRow, accepted, ok := e.pipelineProjectWithRow(ctx, row, rowPlan, nil, scope)
		if !ok {
			return nil, false
		}
		if !accepted {
			continue
		}
		out = append(out, newRow)
		if needsOrderScopes {
			// ORDER BY may name a projected expression by its text; its value
			// is the projection's (orderedProjectionExpressions). A projected
			// name keeps its own value.
			for _, ordered := range orderedExpressions {
				if _, projected := newRow[ordered.expression]; !projected {
					scope[ordered.expression] = newRow[ordered.alias]
				}
			}
			orderScopes = append(orderScopes, scope)
		}
	}
	if withDistinct {
		out, orderScopes = deduplicatePipelineRowsWithScopes(out, orderScopes, projectionAliases)
	}
	if !e.orderPipelineRowsWithScopes(ctx, out, orderScopes, orderTerms) {
		return nil, false
	}
	return e.filterPipelineWindow(ctx, out, orderScopes, withSkip, withLimit, windowedWhere), true
}

// filterPipelineWindow applies SKIP / LIMIT to rows, then keeps the rows of
// the window whose scope (the row's projected and incoming values) satisfies
// whereClause: the WHERE after WITH … SKIP / LIMIT.
func (e *StorageExecutor) filterPipelineWindow(ctx context.Context, rows, scopes []pipelineRow, skip, limit int, whereClause string) []pipelineRow {
	if whereClause == "" {
		return applyPipelineWindow(rows, skip, limit)
	}
	start, end := 0, len(rows)
	if skip > 0 {
		start = skip
		if start > end {
			start = end
		}
	}
	if limit >= 0 && start+limit < end {
		end = start + limit
	}
	filtered := make([]pipelineRow, 0, end-start)
	for index := start; index < end; index++ {
		if e.evaluateWithWhereCondition(ctx, whereClause, map[string]interface{}(scopes[index])) {
			filtered = append(filtered, rows[index])
		}
	}
	return filtered
}

func pipelinePaginationExpression(body, keyword string) string {
	index := topLevelKeywordIndex(body, keyword)
	if index < 0 {
		return ""
	}
	expression := strings.TrimSpace(body[index+len(keyword):])
	end := len(expression)
	// WITH … SKIP / LIMIT n WHERE p: the WHERE isn't part of n.
	for _, nextKeyword := range []string{"SKIP", "LIMIT", "WHERE"} {
		if nextIndex := topLevelKeywordIndex(expression, nextKeyword); nextIndex >= 0 && nextIndex < end {
			end = nextIndex
		}
	}
	return strings.TrimSpace(expression[:end])
}

func (e *StorageExecutor) evaluatePipelinePagination(ctx context.Context, expression string, rows []pipelineRow) (int, bool) {
	// SKIP / LIMIT see the statement's parameters ($l) whatever the
	// projection kept in the row.
	values := e.parameterRow(ctx)
	if len(rows) > 0 {
		for name, value := range rows[0] {
			values[name] = value
		}
	}
	value, evaluated := e.evaluateRowExpressionWithContext(ctx, expression, values)
	if !evaluated {
		return 0, false
	}
	integer, valid := cypherIntegerValue(value)
	if !valid || integer < 0 || int64(int(integer)) != integer {
		return 0, false
	}
	return int(integer), true
}

// orderPipelineRows applies every ORDER BY term lexicographically. WITH has
// already materialized its projection at this point, so aliases and retained
// entity properties resolve from the same scope exposed to the next clause.
func (e *StorageExecutor) orderPipelineRows(ctx context.Context, rows []pipelineRow, terms []orderByTerm) bool {
	return e.orderPipelineRowsWithScopes(ctx, rows, rows, terms)
}

// orderedProjection is a projection expression an ORDER BY term repeats, with
// the alias of the projected column.
type orderedProjection struct {
	expression string
	alias      string
}

// orderedProjectionExpressions returns the projection items (count of them,
// read by item) whose expression an ORDER BY term repeats, as in
// RETURN size(n.s) AS n ORDER BY size(n.s). Neo4j orders such a term by the
// projected column even when an alias shadows a variable of the expression,
// so the order scope maps the expression text to the projected value, as the
// aggregating WITH does. Nil when no term repeats one.
func orderedProjectionExpressions(terms []orderByTerm, count int, item func(index int) (expression, alias string)) []orderedProjection {
	var ordered []orderedProjection
	for _, term := range terms {
		column := strings.TrimSpace(term.column)
		for index := 0; index < count; index++ {
			expression, alias := item(index)
			if expression != alias && strings.TrimSpace(expression) == column {
				ordered = append(ordered, orderedProjection{expression: column, alias: alias})
				break
			}
		}
	}
	return ordered
}

// orderPipelineRowsWithScopes is orderPipelineRows with each row's terms
// evaluated in scopes[i]. A term the row evaluator can't resolve returns
// false; when an operator of it failed (1/0, a runtime TypeError) that failure
// is recorded as the statement's error, as for a projection item.
func (e *StorageExecutor) orderPipelineRowsWithScopes(ctx context.Context, rows, scopes []pipelineRow, terms []orderByTerm) bool {
	if len(terms) == 0 || len(rows) < 2 {
		return true
	}
	if len(rows) != len(scopes) {
		return false
	}
	type orderValue struct {
		values []interface{}
		row    pipelineRow
	}
	ordered := make([]orderValue, 0, len(rows))
	for index, row := range rows {
		values := make([]interface{}, len(terms))
		for termIndex, term := range terms {
			// The projections' evaluator: subquery values (COUNT { … }) and
			// recorded failures behave as in a RETURN item.
			value, ok := e.evaluateRowExpressionWithContext(ctx, term.column, scopes[index])
			if !ok {
				return false
			}
			values[termIndex] = value
		}
		ordered = append(ordered, orderValue{values: values, row: row})
	}
	sort.SliceStable(ordered, func(left, right int) bool {
		for index, term := range terms {
			comparison := compareValuesForSort(ordered[left].values[index], ordered[right].values[index])
			if comparison == 0 {
				continue
			}
			if term.descending {
				return comparison > 0
			}
			return comparison < 0
		}
		return false
	})
	for index := range rows {
		rows[index] = ordered[index].row
	}
	return true
}

func applyPipelineWindow(rows []pipelineRow, skip, limit int) []pipelineRow {
	if skip >= len(rows) {
		return []pipelineRow{}
	}
	if skip > 0 {
		rows = rows[skip:]
	}
	if limit >= 0 && limit < len(rows) {
		rows = rows[:limit]
	}
	return rows
}

func (e *StorageExecutor) filterPipelineRows(ctx context.Context, rows []pipelineRow, whereClause string) []pipelineRow {
	if whereClause == "" {
		return rows
	}
	filtered := rows[:0]
	for _, row := range rows {
		if e.evaluateWithWhereCondition(ctx, whereClause, map[string]interface{}(row)) {
			filtered = append(filtered, row)
		}
	}
	return filtered
}

// pipelineRowColumns returns the variables bound in rows, sorted: the
// columns of WITH *.
func pipelineRowColumns(rows []pipelineRow) []string {
	seen := make(map[string]struct{})
	columns := make([]string, 0)
	for _, row := range rows {
		for name := range row {
			if _, ok := seen[name]; !ok {
				seen[name] = struct{}{}
				columns = append(columns, name)
			}
		}
	}
	sort.Strings(columns)
	return columns
}

func deduplicatePipelineRows(rows []pipelineRow, columns []string) []pipelineRow {
	unique, _ := deduplicatePipelineRowsWithScopes(rows, rows, columns)
	return unique
}

func deduplicatePipelineRowsWithScopes(rows, scopes []pipelineRow, columns []string) ([]pipelineRow, []pipelineRow) {
	seen := make(map[string]struct{}, len(rows))
	unique := make([]pipelineRow, 0, len(rows))
	uniqueScopes := make([]pipelineRow, 0, len(rows))
	keys := make([]string, len(columns))
	for rowIndex, row := range rows {
		for i, column := range columns {
			keys[i] = cypherEquivalenceKey(row[column])
		}
		key := strings.Join(keys, "\x1f")
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		unique = append(unique, row)
		uniqueScopes = append(uniqueScopes, scopes[rowIndex])
	}
	return unique, uniqueScopes
}

// pipelineApplyUnwind evaluates the list expression (which may be a literal,
// a reference to a bound variable, or a bare property access) and produces
// one row per element.
func (e *StorageExecutor) pipelineApplyUnwind(ctx context.Context, rows []pipelineRow, clause string) ([]pipelineRow, bool) {
	out, _, ok := e.pipelineApplyUnwindPrefix(ctx, rows, []pipelineClause{{kind: pipelineClauseUnwind, text: clause}})
	return out, ok
}

func (e *StorageExecutor) pipelineApplyForeach(ctx context.Context, rows []pipelineRow, clause string) (*QueryStats, error) {
	invalid := func() error {
		return newSemanticError("Neo.ClientError.Statement.SyntaxError", "InvalidForeach", "invalid or unsupported FOREACH update")
	}
	variable, listExpr, updates, err := parsePipelineForeach(clause)
	if err != nil {
		return nil, err
	}
	stats := &QueryStats{}
	for _, row := range rows {
		items, ok := e.evaluateListForPipelineWithContext(ctx, listExpr, row)
		if !ok {
			return nil, invalid()
		}
		for _, item := range items {
			// The loop is a mutation boundary: a cancellation between items
			// stops the remaining writes (the MERGE branch probes too).
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			child := make(pipelineRow, len(row)+1)
			for name, value := range row {
				child[name] = value
			}
			child[variable] = item
			scope := make(map[string]struct{}, len(child))
			for name := range child {
				scope[name] = struct{}{}
			}
			result, handled, err := e.runPipelineClauses(ctx, []pipelineRow{child}, scope, updates, updates)
			if err != nil {
				return nil, err
			}
			if !handled {
				return nil, invalid()
			}
			addQueryStats(stats, result.Stats)
		}
	}
	return stats, nil
}

// parsePipelineAggregate recognizes the standard Cypher aggregate functions
// and separates their input expression from an optional DISTINCT modifier.
func parsePipelineAggregate(expr string) (name, inner string, distinct, ok bool) {
	// An aggregate is a function call: without a parenthesis there is none.
	if strings.IndexByte(expr, '(') < 0 || !isAggregateFunc(expr) {
		return "", "", false, false
	}
	open := strings.Index(expr, "(")
	if open < 0 {
		return "", "", false, false
	}
	name = lowerASCII(strings.TrimSpace(expr[:open]))
	inner = strings.TrimSpace(extractFuncInner(expr))
	inner, distinct = cutDistinctArgument(inner)
	if inner == "" {
		return "", "", false, false
	}
	return name, inner, distinct, true
}

func pipelineExpressionContainsAggregate(expr string) bool {
	if strings.IndexByte(expr, '(') < 0 {
		return false
	}
	return len(findAggregateSpans(strings.TrimSpace(expr))) > 0
}

func (e *StorageExecutor) evaluatePipelineAggregateExpression(rows []pipelineRow, expr string) (interface{}, bool) {
	return e.evaluatePipelineAggregateExpressionWithContext(context.Background(), rows, expr)
}

func (e *StorageExecutor) evaluatePipelineAggregateExpressionWithContext(ctx context.Context, rows []pipelineRow, expr string) (interface{}, bool) {
	expr = strings.TrimSpace(expr)
	if name, inner, distinct, ok := parsePipelineAggregate(expr); ok {
		return e.evaluatePipelineAggregateWithContext(ctx, rows, name, inner, distinct)
	}
	if inner, enclosed := stripEnclosingRowDelimiter(expr, '{', '}'); enclosed {
		result := make(map[string]interface{})
		if inner == "" {
			return result, true
		}
		for _, pair := range splitTopLevelComma(inner) {
			separator := findTopLevelMapKeyValueSeparator(pair)
			if separator <= 0 {
				return nil, false
			}
			key := normalizePropertyKey(strings.TrimSpace(pair[:separator]))
			value, ok := e.evaluatePipelineAggregateExpressionWithContext(ctx, rows, pair[separator+1:])
			if !ok {
				return nil, false
			}
			result[key] = value
		}
		return result, true
	}
	if inner, enclosed := stripEnclosingRowDelimiter(expr, '[', ']'); enclosed {
		if inner == "" {
			return []interface{}{}, true
		}
		if _, _, _, _, comprehension := parseListComprehension(inner); !comprehension {
			result := make([]interface{}, 0)
			for _, item := range splitTopLevelComma(inner) {
				value, ok := e.evaluatePipelineAggregateExpressionWithContext(ctx, rows, item)
				if !ok {
					return nil, false
				}
				result = append(result, value)
			}
			return result, true
		}
	}
	if spans := findAggregateSpans(expr); len(spans) > 0 {
		values := make(pipelineRow, len(spans))
		// Mixed aggregate expressions are evaluated after isolating aggregate
		// calls, but their non-aggregate terms still resolve against the group's
		// grouping row. Preserve that scope in the shared row evaluator instead
		// of adding operator-specific aggregate paths.
		if len(rows) > 0 {
			values = make(pipelineRow, len(rows[0])+len(spans))
			for name, value := range rows[0] {
				values[name] = value
			}
		}
		var rewritten strings.Builder
		last := 0
		for index, span := range spans {
			name, inner, distinct, ok := parsePipelineAggregate(expr[span.start:span.end])
			if !ok {
				return nil, false
			}
			value, ok := e.evaluatePipelineAggregateWithContext(ctx, rows, name, inner, distinct)
			if !ok {
				return nil, false
			}
			placeholder := traversalAggPlaceholder(index)
			rewritten.WriteString(expr[last:span.start])
			rewritten.WriteString(placeholder)
			values[placeholder] = value
			last = span.end
		}
		rewritten.WriteString(expr[last:])
		return e.evaluateRowExpressionWithContext(ctx, rewritten.String(), values)
	}
	if len(rows) == 0 {
		return e.evaluateRowExpressionWithContext(ctx, expr, pipelineRow{})
	}
	return e.evaluateRowExpressionWithContext(ctx, expr, rows[0])
}

// evaluatePipelineAggregate applies an aggregate to one logical group. Null
// inputs are ignored by every standard aggregate, including collect().
func (e *StorageExecutor) evaluatePipelineAggregate(rows []pipelineRow, name, expr string, distinct bool) (interface{}, bool) {
	return e.evaluatePipelineAggregateWithContext(context.Background(), rows, name, expr, distinct)
}

func (e *StorageExecutor) evaluatePipelineAggregateWithContext(ctx context.Context, rows []pipelineRow, name, expr string, distinct bool) (interface{}, bool) {
	if name == "percentilecont" || name == "percentiledisc" {
		return e.evaluatePipelinePercentile(ctx, rows, name, expr, distinct)
	}
	state := pipelineAggregateState{name: name, expression: expr, distinct: distinct}
	for _, row := range rows {
		if !state.add(ctx, e, row) {
			return nil, false
		}
	}
	return state.result(ctx, e)
}

func pipelineAggregateNumber(value interface{}) (float64, int64, bool, bool) {
	switch number := value.(type) {
	case int:
		return float64(number), int64(number), true, true
	case int8:
		return float64(number), int64(number), true, true
	case int16:
		return float64(number), int64(number), true, true
	case int32:
		return float64(number), int64(number), true, true
	case int64:
		return float64(number), number, true, true
	case uint:
		return float64(number), int64(number), true, true
	case uint8:
		return float64(number), int64(number), true, true
	case uint16:
		return float64(number), int64(number), true, true
	case uint32:
		return float64(number), int64(number), true, true
	case uint64:
		return float64(number), int64(number), true, true
	case float32:
		return float64(number), 0, false, true
	case float64:
		return number, 0, false, true
	default:
		return 0, 0, false, false
	}
}

// returnProjection is one RETURN item: its expression, its column name, and
// for an aggregating item the aggregate (aggregateName empty when the
// aggregate is nested in a larger expression, aggregateExpr then being the
// whole expression).
type returnProjection struct {
	expr          string
	alias         string
	isAggr        bool
	aggregateName string
	aggregateExpr string
	distinct      bool
}

// returnProjectionPlan is a RETURN clause parsed for pipelineApplyReturn:
// its items, columns, DISTINCT and trailing ORDER BY / SKIP / LIMIT. A plan
// depends only on the clause text, so it is parsed once per text
// (returnProjectionPlanFor). valid is false when the clause has no items.
type returnProjectionPlan struct {
	valid bool
	star  bool
	// starItems are the items after a leading * (RETURN *, x AS y, #883);
	// the * stands for every variable in scope, in name order.
	starItems    []string
	distinct     bool
	modifiers    string
	projections  []returnProjection
	columns      []string
	hasAggregate bool
}

// returnProjectionPlans caches parsed RETURN clauses by text. Plans are
// immutable once cached; the cache is cleared when it reaches
// returnProjectionPlanLimit entries, which bounds it for workloads with
// unbounded distinct query texts.
var returnProjectionPlans = struct {
	sync.RWMutex
	plans map[string]*returnProjectionPlan
}{plans: make(map[string]*returnProjectionPlan)}

const returnProjectionPlanLimit = 4096

func returnProjectionPlanFor(clause string) *returnProjectionPlan {
	returnProjectionPlans.RLock()
	plan, cached := returnProjectionPlans.plans[clause]
	returnProjectionPlans.RUnlock()
	if cached {
		return plan
	}
	plan = parseReturnProjectionPlan(clause)
	returnProjectionPlans.Lock()
	if len(returnProjectionPlans.plans) >= returnProjectionPlanLimit {
		returnProjectionPlans.plans = make(map[string]*returnProjectionPlan)
	}
	returnProjectionPlans.plans[clause] = plan
	returnProjectionPlans.Unlock()
	return plan
}

func parseReturnProjectionPlan(clause string) *returnProjectionPlan {
	// The clause is scanned with its keyword, which tells a keyword-named
	// first item from a clause (RETURN by ORDER BY by, #894).
	body := strings.TrimSpace(clause)
	modifierStart := len(body)
	if cut := firstTopLevelModifierIndex(body); cut >= 0 {
		modifierStart = cut
	}
	plan := &returnProjectionPlan{modifiers: strings.TrimSpace(body[modifierStart:])}
	body, plan.distinct = cutDistinct(pipelineClauseBody(body[:modifierStart], "RETURN"))
	items := splitTopLevelComma(body)
	if len(items) > 0 && strings.TrimSpace(items[0]) == "*" {
		plan.valid, plan.star = true, true
		for _, item := range items[1:] {
			plan.starItems = append(plan.starItems, strings.TrimSpace(item))
		}
		return plan
	}
	for _, rawItem := range items {
		// Semantic validation rejects an empty item (RETURN 1,,2).
		// Same alias parsing as WITH (parseProjectionExprAlias, #547).
		expr, alias := parseProjectionExprAlias(strings.TrimSpace(rawItem))
		plan.addProjection(expr, alias)
	}
	plan.valid = len(plan.projections) > 0
	return plan
}

// withStarExpanded is the plan of RETURN *, items with the * written out
// (starProjectionItems, #883).
func (plan *returnProjectionPlan) withStarExpanded(columns []string) *returnProjectionPlan {
	expanded := &returnProjectionPlan{valid: true, distinct: plan.distinct, modifiers: plan.modifiers}
	for _, item := range starProjectionItems(columns, plan.starItems) {
		expanded.addProjection(parseProjectionExprAlias(item))
	}
	return expanded
}

// pipelineOriginalReturnText is the text the statement gave the RETURN
// clause at idx, before rewrites, or "".
func pipelineOriginalReturnText(originalClauses []pipelineClause, idx int) string {
	if idx < len(originalClauses) && originalClauses[idx].kind == pipelineClauseReturn {
		return originalClauses[idx].text
	}
	return ""
}

// pipelineNameReturnColumns names final's columns. A RETURN * (with or
// without more items) that produced no rows lists the variables in scope, as
// Neo4j does, since no row carries them (#883). Otherwise the columns take
// the statement's own item texts (original), which rewrites may have changed.
func pipelineNameReturnColumns(final *ExecuteResult, clause, original string, scope map[string]struct{}) {
	if plan := returnProjectionPlanFor(clause); plan.star {
		if len(final.Rows) == 0 {
			final.Columns = plan.withStarExpanded(pipelineScopeColumns(scope)).columns
		}
		return
	}
	if original == "" {
		return
	}
	if columns := pipelineReturnSourceColumns(original); len(columns) == len(final.Columns) {
		final.Columns = columns
	}
}

// starProjectionItems writes out the * of `WITH *, items` or
// `RETURN *, items` (#883), as Neo4j orders the columns: each of columns, in
// the given (name) order, as a variable item, except a column one of items
// names (`RETURN *, a + 1 AS a` has the one column a), then items.
func starProjectionItems(columns, items []string) []string {
	named := make(map[string]bool, len(items))
	for _, item := range items {
		_, alias := parseProjectionExprAlias(item)
		named[alias] = true
	}
	out := make([]string, 0, len(columns)+len(items))
	for _, column := range columns {
		if !named[column] {
			variable := projectionVariableText(column)
			out = append(out, variable+" AS "+variable)
		}
	}
	return append(out, items...)
}

// projectionVariableText is a variable as an expression: backtick-quoted when
// it isn't a plain identifier.
func projectionVariableText(name string) string {
	if isSimpleIdentifier(name) && !strings.Contains(name, "`") {
		return name
	}
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

func (plan *returnProjectionPlan) addProjection(expr, alias string) {
	aggregateName, aggregateExpr, distinct, isAggr := parsePipelineAggregate(expr)
	if !isAggr && pipelineExpressionContainsAggregate(expr) {
		isAggr = true
		aggregateExpr = expr
	}
	plan.hasAggregate = plan.hasAggregate || isAggr
	plan.projections = append(plan.projections, returnProjection{expr: expr, alias: alias, isAggr: isAggr, aggregateName: aggregateName, aggregateExpr: aggregateExpr, distinct: distinct})
	plan.columns = append(plan.columns, alias)
}

func returnProjectionPlanFromItems(items []returnItem) *returnProjectionPlan {
	plan := &returnProjectionPlan{}
	for _, item := range items {
		alias := item.alias
		if alias == "" {
			alias = item.expr
		}
		plan.addProjection(item.expr, alias)
	}
	plan.valid = len(plan.projections) > 0
	return plan
}

// pipelineApplyReturn projects each binding row through the RETURN list.
// Supports:
//   - `count(*)` / `count(var)` (aggregate — collapses all rows to one)
//   - bare variable              (`RETURN node`)
//   - property access            (`RETURN m.name`)
//   - aliased forms              (`RETURN m.name AS probeName`)
//   - literal scalar             (`RETURN 42 AS answer`)
//
// Returns (nil, false) if any item can't be projected, so the caller falls
// back to the established RETURN projection.
func (e *StorageExecutor) pipelineApplyReturn(ctx context.Context, rows []pipelineRow, clause string) (*ExecuteResult, bool) {
	return e.pipelineApplyReturnSource(ctx, rows, clause, pipelineRowsSource(rows), false)
}

func (e *StorageExecutor) pipelineApplyReturnSource(ctx context.Context, rows []pipelineRow, clause string, source pipelineRowSource, rowsValidated bool, preparedGroups ...[]*pipelineAggregateGroup) (*ExecuteResult, bool) {
	return e.pipelineApplyReturnPlan(ctx, rows, returnProjectionPlanFor(clause), source, rowsValidated, preparedGroups...)
}

func (e *StorageExecutor) pipelineApplyReturnPlan(ctx context.Context, rows []pipelineRow, plan *returnProjectionPlan, source pipelineRowSource, rowsValidated bool, preparedGroups ...[]*pipelineAggregateGroup) (*ExecuteResult, bool) {
	if !plan.valid {
		return nil, false
	}
	if rows == nil && source != nil && (plan.star || (!plan.hasAggregate && (plan.modifiers != "" || plan.distinct))) {
		var materialized bool
		rows, materialized = materializePipelineSource(source)
		if !materialized {
			return nil, false
		}
	}
	modifiers, returnDistinct := plan.modifiers, plan.distinct
	if plan.star && len(plan.starItems) > 0 {
		return e.pipelineApplyReturnPlan(ctx, rows, plan.withStarExpanded(pipelineWildcardColumns(rows)), source, rowsValidated)
	}
	if plan.star {
		columns := pipelineWildcardColumns(rows)
		result := &ExecuteResult{Columns: columns, Rows: make([][]interface{}, 0, len(rows))}
		for _, row := range rows {
			projected := make([]interface{}, len(columns))
			for index, column := range columns {
				projected[index] = row[column]
			}
			result.Rows = append(result.Rows, projected)
		}
		if returnDistinct {
			result.Rows = deduplicatePipelineResultRows(result.Rows)
		}
		result, err := e.applyResultModifiers(ctx, result, modifiers)
		return result, err == nil
	}
	projs, hasAggregate := plan.projections, plan.hasAggregate
	result := &ExecuteResult{Columns: append([]string(nil), plan.columns...)}

	if hasAggregate {
		var groups []*pipelineAggregateGroup
		if len(preparedGroups) > 0 {
			groups = preparedGroups[0]
		} else {
			var ok bool
			groups, ok = e.pipelineAggregateGroups(ctx, source, projs, rowsValidated)
			if !ok {
				return nil, false
			}
		}

		for _, group := range groups {
			outRow := make([]interface{}, 0, len(projs))
			for index, projection := range projs {
				value, ok := group.value(ctx, e, index)
				if !ok {
					pipelineItemUnevaluable(ctx, projection.expr)
					return nil, false
				}
				outRow = append(outRow, value)
			}
			result.Rows = append(result.Rows, outRow)
		}
		if returnDistinct {
			result.Rows = deduplicatePipelineResultRows(result.Rows)
		}
		result, err := e.applyResultModifiers(ctx, result, modifiers)
		return result, err == nil
	}

	// Without DISTINCT, ORDER BY, SKIP or LIMIT the projected values are the
	// result rows: no per-row map or ORDER BY scope is needed.
	if modifiers == "" && !returnDistinct {
		result.Rows = make([][]interface{}, 0, len(rows))
		projected, completed := e.pipelineProjectPlainReturn(ctx, projs, rows, source, &result.Rows, nil)
		if !completed {
			return nil, false
		}
		if !projected {
			if failure := getExpressionFailure(ctx); failure == nil || newPipelineDispatchOutcome(nil, true, failure).state == pipelineDispatchParseRejected {
				return nil, false
			}
			return result, false
		}
		return result, true
	}

	// ORDER BY and DISTINCT see the incoming row with the projection over
	// it; the merged scope is built only when one of them needs it.
	orderTerms := parseOrderByTerms(modifiers)
	orderedExpressions := orderedProjectionExpressions(orderTerms, len(projs), func(index int) (string, string) {
		return projs[index].expr, projs[index].alias
	})
	needsOrderScopes := len(orderTerms) > 0 || returnDistinct
	projectedRows := make([]pipelineRow, 0, len(rows))
	var orderScopes []pipelineRow
	if needsOrderScopes {
		orderScopes = make([]pipelineRow, 0, len(rows))
	}
	for _, row := range rows {
		projected := make(pipelineRow, len(projs))
		for _, p := range projs {
			val, ok := e.evaluateRowExpressionWithContext(ctx, p.expr, row)
			if !ok {
				pipelineItemUnevaluable(ctx, p.expr)
				return nil, false
			}
			projected[p.alias] = val
		}
		projectedRows = append(projectedRows, projected)
		if !needsOrderScopes {
			continue
		}
		scope := make(pipelineRow, len(row)+len(projected)+len(orderedExpressions))
		for name, value := range row {
			scope[name] = value
		}
		for _, ordered := range orderedExpressions {
			scope[ordered.expression] = projected[ordered.alias]
		}
		for name, value := range projected {
			scope[name] = value
		}
		orderScopes = append(orderScopes, scope)
	}
	if returnDistinct {
		projectedRows, orderScopes = deduplicatePipelineRowsWithScopes(projectedRows, orderScopes, result.Columns)
	}
	if !e.orderPipelineRowsWithScopes(ctx, projectedRows, orderScopes, orderTerms) {
		return nil, false
	}
	skip := 0
	if value, ok := e.parseIntModifier(ctx, modifiers, "SKIP"); ok {
		skip = value
	}
	limit := -1
	if value, ok := e.parseIntModifier(ctx, modifiers, "LIMIT"); ok {
		limit = value
	}
	projectedRows = applyPipelineWindow(projectedRows, skip, limit)
	result.Rows = make([][]interface{}, 0, len(projectedRows))
	for _, projected := range projectedRows {
		outRow := make([]interface{}, len(result.Columns))
		for index, column := range result.Columns {
			outRow[index] = projected[column]
		}
		result.Rows = append(result.Rows, outRow)
	}
	return result, true
}

// pipelineProjectPlainReturn projects the rows of a RETURN without DISTINCT,
// ORDER BY, SKIP, LIMIT or aggregation: rows, or source's rows when rows is
// nil. Each projected row is appended to *out, or handed to emit when emit
// is set, which stops the projection by returning false. projected is false
// when a row could not be projected (the expression error, if any, is
// recorded); completed is false when source declined its shape.
func (e *StorageExecutor) pipelineProjectPlainReturn(ctx context.Context, projs []returnProjection, rows []pipelineRow, source pipelineRowSource, out *[][]interface{}, emit func([]interface{}) bool) (projected, completed bool) {
	if rows == nil && source != nil {
		// A local the closure captures, so the rows path below doesn't pay
		// for a captured result variable.
		sourceProjected := true
		sourceCompleted := source(func(row pipelineRow) bool {
			values, evaluated := e.pipelineProjectReturnRow(ctx, projs, row)
			if !evaluated {
				sourceProjected = false
				return false
			}
			if emit != nil {
				return emit(values)
			}
			*out = append(*out, values)
			return true
		})
		return sourceProjected, sourceCompleted
	}
	for _, row := range rows {
		values, evaluated := e.pipelineProjectReturnRow(ctx, projs, row)
		if !evaluated {
			return false, true
		}
		if emit == nil {
			*out = append(*out, values)
		} else if !emit(values) {
			break
		}
	}
	return true, true
}

// pipelineStreamReturn runs the statement's RETURN into stream (#939), for
// a RETURN streamsReturn takes: its rows go to the stream as they are
// projected. A RETURN that ends before the stream started returns its
// rows, as usual (nil after streaming). Its errors and declines are the
// usual RETURN's until the stream started (a decline is handled false and
// no error, pipelineDecline); after, a decline is an error, since the
// client already has rows and no other route may run the statement again.
func (e *StorageExecutor) pipelineStreamReturn(ctx context.Context, stream *ResultStream, rows []pipelineRow, source pipelineRowSource, clauses, originalClauses []pipelineClause, idx int, scope map[string]struct{}, wrote bool) (columns []string, out [][]interface{}, handled bool, err error) {
	clause := clauses[idx]
	plan := returnProjectionPlanFor(clause.text)
	// The statement only reads, so no row holds an entity it deleted
	// (validateDeletedEntityReads), and the RETURN doesn't aggregate
	// (validatePipelinePercentileArguments). The argument checks run on each
	// row as it arrives, as WITH's do: a row that fails them fails the
	// statement where the row is, as in Neo4j.
	if err := e.validatePipelineProjectionValues(rows, clause.text, "RETURN"); err != nil {
		return nil, nil, true, err
	}
	var validationErr error
	if rows == nil && source != nil && strings.ContainsAny(clause.text, "([") {
		input := source
		source = func(yield func(pipelineRow) bool) bool {
			return input(func(row pipelineRow) bool {
				if err := e.validatePipelineProjectionValues([]pipelineRow{row}, clause.text, "RETURN"); err != nil {
					validationErr = err
					return false
				}
				return yield(row)
			})
		}
	}
	named := &ExecuteResult{Columns: append([]string(nil), plan.columns...)}
	pipelineNameReturnColumns(named, clause.text, pipelineOriginalReturnText(originalClauses, idx), scope)
	stream.begin(named.Columns)
	stopped := false
	projected, completed := e.pipelineProjectPlainReturn(ctx, plan.projections, rows, source, nil, func(values []interface{}) bool {
		if !stream.emit(ctx, values) {
			stopped = true
			return false
		}
		return true
	})
	failure := validationErr
	if failure == nil && !stopped && !(projected && completed) {
		failure = getExpressionFailure(ctx)
	}
	if failure != nil {
		stream.fail()
	}
	buffered, streamed := stream.end()
	switch {
	case stopped:
		return nil, nil, true, ctx.Err()
	case validationErr != nil:
		stream.discard()
		return nil, nil, true, validationErr
	case projected && completed:
		if streamed {
			return named.Columns, nil, true, nil
		}
		if buffered == nil {
			buffered = [][]interface{}{}
		}
		return named.Columns, buffered, true, nil
	case !streamed:
		stream.discard()
		_, handled, err := pipelineDecline(ctx, wrote, clause.text)
		return nil, nil, handled, err
	}
	if failure != nil {
		return nil, nil, true, failure
	}
	return nil, nil, true, localizedError(localization.CypherInvariantsPipelineDeclinedAfterStreaming(clause.text), nil)
}

// streamableReturnPlan reports whether a RETURN's rows can go to a result
// stream as they are projected: no DISTINCT, ORDER BY, SKIP, LIMIT,
// aggregation or *.
func streamableReturnPlan(plan *returnProjectionPlan) bool {
	return plan.valid && !plan.star && !plan.hasAggregate && !plan.distinct && plan.modifiers == ""
}

// streamsReturn reports whether clauses[idx] is a RETURN the statement's
// result stream takes (pipelineStreamReturn), so its input may stay a row
// source.
func streamsReturn(ctx context.Context, clauses []pipelineClause, idx int) bool {
	return clauses[idx].kind == pipelineClauseReturn && boundResultStream(ctx, &clauses[idx]) != nil &&
		streamableReturnPlan(returnProjectionPlanFor(clauses[idx].text))
}

func deduplicatePipelineResultRows(rows [][]interface{}) [][]interface{} {
	seen := make(map[string]struct{}, len(rows))
	unique := make([][]interface{}, 0, len(rows))
	keys := make([]string, 0)
	for _, row := range rows {
		if cap(keys) < len(row) {
			keys = make([]string, len(row))
		} else {
			keys = keys[:len(row)]
		}
		for index, value := range row {
			keys[index] = cypherEquivalenceKey(value)
		}
		key := strings.Join(keys, "\x1f")
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		unique = append(unique, row)
	}
	return unique
}

func pipelineWildcardColumns(rows []pipelineRow) []string {
	seen := make(map[string]struct{})
	for _, row := range rows {
		for column := range row {
			if strings.HasPrefix(column, "$") || isGeneratedVariable(column) {
				continue
			}
			seen[column] = struct{}{}
		}
	}
	columns := make([]string, 0, len(seen))
	for column := range seen {
		columns = append(columns, column)
	}
	sort.Strings(columns)
	return columns
}

func pipelineScopeColumns(scope map[string]struct{}) []string {
	columns := make([]string, 0, len(scope))
	for column := range scope {
		if !strings.HasPrefix(column, "$") && !isGeneratedVariable(column) {
			columns = append(columns, column)
		}
	}
	sort.Strings(columns)
	return columns
}

// projectFromRow resolves a RETURN / WITH expression against a single
// binding row. Returns (value, true) on success, (nil, false) otherwise.
func projectFromRow(row pipelineRow, expr string) (interface{}, bool) {
	expr = strings.TrimSpace(expr)
	if val, ok := row[expr]; ok {
		return val, true
	}
	upperExpr := upperASCII(expr)
	if strings.HasPrefix(upperExpr, "SIZE(") && strings.HasSuffix(expr, ")") {
		value, ok := projectFromRow(row, strings.TrimSpace(expr[len("size("):len(expr)-1]))
		if !ok {
			return nil, false
		}
		return int64(len(toAnySlice(value))), true
	}
	if dot := strings.Index(expr, "."); dot > 0 {
		base := strings.TrimSpace(expr[:dot])
		field := strings.TrimSpace(expr[dot+1:])
		if baseVal, ok := row[base]; ok {
			if node, isNode := baseVal.(*storage.Node); isNode && node != nil {
				return node.Properties[field], true
			}
			if edge, isEdge := baseVal.(*storage.Edge); isEdge && edge != nil {
				return edge.Properties[field], true
			}
			if m, isMap := toStringAnyMap(baseVal); isMap {
				return m[field], true
			}
		}
	}
	if v, ok := parseLiteralScalarForPipeline(expr); ok {
		return v, true
	}
	return nil, false
}

// ---- helpers ----

// referencesVariable returns true if the query text refers to the variable
// `name` outside string literals and outside property-access positions.
func referencesVariable(query, name string) bool {
	if name == "" {
		return false
	}
	// Use the existing identifier-aware scanner by replacing with a sentinel
	// and checking for a diff.
	const sentinel = "\x00"
	replaced := replaceIdentifierOutsideQuotes(query, name, sentinel)
	return strings.Contains(replaced, sentinel)
}

// evaluateListForPipeline evaluates a list expression against a binding row.
// Supports three forms:
//  1. Bare variable:  UNWIND items AS x
//  2. Property access: UNWIND row.products AS prodRef
//  3. Literal list:   UNWIND [{...}, {...}] AS x  (already a literal)
//
// A value that isn't a list is one element (coerceToUnwindItems).
//
// Returns nil if the expression can't be evaluated.
func evaluateListForPipeline(expr string, row pipelineRow) []interface{} {
	items, _ := evaluateStaticListForPipeline(expr, row)
	return items
}

// evaluateStaticListForPipeline evaluates an UNWIND list that is a row
// variable, a property of one, or a literal list, without the row evaluator.
// Its value is coerced as UNWIND coerces any value (coerceToUnwindItems).
func evaluateStaticListForPipeline(expr string, row pipelineRow) ([]interface{}, bool) {
	expr = strings.TrimSpace(expr)
	// Bare variable.
	if val, ok := row[expr]; ok {
		return coerceToUnwindItems(val), true
	}
	// Property access (a.b).
	if dot := strings.Index(expr, "."); dot > 0 {
		base := strings.TrimSpace(expr[:dot])
		field := strings.TrimSpace(expr[dot+1:])
		if baseVal, ok := row[base]; ok {
			if asMap, ok := toStringAnyMap(baseVal); ok {
				if v, ok := asMap[field]; ok {
					return coerceToUnwindItems(v), true
				}
			}
			if node, ok := baseVal.(*storage.Node); ok && node != nil {
				return coerceToUnwindItems(node.Properties[field]), true
			}
		}
	}
	// Literal list — parse via an ad-hoc evaluator. The simplest reliable
	// thing to do is wrap the literal and let the storage executor parse it
	// as a value. Without plumbing a full parser here we only accept the
	// `[...]` form and split top-level items.
	if strings.HasPrefix(expr, "[") && strings.HasSuffix(expr, "]") {
		parsed, ok := parseLiteralValueForPipeline(expr)
		if !ok {
			return nil, false
		}
		return coerceToUnwindItems(parsed), true
	}
	return nil, false
}

func (e *StorageExecutor) evaluateListForPipelineWithContext(ctx context.Context, expr string, row pipelineRow) ([]interface{}, bool) {
	if inner, wrapped := stripEnclosingExpressionParentheses(strings.TrimSpace(expr)); wrapped {
		expr = inner
	}
	if mayContainSubqueryExpression(expr) {
		if value, ok := e.evaluateRowExpressionWithContext(ctx, expr, row); ok {
			return coerceToUnwindItems(value), true
		}
	}
	if items, ok := evaluateStaticListForPipeline(expr, row); ok {
		return items, true
	}
	if matchFuncStartAndSuffix(expr, "range") {
		args := e.splitFunctionArgs(extractFuncArgs(expr, "range"))
		if len(args) < 2 || len(args) > 3 {
			return nil, false
		}
		arguments := make([]interface{}, len(args))
		for index, argument := range args {
			value, resolved := e.evaluateRowExpressionWithContext(ctx, strings.TrimSpace(argument), row)
			if !resolved {
				return nil, false
			}
			arguments[index] = value
		}
		items, err := evaluateCypherRange(arguments)
		if err != nil {
			recordExpressionFailure(ctx, err)
			return nil, false
		}
		return items, true
	}
	if value, ok := e.evaluateRowExpressionWithContext(ctx, expr, row); ok {
		return coerceToUnwindItems(value), true
	}

	materialized := expr
	for name, value := range row {
		materialized = replaceIdentifierOutsideQuotes(materialized, name, e.valueToLiteral(value))
	}
	value := e.evaluateExpressionWithContext(ctx, materialized, nil, nil)
	if text, unresolved := value.(string); unresolved && text == materialized && !isWholeCypherQuotedString(materialized) {
		return nil, false
	}
	if value == nil && !strings.EqualFold(strings.TrimSpace(materialized), "null") && !looksLikeFunctionCall(materialized) {
		return nil, false
	}
	return coerceToUnwindItems(value), true
}

func toAnySlice(v interface{}) []interface{} {
	switch s := v.(type) {
	case []interface{}:
		return s
	case []map[string]interface{}:
		out := make([]interface{}, len(s))
		for i, m := range s {
			out[i] = m
		}
		return out
	case []string:
		out := make([]interface{}, len(s))
		for i := range s {
			out[i] = s[i]
		}
		return out
	case []int:
		out := make([]interface{}, len(s))
		for i := range s {
			out[i] = int64(s[i])
		}
		return out
	case []int64:
		out := make([]interface{}, len(s))
		for i := range s {
			out[i] = s[i]
		}
		return out
	case []float64:
		out := make([]interface{}, len(s))
		for i := range s {
			out[i] = s[i]
		}
		return out
	case []float32:
		out := make([]interface{}, len(s))
		for i := range s {
			out[i] = float64(s[i])
		}
		return out
	case []bool:
		out := make([]interface{}, len(s))
		for i := range s {
			out[i] = s[i]
		}
		return out
	}
	rv := reflect.ValueOf(v)
	if !rv.IsValid() || (rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array) {
		return nil
	}
	out := make([]interface{}, rv.Len())
	for i := 0; i < rv.Len(); i++ {
		out[i] = rv.Index(i).Interface()
	}
	return out
}

func parseLiteralMapForPipeline(s string) map[string]interface{} {
	s = strings.TrimSpace(s)
	if !strings.HasPrefix(s, "{") || !strings.HasSuffix(s, "}") {
		return nil
	}
	inner := strings.TrimSpace(s[1 : len(s)-1])
	if inner == "" {
		return map[string]interface{}{}
	}
	pairs := splitTopLevelComma(inner)
	out := make(map[string]interface{}, len(pairs))
	for _, pair := range pairs {
		colon := findTopLevelMapKeyValueSeparator(pair)
		if colon <= 0 {
			return nil
		}
		k := normalizePropertyKey(strings.TrimSpace(pair[:colon]))
		vRaw := strings.TrimSpace(pair[colon+1:])
		v, ok := parseLiteralValueForPipeline(vRaw)
		if !ok {
			return nil
		}
		out[k] = v
	}
	return out
}

func parseLiteralListForPipeline(s string) ([]interface{}, bool) {
	s = strings.TrimSpace(s)
	if !strings.HasPrefix(s, "[") || !strings.HasSuffix(s, "]") {
		return nil, false
	}
	inner := strings.TrimSpace(s[1 : len(s)-1])
	if inner == "" {
		return []interface{}{}, true
	}
	parts := splitTopLevelComma(inner)
	out := make([]interface{}, 0, len(parts))
	for _, part := range parts {
		v, ok := parseLiteralValueForPipeline(part)
		if !ok {
			return nil, false
		}
		out = append(out, v)
	}
	return out, true
}

func parseLiteralValueForPipeline(s string) (interface{}, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, false
	}
	if strings.HasPrefix(s, "{") && strings.HasSuffix(s, "}") {
		m := parseLiteralMapForPipeline(s)
		if m == nil {
			return nil, false
		}
		return m, true
	}
	if strings.HasPrefix(s, "[") && strings.HasSuffix(s, "]") {
		return parseLiteralListForPipeline(s)
	}
	return parseLiteralScalarForPipeline(s)
}

func parseLiteralScalarForPipeline(s string) (interface{}, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, false
	}
	// Quoted string.
	if isWholeCypherQuotedString(s) {
		return decodeCypherQuotedString(s)
	}
	// true, false, null, NaN, Infinity.
	if value, ok := literalKeywordValue(s); ok {
		return value, true
	}
	// Int.
	if i, ok := parseIntFast(s); ok {
		return i, true
	}
	// Float.
	if f, ok := parseFloatFast(s); ok {
		return f, true
	}
	return nil, false
}

func parseIntFast(s string) (int64, bool) {
	if s == "" {
		return 0, false
	}
	negative := false
	digits := 0
	if s[0] == '-' {
		negative = true
		digits = 1
	} else if s[0] == '+' {
		digits = 1
	}
	if digits == len(s) {
		return 0, false
	}
	base := 10
	if digits+2 <= len(s) && s[digits] == '0' {
		switch s[digits+1] {
		case 'x', 'X':
			base = 16
			digits += 2
		case 'o', 'O':
			base = 8
			digits += 2
		}
	}
	if base == 10 {
		// Only digits can follow the sign; checking first keeps text that
		// isn't a number (n.name, e.uuid) from building a parse error.
		for i := digits; i < len(s); i++ {
			if s[i] < '0' || s[i] > '9' {
				return 0, false
			}
		}
		value, err := strconv.ParseInt(s, 10, 64)
		return value, err == nil
	}
	if digits == len(s) {
		return 0, false
	}
	magnitude, err := strconv.ParseUint(s[digits:], base, 64)
	if err != nil || numericMagnitudeOverflowsInt64(magnitude, negative) {
		return 0, false
	}
	if !negative {
		return int64(magnitude), true
	}
	if magnitude == uint64(math.MaxInt64)+1 {
		return math.MinInt64, true
	}
	return -int64(magnitude), true
}

// startsLikeDecimalNumber reports whether s can be a decimal number: after an
// optional sign, a digit, or a '.' followed by a digit. Text that can't be one
// (e.uuid, n.name) is rejected before strconv builds a parse error for it.
func startsLikeDecimalNumber(s string) bool {
	i := 0
	if i < len(s) && (s[i] == '-' || s[i] == '+') {
		i++
	}
	if i < len(s) && s[i] == '.' {
		i++
	}
	return i < len(s) && s[i] >= '0' && s[i] <= '9'
}

func parseFloatFast(s string) (float64, bool) {
	if !strings.ContainsAny(s, ".eE") || !startsLikeDecimalNumber(s) {
		return 0, false
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, false
	}
	return f, true
}

// precededByOptionalKeyword reports whether the clause keyword at position in
// cypher follows the keyword OPTIONAL (OPTIONAL MATCH, OPTIONAL CALL), not a
// variable named optional. It doesn't allocate.
func precededByOptionalKeyword(cypher string, position int) bool {
	end := len(strings.TrimRight(cypher[:position], " \t\n\r"))
	return end >= len("OPTIONAL") && equalFoldASCII(cypher[end-len("OPTIONAL"):end], "OPTIONAL") &&
		!clauseKeywordUsedAsName(cypher, end-len("OPTIONAL"), end, "OPTIONAL")
}

// setSourceLiteralTypeError rejects SET x = <source> and SET x += <source>
// (property "") whose source is a literal of a type other than a map: Neo4j
// 5.26 types it before the statement runs ("Type mismatch: expected Map, Node
// or Relationship"). A null source passes here and is a TypeError when the
// SET runs (setPropertyMapValue), as in Neo4j (#907).
func setSourceLiteralTypeError(property, operator, source string) error {
	if property != "" || (operator != "=" && operator != "+=") {
		return nil
	}
	switch typeName := staticLiteralTypeName(source); typeName {
	case "", "Map", "Null":
		return nil
	default:
		return typeNameMismatchError("Map, Node or Relationship", typeName)
	}
}
