package fabric

// Author: declared-list-owners lane (Claude), 2026-10-30: one join projection result header.

import (
	"context"
	"encoding/binary"
	"fmt"
	math "github.com/orneryd/nornicdb/pkg/math/libm"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/orneryd/nornicdb/pkg/cypher/resultmetadata"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/pkg/util"
)

// FabricExecutor walks a Fragment tree and dispatches each FragmentExec
// to either a local or remote executor based on the graph location resolved
// from the Catalog.
//
// This mirrors Neo4j's FabricExecutor.java.
type FabricExecutor struct {
	catalog *Catalog
	local   *LocalFragmentExecutor
	remote  *RemoteFragmentExecutor
	// Explicit local participants and all remote transaction handles share mutable
	// execution state. Dispatch owns that boundary for UNION and APPLY fan-out.
	serializeExec bool
	execMu        sync.Mutex

	evaluateGraphArguments GraphArgumentEvaluator
	authorizeGraph         func(context.Context, string, string) error
	remoteStatementPolicy  func(context.Context, string) (RemoteStatementPolicy, error)
	union                  UnionContract
	resultColumns          resultmetadata.Provider
	codec                  RemoteValueCodec
}

// RemoteValueCodec is the Cypher executor's view of the values a remote
// participant exchanges with it, for Bolt and HTTP alike. Values reach a
// participant as statement parameters, which the remote transport sends as
// their Cypher types (storage.RemoteParameterValue).
type RemoteValueCodec struct {
	// Import returns an error for a row value no remote participant may
	// import, such as a node (Neo4j's 22N16).
	Import func(value interface{}) error
	// Decode returns the value a remote participant's result value denotes.
	Decode func(value interface{}) (interface{}, error)
}

// SetRemoteValueCodec installs the Cypher executor's remote value codec.
func (e *FabricExecutor) SetRemoteValueCodec(codec RemoteValueCodec) {
	e.codec = codec
}

// UnionContract is the Cypher executor's UNION semantics for the branches
// Fabric combines, so a composite UNION returns what the same UNION returns
// on one graph. Unset, branches combine by position and DISTINCT by row hash.
type UnionContract struct {
	// ColumnOrder reports whether branch returns the same column names as
	// columns and, when their order differs, the index in branch of each.
	ColumnOrder func(columns, branch []string) (order []int, sameNames bool)
	// ColumnsError is the error for a branch returning other column names.
	ColumnsError func(columns, branch []string) error
	// NewDistinct returns the add of an empty row set: add reports whether an
	// equivalent row was already added.
	NewDistinct func() func(row []interface{}) bool
}

// SetUnionContract installs the UNION semantics every UNION route applies.
func (e *FabricExecutor) SetUnionContract(contract UnionContract) {
	e.union = contract
}

// SetConcurrentExecution configures whether fragments may execute concurrently.
// Explicit transactions use serial dispatch because their local executors are
// shared. Remote dispatch always serializes: autocommit statements also share
// remote transaction handles across their branches.
func (e *FabricExecutor) SetConcurrentExecution(concurrent bool) {
	e.serializeExec = !concurrent
}

const (
	// Keep Fabric-side in-memory APPLY rewrites bounded; above this, fall back
	// to correlated execution to avoid large intermediate allocations.
	fabricApplyInMemoryMaxRows = 1024
	// Chunk correlated batched collect lookups so key arrays never grow unbounded.
	fabricApplyLookupBatchSize = 512
	// Bound async fan-out for read-only batched APPLY lookups.
	fabricApplyLookupMaxConcurrency = 8
)

// NewFabricExecutor creates a fabric executor.
func NewFabricExecutor(catalog *Catalog, local *LocalFragmentExecutor, remote *RemoteFragmentExecutor) *FabricExecutor {
	executor := &FabricExecutor{
		catalog: catalog,
		local:   local,
		remote:  remote,
	}
	if local != nil {
		provider, _ := local.cypherExec.(resultmetadata.Provider)
		executor.SetResultColumnProvider(provider)
	}
	return executor
}

// SetResultColumnProvider supplies the Cypher owner's cache to standalone
// dispatchers. Actual stream fields remain authoritative.
func (e *FabricExecutor) SetResultColumnProvider(provider resultmetadata.Provider) {
	e.resultColumns = provider
}

func (e *FabricExecutor) restoreCachedResultColumns(ctx context.Context, result *ResultStream, query string, bindings map[string]interface{}) {
	if result == nil || result.Columns != nil || e.resultColumns == nil {
		return
	}
	incoming := make([]string, 0, len(bindings))
	for name := range bindings {
		incoming = append(incoming, name)
	}
	e.resultColumns.CachedResultColumns(ctx, query, incoming).Visit(func(names []string) { result.Columns = names }, func() {})
}

// Execute runs a Fragment tree within the context of a FabricTransaction.
//
// Parameters:
//   - ctx: context for cancellation/deadline propagation
//   - tx: the distributed transaction (may be nil for auto-commit)
//   - fragment: the root of the Fragment tree to execute
//   - params: query parameters
//   - authToken: the caller's auth token for OIDC forwarding to remote shards
func (e *FabricExecutor) Execute(ctx context.Context, tx *FabricTransaction, fragment Fragment, params map[string]interface{}, authToken string) (*ResultStream, error) {
	if tx != nil {
		tx.BindContext(ctx)
	}
	switch f := fragment.(type) {
	case *FragmentInit:
		return e.executeInit(f)
	case *FragmentLeaf:
		return nil, fmt.Errorf("FragmentLeaf should be resolved to FragmentExec before execution")
	case *FragmentExec:
		return e.executeExec(ctx, tx, f, params, authToken)
	case *FragmentApply:
		return e.executeApply(ctx, tx, f, params, authToken)
	case *FragmentUnion:
		return e.executeUnion(ctx, tx, f, params, authToken)
	default:
		return nil, fmt.Errorf("unknown fragment type: %T", fragment)
	}
}

// executeInit produces a single empty row — the entry point of the Fragment tree.
func (e *FabricExecutor) executeInit(f *FragmentInit) (*ResultStream, error) {
	// A single empty row with no columns, matching Neo4j's Init semantics.
	return &ResultStream{
		Columns: f.Columns,
		Rows:    [][]interface{}{{}},
	}, nil
}

// executeExec dispatches a bound executable fragment to local or remote.
func (e *FabricExecutor) executeExec(ctx context.Context, tx *FabricTransaction, f *FragmentExec, params map[string]interface{}, authToken string) (*ResultStream, error) {
	result, _, err := e.dispatchExec(ctx, tx, f, params, authToken)
	return result, err
}

// dispatchExec owns query execution for both materialized and iterator callers.
// Local and remote row APIs already collect their results before returning an
// iterator; keeping that dispatch here also keeps transaction state serialized.
func (e *FabricExecutor) dispatchExec(ctx context.Context, tx *FabricTransaction, f *FragmentExec, params map[string]interface{}, authToken string) (*ResultStream, int, error) {
	if e.serializeExec {
		e.execMu.Lock()
		defer e.execMu.Unlock()
	}
	loc, execParams, ctx, err := e.prepareExecDispatch(ctx, tx, f, params)
	if err != nil {
		return nil, 0, err
	}
	if _, remote := loc.(*LocationRemote); remote && !e.serializeExec {
		e.execMu.Lock()
		defer e.execMu.Unlock()
	}
	recordBindings, _ := RecordBindingsFromContext(ctx)

	switch l := loc.(type) {
	case *LocationLocal:
		if e.local == nil {
			return nil, 0, fmt.Errorf("local executor not configured")
		}
		query, localParams, scoped := f.Query, execParams, []string(nil)
		if len(f.ScopeImports) > 0 {
			query, localParams, scoped, err = e.importStatement(f, execParams, recordBindings, nil)
			if err != nil {
				return nil, 0, err
			}
		}
		res, err := e.local.ExecuteWithRecord(ctx, l, query, localParams, recordBindings)
		if err != nil {
			return nil, 0, err
		}
		if res != nil && len(scoped) > 0 {
			dropResultColumns(res, scoped)
		}
		e.restoreCachedResultColumns(ctx, res, f.Query, recordBindings)
		return res, 256, nil

	case *LocationRemote:
		if e.remote == nil {
			return nil, 0, fmt.Errorf("remote executor not configured")
		}
		query, remoteParams, scoped, err := e.remoteImportStatement(f, params, recordBindings)
		if err != nil {
			return nil, 0, err
		}
		policy := RemoteStatementPolicy{AccessMode: storage.RemoteAccessWrite, Uncertain: true}
		if e.remoteStatementPolicy != nil {
			policy, err = e.remoteStatementPolicy(ctx, query)
			if err != nil {
				return nil, 0, err
			}
		}
		if policy.Uncertain {
			ctx = WithUnresolvedRemoteMode(ctx)
		}
		res, err := e.remote.Execute(ctx, l, query, remoteParams, authToken, policy.AccessMode)
		if err != nil {
			return nil, 0, err
		}
		if res != nil && len(scoped) > 0 {
			dropResultColumns(res, scoped)
		}
		e.restoreCachedResultColumns(ctx, res, f.Query, recordBindings)
		if res != nil && e.codec.Decode != nil {
			for _, row := range res.Rows {
				for i, value := range row {
					if row[i], err = e.codec.Decode(value); err != nil {
						return nil, 0, err
					}
				}
			}
		}
		return res, 128, nil

	default:
		return nil, 0, fmt.Errorf("unsupported location type: %T", loc)
	}
}

func (e *FabricExecutor) executeRows(ctx context.Context, tx *FabricTransaction, fragment Fragment, params map[string]interface{}, authToken string) ([]string, RowIterator, error) {
	switch f := fragment.(type) {
	case *FragmentInit:
		res, err := e.executeInit(f)
		if err != nil {
			return nil, nil, err
		}
		return res.Columns, wrapPipelineIterator(ctx, NewResultRowIterator(res), 128), nil
	case *FragmentExec:
		res, prefetch, err := e.dispatchExec(ctx, tx, f, params, authToken)
		columns, rows, err := resultRows(res, err)
		if err != nil {
			return nil, nil, err
		}
		return columns, wrapPipelineIterator(ctx, rows, prefetch), nil
	case *FragmentUnion:
		return e.executeUnionRows(ctx, tx, f, params, authToken)
	default:
		res, err := e.Execute(ctx, tx, fragment, params, authToken)
		columns, iterator, err := resultRows(res, err)
		if err != nil {
			return nil, nil, err
		}
		rows := wrapPipelineIterator(ctx, iterator, 64)
		if res != nil && res.RowWrapper {
			rows = rowWrapperRows{rows}
		}
		return columns, rows, nil
	}
}

// rowWrapperRows carries the RowWrapper mark of a result through the row
// iterators APPLY inputs and UNION branches are read with.
type rowWrapperRows struct{ RowIterator }

// isRowWrapperRows reports whether it is the rows of a RowWrapper result.
func isRowWrapperRows(it RowIterator) bool {
	_, wrapper := it.(rowWrapperRows)
	return wrapper
}

func wrapPipelineIterator(ctx context.Context, it RowIterator, prefetch int) RowIterator {
	if it == nil {
		it = NewResultRowIterator(nil)
	}
	// Keep conversion lazy (executed only on row consumption) and isolate
	// downstream operators from row-buffer reuse by any underlying iterator.
	it = NewConvertingRowIterator(it, func(row []interface{}) []interface{} {
		if len(row) == 0 {
			return nil
		}
		out := make([]interface{}, len(row))
		copy(out, row)
		return out
	})
	return NewPrefetchRowIterator(ctx, it, prefetch)
}

// prepareExecDispatch resolves the graph a fragment runs on
// (resolveExecGraph), preserves its catalog name for selection checks, joins the distributed
// transaction, and merges the row's variables into the parameters.
func (e *FabricExecutor) prepareExecDispatch(ctx context.Context, tx *FabricTransaction, f *FragmentExec, params map[string]interface{}) (Location, map[string]interface{}, context.Context, error) {
	recordBindings, _ := RecordBindingsFromContext(ctx)
	execParams := params
	if len(recordBindings) > 0 {
		execParams = make(map[string]interface{}, util.SafePreallocSum(len(params), len(recordBindings)))
		for k, v := range params {
			execParams[k] = v
		}
		for k, v := range recordBindings {
			execParams[k] = v
		}
	}
	graph, loc, err := e.resolveExecGraph(ctx, f, execParams)
	if err != nil {
		return nil, nil, ctx, err
	}
	if e.authorizeGraph != nil {
		if err := e.authorizeGraph(ctx, graph, f.Query); err != nil {
			return nil, nil, ctx, err
		}
	}
	ctx = context.WithValue(ctx, graphSelectionContextKey{}, graph)
	if tx != nil {
		participant := ParticipantKeyFromLocation(loc)
		// Opening a participant does not mean it changed anything. Its storage
		// adapter claims the writer when the first mutation is staged.
		sub, err := tx.GetOrOpen(participant, false)
		if err != nil {
			return nil, nil, ctx, err
		}
		ctx = WithFabricTransaction(ctx, tx)
		ctx = WithSubTransaction(ctx, sub)
	}
	return loc, execParams, ctx, nil
}

// SetGraphAuthorizer installs the statement owner's selection check. Dispatch
// calls it with the original catalog name before opening a participant.
func (e *FabricExecutor) SetGraphAuthorizer(authorize func(context.Context, string, string) error) {
	e.authorizeGraph = authorize
}

func materializeIterator(columns []string, it RowIterator) (*ResultStream, error) {
	if it == nil {
		return &ResultStream{Columns: append([]string(nil), columns...)}, nil
	}
	defer func() { _ = it.Close() }()
	rows := make([][]interface{}, 0, 64)
	for it.Next() {
		row := it.Row()
		copied := make([]interface{}, len(row))
		copy(copied, row)
		rows = append(rows, copied)
	}
	if err := it.Err(); err != nil {
		return nil, err
	}
	return &ResultStream{
		Columns:    append([]string(nil), columns...),
		Rows:       rows,
		RowWrapper: isRowWrapperRows(it),
	}, nil
}

func lastKeywordIndexFold(s, keyword string) int {
	if len(keyword) == 0 || len(s) < len(keyword) {
		return -1
	}
	for i := len(s) - len(keyword); i >= 0; i-- {
		if keywordAt(s, i, keyword) {
			return i
		}
	}
	return -1
}

func lastAsIndexFold(s string) int {
	if len(s) < 4 {
		return -1
	}
	for i := len(s) - 4; i >= 0; i-- {
		if strings.EqualFold(s[i:i+4], " AS ") {
			return i
		}
	}
	return -1
}

func splitTopLevelCSV(s string) []string {
	var parts []string
	start := 0
	paren, bracket, brace := 0, 0, 0
	for i := 0; i < len(s); i++ {
		if next, quoted := cyphertext.SkipToken(s, i); quoted {
			i = next - 1
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
		case ',':
			if paren == 0 && bracket == 0 && brace == 0 {
				parts = append(parts, s[start:i])
				start = i + 1
			}
		}
	}
	parts = append(parts, s[start:])
	return parts
}

// ParticipantKeyFromLocation returns the transaction identity for a resolved location.
// Local locations share an identity by database name, so composite routing and
// ordinary fragment dispatch join the same local transaction participant.
func ParticipantKeyFromLocation(loc Location) string {
	switch l := loc.(type) {
	case *LocationLocal:
		return "local:" + strings.TrimSpace(l.DBName)
	case *LocationRemote:
		if l.GraphName != "" {
			return "remote:" + l.GraphName
		}
		return "remote:" + strings.TrimSpace(l.URI) + "|" + strings.TrimSpace(l.DBName)
	default:
		return fmt.Sprintf("unknown:%T", loc)
	}
}

// executeApply implements correlated subquery semantics:
// for each row from Input, execute Inner with imported variables.
func (e *FabricExecutor) executeApply(ctx context.Context, tx *FabricTransaction, f *FragmentApply, params map[string]interface{}, authToken string) (*ResultStream, error) {
	// Stream rows from the outer fragment and only materialize when an APPLY
	// batch rewrite requires full-row inspection.
	inputCols, inputIter, err := e.executeRows(ctx, tx, f.Input, params, authToken)
	if err != nil {
		return nil, fmt.Errorf("apply input failed: %w", err)
	}
	inputWrapper := isRowWrapperRows(inputIter)
	if inputIter == nil {
		inputIter = NewResultRowIterator(nil)
	}
	if continuation, ok := f.Inner.(*FragmentExec); ok && f.Projection {
		inputResult, err := materializeIterator(inputCols, inputIter)
		if err != nil {
			return nil, fmt.Errorf("apply input failed: %w", err)
		}
		if result, handled, err := e.executeApplyAsPipeline(ctx, tx, inputResult, continuation, params, authToken); handled {
			return result, err
		}
		inputIter = NewResultRowIterator(inputResult)
	}

	// The remaining paths execute correlated CALL bodies per outer record. Their
	// lookup rewrites do not apply to the outer continuation above, nor to
	// OPTIONAL or unit CALL, whose input rows the per-row merge below keeps.
	if execFrag, ok := f.Inner.(*FragmentExec); ok && execFrag.Graph == nil && !f.Optional && !f.Unit && !f.Scoped {
		// Streaming fast path: avoid full outer materialization when correlated
		// batched row lookup rewrite can consume the iterator directly.
		if streamed, handled, err := e.tryExecuteApplyBatchedLookup(ctx, tx, inputCols, inputIter, execFrag, params, authToken, nil); handled {
			_ = inputIter.Close()
			if err != nil {
				return nil, fmt.Errorf("apply inner failed: %w", err)
			}
			return streamed, nil
		}
		inputResult, err := materializeIterator(inputCols, inputIter)
		if err != nil {
			return nil, fmt.Errorf("apply input failed: %w", err)
		}
		if !f.Projection && len(inputResult.Rows) <= fabricApplyInMemoryMaxRows {
			if streamRes, handled := executeApplyInMemoryProjectionColumns(inputResult, execFrag.Query, e.executionProjection(ctx, execFrag.Query, inputResult.Columns).Names); handled {
				return streamRes, nil
			}
		}
		if batched, handled, err := e.tryExecuteApplyBatchedLookup(ctx, tx, inputResult.Columns, nil, execFrag, params, authToken, inputResult); handled {
			if err != nil {
				return nil, fmt.Errorf("apply inner failed: %w", err)
			}
			return batched, nil
		}
		if batched, handled, err := e.tryExecuteApplyBatchedCountLookup(ctx, tx, inputResult, execFrag, params, authToken); handled {
			if err != nil {
				return nil, fmt.Errorf("apply inner failed: %w", err)
			}
			return batched, nil
		}
		if batched, handled, err := e.tryExecuteApplyBatchedLookupRows(ctx, tx, inputResult, execFrag, params, authToken); handled {
			if err != nil {
				return nil, fmt.Errorf("apply inner failed: %w", err)
			}
			return batched, nil
		}
		// Re-seed iterator for the generic correlated APPLY fallback.
		inputCols = inputResult.Columns
		inputIter = NewResultRowIterator(inputResult)
	}

	result := &ResultStream{Columns: append([]string(nil), f.Columns...)}
	innerFragment := f.Inner
	outerIdx := buildColumnIndex(inputCols)
	if f.Unit && len(result.Columns) == 0 {
		// A unit subquery exports nothing, so its columns are its input's,
		// also when there is no input row.
		result.Columns = append([]string(nil), inputCols...)
	}

	// For each input row, execute the inner fragment with imported variables.
	parentBindings, _ := RecordBindingsFromContext(ctx)
	for inputIter.Next() {
		inputRow := inputIter.Row()
		mergedBind := bindingsFromParentAndRow(parentBindings, inputCols, inputRow)
		if f.Scoped {
			mergedBind = scopedBindings(mergedBind, f.Scope)
		}
		innerCtx := WithRecordBindings(ctx, mergedBind)

		if execFrag, ok := innerFragment.(*FragmentExec); ok && !f.Projection && !f.Optional && !f.Unit && !f.Scoped && execFrag.Graph == nil {
			if cols, projected, ok := projectSimpleProjectionFromRow(e.executionProjection(ctx, execFrag.Query, inputCols), inputCols, inputRow); ok {
				if len(result.Columns) == 0 {
					result.Columns = cols
				}
				result.Rows = append(result.Rows, projected)
				continue
			}
		}

		innerResult, err := e.Execute(innerCtx, tx, innerFragment, params, authToken)
		if err != nil {
			return nil, fmt.Errorf("apply inner failed: %w", err)
		}

		if innerResult == nil {
			innerResult = &ResultStream{}
		}
		// A unit subquery runs its body for the body's effects and exports
		// nothing: the input row is kept exactly once, whatever rows the body
		// produced internally (none, or a nested CALL's).
		if f.Unit {
			result.Rows = append(result.Rows, newCompiledRowCombiner(result.Columns, outerIdx, nil).combine(inputRow, nil))
			continue
		}
		if len(innerResult.Rows) == 0 {
			if execFrag, ok := innerFragment.(*FragmentExec); ok {
				if cols, row, ok := synthesizeEmptyCollectProjection(e.executionProjection(ctx, execFrag.Query, inputCols)); ok {
					innerResult.Columns = cols
					innerResult.Rows = [][]interface{}{row}
				}
			}
		}

		// Combine input and inner columns/rows.
		if len(result.Columns) == 0 {
			if f.Projection {
				result.Columns = append([]string(nil), innerResult.Columns...)
				result.RowWrapper = innerResult.RowWrapper
			} else {
				outerCols, innerCols := inputCols, innerResult.Columns
				if inputWrapper {
					outerCols = nil
				}
				if innerResult.RowWrapper {
					innerCols = nil
				}
				result.Columns = combineColumns(outerCols, innerCols)
			}
		}
		if len(innerResult.Rows) == 0 {
			// OPTIONAL CALL keeps the input row with null for the body's columns.
			if f.Optional {
				combiner := newCompiledRowCombiner(result.Columns, outerIdx, buildColumnIndex(innerResult.Columns))
				result.Rows = append(result.Rows, combiner.combine(inputRow, make([]interface{}, len(innerResult.Columns))))
			}
			continue
		}
		innerIdx := buildColumnIndex(innerResult.Columns)
		combiner := newCompiledRowCombiner(result.Columns, outerIdx, innerIdx)
		for _, innerRow := range innerResult.Rows {
			combined := combiner.combine(inputRow, innerRow)
			result.Rows = append(result.Rows, combined)
		}
	}
	if err := inputIter.Err(); err != nil {
		return nil, fmt.Errorf("apply input failed: %w", err)
	}
	_ = inputIter.Close()

	// A projection declares its fields even when there was no input record.
	// Consume the same execution plan used by the row path; a missing plan or
	// an unknown wildcard stays unknown and never invokes the CALL body.
	if result.Columns == nil && len(result.Rows) == 0 && !f.Unit {
		if inner, ok := innerFragment.(*FragmentExec); ok {
			e.executionProjection(ctx, inner.Query, inputCols).Names.Visit(func(names []string) {
				if f.Projection {
					result.Columns = names
				} else {
					result.Columns = combineColumns(inputCols, names)
				}
			}, func() {})
		}
	}

	return result, nil
}

func buildResultModifierAliasMap(items []fabricReturnItem) map[string]string {
	out := make(map[string]string, len(items)*2)
	for _, item := range items {
		if strings.TrimSpace(item.alias) != "" {
			out[item.alias] = item.alias
		}
		if strings.TrimSpace(item.expr) != "" {
			out[item.expr] = item.alias
		}
	}
	return out
}

func isSimpleCountExpr(expr string) bool {
	expr = strings.TrimSpace(expr)
	if !startsWithFold(expr, "count(") || !strings.HasSuffix(expr, ")") {
		return false
	}
	inner := strings.TrimSpace(expr[len("count(") : len(expr)-1])
	if inner == "*" || cyphertext.IsIdentifier(inner) {
		return true
	}
	_, _, ok := parseFabricVarProp(inner)
	return ok
}

func zeroCountValues(n int) []interface{} {
	out := make([]interface{}, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, int64(0))
	}
	return out
}

func parseApplyCorrelatedLookupSubquery(ctx context.Context, query string) (applyCorrelatedLookupSpec, bool) {
	trimmed := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(query), ";"))
	if !startsWithFold(trimmed, "WITH ") {
		return applyCorrelatedLookupSpec{}, false
	}

	rest := strings.TrimSpace(trimmed[len("WITH "):])
	if rest == "" {
		return applyCorrelatedLookupSpec{}, false
	}
	if commaIdx := strings.Index(rest, ","); commaIdx >= 0 && commaIdx < strings.Index(rest+" ", " ") {
		return applyCorrelatedLookupSpec{}, false
	}

	importCol, remainder := firstToken(rest)
	if importCol == "" || !cyphertext.IsIdentifier(importCol) {
		return applyCorrelatedLookupSpec{}, false
	}
	rest = strings.TrimSpace(remainder)
	if rest == "" {
		return applyCorrelatedLookupSpec{}, false
	}

	useClause := ""
	reference, remaining, hasUse, err := ReadUseClause(ctx, rest, true, cyphertext.AcceptedSource)
	if err != nil {
		return applyCorrelatedLookupSpec{}, false
	}
	if hasUse {
		useClause = rest[:reference.End]
		rest = remaining
	}

	if !startsWithFold(rest, "MATCH ") {
		return applyCorrelatedLookupSpec{}, false
	}
	rest = strings.TrimSpace(rest[len("MATCH "):])
	if rest == "" {
		return applyCorrelatedLookupSpec{}, false
	}

	returnIdx := indexTopLevelKeyword(rest, "RETURN")
	if returnIdx < 0 {
		return applyCorrelatedLookupSpec{}, false
	}
	beforeReturn := strings.TrimSpace(rest[:returnIdx])
	if beforeReturn == "" {
		return applyCorrelatedLookupSpec{}, false
	}
	returnClause := strings.TrimSpace(rest[returnIdx+len("RETURN"):])
	if returnClause == "" {
		return applyCorrelatedLookupSpec{}, false
	}

	matchPart := beforeReturn
	wherePart := ""
	if whereIdx := indexTopLevelKeyword(beforeReturn, "WHERE"); whereIdx >= 0 {
		matchPart = strings.TrimSpace(beforeReturn[:whereIdx])
		wherePart = strings.TrimSpace(beforeReturn[whereIdx+len("WHERE"):])
		if matchPart == "" {
			return applyCorrelatedLookupSpec{}, false
		}
	}

	projection, modifiers := splitTopLevelResultModifiers(returnClause)
	if strings.TrimSpace(projection) == "" {
		return applyCorrelatedLookupSpec{}, false
	}

	return applyCorrelatedLookupSpec{
		importCol:        importCol,
		useClause:        strings.TrimSpace(useClause),
		matchPart:        matchPart,
		wherePart:        wherePart,
		returnProjection: strings.TrimSpace(projection),
		returnModifiers:  strings.TrimSpace(modifiers),
	}, true
}

func firstToken(s string) (token string, rest string) {
	s = strings.TrimSpace(s)
	if s == "" {
		return "", ""
	}
	for i := 0; i < len(s); i++ {
		if cyphertext.IsWhitespaceAt(s, i) {
			return strings.TrimSpace(s[:i]), strings.TrimSpace(s[i:])
		}
	}
	return s, ""
}

func indexTopLevelKeyword(s string, keyword string) int {
	paren, bracket, brace := 0, 0, 0
	for i := 0; i < len(s); i++ {
		if next, quoted := cyphertext.SkipToken(s, i); quoted {
			i = next - 1
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
		if keywordAt(s, i, keyword) {
			return i
		}
	}
	return -1
}

// splitTopLevelAnd splits whereClause at each top-level AND that
// indexTopLevelKeyword finds, the owner of quote, comment and nesting
// traversal. Each search continues after the previous AND, so the scans don't
// overlap and a term holding a quoted or nested AND is kept whole.
func splitTopLevelAnd(whereClause string) []string {
	parts := make([]string, 0, 4)
	start := 0
	for {
		idx := indexTopLevelKeyword(whereClause[start:], "AND")
		if idx < 0 {
			parts = append(parts, strings.TrimSpace(whereClause[start:]))
			return parts
		}
		parts = append(parts, strings.TrimSpace(whereClause[start:start+idx]))
		start += idx + len("AND")
	}
}

func splitTopLevelEquality(expr string) (lhs, rhs string, ok bool) {
	paren, bracket, brace := 0, 0, 0
	for i := 0; i < len(expr); i++ {
		if next, quoted := cyphertext.SkipToken(expr, i); quoted {
			i = next - 1
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
			if i > 0 {
				prev := expr[i-1]
				if prev == '<' || prev == '>' || prev == '!' {
					continue
				}
			}
			if i+1 < len(expr) {
				next := expr[i+1]
				if next == '=' {
					continue
				}
			}
			return strings.TrimSpace(expr[:i]), strings.TrimSpace(expr[i+1:]), true
		}
	}
	return "", "", false
}

func parseFabricVarProp(expr string) (varName, prop string, ok bool) {
	dot := strings.IndexByte(expr, '.')
	if dot <= 0 || dot >= len(expr)-1 {
		return "", "", false
	}
	lhs := strings.TrimSpace(expr[:dot])
	rhs := strings.TrimSpace(expr[dot+1:])
	if !cyphertext.IsIdentifier(lhs) || !cyphertext.IsIdentifier(rhs) {
		return "", "", false
	}
	return lhs, rhs, true
}

func (e *FabricExecutor) executionProjection(ctx context.Context, query string, incoming []string) resultmetadata.Projection {
	if provider, ok := e.resultColumns.(resultmetadata.ProjectionProvider); ok {
		return provider.ResultProjection(ctx, query, incoming, AcceptedSource)
	}
	return resultmetadata.Projection{}
}

func synthesizeEmptyCollectProjection(plan resultmetadata.Projection) ([]string, []interface{}, bool) {
	var columns []string
	plan.Names.Visit(func(names []string) { columns = names }, func() {})
	if plan.Distinct || plan.Modifiers != "" || len(columns) == 0 || len(columns) != len(plan.Expressions) {
		return nil, nil, false
	}
	row := make([]interface{}, 0, len(columns))
	for _, expr := range plan.Expressions {
		if !startsWithFold(strings.TrimSpace(expr), "collect(") {
			return nil, nil, false
		}
		row = append(row, []interface{}{})
	}
	return columns, row, true
}

func projectSimpleProjectionFromRow(plan resultmetadata.Projection, inputCols []string, inputRow []interface{}) ([]string, []interface{}, bool) {
	var columns []string
	plan.Names.Visit(func(names []string) { columns = names }, func() {})
	if !plan.Standalone || plan.Distinct || plan.Modifiers != "" || len(columns) == 0 || len(columns) != len(plan.Expressions) {
		return nil, nil, false
	}
	index := buildColumnIndex(inputCols)
	values := make([]interface{}, 0, len(columns))
	for i, source := range plan.Expressions {
		if !cyphertext.IsIdentifier(source) || !cyphertext.IsIdentifier(columns[i]) {
			return nil, nil, false
		}
		position, found := index[source]
		if !found || position >= len(inputRow) {
			return nil, nil, false
		}
		values = append(values, inputRow[position])
	}
	return columns, values, true
}
func bindingsFromParentAndRow(parent map[string]interface{}, columns []string, row []interface{}) map[string]interface{} {
	if len(parent) == 0 && (len(columns) == 0 || len(row) == 0) {
		return nil
	}
	out := make(map[string]interface{}, util.SafePreallocSum(len(parent), len(columns)))
	for k, v := range parent {
		out[k] = v
	}
	for i, col := range columns {
		if i >= len(row) {
			break
		}
		if strings.TrimSpace(col) == "" {
			continue
		}
		out[col] = row[i]
	}
	return out
}

// rowBindings returns bindings for one outer row.
// Kept for targeted unit tests that validate binding edge-cases.
func rowBindings(columns []string, row []interface{}) map[string]interface{} {
	if len(columns) == 0 || len(row) == 0 {
		return nil
	}
	out := make(map[string]interface{}, len(columns))
	for i, col := range columns {
		if i >= len(row) {
			break
		}
		if strings.TrimSpace(col) == "" {
			continue
		}
		out[col] = row[i]
	}
	return out
}

// mergeBindings merges parent and row bindings.
// Kept for unit-test compatibility; production hot-path uses
// bindingsFromParentAndRow to avoid double map allocation.
func mergeBindings(parent, row map[string]interface{}) map[string]interface{} {
	if len(parent) == 0 && len(row) == 0 {
		return nil
	}
	out := make(map[string]interface{}, len(parent)+len(row))
	for k, v := range parent {
		out[k] = v
	}
	for k, v := range row {
		out[k] = v
	}
	return out
}

var reWithCollectMapOnly = regexp.MustCompile(`(?is)^\s*WITH\s+collect\s*\(\s*\{([^}]*)\}\s*\)\s+AS\s+([A-Za-z_][A-Za-z0-9_]*)(?:\s+RETURN\s+([A-Za-z_][A-Za-z0-9_]*))?\s*;?\s*$`)

var reWithCollectDistinctKeys = regexp.MustCompile(`(?is)^\s*WITH\s+collect\s*\(\s*\{([^}]*)\}\s*\)\s+AS\s+([A-Za-z_][A-Za-z0-9_]*)\s+UNWIND\s+([A-Za-z_][A-Za-z0-9_]*)\s+AS\s+([A-Za-z_][A-Za-z0-9_]*)\s+WITH\s+collect\s*\(\s*DISTINCT\s+([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)\s*\)\s+AS\s+([A-Za-z_][A-Za-z0-9_]*)\s+RETURN\s+([A-Za-z_][A-Za-z0-9_]*)\s*;?\s*$`)

var reWithCollectJoinFlat = regexp.MustCompile(`(?is)^\s*WITH\s+([A-Za-z_][A-Za-z0-9_]*)\s*,\s*collect\s*\(\s*\{([^}]*)\}\s*\)\s+AS\s+([A-Za-z_][A-Za-z0-9_]*)\s+UNWIND\s+([A-Za-z_][A-Za-z0-9_]*)\s+AS\s+([A-Za-z_][A-Za-z0-9_]*)\s+WITH\s+([A-Za-z_][A-Za-z0-9_]*)\s*,\s*\[\s*([A-Za-z_][A-Za-z0-9_]*)\s+IN\s+([A-Za-z_][A-Za-z0-9_]*)\s+WHERE\s+([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)\s*=\s*([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)\s*\]\s+AS\s+([A-Za-z_][A-Za-z0-9_]*)\s+UNWIND\s+([A-Za-z_][A-Za-z0-9_]*)\s+AS\s+([A-Za-z_][A-Za-z0-9_]*)\s+RETURN\s+(.+?)\s*;?\s*$`)

func executeApplyInMemoryProjectionColumns(inputResult *ResultStream, query string, names resultmetadata.Names) (*ResultStream, bool) {
	if inputResult == nil {
		return nil, false
	}
	var columns []string
	names.Visit(func(known []string) { columns = known }, func() {})
	if columns == nil {
		return nil, false
	}
	trimmed := strings.TrimSpace(query)
	if trimmed == "" {
		return nil, false
	}

	if startsWithFold(trimmed, "RETURN ") {
		clause := strings.TrimSpace(trimmed[len("RETURN "):])
		projectionClause, modifierClause := splitTopLevelResultModifiers(clause)
		items := splitTopLevelCSV(projectionClause)
		if len(items) > 0 {
			type retItem struct {
				src   string
				alias string
			}
			parsed := make([]retItem, 0, len(items))
			for _, it := range items {
				it = strings.TrimSpace(it)
				if it == "" {
					return nil, false
				}
				src := it
				alias := it
				if as := lastAsIndexFold(it); as >= 0 {
					src = strings.TrimSpace(it[:as])
					alias = strings.TrimSpace(it[as+4:])
				}
				if !cyphertext.IsIdentifier(src) || !cyphertext.IsIdentifier(alias) {
					return nil, false
				}
				parsed = append(parsed, retItem{src: src, alias: alias})
			}
			colIdx := buildColumnIndex(inputResult.Columns)
			for _, p := range parsed {
				if _, ok := colIdx[p.src]; !ok {
					return nil, false
				}
			}
			out := &ResultStream{
				Columns: columns,
				Rows:    make([][]interface{}, 0, len(inputResult.Rows)),
			}
			aliasToCol := make(map[string]string, len(parsed))
			for _, p := range parsed {
				aliasToCol[p.alias] = p.alias
				if _, exists := aliasToCol[p.src]; !exists {
					aliasToCol[p.src] = p.alias
				}
			}
			for _, in := range inputResult.Rows {
				row := make([]interface{}, len(parsed))
				for i, p := range parsed {
					if idx, ok := colIdx[p.src]; ok && idx < len(in) {
						row[i] = in[idx]
					}
				}
				out.Rows = append(out.Rows, row)
			}
			applySimpleResultModifiers(out, modifierClause, aliasToCol)
			return out, true
		}
	}

	if m := reWithCollectDistinctKeys.FindStringSubmatch(trimmed); len(m) == 9 {
		mapSpec := strings.TrimSpace(m[1])
		rowsAlias := strings.TrimSpace(m[2])
		unwindList := strings.TrimSpace(m[3])
		unwindVar := strings.TrimSpace(m[4])
		distinctVar := strings.TrimSpace(m[5])
		keysProp := strings.TrimSpace(m[6])
		keysAlias := strings.TrimSpace(m[7])
		returnAlias := strings.TrimSpace(m[8])
		if !strings.EqualFold(rowsAlias, unwindList) || !strings.EqualFold(unwindVar, distinctVar) || !strings.EqualFold(keysAlias, returnAlias) {
			return nil, false
		}
		rowMaps, ok := projectInputRowsAsMaps(inputResult, mapSpec)
		if !ok {
			return nil, false
		}
		seen := map[interface{}]struct{}{}
		keys := make([]interface{}, 0, len(rowMaps))
		for _, rm := range rowMaps {
			v := rm[keysProp]
			if _, exists := seen[v]; exists {
				continue
			}
			seen[v] = struct{}{}
			keys = append(keys, v)
		}
		return &ResultStream{
			Columns: columns,
			Rows:    [][]interface{}{{keys}},
		}, true
	}

	if m := reWithCollectMapOnly.FindStringSubmatch(trimmed); len(m) == 4 {
		mapSpec := strings.TrimSpace(m[1])
		rowsAlias := strings.TrimSpace(m[2])
		returnAlias := strings.TrimSpace(m[3])
		if returnAlias != "" && !strings.EqualFold(rowsAlias, returnAlias) {
			return nil, false
		}
		rowMaps, ok := projectInputRowsAsMaps(inputResult, mapSpec)
		if !ok {
			return nil, false
		}
		items := make([]interface{}, 0, len(rowMaps))
		for _, rm := range rowMaps {
			items = append(items, rm)
		}
		return &ResultStream{
			Columns: columns,
			Rows:    [][]interface{}{{items}},
		}, true
	}

	// Specialized stream join used by the batched composite query shape:
	// WITH rows, collect({k: k, texts: texts}) AS grouped
	// UNWIND rows AS r
	// WITH r, [g IN grouped WHERE g.k = r.textKey128][0] AS hit
	// RETURN r.textKey AS textKey, r.textKey128 AS textKey128, coalesce(hit.texts, []) AS texts
	if startsWithFold(trimmed, "WITH rows, collect({k: k, texts: texts}) AS grouped") &&
		containsFold(trimmed, "UNWIND rows AS r") &&
		strings.Contains(trimmed, "g.k = r.textKey128") &&
		containsFold(trimmed, "COALESCE(hit.texts, []) AS texts") {
		result := executeRowsGroupedJoinProjection(inputResult)
		result.Columns = columns
		return result, true
	}

	// Generic set-based flatten projection:
	// WITH <rows>, collect({...}) AS <grouped>
	// UNWIND <rows> AS <r>
	// WITH <r>, [g IN <grouped> WHERE g.<k1> = <r>.<k2>] AS <ms>
	// UNWIND <ms> AS <m>
	// RETURN <r>.<...> AS ..., <m>.<...> AS ...
	if projected, ok := executeCollectedMapJoinFlatProjectionColumns(inputResult, trimmed, resultmetadata.Known(columns)); ok {
		projected.Columns = columns
		return projected, true
	}

	return nil, false
}

type simpleOrderSpec struct {
	column string
	desc   bool
}

func splitTopLevelResultModifiers(clause string) (projection string, modifiers string) {
	projection = strings.TrimSpace(clause)
	modifiers = ""
	paren, bracket, brace := 0, 0, 0
	for i := 0; i < len(clause); i++ {
		if next, quoted := cyphertext.SkipToken(clause, i); quoted {
			i = next - 1
			continue
		}
		ch := clause[i]
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
		if keywordAt(clause, i, "ORDER BY") || keywordAt(clause, i, "SKIP") || keywordAt(clause, i, "LIMIT") {
			projection = strings.TrimSpace(clause[:i])
			modifiers = strings.TrimSpace(clause[i:])
			return projection, modifiers
		}
	}
	return projection, modifiers
}

func applySimpleResultModifiers(result *ResultStream, modifiers string, aliasToCol map[string]string) {
	if result == nil || len(result.Rows) == 0 {
		return
	}
	orderSpecs, skip, limit := parseSimpleResultModifiers(modifiers, aliasToCol)
	if len(orderSpecs) > 0 {
		colIdx := buildColumnIndex(result.Columns)
		sort.SliceStable(result.Rows, func(i, j int) bool {
			left := result.Rows[i]
			right := result.Rows[j]
			for _, spec := range orderSpecs {
				idx, ok := colIdx[spec.column]
				if !ok {
					continue
				}
				cmp := compareSimpleOrderValues(valueAtRowIndex(left, idx), valueAtRowIndex(right, idx))
				if cmp == 0 {
					continue
				}
				if spec.desc {
					return cmp > 0
				}
				return cmp < 0
			}
			return false
		})
	}
	if skip > 0 {
		if skip >= len(result.Rows) {
			result.Rows = result.Rows[:0]
			return
		}
		result.Rows = result.Rows[skip:]
	}
	if limit >= 0 && limit < len(result.Rows) {
		result.Rows = result.Rows[:limit]
	}
}

func parseSimpleResultModifiers(modifiers string, aliasToCol map[string]string) ([]simpleOrderSpec, int, int) {
	if strings.TrimSpace(modifiers) == "" {
		return nil, 0, -1
	}
	orderSpecs := []simpleOrderSpec{}
	skip := 0
	limit := -1
	remaining := strings.TrimSpace(modifiers)
	for remaining != "" {
		switch {
		case startsWithFold(remaining, "ORDER BY"):
			orderClause, rest := splitLeadingModifierClause(remaining, "ORDER BY")
			orderSpecs = parseSimpleOrderByClause(strings.TrimSpace(orderClause[len("ORDER BY"):]), aliasToCol)
			remaining = strings.TrimSpace(rest)
		case startsWithFold(remaining, "SKIP"):
			skipClause, rest := splitLeadingModifierClause(remaining, "SKIP")
			if n, ok := parseSimplePositiveInt(strings.TrimSpace(skipClause[len("SKIP"):])); ok {
				skip = n
			}
			remaining = strings.TrimSpace(rest)
		case startsWithFold(remaining, "LIMIT"):
			limitClause, rest := splitLeadingModifierClause(remaining, "LIMIT")
			if n, ok := parseSimplePositiveInt(strings.TrimSpace(limitClause[len("LIMIT"):])); ok {
				limit = n
			}
			remaining = strings.TrimSpace(rest)
		default:
			remaining = ""
		}
	}
	return orderSpecs, skip, limit
}

func splitLeadingModifierClause(s string, keyword string) (clause string, rest string) {
	trimmed := strings.TrimSpace(s)
	if trimmed == "" {
		return "", ""
	}
	end := len(trimmed)
	if strings.EqualFold(keyword, "ORDER BY") {
		for i := len(keyword); i < len(trimmed); i++ {
			if keywordAt(trimmed, i, "SKIP") || keywordAt(trimmed, i, "LIMIT") {
				end = i
				break
			}
		}
	} else {
		for i := len(keyword); i < len(trimmed); i++ {
			if keywordAt(trimmed, i, "ORDER BY") || keywordAt(trimmed, i, "SKIP") || keywordAt(trimmed, i, "LIMIT") {
				end = i
				break
			}
		}
	}
	return strings.TrimSpace(trimmed[:end]), strings.TrimSpace(trimmed[end:])
}

func parseSimpleOrderByClause(clause string, aliasToCol map[string]string) []simpleOrderSpec {
	parts := splitTopLevelCSV(clause)
	specs := make([]simpleOrderSpec, 0, len(parts))
	for _, part := range parts {
		item := strings.TrimSpace(part)
		if item == "" {
			continue
		}
		desc := false
		if len(item) > 5 && strings.EqualFold(item[len(item)-5:], " DESC") {
			desc = true
			item = strings.TrimSpace(item[:len(item)-5])
		} else if len(item) > 4 && strings.EqualFold(item[len(item)-4:], " ASC") {
			item = strings.TrimSpace(item[:len(item)-4])
		}
		if mapped, ok := aliasToCol[item]; ok {
			item = mapped
		}
		if !cyphertext.IsIdentifier(item) {
			continue
		}
		specs = append(specs, simpleOrderSpec{column: item, desc: desc})
	}
	return specs
}

func parseSimplePositiveInt(s string) (int, bool) {
	token, ok := cyphertext.ScanNumber(s, 0)
	if !ok || token.Invalid || token.End != len(s) || token.Base != 10 || token.Fraction || token.Exponent || token.Grouped || token.Suffix != 0 {
		return 0, false
	}
	value := 0
	for index := 0; index < token.End; index++ {
		value = value*10 + int(s[index]-'0')
	}
	return value, true
}

func valueAtRowIndex(row []interface{}, idx int) interface{} {
	if idx < 0 || idx >= len(row) {
		return nil
	}
	return row[idx]
}

func compareSimpleOrderValues(left interface{}, right interface{}) int {
	if left == nil && right == nil {
		return 0
	}
	if left == nil {
		return -1
	}
	if right == nil {
		return 1
	}
	if lf, ok := asComparableFloat(left); ok {
		if rf, ok := asComparableFloat(right); ok {
			switch {
			case lf < rf:
				return -1
			case lf > rf:
				return 1
			default:
				return 0
			}
		}
	}
	if lb, ok := left.(bool); ok {
		if rb, ok := right.(bool); ok {
			switch {
			case lb == rb:
				return 0
			case !lb && rb:
				return -1
			default:
				return 1
			}
		}
	}
	ls := fmt.Sprint(left)
	rs := fmt.Sprint(right)
	switch {
	case ls < rs:
		return -1
	case ls > rs:
		return 1
	default:
		return 0
	}
}

func asComparableFloat(v interface{}) (float64, bool) {
	switch x := v.(type) {
	case int:
		return float64(x), true
	case int8:
		return float64(x), true
	case int16:
		return float64(x), true
	case int32:
		return float64(x), true
	case int64:
		return float64(x), true
	case uint:
		return float64(x), true
	case uint8:
		return float64(x), true
	case uint16:
		return float64(x), true
	case uint32:
		return float64(x), true
	case uint64:
		return float64(x), true
	case float32:
		return float64(x), true
	case float64:
		return x, true
	default:
		return 0, false
	}
}

func projectInputRowsAsMaps(input *ResultStream, mapSpec string) ([]map[string]interface{}, bool) {
	colIdx := buildColumnIndex(input.Columns)
	entries := splitTopLevelCSV(mapSpec)
	if len(entries) == 0 {
		return nil, false
	}
	type kv struct {
		key string
		col string
	}
	pairs := make([]kv, 0, len(entries))
	for _, e := range entries {
		e = strings.TrimSpace(e)
		colon := strings.Index(e, ":")
		if colon <= 0 {
			return nil, false
		}
		k := strings.TrimSpace(e[:colon])
		v := strings.TrimSpace(e[colon+1:])
		if !cyphertext.IsIdentifier(k) || !cyphertext.IsIdentifier(v) {
			return nil, false
		}
		pairs = append(pairs, kv{key: k, col: v})
	}

	out := make([]map[string]interface{}, 0, len(input.Rows))
	for _, row := range input.Rows {
		m := make(map[string]interface{}, len(pairs))
		for _, p := range pairs {
			if idx, ok := colIdx[p.col]; ok && idx < len(row) {
				m[p.key] = row[idx]
			}
		}
		out = append(out, m)
	}
	return out, true
}

func executeRowsGroupedJoinProjection(input *ResultStream) *ResultStream {
	colIdx := buildColumnIndex(input.Columns)
	rowsIdx, okRows := colIdx["rows"]
	keyIdx, okK := colIdx["k"]
	textsIdx, okTexts := colIdx["texts"]
	// out is the one result header of every return path.
	out := &ResultStream{Columns: []string{"textKey", "textKey128", "texts"}, Rows: [][]interface{}{}}
	if !okRows || !okK || !okTexts || len(input.Rows) == 0 {
		return out
	}

	// Build grouped lookup from k -> texts.
	grouped := make(map[interface{}]interface{}, len(input.Rows))
	for _, row := range input.Rows {
		if keyIdx < len(row) && textsIdx < len(row) {
			grouped[row[keyIdx]] = row[textsIdx]
		}
	}

	// Use rows list from first row (prefix projection result is identical across rows).
	first := input.Rows[0]
	if rowsIdx >= len(first) {
		return out
	}
	var rowsAny []interface{}
	switch v := first[rowsIdx].(type) {
	case []interface{}:
		rowsAny = v
	case []map[string]interface{}:
		rowsAny = make([]interface{}, 0, len(v))
		for _, it := range v {
			rowsAny = append(rowsAny, it)
		}
	default:
		return out
	}

	out.Rows = make([][]interface{}, 0, len(rowsAny))
	for _, it := range rowsAny {
		rm, ok := it.(map[string]interface{})
		if !ok {
			continue
		}
		hash := rm["textKey128"]
		texts, ok := grouped[hash]
		if !ok || texts == nil {
			texts = []interface{}{}
		}
		out.Rows = append(out.Rows, []interface{}{rm["textKey"], hash, texts})
	}
	return out
}

func executeCollectedMapJoinFlatProjectionColumns(input *ResultStream, query string, names resultmetadata.Names) (*ResultStream, bool) {
	if input == nil || len(input.Rows) == 0 {
		return nil, false
	}
	m := reWithCollectJoinFlat.FindStringSubmatch(query)
	if len(m) != 17 {
		return nil, false
	}

	rowsAlias := strings.TrimSpace(m[1])
	mapSpec := strings.TrimSpace(m[2])
	groupAlias := strings.TrimSpace(m[3])
	unwindList := strings.TrimSpace(m[4])
	rowVar := strings.TrimSpace(m[5])
	withRowVar := strings.TrimSpace(m[6])
	groupVar := strings.TrimSpace(m[7])
	inGroupAlias := strings.TrimSpace(m[8])
	groupVarRef := strings.TrimSpace(m[9])
	groupKeyField := strings.TrimSpace(m[10])
	rowVarRef := strings.TrimSpace(m[11])
	rowKeyField := strings.TrimSpace(m[12])
	msAlias := strings.TrimSpace(m[13])
	unwindMs := strings.TrimSpace(m[14])
	itemVar := strings.TrimSpace(m[15])
	returnClause := strings.TrimSpace(m[16])

	if !strings.EqualFold(rowsAlias, unwindList) ||
		!strings.EqualFold(rowVar, withRowVar) ||
		!strings.EqualFold(groupAlias, inGroupAlias) ||
		!strings.EqualFold(groupVar, groupVarRef) ||
		!strings.EqualFold(rowVar, rowVarRef) ||
		!strings.EqualFold(msAlias, unwindMs) {
		return nil, false
	}

	collectPairs := splitTopLevelCSV(mapSpec)
	if len(collectPairs) == 0 {
		return nil, false
	}
	type mapPair struct {
		key string
		src string
	}
	mapPairs := make([]mapPair, 0, len(collectPairs))
	joinFieldExists := false
	for _, p := range collectPairs {
		p = strings.TrimSpace(p)
		colon := strings.IndexByte(p, ':')
		if colon <= 0 {
			return nil, false
		}
		key := strings.TrimSpace(p[:colon])
		src := strings.TrimSpace(p[colon+1:])
		if !cyphertext.IsIdentifier(key) || !cyphertext.IsIdentifier(src) {
			return nil, false
		}
		if strings.EqualFold(key, groupKeyField) {
			joinFieldExists = true
		}
		mapPairs = append(mapPairs, mapPair{key: key, src: src})
	}
	if !joinFieldExists {
		return nil, false
	}

	type retItem struct {
		srcVar string
		srcKey string
		alias  string
	}
	retParts := splitTopLevelCSV(returnClause)
	if len(retParts) == 0 {
		return nil, false
	}
	retItems := make([]retItem, 0, len(retParts))
	for _, part := range retParts {
		part = strings.TrimSpace(part)
		as := lastAsIndexFold(part)
		if as < 0 {
			return nil, false
		}
		expr := strings.TrimSpace(part[:as])
		alias := strings.TrimSpace(part[as+4:])
		if !cyphertext.IsIdentifier(alias) {
			return nil, false
		}
		v, k, ok := parseFabricVarProp(expr)
		if !ok {
			return nil, false
		}
		retItems = append(retItems, retItem{srcVar: v, srcKey: k, alias: alias})
	}

	colIdx := buildColumnIndex(input.Columns)
	rowsIdx, ok := colIdx[rowsAlias]
	if !ok || len(input.Rows) == 0 || rowsIdx >= len(input.Rows[0]) {
		return nil, false
	}
	for _, pair := range mapPairs {
		if _, ok := colIdx[pair.src]; !ok {
			return nil, false
		}
	}

	// Build grouped lookup from join key -> collected map rows.
	grouped := make(map[string][]map[string]interface{}, len(input.Rows))
	for _, row := range input.Rows {
		collectMap := make(map[string]interface{}, len(mapPairs))
		for _, pair := range mapPairs {
			idx := colIdx[pair.src]
			if idx < len(row) {
				collectMap[pair.key] = row[idx]
			}
		}
		key := applyLookupKeyString(collectMap[groupKeyField])
		grouped[key] = append(grouped[key], collectMap)
	}

	var rowsAny []interface{}
	switch v := input.Rows[0][rowsIdx].(type) {
	case []interface{}:
		rowsAny = v
	case []map[string]interface{}:
		rowsAny = make([]interface{}, 0, len(v))
		for _, it := range v {
			rowsAny = append(rowsAny, it)
		}
	default:
		return nil, false
	}

	var outCols []string
	names.Visit(func(columns []string) { outCols = columns }, func() {})
	if outCols == nil || len(outCols) != len(retItems) {
		return nil, false
	}

	out := &ResultStream{
		Columns: outCols,
		Rows:    make([][]interface{}, 0, len(rowsAny)),
	}

	for _, it := range rowsAny {
		rowMap, ok := it.(map[string]interface{})
		if !ok {
			continue
		}
		key := applyLookupKeyString(rowMap[rowKeyField])
		matches := grouped[key]
		if len(matches) == 0 {
			continue
		}
		for _, match := range matches {
			outRow := make([]interface{}, 0, len(retItems))
			for _, item := range retItems {
				switch {
				case strings.EqualFold(item.srcVar, rowVar):
					outRow = append(outRow, rowMap[item.srcKey])
				case strings.EqualFold(item.srcVar, itemVar):
					outRow = append(outRow, match[item.srcKey])
				default:
					outRow = append(outRow, nil)
				}
			}
			out.Rows = append(out.Rows, outRow)
		}
	}
	return out, true
}

func (e *FabricExecutor) executeApplyAsPipeline(
	ctx context.Context,
	tx *FabricTransaction,
	inputResult *ResultStream,
	inner *FragmentExec,
	params map[string]interface{},
	authToken string,
) (*ResultStream, bool, error) {
	if inputResult == nil || len(inputResult.Columns) == 0 {
		return nil, false, nil
	}

	trimmed := strings.TrimSpace(inner.Query)
	if trimmed == "" {
		return nil, false, nil
	}
	// The planner's Projection marker owns this decision. A continuation consumes
	// the complete input stream even when it starts with a simple WITH, UNWIND or
	// another CALL. Guessing from the leading clause loses later query horizons.

	rows := make([]map[string]interface{}, 0, len(inputResult.Rows))
	for _, row := range inputResult.Rows {
		m := make(map[string]interface{}, len(inputResult.Columns))
		for i, col := range inputResult.Columns {
			if i < len(row) {
				m[col] = row[i]
			}
		}
		rows = append(rows, m)
	}

	if inputResult.RowWrapper {
		return nil, false, nil
	}
	inputNames := make(map[string]interface{}, len(inputResult.Columns))
	for _, col := range inputResult.Columns {
		inputNames[col] = nil
	}
	rowsParam, rowVar := freeName("__fabric_apply_rows", trimmed, params), freeName("__fabric_row", trimmed, inputNames)
	projections := make([]string, 0, len(inputResult.Columns))
	for _, col := range inputResult.Columns {
		col = strings.TrimSpace(col)
		if col == "" {
			continue
		}
		quotedCol := cyphertext.PrintIdentifier(col, cyphertext.IsIdentifier, cyphertext.AcceptedSource)
		projections = append(projections, rowVar+"."+quotedCol+" AS "+quotedCol)
	}
	if len(projections) == 0 {
		return nil, false, nil
	}

	rewritten := "UNWIND $" + rowsParam + " AS " + rowVar + " WITH " + strings.Join(projections, ", ") + " " + trimmed
	mergedParams := make(map[string]interface{}, util.SafePreallocSum(len(params), 1))
	for k, v := range params {
		mergedParams[k] = v
	}
	mergedParams[rowsParam] = rows

	copied := *inner
	copied.Query = rewritten
	res, err := e.executeExec(ctx, tx, &copied, mergedParams, authToken)
	if err != nil {
		return nil, true, err
	}
	if res != nil && len(res.Columns) == 1 && res.Columns[0] == rowVar {
		// Fallback: when pipeline execution leaks the wrapper row map column,
		// apply structural in-memory projection from the original inner query.
		if projected, ok := executeCollectedMapJoinFlatProjectionColumns(inputResult, trimmed, e.executionProjection(ctx, trimmed, inputResult.Columns).Names); ok {
			return projected, true, nil
		}
		res.RowWrapper = true
	}
	return res, true, nil
}

func importColumnsFromFragment(f Fragment) []string {
	if f == nil {
		return nil
	}
	if init, ok := f.(*FragmentInit); ok {
		if len(init.ImportColumns) > 0 {
			return init.ImportColumns
		}
		return init.Columns
	}
	return nil
}

func rewriteFragmentWithImports(fragment Fragment) Fragment {
	if fragment == nil {
		return nil
	}

	switch f := fragment.(type) {
	case *FragmentExec:
		importCols := importColumnsFromFragment(f.Input)
		if rewritten := rewriteLeadingWithImports(f.Query, importCols); rewritten != f.Query {
			copied := *f
			copied.Query = rewritten
			return &copied
		}
		return fragment
	case *FragmentApply:
		copied := *f
		copied.Input = rewriteFragmentWithImports(copied.Input)
		copied.Inner = rewriteFragmentWithImports(copied.Inner)
		return &copied
	case *FragmentUnion:
		copied := *f
		copied.LHS = rewriteFragmentWithImports(copied.LHS)
		copied.RHS = rewriteFragmentWithImports(copied.RHS)
		return &copied
	default:
		return fragment
	}
}

func rewriteFragmentWithRuntimeImports(fragment Fragment, runtimeCols []string) Fragment {
	if fragment == nil {
		return nil
	}
	switch f := fragment.(type) {
	case *FragmentExec:
		importCols := importColumnsFromFragment(f.Input)
		merged := mergeImportColumns(runtimeCols, importCols)
		if rewritten := rewriteLeadingWithImports(f.Query, merged); rewritten != f.Query {
			copied := *f
			copied.Query = rewritten
			return &copied
		}
		return fragment
	case *FragmentApply:
		copied := *f
		copied.Input = rewriteFragmentWithRuntimeImports(copied.Input, runtimeCols)
		copied.Inner = rewriteFragmentWithRuntimeImports(copied.Inner, runtimeCols)
		return &copied
	case *FragmentUnion:
		copied := *f
		copied.LHS = rewriteFragmentWithRuntimeImports(copied.LHS, runtimeCols)
		copied.RHS = rewriteFragmentWithRuntimeImports(copied.RHS, runtimeCols)
		return &copied
	default:
		return fragment
	}
}

func mergeImportColumns(a, b []string) []string {
	seen := make(map[string]struct{}, len(a)+len(b))
	out := make([]string, 0, len(a)+len(b))
	for _, col := range a {
		col = strings.TrimSpace(col)
		if col == "" {
			continue
		}
		if _, ok := seen[col]; ok {
			continue
		}
		seen[col] = struct{}{}
		out = append(out, col)
	}
	for _, col := range b {
		col = strings.TrimSpace(col)
		if col == "" {
			continue
		}
		if _, ok := seen[col]; ok {
			continue
		}
		seen[col] = struct{}{}
		out = append(out, col)
	}
	return out
}

func fragmentHasStaticImports(fragment Fragment) bool {
	switch f := fragment.(type) {
	case *FragmentExec:
		return len(importColumnsFromFragment(f.Input)) > 0
	case *FragmentApply:
		return fragmentHasStaticImports(f.Input) || fragmentHasStaticImports(f.Inner)
	case *FragmentUnion:
		return fragmentHasStaticImports(f.LHS) || fragmentHasStaticImports(f.RHS)
	default:
		return false
	}
}

// scopedBindings keeps the variables a CALL scope clause imports.
func scopedBindings(bindings map[string]interface{}, scope []string) map[string]interface{} {
	kept := make(map[string]interface{}, len(scope))
	for _, name := range scope {
		if name == "*" {
			return bindings
		}
		if value, ok := bindings[name]; ok {
			kept[name] = value
		}
	}
	return kept
}

// remoteImportStatement is the statement and parameters a remote participant
// runs f with. The remote statement cannot see the row, so each variable f
// imports that the row binds is projected ahead of f's own clauses from a
// parameter of its own, under a name free in the statement and among the
// parameters (freeName): WITH $__fabric_import_0 AS x WITH x ... An
// importing WITH * imports every row variable. Variables a scope clause
// imports stay visible throughout the body, so a scoped fragment runs in that
// scope on the remote too: WITH $__fabric_import_0 AS x CALL (x) { ... }
// RETURN *. scoped is then the imported variables, which the caller drops
// from the result. Imported values and the caller's parameters alike are
// sent as parameters, which the remote transport writes as their Cypher
// types; a fragment that imports nothing runs as it is.
func (e *FabricExecutor) remoteImportStatement(f *FragmentExec, params, record map[string]interface{}) (query string, out map[string]interface{}, scoped []string, err error) {
	query, out, scoped, err = e.importStatement(f, params, record, e.codec.Import)
	if err == nil {
		query = cyphertext.EmitRawSource(query)
	}
	return query, out, scoped, err
}

// importStatement preserves scoped imports with the canonical CALL lifetime on
// both local and remote participants. Remote transports additionally validate
// which values may cross a server boundary.
func (e *FabricExecutor) importStatement(f *FragmentExec, params, record map[string]interface{}, validate func(interface{}) error) (query string, out map[string]interface{}, scoped []string, err error) {
	imports := f.ScopeImports
	if len(imports) == 0 {
		imports = importColumnsFromFragment(f.Input)
	}
	names := importedNames(imports, record)
	if len(names) == 0 {
		return f.Query, params, nil, nil
	}
	out = make(map[string]interface{}, len(params)+len(names))
	for key, value := range params {
		out[key] = value
	}
	var head strings.Builder
	head.WriteString("WITH ")
	for i, name := range names {
		value := record[name]
		if validate != nil {
			if err := validate(value); err != nil {
				return "", nil, nil, err
			}
		}
		parameter := freeName("__fabric_import_"+strconv.Itoa(i), f.Query, out)
		out[parameter] = value
		if i > 0 {
			head.WriteString(", ")
		}
		head.WriteString("$" + parameter + " AS " + cyphertext.PrintIdentifier(name, cyphertext.IsIdentifier, cyphertext.AcceptedSource))
	}
	body := strings.TrimSpace(f.Query)
	if len(f.ScopeImports) == 0 {
		return head.String() + " " + body, out, nil, nil
	}
	quoted := make([]string, len(names))
	for i, name := range names {
		quoted[i] = cyphertext.PrintIdentifier(name, cyphertext.IsIdentifier, cyphertext.AcceptedSource)
	}
	return head.String() + " CALL (" + strings.Join(quoted, ", ") + ") { " + body + " } RETURN *", out, names, nil
}

// importedNames is the variables a fragment imports that the row binds, in a
// stable order. An importing WITH * imports every row variable.
func importedNames(imports []string, record map[string]interface{}) []string {
	if len(imports) == 0 || len(record) == 0 {
		return nil
	}
	names := make([]string, 0, len(imports))
	seen := make(map[string]bool, len(imports))
	for _, name := range imports {
		if name != "*" {
			if _, bound := record[name]; bound && !seen[name] {
				seen[name] = true
				names = append(names, name)
			}
			continue
		}
		for key := range record {
			if !seen[key] {
				seen[key] = true
				names = append(names, key)
			}
		}
	}
	sort.Strings(names)
	return names
}

// freeName is a name for a value Fabric adds to a statement: base, or else
// base_1, base_2 and so on, the first that occurs nowhere in text, so no
// variable, alias or parameter written there can be that name, and that names
// none of taken (parameters or row variables already bound).
func freeName(base, text string, taken ...map[string]interface{}) string {
	for n := 0; ; n++ {
		name := base
		if n > 0 {
			name += "_" + strconv.Itoa(n)
		}
		if strings.Contains(text, name) {
			continue
		}
		free := true
		for _, names := range taken {
			if _, used := names[name]; used {
				free = false
				break
			}
		}
		if free {
			return name
		}
	}
}

// dropResultColumns removes columns from a result and its rows.
func dropResultColumns(res *ResultStream, columns []string) {
	drop := make(map[string]bool, len(columns))
	for _, column := range columns {
		drop[column] = true
	}
	keep := make([]int, 0, len(res.Columns))
	kept := make([]string, 0, len(res.Columns))
	for i, column := range res.Columns {
		if !drop[column] {
			keep = append(keep, i)
			kept = append(kept, column)
		}
	}
	if len(kept) == len(res.Columns) {
		return
	}
	for r, row := range res.Rows {
		projected := make([]interface{}, len(keep))
		for i, index := range keep {
			if index < len(row) {
				projected[i] = row[index]
			}
		}
		res.Rows[r] = projected
	}
	res.Columns = kept
}

func rewriteLeadingWithImports(query string, importCols []string) string {
	if len(importCols) == 0 {
		return query
	}
	assignments := make([]string, 0, len(importCols))
	for _, col := range importCols {
		col = strings.TrimSpace(col)
		if col == "" {
			continue
		}
		assignments = append(assignments, "$"+col+" AS "+col)
	}
	if len(assignments) == 0 {
		return query
	}

	trimmed := strings.TrimSpace(query)
	if !startsWithFold(trimmed, "WITH ") {
		// For projection-only fragments produced by planner (e.g. trailing
		// RETURN after APPLY), import outer variables explicitly.
		if startsWithFold(trimmed, "RETURN ") {
			return "WITH " + strings.Join(assignments, ", ") + " " + trimmed
		}
		return query
	}

	withEnd, ok := findLeadingWithClauseEnd(trimmed, nil)
	if !ok || withEnd <= 0 {
		return query
	}
	withClause := strings.TrimSpace(trimmed[:withEnd])
	rest := strings.TrimSpace(trimmed[withEnd:])
	if rest == "" {
		return "WITH " + strings.Join(assignments, ", ")
	}
	// Keep correlated semantics for simple import-only WITH clauses by
	// substituting imported vars directly into MATCH predicates.
	if (startsWithFold(rest, "MATCH ") || startsWithFold(rest, "OPTIONAL MATCH ")) &&
		isSimpleWithImportClause(withClause, importCols) {
		return substituteVarsWithParams(rest, importCols)
	}
	return "WITH " + strings.Join(assignments, ", ") + " " + rest
}

func isSimpleWithImportClause(withClause string, importCols []string) bool {
	if len(importCols) == 0 {
		return false
	}
	trimmed := strings.TrimSpace(withClause)
	if !startsWithFold(trimmed, "WITH ") {
		return false
	}
	lhs := strings.TrimSpace(trimmed[len("WITH "):])
	if lhs == "" {
		return false
	}
	parts := splitTopLevelCSV(lhs)
	if len(parts) == 0 {
		return false
	}
	allowed := make(map[string]struct{}, len(importCols))
	for _, col := range importCols {
		col = strings.TrimSpace(col)
		if col == "" {
			continue
		}
		allowed[col] = struct{}{}
	}
	for _, p := range parts {
		name := strings.TrimSpace(p)
		if !cyphertext.IsIdentifier(name) {
			return false
		}
		if _, ok := allowed[name]; !ok {
			return false
		}
	}
	return true
}

func substituteVarsWithParams(query string, vars []string) string {
	out := query
	for _, v := range vars {
		v = strings.TrimSpace(v)
		if v == "" {
			continue
		}
		out = replaceStandaloneVar(out, v, "$"+v)
	}
	return out
}

func replaceStandaloneVar(s, ident, replacement string) string {
	if ident == "" || len(s) < len(ident) {
		return s
	}
	var b strings.Builder
	b.Grow(len(s) + 8)
	for i := 0; i < len(s); {
		if next, quoted := cyphertext.SkipToken(s, i); quoted {
			b.WriteString(s[i:next])
			i = next
			continue
		}
		ch := s[i]

		if i+len(ident) <= len(s) && s[i:i+len(ident)] == ident {
			prevOK := i == 0 || (!cyphertext.NamePartAt(s, i-1) && s[i-1] != '.')
			next := i + len(ident)
			nextOK := next >= len(s) || !cyphertext.NamePartAt(s, next)
			if prevOK && nextOK {
				b.WriteString(replacement)
				i = next
				continue
			}
		}
		b.WriteByte(ch)
		i++
	}
	return b.String()
}

func findLeadingWithClauseEnd(query string, isName ClauseKeywordNameReader) (int, bool) {
	if query == "" {
		return 0, false
	}
	i := skipLeadingSpace(query, 0)
	if !keywordAt(query, i, "WITH") {
		return 0, false
	}
	i += len("WITH")
	i = skipLeadingSpace(query, i)
	return nextTopLevelClause(query, i, isName), true
}

// nextTopLevelClause returns the start of the first clause keyword at or
// after from that is outside brackets, quoted text and comments, len(query)
// if none.
func nextTopLevelClause(query string, from int, isName ClauseKeywordNameReader) int {
	depth := 0
	for idx := from; idx < len(query); idx++ {
		if next, quoted := cyphertext.SkipToken(query, idx); quoted {
			idx = next - 1
			continue
		}
		ch := query[idx]
		switch ch {
		case '(', '[', '{':
			depth++
			continue
		case ')', ']', '}':
			if depth > 0 {
				depth--
			}
			continue
		}

		if depth == 0 && isCypherClauseStart(query, idx, isName) {
			return idx
		}
	}
	return len(query)
}

// projectionModifierAt reports whether a WITH or RETURN projection's WHERE,
// ORDER BY, SKIP or LIMIT starts at idx.
func projectionModifierAt(query string, idx int) bool {
	for _, kw := range [...]string{"WHERE", "ORDER BY", "SKIP", "LIMIT"} {
		if keywordAt(query, idx, kw) {
			return true
		}
	}
	return false
}

func isCypherClauseStart(query string, idx int, isName ClauseKeywordNameReader) bool {
	for _, kw := range clauseStartKeywords {
		if clauseKeywordAt(query, idx, kw, isName) {
			return true
		}
	}
	return false
}

var clauseStartKeywords = [...]string{
	"OPTIONAL MATCH",
	"DETACH DELETE",
	"ORDER BY",
	"LOAD CSV",
	"USE",
	"MATCH",
	"RETURN",
	"CALL",
	"CREATE",
	"MERGE",
	"UNWIND",
	"WHERE",
	"SET",
	"DELETE",
	"WITH",
	"FOREACH",
	"UNION",
	"LIMIT",
	"SKIP",
}

func skipLeadingSpace(s string, idx int) int {
	for idx < len(s) && cyphertext.IsWhitespaceAt(s, idx) {
		idx++
	}
	return idx
}

func combineColumns(outer, inner []string) []string {
	seen := make(map[string]bool, len(outer))
	result := make([]string, 0, util.SafePreallocSum(len(outer), len(inner)))
	for _, col := range outer {
		seen[col] = true
		result = append(result, col)
	}
	for _, col := range inner {
		if !seen[col] {
			result = append(result, col)
		}
	}
	return result
}

func combineRowsByColumns(resultCols, outerCols []string, outerRow []interface{}, innerCols []string, innerRow []interface{}) []interface{} {
	return combineRowsByIndexes(resultCols, buildColumnIndex(outerCols), outerRow, buildColumnIndex(innerCols), innerRow)
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func buildColumnIndex(cols []string) map[string]int {
	idx := make(map[string]int, len(cols))
	for i, col := range cols {
		if strings.TrimSpace(col) == "" {
			continue
		}
		idx[col] = i
	}
	return idx
}

func combineRowsByIndexes(resultCols []string, outerIdx map[string]int, outerRow []interface{}, innerIdx map[string]int, innerRow []interface{}) []interface{} {
	return newCompiledRowCombiner(resultCols, outerIdx, innerIdx).combine(outerRow, innerRow)
}

type compiledRowCombiner struct {
	fromOuter []int
	fromInner []int
}

func newCompiledRowCombiner(resultCols []string, outerIdx map[string]int, innerIdx map[string]int) compiledRowCombiner {
	c := compiledRowCombiner{
		fromOuter: make([]int, len(resultCols)),
		fromInner: make([]int, len(resultCols)),
	}
	for i := range resultCols {
		c.fromOuter[i] = -1
		c.fromInner[i] = -1
	}
	for i, col := range resultCols {
		if idx, ok := innerIdx[col]; ok {
			c.fromInner[i] = idx
			continue
		}
		if idx, ok := outerIdx[col]; ok {
			c.fromOuter[i] = idx
		}
	}
	return c
}

func (c compiledRowCombiner) combine(outerRow []interface{}, innerRow []interface{}) []interface{} {
	return NewJoinedRowView(outerRow, innerRow, c.fromOuter, c.fromInner).Materialize()
}

// deduplicateRows removes duplicate rows based on string representation.
func deduplicateRows(rows [][]interface{}) [][]interface{} {
	seen := make(map[uint64]struct{}, len(rows))
	result := make([][]interface{}, 0, len(rows))
	for _, row := range rows {
		key := hashRow(row)
		if _, ok := seen[key]; !ok {
			seen[key] = struct{}{}
			result = append(result, row)
		}
	}
	return result
}

func hashRow(row []interface{}) uint64 {
	h := newFNV64a()
	for _, v := range row {
		writeAnyHash(&h, v)
		h.writeByte(0)
	}
	return h.sum64()
}

func writeAnyHash(h *fnv64a, v interface{}) {
	switch t := v.(type) {
	case nil:
		h.writeByte('n')
	case string:
		h.writeByte('s')
		h.writeString(t)
	case bool:
		h.writeByte('b')
		if t {
			h.writeByte(1)
		} else {
			h.writeByte(0)
		}
	case int:
		writeUint64Hash(h, uint64(t), 'i')
	case int8:
		writeUint64Hash(h, uint64(t), 'j')
	case int16:
		writeUint64Hash(h, uint64(t), 'k')
	case int32:
		writeUint64Hash(h, uint64(t), 'l')
	case int64:
		writeUint64Hash(h, uint64(t), 'I')
	case uint:
		writeUint64Hash(h, uint64(t), 'u')
	case uint8:
		writeUint64Hash(h, uint64(t), 'v')
	case uint16:
		writeUint64Hash(h, uint64(t), 'w')
	case uint32:
		writeUint64Hash(h, uint64(t), 'x')
	case uint64:
		writeUint64Hash(h, t, 'U')
	case float32:
		writeUint64Hash(h, uint64(math.Float32bits(t)), 'F')
	case float64:
		writeUint64Hash(h, math.Float64bits(t), 'f')
	case []interface{}:
		h.writeByte('a')
		for _, item := range t {
			writeAnyHash(h, item)
			h.writeByte(0)
		}
	case map[string]interface{}:
		h.writeByte('m')
		keys := make([]string, 0, len(t))
		for k := range t {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			h.writeString(k)
			h.writeByte('=')
			writeAnyHash(h, t[k])
			h.writeByte(0)
		}
	default:
		h.writeByte('x')
		h.writeString(fmt.Sprintf("%T:", t))
		h.writeString(fmt.Sprint(t))
	}
}

func writeUint64Hash(h *fnv64a, n uint64, marker byte) {
	var b [9]byte
	b[0] = marker
	binary.LittleEndian.PutUint64(b[1:], n)
	h.writeBytes(b[:])
}

type fnv64a struct {
	sum uint64
}

func newFNV64a() fnv64a {
	return fnv64a{sum: 14695981039346656037}
}

func (h *fnv64a) writeByte(b byte) {
	const prime uint64 = 1099511628211
	h.sum ^= uint64(b)
	h.sum *= prime
}

func (h *fnv64a) writeBytes(bs []byte) {
	for _, b := range bs {
		h.writeByte(b)
	}
}

func (h *fnv64a) writeString(s string) {
	for i := 0; i < len(s); i++ {
		h.writeByte(s[i])
	}
}

func (h *fnv64a) sum64() uint64 {
	return h.sum
}

func (e *FabricExecutor) executionColumnNames(ctx context.Context, query string, incoming []string) []string {
	var columns []string
	e.executionProjection(ctx, query, incoming).Names.Visit(func(names []string) { columns = names }, func() {})
	return columns
}

// RemoteStatementPolicy is the compiler's exact-source transport intent. A
// remote CALL can have an opaque resolved write mode even with WRITE routing.
type RemoteStatementPolicy struct {
	AccessMode storage.RemoteAccessMode
	Uncertain  bool
}

// SetRemoteStatementPolicy installs the statement compiler's policy reader for
// the actual post-import source sent to a remote. No planner write hint supplies
// a READ proof; an absent compiler reader conservatively selects WRITE.
func (e *FabricExecutor) SetRemoteStatementPolicy(read func(context.Context, string) (RemoteStatementPolicy, error)) {
	e.remoteStatementPolicy = read
}
