package fabric

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strings"
	"sync"
	"unicode"

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
	return &FabricExecutor{
		catalog: catalog,
		local:   local,
		remote:  remote,
	}
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
	// Resolve graph location from catalog.
	loc, err := e.catalog.Resolve(f.GraphName)
	if err != nil {
		return nil, fmt.Errorf("cannot route query: %w", err)
	}

	// Register with the distributed transaction if present.
	if tx != nil {
		participant := participantKeyFromLocation(loc)
		sub, err := tx.GetOrOpen(participant, f.IsWrite)
		if err != nil {
			return nil, err
		}
		ctx = WithFabricTransaction(ctx, tx)
		ctx = WithSubTransaction(ctx, sub)
	}

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

	switch l := loc.(type) {
	case *LocationLocal:
		if e.local == nil {
			return nil, fmt.Errorf("local executor not configured")
		}
		res, err := e.local.ExecuteWithRecord(ctx, l, f.Query, execParams, recordBindings)
		if err != nil {
			return nil, err
		}
		if res != nil && len(res.Columns) == 0 {
			res.Columns = inferReturnColumnsFromQuery(f.Query)
		}
		return res, nil

	case *LocationRemote:
		if e.remote == nil {
			return nil, fmt.Errorf("remote executor not configured")
		}
		res, err := e.remote.Execute(ctx, l, f.Query, execParams, authToken)
		if err != nil {
			return nil, err
		}
		if res != nil && len(res.Columns) == 0 {
			res.Columns = inferReturnColumnsFromQuery(f.Query)
		}
		return res, nil

	default:
		return nil, fmt.Errorf("unsupported location type: %T", loc)
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
		// Stream directly from local/remote executors for FragmentExec to avoid
		// eager result materialization in operator paths.
		loc, execParams, execCtx, err := e.prepareExecDispatch(ctx, tx, f, params)
		if err != nil {
			return nil, nil, err
		}
		switch l := loc.(type) {
		case *LocationLocal:
			if e.local == nil {
				return nil, nil, fmt.Errorf("local executor not configured")
			}
			recordBindings, _ := RecordBindingsFromContext(execCtx)
			cols, it, err := e.local.ExecuteWithRecordRows(execCtx, l, f.Query, execParams, recordBindings)
			if err != nil {
				return nil, nil, err
			}
			return cols, wrapPipelineIterator(ctx, it, 256), nil
		case *LocationRemote:
			if e.remote == nil {
				return nil, nil, fmt.Errorf("remote executor not configured")
			}
			cols, it, err := e.remote.ExecuteRows(execCtx, l, f.Query, execParams, authToken)
			if err != nil {
				return nil, nil, err
			}
			return cols, wrapPipelineIterator(ctx, it, 128), nil
		default:
			return nil, nil, fmt.Errorf("unsupported location type: %T", loc)
		}
	case *FragmentUnion:
		return e.executeUnionRows(ctx, tx, f, params, authToken)
	default:
		res, err := e.Execute(ctx, tx, fragment, params, authToken)
		if err != nil {
			return nil, nil, err
		}
		if res == nil {
			return nil, wrapPipelineIterator(ctx, NewResultRowIterator(nil), 64), nil
		}
		return res.Columns, wrapPipelineIterator(ctx, NewResultRowIterator(res), 64), nil
	}
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

func (e *FabricExecutor) prepareExecDispatch(ctx context.Context, tx *FabricTransaction, f *FragmentExec, params map[string]interface{}) (Location, map[string]interface{}, context.Context, error) {
	loc, err := e.catalog.Resolve(f.GraphName)
	if err != nil {
		return nil, nil, ctx, fmt.Errorf("cannot route query: %w", err)
	}
	if tx != nil {
		participant := participantKeyFromLocation(loc)
		sub, err := tx.GetOrOpen(participant, f.IsWrite)
		if err != nil {
			return nil, nil, ctx, err
		}
		ctx = WithFabricTransaction(ctx, tx)
		ctx = WithSubTransaction(ctx, sub)
	}
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
	return loc, execParams, ctx, nil
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
		Columns: append([]string(nil), columns...),
		Rows:    rows,
	}, nil
}

func inferReturnColumnsFromQuery(query string) []string {
	returnIdx := lastKeywordIndexFold(query, "RETURN")
	if returnIdx < 0 {
		return nil
	}
	clause := strings.TrimSpace(query[returnIdx+len("RETURN"):])
	if clause == "" {
		return nil
	}
	// Trim trailing semicolon.
	clause = strings.TrimSuffix(clause, ";")
	// Strip top-level ORDER BY / SKIP / LIMIT tails.
	end := len(clause)
	paren, bracket, brace := 0, 0, 0
	inSingle, inDouble, inBacktick := false, false, false
	for i := 0; i < len(clause); i++ {
		ch := clause[i]
		switch {
		case inSingle:
			if ch == '\'' {
				inSingle = false
			}
			continue
		case inDouble:
			if ch == '"' {
				inDouble = false
			}
			continue
		case inBacktick:
			if ch == '`' {
				inBacktick = false
			}
			continue
		}
		switch ch {
		case '\'':
			inSingle = true
		case '"':
			inDouble = true
		case '`':
			inBacktick = true
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
		if hasKeywordAt(clause, i, "ORDER BY") || hasKeywordAt(clause, i, "SKIP") || hasKeywordAt(clause, i, "LIMIT") {
			end = i
			break
		}
	}
	clause = strings.TrimSpace(clause[:end])
	parts := splitTopLevelCSV(clause)
	cols := make([]string, 0, len(parts))
	for _, p := range parts {
		item := strings.TrimSpace(p)
		if item == "" {
			continue
		}
		if as := lastAsIndexFold(item); as >= 0 {
			alias := strings.TrimSpace(item[as+4:])
			if alias != "" {
				cols = append(cols, strings.Trim(alias, "`"))
				continue
			}
		}
		cols = append(cols, item)
	}
	return cols
}

func lastKeywordIndexFold(s, keyword string) int {
	if len(keyword) == 0 || len(s) < len(keyword) {
		return -1
	}
	for i := len(s) - len(keyword); i >= 0; i-- {
		if hasKeywordAt(s, i, keyword) {
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
	inSingle, inDouble, inBacktick := false, false, false
	for i := 0; i < len(s); i++ {
		ch := s[i]
		switch {
		case inSingle:
			if ch == '\'' {
				inSingle = false
			}
			continue
		case inDouble:
			if ch == '"' {
				inDouble = false
			}
			continue
		case inBacktick:
			if ch == '`' {
				inBacktick = false
			}
			continue
		}
		switch ch {
		case '\'':
			inSingle = true
		case '"':
			inDouble = true
		case '`':
			inBacktick = true
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

func participantKeyFromLocation(loc Location) string {
	switch l := loc.(type) {
	case *LocationLocal:
		return "local:" + strings.TrimSpace(l.DBName)
	case *LocationRemote:
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
	if inputIter == nil {
		inputIter = NewResultRowIterator(nil)
	}

	// For non-simple leading WITH pipelines (e.g. trailing WITH collect(...) after
	// CALL blocks), execute once over the full input row stream instead of per-row.
	if execFrag, ok := f.Inner.(*FragmentExec); ok {
		// Streaming fast path: avoid full outer materialization when correlated
		// batched row lookup rewrite can consume the iterator directly.
		if streamed, handled, err := e.tryExecuteApplyBatchedLookupRowsIter(ctx, tx, inputCols, inputIter, execFrag, params, authToken); handled {
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
		if len(inputResult.Rows) <= fabricApplyInMemoryMaxRows {
			if streamRes, handled := executeApplyInMemoryProjection(inputResult, execFrag.Query); handled {
				return streamRes, nil
			}
		}
		if batched, handled, err := e.tryExecuteApplyBatchedCollectLookup(ctx, tx, inputResult, execFrag, params, authToken); handled {
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
		if len(inputResult.Rows) <= fabricApplyInMemoryMaxRows {
			if piped, handled, err := e.executeApplyAsPipeline(ctx, tx, inputResult, execFrag, params, authToken); handled {
				if err != nil {
					return nil, fmt.Errorf("apply inner failed: %w", err)
				}
				return piped, nil
			}
		}
		// Re-seed iterator for the generic correlated APPLY fallback.
		inputCols = inputResult.Columns
		inputIter = NewResultRowIterator(inputResult)
	}

	result := &ResultStream{Columns: append([]string(nil), f.Columns...)}
	innerFragment := f.Inner
	outerIdx := buildColumnIndex(inputCols)

	// For each input row, execute the inner fragment with imported variables.
	parentBindings, _ := RecordBindingsFromContext(ctx)
	for inputIter.Next() {
		inputRow := inputIter.Row()
		mergedBind := bindingsFromParentAndRow(parentBindings, inputCols, inputRow)
		innerCtx := WithRecordBindings(ctx, mergedBind)

		if execFrag, ok := innerFragment.(*FragmentExec); ok {
			if cols, projected, ok := projectSimpleReturnFromRow(execFrag.Query, inputCols, inputRow); ok {
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
		if len(innerResult.Rows) == 0 {
			if execFrag, ok := innerFragment.(*FragmentExec); ok {
				if cols, row, ok := synthesizeEmptyCollectOnlyReturn(execFrag.Query); ok {
					innerResult.Columns = cols
					innerResult.Rows = [][]interface{}{row}
				}
			}
		}

		// Combine input and inner columns/rows.
		if len(result.Columns) == 0 {
			result.Columns = combineColumns(inputCols, innerResult.Columns)
		}
		if len(innerResult.Rows) == 0 {
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

	return result, nil
}

// tryExecuteApplyBatchedLookupRowsIter is a streaming variant of
// tryExecuteApplyBatchedLookupRows that consumes outer rows from an iterator,
// chunking outer rows and avoiding full outer result materialization.
func (e *FabricExecutor) tryExecuteApplyBatchedLookupRowsIter(
	ctx context.Context,
	tx *FabricTransaction,
	inputCols []string,
	inputIter RowIterator,
	inner *FragmentExec,
	params map[string]interface{},
	authToken string,
) (*ResultStream, bool, error) {
	if len(inputCols) == 0 || inputIter == nil || inner == nil {
		return nil, false, nil
	}

	trimmed := strings.TrimSpace(inner.Query)
	if !startsWithFold(trimmed, "WITH ") || !containsFold(trimmed, "RETURN ") || containsFold(trimmed, "RETURN collect(") || queryIsWrite(trimmed) {
		return nil, false, nil
	}

	m := reApplyLookupRows.FindStringSubmatch(trimmed)
	if len(m) != 6 {
		return nil, false, nil
	}
	importCol := strings.TrimSpace(m[1])
	useClause := strings.TrimSpace(m[2])
	matchPart := strings.TrimSpace(m[3])
	wherePart := strings.TrimSpace(m[4])
	returnPart := strings.TrimSpace(m[5])
	if importCol == "" || wherePart == "" || returnPart == "" {
		return nil, false, nil
	}

	matchVar, matchProp, otherWhere, ok := extractApplyCorrelationWhere(wherePart, importCol)
	if !ok {
		return nil, false, nil
	}
	otherWhere, ok = sanitizeOtherWhereForImportColumn(otherWhere, importCol)
	if !ok {
		return nil, false, nil
	}

	returnItems, ok := parseSimpleBatchedLookupReturnItems(returnPart, matchVar)
	if !ok || len(returnItems) == 0 {
		return nil, false, nil
	}

	outerIdx := buildColumnIndex(inputCols)
	keyIdx, exists := outerIdx[importCol]
	if !exists {
		return nil, false, nil
	}
	innerCols := aliasesFromReturnItems(returnItems)
	innerIdx := buildColumnIndex(innerCols)
	result := &ResultStream{
		Columns: combineColumns(inputCols, innerCols),
		Rows:    make([][]interface{}, 0, 4096),
	}
	markApplyBatchedLookupRows(ctx)
	combiner := newCompiledRowCombiner(result.Columns, outerIdx, innerIdx)

	var rewritten strings.Builder
	if useClause != "" {
		rewritten.WriteString(useClause)
		rewritten.WriteByte(' ')
	}
	rewritten.WriteString("MATCH ")
	rewritten.WriteString(matchPart)
	rewritten.WriteString(" WHERE ")
	rewritten.WriteString(matchVar)
	rewritten.WriteByte('.')
	rewritten.WriteString(matchProp)
	rewritten.WriteString(" IN $__fabric_apply_keys")
	if otherWhere != "" {
		rewritten.WriteString(" AND ")
		rewritten.WriteString(otherWhere)
	}
	rewritten.WriteString(" RETURN ")
	rewritten.WriteString(matchVar)
	rewritten.WriteByte('.')
	rewritten.WriteString(matchProp)
	rewritten.WriteString(" AS __fabric_apply_key")
	for _, item := range returnItems {
		rewritten.WriteString(", ")
		rewritten.WriteString(matchVar)
		rewritten.WriteByte('.')
		rewritten.WriteString(item.prop)
		rewritten.WriteString(" AS ")
		rewritten.WriteString(item.alias)
	}
	lookupQuery := rewritten.String()

	const outerChunkSize = 1024
	chunkRows := make([][]interface{}, 0, outerChunkSize)

	processChunk := func(rows [][]interface{}) error {
		if len(rows) == 0 {
			return nil
		}
		distinctKeys := make([]interface{}, 0, minInt(len(rows), fabricApplyLookupBatchSize))
		seen := make(map[string]struct{}, len(rows))
		outerKeys := make([]string, len(rows))
		outerHasKey := make([]bool, len(rows))
		for i, row := range rows {
			if keyIdx >= len(row) {
				continue
			}
			key := row[keyIdx]
			if key == nil {
				continue
			}
			k := applyLookupKeyString(key)
			outerKeys[i] = k
			outerHasKey[i] = true
			if _, dup := seen[k]; dup {
				continue
			}
			seen[k] = struct{}{}
			distinctKeys = append(distinctKeys, key)
		}
		if len(distinctKeys) == 0 {
			return nil
		}

		groupedRows := make(map[string][][]interface{}, len(distinctKeys))
		executeKeyChunk := func(execCtx context.Context, keys []interface{}) error {
			batchParams := make(map[string]interface{}, util.SafePreallocSum(len(params), 1))
			for k, v := range params {
				batchParams[k] = v
			}
			batchParams["__fabric_apply_keys"] = keys

			batchFrag := *inner
			batchFrag.Query = lookupQuery
			batchResult, err := e.Execute(execCtx, tx, &batchFrag, batchParams, authToken)
			if err != nil {
				return err
			}

			batchIdx := buildColumnIndex(batchResult.Columns)
			keyColIdx, okKey := batchIdx["__fabric_apply_key"]
			if !okKey {
				return fmt.Errorf("batched APPLY row lookup produced unexpected columns: %v", batchResult.Columns)
			}
			for _, row := range batchResult.Rows {
				if keyColIdx >= len(row) {
					continue
				}
				innerRow := make([]interface{}, 0, len(returnItems))
				for _, item := range returnItems {
					idx, ok := batchIdx[item.alias]
					if !ok || idx >= len(row) {
						innerRow = append(innerRow, nil)
						continue
					}
					innerRow = append(innerRow, row[idx])
				}
				k := applyLookupKeyString(row[keyColIdx])
				groupedRows[k] = append(groupedRows[k], innerRow)
			}
			return nil
		}

		if len(distinctKeys) <= fabricApplyLookupBatchSize {
			if err := executeKeyChunk(ctx, distinctKeys); err != nil {
				return err
			}
		} else {
			for start := 0; start < len(distinctKeys); start += fabricApplyLookupBatchSize {
				end := minInt(start+fabricApplyLookupBatchSize, len(distinctKeys))
				if err := executeKeyChunk(ctx, distinctKeys[start:end]); err != nil {
					return err
				}
			}
		}

		for i, outerRow := range rows {
			if !outerHasKey[i] {
				continue
			}
			matches := groupedRows[outerKeys[i]]
			for _, innerRow := range matches {
				result.Rows = append(result.Rows, combiner.combine(outerRow, innerRow))
			}
		}
		return nil
	}

	for inputIter.Next() {
		row := inputIter.Row()
		if row == nil {
			continue
		}
		rowCopy := make([]interface{}, len(row))
		copy(rowCopy, row)
		chunkRows = append(chunkRows, rowCopy)
		if len(chunkRows) >= outerChunkSize {
			if err := processChunk(chunkRows); err != nil {
				return nil, true, err
			}
			chunkRows = chunkRows[:0]
		}
	}
	if err := inputIter.Err(); err != nil {
		return nil, true, fmt.Errorf("apply input failed: %w", err)
	}
	if err := processChunk(chunkRows); err != nil {
		return nil, true, err
	}
	return result, true, nil
}

type applyCorrelatedLookupSpec struct {
	importCol        string
	useClause        string
	matchPart        string
	wherePart        string
	returnProjection string
	returnModifiers  string
}

var reApplyLookupRows = regexp.MustCompile(`(?is)^WITH\s+([A-Za-z_][A-Za-z0-9_]*)\s+((?:USE\s+[A-Za-z0-9_.` + "`" + `]+\s+)?)MATCH\s+(.+?)\s+WHERE\s+(.+?)\s+RETURN\s+(.+?)\s*;?\s*$`)

type fabricReturnItem struct {
	expr  string
	prop  string
	alias string
}

type fabricCountReturnItem struct {
	expr  string
	alias string
}

// tryExecuteApplyBatchedCollectLookup optimizes a common correlated-subquery pattern:
//
//	WITH key USE ... MATCH (...) WHERE n.prop = key RETURN collect(...) AS texts
//
// by executing a single batched IN lookup and remapping results per outer row.
func (e *FabricExecutor) tryExecuteApplyBatchedCollectLookup(
	ctx context.Context,
	tx *FabricTransaction,
	inputResult *ResultStream,
	inner *FragmentExec,
	params map[string]interface{},
	authToken string,
) (*ResultStream, bool, error) {
	if inputResult == nil || len(inputResult.Rows) == 0 || len(inputResult.Columns) == 0 || inner == nil {
		return nil, false, nil
	}

	trimmed := strings.TrimSpace(inner.Query)
	if !startsWithFold(trimmed, "WITH ") || !containsFold(trimmed, "RETURN collect(") || queryIsWrite(trimmed) {
		return nil, false, nil
	}

	spec, ok := parseApplyCorrelatedLookupSubquery(trimmed)
	if !ok || spec.importCol == "" || strings.TrimSpace(spec.wherePart) == "" || strings.TrimSpace(spec.returnModifiers) != "" {
		return nil, false, nil
	}
	collectExp, outAlias, ok := parseSimpleCollectReturnItem(spec.returnProjection)
	if !ok {
		return nil, false, nil
	}

	matchVar, matchProp, otherWhere, ok := extractApplyCorrelationWhere(spec.wherePart, spec.importCol)
	if !ok {
		return nil, false, nil
	}
	otherWhere, ok = sanitizeOtherWhereForImportColumn(otherWhere, spec.importCol)
	if !ok {
		return nil, false, nil
	}

	outerIdx := buildColumnIndex(inputResult.Columns)
	keyIdx, exists := outerIdx[spec.importCol]
	if !exists {
		return nil, false, nil
	}

	distinctKeys := make([]interface{}, 0, minInt(len(inputResult.Rows), fabricApplyLookupBatchSize))
	seen := make(map[string]struct{}, len(inputResult.Rows))
	grouped := make(map[string]interface{}, len(inputResult.Rows))
	outerKeys := make([]string, len(inputResult.Rows))

	for i, row := range inputResult.Rows {
		keyStr := applyLookupKeyString(nil)
		if keyIdx >= len(row) {
			outerKeys[i] = keyStr
			continue
		}
		key := row[keyIdx]
		if key == nil {
			outerKeys[i] = keyStr
			continue
		}
		keyStr = applyLookupKeyString(key)
		outerKeys[i] = keyStr
		if _, dup := seen[keyStr]; dup {
			continue
		}
		seen[keyStr] = struct{}{}
		distinctKeys = append(distinctKeys, key)
	}
	if len(seen) == 0 {
		result := &ResultStream{
			Columns: combineColumns(inputResult.Columns, []string{outAlias}),
			Rows:    make([][]interface{}, 0, len(inputResult.Rows)),
		}
		innerCols := []string{outAlias}
		innerIdx := buildColumnIndex(innerCols)
		for _, outerRow := range inputResult.Rows {
			innerRow := []interface{}{[]interface{}{}}
			result.Rows = append(result.Rows, combineRowsByIndexes(result.Columns, outerIdx, outerRow, innerIdx, innerRow))
		}
		return result, true, nil
	}
	var groupedMu sync.Mutex
	ctxBatch, cancelBatch := context.WithCancel(ctx)
	defer cancelBatch()
	sem := make(chan struct{}, fabricApplyLookupMaxConcurrency)
	errCh := make(chan error, 1)
	var wg sync.WaitGroup
	for start := 0; start < len(distinctKeys); start += fabricApplyLookupBatchSize {
		end := minInt(start+fabricApplyLookupBatchSize, len(distinctKeys))
		chunk := append([]interface{}(nil), distinctKeys[start:end]...)
		wg.Add(1)
		go func(chunk []interface{}) {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
			case <-ctxBatch.Done():
				return
			}
			defer func() { <-sem }()

			var rewritten strings.Builder
			if spec.useClause != "" {
				rewritten.WriteString(spec.useClause)
				rewritten.WriteByte(' ')
			}
			rewritten.WriteString("MATCH ")
			rewritten.WriteString(spec.matchPart)
			rewritten.WriteString(" WHERE ")
			rewritten.WriteString(matchVar)
			rewritten.WriteByte('.')
			rewritten.WriteString(matchProp)
			rewritten.WriteString(" IN $__fabric_apply_keys")
			if otherWhere != "" {
				rewritten.WriteString(" AND ")
				rewritten.WriteString(otherWhere)
			}
			rewritten.WriteString(" RETURN ")
			rewritten.WriteString(matchVar)
			rewritten.WriteByte('.')
			rewritten.WriteString(matchProp)
			rewritten.WriteString(" AS __fabric_apply_key, collect(")
			rewritten.WriteString(collectExp)
			rewritten.WriteString(") AS ")
			rewritten.WriteString(outAlias)

			batchParams := make(map[string]interface{}, util.SafePreallocSum(len(params), 1))
			for k, v := range params {
				batchParams[k] = v
			}
			batchParams["__fabric_apply_keys"] = chunk

			batchFrag := *inner
			batchFrag.Query = rewritten.String()
			batchResult, err := e.Execute(ctxBatch, tx, &batchFrag, batchParams, authToken)
			if err != nil {
				select {
				case errCh <- err:
					cancelBatch()
				default:
				}
				return
			}

			batchIdx := buildColumnIndex(batchResult.Columns)
			keyColIdx, okKey := batchIdx["__fabric_apply_key"]
			valColIdx, okVal := batchIdx[outAlias]
			if !okKey || !okVal {
				select {
				case errCh <- fmt.Errorf("batched APPLY lookup produced unexpected columns: %v", batchResult.Columns):
					cancelBatch()
				default:
				}
				return
			}
			localGrouped := make(map[string]interface{}, len(batchResult.Rows))
			for _, row := range batchResult.Rows {
				if keyColIdx >= len(row) || valColIdx >= len(row) {
					continue
				}
				localGrouped[applyLookupKeyString(row[keyColIdx])] = row[valColIdx]
			}
			groupedMu.Lock()
			for k, v := range localGrouped {
				grouped[k] = v
			}
			groupedMu.Unlock()
		}(chunk)
	}
	wg.Wait()
	select {
	case err := <-errCh:
		return nil, true, err
	default:
	}

	result := &ResultStream{
		Columns: combineColumns(inputResult.Columns, []string{outAlias}),
		Rows:    make([][]interface{}, 0, len(inputResult.Rows)),
	}
	innerCols := []string{outAlias}
	innerIdx := buildColumnIndex(innerCols)
	combiner := newCompiledRowCombiner(result.Columns, outerIdx, innerIdx)
	for i, outerRow := range inputResult.Rows {
		val, exists := grouped[outerKeys[i]]
		if !exists || val == nil {
			val = []interface{}{}
		}
		innerRow := []interface{}{val}
		result.Rows = append(result.Rows, combiner.combine(outerRow, innerRow))
	}

	return result, true, nil
}

// tryExecuteApplyBatchedCountLookup optimizes correlated scalar-aggregate COUNT subqueries:
//
//	WITH key USE ... MATCH (...) WHERE n.prop = key RETURN count(*) AS c
//
// by rewriting to batched IN lookups with GROUP BY correlation key and then
// remapping grouped counts to each outer row.
func (e *FabricExecutor) tryExecuteApplyBatchedCountLookup(
	ctx context.Context,
	tx *FabricTransaction,
	inputResult *ResultStream,
	inner *FragmentExec,
	params map[string]interface{},
	authToken string,
) (*ResultStream, bool, error) {
	if inputResult == nil || len(inputResult.Rows) == 0 || len(inputResult.Columns) == 0 || inner == nil {
		return nil, false, nil
	}

	trimmed := strings.TrimSpace(inner.Query)
	if !startsWithFold(trimmed, "WITH ") || !containsFold(trimmed, "RETURN ") || !containsFold(trimmed, "count(") || queryIsWrite(trimmed) {
		return nil, false, nil
	}

	spec, ok := parseApplyCorrelatedLookupSubquery(trimmed)
	if !ok || spec.importCol == "" || strings.TrimSpace(spec.wherePart) == "" || strings.TrimSpace(spec.returnProjection) == "" || strings.TrimSpace(spec.returnModifiers) != "" {
		return nil, false, nil
	}

	matchVar, matchProp, otherWhere, ok := extractApplyCorrelationWhere(spec.wherePart, spec.importCol)
	if !ok {
		return nil, false, nil
	}
	otherWhere, ok = sanitizeOtherWhereForImportColumn(otherWhere, spec.importCol)
	if !ok {
		return nil, false, nil
	}

	countItems, ok := parseSimpleBatchedCountReturnItems(spec.returnProjection)
	if !ok || len(countItems) == 0 {
		return nil, false, nil
	}

	outerIdx := buildColumnIndex(inputResult.Columns)
	keyIdx, exists := outerIdx[spec.importCol]
	if !exists {
		return nil, false, nil
	}

	distinctKeys := make([]interface{}, 0, minInt(len(inputResult.Rows), fabricApplyLookupBatchSize))
	seen := make(map[string]struct{}, len(inputResult.Rows))
	grouped := make(map[string][]interface{}, len(inputResult.Rows))
	outerKeys := make([]string, len(inputResult.Rows))
	for i, row := range inputResult.Rows {
		keyStr := applyLookupKeyString(nil)
		if keyIdx >= len(row) {
			outerKeys[i] = keyStr
			continue
		}
		key := row[keyIdx]
		if key == nil {
			outerKeys[i] = keyStr
			continue
		}
		keyStr = applyLookupKeyString(key)
		outerKeys[i] = keyStr
		if _, dup := seen[keyStr]; dup {
			continue
		}
		seen[keyStr] = struct{}{}
		distinctKeys = append(distinctKeys, key)
	}
	if len(seen) == 0 {
		result := &ResultStream{
			Columns: combineColumns(inputResult.Columns, aliasesFromCountReturnItems(countItems)),
			Rows:    make([][]interface{}, 0, len(inputResult.Rows)),
		}
		innerCols := aliasesFromCountReturnItems(countItems)
		innerIdx := buildColumnIndex(innerCols)
		zeroes := zeroCountValues(len(countItems))
		for _, outerRow := range inputResult.Rows {
			result.Rows = append(result.Rows, combineRowsByIndexes(result.Columns, outerIdx, outerRow, innerIdx, zeroes))
		}
		return result, true, nil
	}

	var groupedMu sync.Mutex
	ctxBatch, cancelBatch := context.WithCancel(ctx)
	defer cancelBatch()
	sem := make(chan struct{}, fabricApplyLookupMaxConcurrency)
	errCh := make(chan error, 1)
	var wg sync.WaitGroup
	for start := 0; start < len(distinctKeys); start += fabricApplyLookupBatchSize {
		end := minInt(start+fabricApplyLookupBatchSize, len(distinctKeys))
		chunk := append([]interface{}(nil), distinctKeys[start:end]...)
		wg.Add(1)
		go func(chunk []interface{}) {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
			case <-ctxBatch.Done():
				return
			}
			defer func() { <-sem }()

			var rewritten strings.Builder
			if spec.useClause != "" {
				rewritten.WriteString(spec.useClause)
				rewritten.WriteByte(' ')
			}
			rewritten.WriteString("MATCH ")
			rewritten.WriteString(spec.matchPart)
			rewritten.WriteString(" WHERE ")
			rewritten.WriteString(matchVar)
			rewritten.WriteByte('.')
			rewritten.WriteString(matchProp)
			rewritten.WriteString(" IN $__fabric_apply_keys")
			if otherWhere != "" {
				rewritten.WriteString(" AND ")
				rewritten.WriteString(otherWhere)
			}
			rewritten.WriteString(" RETURN ")
			rewritten.WriteString(matchVar)
			rewritten.WriteByte('.')
			rewritten.WriteString(matchProp)
			rewritten.WriteString(" AS __fabric_apply_key")
			for _, item := range countItems {
				rewritten.WriteString(", ")
				rewritten.WriteString(item.expr)
				rewritten.WriteString(" AS ")
				rewritten.WriteString(item.alias)
			}
			batchParams := make(map[string]interface{}, util.SafePreallocSum(len(params), 1))
			for k, v := range params {
				batchParams[k] = v
			}
			batchParams["__fabric_apply_keys"] = chunk

			batchFrag := *inner
			batchFrag.Query = rewritten.String()
			batchResult, err := e.Execute(ctxBatch, tx, &batchFrag, batchParams, authToken)
			if err != nil {
				select {
				case errCh <- err:
					cancelBatch()
				default:
				}
				return
			}

			batchIdx := buildColumnIndex(batchResult.Columns)
			keyColIdx, ok := batchIdx["__fabric_apply_key"]
			if !ok {
				select {
				case errCh <- fmt.Errorf("batched APPLY count lookup produced unexpected columns: %v", batchResult.Columns):
					cancelBatch()
				default:
				}
				return
			}
			localGrouped := make(map[string][]interface{}, len(batchResult.Rows))
			for _, row := range batchResult.Rows {
				if keyColIdx >= len(row) {
					continue
				}
				values := make([]interface{}, 0, len(countItems))
				for _, item := range countItems {
					idx, ok := batchIdx[item.alias]
					if !ok || idx >= len(row) {
						values = append(values, int64(0))
						continue
					}
					values = append(values, row[idx])
				}
				localGrouped[applyLookupKeyString(row[keyColIdx])] = values
			}
			groupedMu.Lock()
			for k, v := range localGrouped {
				grouped[k] = v
			}
			groupedMu.Unlock()
		}(chunk)
	}
	wg.Wait()
	select {
	case err := <-errCh:
		return nil, true, err
	default:
	}

	innerCols := aliasesFromCountReturnItems(countItems)
	innerIdx := buildColumnIndex(innerCols)
	zeroes := zeroCountValues(len(countItems))
	result := &ResultStream{
		Columns: combineColumns(inputResult.Columns, innerCols),
		Rows:    make([][]interface{}, 0, len(inputResult.Rows)),
	}
	combiner := newCompiledRowCombiner(result.Columns, outerIdx, innerIdx)
	for i, outerRow := range inputResult.Rows {
		values, ok := grouped[outerKeys[i]]
		if !ok {
			values = zeroes
		}
		result.Rows = append(result.Rows, combiner.combine(outerRow, values))
	}
	return result, true, nil
}

func applyLookupKeyString(v interface{}) string {
	switch x := v.(type) {
	case nil:
		return "<nil>"
	case string:
		return "s:" + normalizeLookupString(x)
	case []byte:
		return "s:" + normalizeLookupString(string(x))
	case int:
		return fmt.Sprintf("i:%d", x)
	case int64:
		return fmt.Sprintf("i64:%d", x)
	case float64:
		return fmt.Sprintf("f:%g", x)
	case bool:
		if x {
			return "b:1"
		}
		return "b:0"
	default:
		return fmt.Sprintf("%T:%v", v, v)
	}
}

func normalizeLookupString(s string) string {
	s = strings.TrimSpace(s)
	// Some ingest paths preserve wrapped quotes in string payloads; normalize both
	// sides so correlated joins remain stable across equivalent values.
	s = strings.Trim(s, `"`)
	return s
}

// tryExecuteApplyBatchedLookupRows optimizes a correlated row-producing subquery pattern:
//
//	WITH key USE ... MATCH (...) WHERE n.prop = key RETURN n.a AS a, n.b AS b
//
// by executing batched IN lookups and combining rows without per-outer-row re-execution.
func (e *FabricExecutor) tryExecuteApplyBatchedLookupRows(
	ctx context.Context,
	tx *FabricTransaction,
	inputResult *ResultStream,
	inner *FragmentExec,
	params map[string]interface{},
	authToken string,
) (*ResultStream, bool, error) {
	if inputResult == nil || len(inputResult.Rows) == 0 || len(inputResult.Columns) == 0 || inner == nil {
		return nil, false, nil
	}

	trimmed := strings.TrimSpace(inner.Query)
	if !startsWithFold(trimmed, "WITH ") || !containsFold(trimmed, "RETURN ") || containsFold(trimmed, "RETURN collect(") || queryIsWrite(trimmed) {
		return nil, false, nil
	}

	spec, ok := parseApplyCorrelatedLookupSubquery(trimmed)
	if !ok || spec.importCol == "" || strings.TrimSpace(spec.wherePart) == "" || strings.TrimSpace(spec.returnProjection) == "" {
		return nil, false, nil
	}
	matchVar, matchProp, otherWhere, ok := extractApplyCorrelationWhere(spec.wherePart, spec.importCol)
	if !ok {
		return nil, false, nil
	}
	otherWhere, ok = sanitizeOtherWhereForImportColumn(otherWhere, spec.importCol)
	if !ok {
		return nil, false, nil
	}

	returnItems, ok := parseSimpleBatchedLookupReturnItems(spec.returnProjection, matchVar)
	if !ok || len(returnItems) == 0 {
		return nil, false, nil
	}

	outerIdx := buildColumnIndex(inputResult.Columns)
	keyIdx, exists := outerIdx[spec.importCol]
	if !exists {
		return nil, false, nil
	}
	innerCols := aliasesFromReturnItems(returnItems)
	innerIdx := buildColumnIndex(innerCols)
	result := &ResultStream{
		Columns: combineColumns(inputResult.Columns, innerCols),
		Rows:    make([][]interface{}, 0, minInt(len(inputResult.Rows)*2, 4096)),
	}
	markApplyBatchedLookupRows(ctx)
	combiner := newCompiledRowCombiner(result.Columns, outerIdx, innerIdx)

	var rewritten strings.Builder
	if spec.useClause != "" {
		rewritten.WriteString(spec.useClause)
		rewritten.WriteByte(' ')
	}
	rewritten.WriteString("MATCH ")
	rewritten.WriteString(spec.matchPart)
	rewritten.WriteString(" WHERE ")
	rewritten.WriteString(matchVar)
	rewritten.WriteByte('.')
	rewritten.WriteString(matchProp)
	rewritten.WriteString(" IN $__fabric_apply_keys")
	if otherWhere != "" {
		rewritten.WriteString(" AND ")
		rewritten.WriteString(otherWhere)
	}
	rewritten.WriteString(" RETURN ")
	rewritten.WriteString(matchVar)
	rewritten.WriteByte('.')
	rewritten.WriteString(matchProp)
	rewritten.WriteString(" AS __fabric_apply_key")
	for _, item := range returnItems {
		rewritten.WriteString(", ")
		rewritten.WriteString(matchVar)
		rewritten.WriteByte('.')
		rewritten.WriteString(item.prop)
		rewritten.WriteString(" AS ")
		rewritten.WriteString(item.alias)
	}
	lookupQuery := rewritten.String()

	const outerChunkSize = 1024
	for rowStart := 0; rowStart < len(inputResult.Rows); rowStart += outerChunkSize {
		rowEnd := minInt(rowStart+outerChunkSize, len(inputResult.Rows))
		chunkRows := inputResult.Rows[rowStart:rowEnd]

		distinctKeys := make([]interface{}, 0, minInt(len(chunkRows), fabricApplyLookupBatchSize))
		seen := make(map[string]struct{}, len(chunkRows))
		outerKeys := make([]string, len(chunkRows))
		outerHasKey := make([]bool, len(chunkRows))
		for i, row := range chunkRows {
			if keyIdx >= len(row) {
				continue
			}
			key := row[keyIdx]
			if key == nil {
				continue
			}
			k := applyLookupKeyString(key)
			outerKeys[i] = k
			outerHasKey[i] = true
			if _, dup := seen[k]; dup {
				continue
			}
			seen[k] = struct{}{}
			distinctKeys = append(distinctKeys, key)
		}
		if len(distinctKeys) == 0 {
			continue
		}

		groupedRows := make(map[string][][]interface{}, len(distinctKeys))
		executeKeyChunk := func(execCtx context.Context, keys []interface{}) error {
			batchParams := make(map[string]interface{}, util.SafePreallocSum(len(params), 1))
			for k, v := range params {
				batchParams[k] = v
			}
			batchParams["__fabric_apply_keys"] = keys

			batchFrag := *inner
			batchFrag.Query = lookupQuery
			batchResult, err := e.Execute(execCtx, tx, &batchFrag, batchParams, authToken)
			if err != nil {
				return err
			}

			batchIdx := buildColumnIndex(batchResult.Columns)
			keyColIdx, okKey := batchIdx["__fabric_apply_key"]
			if !okKey {
				return fmt.Errorf("batched APPLY row lookup produced unexpected columns: %v", batchResult.Columns)
			}
			for _, row := range batchResult.Rows {
				if keyColIdx >= len(row) {
					continue
				}
				innerRow := make([]interface{}, 0, len(returnItems))
				for _, item := range returnItems {
					idx, ok := batchIdx[item.alias]
					if !ok || idx >= len(row) {
						innerRow = append(innerRow, nil)
						continue
					}
					innerRow = append(innerRow, row[idx])
				}
				k := applyLookupKeyString(row[keyColIdx])
				groupedRows[k] = append(groupedRows[k], innerRow)
			}
			return nil
		}

		if len(distinctKeys) <= fabricApplyLookupBatchSize {
			if err := executeKeyChunk(ctx, distinctKeys); err != nil {
				return nil, true, err
			}
		} else {
			var groupedMu sync.Mutex
			ctxBatch, cancelBatch := context.WithCancel(ctx)
			sem := make(chan struct{}, fabricApplyLookupMaxConcurrency)
			errCh := make(chan error, 1)
			var wg sync.WaitGroup
			for start := 0; start < len(distinctKeys); start += fabricApplyLookupBatchSize {
				end := minInt(start+fabricApplyLookupBatchSize, len(distinctKeys))
				keyChunk := append([]interface{}(nil), distinctKeys[start:end]...)
				wg.Add(1)
				go func(keys []interface{}) {
					defer wg.Done()
					select {
					case sem <- struct{}{}:
					case <-ctxBatch.Done():
						return
					}
					defer func() { <-sem }()

					localGrouped := make(map[string][][]interface{})
					localExec := func(execCtx context.Context, keys []interface{}) error {
						batchParams := make(map[string]interface{}, util.SafePreallocSum(len(params), 1))
						for k, v := range params {
							batchParams[k] = v
						}
						batchParams["__fabric_apply_keys"] = keys

						batchFrag := *inner
						batchFrag.Query = lookupQuery
						batchResult, err := e.Execute(execCtx, tx, &batchFrag, batchParams, authToken)
						if err != nil {
							return err
						}

						batchIdx := buildColumnIndex(batchResult.Columns)
						keyColIdx, okKey := batchIdx["__fabric_apply_key"]
						if !okKey {
							return fmt.Errorf("batched APPLY row lookup produced unexpected columns: %v", batchResult.Columns)
						}
						for _, row := range batchResult.Rows {
							if keyColIdx >= len(row) {
								continue
							}
							innerRow := make([]interface{}, 0, len(returnItems))
							for _, item := range returnItems {
								idx, ok := batchIdx[item.alias]
								if !ok || idx >= len(row) {
									innerRow = append(innerRow, nil)
									continue
								}
								innerRow = append(innerRow, row[idx])
							}
							k := applyLookupKeyString(row[keyColIdx])
							localGrouped[k] = append(localGrouped[k], innerRow)
						}
						return nil
					}

					if err := localExec(ctxBatch, keys); err != nil {
						select {
						case errCh <- err:
							cancelBatch()
						default:
						}
						return
					}

					groupedMu.Lock()
					for k, rows := range localGrouped {
						groupedRows[k] = append(groupedRows[k], rows...)
					}
					groupedMu.Unlock()
				}(keyChunk)
			}
			wg.Wait()
			cancelBatch()
			select {
			case err := <-errCh:
				return nil, true, err
			default:
			}
		}

		if strings.TrimSpace(spec.returnModifiers) != "" {
			orderAliasMap := buildResultModifierAliasMap(returnItems)
			for k, rows := range groupedRows {
				rs := &ResultStream{
					Columns: innerCols,
					Rows:    rows,
				}
				applySimpleResultModifiers(rs, spec.returnModifiers, orderAliasMap)
				groupedRows[k] = rs.Rows
			}
		}

		for i, outerRow := range chunkRows {
			if !outerHasKey[i] {
				continue
			}
			matches := groupedRows[outerKeys[i]]
			if len(matches) == 0 {
				continue
			}
			for _, innerRow := range matches {
				result.Rows = append(result.Rows, combiner.combine(outerRow, innerRow))
			}
		}
	}

	return result, true, nil
}

func parseSimpleBatchedLookupReturnItems(returnPart, matchVar string) ([]fabricReturnItem, bool) {
	items := splitTopLevelCSV(returnPart)
	if len(items) == 0 {
		return nil, false
	}
	out := make([]fabricReturnItem, 0, len(items))
	for _, raw := range items {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			return nil, false
		}
		expr := raw
		alias := raw
		if as := lastAsIndexFold(raw); as >= 0 {
			expr = strings.TrimSpace(raw[:as])
			alias = strings.TrimSpace(raw[as+4:])
		}
		v, p, ok := parseFabricVarProp(expr)
		if !ok || !strings.EqualFold(v, matchVar) || !isSimpleIdentifier(alias) {
			return nil, false
		}
		out = append(out, fabricReturnItem{expr: strings.TrimSpace(expr), prop: p, alias: alias})
	}
	return out, true
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

func aliasesFromReturnItems(items []fabricReturnItem) []string {
	cols := make([]string, 0, len(items))
	for _, item := range items {
		cols = append(cols, item.alias)
	}
	return cols
}

func parseSimpleBatchedCountReturnItems(returnPart string) ([]fabricCountReturnItem, bool) {
	items := splitTopLevelCSV(returnPart)
	if len(items) == 0 {
		return nil, false
	}
	out := make([]fabricCountReturnItem, 0, len(items))
	for _, raw := range items {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			return nil, false
		}
		expr := raw
		alias := raw
		if as := lastAsIndexFold(raw); as >= 0 {
			expr = strings.TrimSpace(raw[:as])
			alias = strings.TrimSpace(raw[as+4:])
		}
		if !isSimpleIdentifier(alias) {
			return nil, false
		}
		if !isSimpleCountExpr(expr) {
			return nil, false
		}
		out = append(out, fabricCountReturnItem{expr: expr, alias: alias})
	}
	return out, true
}

func isSimpleCountExpr(expr string) bool {
	expr = strings.TrimSpace(expr)
	if !startsWithFold(expr, "count(") || !strings.HasSuffix(expr, ")") {
		return false
	}
	inner := strings.TrimSpace(expr[len("count(") : len(expr)-1])
	if inner == "*" || isSimpleIdentifier(inner) {
		return true
	}
	_, _, ok := parseFabricVarProp(inner)
	return ok
}

func aliasesFromCountReturnItems(items []fabricCountReturnItem) []string {
	cols := make([]string, 0, len(items))
	for _, item := range items {
		cols = append(cols, item.alias)
	}
	return cols
}

func zeroCountValues(n int) []interface{} {
	out := make([]interface{}, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, int64(0))
	}
	return out
}

func parseSimpleCollectReturnItem(returnPart string) (collectExpr string, alias string, ok bool) {
	parts := splitTopLevelCSV(strings.TrimSpace(returnPart))
	if len(parts) != 1 {
		return "", "", false
	}
	item := strings.TrimSpace(parts[0])
	asIdx := lastAsIndexFold(item)
	if asIdx < 0 {
		return "", "", false
	}
	expr := strings.TrimSpace(item[:asIdx])
	alias = strings.TrimSpace(item[asIdx+4:])
	if !isSimpleIdentifier(alias) {
		return "", "", false
	}
	if !startsWithFold(expr, "collect(") || !strings.HasSuffix(expr, ")") {
		return "", "", false
	}
	inner := strings.TrimSpace(expr[len("collect(") : len(expr)-1])
	if inner == "" {
		return "", "", false
	}
	return inner, alias, true
}

func parseApplyCorrelatedLookupSubquery(query string) (applyCorrelatedLookupSpec, bool) {
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
	if importCol == "" || !isSimpleIdentifier(importCol) {
		return applyCorrelatedLookupSpec{}, false
	}
	rest = strings.TrimSpace(remainder)
	if rest == "" {
		return applyCorrelatedLookupSpec{}, false
	}

	useClause := ""
	if startsWithFold(rest, "USE ") {
		matchIdx := indexTopLevelKeyword(rest, "MATCH")
		if matchIdx <= 0 {
			return applyCorrelatedLookupSpec{}, false
		}
		useClause = strings.TrimSpace(rest[:matchIdx])
		if !startsWithFold(useClause, "USE ") {
			return applyCorrelatedLookupSpec{}, false
		}
		rest = strings.TrimSpace(rest[matchIdx:])
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
		switch s[i] {
		case ' ', '\t', '\n', '\r':
			return strings.TrimSpace(s[:i]), strings.TrimSpace(s[i:])
		}
	}
	return s, ""
}

func indexTopLevelKeyword(s string, keyword string) int {
	paren, bracket, brace := 0, 0, 0
	inSingle, inDouble, inBacktick := false, false, false
	for i := 0; i < len(s); i++ {
		ch := s[i]
		switch {
		case inSingle:
			if ch == '\'' {
				inSingle = false
			}
			continue
		case inDouble:
			if ch == '"' {
				inDouble = false
			}
			continue
		case inBacktick:
			if ch == '`' {
				inBacktick = false
			}
			continue
		}
		switch ch {
		case '\'':
			inSingle = true
		case '"':
			inDouble = true
		case '`':
			inBacktick = true
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
		if hasKeywordAt(s, i, keyword) {
			return i
		}
	}
	return -1
}

func extractApplyCorrelationWhere(whereClause string, importCol string) (matchVar string, matchProp string, otherWhere string, ok bool) {
	terms := splitTopLevelAnd(whereClause)
	if len(terms) == 0 {
		return "", "", "", false
	}
	correlationIdx := -1
	for i, term := range terms {
		lhs, rhs, isEq := splitTopLevelEquality(term)
		if !isEq {
			continue
		}
		leftVar, leftProp, leftOK := parseFabricVarProp(lhs)
		rightVar, rightProp, rightOK := parseFabricVarProp(rhs)
		switch {
		case leftOK && isSimpleIdentifier(strings.TrimSpace(rhs)) && strings.EqualFold(strings.TrimSpace(rhs), importCol):
			matchVar, matchProp = leftVar, leftProp
			correlationIdx = i
		case rightOK && isSimpleIdentifier(strings.TrimSpace(lhs)) && strings.EqualFold(strings.TrimSpace(lhs), importCol):
			matchVar, matchProp = rightVar, rightProp
			correlationIdx = i
		}
		if correlationIdx >= 0 {
			break
		}
	}
	if correlationIdx < 0 || matchVar == "" || matchProp == "" {
		return "", "", "", false
	}
	remaining := make([]string, 0, len(terms)-1)
	for i, term := range terms {
		if i == correlationIdx {
			continue
		}
		t := strings.TrimSpace(term)
		if t != "" {
			remaining = append(remaining, t)
		}
	}
	return matchVar, matchProp, strings.Join(remaining, " AND "), true
}

func sanitizeOtherWhereForImportColumn(otherWhere string, importCol string) (string, bool) {
	if strings.TrimSpace(otherWhere) == "" {
		return "", true
	}
	terms := splitTopLevelAnd(otherWhere)
	if len(terms) == 0 {
		return "", true
	}
	kept := make([]string, 0, len(terms))
	for _, term := range terms {
		term = strings.TrimSpace(term)
		if term == "" {
			continue
		}
		if isImportNotNullGuardTerm(term, importCol) {
			continue
		}
		if containsStandaloneIdentifier(term, importCol) {
			return "", false
		}
		kept = append(kept, term)
	}
	if len(kept) == 0 {
		return "", true
	}
	return strings.Join(kept, " AND "), true
}

func isImportNotNullGuardTerm(term string, importCol string) bool {
	parts := strings.Fields(strings.TrimSpace(term))
	if len(parts) != 4 {
		return false
	}
	left := strings.TrimSpace(parts[0])
	left = strings.Trim(left, "`")
	if !strings.EqualFold(left, strings.TrimSpace(importCol)) {
		return false
	}
	return strings.EqualFold(parts[1], "IS") &&
		strings.EqualFold(parts[2], "NOT") &&
		strings.EqualFold(parts[3], "NULL")
}

func splitTopLevelAnd(whereClause string) []string {
	parts := make([]string, 0, 4)
	start := 0
	paren, bracket, brace := 0, 0, 0
	inSingle, inDouble, inBacktick := false, false, false
	for i := 0; i < len(whereClause); i++ {
		ch := whereClause[i]
		switch {
		case inSingle:
			if ch == '\'' {
				inSingle = false
			}
			continue
		case inDouble:
			if ch == '"' {
				inDouble = false
			}
			continue
		case inBacktick:
			if ch == '`' {
				inBacktick = false
			}
			continue
		}
		switch ch {
		case '\'':
			inSingle = true
		case '"':
			inDouble = true
		case '`':
			inBacktick = true
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
		if hasKeywordAt(whereClause, i, "AND") {
			parts = append(parts, strings.TrimSpace(whereClause[start:i]))
			i += len("AND") - 1
			start = i + 1
		}
	}
	parts = append(parts, strings.TrimSpace(whereClause[start:]))
	return parts
}

func splitTopLevelEquality(expr string) (lhs, rhs string, ok bool) {
	inSingle, inDouble, inBacktick := false, false, false
	paren, bracket, brace := 0, 0, 0
	for i := 0; i < len(expr); i++ {
		ch := expr[i]
		switch {
		case inSingle:
			if ch == '\'' {
				inSingle = false
			}
			continue
		case inDouble:
			if ch == '"' {
				inDouble = false
			}
			continue
		case inBacktick:
			if ch == '`' {
				inBacktick = false
			}
			continue
		}
		switch ch {
		case '\'':
			inSingle = true
			continue
		case '"':
			inDouble = true
			continue
		case '`':
			inBacktick = true
			continue
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
	if !isSimpleIdentifier(lhs) || !isSimpleIdentifier(rhs) {
		return "", "", false
	}
	return lhs, rhs, true
}

func containsStandaloneIdentifier(s, ident string) bool {
	if ident == "" || len(s) < len(ident) {
		return false
	}
	inSingle, inDouble, inBacktick := false, false, false
	for i := 0; i+len(ident) <= len(s); i++ {
		ch := s[i]
		switch {
		case inSingle:
			if ch == '\'' {
				inSingle = false
			}
			continue
		case inDouble:
			if ch == '"' {
				inDouble = false
			}
			continue
		case inBacktick:
			if ch == '`' {
				inBacktick = false
			}
			continue
		}
		if ch == '\'' {
			inSingle = true
			continue
		}
		if ch == '"' {
			inDouble = true
			continue
		}
		if ch == '`' {
			inBacktick = true
			continue
		}
		if s[i:i+len(ident)] != ident {
			continue
		}
		prevOK := i == 0 || !isIdentChar(s[i-1])
		next := i + len(ident)
		nextOK := next >= len(s) || !isIdentChar(s[next])
		if prevOK && nextOK {
			return true
		}
	}
	return false
}

func synthesizeEmptyCollectOnlyReturn(query string) ([]string, []interface{}, bool) {
	trimmed := strings.TrimSpace(query)
	retIdx := lastKeywordIndexFold(trimmed, "RETURN")
	if retIdx < 0 {
		return nil, nil, false
	}
	clause := strings.TrimSpace(trimmed[retIdx+len("RETURN"):])
	if clause == "" {
		return nil, nil, false
	}
	items := splitTopLevelCSV(clause)
	if len(items) == 0 {
		return nil, nil, false
	}
	cols := make([]string, 0, len(items))
	row := make([]interface{}, 0, len(items))
	for _, it := range items {
		it = strings.TrimSpace(it)
		if it == "" {
			return nil, nil, false
		}
		expr := it
		alias := it
		if as := lastAsIndexFold(it); as >= 0 {
			expr = strings.TrimSpace(it[:as])
			alias = strings.TrimSpace(it[as+4:])
		}
		if !startsWithFold(strings.TrimSpace(expr), "collect(") {
			return nil, nil, false
		}
		cols = append(cols, strings.Trim(alias, "`"))
		row = append(row, []interface{}{})
	}
	return cols, row, true
}

func projectSimpleReturnFromRow(query string, inputCols []string, inputRow []interface{}) ([]string, []interface{}, bool) {
	trimmed := strings.TrimSpace(query)
	if !startsWithFold(trimmed, "RETURN ") {
		return nil, nil, false
	}
	clause := strings.TrimSpace(trimmed[len("RETURN "):])
	if clause == "" {
		return nil, nil, false
	}
	items := splitTopLevelCSV(clause)
	if len(items) == 0 {
		return nil, nil, false
	}
	colIdx := buildColumnIndex(inputCols)
	cols := make([]string, 0, len(items))
	values := make([]interface{}, 0, len(items))
	for _, it := range items {
		it = strings.TrimSpace(it)
		if it == "" {
			return nil, nil, false
		}
		src := it
		alias := it
		if as := lastAsIndexFold(it); as >= 0 {
			src = strings.TrimSpace(it[:as])
			alias = strings.TrimSpace(it[as+4:])
		}
		if !isSimpleIdentifier(src) || !isSimpleIdentifier(alias) {
			return nil, nil, false
		}
		idx, ok := colIdx[src]
		if !ok || idx >= len(inputRow) {
			return nil, nil, false
		}
		cols = append(cols, alias)
		values = append(values, inputRow[idx])
	}
	return cols, values, true
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

func executeApplyInMemoryProjection(inputResult *ResultStream, query string) (*ResultStream, bool) {
	if inputResult == nil {
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
				if !isSimpleIdentifier(src) || !isSimpleIdentifier(alias) {
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
				Columns: make([]string, len(parsed)),
				Rows:    make([][]interface{}, 0, len(inputResult.Rows)),
			}
			for i, p := range parsed {
				out.Columns[i] = p.alias
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
			Columns: []string{keysAlias},
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
			Columns: []string{rowsAlias},
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
		return executeRowsGroupedJoinProjection(inputResult), true
	}

	// Generic set-based flatten projection:
	// WITH <rows>, collect({...}) AS <grouped>
	// UNWIND <rows> AS <r>
	// WITH <r>, [g IN <grouped> WHERE g.<k1> = <r>.<k2>] AS <ms>
	// UNWIND <ms> AS <m>
	// RETURN <r>.<...> AS ..., <m>.<...> AS ...
	if projected, ok := executeCollectedMapJoinFlatProjection(inputResult, trimmed); ok {
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
	inSingle, inDouble, inBacktick := false, false, false
	for i := 0; i < len(clause); i++ {
		ch := clause[i]
		switch {
		case inSingle:
			if ch == '\'' {
				inSingle = false
			}
			continue
		case inDouble:
			if ch == '"' {
				inDouble = false
			}
			continue
		case inBacktick:
			if ch == '`' {
				inBacktick = false
			}
			continue
		}
		switch ch {
		case '\'':
			inSingle = true
		case '"':
			inDouble = true
		case '`':
			inBacktick = true
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
		if hasKeywordAt(clause, i, "ORDER BY") || hasKeywordAt(clause, i, "SKIP") || hasKeywordAt(clause, i, "LIMIT") {
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
			if hasKeywordAt(trimmed, i, "SKIP") || hasKeywordAt(trimmed, i, "LIMIT") {
				end = i
				break
			}
		}
	} else {
		for i := len(keyword); i < len(trimmed); i++ {
			if hasKeywordAt(trimmed, i, "ORDER BY") || hasKeywordAt(trimmed, i, "SKIP") || hasKeywordAt(trimmed, i, "LIMIT") {
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
		if !isSimpleIdentifier(item) {
			continue
		}
		specs = append(specs, simpleOrderSpec{column: item, desc: desc})
	}
	return specs
}

func parseSimplePositiveInt(s string) (int, bool) {
	if s == "" {
		return 0, false
	}
	value := 0
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return 0, false
		}
		value = value*10 + int(s[i]-'0')
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
		if !isSimpleIdentifier(k) || !isSimpleIdentifier(v) {
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
	if !okRows || !okK || !okTexts || len(input.Rows) == 0 {
		return &ResultStream{Columns: []string{"textKey", "textKey128", "texts"}, Rows: [][]interface{}{}}
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
		return &ResultStream{Columns: []string{"textKey", "textKey128", "texts"}, Rows: [][]interface{}{}}
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
		return &ResultStream{Columns: []string{"textKey", "textKey128", "texts"}, Rows: [][]interface{}{}}
	}

	out := &ResultStream{
		Columns: []string{"textKey", "textKey128", "texts"},
		Rows:    make([][]interface{}, 0, len(rowsAny)),
	}
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

func executeCollectedMapJoinFlatProjection(input *ResultStream, query string) (*ResultStream, bool) {
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
		if !isSimpleIdentifier(key) || !isSimpleIdentifier(src) {
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
		if !isSimpleIdentifier(alias) {
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

	outCols := make([]string, 0, len(retItems))
	for _, item := range retItems {
		outCols = append(outCols, item.alias)
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
	isWith := startsWithFold(trimmed, "WITH ")
	isReturn := startsWithFold(trimmed, "RETURN ")
	if !isWith && !isReturn {
		return nil, false, nil
	}
	// Do not pipeline plain RETURN fragments through UNWIND $__fabric_apply_rows.
	// Current Cypher execution can surface the raw map variable (e.g. "__fabric_row")
	// instead of projected aliases for this shape, which leaks internal columns to clients.
	// Keep RETURN fragments on the per-row correlated path for correctness.
	if isReturn {
		return nil, false, nil
	}
	if isWith {
		withEnd, ok := findLeadingWithClauseEnd(trimmed)
		if !ok || withEnd <= 0 {
			return nil, false, nil
		}
		withClause := strings.TrimSpace(trimmed[:withEnd])
		// Keep correlated per-row APPLY for simple import WITH clauses.
		if isSimpleWithImportClause(withClause, inputResult.Columns) {
			return nil, false, nil
		}
	}

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

	projections := make([]string, 0, len(inputResult.Columns))
	for _, col := range inputResult.Columns {
		col = strings.TrimSpace(col)
		if col == "" {
			continue
		}
		if strings.HasPrefix(col, "__fabric_") {
			continue
		}
		if !isSimpleIdentifier(col) {
			return nil, false, nil
		}
		projections = append(projections, "__fabric_row."+col+" AS "+col)
	}
	if len(projections) == 0 {
		return nil, false, nil
	}

	rewritten := "UNWIND $__fabric_apply_rows AS __fabric_row WITH " + strings.Join(projections, ", ") + " " + trimmed
	mergedParams := make(map[string]interface{}, util.SafePreallocSum(len(params), 1))
	for k, v := range params {
		mergedParams[k] = v
	}
	mergedParams["__fabric_apply_rows"] = rows

	copied := *inner
	copied.Query = rewritten
	res, err := e.executeExec(ctx, tx, &copied, mergedParams, authToken)
	if err != nil {
		return nil, true, err
	}
	if res != nil && len(res.Columns) == 1 && res.Columns[0] == "__fabric_row" {
		// Fallback: when pipeline execution leaks the wrapper row map column,
		// apply structural in-memory projection from the original inner query.
		if projected, ok := executeCollectedMapJoinFlatProjection(inputResult, trimmed); ok {
			return projected, true, nil
		}
	}
	return res, true, nil
}

func isSimpleIdentifier(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if i == 0 {
			if !(unicode.IsLetter(rune(s[i])) || s[i] == '_') {
				return false
			}
			continue
		}
		if !(unicode.IsLetter(rune(s[i])) || unicode.IsDigit(rune(s[i])) || s[i] == '_') {
			return false
		}
	}
	return true
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

	withEnd, ok := findLeadingWithClauseEnd(trimmed)
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
		if !isSimpleIdentifier(name) {
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
	inSingle := false
	inDouble := false
	inBacktick := false
	for i := 0; i < len(s); {
		ch := s[i]
		switch {
		case inSingle:
			b.WriteByte(ch)
			if ch == '\'' {
				inSingle = false
			}
			i++
			continue
		case inDouble:
			b.WriteByte(ch)
			if ch == '"' {
				inDouble = false
			}
			i++
			continue
		case inBacktick:
			b.WriteByte(ch)
			if ch == '`' {
				inBacktick = false
			}
			i++
			continue
		}

		if ch == '\'' {
			inSingle = true
			b.WriteByte(ch)
			i++
			continue
		}
		if ch == '"' {
			inDouble = true
			b.WriteByte(ch)
			i++
			continue
		}
		if ch == '`' {
			inBacktick = true
			b.WriteByte(ch)
			i++
			continue
		}

		if i+len(ident) <= len(s) && s[i:i+len(ident)] == ident {
			prevOK := i == 0 || (!isIdentChar(s[i-1]) && s[i-1] != '.')
			next := i + len(ident)
			nextOK := next >= len(s) || !isIdentChar(s[next])
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

func findLeadingWithClauseEnd(query string) (int, bool) {
	if query == "" {
		return 0, false
	}
	i := skipLeadingSpace(query, 0)
	if !hasKeywordAt(query, i, "WITH") {
		return 0, false
	}
	i += len("WITH")
	i = skipLeadingSpace(query, i)

	depth := 0
	inSingle := false
	inDouble := false
	inBacktick := false

	for idx := i; idx < len(query); idx++ {
		ch := query[idx]

		switch {
		case inSingle:
			if ch == '\'' {
				if idx+1 < len(query) && query[idx+1] == '\'' {
					idx++
					continue
				}
				inSingle = false
			}
			continue
		case inDouble:
			if ch == '"' {
				inDouble = false
			}
			continue
		case inBacktick:
			if ch == '`' {
				inBacktick = false
			}
			continue
		}

		switch ch {
		case '\'':
			inSingle = true
			continue
		case '"':
			inDouble = true
			continue
		case '`':
			inBacktick = true
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

		if depth == 0 && isCypherClauseStart(query, idx) {
			return idx, true
		}
	}

	return len(query), true
}

func isCypherClauseStart(query string, idx int) bool {
	for _, kw := range clauseStartKeywords {
		if hasKeywordAt(query, idx, kw) {
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

func hasKeywordAt(query string, idx int, keyword string) bool {
	if idx < 0 || idx+len(keyword) > len(query) {
		return false
	}
	if idx > 0 {
		prev := rune(query[idx-1])
		if unicode.IsLetter(prev) || unicode.IsDigit(prev) || prev == '_' {
			return false
		}
	}
	segment := query[idx : idx+len(keyword)]
	if !strings.EqualFold(segment, keyword) {
		return false
	}
	end := idx + len(keyword)
	if end < len(query) {
		next := rune(query[end])
		if unicode.IsLetter(next) || unicode.IsDigit(next) || next == '_' {
			return false
		}
	}
	return true
}

func skipLeadingSpace(s string, idx int) int {
	for idx < len(s) && unicode.IsSpace(rune(s[idx])) {
		idx++
	}
	return idx
}

// executeUnion executes both branches and merges results.
func (e *FabricExecutor) executeUnion(ctx context.Context, tx *FabricTransaction, f *FragmentUnion, params map[string]interface{}, authToken string) (*ResultStream, error) {
	cols, it, err := e.executeUnionRows(ctx, tx, f, params, authToken)
	if err != nil {
		return nil, err
	}
	return materializeIterator(cols, it)
}

func (e *FabricExecutor) executeUnionRows(ctx context.Context, tx *FabricTransaction, f *FragmentUnion, params map[string]interface{}, authToken string) ([]string, RowIterator, error) {
	// Keep write branch execution sequential so shard write-routing remains deterministic.
	// Read-only UNION branches execute concurrently and are merged in LHS/RHS order.
	if fragmentContainsWrite(f.LHS) || fragmentContainsWrite(f.RHS) {
		lhsCols, lhsIt, err := e.executeRows(ctx, tx, f.LHS, params, authToken)
		if err != nil {
			return nil, nil, fmt.Errorf("union LHS failed: %w", err)
		}
		rhsCols, rhsIt, err := e.executeRows(ctx, tx, f.RHS, params, authToken)
		if err != nil {
			_ = lhsIt.Close()
			return nil, nil, fmt.Errorf("union RHS failed: %w", err)
		}
		cols := f.Columns
		if len(cols) == 0 {
			cols = lhsCols
			if len(cols) == 0 {
				cols = rhsCols
			}
		}
		out := NewConcatRowIterator(lhsIt, rhsIt)
		if f.Distinct {
			out = NewDistinctRowIterator(out)
		}
		return cols, out, nil
	}

	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		lhsCols []string
		lhsIt   RowIterator
		rhsCols []string
		rhsIt   RowIterator
		err     error
	)
	wg.Add(2)
	go func() {
		defer wg.Done()
		cols, it, runErr := e.executeRows(ctx, tx, f.LHS, params, authToken)
		mu.Lock()
		defer mu.Unlock()
		if runErr != nil {
			err = fmt.Errorf("union LHS failed: %w", runErr)
			return
		}
		lhsCols = cols
		lhsIt = it
	}()
	go func() {
		defer wg.Done()
		cols, it, runErr := e.executeRows(ctx, tx, f.RHS, params, authToken)
		mu.Lock()
		defer mu.Unlock()
		if runErr != nil {
			if err == nil {
				err = fmt.Errorf("union RHS failed: %w", runErr)
			}
			return
		}
		rhsCols = cols
		rhsIt = it
	}()
	wg.Wait()
	if err != nil {
		if lhsIt != nil {
			_ = lhsIt.Close()
		}
		if rhsIt != nil {
			_ = rhsIt.Close()
		}
		return nil, nil, err
	}
	cols := f.Columns
	if len(cols) == 0 {
		cols = lhsCols
		if len(cols) == 0 {
			cols = rhsCols
		}
	}
	out := NewConcatRowIterator(lhsIt, rhsIt)
	if f.Distinct {
		out = NewDistinctRowIterator(out)
	}
	return cols, out, nil
}

// executeUnionSequential preserves direct test-call compatibility while using
// the iterator-based UNION execution path.
func (e *FabricExecutor) executeUnionSequential(ctx context.Context, tx *FabricTransaction, f *FragmentUnion, params map[string]interface{}, authToken string) (*ResultStream, error) {
	lhsCols, lhsIt, err := e.executeRows(ctx, tx, f.LHS, params, authToken)
	if err != nil {
		return nil, fmt.Errorf("union LHS failed: %w", err)
	}
	rhsCols, rhsIt, err := e.executeRows(ctx, tx, f.RHS, params, authToken)
	if err != nil {
		_ = lhsIt.Close()
		return nil, fmt.Errorf("union RHS failed: %w", err)
	}
	cols := f.Columns
	if len(cols) == 0 {
		cols = lhsCols
		if len(cols) == 0 {
			cols = rhsCols
		}
	}
	out := NewConcatRowIterator(lhsIt, rhsIt)
	if f.Distinct {
		out = NewDistinctRowIterator(out)
	}
	return materializeIterator(cols, out)
}

// executeUnionParallel preserves direct test-call compatibility while using
// the iterator-based UNION execution path.
func (e *FabricExecutor) executeUnionParallel(ctx context.Context, tx *FabricTransaction, f *FragmentUnion, params map[string]interface{}, authToken string) (*ResultStream, error) {
	var (
		wg  sync.WaitGroup
		mu  sync.Mutex
		lhs []string
		lit RowIterator
		rhs []string
		rit RowIterator
		err error
	)
	wg.Add(2)
	go func() {
		defer wg.Done()
		cols, it, runErr := e.executeRows(ctx, tx, f.LHS, params, authToken)
		mu.Lock()
		defer mu.Unlock()
		if runErr != nil {
			err = fmt.Errorf("union LHS failed: %w", runErr)
			return
		}
		lhs, lit = cols, it
	}()
	go func() {
		defer wg.Done()
		cols, it, runErr := e.executeRows(ctx, tx, f.RHS, params, authToken)
		mu.Lock()
		defer mu.Unlock()
		if runErr != nil {
			if err == nil {
				err = fmt.Errorf("union RHS failed: %w", runErr)
			}
			return
		}
		rhs, rit = cols, it
	}()
	wg.Wait()
	if err != nil {
		if lit != nil {
			_ = lit.Close()
		}
		if rit != nil {
			_ = rit.Close()
		}
		return nil, err
	}
	cols := f.Columns
	if len(cols) == 0 {
		cols = lhs
		if len(cols) == 0 {
			cols = rhs
		}
	}
	out := NewConcatRowIterator(lit, rit)
	if f.Distinct {
		out = NewDistinctRowIterator(out)
	}
	return materializeIterator(cols, out)
}

func fragmentContainsWrite(fragment Fragment) bool {
	switch f := fragment.(type) {
	case *FragmentExec:
		return f.IsWrite
	case *FragmentApply:
		return fragmentContainsWrite(f.Input) || fragmentContainsWrite(f.Inner)
	case *FragmentUnion:
		return fragmentContainsWrite(f.LHS) || fragmentContainsWrite(f.RHS)
	case *FragmentLeaf:
		return fragmentContainsWrite(f.Input)
	case *FragmentInit:
		return false
	default:
		return false
	}
}

// combineColumns merges two column lists, deduplicating names.
func combineColumns(outer, inner []string) []string {
	seen := make(map[string]bool, len(outer))
	result := make([]string, 0, util.SafePreallocSum(len(outer), len(inner)))
	for _, col := range outer {
		if strings.HasPrefix(col, "__fabric_") {
			continue
		}
		seen[col] = true
		result = append(result, col)
	}
	for _, col := range inner {
		if strings.HasPrefix(col, "__fabric_") {
			continue
		}
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
