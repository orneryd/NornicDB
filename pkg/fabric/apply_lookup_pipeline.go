package fabric

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/orneryd/nornicdb/pkg/convert"
	"github.com/orneryd/nornicdb/pkg/cypher/bulk"
	"github.com/orneryd/nornicdb/pkg/util"
)

// applyLookupShape is the result shape an accepted batched lookup produces. It
// describes the product only; admission stays in planApplyLookupShape.
type applyLookupShape int

const (
	applyLookupShapeCount applyLookupShape = iota
	applyLookupShapeRows
	applyLookupShapeCollect
)

// unexpectedColumns is the shape's exact historical column error.
func (s applyLookupShape) unexpectedColumns(columns []string) error {
	if s == applyLookupShapeCount {
		return fmt.Errorf("batched APPLY count lookup produced unexpected columns: %v", columns)
	}
	if s == applyLookupShapeCollect {
		return fmt.Errorf("batched APPLY lookup produced unexpected columns: %v", columns)
	}
	return fmt.Errorf("batched APPLY row lookup produced unexpected columns: %v", columns)
}

// applyLookupInputKind preserves the distinct iterator/materialized admission
// contracts while both accepted paths share the same plan and row pipeline.
type applyLookupInputKind uint8

const (
	applyLookupMaterializedInput applyLookupInputKind = iota
	applyLookupIteratorInput
)

// applyLookupPlan is the immutable product of admission for one accepted
// invocation: parsed correlation, private parameter/alias names, projection
// facts and rendered batch query. Consumers do not rebuild those facts.
type applyLookupPlan struct {
	shape applyLookupShape

	spec applyCorrelatedLookupSpec

	matchVar   string
	matchProp  string
	otherWhere string

	keysParam string
	keyAlias  string

	importCol    string
	keyIndex     int
	outerIndex   map[string]int
	outerColumns []string
	ownerCols    []string

	applyLookupProjectionFacts

	missingValues   []interface{}
	modifiers       string
	modifierAliases map[string]string

	batchQuery string
}

// planApplyLookupShape owns shape admission, projection/correlation facts and
// private batch names. Collect preserves its earlier projection admission and
// metadata-before-missing-key boundary; declined iterators are never consumed.
func (e *FabricExecutor) planApplyLookupShape(
	ctx context.Context,
	shape applyLookupShape,
	inputKind applyLookupInputKind,
	inner *FragmentExec,
	outerColumns, metadataInputColumns []string,
	params map[string]interface{},
) (applyLookupPlan, bool) {
	var plan applyLookupPlan
	if inner == nil || len(outerColumns) == 0 {
		return plan, false
	}
	trimmed := strings.TrimSpace(inner.Query)
	switch shape {
	case applyLookupShapeCount:
		if !startsWithFold(trimmed, "WITH ") || !containsFold(trimmed, "RETURN ") || !containsFold(trimmed, "count(") || queryIsWrite(trimmed) {
			return plan, false
		}
	case applyLookupShapeRows:
		if !startsWithFold(trimmed, "WITH ") || !containsFold(trimmed, "RETURN ") || containsFold(trimmed, "RETURN collect(") || queryIsWrite(trimmed) {
			return plan, false
		}
	case applyLookupShapeCollect:
		if !startsWithFold(trimmed, "WITH ") || queryIsWrite(trimmed) || !containsFold(trimmed, "RETURN collect(") {
			return plan, false
		}
	default:
		return plan, false
	}

	spec, ok := parseApplyCorrelatedLookupSubquery(ctx, trimmed)
	if !ok || spec.importCol == "" || strings.TrimSpace(spec.wherePart) == "" || strings.TrimSpace(spec.returnProjection) == "" {
		return plan, false
	}
	if (shape != applyLookupShapeRows || inputKind == applyLookupIteratorInput) && strings.TrimSpace(spec.returnModifiers) != "" {
		return plan, false
	}
	var projection applyLookupProjectionFacts
	if shape == applyLookupShapeCollect {
		projection, ok = readApplyLookupProjection(shape, spec.returnProjection, "")
		if !ok {
			return plan, false
		}
	}
	matchVar, matchProp, otherWhere, ok := cyphertext.ExtractCorrelationWhere(spec.wherePart, spec.importCol, splitTopLevelAnd, splitTopLevelEquality, parseFabricVarProp, cyphertext.IsIdentifier)
	if !ok {
		return plan, false
	}
	otherWhere, ok = cyphertext.FilterImportConjuncts(otherWhere, spec.importCol, cyphertext.IdentifierExact, splitTopLevelAnd)
	if !ok {
		return plan, false
	}
	plan = applyLookupPlan{
		shape: shape, spec: spec, matchVar: matchVar, matchProp: matchProp,
		otherWhere: otherWhere, importCol: spec.importCol, outerColumns: outerColumns,
		outerIndex: buildColumnIndex(outerColumns),
		keysParam:  freeName("__fabric_apply_keys", trimmed, params),
		keyAlias:   freeName("__fabric_apply_key", trimmed),
	}
	var keyExists bool
	plan.keyIndex, keyExists = plan.outerIndex[spec.importCol]
	if shape != applyLookupShapeCollect {
		if !keyExists {
			return applyLookupPlan{}, false
		}
		projection, ok = readApplyLookupProjection(shape, spec.returnProjection, matchVar)
		if !ok {
			return applyLookupPlan{}, false
		}
	}
	plan.applyLookupProjectionFacts = projection
	switch shape {
	case applyLookupShapeCount:
		plan.missingValues = zeroCountValues(len(plan.countItems))
	case applyLookupShapeRows:
		plan.missingValues = make([]interface{}, len(plan.returnItems))
		plan.modifiers = strings.TrimSpace(spec.returnModifiers)
		if plan.modifiers != "" {
			plan.modifierAliases = buildResultModifierAliasMap(plan.returnItems)
		}
	}
	plan.ownerCols = e.executionColumnNames(ctx, inner.Query, metadataInputColumns)
	if plan.ownerCols == nil || !keyExists {
		return applyLookupPlan{}, false
	}
	plan.batchQuery = renderApplyLookupBatchQuery(plan)
	return plan, true
}

// renderApplyLookupBatchQuery owns every accepted batch query. It emits
// USE, MATCH and the correlation
// predicate against the plan's private key parameter, the retained other
// predicates, and the private key projection followed by each admitted
// projection expression and alias, with the original spacing.
func renderApplyLookupBatchQuery(plan applyLookupPlan) string {
	var rewritten strings.Builder
	if plan.spec.useClause != "" {
		rewritten.WriteString(plan.spec.useClause)
		rewritten.WriteByte(' ')
	}
	rewritten.WriteString("MATCH ")
	rewritten.WriteString(plan.spec.matchPart)
	rewritten.WriteString(" WHERE ")
	rewritten.WriteString(plan.matchVar)
	rewritten.WriteByte('.')
	rewritten.WriteString(plan.matchProp)
	rewritten.WriteString(" IN $" + plan.keysParam)
	if plan.otherWhere != "" {
		rewritten.WriteString(" AND ")
		rewritten.WriteString(plan.otherWhere)
	}
	rewritten.WriteString(" RETURN ")
	rewritten.WriteString(plan.matchVar)
	rewritten.WriteByte('.')
	rewritten.WriteString(plan.matchProp)
	rewritten.WriteString(" AS " + plan.keyAlias)
	for _, item := range plan.countItems {
		rewritten.WriteString(", ")
		rewritten.WriteString(item.expr)
		rewritten.WriteString(" AS ")
		rewritten.WriteString(item.alias)
	}
	for _, item := range plan.returnItems {
		rewritten.WriteString(", ")
		rewritten.WriteString(plan.matchVar)
		rewritten.WriteByte('.')
		rewritten.WriteString(item.prop)
		rewritten.WriteString(" AS ")
		rewritten.WriteString(item.alias)
	}
	if plan.shape == applyLookupShapeCollect {
		rewritten.WriteString(", collect(")
		rewritten.WriteString(plan.collectExpression)
		rewritten.WriteString(") AS ")
		rewritten.WriteString(plan.collectAlias)
	}
	return rewritten.String()
}

// applyLookupKeyEntry is one distinct correlation key, in first-encounter order.
type applyLookupKeyEntry struct {
	key  interface{}
	text string
}

// applyLookupNoKey marks an outer row whose key cell is nil, missing or past
// the row's end. Such a row runs no lookup and keeps its shape's absent value.
const applyLookupNoKey = -1

// applyLookupOuterKeys maps each outer row to the key entry that produced it.
type applyLookupOuterKeys struct {
	entry []int
}

func (c applyLookupOuterKeys) entryAt(i int) (int, bool) {
	if i < 0 || i >= len(c.entry) || c.entry[i] == applyLookupNoKey {
		return 0, false
	}
	return c.entry[i], true
}

// collectApplyLookupKeys is the single key collector: distinct keys in
// encounter order plus the outer-row correspondence; initialCapacity is a
// capacity hint, not a limit on accepted distinct keys.
func collectApplyLookupKeys(rows [][]interface{}, keyIdx int, initialCapacity int) ([]applyLookupKeyEntry, applyLookupOuterKeys) {
	if initialCapacity > len(rows) {
		initialCapacity = len(rows)
	}
	entries := make([]applyLookupKeyEntry, 0, initialCapacity)
	correspondence := applyLookupOuterKeys{entry: make([]int, len(rows))}
	seen := make(map[string]int, len(rows))
	for i, row := range rows {
		correspondence.entry[i] = applyLookupNoKey
		if keyIdx < 0 || keyIdx >= len(row) {
			continue
		}
		key := row[keyIdx]
		if key == nil {
			continue
		}
		text := applyLookupKeyString(key)
		if at, dup := seen[text]; dup {
			correspondence.entry[i] = at
			continue
		}
		seen[text] = len(entries)
		correspondence.entry[i] = len(entries)
		entries = append(entries, applyLookupKeyEntry{key: key, text: text})
	}
	return entries, correspondence
}

func applyLookupKeys(entries []applyLookupKeyEntry) []interface{} {
	keys := make([]interface{}, len(entries))
	for i, entry := range entries {
		keys[i] = entry.key
	}
	return keys
}

// applyLookupRow is one matched batch row, projected through this plan's
// aliases.
type applyLookupRow struct {
	values         []interface{}
	aggregateValue interface{}
}

// applyLookupReducer is the typed per-key product a shape builds before any
// outer replay. Count replaces: a later backend row for one key overwrites an
// earlier one, exactly as the shipped count consumer does. Property rows append,
// preserving the batch's fanout order.
type applyLookupReducer interface {
	empty() interface{}
	add(product interface{}, row applyLookupRow) interface{}
}

type applyLookupCountReducer struct{}

func (applyLookupCountReducer) empty() interface{} { return nil }

func (applyLookupCountReducer) add(_ interface{}, row applyLookupRow) interface{} { return row.values }

type applyLookupCollectReducer struct{}

func (applyLookupCollectReducer) empty() interface{} { return nil }

func (applyLookupCollectReducer) add(_ interface{}, row applyLookupRow) interface{} {
	return row.aggregateValue
}

type applyLookupRowsReducer struct{}

func (applyLookupRowsReducer) empty() interface{} { return [][]interface{}(nil) }

func (applyLookupRowsReducer) add(product interface{}, row applyLookupRow) interface{} {
	return append(product.([][]interface{}), row.values)
}

func (r applyLookupRowsReducer) merged(product, added interface{}) interface{} {
	return append(product.([][]interface{}), added.([][]interface{})...)
}

// applyLookupRowProducts retains the original typed row collection through
// binding, per-key modifiers and replay; a missing key has no matching rows.
func applyLookupRowProducts(product interface{}) [][]interface{} {
	if product == nil {
		return nil
	}
	return product.([][]interface{})
}

func mergeApplyLookupProduct(reducer applyLookupReducer, product, added interface{}) interface{} {
	if r, ok := reducer.(applyLookupRowsReducer); ok {
		return r.merged(product, added)
	}
	return added
}

// applyLookupAliasIndexes is the alias framing of one batch result.
type applyLookupAliasIndexes struct {
	key       int
	values    []int
	aggregate int
}

func resolveApplyLookupAliasIndexes(plan applyLookupPlan, columns []string) (applyLookupAliasIndexes, bool) {
	index := buildColumnIndex(columns)
	keyIdx, ok := index[plan.keyAlias]
	if !ok {
		return applyLookupAliasIndexes{}, false
	}
	if plan.shape == applyLookupShapeCollect {
		valueIdx, found := index[plan.collectAlias]
		return applyLookupAliasIndexes{key: keyIdx, aggregate: valueIdx}, found
	}
	resolved := applyLookupAliasIndexes{key: keyIdx, values: make([]int, len(plan.aliases))}
	for i, alias := range plan.aliases {
		at, found := index[alias]
		if !found {
			at = -1
		}
		resolved.values[i] = at
	}
	return resolved, true
}

// bindApplyLookupBatchRows binds each returned row once: a row without its
// private key cell is skipped, and an absent value alias contributes the shape's
// absent value (int64(0) for count, nil for property rows). Collect requires
// both aliases and both cells, and retains the raw aggregate product.
func bindApplyLookupBatchRows(plan applyLookupPlan, reducer applyLookupReducer, batchResult *ResultStream) (map[string]interface{}, error) {
	indexes, ok := resolveApplyLookupAliasIndexes(plan, batchResult.Columns)
	if !ok {
		return nil, plan.shape.unexpectedColumns(batchResult.Columns)
	}
	grouped := make(map[string]interface{}, len(batchResult.Rows))
	for _, row := range batchResult.Rows {
		if indexes.key >= len(row) {
			continue
		}
		binding := applyLookupRow{}
		if plan.shape == applyLookupShapeCollect {
			if indexes.aggregate >= len(row) {
				continue
			}
			binding.aggregateValue = row[indexes.aggregate]
		} else {
			binding.values = make([]interface{}, len(indexes.values))
			for i, at := range indexes.values {
				if at < 0 || at >= len(row) {
					binding.values[i] = plan.missingValues[i]
					continue
				}
				binding.values[i] = row[at]
			}
		}
		key := applyLookupKeyString(row[indexes.key])
		if existing, found := grouped[key]; found {
			grouped[key] = reducer.add(existing, binding)
			continue
		}
		grouped[key] = reducer.add(reducer.empty(), binding)
	}
	return grouped, nil
}

// executeLookupKeyBatch is the single batch transport-and-bind operation: copy
// the caller's params, attach only this batch's keys, value-copy the complete
// FragmentExec (GraphName, Scope, ScopeImports, IsWrite, Input, Columns) and
// replace Query only, dispatch through e.Execute with the caller's transaction
// and auth token, then bind the alias projection once per returned row.
func (e *FabricExecutor) executeLookupKeyBatch(
	ctx context.Context,
	tx *FabricTransaction,
	plan applyLookupPlan,
	inner *FragmentExec,
	params map[string]interface{},
	authToken string,
	keys []interface{},
	reducer applyLookupReducer,
) (map[string]interface{}, error) {
	batchParams := make(map[string]interface{}, util.SafePreallocSum(len(params), 1))
	for k, v := range params {
		batchParams[k] = v
	}
	batchParams[plan.keysParam] = keys

	batchFrag := *inner
	batchFrag.Query = plan.batchQuery
	batchResult, err := e.Execute(ctx, tx, &batchFrag, batchParams, authToken)
	if err != nil {
		return nil, err
	}
	return bindApplyLookupBatchRows(plan, reducer, batchResult)
}

// applyLookupKeyBatches slices distinct keys into copied batches, so no worker
// shares a slice with the caller or with another worker.
func applyLookupKeyBatches(entries []applyLookupKeyEntry) [][]interface{} {
	batches := make([][]interface{}, 0, (len(entries)+fabricApplyLookupBatchSize-1)/fabricApplyLookupBatchSize)
	for start := 0; start < len(entries); start += fabricApplyLookupBatchSize {
		end := minInt(start+fabricApplyLookupBatchSize, len(entries))
		batches = append(batches, applyLookupKeys(entries[start:end]))
	}
	return batches
}

// mergeApplyLookupBatch merges one completed local product into the shared one.
// Its caller holds the product mutex, or runs sequentially.
func mergeApplyLookupBatch(reducer applyLookupReducer, shared map[string]interface{}, local map[string]interface{}) {
	for k, v := range local {
		if existing, found := shared[k]; found {
			shared[k] = mergeApplyLookupProduct(reducer, existing, v)
			continue
		}
		shared[k] = v
	}
}

// runApplyLookupKeyBatches is the sole scheduler owner. Every caller supplies
// its explicit policy; no default is inferred and no policy is unified:
//
//	count and collect: concurrent always;
//	materialized rows: direct at <= fabricApplyLookupBatchSize keys, concurrent above;
//	iterator rows:     sequential.
//
// Concurrent execution shares one semaphore of fabricApplyLookupMaxConcurrency,
// records the first dispatched error, cancels the batch context, waits for every
// worker, and only then returns that error. An empty key set executes nothing.
func (e *FabricExecutor) runApplyLookupKeyBatches(
	ctx context.Context,
	tx *FabricTransaction,
	plan applyLookupPlan,
	inner *FragmentExec,
	params map[string]interface{},
	authToken string,
	entries []applyLookupKeyEntry,
	reducer applyLookupReducer,
	concurrent bool,
) (map[string]interface{}, error) {
	if len(entries) == 0 {
		return map[string]interface{}{}, nil
	}
	batches := applyLookupKeyBatches(entries)
	if !concurrent {
		shared := make(map[string]interface{}, len(entries))
		for _, batch := range batches {
			local, err := e.executeLookupKeyBatch(ctx, tx, plan, inner, params, authToken, batch, reducer)
			if err != nil {
				return nil, err
			}
			mergeApplyLookupBatch(reducer, shared, local)
		}
		return shared, nil
	}

	shared := make(map[string]interface{}, len(entries))
	var sharedMu sync.Mutex
	ctxBatch, cancelBatch := context.WithCancel(ctx)
	defer cancelBatch()
	sem := make(chan struct{}, fabricApplyLookupMaxConcurrency)
	errCh := make(chan error, 1)
	var wg sync.WaitGroup
	for _, batch := range batches {
		wg.Add(1)
		go func(batch []interface{}) {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
			case <-bulk.Done(ctxBatch):
				return
			}
			defer func() { <-sem }()

			local, err := e.executeLookupKeyBatch(ctxBatch, tx, plan, inner, params, authToken, batch, reducer)
			if err != nil {
				select {
				case errCh <- err:
					cancelBatch()
				default:
				}
				return
			}
			sharedMu.Lock()
			mergeApplyLookupBatch(reducer, shared, local)
			sharedMu.Unlock()
		}(batch)
	}
	wg.Wait()
	select {
	case err := <-errCh:
		return nil, err
	default:
	}
	return shared, nil
}

// runCountLookup executes a count plan: concurrent key batches, one combined
// output row per outer row in outer encounter order, and int64(0) for every
// missing, absent or unmatched count. Duplicate outer keys share one lookup and
// every outer row still replays.
func (e *FabricExecutor) runCountLookup(
	ctx context.Context,
	tx *FabricTransaction,
	plan applyLookupPlan,
	input *ResultStream,
	inner *FragmentExec,
	params map[string]interface{},
	authToken string,
) (*ResultStream, error) {
	outerIdx := plan.outerIndex
	entries, correspondence := collectApplyLookupKeys(input.Rows, plan.keyIndex, len(input.Rows))
	grouped, err := e.runApplyLookupKeyBatches(ctx, tx, plan, inner, params, authToken, entries, applyLookupCountReducer{}, true)
	if err != nil {
		return nil, err
	}

	result := &ResultStream{
		Columns: combineColumns(input.Columns, plan.ownerCols),
		Rows:    make([][]interface{}, 0, len(input.Rows)),
	}
	combiner := newCompiledRowCombiner(result.Columns, outerIdx, buildColumnIndex(plan.ownerCols))
	for i, outerRow := range input.Rows {
		values := plan.missingValues
		if entry, ok := correspondence.entryAt(i); ok {
			if found, exists := grouped[entries[entry].text]; exists && found != nil {
				if listed, ok := found.([]interface{}); ok {
					values = listed
				}
			}
		}
		result.Rows = append(result.Rows, combiner.combine(outerRow, values))
	}
	return result, nil
}

// runIteratorRowLookup is the streaming property-row consumer: it windows the
// outer iterator at 1024 rows, copies each window, and executes that window's
// distinct keys sequentially, preserving the shipped policy. A declined input
// never reaches it, so a decline never consumes the iterator.
func (e *FabricExecutor) runIteratorRowLookup(
	ctx context.Context,
	tx *FabricTransaction,
	plan applyLookupPlan,
	inputIter RowIterator,
	inner *FragmentExec,
	params map[string]interface{},
	authToken string,
) (*ResultStream, error) {
	result := &ResultStream{
		Columns: combineColumns(plan.outerColumns, plan.ownerCols),
		Rows:    make([][]interface{}, 0, 4096),
	}
	markApplyBatchedLookupRows(ctx)
	combiner := newCompiledRowCombiner(result.Columns, plan.outerIndex, buildColumnIndex(plan.ownerCols))

	const outerChunkSize = 1024
	window := make([][]interface{}, 0, outerChunkSize)
	flush := func() error {
		if len(window) == 0 {
			return nil
		}
		if _, err := e.runRowLookupWindow(ctx, tx, plan, window, result, combiner, inner, params, authToken, false); err != nil {
			return err
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
		window = append(window, rowCopy)
		if len(window) >= outerChunkSize {
			if err := flush(); err != nil {
				return nil, err
			}
			window = window[:0]
		}
	}
	if err := inputIter.Err(); err != nil {
		return nil, fmt.Errorf("apply input failed: %w", err)
	}
	if err := flush(); err != nil {
		return nil, err
	}
	return result, nil
}

// runMaterializedRowLookup is the materialized property-row consumer: it
// windows the input at 1024 rows and executes each window's distinct keys
// directly up to one key batch and concurrently above it, preserving the
// shipped policy.
func (e *FabricExecutor) runMaterializedRowLookup(
	ctx context.Context,
	tx *FabricTransaction,
	plan applyLookupPlan,
	input *ResultStream,
	inner *FragmentExec,
	params map[string]interface{},
	authToken string,
) (*ResultStream, error) {
	result := &ResultStream{
		Columns: combineColumns(plan.outerColumns, plan.ownerCols),
		Rows:    make([][]interface{}, 0, minInt(len(input.Rows)*2, 4096)),
	}
	markApplyBatchedLookupRows(ctx)
	combiner := newCompiledRowCombiner(result.Columns, plan.outerIndex, buildColumnIndex(plan.ownerCols))

	const outerChunkSize = 1024
	for start := 0; start < len(input.Rows); start += outerChunkSize {
		end := minInt(start+outerChunkSize, len(input.Rows))
		window := input.Rows[start:end]
		if _, err := e.runRowLookupWindow(ctx, tx, plan, window, result, combiner, inner, params, authToken, true); err != nil {
			return nil, err
		}
	}
	return result, nil
}

// runRowLookupWindow executes one 1024-row outer window of a property-row plan
// and appends to result in outer encounter order. Per-key return modifiers run
// once per key, after grouping and before replay. Sequential and concurrent
// windows share this one execute-and-bind path.
func (e *FabricExecutor) runRowLookupWindow(
	ctx context.Context,
	tx *FabricTransaction,
	plan applyLookupPlan,
	rows [][]interface{},
	result *ResultStream,
	combiner compiledRowCombiner,
	inner *FragmentExec,
	params map[string]interface{},
	authToken string,
	allowConcurrent bool,
) (bool, error) {
	entries, correspondence := collectApplyLookupKeys(rows, plan.keyIndex, fabricApplyLookupBatchSize)
	if len(entries) == 0 {
		return false, nil
	}
	grouped, err := e.runApplyLookupKeyBatches(ctx, tx, plan, inner, params, authToken, entries, applyLookupRowsReducer{}, allowConcurrent && len(entries) > fabricApplyLookupBatchSize)
	if err != nil {
		return true, err
	}
	if plan.modifiers != "" {
		for k, product := range grouped {
			matches := applyLookupRowProducts(product)
			if len(matches) == 0 {
				continue
			}
			rs := &ResultStream{Columns: plan.ownerCols, Rows: matches}
			applySimpleResultModifiers(rs, plan.modifiers, plan.modifierAliases)
			grouped[k] = rs.Rows
		}
	}
	for i, outerRow := range rows {
		entry, ok := correspondence.entryAt(i)
		if !ok {
			continue
		}
		matches := applyLookupRowProducts(grouped[entries[entry].text])
		for _, innerRow := range matches {
			result.Rows = append(result.Rows, combiner.combine(outerRow, innerRow))
		}
	}
	return true, nil
}

// applyLookupKeyString is the key correlating an outer row's lookup value
// with the rows a batched lookup returned for it: numbers are keyed by Cypher
// equality (convert's equivalence keys), whichever participant or transport
// produced them.
func applyLookupKeyString(v interface{}) string {
	switch x := v.(type) {
	case nil:
		return "<nil>"
	case string:
		return "s:" + normalizeLookupString(x)
	case []byte:
		return "s:" + normalizeLookupString(string(x))
	case int:
		return convert.IntegerEquivalenceKey(int64(x))
	case int32:
		return convert.IntegerEquivalenceKey(int64(x))
	case int64:
		return convert.IntegerEquivalenceKey(x)
	case uint64:
		return convert.UnsignedEquivalenceKey(x)
	case float32:
		return convert.FloatEquivalenceKey(float64(x))
	case float64:
		// Cypher equality: 42 and 42.0 are one key, NaN matches nothing.
		return convert.FloatEquivalenceKey(x)
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

// applyCorrelatedLookupSpec is the parsed correlated subquery every batched
// APPLY lookup owner reads. Relocated with the pipeline that owns it.
type applyCorrelatedLookupSpec struct {
	importCol        string
	useClause        string
	matchPart        string
	wherePart        string
	returnProjection string
	returnModifiers  string
}

type fabricReturnItem struct {
	expr  string
	prop  string
	alias string
}

type fabricCountReturnItem struct {
	expr  string
	alias string
}

// tryExecuteApplyBatchedLookupRows is the materialized property-row adapter. It
// admits the shape once, before any execution, then delegates the whole
// materialized window pipeline; it returns handled=false for every declined
// input and never partially executes.
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
	plan, ok := e.planApplyLookupShape(ctx, applyLookupShapeRows, applyLookupMaterializedInput, inner, inputResult.Columns, inputResult.Columns, params)
	if !ok {
		return nil, false, nil
	}
	result, err := e.runMaterializedRowLookup(ctx, tx, plan, inputResult, inner, params, authToken)
	if err != nil {
		return nil, true, err
	}
	return result, true, nil
}

// tryExecuteApplyBatchedLookup keeps both shipped roles and its signature.
// aggregateInput == nil selects the streaming iterator property-row consumer,
// admitted once before any iteration so a declined query never consumes the
// iterator. A non-nil aggregateInput selects the collect consumer, whose
// aggregate cardinality and reducer share the same source/transport plan.
func (e *FabricExecutor) tryExecuteApplyBatchedLookup(
	ctx context.Context,
	tx *FabricTransaction,
	inputCols []string,
	inputIter RowIterator,
	inner *FragmentExec,
	params map[string]interface{},
	authToken string,
	aggregateInput *ResultStream,
) (*ResultStream, bool, error) {
	if aggregateInput != nil {
		return e.tryExecuteApplyBatchedCollectLookup(ctx, tx, inputCols, inner, params, authToken, aggregateInput)
	}
	if len(inputCols) == 0 || inputIter == nil || inner == nil {
		return nil, false, nil
	}
	plan, ok := e.planApplyLookupShape(ctx, applyLookupShapeRows, applyLookupIteratorInput, inner, inputCols, inputCols, params)
	if !ok {
		return nil, false, nil
	}
	result, err := e.runIteratorRowLookup(ctx, tx, plan, inputIter, inner, params, authToken)
	if err != nil {
		return nil, true, err
	}
	return result, true, nil
}

// tryExecuteApplyBatchedCountLookup is the count adapter. It admits the shape
// once, then delegates the concurrent count pipeline; it returns handled=false
// for every declined input and never partially executes.
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
	plan, ok := e.planApplyLookupShape(ctx, applyLookupShapeCount, applyLookupMaterializedInput, inner, inputResult.Columns, inputResult.Columns, params)
	if !ok {
		return nil, false, nil
	}
	result, err := e.runCountLookup(ctx, tx, plan, inputResult, inner, params, authToken)
	if err != nil {
		return nil, true, err
	}
	return result, true, nil
}

// tryExecuteApplyBatchedCollectLookup retains aggregate admission and replay;
// projection, correlation, query construction and scheduling have one plan owner.
func (e *FabricExecutor) tryExecuteApplyBatchedCollectLookup(
	ctx context.Context,
	tx *FabricTransaction,
	inputCols []string,
	inner *FragmentExec,
	params map[string]interface{},
	authToken string,
	aggregateInput *ResultStream,
) (*ResultStream, bool, error) {
	if len(inputCols) == 0 || inner == nil || aggregateInput == nil || len(aggregateInput.Rows) == 0 {
		return nil, false, nil
	}
	plan, ok := e.planApplyLookupShape(ctx, applyLookupShapeCollect, applyLookupMaterializedInput, inner, inputCols, aggregateInput.Columns, params)
	if !ok {
		return nil, false, nil
	}
	entries, correspondence := collectApplyLookupKeys(aggregateInput.Rows, plan.keyIndex, fabricApplyLookupBatchSize)
	grouped, err := e.runApplyLookupKeyBatches(ctx, tx, plan, inner, params, authToken, entries, applyLookupCollectReducer{}, true)
	if err != nil {
		return nil, true, err
	}
	result := &ResultStream{
		Columns: combineColumns(aggregateInput.Columns, plan.ownerCols),
		Rows:    make([][]interface{}, 0, len(aggregateInput.Rows)),
	}
	combiner := newCompiledRowCombiner(result.Columns, plan.outerIndex, buildColumnIndex(plan.ownerCols))
	for i, outerRow := range aggregateInput.Rows {
		var value interface{}
		if entry, ok := correspondence.entryAt(i); ok {
			value = grouped[entries[entry].text]
		}
		if value == nil {
			value = []interface{}{}
		}
		result.Rows = append(result.Rows, combiner.combine(outerRow, []interface{}{value}))
	}
	return result, true, nil
}
