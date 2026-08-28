package cypher

// Fast path for bulk-seed UNWIND + multi-MATCH + CREATE queries used by
// seeders (Northwind, fixtures, migrations, etc.).
//
// Shape:
//
//	UNWIND $rows AS row
//	MATCH (a:LabelA {keyA: row.fieldA})     (1..N independent MATCH clauses)
//	MATCH (b:LabelB {keyB: row.fieldB})
//	CREATE (n:LabelN {prop1: row.f1, ...})  (1..N CREATE node clauses)
//	CREATE (n)-[:REL]->(a)                  (0..N CREATE edge clauses
//	CREATE (b)-[:REL2]->(n)                  between bound or new nodes)
//
// No RETURN, no WITH, no WHERE, no nested UNWIND.
//
// The handler parses the mutation body ONCE and then for each row:
//   1. Looks up each MATCH target via the property index when available,
//      falling back to label scan + property filter.
//   2. Constructs storage.Node values for each CREATE-node pattern and
//      calls storage.CreateNode once each.
//   3. Constructs storage.Edge values for each CREATE-edge pattern and
//      calls storage.CreateEdge once each.
//
// This avoids the per-row `replaceVariableInMutationQuery` + `executeInternal`
// cycle that re-parses the Cypher text for every UNWIND item.

import (
	"context"
	"fmt"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/pkg/util"
)

// unwindMultiMatchCreatePlan is the parsed form of the mutation body.
type unwindMultiMatchCreatePlan struct {
	matches     []matchClauseSpec
	nodeCreates []createNodeSpec
	edgeCreates []createEdgeSpec
}

// matchClauseSpec represents a single simple MATCH clause:
//
//	MATCH (variable:Label {propName: row.fieldName})
type matchClauseSpec struct {
	variable string
	label    string
	propName string
	rowField string // row.<rowField>
	byID     bool
}

func (m matchClauseSpec) batchKey() nodeBatchMatchKey {
	if m.byID {
		return nodeBatchMatchKey{label: m.label, prop: "\x00elementId:" + m.rowField}
	}
	return nodeBatchMatchKey{label: m.label, prop: m.propName}
}

// createNodeSpec represents `CREATE (variable:Label {...})`. Properties may
// reference `row.<field>` (rowFieldRefs) or be concrete literals (literals).
type createNodeSpec struct {
	variable     string
	label        string
	rowFieldRefs map[string]string // property name → row field name
	literals     map[string]any    // property name → literal value
}

// createEdgeSpec represents `CREATE (src)-[:TYPE {...}]->(dst)`.
type createEdgeSpec struct {
	startVar     string
	endVar       string
	relType      string
	rowFieldRefs map[string]string
	literals     map[string]any
}

// executeUnwindMultiMatchCreateBatch attempts the fast path. Returns
// (result, true, err) on success, (nil, false, nil) if the shape doesn't
// match (caller falls back), or (nil, true, err) on mid-execution error.
func (e *StorageExecutor) executeUnwindMultiMatchCreateBatch(
	ctx context.Context, unwindVar string, items []interface{}, restQuery string,
) (*ExecuteResult, bool, error) {
	// Bail on shapes we don't handle.
	trimmed := strings.TrimSpace(restQuery)
	if trimmed == "" {
		return nil, false, nil
	}

	mutationPart := trimmed
	returnPart := ""
	countAlias := ""
	if returnIdx := findKeywordIndexInContext(trimmed, "RETURN"); returnIdx >= 0 {
		mutationPart = strings.TrimSpace(trimmed[:returnIdx])
		returnPart = strings.TrimSpace(trimmed[returnIdx:])
		alias, ok := parseUnwindBatchCountReturn(returnPart)
		if !ok {
			return nil, false, nil
		}
		countAlias = alias
	}
	if withIdx := findKeywordIndexInContext(mutationPart, "WITH"); withIdx >= 0 {
		withClause := strings.TrimSpace(mutationPart[withIdx+len("WITH"):])
		if !isSimpleWithPassthroughClause(withClause) {
			return nil, false, nil
		}
		mutationPart = strings.TrimSpace(mutationPart[:withIdx])
	}
	if mutationPart == "" {
		return nil, false, nil
	}

	upper := strings.ToUpper(mutationPart)
	// nested UNWIND / SET / MERGE / DELETE / REMOVE / FOREACH disqualify
	// this fast path. RETURN and a simple passthrough WITH are handled above.
	disqualifiers := []string{"SET", "MERGE", "DELETE", "REMOVE", "FOREACH", "OPTIONAL MATCH"}
	for _, d := range disqualifiers {
		if findKeywordIndex(upper, d) >= 0 {
			return nil, false, nil
		}
	}
	// Nested UNWIND.
	if findKeywordIndex(upper, "UNWIND") >= 0 {
		return nil, false, nil
	}

	plan, ok := parseUnwindMultiMatchCreatePlan(mutationPart, unwindVar)
	if !ok {
		return nil, false, nil
	}

	store := e.getStorage(ctx)
	result := &ExecuteResult{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Stats:   &QueryStats{},
	}
	processedRows := 0

	// Collect all nodes and edges across every row, then issue two bulk
	// storage calls. On Badger+WAL+Async this collapses N*(WAL fsync +
	// schema/lookup index mutation) to 2 round-trips per UNWIND batch.
	pendingNodes := make([]*storage.Node, 0, util.SafePreallocProduct(len(items), len(plan.nodeCreates)))
	pendingEdges := make([]*storage.Edge, 0, util.SafePreallocProduct(len(items), util.SafePreallocSum(len(plan.edgeCreates), 1)))

	// Deterministic resolution rule (no heuristics): every MATCH in a bulk
	// batch MUST have a declared schema property index. The index is used as
	// the accelerator, but any unresolved requested value is verified by a
	// bounded label scan so an incomplete index cannot silently drop rows.
	// If no index exists, refuse the fast path and let the caller fall back.
	schema := store.GetSchema()
	for _, m := range plan.matches {
		if m.byID {
			continue
		}
		if schema == nil || !schema.HasPropertyIndex(m.label, m.propName) {
			return nil, false, nil
		}
	}

	// Coerce the items to maps once so we can walk them twice (prefetch +
	// plan). Refuse the fast path for any non-map row so the caller falls
	// back without us having silently skipped rows.
	rows := make([]map[string]any, 0, len(items))
	for _, item := range items {
		row, ok := toStringAnyMap(item)
		if !ok {
			return nil, false, nil
		}
		rows = append(rows, row)
	}

	// Per-batch MATCH prefetch: for each (label, prop) pair in the plan,
	// gather the distinct set of row values, do one PropertyIndexLookup
	// per distinct value, and BatchGetNodes the resulting IDs in a single
	// round-trip. Build a per-(label, prop) map keyed by row value. This
	// is still deterministic — the lookup is index-driven — but collapses
	// N × 2 per-row Badger point-reads into a bounded handful of calls.
	//
	// The map key format mirrors propEqKeyBatch below so int64/float64
	// numeric equivalents collide (a Bolt int64 row value and a float64
	// stored property both hash to the same key).
	batchIndex := make(map[nodeBatchMatchKey]map[string]*storage.Node, len(plan.matches))
	for _, m := range plan.matches {
		key := m.batchKey()
		if _, seen := batchIndex[key]; seen {
			continue
		}
		// Collect distinct row values for this match.
		distinct := make(map[string]any, len(rows))
		for _, r := range rows {
			val, ok := r[m.rowField]
			if !ok {
				continue
			}
			distinct[propEqKeyBatch(val)] = val
		}
		if len(distinct) == 0 {
			batchIndex[key] = map[string]*storage.Node{}
			continue
		}
		if m.byID {
			idx := make(map[string]*storage.Node, len(distinct))
			for k, value := range distinct {
				elementID, ok := value.(string)
				if !ok {
					continue
				}
				lookupID := strings.TrimSpace(elementID)
				if parts := strings.SplitN(lookupID, ":", 3); len(parts) == 3 && parts[0] == "4" {
					lookupID = parts[2]
				}
				node, err := store.GetNode(storage.NodeID(lookupID))
				if err == nil && node != nil && (m.label == "" || nodeHasAnyLabel(node, []string{m.label})) {
					idx[k] = node
				}
			}
			batchIndex[key] = idx
			continue
		}
		// One PropertyIndexLookup per distinct row value + one GetNode per
		// lookup result. Both operations are O(1) lookups on in-memory
		// structures (schema map + Badger cache/skiplist point read); we
		// avoid BatchGetNodes here because NamespacedEngine.BatchGetNodes
		// returns a map keyed by UNPREFIXED IDs while the schema index
		// yields PREFIXED IDs, so a batched-read + map-lookup mismatches.
		// GetNode handles both forms via idempotent prefixNodeID.
		idx := make(map[string]*storage.Node, len(distinct))
		unresolved := make(map[string]any)
		for k, val := range distinct {
			ids := schema.PropertyIndexLookup(m.label, m.propName, val)
			matched := 0
			for _, id := range ids {
				n, err := store.GetNode(id)
				if err == nil && nodeBatchMatchesValue(n, key, val) {
					matched++
					idx[k] = n
				}
			}
			if matched != 1 {
				delete(idx, k)
				unresolved[k] = val
			}
		}
		if len(unresolved) > 0 {
			scanned, scanUnique, err := buildNodeBatchLabelMatchIndex(store, key, unresolved)
			if err != nil {
				return nil, true, err
			}
			if !scanUnique {
				return nil, false, nil
			}
			for k, node := range scanned {
				idx[k] = node
			}
		}
		batchIndex[key] = idx
	}

	for _, row := range rows {
		prevNodes := len(pendingNodes)
		prevEdges := len(pendingEdges)
		if err := e.planUnwindMultiMatchCreateRowIndexed(plan, row, batchIndex, &pendingNodes, &pendingEdges); err != nil {
			return nil, true, err
		}
		if len(pendingNodes) > prevNodes || len(pendingEdges) > prevEdges {
			processedRows++
		}
	}

	if len(pendingNodes) > 0 {
		if err := store.BulkCreateNodes(pendingNodes); err != nil {
			return nil, true, fmt.Errorf("BulkCreateNodes: %w", err)
		}
		result.Stats.NodesCreated += len(pendingNodes)
	}
	if len(pendingEdges) > 0 {
		if err := store.BulkCreateEdges(pendingEdges); err != nil {
			return nil, true, fmt.Errorf("BulkCreateEdges: %w", err)
		}
		result.Stats.RelationshipsCreated += len(pendingEdges)
	}

	e.markUnwindMultiMatchCreateBatchUsed()
	if countAlias != "" {
		result.Columns = []string{countAlias}
		result.Rows = [][]interface{}{{int64(processedRows)}}
	}
	return result, true, nil
}

func isSimpleWithPassthroughClause(clause string) bool {
	trimmed := strings.TrimSpace(clause)
	if trimmed == "" || strings.HasPrefix(trimmed, ",") || strings.HasSuffix(trimmed, ",") {
		return false
	}
	parts := splitTopLevelComma(clause)
	if len(parts) == 0 {
		return false
	}
	for _, raw := range parts {
		token := strings.TrimSpace(raw)
		if token == "" {
			return false
		}
		asIdx := findKeywordIndexInContext(token, "AS")
		if asIdx >= 0 {
			lhs := strings.TrimSpace(token[:asIdx])
			rhs := strings.TrimSpace(token[asIdx+2:])
			if !isSimpleIdentifier(lhs) || !isSimpleIdentifier(rhs) {
				return false
			}
			continue
		}
		if !isSimpleIdentifier(token) {
			return false
		}
	}
	return true
}

// planUnwindMultiMatchCreateRowIndexed resolves row-bound MATCH targets
// from the per-batch index the caller prebuilt. The caller already ran the
// per-distinct-value PropertyIndexLookup + BatchGetNodes, so this hot loop
// is pure in-memory map access.
//
// An empty lookup result means the row legitimately has no match in the
// user's data (Cypher MATCH semantics: zero the row stream); we return nil
// and skip the row's CREATE clauses, matching standard behaviour.
func (e *StorageExecutor) planUnwindMultiMatchCreateRowIndexed(
	plan unwindMultiMatchCreatePlan,
	row map[string]any,
	batchIndex map[nodeBatchMatchKey]map[string]*storage.Node,
	pendingNodes *[]*storage.Node, pendingEdges *[]*storage.Edge,
) error {
	bound := make(map[string]*storage.Node, len(plan.matches)+len(plan.nodeCreates))

	// 1. Resolve every MATCH target from the pre-batched index.
	for _, m := range plan.matches {
		val, ok := row[m.rowField]
		if !ok {
			return nil
		}
		key := m.batchKey()
		idx, ok := batchIndex[key]
		if !ok {
			return fmt.Errorf("internal: missing batch index for %s.%s", m.label, m.propName)
		}
		node := idx[propEqKeyBatch(val)]
		if node == nil {
			return nil
		}
		bound[m.variable] = node
	}

	// 2. Queue each new node with a minted ID so downstream edges can
	// reference it.
	for _, c := range plan.nodeCreates {
		props := buildPropsFromSpec(row, c.rowFieldRefs, c.literals)
		node := &storage.Node{
			ID:         storage.NodeID(e.generateID()),
			Labels:     []string{c.label},
			Properties: props,
		}
		bound[c.variable] = node
		*pendingNodes = append(*pendingNodes, node)
	}

	// 3. Queue each edge. Both endpoints must be bound (either from MATCH
	// or from a freshly queued CREATE above).
	for _, c := range plan.edgeCreates {
		start, ok := bound[c.startVar]
		if !ok || start == nil {
			return fmt.Errorf("CREATE edge: start variable %q not bound", c.startVar)
		}
		end, ok := bound[c.endVar]
		if !ok || end == nil {
			return fmt.Errorf("CREATE edge: end variable %q not bound", c.endVar)
		}
		edge := &storage.Edge{
			ID:         storage.EdgeID(e.generateID()),
			Type:       c.relType,
			StartNode:  start.ID,
			EndNode:    end.ID,
			Properties: buildPropsFromSpec(row, c.rowFieldRefs, c.literals),
		}
		*pendingEdges = append(*pendingEdges, edge)
	}

	return nil
}

// buildPropsFromSpec assembles a property map for a CREATE from a row.
func buildPropsFromSpec(row map[string]any, rowRefs map[string]string, literals map[string]any) map[string]any {
	props := make(map[string]any, len(rowRefs)+len(literals))
	for propName, rowField := range rowRefs {
		if v, ok := row[rowField]; ok {
			props[propName] = v
		}
	}
	for k, v := range literals {
		props[k] = v
	}
	return props
}

// parseUnwindMultiMatchCreatePlan parses the mutation body into structured
// clauses. Returns (plan, true) on success, (empty, false) if the shape is
// unsupported (any ambiguity means fallback).
func parseUnwindMultiMatchCreatePlan(restQuery, unwindVar string) (unwindMultiMatchCreatePlan, bool) {
	plan := unwindMultiMatchCreatePlan{}

	// Split into clauses on MATCH/CREATE boundaries. We use the existing
	// position-finder so word boundaries are respected.
	matchPositions := findAllKeywordPositions(restQuery, "MATCH")
	createPositions := findAllKeywordPositions(restQuery, "CREATE")

	type boundary struct {
		pos  int
		kind int // 0=MATCH, 1=CREATE
	}
	boundaries := make([]boundary, 0, len(matchPositions)+len(createPositions))
	for _, p := range matchPositions {
		boundaries = append(boundaries, boundary{pos: p, kind: 0})
	}
	for _, p := range createPositions {
		boundaries = append(boundaries, boundary{pos: p, kind: 1})
	}
	// Insertion sort by pos.
	for i := 1; i < len(boundaries); i++ {
		j := i
		for j > 0 && boundaries[j-1].pos > boundaries[j].pos {
			boundaries[j-1], boundaries[j] = boundaries[j], boundaries[j-1]
			j--
		}
	}
	if len(boundaries) == 0 {
		return plan, false
	}

	for i, b := range boundaries {
		end := len(restQuery)
		if i+1 < len(boundaries) {
			end = boundaries[i+1].pos
		}
		body := strings.TrimSpace(restQuery[b.pos:end])
		if b.kind == 0 {
			m, ok := parseSimpleMatchClause(body, unwindVar)
			if !ok {
				return unwindMultiMatchCreatePlan{}, false
			}
			plan.matches = append(plan.matches, m)
		} else {
			// CREATE — could be node or edge.
			node, edge, kind, ok := parseSimpleCreateClause(body, unwindVar)
			if !ok {
				return unwindMultiMatchCreatePlan{}, false
			}
			switch kind {
			case 'n':
				plan.nodeCreates = append(plan.nodeCreates, node)
			case 'e':
				plan.edgeCreates = append(plan.edgeCreates, edge)
			default:
				return unwindMultiMatchCreatePlan{}, false
			}
		}
	}

	// Must have at least one MATCH and at least one CREATE for this fast
	// path to be worth taking.
	if len(plan.matches) == 0 {
		return unwindMultiMatchCreatePlan{}, false
	}
	if len(plan.nodeCreates) == 0 && len(plan.edgeCreates) == 0 {
		return unwindMultiMatchCreatePlan{}, false
	}
	return plan, true
}

// parseSimpleMatchClause parses `MATCH (var:Label {prop: unwindVar.field})`.
func parseSimpleMatchClause(clause, unwindVar string) (matchClauseSpec, bool) {
	body := strings.TrimSpace(strings.TrimPrefix(clause, "MATCH"))
	body = strings.TrimPrefix(body, "match")
	body = strings.TrimSpace(body)
	if whereIdx := findKeywordIndexInContext(body, "WHERE"); whereIdx >= 0 {
		pattern := strings.TrimSpace(body[:whereIdx])
		whereExpr := strings.TrimSpace(body[whereIdx+len("WHERE"):])
		if !strings.HasPrefix(pattern, "(") || !strings.HasSuffix(pattern, ")") {
			return matchClauseSpec{}, false
		}
		inner := strings.TrimSpace(pattern[1 : len(pattern)-1])
		parts := strings.SplitN(inner, ":", 2)
		varName := strings.TrimSpace(parts[0])
		label := ""
		if len(parts) == 2 {
			label = strings.TrimSpace(parts[1])
		}
		if !isSimpleIdentifier(varName) || (label != "" && !isSimpleIdentifier(label)) {
			return matchClauseSpec{}, false
		}
		equals := strings.SplitN(whereExpr, "=", 2)
		if len(equals) != 2 || !strings.EqualFold(strings.TrimSpace(equals[0]), "elementId("+varName+")") {
			return matchClauseSpec{}, false
		}
		right := strings.TrimSpace(equals[1])
		dot := strings.Index(right, ".")
		if dot <= 0 || strings.TrimSpace(right[:dot]) != unwindVar {
			return matchClauseSpec{}, false
		}
		rowField := strings.TrimSpace(right[dot+1:])
		if !isSimpleIdentifier(rowField) {
			return matchClauseSpec{}, false
		}
		return matchClauseSpec{variable: varName, label: label, rowField: rowField, byID: true}, true
	}
	if !strings.HasPrefix(body, "(") {
		return matchClauseSpec{}, false
	}
	closeIdx := indexMatchingParen(body)
	if closeIdx < 0 || closeIdx != len(body)-1 {
		return matchClauseSpec{}, false
	}
	inner := strings.TrimSpace(body[1:closeIdx])
	// Must be `var:Label {prop: var.field}`.
	braceIdx := strings.Index(inner, "{")
	if braceIdx < 0 {
		return matchClauseSpec{}, false
	}
	head := strings.TrimSpace(inner[:braceIdx])
	closeBrace := strings.LastIndex(inner, "}")
	if closeBrace < 0 {
		return matchClauseSpec{}, false
	}
	propsBody := strings.TrimSpace(inner[braceIdx+1 : closeBrace])
	parts := strings.SplitN(head, ":", 2)
	if len(parts) != 2 {
		return matchClauseSpec{}, false
	}
	varName := strings.TrimSpace(parts[0])
	label := strings.TrimSpace(parts[1])
	if !isSimpleIdentifier(varName) || !isSimpleIdentifier(label) {
		return matchClauseSpec{}, false
	}
	// propsBody must be a single `key: var.field` pair (no commas).
	if strings.Contains(propsBody, ",") {
		return matchClauseSpec{}, false
	}
	colonIdx := strings.Index(propsBody, ":")
	if colonIdx <= 0 {
		return matchClauseSpec{}, false
	}
	propName := strings.TrimSpace(propsBody[:colonIdx])
	expr := strings.TrimSpace(propsBody[colonIdx+1:])
	if !isSimpleIdentifier(propName) {
		return matchClauseSpec{}, false
	}
	// expr must be `unwindVar.field`.
	dot := strings.Index(expr, ".")
	if dot <= 0 {
		return matchClauseSpec{}, false
	}
	base := strings.TrimSpace(expr[:dot])
	field := strings.TrimSpace(expr[dot+1:])
	if base != unwindVar || !isSimpleIdentifier(field) {
		return matchClauseSpec{}, false
	}
	return matchClauseSpec{
		variable: varName,
		label:    label,
		propName: propName,
		rowField: field,
	}, true
}

// parseSimpleCreateClause returns either a node spec or edge spec. kind is
// 'n' for node, 'e' for edge, or 0 if unrecognised.
func parseSimpleCreateClause(clause, unwindVar string) (createNodeSpec, createEdgeSpec, byte, bool) {
	body := strings.TrimSpace(strings.TrimPrefix(clause, "CREATE"))
	body = strings.TrimPrefix(body, "create")
	body = strings.TrimSpace(body)

	// Edge form: (a)-[:TYPE]->(b)  or  (a)-[:TYPE {...}]->(b)
	if strings.Contains(body, "-[") && (strings.Contains(body, "]->") || strings.Contains(body, "]<-") ||
		strings.Contains(body, "]-(")) {
		edge, ok := parseSimpleCreateEdge(body, unwindVar)
		if !ok {
			return createNodeSpec{}, createEdgeSpec{}, 0, false
		}
		return createNodeSpec{}, edge, 'e', true
	}

	// Node form: (var:Label {props})
	node, ok := parseSimpleCreateNode(body, unwindVar)
	if !ok {
		return createNodeSpec{}, createEdgeSpec{}, 0, false
	}
	return node, createEdgeSpec{}, 'n', true
}

func parseSimpleCreateNode(body, unwindVar string) (createNodeSpec, bool) {
	if !strings.HasPrefix(body, "(") {
		return createNodeSpec{}, false
	}
	closeIdx := indexMatchingParen(body)
	if closeIdx < 0 || closeIdx != len(body)-1 {
		return createNodeSpec{}, false
	}
	inner := strings.TrimSpace(body[1:closeIdx])
	braceIdx := strings.Index(inner, "{")
	if braceIdx < 0 {
		return createNodeSpec{}, false
	}
	head := strings.TrimSpace(inner[:braceIdx])
	closeBrace := strings.LastIndex(inner, "}")
	if closeBrace < 0 {
		return createNodeSpec{}, false
	}
	propsBody := strings.TrimSpace(inner[braceIdx+1 : closeBrace])
	parts := strings.SplitN(head, ":", 2)
	if len(parts) != 2 {
		return createNodeSpec{}, false
	}
	varName := strings.TrimSpace(parts[0])
	label := strings.TrimSpace(parts[1])
	if !isSimpleIdentifier(varName) || !isSimpleIdentifier(label) {
		return createNodeSpec{}, false
	}
	rowRefs, literals, ok := parsePropsBodyForUnwindFastPath(propsBody, unwindVar)
	if !ok {
		return createNodeSpec{}, false
	}
	return createNodeSpec{
		variable:     varName,
		label:        label,
		rowFieldRefs: rowRefs,
		literals:     literals,
	}, true
}

func parseSimpleCreateEdge(body, unwindVar string) (createEdgeSpec, bool) {
	// Only handle outgoing arrow form: (start)-[:TYPE [{props}]]->(end).
	arrowIdx := strings.Index(body, "]->")
	if arrowIdx < 0 {
		return createEdgeSpec{}, false
	}
	lBracketIdx := strings.LastIndex(body[:arrowIdx], "-[")
	if lBracketIdx < 0 {
		return createEdgeSpec{}, false
	}
	// Start node: body[0 : lBracketIdx] — must be "(start)".
	startPart := strings.TrimSpace(body[:lBracketIdx])
	if !strings.HasPrefix(startPart, "(") || !strings.HasSuffix(startPart, ")") {
		return createEdgeSpec{}, false
	}
	startVar := strings.TrimSpace(startPart[1 : len(startPart)-1])
	if !isSimpleIdentifier(startVar) {
		return createEdgeSpec{}, false
	}
	// End node: body[arrowIdx+3 :] — must be "(end)".
	endPart := strings.TrimSpace(body[arrowIdx+3:])
	if !strings.HasPrefix(endPart, "(") || !strings.HasSuffix(endPart, ")") {
		return createEdgeSpec{}, false
	}
	endVar := strings.TrimSpace(endPart[1 : len(endPart)-1])
	if !isSimpleIdentifier(endVar) {
		return createEdgeSpec{}, false
	}
	// Relationship: body[lBracketIdx+2 : arrowIdx] — `:TYPE` or `:TYPE {props}`.
	rel := strings.TrimSpace(body[lBracketIdx+2 : arrowIdx])
	if !strings.HasPrefix(rel, ":") {
		return createEdgeSpec{}, false
	}
	rel = strings.TrimSpace(rel[1:])
	relType := rel
	propsBody := ""
	if braceIdx := strings.Index(rel, "{"); braceIdx >= 0 {
		relType = strings.TrimSpace(rel[:braceIdx])
		closeBrace := strings.LastIndex(rel, "}")
		if closeBrace < 0 {
			return createEdgeSpec{}, false
		}
		propsBody = strings.TrimSpace(rel[braceIdx+1 : closeBrace])
	}
	if !isSimpleIdentifier(relType) {
		return createEdgeSpec{}, false
	}
	rowRefs, literals, ok := parsePropsBodyForUnwindFastPath(propsBody, unwindVar)
	if !ok {
		return createEdgeSpec{}, false
	}
	return createEdgeSpec{
		startVar:     startVar,
		endVar:       endVar,
		relType:      relType,
		rowFieldRefs: rowRefs,
		literals:     literals,
	}, true
}

// parsePropsBodyForUnwindFastPath parses `k1: unwindVar.f1, k2: 42, k3: 'str'`.
// Values may be `row.field` references or simple scalars (int / float / string /
// bool / null). If any value doesn't fit these forms, returns ok=false.
func parsePropsBodyForUnwindFastPath(propsBody, unwindVar string) (map[string]string, map[string]any, bool) {
	propsBody = strings.TrimSpace(propsBody)
	if propsBody == "" {
		return map[string]string{}, map[string]any{}, true
	}
	rowRefs := map[string]string{}
	literals := map[string]any{}
	for _, pair := range splitTopLevelComma(propsBody) {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		colon := findTopLevelMapKeyValueSeparator(pair)
		if colon <= 0 {
			return nil, nil, false
		}
		key := normalizePropertyKey(strings.TrimSpace(pair[:colon]))
		expr := strings.TrimSpace(pair[colon+1:])
		if !isSimpleIdentifier(key) {
			return nil, nil, false
		}
		// row.field reference?
		if dot := strings.Index(expr, "."); dot > 0 {
			base := strings.TrimSpace(expr[:dot])
			field := strings.TrimSpace(expr[dot+1:])
			if base == unwindVar && isSimpleIdentifier(field) {
				rowRefs[key] = field
				continue
			}
		}
		// Scalar literal.
		if v, ok := parseLiteralScalarForPipeline(expr); ok {
			literals[key] = v
			continue
		}
		// Unsupported expression — bail.
		return nil, nil, false
	}
	return rowRefs, literals, true
}

// propEqKeyBatch produces a stable string key used to index the per-batch
// MATCH-prefetch map. It coerces integer and float types so an int64
// coming from a Bolt row and an equivalent float64 stored on a node hash
// to the same key. Strings, bools, and nil get their own unambiguous
// prefixes so they cannot collide with numeric keys.
func propEqKeyBatch(v interface{}) string {
	if v == nil {
		return "n:"
	}
	if i, ok := coerceInt64(v); ok {
		return fmt.Sprintf("i:%d", i)
	}
	if f, ok := coerceFloat64(v); ok {
		// Integer-valued floats hash to the same key as the int form so a
		// Bolt int64 and a Cypher float literal compare equal through the map.
		if f == float64(int64(f)) {
			return fmt.Sprintf("i:%d", int64(f))
		}
		return fmt.Sprintf("f:%g", f)
	}
	if s, ok := v.(string); ok {
		return "s:" + s
	}
	if b, ok := v.(bool); ok {
		return fmt.Sprintf("b:%t", b)
	}
	return fmt.Sprintf("x:%v", v)
}

func coerceInt64(v interface{}) (int64, bool) {
	switch x := v.(type) {
	case int:
		return int64(x), true
	case int8:
		return int64(x), true
	case int16:
		return int64(x), true
	case int32:
		return int64(x), true
	case int64:
		return x, true
	case uint:
		return int64(x), true
	case uint8:
		return int64(x), true
	case uint16:
		return int64(x), true
	case uint32:
		return int64(x), true
	case uint64:
		return int64(x), true
	}
	return 0, false
}

func coerceFloat64(v interface{}) (float64, bool) {
	switch x := v.(type) {
	case float32:
		return float64(x), true
	case float64:
		return x, true
	}
	if i, ok := coerceInt64(v); ok {
		return float64(i), true
	}
	return 0, false
}

// indexMatchingParen returns the index of the `)` that matches the `(` at
// position 0, respecting nested parens. Returns -1 if not found.
func indexMatchingParen(s string) int {
	if len(s) == 0 || s[0] != '(' {
		return -1
	}
	depth := 0
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return i
			}
		}
	}
	return -1
}
