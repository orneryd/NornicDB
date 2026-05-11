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
	upper := strings.ToUpper(trimmed)
	// RETURN / WITH / nested UNWIND / SET / MERGE / DELETE / REMOVE / FOREACH
	// disqualify this fast path. The router has other paths for those.
	disqualifiers := []string{"RETURN", "WITH", "SET", "MERGE", "DELETE", "REMOVE", "FOREACH", "OPTIONAL MATCH"}
	for _, d := range disqualifiers {
		if findKeywordIndex(upper, d) >= 0 {
			return nil, false, nil
		}
	}
	// Nested UNWIND.
	if findKeywordIndex(upper, "UNWIND") >= 0 {
		return nil, false, nil
	}

	plan, ok := parseUnwindMultiMatchCreatePlan(restQuery, unwindVar)
	if !ok {
		return nil, false, nil
	}

	store := e.getStorage(ctx)
	result := &ExecuteResult{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Stats:   &QueryStats{},
	}

	// Collect all nodes and edges across every row, then issue two bulk
	// storage calls. On Badger+WAL+Async this collapses N*(WAL fsync +
	// schema/lookup index mutation) to 2 round-trips per UNWIND batch.
	pendingNodes := make([]*storage.Node, 0, len(items)*len(plan.nodeCreates))
	pendingEdges := make([]*storage.Edge, 0, len(items)*(len(plan.edgeCreates)+1))

	// For every MATCH target, decide on the cheapest lookup strategy:
	//   - If the storage schema exposes a property index for (label, prop),
	//     use it per-row — no preload needed.
	//   - Else preload all nodes with that label once and build an in-memory
	//     (propValue → node) map shared across the batch. This avoids an
	//     O(rows × labelCardinality) scan blowup.
	labelPropIndex := make(map[struct{ label, prop string }]map[string]*storage.Node, len(plan.matches))
	useSchemaIndex := make(map[struct{ label, prop string }]bool, len(plan.matches))
	schema := store.GetSchema()
	for _, m := range plan.matches {
		k := struct{ label, prop string }{m.label, m.propName}
		if _, seen := labelPropIndex[k]; seen {
			continue
		}
		if _, seen := useSchemaIndex[k]; seen {
			continue
		}
		// Does the schema have a property index? (SchemaManager keeps internal
		// maps; a non-nil schema with a configured index will return a slice
		// here even if empty when we ask for a real value.) We detect index
		// presence by asking if ANY IDs exist for a sentinel lookup — but
		// that's wasteful; instead rely on per-row lookup falling back to
		// preload if it returns empty on the first row.
		if schema != nil && schema.HasPropertyIndex(m.label, m.propName) {
			useSchemaIndex[k] = true
			continue
		}
		nodes, err := store.GetNodesByLabel(m.label)
		if err != nil {
			return nil, true, fmt.Errorf("preload %s: %w", m.label, err)
		}
		idx := make(map[string]*storage.Node, len(nodes))
		for _, node := range nodes {
			if node == nil {
				continue
			}
			if v, ok := node.Properties[m.propName]; ok {
				idx[propEqKey(v)] = node
			}
		}
		labelPropIndex[k] = idx
	}

	for _, item := range items {
		row, ok := toStringAnyMap(item)
		if !ok {
			return nil, false, nil
		}
		if err := e.planUnwindMultiMatchCreateRowPreloaded(plan, store, row, labelPropIndex, useSchemaIndex, &pendingNodes, &pendingEdges); err != nil {
			return nil, true, err
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
	return result, true, nil
}

// planUnwindMultiMatchCreateRowPreloaded resolves row-bound MATCH targets
// using either a schema property index (fast per-row lookup) or the
// batch-preloaded in-memory map, whichever was selected for each
// (label, propName) in the plan.
func (e *StorageExecutor) planUnwindMultiMatchCreateRowPreloaded(
	plan unwindMultiMatchCreatePlan,
	store storage.Engine,
	row map[string]any,
	labelPropIndex map[struct{ label, prop string }]map[string]*storage.Node,
	useSchemaIndex map[struct{ label, prop string }]bool,
	pendingNodes *[]*storage.Node, pendingEdges *[]*storage.Edge,
) error {
	bound := make(map[string]*storage.Node, len(plan.matches)+len(plan.nodeCreates))

	// 1. Resolve every MATCH target. Missing row field or no matching node
	// zeroes the row (Cypher MATCH semantics).
	for _, m := range plan.matches {
		val, ok := row[m.rowField]
		if !ok {
			return nil
		}
		key := struct{ label, prop string }{m.label, m.propName}
		var node *storage.Node
		if useSchemaIndex[key] {
			// Schema-indexed lookup.
			if schema := store.GetSchema(); schema != nil {
				ids := schema.PropertyIndexLookup(m.label, m.propName, val)
				for _, id := range ids {
					n, err := store.GetNode(id)
					if err == nil && n != nil {
						node = n
						break
					}
				}
			}
		} else {
			idx, ok := labelPropIndex[key]
			if !ok {
				return fmt.Errorf("internal: missing preloaded index for %s.%s", m.label, m.propName)
			}
			node = idx[propEqKey(val)]
		}
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

// lookupNodeByLabelAndProperty returns the first node with the given label
// whose property matches. Uses the property index when present and falls
// back to a label scan + linear property match.
func (e *StorageExecutor) lookupNodeByLabelAndProperty(
	store storage.Engine, label, propName string, value interface{},
) (*storage.Node, error) {
	if schema := store.GetSchema(); schema != nil {
		ids := schema.PropertyIndexLookup(label, propName, value)
		for _, id := range ids {
			node, err := store.GetNode(id)
			if err == nil && node != nil {
				return node, nil
			}
		}
	}
	nodes, err := store.GetNodesByLabel(label)
	if err != nil {
		return nil, err
	}
	for _, node := range nodes {
		if node == nil {
			continue
		}
		if propEq(node.Properties[propName], value) {
			return node, nil
		}
	}
	return nil, nil
}

// propEqKey returns a stable string key used as a map index for property
// value equality. It normalises integer and float types so an int64 stored
// on a node matches a float64 coming in from Bolt (and vice versa).
func propEqKey(v interface{}) string {
	if v == nil {
		return "n:"
	}
	if i, ok := asInt64(v); ok {
		return fmt.Sprintf("i:%d", i)
	}
	if f, ok := asFloat64(v); ok {
		// Integer-valued floats share the int key so ints and floats collide.
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

// propEq compares two property values with type-insensitive numeric coercion.
// (Property values coming from Bolt row maps are int64/float64; stored node
// properties may be the same. Accepting either avoids MATCH misses.)
func propEq(a, b interface{}) bool {
	if a == nil || b == nil {
		return a == b
	}
	if a == b {
		return true
	}
	if aa, ok := asInt64(a); ok {
		if bb, ok := asInt64(b); ok {
			return aa == bb
		}
	}
	if aa, ok := asFloat64(a); ok {
		if bb, ok := asFloat64(b); ok {
			return aa == bb
		}
	}
	return false
}

func asInt64(v interface{}) (int64, bool) {
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

func asFloat64(v interface{}) (float64, bool) {
	switch x := v.(type) {
	case float32:
		return float64(x), true
	case float64:
		return x, true
	}
	if i, ok := asInt64(v); ok {
		return float64(i), true
	}
	return 0, false
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
		colon := strings.Index(pair, ":")
		if colon <= 0 {
			return nil, nil, false
		}
		key := strings.TrimSpace(pair[:colon])
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
