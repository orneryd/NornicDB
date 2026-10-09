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
	variable   string
	label      string
	propName   string
	rowField   string // set for the simple row.<field> form used by older batch helpers
	lookupExpr string
	bindingVar string
	byID       bool
}

func (m matchClauseSpec) batchKey() nodeBatchMatchKey {
	if m.byID {
		return nodeBatchMatchKey{label: m.label, prop: "\x00elementId"}
	}
	return nodeBatchMatchKey{label: m.label, prop: m.propName}
}

type unwindBatchRow struct {
	item    interface{}
	itemMap map[string]interface{}
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
	if returnIdx := findKeywordIndexInContext(trimmed, "RETURN"); returnIdx >= 0 {
		mutationPart = strings.TrimSpace(trimmed[:returnIdx])
		returnPart = strings.TrimSpace(trimmed[returnIdx:])
		_, ok := parseUnwindBatchCountReturn(returnPart)
		if !ok {
			return nil, false, nil
		}
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

	upper := upperASCII(mutationPart)
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

	for _, created := range plan.nodeCreates {
		for _, match := range plan.matches {
			if created.label == match.label {
				return nil, false, nil
			}
		}
	}
	schema := e.getStorage(ctx).GetSchema()
	for _, match := range plan.matches {
		if !match.byID && (schema == nil || !schema.HasPropertyIndex(match.label, match.propName)) {
			return nil, false, nil
		}
	}
	rows := make([]map[string]interface{}, 0, len(items))
	for _, item := range items {
		row, ok := toStringAnyMap(item)
		if !ok {
			rows = nil
			break
		}
		rows = append(rows, row)
	}
	if len(rows) > 0 {
		matches := make([]matchClauseSpec, 0, len(plan.matches))
		for _, match := range plan.matches {
			if !match.byID {
				matches = append(matches, match)
			}
		}
		prefetched, unique, err := e.buildRelationshipBatchNodeMatchIndex(e.getStorage(ctx), rows, matches)
		if err != nil {
			return nil, true, err
		}
		if !unique {
			return nil, false, nil
		}
		ctx = context.WithValue(ctx, pipelinePrefetchedNodeCandidatesKey{}, prefetched)
	}
	ctx = context.WithValue(ctx, pipelineIndependentCreateBatchKey{}, true)
	result, handled, err := e.executeUnwindRowsPipeline(ctx, unwindVar, items, restQuery)
	if handled && err == nil {
		e.markUnwindMultiMatchCreateBatchUsed()
	}
	return result, handled, err
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

func (e *StorageExecutor) evaluateUnwindBatchLookupExpr(match matchClauseSpec, row unwindBatchRow) (interface{}, bool) {
	return e.evaluateBatchLookupExpr(match, row.item, row.itemMap)
}

func (e *StorageExecutor) evaluateBatchLookupExpr(match matchClauseSpec, bindingValue interface{}, bindingMap map[string]interface{}) (interface{}, bool) {
	if match.rowField != "" && bindingMap != nil {
		if v, ok := bindingMap[match.rowField]; ok {
			return normalizePropValue(v), true
		}
	}
	expr := strings.TrimSpace(match.lookupExpr)
	if expr == "" {
		return nil, false
	}
	if value, ok := e.evaluateBatchLookupExprFast(expr, match.bindingVar, bindingValue, bindingMap); ok {
		return normalizePropValue(value), true
	}

	values := make(map[string]interface{}, 1)
	if match.bindingVar != "" {
		values[match.bindingVar] = bindingValue
	}
	value := e.evaluateExpressionFromValues(expr, values)
	if literal, ok := value.(string); ok && literal == expr {
		if parsed, parsedOK := parseLiteralValueFromComputedRow(expr); parsedOK {
			return normalizePropValue(parsed), true
		}
		return nil, false
	}
	return normalizePropValue(value), true
}

func (e *StorageExecutor) evaluateBatchLookupExprFast(expr, bindingVar string, bindingValue interface{}, bindingMap map[string]interface{}) (interface{}, bool) {
	if value, ok := evaluateBatchLookupOperand(expr, bindingVar, bindingValue, bindingMap); ok {
		return value, true
	}
	return e.evaluateBatchArithmeticLookupExpr(expr, bindingVar, bindingValue, bindingMap)
}

func (e *StorageExecutor) evaluateBatchArithmeticLookupExpr(expr, bindingVar string, bindingValue interface{}, bindingMap map[string]interface{}) (interface{}, bool) {
	if leftExpr, rightExpr, ok := splitByOperatorWithOptions(expr, " + ", true, false); ok {
		left, leftOK := evaluateBatchLookupOperand(leftExpr, bindingVar, bindingValue, bindingMap)
		right, rightOK := evaluateBatchLookupOperand(rightExpr, bindingVar, bindingValue, bindingMap)
		return e.evaluateArithmeticLookupResult('+', left, leftOK, right, rightOK)
	}
	if leftExpr, rightExpr, ok := splitByOperatorWithOptions(expr, "+", true, false); ok {
		left, leftOK := evaluateBatchLookupOperand(leftExpr, bindingVar, bindingValue, bindingMap)
		right, rightOK := evaluateBatchLookupOperand(rightExpr, bindingVar, bindingValue, bindingMap)
		return e.evaluateArithmeticLookupResult('+', left, leftOK, right, rightOK)
	}
	if leftExpr, rightExpr, ok := splitByOperatorWithOptions(expr, "*", true, false); ok {
		left, leftOK := evaluateBatchLookupOperand(leftExpr, bindingVar, bindingValue, bindingMap)
		right, rightOK := evaluateBatchLookupOperand(rightExpr, bindingVar, bindingValue, bindingMap)
		return e.evaluateArithmeticLookupResult('*', left, leftOK, right, rightOK)
	}
	if leftExpr, rightExpr, ok := splitByOperatorWithOptions(expr, "/", true, false); ok {
		left, leftOK := evaluateBatchLookupOperand(leftExpr, bindingVar, bindingValue, bindingMap)
		right, rightOK := evaluateBatchLookupOperand(rightExpr, bindingVar, bindingValue, bindingMap)
		return e.evaluateArithmeticLookupResult('/', left, leftOK, right, rightOK)
	}
	if leftExpr, rightExpr, ok := splitByOperatorWithOptions(expr, "%", true, false); ok {
		left, leftOK := evaluateBatchLookupOperand(leftExpr, bindingVar, bindingValue, bindingMap)
		right, rightOK := evaluateBatchLookupOperand(rightExpr, bindingVar, bindingValue, bindingMap)
		return e.evaluateArithmeticLookupResult('%', left, leftOK, right, rightOK)
	}
	if leftExpr, rightExpr, ok := splitByOperatorWithOptions(expr, " - ", true, false); ok {
		left, leftOK := evaluateBatchLookupOperand(leftExpr, bindingVar, bindingValue, bindingMap)
		right, rightOK := evaluateBatchLookupOperand(rightExpr, bindingVar, bindingValue, bindingMap)
		return e.evaluateArithmeticLookupResult('-', left, leftOK, right, rightOK)
	}
	if leftExpr, rightExpr, ok := splitByOperatorWithOptions(expr, "-", true, false); ok && strings.TrimSpace(leftExpr) != "" {
		left, leftOK := evaluateBatchLookupOperand(leftExpr, bindingVar, bindingValue, bindingMap)
		right, rightOK := evaluateBatchLookupOperand(rightExpr, bindingVar, bindingValue, bindingMap)
		return e.evaluateArithmeticLookupResult('-', left, leftOK, right, rightOK)
	}
	return nil, false
}

func evaluateBatchLookupOperand(expr, bindingVar string, bindingValue interface{}, bindingMap map[string]interface{}) (interface{}, bool) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return nil, false
	}
	if bindingVar != "" && expr == bindingVar {
		return bindingValue, true
	}
	if field, ok := simpleUnwindFieldRef(expr, bindingVar); ok && bindingMap != nil {
		value, found := bindingMap[field]
		return value, found
	}
	if parsed, ok := parseLiteralValueFromComputedRow(expr); ok {
		return parsed, true
	}
	return nil, false
}

// buildPropsFromSpec assembles a property map for a CREATE from a row with
// the same value semantics as the CREATE core: values are normalized
// (normalizePropValue) and a null value leaves the property unset.
func buildPropsFromSpec(row map[string]any, rowRefs map[string]string, literals map[string]any) map[string]any {
	props := make(map[string]any, len(rowRefs)+len(literals))
	put := func(name string, value any) {
		if value = normalizePropValue(value); value != nil {
			props[name] = value
		}
	}
	for propName, rowField := range rowRefs {
		if row != nil {
			if v, ok := row[rowField]; ok {
				put(propName, v)
			}
		}
	}
	for k, v := range literals {
		put(k, v)
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
		if !referencesUnwindBinding(right, unwindVar) {
			return matchClauseSpec{}, false
		}
		rowField, _ := simpleUnwindFieldRef(right, unwindVar)
		return matchClauseSpec{variable: varName, label: label, rowField: rowField, lookupExpr: right, bindingVar: unwindVar, byID: true}, true
	}
	if !strings.HasPrefix(body, "(") {
		return matchClauseSpec{}, false
	}
	closeIdx := findMatchingParen(body, 0)
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
	if !referencesUnwindBinding(expr, unwindVar) {
		return matchClauseSpec{}, false
	}
	field, _ := simpleUnwindFieldRef(expr, unwindVar)
	return matchClauseSpec{
		variable:   varName,
		label:      label,
		propName:   propName,
		rowField:   field,
		lookupExpr: expr,
		bindingVar: unwindVar,
	}, true
}

func simpleUnwindFieldRef(expr, unwindVar string) (string, bool) {
	expr = strings.TrimSpace(expr)
	dot := strings.Index(expr, ".")
	if dot <= 0 {
		return "", false
	}
	base := strings.TrimSpace(expr[:dot])
	field := strings.TrimSpace(expr[dot+1:])
	if base == unwindVar && isSimpleIdentifier(field) {
		return field, true
	}
	return "", false
}

func referencesUnwindBinding(expr, unwindVar string) bool {
	expr = strings.TrimSpace(expr)
	if expr == unwindVar {
		return true
	}
	for i := 0; i < len(expr); {
		ch := expr[i]
		if isIdentStartByte(ch) {
			start := i
			i++
			for i < len(expr) && isIdentByte(expr[i]) {
				i++
			}
			if expr[start:i] == unwindVar {
				return true
			}
			continue
		}
		i++
	}
	return false
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
	closeIdx := findMatchingParen(body, 0)
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
