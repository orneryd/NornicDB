package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"sort"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func (e *StorageExecutor) evaluateCoalesceInContext(view braceView, nodeMap map[string]*storage.Node, edgeMap map[string]*storage.Edge, computedValues map[string]interface{}) interface{} {
	expr := view.text()
	// Extract arguments from COALESCE(arg1, arg2, ...)
	innerStart := strings.Index(expr, "(")
	innerEnd := strings.LastIndex(expr, ")")
	if innerStart == -1 || innerEnd == -1 {
		return nil
	}
	// Split arguments
	args := view.window(innerStart+1, innerEnd).commaViews()

	for _, input := range args {
		arg := input.trim().text()

		// Check computed values
		if val, ok := computedValues[arg]; ok && val != nil {
			return val
		}

		// Handle property access
		if strings.Contains(arg, ".") {
			parts := strings.SplitN(arg, ".", 2)
			varName := parts[0]
			propName := parts[1]

			var receiver interface{}
			if value, ok := computedValues[varName]; ok {
				receiver = value
			} else if node, ok := nodeMap[varName]; ok {
				receiver = node
			} else if edge, ok := edgeMap[varName]; ok {
				receiver = edge
			}
			if value, supported := rowPropertyValue(e.queryVisibleValue(receiver), propName); supported && value != nil {
				return value
			}
			continue
		}

		// Check node map
		if node, ok := nodeMap[arg]; ok && node != nil {
			return node
		}

		// Literal value
		if strings.HasPrefix(arg, "'") && strings.HasSuffix(arg, "'") {
			return arg[1 : len(arg)-1]
		}
	}

	return nil
}

func (e *StorageExecutor) evaluateWithWhereCondition(ctx context.Context, whereClause braceView, values map[string]interface{}) bool {
	return e.evaluateRowPredicate(ctx, whereClause, values)
}

// evaluateMatchWhereCondition is the MATCH-clause entry into the shared row
// predicate evaluator: every WHERE position evaluates identically — a
// non-boolean value is a TypeError, null drops the row (#728).
func (e *StorageExecutor) evaluateMatchWhereCondition(ctx context.Context, whereClause braceView, values map[string]interface{}) bool {
	return e.evaluateMatchRowPredicate(ctx, whereClause, values)
}

// withWhereNeedsFullEvaluator reports whether a WITH-attached WHERE predicate
// has to go through the general expression evaluator rather than this
// function's operator splitting. Function calls break the split because the
// call's own parentheses and dots confuse left/right resolution; label tests
// and boolean combinations have no operator to split on at all.
func withWhereNeedsFullEvaluator(whereClause string) bool {
	if strings.ContainsAny(whereClause, "(") {
		return true
	}
	upper := upperASCII(whereClause)
	for _, tok := range []string{" AND ", " OR ", " XOR ", "NOT "} {
		if strings.Contains(upper, tok) {
			return true
		}
	}
	// A bare label test such as `n:Workload` or `n:A:B`.
	return withWhereIsLabelTest(whereClause)
}

// parseWithWhereLabelTest splits a bare label test such as `n:Workload`,
// `n:A:B` or the label expression `n:A|B` (#860) into its variable and label
// expression. Label tests carry no comparison operator, so operator splitting
// never sees them, and the general expression evaluator does not accept the
// `var:Label` form either -- it has to be evaluated here.
func parseWithWhereLabelTest(whereClause string) (string, *labelExpression, bool) {
	if !withWhereIsLabelTest(whereClause) {
		return "", nil, false
	}
	variable, chain, _ := splitNodeHead(strings.TrimSpace(whereClause))
	if hasLabelExpressionOperator(chain) {
		expr, _ := parseLabelExpression(chain)
		return variable, expr, true
	}
	return variable, labelExpressionOfNames(labelChainNames(chain)), true
}

// entityHasAllLabelsOrTypes evaluates Cypher's colon predicate for a graph
// entity: nodes test their labels against the label expression and
// relationships test their single type as their one label (r:A|B, #860). A
// null binding produces null so projections preserve Cypher's three-valued
// semantics; predicate callers coerce that null to false.
func entityHasAllLabelsOrTypes(value interface{}, expr *labelExpression) interface{} {
	if value == nil {
		return nil
	}
	switch entity := value.(type) {
	case *storage.Node:
		if entity == nil {
			return nil
		}
		return expr.matches(entity.Labels)
	case *storage.Edge:
		if entity == nil {
			return nil
		}
		return expr.matches([]string{entity.Type})
	default:
		return false
	}
}

// entityHasAllLabelsOrTypesPredicate applies WHERE's truthiness to a colon
// predicate. In WHERE, both null and false reject the row.
func entityHasAllLabelsOrTypesPredicate(value interface{}, expr *labelExpression) bool {
	matched, _ := entityHasAllLabelsOrTypes(value, expr).(bool)
	return matched
}

// withWhereIsLabelTest reports whether the predicate is a bare label test,
// which carries no comparison operator and so is invisible to operator
// splitting.
func withWhereIsLabelTest(whereClause string) bool {
	trimmed := strings.TrimSpace(whereClause)
	variable, chain, hasLabels := splitNodeHead(trimmed)
	if !hasLabels || !isSimpleVariable(variable) {
		return false
	}
	if hasLabelExpressionOperator(chain) {
		_, ok := parseLabelExpression(chain)
		return ok
	}
	valid := true
	wellFormed := eachChainLabel(chain, func(name string, quoted bool) {
		if name == "" || (!quoted && !isSimpleVariable(name)) {
			valid = false
		}
	})
	return wellFormed && valid
}

// substituteWithWhereLabelTests replaces every `var:Label` test in a predicate
// with the boolean literal it evaluates to, so a predicate combining label
// tests with AND/OR/NOT can be handed to the general expression evaluator,
// which understands the boolean operators but not the label-test form.
//
// Only tokens shaped exactly like an identifier followed by one or more
// `:Label` segments are rewritten, and only outside string literals, so a colon
// inside quoted text or a map literal is left alone.
func substituteWithWhereLabelTests(view braceView, values map[string]interface{}) braceView {
	whereClause := view.text()
	var out strings.Builder
	var edits []cyphertext.SourceEdit
	i := 0
	for i < len(whereClause) {
		if end, quoted := skipCypherLexicalToken(whereClause, i); quoted {
			out.WriteString(whereClause[i:end])
			i = end
			continue
		}
		c := whereClause[i]
		if !cyphertext.NameStartAt(whereClause, i) || (i > 0 && cyphertext.NamePartAt(whereClause, i-1)) {
			out.WriteByte(c)
			i++
			continue
		}
		j := i
		j = cyphertext.NamePartEnd(whereClause, j)
		if j >= len(whereClause) || whereClause[j] != ':' || view.syntax.product.MapKeyAt(view.source, cyphertext.Span{Start: i, End: j}) {
			out.WriteString(whereClause[i:j])
			i = j
			continue
		}
		variable := whereClause[i:j]
		// One or more :<label expression> segments right after the variable
		// (n:A, n:A:B, n:A|B, n:!A; #860). A space after the colon is not a
		// label test (a map entry such as {name: x}).
		var parts []*labelExpression
		k := j
		for k+1 < len(whereClause) && whereClause[k] == ':' && !cyphertext.IsWhitespaceAt(whereClause, k+1) {
			expr, end, ok := parseLabelExpressionPrefix(whereClause[k+1:])
			if !ok {
				break
			}
			parts = append(parts, expr)
			k += 1 + end
		}
		if len(parts) == 0 {
			out.WriteString(whereClause[i:j])
			i = j
			continue
		}
		expr := parts[0]
		if len(parts) > 1 {
			expr = &labelExpression{kind: labelExpressionAnd, operands: parts}
		}
		acceptedStart := out.Len()
		if entityHasAllLabelsOrTypesPredicate(values[variable], expr) {
			out.WriteString("true")
		} else {
			out.WriteString("false")
		}
		edits = append(edits, cyphertext.SourceEdit{Original: cyphertext.Span{Start: i, End: k}, Accepted: cyphertext.Span{Start: acceptedStart, End: out.Len()}})
		i = k
	}
	if len(edits) == 0 {
		return view
	}
	return braceView{syntax: view.syntax, source: view.source.RewriteWithCopies(out.String(), edits, nil)}
}

// withWhereValueContext splits the computed WITH values into the node and edge
// maps the general expression evaluator expects. Non-graph values (scalars,
// lists, aggregates) are not representable in those maps and are left out; a
// predicate over them is handled by this function's own operator paths.
func withWhereValueContext(values map[string]interface{}) (map[string]*storage.Node, map[string]*storage.Edge) {
	nodeCtx := make(map[string]*storage.Node)
	edgeCtx := make(map[string]*storage.Edge)
	for name, val := range values {
		switch v := val.(type) {
		case *storage.Node:
			nodeCtx[name] = v
		case *storage.Edge:
			edgeCtx[name] = v
		}
	}
	return nodeCtx, edgeCtx
}

// filterNodesByWhereClause filters nodes based on a WHERE clause condition.
// Uses evaluateWhere for consistent condition evaluation.
func (e *StorageExecutor) filterNodesByWhereClause(ctx context.Context, nodes []*storage.Node, whereClause braceView, variable string) []*storage.Node {
	if whereClause.empty() {
		return nodes
	}

	filterFn := e.compileNodeWhereFilter(ctx, variable, whereClause)

	return parallelFilterNodes(nodes, filterFn)
}

// orderSpec represents a single ORDER BY column specification
type orderSpec struct {
	colIdx     int
	descending bool
	propPath   []string
}

// orderResultRows sorts result rows by the specified ORDER BY expression.
// Supports multiple columns: "col1 ASC, col2 DESC"
func (e *StorageExecutor) orderResultRows(rows [][]interface{}, columns []string, orderExpr braceView) [][]interface{} {
	if len(rows) <= 1 {
		return rows
	}

	return e.orderRowsBySpecs(rows, e.parseOrderBySpecs(orderExpr, columns))
}

func (e *StorageExecutor) orderResultRowsForReturnItems(rows [][]interface{}, columns []string, returnItems []returnItem, orderExpr braceView) [][]interface{} {
	if len(rows) <= 1 {
		return rows
	}

	return e.orderRowsBySpecs(rows, e.parseOrderBySpecsWithResolver(orderExpr, columns, func(colName string) int {
		for _, item := range returnItems {
			if item.alias == "" || !strings.EqualFold(strings.TrimSpace(item.expr), colName) {
				continue
			}
			return findOrderByColumnIndex(columns, item.alias)
		}
		return -1
	}))
}

func (e *StorageExecutor) orderRowsBySpecs(rows [][]interface{}, orderSpecs []orderSpec) [][]interface{} {
	if len(orderSpecs) == 0 {
		return rows
	}

	sort.Slice(rows, func(i, j int) bool {
		for _, spec := range orderSpecs {
			left := rows[i][spec.colIdx]
			right := rows[j][spec.colIdx]
			if len(spec.propPath) > 0 {
				left = extractOrderByPropertyValue(left, spec.propPath)
				right = extractOrderByPropertyValue(right, spec.propPath)
			}
			cmp := e.compareOrderValues(left, right)
			if cmp != 0 {
				if spec.descending {
					return cmp > 0
				}
				return cmp < 0
			}
		}
		return false
	})

	return rows
}

// parseOrderBySpecs parses "col1 ASC, col2 DESC" into orderSpec slice
func (e *StorageExecutor) parseOrderBySpecs(orderExpr braceView, columns []string) []orderSpec {
	return e.parseOrderBySpecsWithResolver(orderExpr, columns, nil)
}

func (e *StorageExecutor) parseOrderBySpecsWithResolver(orderExpr braceView, columns []string, resolveExpression func(colName string) int) []orderSpec {
	var specs []orderSpec

	for _, term := range parseOrderByClause(orderExpr) {
		colName, descending := term.column, term.descending

		colIdx := findOrderByColumnIndex(columns, colName)

		if colIdx == -1 && resolveExpression != nil {
			colIdx = resolveExpression(colName)
		}

		propPath := make([]string, 0)
		if colIdx == -1 {
			varName := colName
			baseView := term.view.trim()
			for {
				base, property, ok := splitPostfixPropertyAccess(baseView)
				if !ok {
					break
				}
				propPath = append(propPath, property)
				varName = base
				baseView = baseView.window(0, len(base)).trim()
			}
			for left, right := 0, len(propPath)-1; left < right; left, right = left+1, right-1 {
				propPath[left], propPath[right] = propPath[right], propPath[left]
			}
			for i, col := range columns {
				if strings.EqualFold(col, varName) {
					colIdx = i
					break
				}
			}
			if colIdx == -1 {
				continue
			}
			if len(propPath) == 0 {
				continue
			}
		}

		specs = append(specs, orderSpec{colIdx: colIdx, descending: descending, propPath: propPath})
	}

	return specs
}

func findOrderByColumnIndex(columns []string, colName string) int {
	for i, col := range columns {
		if strings.EqualFold(col, colName) {
			return i
		}
	}
	return -1
}

// sliceRows applies Cypher SKIP and LIMIT after operators such as DISTINCT and
// ORDER BY have produced their complete row stream. A negative limit means no
// limit.
func sliceRows(rows [][]interface{}, skip, limit int) [][]interface{} {
	start := skip
	if start < 0 {
		start = 0
	}
	if start > len(rows) {
		start = len(rows)
	}
	end := len(rows)
	if limit >= 0 && limit < end-start {
		end = start + limit
	}
	return rows[start:end]
}

func extractOrderByPropertyValue(value interface{}, propPath []string) interface{} {
	current := value
	for _, part := range propPath {
		switch v := current.(type) {
		case *storage.Node:
			switch {
			case strings.EqualFold(part, "id"):
				current = string(v.ID)
			case strings.EqualFold(part, "createdAt"):
				// Cypher property access should prefer explicit node properties.
				// Fall back to metadata field for nodes without a createdAt property.
				if v.Properties != nil {
					if propVal, ok := mapLookupCaseInsensitive(v.Properties, "createdAt"); ok {
						current = propVal
						break
					}
				}
				current = v.CreatedAt
			case strings.EqualFold(part, "updatedAt"):
				// Cypher property access should prefer explicit node properties.
				// Fall back to metadata field for nodes without an updatedAt property.
				if v.Properties != nil {
					if propVal, ok := mapLookupCaseInsensitive(v.Properties, "updatedAt"); ok {
						current = propVal
						break
					}
				}
				current = v.UpdatedAt
			default:
				if v.Properties == nil {
					return nil
				}
				if propVal, ok := mapLookupCaseInsensitive(v.Properties, part); ok {
					current = propVal
				} else {
					current = nil
				}
			}
		case map[string]interface{}:
			// Match evaluateExpressionFromValues semantics:
			// properties sub-map first, then top-level keys.
			if propsRaw, ok := mapLookupCaseInsensitive(v, "properties"); ok {
				if props, ok := propsRaw.(map[string]interface{}); ok {
					if propVal, ok := mapLookupCaseInsensitive(props, part); ok {
						current = propVal
						break
					}
				}
			}
			if propVal, ok := mapLookupCaseInsensitive(v, part); ok {
				current = propVal
			} else {
				return nil
			}
		default:
			return nil
		}
		if current == nil {
			return nil
		}
	}
	return current
}

func mapLookupCaseInsensitive(m map[string]interface{}, key string) (interface{}, bool) {
	if val, ok := m[key]; ok {
		return val, true
	}
	for k, v := range m {
		if strings.EqualFold(k, key) {
			return v, true
		}
	}
	return nil, false
}

// compareOrderValues compares two values for ordering
// Returns -1 if a < b, 0 if a == b, 1 if a > b
func (e *StorageExecutor) compareOrderValues(a, b interface{}) int {
	return compareValuesForSort(a, b)
}

// filterNodesByProperties filters nodes to only include those matching ALL specified properties.
// This is used for MATCH pattern property filtering like MATCH (n:Label {prop: value}).
// Uses parallel execution for large datasets (>1000 nodes) for improved performance.
func (e *StorageExecutor) filterNodesByProperties(nodes []*storage.Node, props map[string]interface{}) []*storage.Node {
	if len(props) == 0 {
		return nodes
	}

	// The node-pattern property test (nodePropertiesMatch) is the one owner
	// of pattern-property equality.
	filterFn := func(node *storage.Node) bool {
		return e.nodeMatchesProps(node, props)
	}

	// Use parallel filtering for large datasets
	return parallelFilterNodes(nodes, filterFn)
}

// executeMatchWithUnwind handles MATCH ... WITH ... UNWIND ... RETURN queries
// Complex UNWIND + CALL pattern used by stats queries:
// MATCH (f:File) WITH f, [...] as list UNWIND list as item WITH item, COUNT(*) RETURN item
