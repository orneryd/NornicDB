// SET clause helpers for NornicDB Cypher.
//
// This file contains helper functions for processing SET clauses in Cypher queries.
// These functions handle property assignments, expression evaluation, and string
// operations used during SET operations.
//
// # SET Clause Syntax
//
// SET operations modify node and relationship properties:
//
//	SET n.name = 'Alice'           - Single property
//	SET n.name = 'Alice', n.age = 30  - Multiple properties
//	SET n += {name: 'Alice'}       - Map merge
//	SET n:Label                    - Add label
//
// # Expression Evaluation
//
// SET clauses can use various expressions:
//
//	SET n.id = randomUUID()                    - Function call
//	SET n.ts = timestamp()                     - Built-in function
//	SET n.name = 'prefix-' + toString(n.id)   - String concatenation
//	SET n.list = [1, 2, 3]                     - Array literal
//
// # ELI12
//
// Think of SET like filling out a form:
//
//	SET n.name = 'Alice'
//
// You're writing "Alice" in the "name" box on the form. These helper
// functions make sure the value goes in the right format:
//   - Strings stay as strings: 'hello' → "hello"
//   - Numbers become numbers: 42 → 42
//   - Arrays become lists: [1,2] → [1, 2]
//   - Functions run and give results: timestamp() → 1234567890
//
// # Neo4j Compatibility
//
// These helpers ensure SET clauses work identically to Neo4j.

package cypher

import (
	"context"
	"crypto/rand"
	"fmt"
	"strconv"
	"strings"
	"time"

	cyphertext "github.com/orneryd/nornicdb/pkg/cypher/internal/text"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// applySetMapMergeToNode applies SET n += <expr>: every key of the map (or of
// the node / relationship) is written, and a null value removes the key.
// Row bindings from UNWIND / WITH that travel in the parameter context are
// visible to the expression. Non-map values are an error (setSourceMap).
func (e *StorageExecutor) applySetMapMergeToNode(ctx context.Context, node *storage.Node, varName string, rightExpr string, nodes map[string]*storage.Node, rels map[string]*storage.Edge, writes *setWrites) error {
	if node == nil {
		return nil
	}
	if err := requireSetParameter(ctx, rightExpr); err != nil {
		return err
	}
	// Direct $param / row-path references skip the expression evaluator, so
	// typed maps keep their Go value types and UNWIND rows stay cheap.
	value, direct := resolveDirectParamRef(ctx, rightExpr)
	if !direct {
		value, direct = resolveContextPathRef(ctx, rightExpr)
	}
	if direct {
		props, err := setPropertyMapValue(value, "+=")
		if err != nil {
			return err
		}
		writes.mapEntries(node.Properties, props, false)
		for k, v := range props {
			setNodeProperty(node, k, normalizePropValue(v))
		}
		return nil
	}
	// Row bindings from UNWIND / WITH that travel in the parameter context on
	// fallback mutation paths are variables in the value scope.
	params := getParamsFromContext(ctx)
	evalCtx := ctx
	if len(params) > 0 {
		values := valueBindingsLayer(ctx, len(params))
		for name, value := range params {
			if _, isNode := nodes[name]; !isNode {
				values[name] = value
			}
		}
		evalCtx = withValueBindings(ctx, values)
	}
	props, err := e.setSourceMap(evalCtx, rightExpr, "+=", nodes, rels)
	if err != nil {
		return err
	}
	writes.mapEntries(node.Properties, props, false)
	for k, v := range props {
		setNodeProperty(node, k, normalizePropValue(v))
	}
	return nil
}

// setNodeProperty sets a property on a node.
//
// setNodeProperty sets a property on a node.
//
// The "embedding" property is treated like any other property — it is stored in
// node.Properties and indexed normally. Managed embeddings (from WITH EMBEDDING
// or the background worker) are stored separately in node.ChunkEmbeddings via
// ApplyManagedEmbedding; storage keeps or invalidates them when the node is
// updated, by whether the change touches the embedding source (#963).
//
// # Parameters
//
//   - node: The node to update
//   - propName: The property name
//   - value: The value to set
func setNodeProperty(node *storage.Node, propName string, value interface{}) {
	if node.Properties == nil {
		node.Properties = make(map[string]interface{})
	}
	value = normalizePropValue(value)
	if value == nil {
		delete(node.Properties, propName)
		return
	}
	node.Properties[propName] = value
}

func setRelationshipProperty(relationship *storage.Edge, propName string, value interface{}) {
	if relationship.Properties == nil {
		relationship.Properties = make(map[string]interface{})
	}
	value = normalizePropValue(value)
	if value == nil {
		delete(relationship.Properties, propName)
		return
	}
	relationship.Properties[propName] = value
}

func setPropertyMap(properties map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{}, len(properties))
	for key, value := range properties {
		if value != nil {
			result[key] = normalizePropValue(value)
		}
	}
	return result
}

// propertyMapForSetValue implements Cypher's entity-as-property-map semantics
// for SET target = source while keeping general map coercion strict elsewhere.
func propertyMapForSetValue(value interface{}) (map[string]interface{}, bool) {
	switch entity := value.(type) {
	case *storage.Node:
		if entity == nil {
			return nil, false
		}
		return entity.Properties, true
	case *storage.Edge:
		if entity == nil {
			return nil, false
		}
		return entity.Properties, true
	default:
		return toStringAnyMap(value)
	}
}

func parseSetAssignmentTarget(target string) (variable string, property string, hasProperty bool) {
	target = strings.TrimSpace(target)
	if base, prop, ok := splitPostfixPropertyAccess(target); ok {
		for {
			inner, wrapped := stripEnclosingExpressionParentheses(base)
			if !wrapped {
				break
			}
			base = strings.TrimSpace(inner)
		}
		return strings.TrimSpace(base), prop, true
	}
	for {
		inner, wrapped := stripEnclosingExpressionParentheses(target)
		if !wrapped {
			break
		}
		target = strings.TrimSpace(inner)
	}
	return target, "", false
}

// splitSetAssignments splits a SET clause into individual assignments,
// respecting quotes and nesting in (), [] and {} (function calls, list literals
// such as embeddings, and map literals).
//
// # Parameters
//
//   - setClause: The SET clause string (without "SET" keyword)
//
// # Returns
//
//   - Slice of individual assignments
//
// # Example
//
//	splitSetAssignments("n.name = 'Alice', n.age = 30")
//	// Returns: ["n.name = 'Alice'", "n.age = 30"]
//
//	splitSetAssignments("n.name = concat('a', 'b'), n.x = 1")
//	// Returns: ["n.name = concat('a', 'b')", "n.x = 1"]
//
//	splitSetAssignments("n.embedding = [0.1, 0.2], n.dim = 4")
//	// Returns: ["n.embedding = [0.1, 0.2]", "n.dim = 4"]
func splitSetAssignments(setClause string) []string {
	if strings.TrimSpace(setClause) == "" {
		return nil
	}
	var assignments []string
	var current strings.Builder
	parenDepth := 0
	inQuote := false
	quoteChar := rune(0)

	for i, c := range setClause {
		switch {
		case c == '\'' || c == '"':
			if !inQuote {
				inQuote = true
				quoteChar = c
			} else if c == quoteChar {
				// Check for escaped quote
				if i > 0 && !isBackslashEscaped(setClause, i) {
					inQuote = false
				}
			}
			current.WriteRune(c)
		case (c == '(' || c == '{' || c == '[') && !inQuote:
			parenDepth++
			current.WriteRune(c)
		case (c == ')' || c == '}' || c == ']') && !inQuote:
			parenDepth--
			current.WriteRune(c)
		case c == ',' && !inQuote && parenDepth == 0:
			assignments = append(assignments, strings.TrimSpace(current.String()))
			current.Reset()
		default:
			current.WriteRune(c)
		}
	}

	assignments = append(assignments, strings.TrimSpace(current.String()))

	return assignments
}

// evaluateSetExpression evaluates a Cypher expression for SET clauses.
//
// Handles various expression types including literals, arrays, function calls,
// and string concatenation.
//
// # Parameters
//
//   - expr: The expression string to evaluate
//
// # Returns
//
//   - The evaluated value
//
// # Example
//
//	evaluateSetExpression("'Alice'")      // "Alice"
//	evaluateSetExpression("42")           // int64(42)
//	evaluateSetExpression("true")         // true
//	evaluateSetExpression("[1, 2, 3]")    // []interface{}{1, 2, 3}
//	evaluateSetExpression("timestamp()") // current timestamp
func (e *StorageExecutor) evaluateSetExpression(expr string) interface{} {
	expr = strings.TrimSpace(expr)

	// Handle null
	if strings.EqualFold(expr, "null") {
		return nil
	}

	// Handle simple literals
	if strings.HasPrefix(expr, "'") && strings.HasSuffix(expr, "'") {
		return expr[1 : len(expr)-1]
	}
	if strings.HasPrefix(expr, "\"") && strings.HasSuffix(expr, "\"") {
		return expr[1 : len(expr)-1]
	}
	if expr == "true" {
		return true
	}
	if expr == "false" {
		return false
	}

	// Handle numbers
	if val, err := strconv.ParseInt(expr, 10, 64); err == nil {
		return val
	}
	if val, err := strconv.ParseFloat(expr, 64); err == nil {
		return val
	}

	// Handle arrays (simplified)
	if strings.HasPrefix(expr, "[") && strings.HasSuffix(expr, "]") {
		inner := strings.TrimSpace(expr[1 : len(expr)-1])
		if inner == "" {
			return []interface{}{}
		}
		// Simple split for basic arrays
		parts := strings.Split(inner, ",")
		result := make([]interface{}, len(parts))
		for i, p := range parts {
			result[i] = e.evaluateSetExpression(strings.TrimSpace(p))
		}
		return result
	}

	// Handle function calls and expressions
	lowerExpr := lowerASCII(expr)

	// timestamp() - returns current timestamp in milliseconds
	if lowerExpr == "timestamp()" {
		return time.Now().UnixMilli()
	}

	// datetime() - returns typed datetime
	if lowerExpr == "datetime()" {
		return CypherDateTime{Time: time.Now().UTC()}
	}

	// randomUUID() or randomuuid()
	if lowerExpr == "randomuuid()" {
		return e.generateUUID()
	}

	// Handle string concatenation: 'prefix-' + toString(timestamp()) + '-' + substring(randomUUID(), 0, 8)
	if strings.Contains(expr, " + ") {
		return e.evaluateStringConcat(expr)
	}

	// Handle toString(expr)
	if matchFuncStartAndSuffix(expr, "tostring") {
		inner := extractFuncArgs(expr, "tostring")
		val := e.evaluateSetExpression(inner)
		return fmt.Sprintf("%v", val)
	}

	// Handle substring(str, start, length)
	if matchFuncStartAndSuffix(expr, "substring") {
		return e.evaluateSubstringForSet(expr)
	}

	// If nothing else matched, return as-is (already substituted parameter value)
	return expr
}

// evaluateStringConcat handles string concatenation with +
//
// # Parameters
//
//   - expr: The expression with + operators
//
// # Returns
//
//   - The concatenated string
//
// # Example
//
//	evaluateStringConcat("'Hello' + ' ' + 'World'")
//	// Returns: "Hello World"
func (e *StorageExecutor) evaluateStringConcat(expr string) string {
	var result strings.Builder

	// Split by + but respect quotes and parentheses
	parts := e.splitByPlus(expr)

	for _, part := range parts {
		part = strings.TrimSpace(part)
		val := e.evaluateSetExpression(part)
		result.WriteString(fmt.Sprintf("%v", val))
	}

	return result.String()
}

// hasConcatOperator checks if the expression has a + operator outside of quotes.
// This prevents infinite recursion when property values contain " + " in text.
//
// # Parameters
//
//   - expr: The expression to check
//
// # Returns
//
//   - true if + operator exists at top level
func (e *StorageExecutor) hasConcatOperator(expr string) bool {
	inQuote := false
	quoteChar := rune(0)
	parenDepth := 0

	for i := 0; i < len(expr); i++ {
		c := rune(expr[i])
		switch {
		case c == '\'' || c == '"':
			if !inQuote {
				inQuote = true
				quoteChar = c
			} else if c == quoteChar {
				inQuote = false
			}
		case c == '(' && !inQuote:
			parenDepth++
		case c == ')' && !inQuote:
			parenDepth--
		case c == '+' && !inQuote && parenDepth == 0:
			// Check for space before and after (to avoid matching ++ or += etc)
			hasBefore := i > 0 && expr[i-1] == ' '
			hasAfter := i < len(expr)-1 && expr[i+1] == ' '
			if hasBefore && hasAfter {
				return true
			}
		}
	}
	return false
}

// splitByPlus splits an expression by + operator, respecting quotes and parentheses.
//
// # Parameters
//
//   - expr: The expression to split
//
// # Returns
//
//   - Slice of parts separated by +
func (e *StorageExecutor) splitByPlus(expr string) []string {
	var parts []string
	var current strings.Builder
	parenDepth := 0
	inQuote := false
	quoteChar := rune(0)

	for i := 0; i < len(expr); i++ {
		c := rune(expr[i])
		switch {
		case c == '\'' || c == '"':
			if !inQuote {
				inQuote = true
				quoteChar = c
			} else if c == quoteChar {
				inQuote = false
			}
			current.WriteRune(c)
		case c == '(' && !inQuote:
			parenDepth++
			current.WriteRune(c)
		case c == ')' && !inQuote:
			parenDepth--
			current.WriteRune(c)
		case c == '+' && !inQuote && parenDepth == 0:
			if s := strings.TrimSpace(current.String()); s != "" {
				parts = append(parts, s)
			}
			current.Reset()
		default:
			current.WriteRune(c)
		}
	}

	if s := strings.TrimSpace(current.String()); s != "" {
		parts = append(parts, s)
	}

	return parts
}

// evaluateSubstringForSet handles substring(str, start, length) for SET expressions.
//
// # Parameters
//
//   - expr: The substring function call
//
// # Returns
//
//   - The extracted substring
func (e *StorageExecutor) evaluateSubstringForSet(expr string) string {
	// Extract arguments from substring(str, start, length)
	inner := expr[10 : len(expr)-1] // Remove "substring(" and ")"

	// Split by comma, respecting parentheses
	args := e.splitFunctionArgs(inner)
	if len(args) < 2 {
		return ""
	}

	// Evaluate the string argument
	str := fmt.Sprintf("%v", e.evaluateSetExpression(args[0]))

	// Parse start
	start, err := strconv.Atoi(strings.TrimSpace(args[1]))
	if err != nil {
		start = 0
	}

	// Parse optional length
	length := cyphertext.Length(str) - start
	if len(args) >= 3 {
		if l, err := strconv.Atoi(strings.TrimSpace(args[2])); err == nil {
			length = l
		}
	}

	// Apply substring
	return cyphertext.Substring(str, start, length)
}

// splitFunctionArgs splits a function call's argument text at its top-level
// commas with the shared splitter (splitTopLevelComma): a comma inside
// quotes, a quoted name or a (), [] or {} group belongs to its argument, so
// a map literal with several keys is one argument. The arguments are
// trimmed; an empty last one is dropped.
func (e *StorageExecutor) splitFunctionArgs(args string) []string {
	return splitTopLevelComma(args)
}

// generateUUID generates a simple UUID-like string.
//
// Uses crypto/rand for cryptographically secure random bytes.
//
// # Returns
//
//   - A UUID-formatted string
func (e *StorageExecutor) generateUUID() string {
	// Use crypto/rand for proper UUID
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

// persistSetEntities stores the entities a SET clause wrote (applySetRuns)
// and counts what it wrote in stats (which may be nil): every route stores
// SET's writes this one way. An entity the clause left as it was isn't
// stored, though its writes count (Neo4j counts writes, not changes); a
// node whose labels changed is checked against the label policies first.
// On an error the entity is restored and the error is wrapped by its caller
// (wrap).
func (e *StorageExecutor) persistSetEntities(store storage.Engine, states []setEntityState, stats *QueryStats, wrap func(state setEntityState, err error) error) error {
	for index := range states {
		state := &states[index]
		if stats != nil {
			stats.PropertiesSet += state.written
		}
		if node := state.node; node != nil {
			labelsAdded := addedLabelCount(state.labels, node.Labels)
			if labelsAdded == 0 && changedPropertyCount(state.properties, node.Properties) == 0 {
				continue
			}
			restore := func() {
				node.Properties = state.properties
				node.Labels = state.labels
			}
			if labelsAdded > 0 {
				if err := validatePolicyOnLabelChange(store, node, state.labels); err != nil {
					restore()
					return err
				}
			}
			if stats != nil {
				stats.LabelsAdded += labelsAdded
			}
			if err := store.UpdateNode(node); err != nil {
				restore()
				return wrap(*state, err)
			}
			e.notifyNodeMutated(string(node.ID))
			continue
		}
		relationship := state.relationship
		if changedPropertyCount(state.properties, relationship.Properties) == 0 {
			continue
		}
		if err := store.UpdateEdge(relationship); err != nil {
			relationship.Properties = state.properties
			return wrap(*state, err)
		}
		e.notifyEdgeMutated(string(relationship.ID))
	}
	return nil
}

// setRun is a stretch of a SET clause: its consecutive assignments that
// name one variable (text). The per-entity appliers apply a stretch with
// Neo4j's runs inside it (setPropertyRun); stretches apply in order.
type setRun struct {
	variable string
	text     string
}

// appendSetClauseRuns appends the stretches of a SET body to runs (a
// caller's buffer), in order. Neo4j applies a SET clause's items in order, a
// run of property assignments to one variable at once (setPropertyRun):
// SET n.a = 1, m.b = n.a reads the new n.a, while SET n.a = 1, n.b = n.a
// reads the old one (#907). A chained SET clause (SET a SET b) starts new
// stretches. A stretch's text is a slice of the body. assignments, when not nil, are body's assignments already split
// (splitSetAssignments): they are used when body is one SET clause.
func appendSetClauseRuns(runs []setRun, body string, assignments []string) []setRun {
	for segment, next, ok := nextChainedSetClause(body, 0); ok; segment, next, ok = nextChainedSetClause(body, next) {
		open := false
		start, searchFrom := 0, 0
		items := assignments
		if items == nil || next < len(body) || segment != strings.TrimSpace(body) {
			items = splitSetAssignments(segment)
		}
		for _, assignment := range items {
			assignment = strings.TrimSpace(assignment)
			if assignment == "" {
				continue
			}
			at := searchFrom
			if found := strings.Index(segment[searchFrom:], assignment); found >= 0 {
				at = searchFrom + found
			}
			searchFrom = at + len(assignment)
			target, _, _, _ := splitSetAssignment(assignment)
			if open && runs[len(runs)-1].variable == target {
				runs[len(runs)-1].text = segment[start:searchFrom]
				continue
			}
			runs = append(runs, setRun{variable: target, text: assignment})
			start, open = at, true
		}
	}
	return runs
}

// setEntityState is an entity a SET clause wrote: its properties and
// labels before the clause, and the properties the clause wrote (setWrites).
type setEntityState struct {
	variable     string
	node         *storage.Node
	relationship *storage.Edge
	properties   map[string]interface{}
	labels       []string
	written      int
}

// applySetRuns applies a SET clause's runs (appendSetClauseRuns) in order to the
// nodes and relationships they name, and returns the entities written,
// first written first, appended to states (a caller's buffer). A run on a variable bound to null does nothing; a
// run on a variable that names no entity reports ok false. On an error,
// every entity is restored to its state before the clause.
func (e *StorageExecutor) applySetRuns(ctx context.Context, runs []setRun, nodes map[string]*storage.Node, rels map[string]*storage.Edge, isNull func(variable string) bool, states []setEntityState) ([]setEntityState, bool, error) {
	for _, run := range runs {
		node, relationship := nodes[run.variable], rels[run.variable]
		if node == nil && relationship == nil {
			if isNull(run.variable) {
				continue
			}
			restoreSetEntities(states)
			return nil, false, nil
		}
		target := -1
		for index := range states {
			if states[index].variable == run.variable {
				target = index
				break
			}
		}
		if target < 0 {
			state := setEntityState{variable: run.variable, node: node, relationship: relationship}
			if node != nil {
				state.properties = cloneStringAnyMap(node.Properties)
				state.labels = append([]string(nil), node.Labels...)
			} else {
				state.properties = cloneStringAnyMap(relationship.Properties)
			}
			states = append(states, state)
			target = len(states) - 1
		}
		var written int
		var err error
		if node != nil {
			written, err = e.applySetToNodeWithContext(ctx, node, run.variable, run.text, nodes, rels)
		} else {
			written, err = e.applySetToRelationshipWithContext(ctx, relationship, run.variable, run.text, nodes, rels)
		}
		states[target].written += written
		if err != nil {
			restoreSetEntities(states)
			return nil, true, err
		}
	}
	return states, true, nil
}

// restoreSetEntities puts the entities a SET clause wrote back as they were
// before it.
func restoreSetEntities(states []setEntityState) {
	for _, written := range states {
		if written.node != nil {
			written.node.Properties = written.properties
			written.node.Labels = written.labels
		} else {
			written.relationship.Properties = written.properties
		}
	}
}
