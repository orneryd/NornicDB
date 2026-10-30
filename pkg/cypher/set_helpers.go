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
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// applySetMapMergeToNode applies SET n += <expr>: every key of the map (or of
// the node / relationship) is written, and a null value removes the key.
// Row bindings from UNWIND / WITH are visible in the value scope.
// Non-map values are an error (setSourceMap).
func (e *StorageExecutor) applySetMapMergeToNode(ctx context.Context, node *storage.Node, varName string, rightExpr braceView, nodes map[string]*storage.Node, rels map[string]*storage.Edge, writes *setWrites) error {
	if node == nil {
		return nil
	}
	if err := requireSetParameter(ctx, rightExpr.text()); err != nil {
		return err
	}
	props, err := e.setSourceMap(ctx, rightExpr, "+=", nodes, rels)
	if err != nil {
		return err
	}
	writes.mapEntries(node.Properties, props, false)
	for k, v := range props {
		setNodeProperty(node, k, v)
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
	value = copyStoredPropertyValue(value)
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
	value = copyStoredPropertyValue(value)
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
			result[key] = copyStoredPropertyValue(value)
		}
	}
	return result
}

// copyStoredPropertyValue gives a writer ownership of a generic property list.
// Its elements were already normalized and validated by storablePropertyValue;
// storage copies typed slices at its own boundary, as before.
func copyStoredPropertyValue(value interface{}) interface{} {
	if list, ok := value.([]interface{}); ok {
		copied := make([]interface{}, len(list))
		copy(copied, list)
		return copied
	}
	return value
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

func parseSetAssignmentTarget(target braceView) (variable string, property string, hasProperty bool) {
	return parseSetAssignmentTargetExtent(target, nil)
}

// Author: #1663 lane. The existing target grammar retains the operand it
// selects, including unwrapping parentheses, for Source-taking type consumers.
func parseSetAssignmentTargetExtent(targetView braceView, span *cyphertext.Span) (variable string, property string, hasProperty bool) {
	target := targetView.text()
	extent := cyphertext.TrimWhitespaceSpan(target, cyphertext.Span{End: len(target)})
	if span != nil {
		*span = extent
	}
	targetView = targetView.spanWindow(extent)
	target = targetView.text()
	if base, prop, ok := splitPostfixPropertyAccess(targetView); ok {
		if span != nil {
			span.End = span.Start + len(base)
		}
		for {
			inner, wrapped := stripEnclosingExpressionParentheses(base)
			if !wrapped {
				break
			}
			if span != nil {
				trimmed := cyphertext.TrimWhitespaceSpan(base, cyphertext.Span{Start: 1, End: len(base) - 1})
				start := span.Start
				span.Start, span.End = start+trimmed.Start, start+trimmed.End
			}
			base = inner
		}
		return strings.TrimSpace(base), prop, true
	}
	for {
		inner, wrapped := stripEnclosingExpressionParentheses(target)
		if !wrapped {
			break
		}
		if span != nil {
			trimmed := cyphertext.TrimWhitespaceSpan(target, cyphertext.Span{Start: 1, End: len(target) - 1})
			start := span.Start
			span.Start, span.End = start+trimmed.Start, start+trimmed.End
		}
		target = inner
	}
	return target, "", false
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
	parenDepth := 0

	for i := 0; i < len(expr); i++ {
		if end, quoted := skipCypherLexicalToken(expr, i); quoted {
			i = end - 1
			continue
		}
		c := rune(expr[i])
		switch {
		case c == '(':
			parenDepth++
		case c == ')':
			parenDepth--
		case c == '+' && parenDepth == 0:
			// Check for space before and after (to avoid matching ++ or += etc)
			hasBefore := i > 0 && cyphertext.IsWhitespaceAt(expr, i-1)
			hasAfter := i < len(expr)-1 && cyphertext.IsWhitespaceAt(expr, i+1)
			if hasBefore && hasAfter {
				return true
			}
		}
	}
	return false
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
			if err := updateMutatedNode(store, node); err != nil {
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
		if err := updateMutatedEdge(store, relationship); err != nil {
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
	view     braceView
}

// appendSetClauseRuns appends the stretches of a SET body to runs (a
// caller's buffer), in order. Neo4j applies a SET clause's items in order, a
// run of property assignments to one variable at once (setPropertyRun):
// SET n.a = 1, m.b = n.a reads the new n.a, while SET n.a = 1, n.b = n.a
// reads the old one (#907). A chained SET clause (SET a SET b) starts new
// stretches. A stretch's text is a slice of the body. assignments, when not nil, are body's assignments already split
// (setAssignmentViews): they are used when body is one SET clause.
func appendSetClauseRuns(runs []setRun, body braceView) []setRun {
	for segment, next, ok := nextChainedSetView(body, 0); ok; segment, next, ok = nextChainedSetView(body, next) {
		segment = segment.withoutComments()
		open := false
		start := 0
		for _, span := range commaPartSpans(segment.text(), true) {
			assignment := segment.spanWindow(span)
			if assignment.empty() {
				continue
			}
			target := readSetAssignment(assignment).target
			if open && runs[len(runs)-1].variable == target {
				runs[len(runs)-1].view = segment.window(start, span.End)
				runs[len(runs)-1].text = runs[len(runs)-1].view.text()
				continue
			}
			runs = append(runs, setRun{variable: target, text: assignment.text(), view: assignment})
			start, open = span.Start, true
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
			written, err = e.applySetToNodeWithContext(ctx, node, run.variable, run.view, nodes, rels)
		} else {
			written, err = e.applySetToRelationshipWithContext(ctx, relationship, run.variable, run.view, nodes, rels)
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
