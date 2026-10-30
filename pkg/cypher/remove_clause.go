package cypher

import (
	"context"
	"strings"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// removeItem is one item of a REMOVE clause (parseRemoveItems): labels of a
// node (n:A:$(expr)), a property (n.p), or a dynamic property key (n[expr]).
type removeItem struct {
	variable string
	labels   []labelChainItem
	property string
	// key is the expression of n[expr]; its value names the property at run
	// time (dynamicPropertyKeyOf).
	key string
}

// parseRemoveItems splits a REMOVE body at its top-level commas into items.
// Labels are read by the one label-chain reader (setLabelChainItems), so a
// malformed chain is an error, as is text that is none of the item forms
// (REMOVE n): Neo4j's SyntaxError.
func parseRemoveItems(body string) ([]removeItem, error) {
	parts := splitTopLevelComma(body)
	items := make([]removeItem, 0, len(parts))
	for _, part := range parts {
		item := strings.TrimSpace(part)
		if item == "" {
			continue
		}
		if variable, chain, hasLabels := splitNodeHead(item); hasLabels && cyphertext.IsIdentifier(variable) {
			labels, err := setLabelChainItems(chain)
			if err != nil {
				return nil, err
			}
			items = append(items, removeItem{variable: variable, labels: labels})
			continue
		}
		receiver, inner, subscript, ok := staticPostfixSplit(item)
		variable := ""
		if ok {
			variable, _, _ = parseSetAssignmentTarget(receiver)
		}
		if !cyphertext.IsIdentifier(variable) || (subscript && strings.TrimSpace(inner) == "") {
			return nil, localizedStatusError("Neo.ClientError.Statement.SyntaxError", "InvalidSyntax", localization.CypherMutationsRemoveItemInvalid(item))
		}
		if subscript {
			items = append(items, removeItem{variable: variable, key: strings.TrimSpace(inner)})
			continue
		}
		items = append(items, removeItem{variable: variable, property: normalizePropertyKey(inner)})
	}
	return items, nil
}

// pipelineApplyRemove applies a REMOVE clause to every row: each item's
// labels and dynamic keys are evaluated in the row's context
// (pipelineRowWriteContext), and each entity the clause names is stored once
// per row. A variable bound to null is skipped; a label item on a
// relationship is a TypeError (labelTargetTypeError). A removed property
// counts in properties_set when the entity had it, a removed label in
// labels_removed.
//
// Each row retains an immutable record header and rollback copies of the
// properties and labels it can change. Embedding arrays are shared.
func (e *StorageExecutor) pipelineApplyRemove(ctx context.Context, rows []pipelineRow, clause string, result *ExecuteResult) error {
	items, err := parseRemoveItems(strings.TrimSpace(clause[len("REMOVE"):]))
	if err != nil {
		return err
	}
	store := e.getStorage(ctx)
	// Only a dynamic label or key reads the row's other values.
	dynamic := false
	for _, item := range items {
		dynamic = dynamic || item.key != ""
		for _, label := range item.labels {
			dynamic = dynamic || label.expression != ""
		}
	}
	touched := make([]interface{}, 0, len(items))
	for _, row := range rows {
		if err := ctx.Err(); err != nil {
			return err
		}
		rowCtx, nodes, rels := ctx, map[string]*storage.Node(nil), map[string]*storage.Edge(nil)
		if dynamic {
			rowCtx, nodes, rels = e.pipelineRowWriteContext(ctx, row)
		}
		removals := removeObservations{}
		touched = touched[:0]
		for _, item := range items {
			propertiesBefore, labelsBefore := result.Stats.PropertiesSet, result.Stats.LabelsRemoved
			switch entity := row[item.variable].(type) {
			case *storage.Node:
				if entity == nil {
					continue
				}
				node, state := removals.mutableNode(row, item.variable, entity)
				if err := e.removeFromNode(rowCtx, store, node, item, nodes, rels, result.Stats); err != nil {
					restoreSetEntities(removals.states)
					return err
				}
				if result.Stats.PropertiesSet != propertiesBefore || result.Stats.LabelsRemoved != labelsBefore {
					state.written += result.Stats.PropertiesSet - propertiesBefore
					touched = appendTouchedEntity(touched, node)
				}
			case *storage.Edge:
				if entity == nil {
					continue
				}
				if len(item.labels) > 0 {
					return labelTargetTypeError("Relationship")
				}
				relationship, state := removals.mutableEdge(row, item.variable, entity)
				if err := e.removeProperty(rowCtx, relationship.Properties, item, nodes, rels, result.Stats); err != nil {
					restoreSetEntities(removals.states)
					return err
				}
				if result.Stats.PropertiesSet != propertiesBefore {
					state.written += result.Stats.PropertiesSet - propertiesBefore
					touched = appendTouchedEntity(touched, relationship)
				}
			}
		}
		for _, entity := range touched {
			switch typed := entity.(type) {
			case *storage.Node:
				state := removals.stateOfNode(typed)
				if err := mutateNodeWithObservation(store, state.before, typed); err != nil {
					restoreSetEntities(removals.states)
					return err
				}
				e.notifyNodeMutated(string(typed.ID))
			case *storage.Edge:
				state := removals.stateOfEdge(typed)
				if err := mutateEdgeWithObservation(store, state.beforeEdge, typed); err != nil {
					restoreSetEntities(removals.states)
					return err
				}
				e.notifyEdgeMutated(string(typed.ID))
			}
		}
	}
	return nil
}

// removeObservations retains one pre-clause image per row entity. Its property
// map and labels are also the failure rollback fields.
type removeObservations struct {
	states []setEntityState
}

// mutableNode captures the row's existing entity once before REMOVE changes
// it. Rollback fields also form the immutable observation; embeddings are shared.
func (removals *removeObservations) mutableNode(row pipelineRow, variable string, entity *storage.Node) (*storage.Node, *setEntityState) {
	for index := range removals.states {
		if state := &removals.states[index]; state.node == entity {
			return entity, state
		}
	}
	before := observeNode(entity)
	before.Properties = cloneStringAnyMap(entity.Properties)
	before.Labels = append([]string(nil), entity.Labels...)
	removals.states = append(removals.states, setEntityState{
		variable: variable, node: entity, properties: before.Properties,
		labels: before.Labels, before: before,
	})
	return entity, &removals.states[len(removals.states)-1]
}

// mutableEdge captures only the relationship properties REMOVE can change.
func (removals *removeObservations) mutableEdge(row pipelineRow, variable string, entity *storage.Edge) (*storage.Edge, *setEntityState) {
	for index := range removals.states {
		if state := &removals.states[index]; state.relationship == entity {
			return entity, state
		}
	}
	before := observeEdge(entity)
	before.Properties = cloneStringAnyMap(entity.Properties)
	removals.states = append(removals.states, setEntityState{
		variable: variable, relationship: entity, properties: before.Properties,
		beforeEdge: before,
	})
	return entity, &removals.states[len(removals.states)-1]
}

func (removals *removeObservations) stateOfNode(node *storage.Node) *setEntityState {
	for index := range removals.states {
		if state := &removals.states[index]; state.node == node {
			return state
		}
	}
	return &setEntityState{node: node}
}

func (removals *removeObservations) stateOfEdge(edge *storage.Edge) *setEntityState {
	for index := range removals.states {
		if state := &removals.states[index]; state.relationship == edge {
			return state
		}
	}
	return &setEntityState{relationship: edge}
}

// appendTouchedEntity appends entity to touched unless it is there already.
func appendTouchedEntity(touched []interface{}, entity interface{}) []interface{} {
	for _, existing := range touched {
		if existing == entity {
			return touched
		}
	}
	return append(touched, entity)
}

// removeFromNode applies one REMOVE item to node: its labels (checked
// against the label policies) or one property.
func (e *StorageExecutor) removeFromNode(ctx context.Context, store storage.Engine, node *storage.Node, item removeItem, nodes map[string]*storage.Node, rels map[string]*storage.Edge, stats *QueryStats) error {
	if len(item.labels) == 0 {
		return e.removeProperty(ctx, node.Properties, item, nodes, rels, stats)
	}
	labels, err := e.chainLabelNames(ctx, item.labels, nodes, rels)
	if err != nil {
		return err
	}
	before := node.Labels
	next, removed := removeNodeLabels(node.Labels, labels)
	if removed == 0 {
		return nil
	}
	node.Labels = next
	if err := validatePolicyOnLabelChange(store, node, before); err != nil {
		node.Labels = before
		return err
	}
	stats.LabelsRemoved += int(removed)
	return nil
}

// removeProperty removes the property one REMOVE item names (n.p, or the
// value of n[expr]) from properties.
func (e *StorageExecutor) removeProperty(ctx context.Context, properties map[string]interface{}, item removeItem, nodes map[string]*storage.Node, rels map[string]*storage.Edge, stats *QueryStats) error {
	name := item.property
	if item.key != "" {
		key, err := e.dynamicPropertyKeyOf(ctx, item.key, nodes, rels, true)
		if err != nil {
			return err
		}
		name = key
	}
	if _, exists := properties[name]; exists && name != "" {
		delete(properties, name)
		stats.PropertiesSet++
	}
	return nil
}

// pipelineRowWriteContext is the context a write clause (SET, REMOVE)
// evaluates a row's expressions in: the row's nodes and relationships as
// entity scopes, and its other values as variables. Request parameters keep
// their own namespace even when a row binds a variable with the same name.
func (e *StorageExecutor) pipelineRowWriteContext(ctx context.Context, row pipelineRow) (context.Context, map[string]*storage.Node, map[string]*storage.Edge) {
	nodes := make(map[string]*storage.Node)
	rels := make(map[string]*storage.Edge)
	var values map[string]interface{}
	for name, value := range row {
		switch entity := value.(type) {
		case *storage.Node:
			nodes[name] = entity
		case *storage.Edge:
			rels[name] = entity
		default:
			if values == nil {
				values = valueBindingsLayer(ctx, len(row))
			}
			values[name] = value
		}
	}
	rowCtx := ctx
	if values != nil {
		rowCtx = withValueBindings(rowCtx, values)
	}
	return rowCtx, nodes, rels
}
