package cypher

import (
	"context"
	"strings"

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
		if variable, chain, hasLabels := splitNodeHead(item); hasLabels && isValidIdentifier(variable) {
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
		if !isValidIdentifier(variable) || (subscript && strings.TrimSpace(inner) == "") {
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
func (e *StorageExecutor) pipelineApplyRemove(ctx context.Context, rows []pipelineRow, clause string, result *ExecuteResult) error {
	items, err := parseRemoveItems(strings.TrimSpace(clause[len("REMOVE"):]))
	if err != nil {
		return err
	}
	store := e.getStorage(ctx)
	touched := make([]interface{}, 0, len(items))
	for _, row := range rows {
		if err := ctx.Err(); err != nil {
			return err
		}
		rowCtx, nodes, rels := e.pipelineRowWriteContext(ctx, row)
		touched = touched[:0]
		for _, item := range items {
			switch entity := row[item.variable].(type) {
			case *storage.Node:
				if entity == nil {
					continue
				}
				if err := e.removeFromNode(rowCtx, store, entity, item, nodes, rels, result.Stats); err != nil {
					return err
				}
				touched = appendTouchedEntity(touched, entity)
			case *storage.Edge:
				if entity == nil {
					continue
				}
				if len(item.labels) > 0 {
					return labelTargetTypeError("Relationship")
				}
				if err := e.removeProperty(rowCtx, entity.Properties, item, nodes, rels, result.Stats); err != nil {
					return err
				}
				touched = appendTouchedEntity(touched, entity)
			}
		}
		for _, entity := range touched {
			switch typed := entity.(type) {
			case *storage.Node:
				if err := store.UpdateNode(typed); err != nil {
					return err
				}
				e.notifyNodeMutated(string(typed.ID))
			case *storage.Edge:
				if err := store.UpdateEdge(typed); err != nil {
					return err
				}
				e.notifyEdgeMutated(string(typed.ID))
			}
		}
	}
	return nil
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
// entity scopes, and its other values as variables, also reachable as
// parameters for resolveContextPathRef.
func (e *StorageExecutor) pipelineRowWriteContext(ctx context.Context, row pipelineRow) (context.Context, map[string]*storage.Node, map[string]*storage.Edge) {
	nodes := make(map[string]*storage.Node)
	rels := make(map[string]*storage.Edge)
	params := make(map[string]interface{})
	for name, value := range getParamsFromContext(ctx) {
		params[name] = value
	}
	var values map[string]interface{}
	for name, value := range row {
		switch entity := value.(type) {
		case *storage.Node:
			nodes[name] = entity
		case *storage.Edge:
			rels[name] = entity
		default:
			params[name] = value
			if values == nil {
				values = valueBindingsLayer(ctx, len(row))
			}
			values[name] = value
		}
	}
	rowCtx := withParams(ctx, params)
	if values != nil {
		rowCtx = withValueBindings(rowCtx, values)
	}
	return rowCtx, nodes, rels
}
