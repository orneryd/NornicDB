package cypher

import (
	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
)

// valueTextRestore maps the text of a canonicalized statement back to the
// client's (rewritten keywords, quoted names) in a value of its result. text
// rewrites every string; key, when set, every map key; statement and
// identifier, when set, a plan's statement text and its operators'
// identifiers. Without statement a plan is left as it is.
type valueTextRestore struct {
	text       func(string) string
	key        func(string) string
	statement  func(string) string
	identifier func(string) string
}

// restoreValueText is the one walk that restores a result value (metadata,
// a plan, rows) with restore. It runs on the value walker, copying only
// what changes, so a canceled statement stops it.
func restoreValueText(walk *materializedRowCheckpoint, value interface{}, restore valueTextRestore) (interface{}, bool) {
	var step cypherfn.ValueStep
	step = func(value interface{}) (interface{}, bool, bool) {
		switch typed := value.(type) {
		case string:
			restored := restore.text(typed)
			return restored, restored != typed, false
		case map[string]interface{}:
			if restore.key == nil {
				return value, false, true
			}
			var renamed map[string]interface{}
			for key, item := range observedEntries(walk, typed) {
				if restored := restore.key(key); restored != key {
					if renamed == nil {
						renamed = make(map[string]interface{}, len(typed))
						for k, v := range observedEntries(walk, typed) {
							renamed[k] = v
						}
					}
					delete(renamed, key)
					renamed[restored] = item
				}
			}
			if renamed == nil {
				return value, false, true
			}
			return renamed, true, true
		case [][]interface{}:
			var copied [][]interface{}
			for index, row := range observedItems(walk, typed) {
				restored, changed := cypherfn.TransformValue(walk, row, step)
				if !changed {
					continue
				}
				if copied == nil {
					copied = make([][]interface{}, len(typed))
					for at, kept := range observedItems(walk, typed) {
						copied[at] = kept
					}
				}
				copied[index] = restored.([]interface{})
			}
			if copied == nil {
				return value, false, false
			}
			return copied, true, false
		case *ExecutionPlan:
			if typed == nil || restore.statement == nil {
				return value, false, false
			}
			plan := *typed
			plan.Query = restore.statement(typed.Query)
			root, rootChanged := cypherfn.TransformValue(walk, typed.Root, step)
			plan.Root, _ = root.(*PlanOperator)
			if plan.Query == typed.Query && !rootChanged {
				return value, false, false
			}
			return &plan, true, false
		case *PlanOperator:
			if typed == nil || restore.statement == nil {
				return value, false, false
			}
			operator := *typed
			operator.Description = restore.text(typed.Description)
			changed := operator.Description != typed.Description
			if typed.Arguments != nil {
				arguments, argumentsChanged := cypherfn.TransformValue(walk, typed.Arguments, step)
				operator.Arguments, _ = arguments.(map[string]interface{})
				changed = changed || argumentsChanged
			}
			if restore.identifier != nil && len(typed.Identifiers) > 0 {
				operator.Identifiers = make([]string, len(typed.Identifiers))
				for index, identifier := range observedItems(walk, typed.Identifiers) {
					operator.Identifiers[index] = restore.identifier(identifier)
					changed = changed || operator.Identifiers[index] != identifier
				}
			}
			if len(typed.Children) > 0 {
				operator.Children = make([]*PlanOperator, len(typed.Children))
				for index, child := range observedItems(walk, typed.Children) {
					restored, childChanged := cypherfn.TransformValue(walk, child, step)
					operator.Children[index], _ = restored.(*PlanOperator)
					changed = changed || childChanged
				}
			}
			if !changed {
				return value, false, false
			}
			return &operator, true, false
		case []interface{}:
			return value, false, true
		}
		// Other values, typed containers included, are kept as they are.
		return value, false, false
	}
	return cypherfn.TransformValue(walk, value, step)
}
