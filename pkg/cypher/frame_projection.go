package cypher

import "github.com/orneryd/nornicdb/pkg/fabric"

// frameBindingKey is shared by admitted type frames and execution frames.
// Identifier translation is the statement owner's existing symbolic-name map.
func frameBindingKey[T any](names *quotedVariableNames, scope map[string]T, name string) (string, bool) {
	if _, bound := scope[name]; bound {
		return name, true
	}
	if names != nil {
		for internal, original := range names.internal {
			if original.name == name {
				if _, bound := scope[internal]; bound {
					return internal, true
				}
			}
		}
	}
	return name, false
}

// resolveFrameProjection decides the existing embedding option from the live
// frame. A bound candidate is an ordinary projection, with the same resolved
// binding identity in semantic admission and physical execution.
func resolveFrameProjection[T any](clause pipelineClause, names *quotedVariableNames, scope map[string]T) (pipelineClause, bool) {
	name := clause.embeddingCandidate
	if name == "" {
		return clause, false
	}
	binding, bound := frameBindingKey(names, scope, name)
	if !bound {
		return clause, true
	}
	if binding != name {
		clause.text = "WITH " + binding
		clause.projection = &fabric.QueryProjection{Text: binding, Items: []fabric.QueryProjectionItem{{Expression: binding, Name: binding, Column: binding}}}
		clause.syntax = nil
		clause.input = fabric.ExpressionInput{}
		clause.admission = nil
		clause.noLocalScopes = true
	}
	return clause, false
}
