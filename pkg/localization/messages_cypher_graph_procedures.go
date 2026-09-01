package localization

import "strconv"

const (
	MessageCypherGraphProceduresGraphNameRequired             MessageID = "cyphergraphprocedures.graph_name_required"
	MessageCypherGraphProceduresStreamNodesFailed             MessageID = "cyphergraphprocedures.stream_nodes_failed"
	MessageCypherGraphProceduresStreamEdgesFailed             MessageID = "cyphergraphprocedures.stream_edges_failed"
	MessageCypherGraphProceduresGraphDoesNotExist             MessageID = "cyphergraphprocedures.graph_does_not_exist"
	MessageCypherGraphProceduresGraphDoesNotExistProjectFirst MessageID = "cyphergraphprocedures.graph_does_not_exist_project_first"
	MessageCypherGraphProceduresBuildGraphFailed              MessageID = "cyphergraphprocedures.build_graph_failed"
	MessageCypherGraphProceduresInvalidProcedureCallSyntax    MessageID = "cyphergraphprocedures.invalid_procedure_call_syntax"
	MessageCypherGraphProceduresVariableNotFound              MessageID = "cyphergraphprocedures.variable_not_found"
	MessageCypherGraphProceduresSourceNodeRequired            MessageID = "cyphergraphprocedures.source_node_required"
)

// CypherGraphProceduresGraphNameRequired identifies a missing GDS graph name.
func CypherGraphProceduresGraphNameRequired(procedure string) Message {
	return Message{ID: MessageCypherGraphProceduresGraphNameRequired, Fallback: "graph name required for " + procedure, Data: map[string]any{"Procedure": procedure}}
}

// CypherGraphProceduresStreamNodesFailed identifies a wrapped projection node-stream failure.
func CypherGraphProceduresStreamNodesFailed(cause error) Message {
	return Message{ID: MessageCypherGraphProceduresStreamNodesFailed, Fallback: "failed to stream nodes: " + cause.Error(), Data: map[string]any{"Cause": cause.Error()}}
}

// CypherGraphProceduresStreamEdgesFailed identifies a wrapped projection edge-stream failure.
func CypherGraphProceduresStreamEdgesFailed(cause error) Message {
	return Message{ID: MessageCypherGraphProceduresStreamEdgesFailed, Fallback: "failed to stream edges: " + cause.Error(), Data: map[string]any{"Cause": cause.Error()}}
}

// CypherGraphProceduresGraphDoesNotExist identifies a missing projected graph.
func CypherGraphProceduresGraphDoesNotExist(graph string) Message {
	return Message{ID: MessageCypherGraphProceduresGraphDoesNotExist, Fallback: "graph '" + graph + "' does not exist", Data: map[string]any{"Graph": graph}}
}

// CypherGraphProceduresGraphDoesNotExistProjectFirst identifies a missing FastRP graph projection.
func CypherGraphProceduresGraphDoesNotExistProjectFirst(graph string) Message {
	return Message{ID: MessageCypherGraphProceduresGraphDoesNotExistProjectFirst, Fallback: "graph '" + graph + "' does not exist. Create it with gds.graph.project first", Data: map[string]any{"Graph": graph}}
}

// CypherGraphProceduresBuildGraphFailed identifies a wrapped link-prediction graph build failure.
func CypherGraphProceduresBuildGraphFailed(cause error) Message {
	return Message{ID: MessageCypherGraphProceduresBuildGraphFailed, Fallback: "failed to build graph: " + cause.Error(), Data: map[string]any{"Cause": cause.Error()}}
}

// CypherGraphProceduresInvalidProcedureCallSyntax identifies malformed link-prediction call syntax.
func CypherGraphProceduresInvalidProcedureCallSyntax() Message {
	return Message{ID: MessageCypherGraphProceduresInvalidProcedureCallSyntax, Fallback: "invalid procedure call syntax"}
}

// CypherGraphProceduresVariableNotFound identifies an unresolved id(variable) expression.
func CypherGraphProceduresVariableNotFound(variable string) Message {
	return Message{ID: MessageCypherGraphProceduresVariableNotFound, Fallback: "variable " + strconv.Quote(variable) + " not found in query context (id(" + variable + ") cannot be resolved)", Data: map[string]any{"Variable": variable, "QuotedVariable": strconv.Quote(variable)}}
}

// CypherGraphProceduresSourceNodeRequired identifies a missing link-prediction sourceNode parameter.
func CypherGraphProceduresSourceNodeRequired() Message {
	return Message{ID: MessageCypherGraphProceduresSourceNodeRequired, Fallback: "sourceNode parameter required"}
}
