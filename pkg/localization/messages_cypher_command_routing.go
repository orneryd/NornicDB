package localization

import "fmt"

const (
	MessageCypherCommandRoutingUnknownCommand                   MessageID = "cyphercommandrouting.unknown_command"
	MessageCypherCommandRoutingEmptyCommand                     MessageID = "cyphercommandrouting.empty_command"
	MessageCypherCommandRoutingShellUseDatabaseRequired         MessageID = "cyphercommandrouting.shell_use_database_required"
	MessageCypherCommandRoutingParameterMapRequired             MessageID = "cyphercommandrouting.parameter_map_required"
	MessageCypherCommandRoutingParameterArgumentRequired        MessageID = "cyphercommandrouting.parameter_argument_required"
	MessageCypherCommandRoutingParameterValueRequired           MessageID = "cyphercommandrouting.parameter_value_required"
	MessageCypherCommandRoutingParameterUsage                   MessageID = "cyphercommandrouting.parameter_usage"
	MessageCypherCommandRoutingParameterMapKeysStrings          MessageID = "cyphercommandrouting.parameter_map_keys_strings"
	MessageCypherCommandRoutingParameterMapEntryEmpty           MessageID = "cyphercommandrouting.parameter_map_entry_empty"
	MessageCypherCommandRoutingParameterMapEntryInvalid         MessageID = "cyphercommandrouting.parameter_map_entry_invalid"
	MessageCypherCommandRoutingParameterEvaluationFailed        MessageID = "cyphercommandrouting.parameter_evaluation_failed"
	MessageCypherCommandRoutingParameterProducedNoValue         MessageID = "cyphercommandrouting.parameter_produced_no_value"
	MessageCypherCommandRoutingParameterExpressionUnresolved    MessageID = "cyphercommandrouting.parameter_expression_unresolved"
	MessageCypherCommandRoutingCallTailSeedRequired             MessageID = "cyphercommandrouting.call_tail_seed_required"
	MessageCypherCommandRoutingCallTailLimitInvalid             MessageID = "cyphercommandrouting.call_tail_limit_invalid"
	MessageCypherCommandRoutingCallTailSkipInvalid              MessageID = "cyphercommandrouting.call_tail_skip_invalid"
	MessageCypherCommandRoutingWhereBooleanRequired             MessageID = "cyphercommandrouting.where_boolean_required"
	MessageCypherCommandRoutingUnknownProcedure                 MessageID = "cyphercommandrouting.unknown_procedure"
	MessageCypherCommandRoutingFabricNotPrepared                MessageID = "cyphercommandrouting.fabric_not_prepared"
	MessageCypherCommandRoutingFabricStorageTypeInvalid         MessageID = "cyphercommandrouting.fabric_storage_type_invalid"
	MessageCypherCommandRoutingFabricPreparedContextInvalid     MessageID = "cyphercommandrouting.fabric_prepared_context_invalid"
	MessageCypherCommandRoutingFabricConstituentsFailed         MessageID = "cyphercommandrouting.fabric_constituents_failed"
	MessageCypherCommandRoutingFabricShardTransactionFailed     MessageID = "cyphercommandrouting.fabric_shard_transaction_failed"
	MessageCypherCommandRoutingUseDatabaseRequired              MessageID = "cyphercommandrouting.use_database_required"
	MessageCypherCommandRoutingUseInvalid                       MessageID = "cyphercommandrouting.use_invalid"
	MessageCypherCommandRoutingUseBacktickUnterminated          MessageID = "cyphercommandrouting.use_backtick_unterminated"
	MessageCypherCommandRoutingGraphReferenceInvalid            MessageID = "cyphercommandrouting.graph_reference_invalid"
	MessageCypherCommandRoutingGraphReferenceArgumentRequired   MessageID = "cyphercommandrouting.graph_reference_argument_required"
	MessageCypherCommandRoutingGraphReferenceOpenParenExpected  MessageID = "cyphercommandrouting.graph_reference_open_paren_expected"
	MessageCypherCommandRoutingGraphReferenceUnterminated       MessageID = "cyphercommandrouting.graph_reference_unterminated"
	MessageCypherCommandRoutingGraphReferenceArgumentEmpty      MessageID = "cyphercommandrouting.graph_reference_argument_empty"
	MessageCypherCommandRoutingGraphReferenceStringUnterminated MessageID = "cyphercommandrouting.graph_reference_string_unterminated"
	MessageCypherCommandRoutingBacktickIdentifierUnterminated   MessageID = "cyphercommandrouting.backtick_identifier_unterminated"
	MessageCypherCommandRoutingUseConstituentOutsideComposite   MessageID = "cyphercommandrouting.use_constituent_outside_composite"
	MessageCypherCommandRoutingUseFailed                        MessageID = "cyphercommandrouting.use_failed"
	MessageCypherCommandRoutingUseBackendUnsupported            MessageID = "cyphercommandrouting.use_backend_unsupported"
	MessageCypherCommandRoutingUseDatabaseManagerUnavailable    MessageID = "cyphercommandrouting.use_database_manager_unavailable"
	MessageCypherCommandRoutingUseStorageTypeInvalid            MessageID = "cyphercommandrouting.use_storage_type_invalid"
	MessageCypherCommandRoutingUseDatabaseNotComposite          MessageID = "cyphercommandrouting.use_database_not_composite"
	MessageCypherCommandRoutingUnknownYieldColumn               MessageID = "cyphercommandrouting.unknown_yield_column"
	MessageCypherCommandRoutingProcedureMinArguments            MessageID = "cyphercommandrouting.procedure_min_arguments"
	MessageCypherCommandRoutingProcedureMaxArguments            MessageID = "cyphercommandrouting.procedure_max_arguments"
)

func cypherCommandRoutingMessage(id MessageID, fallback string, data map[string]any) Message {
	return Message{ID: id, Fallback: fallback, Data: data}
}

func CypherCommandRoutingUnknownCommand(command string) Message {
	return cypherCommandRoutingMessage(MessageCypherCommandRoutingUnknownCommand, "unknown command: "+command, map[string]any{"Command": command})
}

func CypherCommandRoutingEmptyCommand() Message {
	return cypherCommandRoutingMessage(MessageCypherCommandRoutingEmptyCommand, "empty command", nil)
}

func CypherCommandRoutingShellUseDatabaseRequired() Message {
	return cypherCommandRoutingMessage(MessageCypherCommandRoutingShellUseDatabaseRequired, ":use requires a database name", nil)
}

func CypherCommandRoutingParameterMapRequired() Message {
	return cypherCommandRoutingMessage(MessageCypherCommandRoutingParameterMapRequired, "parameter expression must evaluate to a map", nil)
}

func CypherCommandRoutingParameterArgumentRequired() Message {
	return cypherCommandRoutingMessage(MessageCypherCommandRoutingParameterArgumentRequired, ":param requires an argument", nil)
}

func CypherCommandRoutingParameterValueRequired() Message {
	return cypherCommandRoutingMessage(MessageCypherCommandRoutingParameterValueRequired, ":param value cannot be empty", nil)
}

func CypherCommandRoutingParameterUsage() Message {
	return cypherCommandRoutingMessage(MessageCypherCommandRoutingParameterUsage, "incorrect usage: expected :param clear, :param list, :param {a: 1}, or :param key => value", nil)
}

func CypherCommandRoutingParameterMapKeysStrings() Message {
	return cypherCommandRoutingMessage(MessageCypherCommandRoutingParameterMapKeysStrings, "parameter map keys must be strings", nil)
}

func CypherCommandRoutingParameterMapEntryEmpty() Message {
	return cypherCommandRoutingMessage(MessageCypherCommandRoutingParameterMapEntryEmpty, "empty map entry", nil)
}

func CypherCommandRoutingParameterMapEntryInvalid(entry string) Message {
	return cypherCommandRoutingMessage(MessageCypherCommandRoutingParameterMapEntryInvalid, fmt.Sprintf("invalid map entry %q", entry), map[string]any{"Entry": entry})
}

func CypherCommandRoutingParameterEvaluationFailed(parameter string, cause error) Message {
	return cypherCommandRoutingMessage(MessageCypherCommandRoutingParameterEvaluationFailed, "cypher: expression evaluation failed: parameter "+parameter+": "+cause.Error(), map[string]any{"Parameter": parameter, "Cause": cause.Error()})
}

func CypherCommandRoutingParameterProducedNoValue(parameter string) Message {
	return cypherCommandRoutingMessage(MessageCypherCommandRoutingParameterProducedNoValue, "cypher: expression evaluation failed: parameter "+parameter+" produced no value", map[string]any{"Parameter": parameter})
}

func CypherCommandRoutingParameterExpressionUnresolved(parameter, expression string) Message {
	return cypherCommandRoutingMessage(MessageCypherCommandRoutingParameterExpressionUnresolved, fmt.Sprintf("cypher: expression evaluation failed: parameter %s unresolved expression %q", parameter, expression), map[string]any{"Parameter": parameter, "Expression": expression})
}

func CypherCommandRoutingCallTailSeedRequired() Message {
	return cypherCommandRoutingMessage(MessageCypherCommandRoutingCallTailSeedRequired, "CALL tail execution requires seed result", nil)
}

func CypherCommandRoutingCallTailLimitInvalid(token string) Message {
	return cypherCommandRoutingMessage(MessageCypherCommandRoutingCallTailLimitInvalid, "invalid CALL tail LIMIT: "+token, map[string]any{"Token": token})
}

func CypherCommandRoutingCallTailSkipInvalid(token string) Message {
	return cypherCommandRoutingMessage(MessageCypherCommandRoutingCallTailSkipInvalid, "invalid CALL tail SKIP: "+token, map[string]any{"Token": token})
}

func CypherCommandRoutingWhereBooleanRequired(value string) Message {
	return cypherCommandRoutingMessage(MessageCypherCommandRoutingWhereBooleanRequired, "WHERE expression did not evaluate to boolean: "+value, map[string]any{"Value": value})
}

func CypherCommandRoutingUnknownProcedure(procedure string) Message {
	return cypherCommandRoutingMessage(MessageCypherCommandRoutingUnknownProcedure, "unknown procedure: "+procedure+" (try SHOW PROCEDURES for available procedures)", map[string]any{"Procedure": procedure})
}

func CypherCommandRoutingFabricNotPrepared() Message {
	return cypherCommandRoutingMessage(MessageCypherCommandRoutingFabricNotPrepared, "fabric execution was not prepared", nil)
}

func CypherCommandRoutingFabricStorageTypeInvalid(database string) Message {
	return cypherCommandRoutingMessage(MessageCypherCommandRoutingFabricStorageTypeInvalid, "storage engine has unexpected type for '"+database+"'", map[string]any{"Database": database})
}

func CypherCommandRoutingFabricPreparedContextInvalid() Message {
	return cypherCommandRoutingMessage(MessageCypherCommandRoutingFabricPreparedContextInvalid, "invalid prepared fabric execution in context", nil)
}

func CypherCommandRoutingFabricConstituentsFailed(database string, cause error) Message {
	return cypherCommandRoutingMessage(MessageCypherCommandRoutingFabricConstituentsFailed, "failed to get constituents for '"+database+"': "+cause.Error(), map[string]any{"Database": database, "Cause": cause.Error()})
}

func CypherCommandRoutingFabricShardTransactionFailed(database string, cause error) Message {
	return cypherCommandRoutingMessage(MessageCypherCommandRoutingFabricShardTransactionFailed, "failed to open local shard transaction for '"+database+"': "+cause.Error(), map[string]any{"Database": database, "Cause": cause.Error()})
}

func CypherCommandRoutingUseDatabaseRequired() Message {
	return cypherCommandRoutingMessage(MessageCypherCommandRoutingUseDatabaseRequired, "USE clause requires a database name", nil)
}

func CypherCommandRoutingUseInvalid(cause error) Message {
	return cypherCommandRoutingMessage(MessageCypherCommandRoutingUseInvalid, "invalid USE clause: "+cause.Error(), map[string]any{"Cause": cause.Error()})
}

func CypherCommandRoutingUseBacktickUnterminated() Message {
	return cypherCommandRoutingMessage(MessageCypherCommandRoutingUseBacktickUnterminated, "invalid USE clause: unterminated backtick identifier", nil)
}

func CypherCommandRoutingGraphReferenceInvalid() Message {
	return cypherCommandRoutingMessage(MessageCypherCommandRoutingGraphReferenceInvalid, "invalid graph reference", nil)
}

func CypherCommandRoutingGraphReferenceArgumentRequired() Message {
	return cypherCommandRoutingMessage(MessageCypherCommandRoutingGraphReferenceArgumentRequired, "graph reference requires an argument", nil)
}

func CypherCommandRoutingGraphReferenceOpenParenExpected(position int) Message {
	return cypherCommandRoutingMessage(MessageCypherCommandRoutingGraphReferenceOpenParenExpected, fmt.Sprintf("expected '(' at position %d", position), map[string]any{"Position": position})
}

func CypherCommandRoutingGraphReferenceUnterminated() Message {
	return cypherCommandRoutingMessage(MessageCypherCommandRoutingGraphReferenceUnterminated, "unterminated graph reference", nil)
}

func CypherCommandRoutingGraphReferenceArgumentEmpty() Message {
	return cypherCommandRoutingMessage(MessageCypherCommandRoutingGraphReferenceArgumentEmpty, "empty graph reference argument", nil)
}

func CypherCommandRoutingGraphReferenceStringUnterminated() Message {
	return cypherCommandRoutingMessage(MessageCypherCommandRoutingGraphReferenceStringUnterminated, "unterminated graph reference string", nil)
}

func CypherCommandRoutingBacktickIdentifierUnterminated() Message {
	return cypherCommandRoutingMessage(MessageCypherCommandRoutingBacktickIdentifierUnterminated, "unterminated backtick identifier", nil)
}

func CypherCommandRoutingUseConstituentOutsideComposite(target, constituent, composite string) Message {
	fallback := fmt.Sprintf("USE %s failed: constituent '%s' is not part of current composite '%s'", target, constituent, composite)
	return cypherCommandRoutingMessage(MessageCypherCommandRoutingUseConstituentOutsideComposite, fallback, map[string]any{"Target": target, "Constituent": constituent, "Composite": composite})
}

func CypherCommandRoutingUseFailed(target string, cause error) Message {
	return cypherCommandRoutingMessage(MessageCypherCommandRoutingUseFailed, "USE "+target+" failed: "+cause.Error(), map[string]any{"Target": target, "Cause": cause.Error()})
}

func CypherCommandRoutingUseBackendUnsupported(target string) Message {
	return cypherCommandRoutingMessage(MessageCypherCommandRoutingUseBackendUnsupported, "USE "+target+" is not supported by this storage backend", map[string]any{"Target": target})
}

func CypherCommandRoutingUseDatabaseManagerUnavailable(target string) Message {
	return cypherCommandRoutingMessage(MessageCypherCommandRoutingUseDatabaseManagerUnavailable, "USE "+target+" failed: database manager not available", map[string]any{"Target": target})
}

func CypherCommandRoutingUseStorageTypeInvalid(target string) Message {
	return cypherCommandRoutingMessage(MessageCypherCommandRoutingUseStorageTypeInvalid, "USE "+target+" failed: storage engine has unexpected type", map[string]any{"Target": target})
}

func CypherCommandRoutingUseDatabaseNotComposite(target, database string) Message {
	return cypherCommandRoutingMessage(MessageCypherCommandRoutingUseDatabaseNotComposite, "USE "+target+" failed: '"+database+"' is not a composite database", map[string]any{"Target": target, "Database": database})
}

func CypherCommandRoutingUnknownYieldColumn(column string) Message {
	return cypherCommandRoutingMessage(MessageCypherCommandRoutingUnknownYieldColumn, "unknown YIELD column: "+column, map[string]any{"Column": column})
}

func CypherCommandRoutingProcedureMinArguments(procedure string, minimum, actual int) Message {
	fallback := fmt.Sprintf("procedure %s requires at least %d arguments, got %d", procedure, minimum, actual)
	return cypherCommandRoutingMessage(MessageCypherCommandRoutingProcedureMinArguments, fallback, map[string]any{"Procedure": procedure, "Minimum": minimum, "Actual": actual})
}

func CypherCommandRoutingProcedureMaxArguments(procedure string, maximum, actual int) Message {
	fallback := fmt.Sprintf("procedure %s accepts at most %d arguments, got %d", procedure, maximum, actual)
	return cypherCommandRoutingMessage(MessageCypherCommandRoutingProcedureMaxArguments, fallback, map[string]any{"Procedure": procedure, "Maximum": maximum, "Actual": actual})
}
