package localization

import (
	"fmt"
	"strconv"
)

const (
	MessageCypherCoreEmptyQuery                          MessageID = "cyphercore.empty_query"
	MessageCypherCoreCompositeTargetRequired             MessageID = "cyphercore.composite_target_required"
	MessageCypherCoreInvalidLabelName                    MessageID = "cyphercore.invalid_label_name"
	MessageCypherCoreInvalidLabelReserved                MessageID = "cyphercore.invalid_label_reserved"
	MessageCypherCoreInvalidPropertyKey                  MessageID = "cyphercore.invalid_property_key"
	MessageCypherCoreInvalidPropertyValue                MessageID = "cyphercore.invalid_property_value"
	MessageCypherCoreEmbeddingTransactionStorageRequired MessageID = "cyphercore.embedding_transaction_storage_required"
	MessageCypherCoreImplicitTransactionPrimeFailed      MessageID = "cyphercore.implicit_transaction_prime_failed"
	MessageCypherCoreImplicitTransactionStartFailed      MessageID = "cyphercore.implicit_transaction_start_failed"
	MessageCypherCoreImplicitTransactionPinFailed        MessageID = "cyphercore.implicit_transaction_pin_failed"
	MessageCypherCoreImplicitTransactionConfigureFailed  MessageID = "cyphercore.implicit_transaction_configure_failed"
	MessageCypherCoreImplicitTransactionWALBeginFailed   MessageID = "cyphercore.implicit_transaction_wal_begin_failed"
	MessageCypherCoreImplicitTransactionCommitFailed     MessageID = "cyphercore.implicit_transaction_commit_failed"
	MessageCypherCoreEmbeddingConfiguredRequired         MessageID = "cyphercore.embedding_configured_required"
	MessageCypherCoreEmbeddingChunkFailed                MessageID = "cyphercore.embedding_chunk_failed"
	MessageCypherCoreEmbeddingNodeFailed                 MessageID = "cyphercore.embedding_node_failed"
	MessageCypherCoreEmbeddingEmptyVector                MessageID = "cyphercore.embedding_empty_vector"
	MessageCypherCoreOptionalMatchRequired               MessageID = "cyphercore.optional_match_required"
	MessageCypherCoreUnterminatedStringLiteral           MessageID = "cyphercore.unterminated_string_literal"
	MessageCypherCoreParseFailed                         MessageID = "cyphercore.parse_failed"
	MessageCypherCoreCaseEnvelopeInvalid                 MessageID = "cyphercore.case_envelope_invalid"
	MessageCypherCoreCaseWhenRequired                    MessageID = "cyphercore.case_when_required"
	MessageCypherCoreCaseThenRequired                    MessageID = "cyphercore.case_then_required"
	MessageCypherCoreFulltextUnexpectedToken             MessageID = "cyphercore.fulltext_unexpected_token"
	MessageCypherCoreFulltextNumberAfterBoostExpected    MessageID = "cyphercore.fulltext_number_after_boost_expected"
	MessageCypherCoreFulltextBadBoost                    MessageID = "cyphercore.fulltext_bad_boost"
	MessageCypherCoreFulltextClosingParenthesisRequired  MessageID = "cyphercore.fulltext_closing_parenthesis_required"
	MessageCypherCoreFulltextRangeTORequired             MessageID = "cyphercore.fulltext_range_to_required"
	MessageCypherCoreFulltextRangeCloseRequired          MessageID = "cyphercore.fulltext_range_close_required"
	MessageCypherCoreFulltextRangeEndpointRequired       MessageID = "cyphercore.fulltext_range_endpoint_required"
	MessageCypherCoreFulltextBadRegex                    MessageID = "cyphercore.fulltext_bad_regex"
	MessageCypherCoreFulltextBadWildcard                 MessageID = "cyphercore.fulltext_bad_wildcard"
	MessageCypherCoreIndexHintNotFound                   MessageID = "cyphercore.index_hint_not_found"
	MessageCypherCoreExecutionPlanBuildFailed            MessageID = "cyphercore.execution_plan_build_failed"
	MessageCypherCoreTypedDecodeRowFailed                MessageID = "cyphercore.typed_decode_row_failed"
	MessageCypherCoreTypedDestinationPointerRequired     MessageID = "cyphercore.typed_destination_pointer_required"
	MessageCypherCoreTypedDestinationUnsupported         MessageID = "cyphercore.typed_destination_unsupported"
	MessageCypherCoreTypedFieldFailed                    MessageID = "cyphercore.typed_field_failed"
	MessageCypherCoreTypedTimeParseFailed                MessageID = "cyphercore.typed_time_parse_failed"
	MessageCypherCoreTypedAssignmentFailed               MessageID = "cyphercore.typed_assignment_failed"
	MessageCypherCoreEmbedderNotConfigured               MessageID = "cyphercore.embedder_not_configured"
	MessageCypherCoreEmbeddingNoOutput                   MessageID = "cyphercore.embedding_no_output"
	MessageCypherCoreDivisionByZero                      MessageID = "cyphercore.division_by_zero"
	MessageCypherCoreYieldPaginationInvalid              MessageID = "cyphercore.yield_pagination_invalid"
	MessageCypherCoreInvalidInput                        MessageID = "cyphercore.invalid_input"
	MessageCypherCoreInvalidInputExpectedExpression      MessageID = "cyphercore.invalid_input_expected_expression"
	MessageCypherCoreListOperandTypeMismatch             MessageID = "cyphercore.list_operand_type_mismatch"
	MessageCypherCoreListParameterTypeMismatch           MessageID = "cyphercore.list_parameter_type_mismatch"
	MessageCypherCoreListCoercionToBoolean               MessageID = "cyphercore.list_coercion_to_boolean"
	MessageCypherCoreListIndexTypeMismatch               MessageID = "cyphercore.list_index_type_mismatch"
	MessageCypherCoreMapKeyTypeMismatch                  MessageID = "cyphercore.map_key_type_mismatch"
	MessageCypherCoreEntityPropertyKeyTypeMismatch       MessageID = "cyphercore.entity_property_key_type_mismatch"
	MessageCypherCoreRegexPatternTypeMismatch            MessageID = "cyphercore.regex_pattern_type_mismatch"
	MessageCypherCoreDurationArithmeticOverflow          MessageID = "cyphercore.duration_arithmetic_overflow"
	MessageCypherCoreFunctionArgumentCount               MessageID = "cyphercore.function_argument_count"
	MessageCypherCoreTrimCharacterLength                 MessageID = "cyphercore.trim_character_length"
	MessageCypherCoreNormalizeFormInvalid                MessageID = "cyphercore.normalize_form_invalid"
	MessageCypherCoreProcedureOutputShadowsVariable      MessageID = "cyphercore.procedure_output_shadows_variable"
	MessageCypherCoreExpressionUnevaluable               MessageID = "cyphercore.expression_unevaluable"
	MessageCypherCoreStandaloneCallModifiers             MessageID = "cyphercore.standalone_call_modifiers"
	MessageCypherCoreYieldWhereMisplaced                 MessageID = "cyphercore.yield_where_misplaced"
	MessageCypherCoreTemporalDateFormConflict            MessageID = "cyphercore.temporal_date_form_conflict"
	MessageCypherCoreTemporalFieldRequired               MessageID = "cyphercore.temporal_field_required"
	MessageCypherCoreTemporalFieldRequiresField          MessageID = "cyphercore.temporal_field_requires_field"
	MessageCypherCoreTemporalFieldOutOfRange             MessageID = "cyphercore.temporal_field_out_of_range"
	MessageCypherCoreTemporalFieldInvalidValue           MessageID = "cyphercore.temporal_field_invalid_value"
	MessageCypherCoreTemporalDayOfYearNotLeapYear        MessageID = "cyphercore.temporal_day_of_year_not_leap_year"
	MessageCypherCoreTemporalFebruary29NotLeapYear       MessageID = "cyphercore.temporal_february_29_not_leap_year"
	MessageCypherCoreTemporalInvalidDate                 MessageID = "cyphercore.temporal_invalid_date"
	MessageCypherCoreTemporalTextUnparseable             MessageID = "cyphercore.temporal_text_unparseable"
	MessageCypherCoreTemporalMapInvalid                  MessageID = "cyphercore.temporal_map_invalid"
	MessageCypherCoreTemporalCallSignature               MessageID = "cyphercore.temporal_call_signature"
	MessageCypherCoreTemporalProcedureCallSignature      MessageID = "cyphercore.temporal_procedure_call_signature"
	MessageCypherCoreTemporalUnsupportedUnit             MessageID = "cyphercore.temporal_unsupported_unit"
	MessageCypherCoreTemporalNoSuchField                 MessageID = "cyphercore.temporal_no_such_field"
	MessageCypherCoreMapProjectionCoercion               MessageID = "cyphercore.map_projection_coercion"
	MessageCypherCoreMapProjectionReceiver               MessageID = "cyphercore.map_projection_receiver"
	MessageCypherCoreTemporalInvalidNanosecond           MessageID = "cyphercore.temporal_invalid_nanosecond"
	MessageCypherCoreTemporalMissingComponent            MessageID = "cyphercore.temporal_missing_component"
	MessageCypherCorePercentileOutOfRange                MessageID = "cyphercore.percentile_out_of_range"
	MessageCypherCorePointMapKeysInvalid                 MessageID = "cyphercore.point_map_keys_invalid"
	MessageCypherCorePointCoordinatesMissing             MessageID = "cyphercore.point_coordinates_missing"
	MessageCypherCoreAggregateNumberOrDurationOnly       MessageID = "cyphercore.aggregate_number_or_duration_only"
	MessageCypherCoreAggregateNumberOnly                 MessageID = "cyphercore.aggregate_number_only"
	MessageCypherCoreAggregateMixesNumberAndDuration     MessageID = "cyphercore.aggregate_mixes_number_and_duration"
	MessageCypherCoreApocPathMinLevel                    MessageID = "cyphercore.apoc_path_min_level"
	MessageCypherCoreApocPathNodeArgument                MessageID = "cyphercore.apoc_path_node_argument"
	MessageCypherCoreApocPathConfigNotMap                MessageID = "cyphercore.apoc_path_config_not_map"
	MessageCypherCoreApocPathSequenceRelationship        MessageID = "cyphercore.apoc_path_sequence_relationship"
	MessageCypherCoreApocPathLabelFilterEmpty            MessageID = "cyphercore.apoc_path_label_filter_empty"
	MessageCypherCoreApocPathSequenceLabel               MessageID = "cyphercore.apoc_path_sequence_label"
	MessageCypherCoreApocPathNodeNotFound                MessageID = "cyphercore.apoc_path_node_not_found"
	MessageCypherCoreApocPathConfigNumber                MessageID = "cyphercore.apoc_path_config_number"
	MessageCypherCoreApocPathConfigString                MessageID = "cyphercore.apoc_path_config_string"
	MessageCypherCoreApocPathLimit                       MessageID = "cyphercore.apoc_path_limit"
	MessageCypherCoreFunctionArgumentInvalid             MessageID = "cyphercore.function_argument_invalid"
)

func cypherCoreMessage(id MessageID, fallback string, data map[string]any) Message {
	return Message{ID: id, Fallback: fallback, Data: data}
}

func cypherCoreCauseMessage(id MessageID, prefix string, cause error, data map[string]any) Message {
	if data == nil {
		data = make(map[string]any, 1)
	}
	data["Cause"] = cause.Error()
	return cypherCoreMessage(id, prefix+cause.Error(), data)
}

func CypherCoreEmptyQuery() Message {
	return cypherCoreMessage(MessageCypherCoreEmptyQuery, "empty query", nil)
}

func CypherCoreCompositeTargetRequired() Message {
	const code = "Neo.ClientError.Statement.NotAllowed"
	return cypherCoreMessage(MessageCypherCoreCompositeTargetRequired, code+": Queries on composite databases require explicit graph targeting. Use USE <composite>.<alias> to target a specific constituent", map[string]any{"Code": code})
}

func CypherCoreInvalidLabelName(label string) Message {
	quoted := strconv.Quote(label)
	return cypherCoreMessage(MessageCypherCoreInvalidLabelName, "invalid label name: "+quoted+" (must be alphanumeric starting with letter or underscore)", map[string]any{"Label": quoted})
}

func CypherCoreInvalidLabelReserved(label string) Message {
	quoted := strconv.Quote(label)
	return cypherCoreMessage(MessageCypherCoreInvalidLabelReserved, "invalid label name: "+quoted+" (contains reserved keyword)", map[string]any{"Label": quoted})
}

func CypherCoreInvalidPropertyKey(key string) Message {
	quoted := strconv.Quote(key)
	return cypherCoreMessage(MessageCypherCoreInvalidPropertyKey, "invalid property key: "+quoted+" (must be alphanumeric starting with letter or underscore)", map[string]any{"Key": quoted})
}

func CypherCoreInvalidPropertyValue(key string) Message {
	quoted := strconv.Quote(key)
	return cypherCoreMessage(MessageCypherCoreInvalidPropertyValue, "invalid property value for key "+quoted+": malformed syntax", map[string]any{"Key": quoted})
}

func CypherCoreEmbeddingTransactionStorageRequired() Message {
	return cypherCoreMessage(MessageCypherCoreEmbeddingTransactionStorageRequired, "WITH EMBEDDING requires transaction-capable storage", nil)
}

func CypherCoreImplicitTransactionPrimeFailed(cause error) Message {
	return cypherCoreCauseMessage(MessageCypherCoreImplicitTransactionPrimeFailed, "failed to prime implicit transaction namespace: ", cause, nil)
}

func CypherCoreImplicitTransactionStartFailed(cause error) Message {
	return cypherCoreCauseMessage(MessageCypherCoreImplicitTransactionStartFailed, "failed to start implicit transaction: ", cause, nil)
}

func CypherCoreImplicitTransactionPinFailed(cause error) Message {
	return cypherCoreCauseMessage(MessageCypherCoreImplicitTransactionPinFailed, "failed to pin implicit transaction namespace: ", cause, nil)
}

func CypherCoreImplicitTransactionConfigureFailed(cause error) Message {
	return cypherCoreCauseMessage(MessageCypherCoreImplicitTransactionConfigureFailed, "failed to configure implicit transaction: ", cause, nil)
}

func CypherCoreImplicitTransactionWALBeginFailed(cause error) Message {
	return cypherCoreCauseMessage(MessageCypherCoreImplicitTransactionWALBeginFailed, "failed to write WAL tx begin: ", cause, nil)
}

func CypherCoreImplicitTransactionCommitFailed(cause error) Message {
	return cypherCoreCauseMessage(MessageCypherCoreImplicitTransactionCommitFailed, "commit failed: ", cause, nil)
}

func CypherCoreEmbeddingConfiguredRequired() Message {
	return cypherCoreMessage(MessageCypherCoreEmbeddingConfiguredRequired, "WITH EMBEDDING requires configured embedder", nil)
}

func CypherCoreEmbeddingChunkFailed(nodeID string, cause error) Message {
	return cypherCoreCauseMessage(MessageCypherCoreEmbeddingChunkFailed, "WITH EMBEDDING chunking failed for node "+nodeID+": ", cause, map[string]any{"NodeID": nodeID})
}

func CypherCoreEmbeddingNodeFailed(nodeID string, cause error) Message {
	return cypherCoreCauseMessage(MessageCypherCoreEmbeddingNodeFailed, "WITH EMBEDDING embed failed for node "+nodeID+": ", cause, map[string]any{"NodeID": nodeID})
}

func CypherCoreEmbeddingEmptyVector(nodeID string) Message {
	return cypherCoreMessage(MessageCypherCoreEmbeddingEmptyVector, "WITH EMBEDDING embed returned empty vector for node "+nodeID, map[string]any{"NodeID": nodeID})
}

func CypherCoreOptionalMatchRequired() Message {
	return cypherCoreMessage(MessageCypherCoreOptionalMatchRequired, "OPTIONAL must be followed by MATCH", nil)
}

func CypherCoreUnterminatedStringLiteral() Message {
	return cypherCoreMessage(MessageCypherCoreUnterminatedStringLiteral, "unterminated string literal", nil)
}

func CypherCoreParseFailed(cause error) Message {
	return cypherCoreCauseMessage(MessageCypherCoreParseFailed, "parse error: ", cause, nil)
}

func CypherCoreCaseEnvelopeInvalid() Message {
	return cypherCoreMessage(MessageCypherCoreCaseEnvelopeInvalid, "invalid CASE expression: must start with CASE and end with END", nil)
}

func CypherCoreCaseWhenRequired() Message {
	return cypherCoreMessage(MessageCypherCoreCaseWhenRequired, "CASE expression must have at least one WHEN clause", nil)
}

func CypherCoreCaseThenRequired(section string) Message {
	return cypherCoreMessage(MessageCypherCoreCaseThenRequired, "WHEN clause must have THEN: "+section, map[string]any{"Section": section})
}

func CypherCoreFulltextUnexpectedToken(token string) Message {
	quoted := strconv.Quote(token)
	return cypherCoreMessage(MessageCypherCoreFulltextUnexpectedToken, "query cannot be parsed: unexpected token "+quoted, map[string]any{"Token": quoted})
}

func CypherCoreFulltextNumberAfterBoostExpected() Message {
	return cypherCoreMessage(MessageCypherCoreFulltextNumberAfterBoostExpected, "query cannot be parsed: expected number after ^", nil)
}

func CypherCoreFulltextBadBoost(boost string, cause error) Message {
	quoted := strconv.Quote(boost)
	return cypherCoreMessage(MessageCypherCoreFulltextBadBoost, "query cannot be parsed: bad boost "+quoted, map[string]any{"Boost": quoted, "Cause": cause.Error()})
}

func CypherCoreFulltextClosingParenthesisRequired() Message {
	return cypherCoreMessage(MessageCypherCoreFulltextClosingParenthesisRequired, "query cannot be parsed: missing ')'", nil)
}

func CypherCoreFulltextRangeTORequired() Message {
	return cypherCoreMessage(MessageCypherCoreFulltextRangeTORequired, "query cannot be parsed: expected TO in range", nil)
}

func CypherCoreFulltextRangeCloseRequired() Message {
	return cypherCoreMessage(MessageCypherCoreFulltextRangeCloseRequired, "query cannot be parsed: expected ] or } to close range", nil)
}

func CypherCoreFulltextRangeEndpointRequired() Message {
	return cypherCoreMessage(MessageCypherCoreFulltextRangeEndpointRequired, "query cannot be parsed: expected range endpoint", nil)
}

func CypherCoreFulltextBadRegex(pattern string, cause error) Message {
	return cypherCoreMessage(MessageCypherCoreFulltextBadRegex, "query cannot be parsed: bad regex /"+pattern+"/: "+cause.Error(), map[string]any{"Pattern": pattern, "Cause": cause.Error()})
}

func CypherCoreFulltextBadWildcard(cause error) Message {
	return cypherCoreCauseMessage(MessageCypherCoreFulltextBadWildcard, "query cannot be parsed: bad wildcard: ", cause, nil)
}

func CypherCoreIndexHintNotFound(hint, label, property string) Message {
	fallback := "no index found for hint: " + hint + " (index on :" + label + "(" + property + ") does not exist)"
	return cypherCoreMessage(MessageCypherCoreIndexHintNotFound, fallback, map[string]any{"Hint": hint, "Label": label, "Property": property})
}

func CypherCoreExecutionPlanBuildFailed(cause error) Message {
	return cypherCoreCauseMessage(MessageCypherCoreExecutionPlanBuildFailed, "failed to build execution plan: ", cause, nil)
}

func CypherCoreTypedDecodeRowFailed(cause error) Message {
	return cypherCoreCauseMessage(MessageCypherCoreTypedDecodeRowFailed, "failed to decode row: ", cause, nil)
}

func CypherCoreTypedDestinationPointerRequired() Message {
	return cypherCoreMessage(MessageCypherCoreTypedDestinationPointerRequired, "dest must be a non-nil pointer", nil)
}

func CypherCoreTypedDestinationUnsupported(kind string) Message {
	return cypherCoreMessage(MessageCypherCoreTypedDestinationUnsupported, "unsupported destination type: "+kind, map[string]any{"Kind": kind})
}

func CypherCoreTypedFieldFailed(field string, cause error) Message {
	return cypherCoreCauseMessage(MessageCypherCoreTypedFieldFailed, "field "+field+": ", cause, map[string]any{"Field": field})
}

func CypherCoreTypedTimeParseFailed(value string, cause error) Message {
	return cypherCoreMessage(MessageCypherCoreTypedTimeParseFailed, "cannot parse time: "+value, map[string]any{"Value": value, "Cause": cause.Error()})
}

func CypherCoreTypedAssignmentFailed(value any, destinationType string) Message {
	valueType := fmt.Sprintf("%T", value)
	return cypherCoreMessage(MessageCypherCoreTypedAssignmentFailed, "cannot assign "+valueType+" to "+destinationType, map[string]any{"ValueType": valueType, "DestinationType": destinationType})
}

func CypherCoreEmbedderNotConfigured() Message {
	return cypherCoreMessage(MessageCypherCoreEmbedderNotConfigured, "no embedder configured", nil)
}

func CypherCoreEmbeddingNoOutput() Message {
	return cypherCoreMessage(MessageCypherCoreEmbeddingNoOutput, "failed to embed query (no embeddings produced)", nil)
}

func CypherCoreDivisionByZero() Message {
	return cypherCoreMessage(MessageCypherCoreDivisionByZero, "/ by zero", nil)
}

func CypherCoreYieldPaginationInvalid() Message {
	return cypherCoreMessage(MessageCypherCoreYieldPaginationInvalid, "SKIP and LIMIT require a non-negative INTEGER", nil)
}

func CypherCoreInvalidInput(token string) Message {
	return cypherCoreMessage(MessageCypherCoreInvalidInput, "Invalid input '"+token+"'", map[string]any{"Token": token})
}

func CypherCoreInvalidInputExpectedExpression(token string) Message {
	return cypherCoreMessage(MessageCypherCoreInvalidInputExpectedExpression, "Invalid input '"+token+"': expected an expression", map[string]any{"Token": token})
}

func CypherCoreListOperandTypeMismatch(typeName string) Message {
	return cypherCoreMessage(MessageCypherCoreListOperandTypeMismatch, "Type mismatch: expected List<T> but was "+typeName, map[string]any{"Type": typeName})
}

func CypherCoreListParameterTypeMismatch(parameter string, typeName string) Message {
	return cypherCoreMessage(MessageCypherCoreListParameterTypeMismatch, "Type mismatch for parameter '"+parameter+"': expected List<T> but was "+typeName, map[string]any{"Parameter": parameter, "Type": typeName})
}

func CypherCoreListCoercionToBoolean() Message {
	return cypherCoreMessage(MessageCypherCoreListCoercionToBoolean, "Coercion of list to boolean is not allowed. Please use `NOT isEmpty(...)` instead.", nil)
}

func CypherCoreListIndexTypeMismatch(typeName string) Message {
	return cypherCoreMessage(MessageCypherCoreListIndexTypeMismatch, "Type mismatch: list index must be given as Integer, but was "+typeName, map[string]any{"Type": typeName})
}

// CypherCoreRegexPatternTypeMismatch is the runtime TypeError of
// text =~ pattern for a string text and a pattern that isn't a string;
// value is the pattern rendered as Neo4j renders values (Long(1)).
func CypherCoreRegexPatternTypeMismatch(value string) Message {
	return cypherCoreMessage(MessageCypherCoreRegexPatternTypeMismatch, "Type mismatch: a regular expression pattern must be a String, but was "+value, map[string]any{"Value": value})
}

// CypherCoreDurationArithmeticOverflow is the ArgumentError of a duration
// multiplied or divided (operator) by a number (operand, rendered as Neo4j
// renders values) whose result doesn't fit a duration.
func CypherCoreDurationArithmeticOverflow(duration, operator, operand string) Message {
	return cypherCoreMessage(MessageCypherCoreDurationArithmeticOverflow, "Duration arithmetic overflows: "+duration+" "+operator+" "+operand,
		map[string]any{"Duration": duration, "Operator": operator, "Operand": operand})
}

func CypherCoreMapKeyTypeMismatch(typeName string) Message {
	return cypherCoreMessage(MessageCypherCoreMapKeyTypeMismatch, "Type mismatch: map key must be given as String, but was "+typeName, map[string]any{"Type": typeName})
}

func CypherCoreEntityPropertyKeyTypeMismatch(typeName string) Message {
	return cypherCoreMessage(MessageCypherCoreEntityPropertyKeyTypeMismatch, "Type mismatch: node or relationship property key must be given as String, but was "+typeName, map[string]any{"Type": typeName})
}

func CypherCoreFunctionArgumentCount(function string, want string, got int) Message {
	return cypherCoreMessage(MessageCypherCoreFunctionArgumentCount, function+"() expects "+want+" argument(s), got "+strconv.Itoa(got), map[string]any{"Function": function, "Expected": want, "Got": got})
}

func CypherCoreTrimCharacterLength() Message {
	return cypherCoreMessage(MessageCypherCoreTrimCharacterLength, "The argument `trimCharacterString` in the `trim()` function must be of length 1.", nil)
}

func CypherCoreNormalizeFormInvalid(form string) Message {
	return cypherCoreMessage(MessageCypherCoreNormalizeFormInvalid, "normalize() normal form must be one of NFC, NFD, NFKC or NFKD, got: "+form, map[string]any{"Form": form})
}

func CypherCoreProcedureOutputShadowsVariable(variable string) Message {
	return cypherCoreMessage(MessageCypherCoreProcedureOutputShadowsVariable, "procedure output "+variable+" shadows an existing variable", map[string]any{"Variable": variable})
}

func CypherCoreExpressionUnevaluable(expression string) Message {
	return cypherCoreMessage(MessageCypherCoreExpressionUnevaluable, "could not evaluate expression: "+expression, map[string]any{"Expression": expression})
}

func CypherCoreStandaloneCallModifiers() Message {
	return cypherCoreMessage(MessageCypherCoreStandaloneCallModifiers, "Cannot use standalone call with WHERE (instead use: `CALL ... WITH * WHERE ... RETURN *`)", nil)
}

func CypherCoreYieldWhereMisplaced() Message {
	return cypherCoreMessage(MessageCypherCoreYieldWhereMisplaced, "Invalid input 'WHERE': a YIELD's WHERE must come before its ORDER BY, SKIP and LIMIT", nil)
}

func CypherCoreTemporalDateFormConflict(field string, form string) Message {
	return cypherCoreMessage(MessageCypherCoreTemporalDateFormConflict, "Cannot assign "+field+" to "+form+" date.", map[string]any{"Field": field, "Form": form})
}

func CypherCoreTemporalFieldRequired(field string) Message {
	return cypherCoreMessage(MessageCypherCoreTemporalFieldRequired, field+" must be specified", map[string]any{"Field": field})
}

func CypherCoreTemporalFieldRequiresField(field string, required string) Message {
	return cypherCoreMessage(MessageCypherCoreTemporalFieldRequiresField, field+" cannot be specified without "+required, map[string]any{"Field": field, "Required": required})
}

func CypherCoreTemporalFieldOutOfRange(field string, valid string, value int64) Message {
	return cypherCoreMessage(MessageCypherCoreTemporalFieldOutOfRange, "Invalid value for "+field+" (valid values "+valid+"): "+strconv.FormatInt(value, 10), map[string]any{"Field": field, "Valid": valid, "Value": value})
}

func CypherCoreTemporalFieldInvalidValue(field string, value int64) Message {
	return cypherCoreMessage(MessageCypherCoreTemporalFieldInvalidValue, "Invalid value for "+field+": "+strconv.FormatInt(value, 10), map[string]any{"Field": field, "Value": value})
}

func CypherCoreTemporalDayOfYearNotLeapYear(year int64) Message {
	return cypherCoreMessage(MessageCypherCoreTemporalDayOfYearNotLeapYear, "Invalid date 'DayOfYear 366' as '"+strconv.FormatInt(year, 10)+"' is not a leap year", map[string]any{"Year": year})
}

func CypherCoreTemporalFebruary29NotLeapYear(year int64) Message {
	return cypherCoreMessage(MessageCypherCoreTemporalFebruary29NotLeapYear, "Invalid date 'February 29' as '"+strconv.FormatInt(year, 10)+"' is not a leap year", map[string]any{"Year": year})
}

func CypherCoreTemporalInvalidDate(month string, day int64) Message {
	return cypherCoreMessage(MessageCypherCoreTemporalInvalidDate, "Invalid date '"+month+" "+strconv.FormatInt(day, 10)+"'", map[string]any{"Month": month, "Day": day})
}

func CypherCoreTemporalTextUnparseable(typeName string, quoted string) Message {
	return cypherCoreMessage(MessageCypherCoreTemporalTextUnparseable, "Text cannot be parsed to a "+typeName+"\n"+quoted+"\n ^", map[string]any{"Type": typeName, "Text": quoted})
}

func CypherCoreTemporalMapInvalid(typeName string, value string) Message {
	return cypherCoreMessage(MessageCypherCoreTemporalMapInvalid, "invalid "+typeName+" value: "+value, map[string]any{"Type": typeName, "Value": value})
}

// CypherCoreFunctionArgumentInvalid is Neo4j's TypeError for a function
// argument of the wrong type found while the statement runs (#893). Value is
// Neo4j's rendering of the argument: Long(1), Double(2.500000e+00), ….
func CypherCoreFunctionArgumentInvalid(function, expected, value string) Message {
	return cypherCoreMessage(MessageCypherCoreFunctionArgumentInvalid, "Invalid input for function '"+function+"()': Expected "+expected+", got: "+value, map[string]any{"Function": function, "Expected": expected, "Value": value})
}

func CypherCoreTemporalCallSignature(typeName string, provided string) Message {
	return cypherCoreMessage(MessageCypherCoreTemporalCallSignature, "Invalid call signature for "+typeName+"Function: Provided input was ["+provided+"]", map[string]any{"Type": typeName, "Provided": provided})
}

// CypherCoreTemporalProcedureCallSignature is a temporal function (truncate,
// duration.between, datetime.fromepoch, …) called with a value of a type it
// doesn't take.
func CypherCoreTemporalProcedureCallSignature(procedure string, provided string) Message {
	return cypherCoreMessage(MessageCypherCoreTemporalProcedureCallSignature, "Invalid call signature for "+procedure+": Provided input was ["+provided+"]", map[string]any{"Procedure": procedure, "Provided": provided})
}

// CypherCoreTemporalUnsupportedUnit is a truncation unit the function doesn't
// know.
func CypherCoreTemporalUnsupportedUnit(unit string) Message {
	return cypherCoreMessage(MessageCypherCoreTemporalUnsupportedUnit, "Unsupported unit: "+unit, map[string]any{"Unit": unit})
}

// CypherCoreTemporalNoSuchField is a field name no temporal value has.
func CypherCoreTemporalNoSuchField(field string) Message {
	return cypherCoreMessage(MessageCypherCoreTemporalNoSuchField, "No such field: "+field, map[string]any{"Field": field})
}

// CypherCoreTemporalInvalidNanosecond is a nanosecond-of-second outside
// 0..999999999.
func CypherCoreTemporalInvalidNanosecond(value int64) Message {
	return cypherCoreMessage(MessageCypherCoreTemporalInvalidNanosecond, "Invalid nanosecond: "+strconv.FormatInt(value, 10), map[string]any{"Value": value})
}

// CypherCoreTemporalMissingComponent is a temporal value without the date or
// time a function needs ("Cannot get the date of: 03:04:05Z").
func CypherCoreTemporalMissingComponent(component string, value string) Message {
	return cypherCoreMessage(MessageCypherCoreTemporalMissingComponent, "Cannot get the "+component+" of: "+value, map[string]any{"Component": component, "Value": value})
}

// CypherCorePercentileOutOfRange is a percentile outside 0.0..1.0.
func CypherCorePercentileOutOfRange(value string) Message {
	return cypherCoreMessage(MessageCypherCorePercentileOutOfRange, "Invalid input '"+value+"' is not a valid argument, must be a number in the range 0.0 to 1.0", map[string]any{"Value": value})
}

// CypherCorePointMapKeysInvalid is a map literal whose keys don't describe a
// point; keys lists them quoted ("'a', 'b'").
func CypherCorePointMapKeysInvalid(keys string) Message {
	return cypherCoreMessage(MessageCypherCorePointMapKeysInvalid, "A map with keys "+keys+" is not describing a valid point, a point is described either by using cartesian coordinates e.g. {x: 2.3, y: 4.5, crs: 'cartesian'} or using geographic coordinates e.g. {latitude: 12.78, longitude: 56.7, crs: 'WGS-84'}.", map[string]any{"Keys": keys})
}

// CypherCorePointCoordinatesMissing is a map value with neither x and y nor
// latitude and longitude.
func CypherCorePointCoordinatesMissing() Message {
	return cypherCoreMessage(MessageCypherCorePointCoordinatesMissing, "A point must contain either 'x' and 'y' or 'latitude' and 'longitude'", nil)
}
// CypherCoreAggregateNumberOrDurationOnly is sum() or avg() over a value that
// is neither a number nor a duration.
func CypherCoreAggregateNumberOrDurationOnly(function string) Message {
	return cypherCoreMessage(MessageCypherCoreAggregateNumberOrDurationOnly, function+"() can only handle numerical values, duration, or null.", map[string]any{"Function": function})
}

// CypherCoreAggregateNumberOnly is stDev() or stDevP() over a value that
// isn't a number.
func CypherCoreAggregateNumberOnly(function string, received string) Message {
	return cypherCoreMessage(MessageCypherCoreAggregateNumberOnly, function+"() can only handle numerical values or null, but received "+received, map[string]any{"Function": function, "Received": received})
}

// CypherCoreAggregateMixesNumberAndDuration is sum() or avg() over both
// numbers and durations.
func CypherCoreAggregateMixesNumberAndDuration(function string) Message {
	return cypherCoreMessage(MessageCypherCoreAggregateMixesNumberAndDuration, function+"() cannot mix number and duration", map[string]any{"Function": function})
}

// CypherCoreApocPathMinLevel is an apoc.path procedure that reaches every
// node once (subgraphNodes, subgraphAll, spanningTree) given a minLevel other
// than 0 or 1.
func CypherCoreApocPathMinLevel(procedure string) Message {
	return cypherCoreMessage(MessageCypherCoreApocPathMinLevel, "minLevel can only be 0 or 1 in "+procedure+"()", map[string]any{"Procedure": procedure})
}

// CypherCoreApocPathNodeArgument is an apoc.path start node, or a node list
// in its config (endNodes, terminatorNodes, ...), that isn't a node, an
// element id or a list of them.
func CypherCoreApocPathNodeArgument(argument string, received string) Message {
	return cypherCoreMessage(MessageCypherCoreApocPathNodeArgument, argument+" must be a node, an element id or a list of them, but was "+received, map[string]any{"Argument": argument, "Received": received})
}

// CypherCoreApocPathConfigNotMap is an apoc.path config that isn't a map.
func CypherCoreApocPathConfigNotMap(received string) Message {
	return cypherCoreMessage(MessageCypherCoreApocPathConfigNotMap, "the apoc.path config must be a map, but was "+received, map[string]any{"Received": received})
}

// CypherCoreApocPathSequenceRelationship is an apoc.path sequence with no
// relationship filter for a step: one with no relationship filter, or,
// with beginSequenceAtStart false, only one (the first applies to the
// first step only).
func CypherCoreApocPathSequenceRelationship(step int) Message {
	return cypherCoreMessage(MessageCypherCoreApocPathSequenceRelationship, "the sequence has no relationship filter for step "+strconv.Itoa(step), map[string]any{"Step": step})
}

// CypherCoreApocPathLabelFilterEmpty is an apoc.path label filter entry that
// is an operator (+, -, /, >) without a label.
func CypherCoreApocPathLabelFilterEmpty(entry string) Message {
	return cypherCoreMessage(MessageCypherCoreApocPathLabelFilterEmpty, "the label filter entry "+entry+" has no label", map[string]any{"Entry": entry})
}

// CypherCoreApocPathSequenceLabel is an apoc.path sequence with no label
// filter for a node it reaches: with beginSequenceAtStart false, one of
// relationship filters only.
func CypherCoreApocPathSequenceLabel(depth int) Message {
	return cypherCoreMessage(MessageCypherCoreApocPathSequenceLabel, "the sequence has no label filter for depth "+strconv.Itoa(depth), map[string]any{"Depth": depth})
}

// CypherCoreApocPathNodeNotFound is an apoc.path node argument given as an
// element id that no node has.
func CypherCoreApocPathNodeNotFound(argument string, id string) Message {
	return cypherCoreMessage(MessageCypherCoreApocPathNodeNotFound, argument+": no node has the element id "+id, map[string]any{"Argument": argument, "ID": id})
}

// CypherCoreApocPathConfigNumber is an apoc.path minLevel, maxLevel or limit
// that is neither a number nor a string holding one.
func CypherCoreApocPathConfigNumber(key string, received string) Message {
	return cypherCoreMessage(MessageCypherCoreApocPathConfigNumber, key+" must be a number, but was "+received, map[string]any{"Key": key, "Received": received})
}

// CypherCoreApocPathConfigString is an apoc.path filter or uniqueness
// setting that isn't a string.
func CypherCoreApocPathConfigString(key string, received string) Message {
	return cypherCoreMessage(MessageCypherCoreApocPathConfigString, key+" must be a string, but was "+received, map[string]any{"Key": key, "Received": received})
}

// CypherCoreApocPathLimit is an apoc.path limit below -1 (no limit).
func CypherCoreApocPathLimit(limit int) Message {
	return cypherCoreMessage(MessageCypherCoreApocPathLimit, "limit must be -1 (no limit) or at least 0, but was "+strconv.Itoa(limit), map[string]any{"Limit": limit})
}

// CypherCoreMapProjectionCoercion is a map projection with .* of a value
// that has fields but no properties, a temporal value or duration
// (d{.*}).
func CypherCoreMapProjectionCoercion(value string) Message {
	return cypherCoreMessage(MessageCypherCoreMapProjectionCoercion, "Can't coerce `"+value+"` to Map", map[string]any{"Value": value})
}

// CypherCoreMapProjectionReceiver is a map projection whose receiver isn't a
// variable (3{.a}, n.prop{.a}, {a: 1}{.a}).
func CypherCoreMapProjectionReceiver(receiver string) Message {
	return cypherCoreMessage(MessageCypherCoreMapProjectionReceiver, "Invalid input '{': only a variable can be projected, not "+receiver, map[string]any{"Receiver": receiver})
}
