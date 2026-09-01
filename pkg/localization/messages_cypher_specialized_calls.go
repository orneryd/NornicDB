package localization

import (
	"fmt"
	"strconv"
)

const (
	MessageCypherSpecializedCallsVectorQueryParseFailed          MessageID = "cypherspecializedcalls.vector_query_parse_failed"
	MessageCypherSpecializedCallsStringQueryEmbedderRequired     MessageID = "cypherspecializedcalls.string_query_embedder_required"
	MessageCypherSpecializedCallsEmbedQueryTextFailed            MessageID = "cypherspecializedcalls.embed_query_text_failed"
	MessageCypherSpecializedCallsParameterNotProvided            MessageID = "cypherspecializedcalls.parameter_not_provided"
	MessageCypherSpecializedCallsParameterNonNumeric             MessageID = "cypherspecializedcalls.parameter_non_numeric"
	MessageCypherSpecializedCallsParameterStringEmbedderRequired MessageID = "cypherspecializedcalls.parameter_string_embedder_required"
	MessageCypherSpecializedCallsEmbedParameterFailed            MessageID = "cypherspecializedcalls.embed_parameter_failed"
	MessageCypherSpecializedCallsParameterUnsupportedType        MessageID = "cypherspecializedcalls.parameter_unsupported_type"
	MessageCypherSpecializedCallsUnsupportedParameters           MessageID = "cypherspecializedcalls.unsupported_parameters"
	MessageCypherSpecializedCallsQueryInputPossiblyUnsupported   MessageID = "cypherspecializedcalls.query_input_possibly_unsupported"
	MessageCypherSpecializedCallsQueryInputRequired              MessageID = "cypherspecializedcalls.query_input_required"
	MessageCypherSpecializedCallsVectorQueryFailed               MessageID = "cypherspecializedcalls.vector_query_failed"
	MessageCypherSpecializedCallsEmbedderNotConfigured           MessageID = "cypherspecializedcalls.embedder_not_configured"
	MessageCypherSpecializedCallsVectorEmbedInvalidSyntax        MessageID = "cypherspecializedcalls.vector_embed_invalid_syntax"
	MessageCypherSpecializedCallsVectorEmbedArgumentRequired     MessageID = "cypherspecializedcalls.vector_embed_argument_required"
	MessageCypherSpecializedCallsVectorEmbedUnmatchedParenthesis MessageID = "cypherspecializedcalls.vector_embed_unmatched_parenthesis"
	MessageCypherSpecializedCallsVectorEmbedTextRequired         MessageID = "cypherspecializedcalls.vector_embed_text_required"
	MessageCypherSpecializedCallsVectorEmbedParameterString      MessageID = "cypherspecializedcalls.vector_embed_parameter_string"
	MessageCypherSpecializedCallsVectorEmbedStringRequired       MessageID = "cypherspecializedcalls.vector_embed_string_required"
	MessageCypherSpecializedCallsVectorEmbedFailed               MessageID = "cypherspecializedcalls.vector_embed_failed"
	MessageCypherSpecializedCallsVectorProcedureNotFound         MessageID = "cypherspecializedcalls.vector_procedure_not_found"
	MessageCypherSpecializedCallsVectorParametersMissing         MessageID = "cypherspecializedcalls.vector_parameters_missing"
	MessageCypherSpecializedCallsVectorUnmatchedParenthesis      MessageID = "cypherspecializedcalls.vector_unmatched_parenthesis"
	MessageCypherSpecializedCallsFulltextIndexNotFound           MessageID = "cypherspecializedcalls.fulltext_index_not_found"
	MessageCypherSpecializedCallsFulltextArgumentCount           MessageID = "cypherspecializedcalls.fulltext_argument_count"
	MessageCypherSpecializedCallsProcedureOptionsMapRequired     MessageID = "cypherspecializedcalls.procedure_options_map_required"
	MessageCypherSpecializedCallsFulltextOptionInvalid           MessageID = "cypherspecializedcalls.fulltext_option_invalid"
	MessageCypherSpecializedCallsTemporalAssertArgumentCount     MessageID = "cypherspecializedcalls.temporal_assert_argument_count"
	MessageCypherSpecializedCallsDateTimeRequired                MessageID = "cypherspecializedcalls.datetime_required"
	MessageCypherSpecializedCallsTemporalReadNodesFailed         MessageID = "cypherspecializedcalls.temporal_read_nodes_failed"
	MessageCypherSpecializedCallsTemporalOverlap                 MessageID = "cypherspecializedcalls.temporal_overlap"
	MessageCypherSpecializedCallsTemporalAsOfArgumentCount       MessageID = "cypherspecializedcalls.temporal_asof_argument_count"
	MessageCypherSpecializedCallsTemporalLookupFailed            MessageID = "cypherspecializedcalls.temporal_lookup_failed"
	MessageCypherSpecializedCallsTemporalInvalidSyntax           MessageID = "cypherspecializedcalls.temporal_invalid_syntax"
	MessageCypherSpecializedCallsTemporalClosingParenthesis      MessageID = "cypherspecializedcalls.temporal_closing_parenthesis"
	MessageCypherSpecializedCallsArgumentRequired                MessageID = "cypherspecializedcalls.argument_required"
	MessageCypherSpecializedCallsArgumentEmpty                   MessageID = "cypherspecializedcalls.argument_empty"
	MessageCypherSpecializedCallsUnsignedNonNegative             MessageID = "cypherspecializedcalls.unsigned_non_negative"
	MessageCypherSpecializedCallsUnsignedWholeNonNegative        MessageID = "cypherspecializedcalls.unsigned_whole_non_negative"
	MessageCypherSpecializedCallsUnsignedValid                   MessageID = "cypherspecializedcalls.unsigned_valid"
	MessageCypherSpecializedCallsTxlogInvalidSyntax              MessageID = "cypherspecializedcalls.txlog_invalid_syntax"
	MessageCypherSpecializedCallsTxlogClosingParenthesis         MessageID = "cypherspecializedcalls.txlog_closing_parenthesis"
	MessageCypherSpecializedCallsTxlogEntriesArgumentRequired    MessageID = "cypherspecializedcalls.txlog_entries_argument_required"
	MessageCypherSpecializedCallsTxlogInvalidSequence            MessageID = "cypherspecializedcalls.txlog_invalid_sequence"
	MessageCypherSpecializedCallsTxlogFromSequencePositive       MessageID = "cypherspecializedcalls.txlog_from_sequence_positive"
	MessageCypherSpecializedCallsWALUnavailable                  MessageID = "cypherspecializedcalls.wal_unavailable"
	MessageCypherSpecializedCallsWALConfigUnavailable            MessageID = "cypherspecializedcalls.wal_config_unavailable"
	MessageCypherSpecializedCallsWALDirectoryNotConfigured       MessageID = "cypherspecializedcalls.wal_directory_not_configured"
	MessageCypherSpecializedCallsTxlogSequenceOrder              MessageID = "cypherspecializedcalls.txlog_sequence_order"
	MessageCypherSpecializedCallsTxlogReadEntriesFailed          MessageID = "cypherspecializedcalls.txlog_read_entries_failed"
	MessageCypherSpecializedCallsTxlogByIDArgumentRequired       MessageID = "cypherspecializedcalls.txlog_by_id_argument_required"
	MessageCypherSpecializedCallsTxlogIDEmpty                    MessageID = "cypherspecializedcalls.txlog_id_empty"
	MessageCypherSpecializedCallsTxlogFindEntriesFailed          MessageID = "cypherspecializedcalls.txlog_find_entries_failed"
)

func cypherSpecializedCallsMessage(id MessageID, fallback string, data map[string]any) Message {
	return Message{ID: id, Fallback: fallback, Data: data}
}

func CypherSpecializedCallsVectorQueryParseFailed(cause error) Message {
	return cypherSpecializedCallsMessage(MessageCypherSpecializedCallsVectorQueryParseFailed, "vector query parse error: "+cause.Error(), map[string]any{"Cause": cause.Error()})
}

func CypherSpecializedCallsStringQueryEmbedderRequired() Message {
	return cypherSpecializedCallsMessage(MessageCypherSpecializedCallsStringQueryEmbedderRequired, "string query provided but no embedder configured; use vector array or configure embedding service", nil)
}

func CypherSpecializedCallsEmbedQueryTextFailed(query string, cause error) Message {
	return cypherSpecializedCallsMessage(MessageCypherSpecializedCallsEmbedQueryTextFailed, fmt.Sprintf("failed to embed query '%s': %s", query, cause), map[string]any{"Query": query, "Cause": cause.Error()})
}

func CypherSpecializedCallsParameterNotProvided(parameter string) Message {
	return cypherSpecializedCallsMessage(MessageCypherSpecializedCallsParameterNotProvided, "parameter $"+parameter+" not provided", map[string]any{"Parameter": parameter})
}

func CypherSpecializedCallsParameterNonNumeric(parameter, valueType string) Message {
	return cypherSpecializedCallsMessage(MessageCypherSpecializedCallsParameterNonNumeric, "parameter $"+parameter+" contains non-numeric value: "+valueType, map[string]any{"Parameter": parameter, "ValueType": valueType})
}

func CypherSpecializedCallsParameterStringEmbedderRequired(parameter string) Message {
	return cypherSpecializedCallsMessage(MessageCypherSpecializedCallsParameterStringEmbedderRequired, "parameter $"+parameter+" is a string but no embedder configured; provide vector array or configure embedding service", map[string]any{"Parameter": parameter})
}

func CypherSpecializedCallsEmbedParameterFailed(parameter, value string, cause error) Message {
	return cypherSpecializedCallsMessage(MessageCypherSpecializedCallsEmbedParameterFailed, fmt.Sprintf("failed to embed parameter $%s value '%s': %s", parameter, value, cause), map[string]any{"Parameter": parameter, "Value": value, "Cause": cause.Error()})
}

func CypherSpecializedCallsParameterUnsupportedType(parameter, valueType string) Message {
	return cypherSpecializedCallsMessage(MessageCypherSpecializedCallsParameterUnsupportedType, "parameter $"+parameter+" has unsupported type for vector query: "+valueType+" (expected []float32, []float64, []interface{}, or string)", map[string]any{"Parameter": parameter, "ValueType": valueType})
}

func CypherSpecializedCallsUnsupportedParameters(parameters string) Message {
	return cypherSpecializedCallsMessage(MessageCypherSpecializedCallsUnsupportedParameters, "no query vector or search text provided - parameter(s) "+parameters+" have unsupported type (expected []float32, []float64, []interface{}, or string)", map[string]any{"Parameters": parameters})
}

func CypherSpecializedCallsQueryInputPossiblyUnsupported() Message {
	return cypherSpecializedCallsMessage(MessageCypherSpecializedCallsQueryInputPossiblyUnsupported, "no query vector or search text provided (parameter may have unsupported type - expected []float32, []float64, []interface{}, or string)", nil)
}

func CypherSpecializedCallsQueryInputRequired() Message {
	return cypherSpecializedCallsMessage(MessageCypherSpecializedCallsQueryInputRequired, "no query vector or search text provided", nil)
}

func CypherSpecializedCallsVectorQueryFailed() Message {
	return cypherSpecializedCallsMessage(MessageCypherSpecializedCallsVectorQueryFailed, "vector query failed", nil)
}

func CypherSpecializedCallsEmbedderNotConfigured() Message {
	return cypherSpecializedCallsMessage(MessageCypherSpecializedCallsEmbedderNotConfigured, "no embedder configured", nil)
}

func CypherSpecializedCallsVectorEmbedInvalidSyntax() Message {
	return cypherSpecializedCallsMessage(MessageCypherSpecializedCallsVectorEmbedInvalidSyntax, "invalid db.index.vector.embed syntax", nil)
}

func CypherSpecializedCallsVectorEmbedArgumentRequired() Message {
	return cypherSpecializedCallsMessage(MessageCypherSpecializedCallsVectorEmbedArgumentRequired, "db.index.vector.embed requires one argument", nil)
}

func CypherSpecializedCallsVectorEmbedUnmatchedParenthesis() Message {
	return cypherSpecializedCallsMessage(MessageCypherSpecializedCallsVectorEmbedUnmatchedParenthesis, "unmatched parenthesis in db.index.vector.embed", nil)
}

func CypherSpecializedCallsVectorEmbedTextRequired() Message {
	return cypherSpecializedCallsMessage(MessageCypherSpecializedCallsVectorEmbedTextRequired, "db.index.vector.embed requires non-empty text", nil)
}

func CypherSpecializedCallsVectorEmbedParameterString(parameter string) Message {
	return cypherSpecializedCallsMessage(MessageCypherSpecializedCallsVectorEmbedParameterString, "db.index.vector.embed parameter $"+parameter+" must be STRING", map[string]any{"Parameter": parameter})
}

func CypherSpecializedCallsVectorEmbedStringRequired() Message {
	return cypherSpecializedCallsMessage(MessageCypherSpecializedCallsVectorEmbedStringRequired, "db.index.vector.embed requires STRING text", nil)
}

func CypherSpecializedCallsVectorEmbedFailed(cause error) Message {
	return cypherSpecializedCallsMessage(MessageCypherSpecializedCallsVectorEmbedFailed, "failed to embed query: "+cause.Error(), map[string]any{"Cause": cause.Error()})
}

func CypherSpecializedCallsVectorProcedureNotFound() Message {
	return cypherSpecializedCallsMessage(MessageCypherSpecializedCallsVectorProcedureNotFound, "vector query procedure not found", nil)
}

func CypherSpecializedCallsVectorParametersMissing() Message {
	return cypherSpecializedCallsMessage(MessageCypherSpecializedCallsVectorParametersMissing, "missing parameters", nil)
}

func CypherSpecializedCallsVectorUnmatchedParenthesis() Message {
	return cypherSpecializedCallsMessage(MessageCypherSpecializedCallsVectorUnmatchedParenthesis, "unmatched parenthesis", nil)
}

func CypherSpecializedCallsFulltextIndexNotFound(index string) Message {
	return cypherSpecializedCallsMessage(MessageCypherSpecializedCallsFulltextIndexNotFound, "there is no such fulltext schema index: "+index, map[string]any{"Index": index})
}

func CypherSpecializedCallsFulltextArgumentCount() Message {
	return cypherSpecializedCallsMessage(MessageCypherSpecializedCallsFulltextArgumentCount, "invalid fulltext query syntax: expected 2 or 3 arguments", nil)
}

func CypherSpecializedCallsProcedureOptionsMapRequired() Message {
	return cypherSpecializedCallsMessage(MessageCypherSpecializedCallsProcedureOptionsMapRequired, "procedure options must be a MAP", nil)
}

func CypherSpecializedCallsFulltextOptionInvalid(option string, value any) Message {
	valueText := fmt.Sprint(value)
	return cypherSpecializedCallsMessage(MessageCypherSpecializedCallsFulltextOptionInvalid, "invalid fulltext options."+option+": "+valueText, map[string]any{"Option": option, "Value": valueText})
}

func CypherSpecializedCallsTemporalAssertArgumentCount() Message {
	return cypherSpecializedCallsMessage(MessageCypherSpecializedCallsTemporalAssertArgumentCount, "db.temporal.assertNoOverlap requires 7 parameters plus optional systemTime and systemSequence", nil)
}

func CypherSpecializedCallsDateTimeRequired(argument string) Message {
	return cypherSpecializedCallsMessage(MessageCypherSpecializedCallsDateTimeRequired, argument+" must be a valid datetime", map[string]any{"Argument": argument})
}

func CypherSpecializedCallsTemporalReadNodesFailed(label string, cause error) Message {
	quotedLabel := strconv.Quote(label)
	return cypherSpecializedCallsMessage(MessageCypherSpecializedCallsTemporalReadNodesFailed, "failed to read nodes for label "+quotedLabel+": "+cause.Error(), map[string]any{"Label": quotedLabel, "Cause": cause.Error()})
}

func CypherSpecializedCallsTemporalOverlap(property string, value any) Message {
	valueText := fmt.Sprint(value)
	return cypherSpecializedCallsMessage(MessageCypherSpecializedCallsTemporalOverlap, "temporal overlap detected for "+property+"="+valueText, map[string]any{"Property": property, "Value": valueText})
}

func CypherSpecializedCallsTemporalAsOfArgumentCount() Message {
	return cypherSpecializedCallsMessage(MessageCypherSpecializedCallsTemporalAsOfArgumentCount, "db.temporal.asOf requires 6 parameters plus optional systemTime and systemSequence", nil)
}

func CypherSpecializedCallsTemporalLookupFailed(label string, cause error) Message {
	quotedLabel := strconv.Quote(label)
	return cypherSpecializedCallsMessage(MessageCypherSpecializedCallsTemporalLookupFailed, "temporal lookup failed for label "+quotedLabel+": "+cause.Error(), map[string]any{"Label": quotedLabel, "Cause": cause.Error()})
}

func CypherSpecializedCallsTemporalInvalidSyntax(procedure string) Message {
	return cypherSpecializedCallsMessage(MessageCypherSpecializedCallsTemporalInvalidSyntax, "invalid "+procedure+" syntax", map[string]any{"Procedure": procedure})
}

func CypherSpecializedCallsTemporalClosingParenthesis(procedure string) Message {
	return cypherSpecializedCallsMessage(MessageCypherSpecializedCallsTemporalClosingParenthesis, "missing closing parenthesis in "+procedure, map[string]any{"Procedure": procedure})
}

func CypherSpecializedCallsArgumentRequired(argument string) Message {
	return cypherSpecializedCallsMessage(MessageCypherSpecializedCallsArgumentRequired, argument+" is required", map[string]any{"Argument": argument})
}

func CypherSpecializedCallsArgumentEmpty(argument string) Message {
	return cypherSpecializedCallsMessage(MessageCypherSpecializedCallsArgumentEmpty, argument+" cannot be empty", map[string]any{"Argument": argument})
}

func CypherSpecializedCallsUnsignedNonNegative(argument string) Message {
	return cypherSpecializedCallsMessage(MessageCypherSpecializedCallsUnsignedNonNegative, argument+" must be non-negative", map[string]any{"Argument": argument})
}

func CypherSpecializedCallsUnsignedWholeNonNegative(argument string) Message {
	return cypherSpecializedCallsMessage(MessageCypherSpecializedCallsUnsignedWholeNonNegative, argument+" must be a whole non-negative number", map[string]any{"Argument": argument})
}

func CypherSpecializedCallsUnsignedValid(argument string) Message {
	return cypherSpecializedCallsMessage(MessageCypherSpecializedCallsUnsignedValid, argument+" must be a valid uint64", map[string]any{"Argument": argument})
}

func CypherSpecializedCallsTxlogInvalidSyntax(procedure string) Message {
	return cypherSpecializedCallsMessage(MessageCypherSpecializedCallsTxlogInvalidSyntax, "invalid "+procedure+" syntax", map[string]any{"Procedure": procedure})
}

func CypherSpecializedCallsTxlogClosingParenthesis(procedure string) Message {
	return cypherSpecializedCallsMessage(MessageCypherSpecializedCallsTxlogClosingParenthesis, "missing closing parenthesis in "+procedure, map[string]any{"Procedure": procedure})
}

func CypherSpecializedCallsTxlogEntriesArgumentRequired() Message {
	return cypherSpecializedCallsMessage(MessageCypherSpecializedCallsTxlogEntriesArgumentRequired, "db.txlog.entries requires at least fromSeq parameter", nil)
}

func CypherSpecializedCallsTxlogInvalidSequence(argument string, cause error) Message {
	return cypherSpecializedCallsMessage(MessageCypherSpecializedCallsTxlogInvalidSequence, "invalid "+argument+": "+cause.Error(), map[string]any{"Argument": argument, "Cause": cause.Error()})
}

func CypherSpecializedCallsTxlogFromSequencePositive() Message {
	return cypherSpecializedCallsMessage(MessageCypherSpecializedCallsTxlogFromSequencePositive, "fromSeq must be greater than 0", nil)
}

func CypherSpecializedCallsWALUnavailable() Message {
	return cypherSpecializedCallsMessage(MessageCypherSpecializedCallsWALUnavailable, "WAL not available (memory-only database)", nil)
}

func CypherSpecializedCallsWALConfigUnavailable() Message {
	return cypherSpecializedCallsMessage(MessageCypherSpecializedCallsWALConfigUnavailable, "WAL config not available", nil)
}

func CypherSpecializedCallsWALDirectoryNotConfigured() Message {
	return cypherSpecializedCallsMessage(MessageCypherSpecializedCallsWALDirectoryNotConfigured, "WAL directory not configured", nil)
}

func CypherSpecializedCallsTxlogSequenceOrder() Message {
	return cypherSpecializedCallsMessage(MessageCypherSpecializedCallsTxlogSequenceOrder, "toSeq must be >= fromSeq", nil)
}

func CypherSpecializedCallsTxlogReadEntriesFailed(cause error) Message {
	return cypherSpecializedCallsMessage(MessageCypherSpecializedCallsTxlogReadEntriesFailed, "failed to read WAL entries: "+cause.Error(), map[string]any{"Cause": cause.Error()})
}

func CypherSpecializedCallsTxlogByIDArgumentRequired() Message {
	return cypherSpecializedCallsMessage(MessageCypherSpecializedCallsTxlogByIDArgumentRequired, "db.txlog.byTxId requires txId parameter", nil)
}

func CypherSpecializedCallsTxlogIDEmpty() Message {
	return cypherSpecializedCallsMessage(MessageCypherSpecializedCallsTxlogIDEmpty, "txId cannot be empty", nil)
}

func CypherSpecializedCallsTxlogFindEntriesFailed(cause error) Message {
	return cypherSpecializedCallsMessage(MessageCypherSpecializedCallsTxlogFindEntriesFailed, "failed to find WAL entries: "+cause.Error(), map[string]any{"Cause": cause.Error()})
}
