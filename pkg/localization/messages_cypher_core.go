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
