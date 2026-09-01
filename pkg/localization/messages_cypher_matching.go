package localization

import (
	"fmt"
	"strconv"
)

const (
	MessageCypherMatchingMatchPatternRequired                    MessageID = "cyphermatching.match_pattern_required"
	MessageCypherMatchingMatchNodePatternRequired                MessageID = "cyphermatching.match_node_pattern_required"
	MessageCypherMatchingReturnExpressionRequired                MessageID = "cyphermatching.return_expression_required"
	MessageCypherMatchingReturnExpressionEmpty                   MessageID = "cyphermatching.return_expression_empty"
	MessageCypherMatchingStorageFailed                           MessageID = "cyphermatching.storage_failed"
	MessageCypherMatchingCollectSubqueryFailed                   MessageID = "cyphermatching.collect_subquery_failed"
	MessageCypherMatchingMatchUnwindClausesRequired              MessageID = "cyphermatching.match_unwind_clauses_required"
	MessageCypherMatchingUnwindASRequired                        MessageID = "cyphermatching.unwind_as_required"
	MessageCypherMatchingWithReturnClausesRequired               MessageID = "cyphermatching.with_return_clauses_required"
	MessageCypherMatchingWithOptionalMatchReturnClausesRequired  MessageID = "cyphermatching.with_optional_match_return_clauses_required"
	MessageCypherMatchingOrderByParseFailed                      MessageID = "cyphermatching.order_by_parse_failed"
	MessageCypherMatchingMatchPatternVariableMissing             MessageID = "cyphermatching.match_pattern_variable_missing"
	MessageCypherMatchingTraversalPatternInvalid                 MessageID = "cyphermatching.traversal_pattern_invalid"
	MessageCypherMatchingReturnAfterWithRequired                 MessageID = "cyphermatching.return_after_with_required"
	MessageCypherMatchingSkipParseFailed                         MessageID = "cyphermatching.skip_parse_failed"
	MessageCypherMatchingLimitParseFailed                        MessageID = "cyphermatching.limit_parse_failed"
	MessageCypherMatchingShortestPathQueryExpected               MessageID = "cyphermatching.shortest_path_query_expected"
	MessageCypherMatchingShortestPathSyntaxInvalid               MessageID = "cyphermatching.shortest_path_syntax_invalid"
	MessageCypherMatchingPathPatternInvalid                      MessageID = "cyphermatching.path_pattern_invalid"
	MessageCypherMatchingShortestPathStartVariableUnresolved     MessageID = "cyphermatching.shortest_path_start_variable_unresolved"
	MessageCypherMatchingShortestPathEndVariableUnresolved       MessageID = "cyphermatching.shortest_path_end_variable_unresolved"
	MessageCypherMatchingOptionalMatchNodeEndpointMissing        MessageID = "cyphermatching.optional_match_node_endpoint_missing"
	MessageCypherMatchingOptionalMatchNodeEndpointUnterminated   MessageID = "cyphermatching.optional_match_node_endpoint_unterminated"
	MessageCypherMatchingOptionalMatchTargetEndpointMissing      MessageID = "cyphermatching.optional_match_target_endpoint_missing"
	MessageCypherMatchingOptionalMatchTargetEndpointUnterminated MessageID = "cyphermatching.optional_match_target_endpoint_unterminated"
	MessageCypherMatchingInitialTraversalMatchFailed             MessageID = "cyphermatching.initial_traversal_match_failed"
	MessageCypherMatchingAggregateCallExpected                   MessageID = "cyphermatching.aggregate_call_expected"
	MessageCypherMatchingFunctionParametersInsufficient          MessageID = "cyphermatching.function_parameters_insufficient"
)

func cypherMatchingMessage(id MessageID, fallback string, data map[string]any) Message {
	return Message{ID: id, Fallback: fallback, Data: data}
}

func cypherMatchingCauseMessage(id MessageID, prefix string, cause error) Message {
	return cypherMatchingMessage(id, prefix+cause.Error(), map[string]any{"Cause": cause.Error()})
}

func CypherMatchingMatchPatternRequired() Message {
	return cypherMatchingMessage(MessageCypherMatchingMatchPatternRequired, "MATCH clause requires a pattern", nil)
}

func CypherMatchingMatchNodePatternRequired() Message {
	return cypherMatchingMessage(MessageCypherMatchingMatchNodePatternRequired, "MATCH clause requires a node pattern, not just a relationship pattern", nil)
}

func CypherMatchingReturnExpressionRequired() Message {
	return cypherMatchingMessage(MessageCypherMatchingReturnExpressionRequired, "RETURN clause requires at least one expression", nil)
}

func CypherMatchingReturnExpressionEmpty() Message {
	return cypherMatchingMessage(MessageCypherMatchingReturnExpressionEmpty, "RETURN clause contains empty expression", nil)
}

func CypherMatchingStorageFailed(cause error) Message {
	return cypherMatchingCauseMessage(MessageCypherMatchingStorageFailed, "storage error: ", cause)
}

func CypherMatchingCollectSubqueryFailed(cause error) Message {
	return cypherMatchingCauseMessage(MessageCypherMatchingCollectSubqueryFailed, "COLLECT subquery failed: ", cause)
}

func CypherMatchingMatchUnwindClausesRequired() Message {
	return cypherMatchingMessage(MessageCypherMatchingMatchUnwindClausesRequired, "MATCH and UNWIND clauses required (e.g., MATCH (n) UNWIND n.items AS item RETURN item)", nil)
}

func CypherMatchingUnwindASRequired() Message {
	return cypherMatchingMessage(MessageCypherMatchingUnwindASRequired, "UNWIND requires AS clause (e.g., UNWIND [1,2,3] AS x)", nil)
}

func CypherMatchingWithReturnClausesRequired() Message {
	return cypherMatchingMessage(MessageCypherMatchingWithReturnClausesRequired, "WITH and RETURN clauses required", nil)
}

func CypherMatchingWithOptionalMatchReturnClausesRequired() Message {
	return cypherMatchingMessage(MessageCypherMatchingWithOptionalMatchReturnClausesRequired, "WITH, OPTIONAL MATCH, and RETURN clauses required", nil)
}

func CypherMatchingOrderByParseFailed() Message {
	return cypherMatchingMessage(MessageCypherMatchingOrderByParseFailed, "failed to parse ORDER BY clause", nil)
}

func CypherMatchingMatchPatternVariableMissing(clause string) Message {
	quoted := strconv.Quote(clause)
	return cypherMatchingMessage(MessageCypherMatchingMatchPatternVariableMissing, "invalid MATCH pattern: missing variable in "+quoted, map[string]any{"Clause": quoted})
}

func CypherMatchingTraversalPatternInvalid(pattern string) Message {
	return cypherMatchingMessage(MessageCypherMatchingTraversalPatternInvalid, "invalid traversal pattern: "+pattern, map[string]any{"Pattern": pattern})
}

func CypherMatchingReturnAfterWithRequired() Message {
	return cypherMatchingMessage(MessageCypherMatchingReturnAfterWithRequired, "RETURN clause required after WITH", nil)
}

func CypherMatchingSkipParseFailed() Message {
	return cypherMatchingMessage(MessageCypherMatchingSkipParseFailed, "failed to parse SKIP clause", nil)
}

func CypherMatchingLimitParseFailed() Message {
	return cypherMatchingMessage(MessageCypherMatchingLimitParseFailed, "failed to parse LIMIT clause", nil)
}

func CypherMatchingShortestPathQueryExpected() Message {
	return cypherMatchingMessage(MessageCypherMatchingShortestPathQueryExpected, "not a shortest path query", nil)
}

func CypherMatchingShortestPathSyntaxInvalid() Message {
	return cypherMatchingMessage(MessageCypherMatchingShortestPathSyntaxInvalid, "invalid shortestPath syntax", nil)
}

func CypherMatchingPathPatternInvalid(pattern string) Message {
	return cypherMatchingMessage(MessageCypherMatchingPathPatternInvalid, "invalid path pattern: "+pattern, map[string]any{"Pattern": pattern})
}

func CypherMatchingShortestPathStartVariableUnresolved(variable string) Message {
	quoted := strconv.Quote(variable)
	return cypherMatchingMessage(MessageCypherMatchingShortestPathStartVariableUnresolved, "shortestPath: could not resolve start variable "+quoted+" from preceding MATCH clause", map[string]any{"Variable": quoted})
}

func CypherMatchingShortestPathEndVariableUnresolved(variable string) Message {
	quoted := strconv.Quote(variable)
	return cypherMatchingMessage(MessageCypherMatchingShortestPathEndVariableUnresolved, "shortestPath: could not resolve end variable "+quoted+" from preceding MATCH clause", map[string]any{"Variable": quoted})
}

func CypherMatchingOptionalMatchNodeEndpointMissing(pattern string) Message {
	quoted := strconv.Quote(pattern)
	return cypherMatchingMessage(MessageCypherMatchingOptionalMatchNodeEndpointMissing, "optional match pattern "+quoted+" has no node endpoint", map[string]any{"Pattern": quoted})
}

func CypherMatchingOptionalMatchNodeEndpointUnterminated(pattern string) Message {
	quoted := strconv.Quote(pattern)
	return cypherMatchingMessage(MessageCypherMatchingOptionalMatchNodeEndpointUnterminated, "optional match pattern "+quoted+" has an unterminated node endpoint", map[string]any{"Pattern": quoted})
}

func CypherMatchingOptionalMatchTargetEndpointMissing(pattern string) Message {
	quoted := strconv.Quote(pattern)
	return cypherMatchingMessage(MessageCypherMatchingOptionalMatchTargetEndpointMissing, "optional match pattern "+quoted+" has no target endpoint", map[string]any{"Pattern": quoted})
}

func CypherMatchingOptionalMatchTargetEndpointUnterminated(pattern string) Message {
	quoted := strconv.Quote(pattern)
	return cypherMatchingMessage(MessageCypherMatchingOptionalMatchTargetEndpointUnterminated, "optional match pattern "+quoted+" has an unterminated target endpoint", map[string]any{"Pattern": quoted})
}

func CypherMatchingInitialTraversalMatchFailed(cause error) Message {
	return cypherMatchingCauseMessage(MessageCypherMatchingInitialTraversalMatchFailed, "failed to execute initial traversal MATCH: ", cause)
}

func CypherMatchingAggregateCallExpected(expression string) Message {
	quoted := strconv.Quote(expression)
	return cypherMatchingMessage(MessageCypherMatchingAggregateCallExpected, "not a whole aggregate call: "+quoted, map[string]any{"Expression": quoted})
}

func CypherMatchingFunctionParametersInsufficient(function string) Message {
	return cypherMatchingMessage(MessageCypherMatchingFunctionParametersInsufficient, fmt.Sprintf("insufficient parameters for function '%s'", function), map[string]any{"Function": function})
}
