package localization

import (
	"fmt"
	"strconv"
)

const (
	MessageCypherMatchingMatchPatternRequired                     MessageID = "cyphermatching.match_pattern_required"
	MessageCypherMatchingMatchNodePatternRequired                 MessageID = "cyphermatching.match_node_pattern_required"
	MessageCypherMatchingReturnExpressionRequired                 MessageID = "cyphermatching.return_expression_required"
	MessageCypherMatchingReturnExpressionEmpty                    MessageID = "cyphermatching.return_expression_empty"
	MessageCypherMatchingStorageFailed                            MessageID = "cyphermatching.storage_failed"
	MessageCypherMatchingCollectSubqueryFailed                    MessageID = "cyphermatching.collect_subquery_failed"
	MessageCypherMatchingMatchUnwindClausesRequired               MessageID = "cyphermatching.match_unwind_clauses_required"
	MessageCypherMatchingUnwindASRequired                         MessageID = "cyphermatching.unwind_as_required"
	MessageCypherMatchingWithReturnClausesRequired                MessageID = "cyphermatching.with_return_clauses_required"
	MessageCypherMatchingWithOptionalMatchReturnClausesRequired   MessageID = "cyphermatching.with_optional_match_return_clauses_required"
	MessageCypherMatchingOrderByParseFailed                       MessageID = "cyphermatching.order_by_parse_failed"
	MessageCypherMatchingMatchPatternVariableMissing              MessageID = "cyphermatching.match_pattern_variable_missing"
	MessageCypherMatchingTraversalPatternInvalid                  MessageID = "cyphermatching.traversal_pattern_invalid"
	MessageCypherMatchingReturnAfterWithRequired                  MessageID = "cyphermatching.return_after_with_required"
	MessageCypherMatchingSkipParseFailed                          MessageID = "cyphermatching.skip_parse_failed"
	MessageCypherMatchingLimitParseFailed                         MessageID = "cyphermatching.limit_parse_failed"
	MessageCypherMatchingShortestPathQueryExpected                MessageID = "cyphermatching.shortest_path_query_expected"
	MessageCypherMatchingShortestPathSyntaxInvalid                MessageID = "cyphermatching.shortest_path_syntax_invalid"
	MessageCypherMatchingPathPatternInvalid                       MessageID = "cyphermatching.path_pattern_invalid"
	MessageCypherMatchingShortestPathStartVariableUnresolved      MessageID = "cyphermatching.shortest_path_start_variable_unresolved"
	MessageCypherMatchingShortestPathEndVariableUnresolved        MessageID = "cyphermatching.shortest_path_end_variable_unresolved"
	MessageCypherMatchingOptionalMatchNodeEndpointMissing         MessageID = "cyphermatching.optional_match_node_endpoint_missing"
	MessageCypherMatchingOptionalMatchNodeEndpointUnterminated    MessageID = "cyphermatching.optional_match_node_endpoint_unterminated"
	MessageCypherMatchingOptionalMatchTargetEndpointMissing       MessageID = "cyphermatching.optional_match_target_endpoint_missing"
	MessageCypherMatchingOptionalMatchTargetEndpointUnterminated  MessageID = "cyphermatching.optional_match_target_endpoint_unterminated"
	MessageCypherMatchingInitialTraversalMatchFailed              MessageID = "cyphermatching.initial_traversal_match_failed"
	MessageCypherMatchingAggregateCallExpected                    MessageID = "cyphermatching.aggregate_call_expected"
	MessageCypherMatchingFunctionParametersInsufficient           MessageID = "cyphermatching.function_parameters_insufficient"
	MessageCypherMatchingLabelExpressionMixedColon                MessageID = "cyphermatching.label_expression_mixed_colon"
	MessageCypherMatchingLabelExpressionMixedIs                   MessageID = "cyphermatching.label_expression_mixed_is"
	MessageCypherMatchingRelationshipTypeColonDisjunction         MessageID = "cyphermatching.relationship_type_colon_disjunction"
	MessageCypherMatchingRelationshipTypeColonConjunction         MessageID = "cyphermatching.relationship_type_colon_conjunction"
	MessageCypherMatchingVariableLengthTypeExpression             MessageID = "cyphermatching.variable_length_type_expression"
	MessageCypherMatchingLabelExpressionInWritePattern            MessageID = "cyphermatching.label_expression_in_write_pattern"
	MessageCypherMatchingRelationshipTypeExpressionInWritePattern MessageID = "cyphermatching.relationship_type_expression_in_write_pattern"
	MessageCypherMatchingSingleRelationshipTypeRequired          MessageID = "cyphermatching.single_relationship_type_required"
	MessageCypherMatchingIsNotOperandInvalid                     MessageID = "cyphermatching.is_not_operand_invalid"
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

// CypherMatchingLabelExpressionMixedColon is Neo4j's SyntaxError: a colon-separated label chain also uses label expression symbols (#860).
func CypherMatchingLabelExpressionMixedColon(expression string) Message {
	return cypherMatchingMessage(MessageCypherMatchingLabelExpressionMixedColon, "Mixing label expression symbols ('|', '&', '!', and '%') with colon (':') between labels is not allowed. Please only use one set of symbols. This expression could be expressed as :"+expression+".", map[string]any{"Expression": expression})
}

// CypherMatchingLabelExpressionMixedIs is Neo4j's SyntaxError: an IS label expression also uses colons between labels (#860).
func CypherMatchingLabelExpressionMixedIs(expression string) Message {
	return cypherMatchingMessage(MessageCypherMatchingLabelExpressionMixedIs, "Mixing the IS keyword with colon (':') between labels is not allowed. This expression could be expressed as IS "+expression+".", map[string]any{"Expression": expression})
}

// CypherMatchingRelationshipTypeColonDisjunction is Neo4j's SyntaxError: the legacy |: separator on a relationship with a variable, properties or a length (#860).
func CypherMatchingRelationshipTypeColonDisjunction(expression string) Message {
	return cypherMatchingMessage(MessageCypherMatchingRelationshipTypeColonDisjunction, "The semantics of using colon in the separation of alternative relationship types in conjunction with\nthe use of variable binding, inlined property predicates, or variable length is no longer supported.\nPlease separate the relationships types using `:"+expression+"` instead.", map[string]any{"Expression": expression})
}

// CypherMatchingRelationshipTypeColonConjunction is Neo4j's SyntaxError: a relationship pattern lists types separated by colons (#860).
func CypherMatchingRelationshipTypeColonConjunction() Message {
	return cypherMatchingMessage(MessageCypherMatchingRelationshipTypeColonConjunction, "Relationship types in a relationship type expressions may not be combined using ':'", nil)
}

// CypherMatchingVariableLengthTypeExpression is Neo4j's SyntaxError: a variable-length relationship uses a type expression other than alternatives (#860).
func CypherMatchingVariableLengthTypeExpression() Message {
	return cypherMatchingMessage(MessageCypherMatchingVariableLengthTypeExpression, "Variable length relationships must not use relationship type expressions.", nil)
}

// CypherMatchingLabelExpressionInWritePattern is Neo4j's SyntaxError: a CREATE or MERGE node pattern uses |, ! or % (#860).
func CypherMatchingLabelExpressionInWritePattern(clause string) Message {
	return cypherMatchingMessage(MessageCypherMatchingLabelExpressionInWritePattern, "Label expressions in patterns are not allowed in a "+clause+" clause, but only in a MATCH clause and in expressions", map[string]any{"Clause": clause})
}

// CypherMatchingRelationshipTypeExpressionInWritePattern is Neo4j's SyntaxError: a CREATE or MERGE relationship pattern uses !, & or % (#860).
func CypherMatchingRelationshipTypeExpressionInWritePattern(clause string) Message {
	return cypherMatchingMessage(MessageCypherMatchingRelationshipTypeExpressionInWritePattern, "Relationship type expressions in patterns are not allowed in a "+clause+" clause, but only in a MATCH clause", map[string]any{"Clause": clause})
}


// CypherMatchingIsNotOperandInvalid is Neo4j's SyntaxError for n IS NOT <label>: IS NOT takes NULL, a type or a normal form, not a label expression (#860).
func CypherMatchingIsNotOperandInvalid(input string) Message {
	return cypherMatchingMessage(MessageCypherMatchingIsNotOperandInvalid, "Invalid input '"+input+"': expected '::', 'NFC', 'NFD', 'NFKC', 'NFKD', 'NORMALIZED', 'NULL' or 'TYPED'", map[string]any{"Input": input})
}

// CypherMatchingSingleRelationshipTypeRequired is Neo4j's SyntaxError for alternative relationship types ([:R|S]) in a CREATE or MERGE pattern.
func CypherMatchingSingleRelationshipTypeRequired(clause string) Message {
	return cypherMatchingMessage(MessageCypherMatchingSingleRelationshipTypeRequired, "A single relationship type must be specified for "+clause, map[string]any{"Clause": clause})
}
