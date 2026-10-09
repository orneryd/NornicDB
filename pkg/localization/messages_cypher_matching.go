package localization

import (
	"fmt"
	"strconv"
)

const (
	MessageCypherMatchingMatchPatternRequired                     MessageID = "cyphermatching.match_pattern_required"
	MessageCypherMatchingMatchNodePatternRequired                 MessageID = "cyphermatching.match_node_pattern_required"
	MessageCypherMatchingReturnExpressionRequired                 MessageID = "cyphermatching.return_expression_required"
	MessageCypherMatchingAggregationScopeVariable                 MessageID = "cyphermatching.aggregation_scope_variable"
	MessageCypherMatchingReturnExpressionEmpty                    MessageID = "cyphermatching.return_expression_empty"
	MessageCypherMatchingStorageFailed                            MessageID = "cyphermatching.storage_failed"
	MessageCypherMatchingCollectSubqueryFailed                    MessageID = "cyphermatching.collect_subquery_failed"
	MessageCypherMatchingMatchUnwindClausesRequired               MessageID = "cyphermatching.match_unwind_clauses_required"
	MessageCypherMatchingUnwindASRequired                         MessageID = "cyphermatching.unwind_as_required"
	MessageCypherMatchingWithReturnClausesRequired                MessageID = "cyphermatching.with_return_clauses_required"
	MessageCypherMatchingOrderByParseFailed                       MessageID = "cyphermatching.order_by_parse_failed"
	MessageCypherMatchingMatchPatternVariableMissing              MessageID = "cyphermatching.match_pattern_variable_missing"
	MessageCypherMatchingTraversalPatternInvalid                  MessageID = "cyphermatching.traversal_pattern_invalid"
	MessageCypherMatchingReturnAfterWithRequired                  MessageID = "cyphermatching.return_after_with_required"
	MessageCypherMatchingSkipParseFailed                          MessageID = "cyphermatching.skip_parse_failed"
	MessageCypherMatchingLimitParseFailed                         MessageID = "cyphermatching.limit_parse_failed"
	MessageCypherMatchingPathPatternInvalid                       MessageID = "cyphermatching.path_pattern_invalid"
	MessageCypherMatchingShortestPathMinimalLength                MessageID = "cyphermatching.shortest_path_minimal_length"
	MessageCypherMatchingShortestPathCommonEndNodes               MessageID = "cyphermatching.shortest_path_common_end_nodes"
	MessageCypherMatchingShortestPathSingleRelationship           MessageID = "cyphermatching.shortest_path_single_relationship"
	MessageCypherMatchingShortestPathUnboundNodes                 MessageID = "cyphermatching.shortest_path_unbound_nodes"
	MessageCypherMatchingShortestPathRelationshipProperties       MessageID = "cyphermatching.shortest_path_relationship_properties"
	MessageCypherMatchingOptionalMatchNodeEndpointMissing         MessageID = "cyphermatching.optional_match_node_endpoint_missing"
	MessageCypherMatchingOptionalMatchNodeEndpointUnterminated    MessageID = "cyphermatching.optional_match_node_endpoint_unterminated"
	MessageCypherMatchingOptionalMatchTargetEndpointMissing       MessageID = "cyphermatching.optional_match_target_endpoint_missing"
	MessageCypherMatchingOptionalMatchTargetEndpointUnterminated  MessageID = "cyphermatching.optional_match_target_endpoint_unterminated"
	MessageCypherMatchingAggregateCallExpected                    MessageID = "cyphermatching.aggregate_call_expected"
	MessageCypherMatchingFunctionParametersInsufficient           MessageID = "cyphermatching.function_parameters_insufficient"
	MessageCypherMatchingLabelExpressionMixedColon                MessageID = "cyphermatching.label_expression_mixed_colon"
	MessageCypherMatchingLabelExpressionMixedIs                   MessageID = "cyphermatching.label_expression_mixed_is"
	MessageCypherMatchingRelationshipTypeColonDisjunction         MessageID = "cyphermatching.relationship_type_colon_disjunction"
	MessageCypherMatchingRelationshipTypeColonConjunction         MessageID = "cyphermatching.relationship_type_colon_conjunction"
	MessageCypherMatchingQuantifierInExpressionPattern            MessageID = "cyphermatching.quantifier_in_expression_pattern"
	MessageCypherMatchingQuantifiedPathInWritePattern             MessageID = "cyphermatching.quantified_path_in_write_pattern"
	MessageCypherMatchingVariableLengthInQuantifiedPath           MessageID = "cyphermatching.variable_length_in_quantified_path"
	MessageCypherMatchingVariableLengthTypeExpression             MessageID = "cyphermatching.variable_length_type_expression"
	MessageCypherMatchingLabelExpressionInWritePattern            MessageID = "cyphermatching.label_expression_in_write_pattern"
	MessageCypherMatchingRelationshipTypeExpressionInWritePattern MessageID = "cyphermatching.relationship_type_expression_in_write_pattern"
	MessageCypherMatchingSingleRelationshipTypeRequired           MessageID = "cyphermatching.single_relationship_type_required"
	MessageCypherMatchingPatternPredicateInWritePattern           MessageID = "cyphermatching.pattern_predicate_in_write_pattern"
	MessageCypherMatchingPatternPredicateVariableLength           MessageID = "cyphermatching.pattern_predicate_variable_length"
	MessageCypherMatchingIsNotOperandInvalid                      MessageID = "cyphermatching.is_not_operand_invalid"
	MessageCypherMatchingReturnStarNoVariables                    MessageID = "cyphermatching.return_star_no_variables"
	MessageCypherMatchingOptionalMatchShapeUnsupported            MessageID = "cyphermatching.optional_match_shape_unsupported"
	MessageCypherMatchingPathSelectorPathCountNotPositive         MessageID = "cyphermatching.path_selector_path_count_not_positive"
	MessageCypherMatchingPathSelectorGroupCountNotPositive        MessageID = "cyphermatching.path_selector_group_count_not_positive"
	MessageCypherMatchingPathSelectorMultiplePatterns             MessageID = "cyphermatching.path_selector_multiple_patterns"
	MessageCypherMatchingPathSelectorWithShortestPathFunction     MessageID = "cyphermatching.path_selector_with_shortest_path_function"
	MessageCypherMatchingPathModeVariableLength                   MessageID = "cyphermatching.path_mode_variable_length"
	MessageCypherMatchingPathSelectorInWritePattern               MessageID = "cyphermatching.path_selector_in_write_pattern"
	MessageCypherMatchingPathSelectorCountInvalid                 MessageID = "cyphermatching.path_selector_count_invalid"
	MessageCypherMatchingPathSelectorCountType                    MessageID = "cyphermatching.path_selector_count_type"
	MessageCypherMatchingPatternMarkerOutsideMatch                MessageID = "cyphermatching.pattern_marker_outside_match"
	MessageCypherMatchingVariableTypeConflict                     MessageID = "cyphermatching.variable_type_conflict"
	MessageCypherMatchingRepeatableElementsUnbounded              MessageID = "cyphermatching.repeatable_elements_unbounded"
	MessageCypherMatchingRepeatableElementsPathMode               MessageID = "cyphermatching.repeatable_elements_path_mode"
	MessageCypherMatchingQuantifiedPathZeroLimit                  MessageID = "cyphermatching.quantified_path_zero_limit"
	MessageCypherMatchingQuantifiedPathVariableOutside            MessageID = "cyphermatching.quantified_path_variable_outside"
	MessageCypherMatchingQuantifiedPathNested                     MessageID = "cyphermatching.quantified_path_nested"
	MessageCypherMatchingQuantifiedPathNoRelationship             MessageID = "cyphermatching.quantified_path_no_relationship"
	MessageCypherMatchingQuantifierBoundsReversed                 MessageID = "cyphermatching.quantifier_bounds_reversed"
	MessageCypherMatchingQuantifiedPathVariableBound              MessageID = "cyphermatching.quantified_path_variable_bound"
	MessageCypherMatchingParenthesisedPathJuxtaposed              MessageID = "cyphermatching.parenthesised_path_juxtaposed"
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

func CypherMatchingPathPatternInvalid(pattern string) Message {
	return cypherMatchingMessage(MessageCypherMatchingPathPatternInvalid, "invalid path pattern: "+pattern, map[string]any{"Pattern": pattern})
}

// CypherMatchingShortestPathMinimalLength is Neo4j's error for a
// shortestPath or allShortestPaths pattern whose minimum length is above 1.
func CypherMatchingShortestPathMinimalLength(function string) Message {
	return cypherMatchingMessage(MessageCypherMatchingShortestPathMinimalLength, function+"(...) does not support a minimal length different from 0 or 1", map[string]any{"Function": function})
}

// CypherMatchingShortestPathSingleRelationship is Neo4j's error for a
// shortestPath or allShortestPaths pattern without exactly one relationship.
func CypherMatchingShortestPathSingleRelationship(function string) Message {
	return cypherMatchingMessage(MessageCypherMatchingShortestPathSingleRelationship, function+"(...) requires a pattern containing a single relationship", map[string]any{"Function": function})
}

// CypherMatchingShortestPathUnboundNodes is Neo4j's error for a
// shortestPath or allShortestPaths expression with an endpoint that isn't a
// bound variable.
func CypherMatchingShortestPathUnboundNodes(function string) Message {
	return cypherMatchingMessage(MessageCypherMatchingShortestPathUnboundNodes, "A "+function+"(...) requires bound nodes when not part of a MATCH clause.", map[string]any{"Function": function})
}

// CypherMatchingShortestPathRelationshipProperties is Neo4j's error for
// relationship properties in a shortestPath or allShortestPaths pattern.
func CypherMatchingShortestPathRelationshipProperties(function, properties string) Message {
	return cypherMatchingMessage(MessageCypherMatchingShortestPathRelationshipProperties, function+"(...) contains properties "+properties+". This is currently not supported.", map[string]any{"Function": function, "Properties": properties})
}

// CypherMatchingShortestPathCommonEndNodes is Neo4j's error for a
// shortestPath search whose start and end are the same node.
func CypherMatchingShortestPathCommonEndNodes() Message {
	return cypherMatchingMessage(MessageCypherMatchingShortestPathCommonEndNodes, "The shortest path algorithm does not work when the start and end nodes are the same. This can happen if you\nperform a shortestPath search after a cartesian product that might have the same start and end nodes for some\nof the rows passed to shortestPath. If you would rather not experience this exception, and can accept the\npossibility of missing results for those rows, disable this in the Neo4j configuration by setting\n`dbms.cypher.forbid_shortestpath_common_nodes` to false. If you cannot accept missing results, and really want the\nshortestPath between two common nodes, then re-write the query using a standard Cypher variable length pattern\nexpression followed by ordering by path length and limiting to one result.", nil)
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
// CypherMatchingQuantifierInExpressionPattern is the SyntaxError for a
// relationship quantifier (token) in a pattern predicate or comprehension,
// which Neo4j's grammar doesn't accept there.
func CypherMatchingQuantifierInExpressionPattern(token string) Message {
	return cypherMatchingMessage(MessageCypherMatchingQuantifierInExpressionPattern, "Invalid input '"+token+"': a relationship quantifier is allowed only in a MATCH pattern or a subquery", map[string]any{"Token": token})
}

// CypherMatchingQuantifiedPathInWritePattern is Neo4j's error for a
// quantified path pattern in CREATE or MERGE (clause).
func CypherMatchingQuantifiedPathInWritePattern(clause string) Message {
	return cypherMatchingMessage(MessageCypherMatchingQuantifiedPathInWritePattern, "Quantified path patterns cannot be used in a "+clause+" clause, but only in a MATCH clause.", map[string]any{"Clause": clause})
}

// CypherMatchingVariableLengthInQuantifiedPath is Neo4j's error for a
// variable-length relationship with a quantifier.
func CypherMatchingVariableLengthInQuantifiedPath() Message {
	return cypherMatchingMessage(MessageCypherMatchingVariableLengthInQuantifiedPath, "Variable length relationships cannot be part of a quantified path pattern.", nil)
}

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

// CypherMatchingPatternPredicateInWritePattern is Neo4j's SyntaxError for a
// node or relationship pattern's own WHERE in CREATE or MERGE (#878).
// Element is Node or Relationship.
func CypherMatchingPatternPredicateInWritePattern(element, clause string) Message {
	return cypherMatchingMessage(MessageCypherMatchingPatternPredicateInWritePattern, element+" pattern predicates are not allowed in a "+clause+" clause, but only in a MATCH clause or inside a pattern comprehension", map[string]any{"Element": element, "Clause": clause})
}

// CypherMatchingPatternPredicateVariableLength is Neo4j's SyntaxError for a
// WHERE inside a variable-length relationship pattern (#878).
func CypherMatchingPatternPredicateVariableLength() Message {
	return cypherMatchingMessage(MessageCypherMatchingPatternPredicateVariableLength, "Relationship pattern predicates are not supported for variable-length relationships.", nil)
}

// CypherMatchingReturnStarNoVariables is the SyntaxError for RETURN * with
// no variable in scope (Neo4j's code; NornicDB's wording).
func CypherMatchingReturnStarNoVariables() Message {
	return cypherMatchingMessage(MessageCypherMatchingReturnStarNoVariables, "RETURN * requires at least one variable in scope", nil)
}

// CypherMatchingOptionalMatchShapeUnsupported is the SyntaxError for a
// MATCH … OPTIONAL MATCH statement the clause pipeline does not run. There is
// no other executor for it (no alternate execution path).
func CypherMatchingOptionalMatchShapeUnsupported(query string) Message {
	return cypherMatchingMessage(MessageCypherMatchingOptionalMatchShapeUnsupported, "this MATCH … OPTIONAL MATCH form is not supported: "+query, map[string]any{"Query": query})
}

// CypherMatchingIsNotOperandInvalid is Neo4j's SyntaxError for n IS NOT <label>: IS NOT takes NULL, a type or a normal form, not a label expression (#860).
func CypherMatchingIsNotOperandInvalid(input string) Message {
	return cypherMatchingMessage(MessageCypherMatchingIsNotOperandInvalid, "Invalid input '"+input+"': expected '::', 'NFC', 'NFD', 'NFKC', 'NFKD', 'NORMALIZED', 'NULL' or 'TYPED'", map[string]any{"Input": input})
}

// CypherMatchingSingleRelationshipTypeRequired is Neo4j's SyntaxError for alternative relationship types ([:R|S]) in a CREATE or MERGE pattern.
func CypherMatchingSingleRelationshipTypeRequired(clause string) Message {
	return cypherMatchingMessage(MessageCypherMatchingSingleRelationshipTypeRequired, "A single relationship type must be specified for "+clause, map[string]any{"Clause": clause})
}

// CypherMatchingAggregationScopeVariable identifies a WITH or RETURN with
// DISTINCT or an aggregation whose WHERE or ORDER BY reads a variable that
// the projection doesn't carry.
func CypherMatchingAggregationScopeVariable(variable string) Message {
	return cypherMatchingMessage(MessageCypherMatchingAggregationScopeVariable,
		"In a WITH/RETURN with DISTINCT or an aggregation, it is not possible to access variables declared before the WITH/RETURN: "+variable,
		map[string]any{"Variable": variable})
}

// CypherMatchingPathSelectorPathCountNotPositive is Neo4j's SyntaxError for
// a path selector whose path count is 0 (SHORTEST 0, ANY 0).
func CypherMatchingPathSelectorPathCountNotPositive() Message {
	return cypherMatchingMessage(MessageCypherMatchingPathSelectorPathCountNotPositive, "The path count needs to be greater than 0.", nil)
}

// CypherMatchingPathSelectorGroupCountNotPositive is Neo4j's SyntaxError for
// SHORTEST 0 GROUPS.
func CypherMatchingPathSelectorGroupCountNotPositive() Message {
	return cypherMatchingMessage(MessageCypherMatchingPathSelectorGroupCountNotPositive, "The group count needs to be greater than 0.", nil)
}

// CypherMatchingPathSelectorMultiplePatterns is Neo4j's SyntaxError for a
// MATCH with a selective path selector (ANY, SHORTEST) and another pattern.
func CypherMatchingPathSelectorMultiplePatterns() Message {
	return cypherMatchingMessage(MessageCypherMatchingPathSelectorMultiplePatterns, "Multiple path patterns cannot be used in the same clause in combination with a selective path selector. You may want to use multiple MATCH clauses, or you might want to consider using the REPEATABLE ELEMENTS match mode.", nil)
}

// CypherMatchingPathSelectorWithShortestPathFunction is Neo4j's SyntaxError
// for shortestPath or allShortestPaths with a path selector, an explicit
// match mode or an explicit path mode.
func CypherMatchingPathSelectorWithShortestPathFunction() Message {
	return cypherMatchingMessage(MessageCypherMatchingPathSelectorWithShortestPathFunction, "Mixing shortestPath/allShortestPaths with path selectors (e.g. `ANY SHORTEST`), explicit match modes (e.g. `DIFFERENT RELATIONSHIPS`) or explicit path modes (e.g. `ACYCLIC`) is not allowed.", nil)
}

// CypherMatchingPathModeVariableLength is Neo4j's SyntaxError for an
// explicit path mode (WALK, TRAIL, ACYCLIC) on a pattern with a
// variable-length relationship (-[*]->).
func CypherMatchingPathModeVariableLength(mode string) Message {
	return cypherMatchingMessage(MessageCypherMatchingPathModeVariableLength, "Using a variable-length relationship such as `-[*]->` together with explicit path mode `"+mode+"` is not available.", map[string]any{"Mode": mode})
}

// CypherMatchingPathSelectorInWritePattern is Neo4j's SyntaxError for a path
// selector in a CREATE or MERGE pattern.
func CypherMatchingPathSelectorInWritePattern(clause string) Message {
	return cypherMatchingMessage(MessageCypherMatchingPathSelectorInWritePattern, "Path selectors such as `SHORTEST 1 PATHS` cannot be used in a "+clause+" clause, but only in a MATCH clause.", map[string]any{"Clause": clause})
}

// CypherMatchingPathSelectorCountInvalid is Neo4j's error for a path
// selector count parameter that is not positive (SHORTEST $k with $k = 0).
func CypherMatchingPathSelectorCountInvalid(value string) Message {
	return cypherMatchingMessage(MessageCypherMatchingPathSelectorCountInvalid, "Count requires positive integer argument, got `"+value+"`", map[string]any{"Value": value})
}

// CypherMatchingPathSelectorCountType is Neo4j's TypeError for a path
// selector count parameter that is not an integer.
func CypherMatchingPathSelectorCountType(valueType string) Message {
	return cypherMatchingMessage(MessageCypherMatchingPathSelectorCountType, "Expected Integer but got "+valueType, map[string]any{"Type": valueType})
}

// CypherMatchingPatternMarkerOutsideMatch is the error for a path selector or
// match mode the statement rewrite left for a MATCH step that did not run it.
func CypherMatchingPatternMarkerOutsideMatch() Message {
	return cypherMatchingMessage(MessageCypherMatchingPatternMarkerOutsideMatch, "a path selector or match mode can only be used in a MATCH pattern", nil)
}

// CypherMatchingVariableTypeConflict is Neo4j's SyntaxError for a variable
// a pattern binds as two types, such as a relationship and a list of them
// (-[r]->, -[r*1..2]->). Defined is the type it was bound as, Expected the
// type this place binds.
func CypherMatchingVariableTypeConflict(variable, defined, expected string) Message {
	return cypherMatchingMessage(MessageCypherMatchingVariableTypeConflict, "Type mismatch: "+variable+" defined with conflicting type "+defined+" (expected "+expected+")", map[string]any{"Variable": variable, "Defined": defined, "Expected": expected})
}

// CypherMatchingRepeatableElementsUnbounded is Neo4j's SyntaxError for a
// quantifier or variable-length relationship without an upper bound in a
// MATCH REPEATABLE ELEMENTS, where it could repeat without end.
func CypherMatchingRepeatableElementsUnbounded() Message {
	return cypherMatchingMessage(MessageCypherMatchingRepeatableElementsUnbounded, "The quantified path pattern may yield an infinite number of rows under match mode 'REPEATABLE ELEMENTS'. Add an upper bound to the quantified path pattern.", nil)
}

// CypherMatchingRepeatableElementsPathMode is Neo4j's SyntaxError for a
// path mode that forbids repeating (TRAIL, ACYCLIC) in a MATCH REPEATABLE
// ELEMENTS.
func CypherMatchingRepeatableElementsPathMode(mode string) Message {
	return cypherMatchingMessage(MessageCypherMatchingRepeatableElementsPathMode, "REPEATABLE ELEMENTS with "+mode+" path mode is not supported.", map[string]any{"Mode": mode})
}

// CypherMatchingQuantifiedPathZeroLimit is Neo4j's SyntaxError for a
// quantified path pattern repeated at most 0 times ({0}, {,0}).
func CypherMatchingQuantifiedPathZeroLimit() Message {
	return cypherMatchingMessage(MessageCypherMatchingQuantifiedPathZeroLimit, "A quantifier for a path pattern must not be limited by 0.", nil)
}

// CypherMatchingQuantifiedPathVariableOutside is Neo4j's SyntaxError for a
// variable named both inside and outside a quantified path pattern.
func CypherMatchingQuantifiedPathVariableOutside(variable string) Message {
	return cypherMatchingMessage(MessageCypherMatchingQuantifiedPathVariableOutside, "The variable `"+variable+"` occurs both inside and outside a quantified path pattern and needs to be renamed.", map[string]any{"Variable": variable})
}

// CypherMatchingQuantifiedPathNested is Neo4j's SyntaxError for a quantified
// path pattern or relationship quantifier inside a quantified path pattern.
func CypherMatchingQuantifiedPathNested() Message {
	return cypherMatchingMessage(MessageCypherMatchingQuantifiedPathNested, "Quantified path patterns are not allowed to be nested.", nil)
}

// CypherMatchingQuantifiedPathNoRelationship is Neo4j's SyntaxError for a
// quantified path pattern without a relationship (((x))+).
func CypherMatchingQuantifiedPathNoRelationship() Message {
	return cypherMatchingMessage(MessageCypherMatchingQuantifiedPathNoRelationship, "A quantified path pattern needs to have at least one relationship.", nil)
}

// CypherMatchingQuantifierBoundsReversed is Neo4j's SyntaxError for a
// quantifier whose lower bound exceeds its upper bound ({2,1}).
func CypherMatchingQuantifierBoundsReversed() Message {
	return cypherMatchingMessage(MessageCypherMatchingQuantifierBoundsReversed, "A quantifier for a path pattern must not have a lower bound which exceeds its upper bound.", nil)
}

// CypherMatchingQuantifiedPathVariableBound is Neo4j's SyntaxError for a
// variable an earlier clause binds, named inside a quantified path pattern.
func CypherMatchingQuantifiedPathVariableBound(variable string) Message {
	return cypherMatchingMessage(MessageCypherMatchingQuantifiedPathVariableBound, "The variable `"+variable+"` is already defined in a previous clause, it cannot be referenced as a node or as a relationship variable inside of a quantified path pattern.", map[string]any{"Variable": variable})
}

// CypherMatchingParenthesisedPathJuxtaposed is Neo4j's SyntaxError for a
// parenthesised path without a quantifier written next to another element.
func CypherMatchingParenthesisedPathJuxtaposed() Message {
	return cypherMatchingMessage(MessageCypherMatchingParenthesisedPathJuxtaposed, "Juxtaposition is currently only supported for quantified path patterns.", nil)
}
