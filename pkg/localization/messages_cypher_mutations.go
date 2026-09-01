package localization

import (
	"fmt"
	"strconv"
)

const (
	MessageCypherMutationsInvalidLabelName                    MessageID = "cyphermutations.invalid_label_name"
	MessageCypherMutationsInvalidLabelReserved                MessageID = "cyphermutations.invalid_label_reserved"
	MessageCypherMutationsInvalidPropertyKey                  MessageID = "cyphermutations.invalid_property_key"
	MessageCypherMutationsInvalidPropertyValue                MessageID = "cyphermutations.invalid_property_value"
	MessageCypherMutationsCreateNodeFailed                    MessageID = "cyphermutations.create_node_failed"
	MessageCypherMutationsCreateSourceNodeFailed              MessageID = "cyphermutations.create_source_node_failed"
	MessageCypherMutationsCreateTargetNodeFailed              MessageID = "cyphermutations.create_target_node_failed"
	MessageCypherMutationsInvalidRelationshipType             MessageID = "cyphermutations.invalid_relationship_type"
	MessageCypherMutationsInvalidRelationshipPropertyKey      MessageID = "cyphermutations.invalid_relationship_property_key"
	MessageCypherMutationsCreateRelationshipFailed            MessageID = "cyphermutations.create_relationship_failed"
	MessageCypherMutationsInvalidPropertyMapCause             MessageID = "cyphermutations.invalid_property_map_cause"
	MessageCypherMutationsRelationshipPatternMustStartNode    MessageID = "cyphermutations.relationship_pattern_must_start_node"
	MessageCypherMutationsRelationshipPatternUnmatchedParen   MessageID = "cyphermutations.relationship_pattern_unmatched_parenthesis"
	MessageCypherMutationsRelationshipPatternUnmatchedBracket MessageID = "cyphermutations.relationship_pattern_unmatched_bracket"
	MessageCypherMutationsRelationshipPatternForwardExpected  MessageID = "cyphermutations.relationship_pattern_forward_node_expected"
	MessageCypherMutationsRelationshipPatternArrowExpected    MessageID = "cyphermutations.relationship_pattern_arrow_node_expected"
	MessageCypherMutationsRelationshipPatternSecondUnmatched  MessageID = "cyphermutations.relationship_pattern_second_node_unmatched_parenthesis"
	MessageCypherMutationsMatchLabelLookupFailed              MessageID = "cyphermutations.match_label_lookup_failed"
	MessageCypherMutationsMatchAllNodesFailed                 MessageID = "cyphermutations.match_all_nodes_failed"
	MessageCypherMutationsRelationshipExistenceCheckFailed    MessageID = "cyphermutations.relationship_existence_check_failed"
	MessageCypherMutationsAddLabelFailed                      MessageID = "cyphermutations.add_label_failed"
	MessageCypherMutationsSetAssignmentParameterNameRequired  MessageID = "cyphermutations.set_assignment_parameter_name_required"
	MessageCypherMutationsSetAssignmentParametersRequired     MessageID = "cyphermutations.set_assignment_parameters_required"
	MessageCypherMutationsSetAssignmentParameterNotFound      MessageID = "cyphermutations.set_assignment_parameter_not_found"
	MessageCypherMutationsReplaceNodePropertiesFailed         MessageID = "cyphermutations.replace_node_properties_failed"
	MessageCypherMutationsReplaceEdgePropertiesFailed         MessageID = "cyphermutations.replace_edge_properties_failed"
	MessageCypherMutationsUnknownSetVariable                  MessageID = "cyphermutations.unknown_set_variable"
	MessageCypherMutationsUpdateNodePropertyFailed            MessageID = "cyphermutations.update_node_property_failed"
	MessageCypherMutationsUpdateEdgePropertyFailed            MessageID = "cyphermutations.update_edge_property_failed"
	MessageCypherMutationsResolveSourceNodeFailed             MessageID = "cyphermutations.resolve_source_node_failed"
	MessageCypherMutationsResolveTargetNodeFailed             MessageID = "cyphermutations.resolve_target_node_failed"
	MessageCypherMutationsRelationshipPropertiesInvalid       MessageID = "cyphermutations.relationship_properties_invalid"
	MessageCypherMutationsVariableNotFound                    MessageID = "cyphermutations.variable_not_found"
	MessageCypherMutationsCreateWithDeleteInvalid             MessageID = "cyphermutations.create_with_delete_invalid"
	MessageCypherMutationsCreateFailed                        MessageID = "cyphermutations.create_failed"
	MessageCypherMutationsDeleteFailed                        MessageID = "cyphermutations.delete_failed"
	MessageCypherMutationsCreateSetClauseMissing              MessageID = "cyphermutations.create_set_clause_missing"
	MessageCypherMutationsCreateInCreateSetFailed             MessageID = "cyphermutations.create_in_create_set_failed"
	MessageCypherMutationsCreateSetWithContinuationRequired   MessageID = "cyphermutations.create_set_with_continuation_required"
	MessageCypherMutationsWithClauseEmpty                     MessageID = "cyphermutations.with_clause_empty"
	MessageCypherMutationsCreateSetTrailingClauseUnsupported  MessageID = "cyphermutations.create_set_trailing_clause_unsupported"
	MessageCypherMutationsSetMergeSyntaxInvalid               MessageID = "cyphermutations.set_merge_syntax_invalid"
	MessageCypherMutationsSetMergeParameterNameRequired       MessageID = "cyphermutations.set_merge_parameter_name_required"
	MessageCypherMutationsSetMergeParametersRequired          MessageID = "cyphermutations.set_merge_parameters_required"
	MessageCypherMutationsSetMergeParameterNotFound           MessageID = "cyphermutations.set_merge_parameter_not_found"
	MessageCypherMutationsSetMergeParseFailed                 MessageID = "cyphermutations.set_merge_parse_failed"
	MessageCypherMutationsSetMergeMapLiteralRequired          MessageID = "cyphermutations.set_merge_map_literal_required"
	MessageCypherMutationsSetMergeMapVariableNotFound         MessageID = "cyphermutations.set_merge_map_variable_not_found"
	MessageCypherMutationsUpdateNodeFailed                    MessageID = "cyphermutations.update_node_failed"
	MessageCypherMutationsUpdateEdgeFailed                    MessageID = "cyphermutations.update_edge_failed"
	MessageCypherMutationsUnknownSetMergeVariable             MessageID = "cyphermutations.unknown_set_merge_variable"
	MessageCypherMutationsRelationshipCreateFailed            MessageID = "cyphermutations.relationship_create_failed"
	MessageCypherMutationsNodeCreateFailed                    MessageID = "cyphermutations.node_create_failed"
	MessageCypherMutationsDeleteMatchRequired                 MessageID = "cyphermutations.delete_match_required"
	MessageCypherMutationsDeleteVariablesRequired             MessageID = "cyphermutations.delete_variables_required"
	MessageCypherMutationsDetachDeleteKeywordsRequired        MessageID = "cyphermutations.detach_delete_keywords_required"
	MessageCypherMutationsSetMatchRequired                    MessageID = "cyphermutations.set_match_required"
	MessageCypherMutationsSetAssignmentRequired               MessageID = "cyphermutations.set_assignment_required"
	MessageCypherMutationsSetMergeVariableRequired            MessageID = "cyphermutations.set_merge_variable_required"
	MessageCypherMutationsSetMergeMapScopeRequired            MessageID = "cyphermutations.set_merge_map_scope_required"
	MessageCypherMutationsSetMergeUpdateFailed                MessageID = "cyphermutations.set_merge_update_failed"
	MessageCypherMutationsSetEntityReplaceFailed              MessageID = "cyphermutations.set_entity_replace_failed"
	MessageCypherMutationsSetEntityRequired                   MessageID = "cyphermutations.set_entity_required"
	MessageCypherMutationsSetPropertyFailed                   MessageID = "cyphermutations.set_property_failed"
	MessageCypherMutationsSetPropertyEntityRequired           MessageID = "cyphermutations.set_property_entity_required"
	MessageCypherMutationsUnwindClauseExpected                MessageID = "cyphermutations.unwind_clause_expected"
	MessageCypherMutationsUnwindASRequired                    MessageID = "cyphermutations.unwind_as_required"
	MessageCypherMutationsUnwindVariableRequired              MessageID = "cyphermutations.unwind_variable_required"
	MessageCypherMutationsUnwindSetReturnRequired             MessageID = "cyphermutations.unwind_set_return_required"
	MessageCypherMutationsUnwindASVariableNonEmpty            MessageID = "cyphermutations.unwind_as_variable_non_empty"
	MessageCypherMutationsWithExpressionRequired              MessageID = "cyphermutations.with_expression_required"
	MessageCypherMutationsSetMergeOperatorExpected            MessageID = "cyphermutations.set_merge_operator_expected"
	MessageCypherMutationsSetMergeStringKeysRequired          MessageID = "cyphermutations.set_merge_string_keys_required"
	MessageCypherMutationsSetMergeMapRequired                 MessageID = "cyphermutations.set_merge_map_required"
	MessageCypherMutationsRemoveMatchRequired                 MessageID = "cyphermutations.remove_match_required"
	MessageCypherMutationsUnwindKeysUnsupported               MessageID = "cyphermutations.unwind_keys_unsupported"
	MessageCypherMutationsUnwindParameterNameRequired         MessageID = "cyphermutations.unwind_parameter_name_required"
	MessageCypherMutationsUnwindParametersRequired            MessageID = "cyphermutations.unwind_parameters_required"
	MessageCypherMutationsUnwindParameterNotFound             MessageID = "cyphermutations.unwind_parameter_not_found"
	MessageCypherMutationsUnwindMutationFailed                MessageID = "cyphermutations.unwind_mutation_failed"
	MessageCypherMutationsUnwindMatchFailed                   MessageID = "cyphermutations.unwind_match_failed"
	MessageCypherMutationsUnwindMergeCreateFailed             MessageID = "cyphermutations.unwind_merge_create_failed"
	MessageCypherMutationsUnwindMergeUpdateFailed             MessageID = "cyphermutations.unwind_merge_update_failed"
	MessageCypherMutationsUnwindMatchUpdateFailed             MessageID = "cyphermutations.unwind_match_update_failed"
	MessageCypherMutationsUnwindRelationshipLookupFailed      MessageID = "cyphermutations.unwind_relationship_lookup_failed"
	MessageCypherMutationsUnwindRelationshipAssignmentFailed  MessageID = "cyphermutations.unwind_relationship_assignment_failed"
	MessageCypherMutationsUnwindRelationshipCreateFailed      MessageID = "cyphermutations.unwind_relationship_create_failed"
	MessageCypherMutationsUnwindRelationshipUpdateFailed      MessageID = "cyphermutations.unwind_relationship_update_failed"
	MessageCypherMutationsUnwindFixedChainFailed              MessageID = "cyphermutations.unwind_fixed_chain_failed"
	MessageCypherMutationsUnwindClauseMissing                 MessageID = "cyphermutations.unwind_clause_missing"
	MessageCypherMutationsUnwindFirstASRequired               MessageID = "cyphermutations.unwind_first_as_required"
	MessageCypherMutationsUnwindDoubleMalformed               MessageID = "cyphermutations.unwind_double_malformed"
	MessageCypherMutationsUnwindSecondExpected                MessageID = "cyphermutations.unwind_second_expected"
	MessageCypherMutationsUnwindSecondASRequired              MessageID = "cyphermutations.unwind_second_as_required"
	MessageCypherMutationsWithOptionalReturnRequired          MessageID = "cyphermutations.with_optional_return_required"
	MessageCypherMutationsWithReturnRequired                  MessageID = "cyphermutations.with_return_required"
	MessageCypherMutationsForeachParenthesesRequired          MessageID = "cyphermutations.foreach_parentheses_required"
	MessageCypherMutationsForeachBalancedParenthesesRequired  MessageID = "cyphermutations.foreach_balanced_parentheses_required"
	MessageCypherMutationsForeachInRequired                   MessageID = "cyphermutations.foreach_in_required"
	MessageCypherMutationsForeachSeparatorRequired            MessageID = "cyphermutations.foreach_separator_required"
)

// CypherMutationsInvalidLabelName identifies an invalid CREATE or SET label.
func CypherMutationsInvalidLabelName(label string) Message {
	return Message{
		ID:       MessageCypherMutationsInvalidLabelName,
		Fallback: "invalid label name: " + strconv.Quote(label) + " (must be alphanumeric starting with letter or underscore)",
		Data:     map[string]any{"Label": label},
	}
}

// CypherMutationsCreateNodeFailed identifies a wrapped node creation failure.
func CypherMutationsCreateNodeFailed(cause error) Message {
	return Message{
		ID:       MessageCypherMutationsCreateNodeFailed,
		Fallback: "failed to create node: " + cause.Error(),
		Data:     map[string]any{"Cause": cause.Error()},
	}
}

func cypherMutationsMessage(id MessageID, fallback string, data map[string]any) Message {
	return Message{ID: id, Fallback: fallback, Data: data}
}

func cypherMutationsCauseMessage(id MessageID, prefix string, cause error) Message {
	return cypherMutationsMessage(id, prefix+cause.Error(), map[string]any{"Cause": cause.Error()})
}

// CypherMutationsInvalidLabelReserved identifies a reserved label identifier.
func CypherMutationsInvalidLabelReserved(label string) Message {
	return cypherMutationsMessage(MessageCypherMutationsInvalidLabelReserved, fmt.Sprintf("invalid label name: %q (contains reserved keyword)", label), map[string]any{"Label": label})
}

// CypherMutationsInvalidPropertyKey identifies an invalid node property key.
func CypherMutationsInvalidPropertyKey(property string) Message {
	return cypherMutationsMessage(MessageCypherMutationsInvalidPropertyKey, fmt.Sprintf("invalid property key: %q (must be alphanumeric starting with letter or underscore)", property), map[string]any{"Property": property})
}

// CypherMutationsInvalidPropertyValue identifies malformed property syntax.
func CypherMutationsInvalidPropertyValue(property string) Message {
	return cypherMutationsMessage(MessageCypherMutationsInvalidPropertyValue, fmt.Sprintf("invalid property value for key %q: malformed syntax", property), map[string]any{"Property": property})
}

// CypherMutationsCreateSourceNodeFailed identifies a wrapped source-node creation failure.
func CypherMutationsCreateSourceNodeFailed(cause error) Message {
	return cypherMutationsCauseMessage(MessageCypherMutationsCreateSourceNodeFailed, "failed to create source node: ", cause)
}

// CypherMutationsCreateTargetNodeFailed identifies a wrapped target-node creation failure.
func CypherMutationsCreateTargetNodeFailed(cause error) Message {
	return cypherMutationsCauseMessage(MessageCypherMutationsCreateTargetNodeFailed, "failed to create target node: ", cause)
}

// CypherMutationsInvalidRelationshipType identifies an invalid relationship type.
func CypherMutationsInvalidRelationshipType(relationshipType string) Message {
	return cypherMutationsMessage(MessageCypherMutationsInvalidRelationshipType, fmt.Sprintf("invalid relationship type: %q (must be alphanumeric starting with letter or underscore)", relationshipType), map[string]any{"RelationshipType": relationshipType})
}

// CypherMutationsInvalidRelationshipPropertyKey identifies an invalid relationship property key.
func CypherMutationsInvalidRelationshipPropertyKey(property string) Message {
	return cypherMutationsMessage(MessageCypherMutationsInvalidRelationshipPropertyKey, fmt.Sprintf("invalid relationship property key: %q (must be alphanumeric starting with letter or underscore)", property), map[string]any{"Property": property})
}

// CypherMutationsCreateRelationshipFailed identifies a wrapped relationship creation failure.
func CypherMutationsCreateRelationshipFailed(cause error) Message {
	return cypherMutationsCauseMessage(MessageCypherMutationsCreateRelationshipFailed, "failed to create relationship: ", cause)
}

// CypherMutationsInvalidPropertyMapCause identifies malformed CREATE property syntax.
func CypherMutationsInvalidPropertyMapCause(cause error) Message {
	return cypherMutationsCauseMessage(MessageCypherMutationsInvalidPropertyMapCause, "invalid property map syntax in pattern: ", cause)
}

// CypherMutationsRelationshipPatternMustStartNode identifies a missing initial node pattern.
func CypherMutationsRelationshipPatternMustStartNode() Message {
	return cypherMutationsMessage(MessageCypherMutationsRelationshipPatternMustStartNode, "invalid relationship pattern: must start with (", nil)
}

// CypherMutationsRelationshipPatternUnmatchedParen identifies an unmatched first node parenthesis.
func CypherMutationsRelationshipPatternUnmatchedParen() Message {
	return cypherMutationsMessage(MessageCypherMutationsRelationshipPatternUnmatchedParen, "invalid relationship pattern: unmatched parenthesis", nil)
}

// CypherMutationsRelationshipPatternUnmatchedBracket identifies an unmatched relationship bracket.
func CypherMutationsRelationshipPatternUnmatchedBracket() Message {
	return cypherMutationsMessage(MessageCypherMutationsRelationshipPatternUnmatchedBracket, "invalid relationship pattern: unmatched bracket", nil)
}

// CypherMutationsRelationshipPatternForwardExpected identifies a missing forward target node.
func CypherMutationsRelationshipPatternForwardExpected() Message {
	return cypherMutationsMessage(MessageCypherMutationsRelationshipPatternForwardExpected, "invalid relationship pattern: expected -( after ]", nil)
}

// CypherMutationsRelationshipPatternArrowExpected identifies a missing arrow target node.
func CypherMutationsRelationshipPatternArrowExpected() Message {
	return cypherMutationsMessage(MessageCypherMutationsRelationshipPatternArrowExpected, "invalid relationship pattern: expected ->( after ]", nil)
}

// CypherMutationsRelationshipPatternSecondUnmatched identifies an unmatched target-node parenthesis.
func CypherMutationsRelationshipPatternSecondUnmatched() Message {
	return cypherMutationsMessage(MessageCypherMutationsRelationshipPatternSecondUnmatched, "invalid relationship pattern: unmatched parenthesis for second node", nil)
}

// CypherMutationsMatchLabelLookupFailed identifies a failed MATCH label lookup.
func CypherMutationsMatchLabelLookupFailed(label string, cause error) Message {
	return cypherMutationsMessage(MessageCypherMutationsMatchLabelLookupFailed, fmt.Sprintf("failed to get nodes by label %q in MATCH segment: %s", label, cause), map[string]any{"Label": label, "Cause": cause.Error()})
}

// CypherMutationsMatchAllNodesFailed identifies a failed MATCH node scan.
func CypherMutationsMatchAllNodesFailed(cause error) Message {
	return cypherMutationsCauseMessage(MessageCypherMutationsMatchAllNodesFailed, "failed to get all nodes in MATCH segment: ", cause)
}

// CypherMutationsRelationshipExistenceCheckFailed identifies a failed relationship lookup.
func CypherMutationsRelationshipExistenceCheckFailed(startVariable, relationshipType, endVariable string, cause error) Message {
	return cypherMutationsMessage(MessageCypherMutationsRelationshipExistenceCheckFailed, fmt.Sprintf("failed relationship existence check for %s-[:%s]->%s: %s", startVariable, relationshipType, endVariable, cause), map[string]any{"StartVariable": startVariable, "RelationshipType": relationshipType, "EndVariable": endVariable, "Cause": cause.Error()})
}

// CypherMutationsAddLabelFailed identifies a wrapped label update failure.
func CypherMutationsAddLabelFailed(cause error) Message {
	return cypherMutationsCauseMessage(MessageCypherMutationsAddLabelFailed, "failed to add label: ", cause)
}

// CypherMutationsSetAssignmentParameterNameRequired identifies an empty SET parameter name.
func CypherMutationsSetAssignmentParameterNameRequired() Message {
	return cypherMutationsMessage(MessageCypherMutationsSetAssignmentParameterNameRequired, "SET assignment requires a valid parameter name after $", nil)
}

// CypherMutationsSetAssignmentParametersRequired identifies missing SET parameters.
func CypherMutationsSetAssignmentParametersRequired(parameter string) Message {
	return cypherMutationsMessage(MessageCypherMutationsSetAssignmentParametersRequired, "SET assignment parameter $"+parameter+" requires parameters to be provided", map[string]any{"Parameter": parameter})
}

// CypherMutationsSetAssignmentParameterNotFound identifies an unknown SET parameter.
func CypherMutationsSetAssignmentParameterNotFound(parameter string) Message {
	return cypherMutationsMessage(MessageCypherMutationsSetAssignmentParameterNotFound, "SET assignment parameter $"+parameter+" not found in provided parameters", map[string]any{"Parameter": parameter})
}

// CypherMutationsReplaceNodePropertiesFailed identifies a wrapped node map replacement failure.
func CypherMutationsReplaceNodePropertiesFailed(cause error) Message {
	return cypherMutationsCauseMessage(MessageCypherMutationsReplaceNodePropertiesFailed, "failed to replace node properties: ", cause)
}

// CypherMutationsReplaceEdgePropertiesFailed identifies a wrapped relationship map replacement failure.
func CypherMutationsReplaceEdgePropertiesFailed(cause error) Message {
	return cypherMutationsCauseMessage(MessageCypherMutationsReplaceEdgePropertiesFailed, "failed to replace edge properties: ", cause)
}

// CypherMutationsUnknownSetVariable identifies an unknown SET variable.
func CypherMutationsUnknownSetVariable(variable string) Message {
	return cypherMutationsMessage(MessageCypherMutationsUnknownSetVariable, "unknown variable in SET clause: "+variable, map[string]any{"Variable": variable})
}

// CypherMutationsUpdateNodePropertyFailed identifies a wrapped node property update failure.
func CypherMutationsUpdateNodePropertyFailed(cause error) Message {
	return cypherMutationsCauseMessage(MessageCypherMutationsUpdateNodePropertyFailed, "failed to update node property: ", cause)
}

// CypherMutationsUpdateEdgePropertyFailed identifies a wrapped relationship property update failure.
func CypherMutationsUpdateEdgePropertyFailed(cause error) Message {
	return cypherMutationsCauseMessage(MessageCypherMutationsUpdateEdgePropertyFailed, "failed to update edge property: ", cause)
}

// CypherMutationsResolveSourceNodeFailed identifies a wrapped source resolution failure.
func CypherMutationsResolveSourceNodeFailed(cause error) Message {
	return cypherMutationsCauseMessage(MessageCypherMutationsResolveSourceNodeFailed, "failed to resolve source node: ", cause)
}

// CypherMutationsResolveTargetNodeFailed identifies a wrapped target resolution failure.
func CypherMutationsResolveTargetNodeFailed(cause error) Message {
	return cypherMutationsCauseMessage(MessageCypherMutationsResolveTargetNodeFailed, "failed to resolve target node: ", cause)
}

// CypherMutationsRelationshipPropertiesInvalid identifies malformed relationship properties.
func CypherMutationsRelationshipPropertiesInvalid() Message {
	return cypherMutationsMessage(MessageCypherMutationsRelationshipPropertiesInvalid, "invalid relationship properties", nil)
}

// CypherMutationsVariableNotFound identifies an unresolved CREATE variable.
func CypherMutationsVariableNotFound(variable, available string) Message {
	return cypherMutationsMessage(MessageCypherMutationsVariableNotFound, fmt.Sprintf("variable '%s' not found (have: %s)", variable, available), map[string]any{"Variable": variable, "Available": available})
}

// CypherMutationsCreateWithDeleteInvalid identifies an invalid CREATE/WITH/DELETE pipeline.
func CypherMutationsCreateWithDeleteInvalid() Message {
	return cypherMutationsMessage(MessageCypherMutationsCreateWithDeleteInvalid, "invalid CREATE...WITH...DELETE query", nil)
}

// CypherMutationsCreateFailed identifies a wrapped CREATE pipeline failure.
func CypherMutationsCreateFailed(cause error) Message {
	return cypherMutationsCauseMessage(MessageCypherMutationsCreateFailed, "CREATE failed: ", cause)
}

// CypherMutationsDeleteFailed identifies a wrapped DELETE pipeline failure.
func CypherMutationsDeleteFailed(cause error) Message {
	return cypherMutationsCauseMessage(MessageCypherMutationsDeleteFailed, "DELETE failed: ", cause)
}

// CypherMutationsCreateSetClauseMissing identifies a missing SET clause.
func CypherMutationsCreateSetClauseMissing() Message {
	return cypherMutationsMessage(MessageCypherMutationsCreateSetClauseMissing, "SET clause not found in CREATE...SET query", nil)
}

// CypherMutationsCreateInCreateSetFailed identifies a wrapped CREATE/SET failure.
func CypherMutationsCreateInCreateSetFailed(cause error) Message {
	return cypherMutationsCauseMessage(MessageCypherMutationsCreateInCreateSetFailed, "CREATE failed in CREATE...SET: ", cause)
}

// CypherMutationsCreateSetWithContinuationRequired identifies a terminal WITH in CREATE/SET.
func CypherMutationsCreateSetWithContinuationRequired() Message {
	return cypherMutationsMessage(MessageCypherMutationsCreateSetWithContinuationRequired, "WITH in CREATE...SET requires CREATE or RETURN clause", nil)
}

// CypherMutationsWithClauseEmpty identifies an empty WITH projection.
func CypherMutationsWithClauseEmpty() Message {
	return cypherMutationsMessage(MessageCypherMutationsWithClauseEmpty, "WITH clause cannot be empty", nil)
}

// CypherMutationsCreateSetTrailingClauseUnsupported identifies an unsupported post-SET clause.
func CypherMutationsCreateSetTrailingClauseUnsupported(clause string) Message {
	return cypherMutationsMessage(MessageCypherMutationsCreateSetTrailingClauseUnsupported, "unsupported clause after SET in CREATE...SET query: "+clause, map[string]any{"Clause": clause})
}

// CypherMutationsSetMergeSyntaxInvalid identifies malformed SET += syntax.
func CypherMutationsSetMergeSyntaxInvalid() Message {
	return cypherMutationsMessage(MessageCypherMutationsSetMergeSyntaxInvalid, "invalid SET += syntax", nil)
}

// CypherMutationsSetMergeParameterNameRequired identifies an empty SET += parameter name.
func CypherMutationsSetMergeParameterNameRequired() Message {
	return cypherMutationsMessage(MessageCypherMutationsSetMergeParameterNameRequired, "SET += requires a valid parameter name after $", nil)
}

// CypherMutationsSetMergeParametersRequired identifies missing SET += parameters.
func CypherMutationsSetMergeParametersRequired(parameter string) Message {
	return cypherMutationsMessage(MessageCypherMutationsSetMergeParametersRequired, "SET += parameter $"+parameter+" requires parameters to be provided", map[string]any{"Parameter": parameter})
}

// CypherMutationsSetMergeParameterNotFound identifies an unknown SET += parameter.
func CypherMutationsSetMergeParameterNotFound(parameter string) Message {
	return cypherMutationsMessage(MessageCypherMutationsSetMergeParameterNotFound, "SET += parameter $"+parameter+" not found in provided parameters", map[string]any{"Parameter": parameter})
}

// CypherMutationsSetMergeParseFailed identifies a wrapped SET += map parse failure.
func CypherMutationsSetMergeParseFailed(cause error) Message {
	return cypherMutationsCauseMessage(MessageCypherMutationsSetMergeParseFailed, "failed to parse properties in SET +=: ", cause)
}

// CypherMutationsSetMergeMapLiteralRequired identifies a non-map SET += literal.
func CypherMutationsSetMergeMapLiteralRequired() Message {
	return cypherMutationsMessage(MessageCypherMutationsSetMergeMapLiteralRequired, "failed to parse properties in SET +=: map literal must be enclosed in { ... }", nil)
}

// CypherMutationsSetMergeMapVariableNotFound identifies a missing SET += map variable.
func CypherMutationsSetMergeMapVariableNotFound(variable string) Message {
	return cypherMutationsMessage(MessageCypherMutationsSetMergeMapVariableNotFound, fmt.Sprintf("SET += map variable %q not found in scope", variable), map[string]any{"Variable": variable})
}

// CypherMutationsUpdateNodeFailed identifies a wrapped node update failure.
func CypherMutationsUpdateNodeFailed(cause error) Message {
	return cypherMutationsCauseMessage(MessageCypherMutationsUpdateNodeFailed, "failed to update node: ", cause)
}

// CypherMutationsUpdateEdgeFailed identifies a wrapped relationship update failure.
func CypherMutationsUpdateEdgeFailed(cause error) Message {
	return cypherMutationsCauseMessage(MessageCypherMutationsUpdateEdgeFailed, "failed to update edge: ", cause)
}

// CypherMutationsUnknownSetMergeVariable identifies an unknown SET += variable.
func CypherMutationsUnknownSetMergeVariable(variable string) Message {
	return cypherMutationsMessage(MessageCypherMutationsUnknownSetMergeVariable, "unknown variable in SET +=: "+variable, map[string]any{"Variable": variable})
}

// CypherMutationsRelationshipCreateFailed identifies a wrapped relationship CREATE segment failure.
func CypherMutationsRelationshipCreateFailed(cause error) Message {
	return cypherMutationsCauseMessage(MessageCypherMutationsRelationshipCreateFailed, "relationship CREATE failed: ", cause)
}

// CypherMutationsNodeCreateFailed identifies a wrapped node CREATE segment failure.
func CypherMutationsNodeCreateFailed(cause error) Message {
	return cypherMutationsCauseMessage(MessageCypherMutationsNodeCreateFailed, "node CREATE failed: ", cause)
}

// CypherMutationsDeleteMatchRequired identifies DELETE without MATCH.
func CypherMutationsDeleteMatchRequired() Message {
	return cypherMutationsMessage(MessageCypherMutationsDeleteMatchRequired, "DELETE requires a MATCH clause first (e.g., MATCH (n) DELETE n)", nil)
}

// CypherMutationsDeleteVariablesRequired identifies DELETE without a target.
func CypherMutationsDeleteVariablesRequired() Message {
	return cypherMutationsMessage(MessageCypherMutationsDeleteVariablesRequired, "DELETE clause must specify variable(s) to delete (e.g., DELETE n)", nil)
}

// CypherMutationsDetachDeleteKeywordsRequired identifies incomplete DETACH DELETE syntax.
func CypherMutationsDetachDeleteKeywordsRequired() Message {
	return cypherMutationsMessage(MessageCypherMutationsDetachDeleteKeywordsRequired, "DETACH DELETE requires both DETACH and DELETE keywords together", nil)
}

// CypherMutationsSetMatchRequired identifies SET without MATCH.
func CypherMutationsSetMatchRequired() Message {
	return cypherMutationsMessage(MessageCypherMutationsSetMatchRequired, "SET requires a MATCH clause first (e.g., MATCH (n) SET n.property = value)", nil)
}

// CypherMutationsSetAssignmentRequired identifies SET without assignments.
func CypherMutationsSetAssignmentRequired() Message {
	return cypherMutationsMessage(MessageCypherMutationsSetAssignmentRequired, "SET clause requires at least one assignment", nil)
}

// CypherMutationsSetMergeVariableRequired identifies SET += without a target.
func CypherMutationsSetMergeVariableRequired() Message {
	return cypherMutationsMessage(MessageCypherMutationsSetMergeVariableRequired, "SET += requires a variable target", nil)
}

// CypherMutationsSetMergeMapScopeRequired identifies a missing row-scope map.
func CypherMutationsSetMergeMapScopeRequired(variable string) Message {
	return cypherMutationsMessage(MessageCypherMutationsSetMergeMapScopeRequired, fmt.Sprintf("SET += requires a map variable in scope (missing %q)", variable), map[string]any{"Variable": variable})
}

// CypherMutationsSetMergeUpdateFailed identifies a wrapped SET += entity update.
func CypherMutationsSetMergeUpdateFailed(variable string, cause error) Message {
	return cypherMutationsMessage(MessageCypherMutationsSetMergeUpdateFailed, fmt.Sprintf("SET %s +=: %s", variable, cause), map[string]any{"Variable": variable, "Cause": cause.Error()})
}

// CypherMutationsSetEntityReplaceFailed identifies a wrapped SET variable map replacement.
func CypherMutationsSetEntityReplaceFailed(variable string, cause error) Message {
	return cypherMutationsMessage(MessageCypherMutationsSetEntityReplaceFailed, fmt.Sprintf("SET %s =: %s", variable, cause), map[string]any{"Variable": variable, "Cause": cause.Error()})
}

// CypherMutationsSetEntityRequired identifies SET map replacement on a scalar.
func CypherMutationsSetEntityRequired(variable string) Message {
	return cypherMutationsMessage(MessageCypherMutationsSetEntityRequired, "SET "+variable+" = requires a node or relationship", map[string]any{"Variable": variable})
}

// CypherMutationsSetPropertyFailed identifies a wrapped SET property update.
func CypherMutationsSetPropertyFailed(variable, property string, cause error) Message {
	return cypherMutationsMessage(MessageCypherMutationsSetPropertyFailed, fmt.Sprintf("SET %s.%s: %s", variable, property, cause), map[string]any{"Variable": variable, "Property": property, "Cause": cause.Error()})
}

// CypherMutationsSetPropertyEntityRequired identifies SET property on a scalar.
func CypherMutationsSetPropertyEntityRequired(variable, property string) Message {
	return cypherMutationsMessage(MessageCypherMutationsSetPropertyEntityRequired, "SET "+variable+"."+property+" requires a node or relationship", map[string]any{"Variable": variable, "Property": property})
}

// CypherMutationsUnwindClauseExpected identifies a missing trailing UNWIND clause.
func CypherMutationsUnwindClauseExpected() Message {
	return cypherMutationsMessage(MessageCypherMutationsUnwindClauseExpected, "UNWIND clause expected", nil)
}

// CypherMutationsUnwindASRequired identifies UNWIND without AS.
func CypherMutationsUnwindASRequired() Message {
	return cypherMutationsMessage(MessageCypherMutationsUnwindASRequired, "UNWIND requires AS clause (e.g., UNWIND [1,2,3] AS x)", nil)
}

// CypherMutationsUnwindVariableRequired identifies a missing variable after AS.
func CypherMutationsUnwindVariableRequired() Message {
	return cypherMutationsMessage(MessageCypherMutationsUnwindVariableRequired, "UNWIND requires a variable after AS", nil)
}

// CypherMutationsUnwindSetReturnRequired identifies SET/UNWIND without RETURN.
func CypherMutationsUnwindSetReturnRequired() Message {
	return cypherMutationsMessage(MessageCypherMutationsUnwindSetReturnRequired, "UNWIND in SET query requires RETURN clause", nil)
}

// CypherMutationsUnwindASVariableNonEmpty identifies an empty AS variable.
func CypherMutationsUnwindASVariableNonEmpty() Message {
	return cypherMutationsMessage(MessageCypherMutationsUnwindASVariableNonEmpty, "UNWIND requires a non-empty AS variable", nil)
}

// CypherMutationsWithExpressionRequired identifies WITH without projection expressions.
func CypherMutationsWithExpressionRequired() Message {
	return cypherMutationsMessage(MessageCypherMutationsWithExpressionRequired, "WITH clause requires at least one expression", nil)
}

// CypherMutationsSetMergeOperatorExpected identifies a missing += operator.
func CypherMutationsSetMergeOperatorExpected() Message {
	return cypherMutationsMessage(MessageCypherMutationsSetMergeOperatorExpected, "expected += operator", nil)
}

// CypherMutationsSetMergeStringKeysRequired identifies a non-string SET += map key.
func CypherMutationsSetMergeStringKeysRequired(source, keyType string) Message {
	return cypherMutationsMessage(MessageCypherMutationsSetMergeStringKeysRequired, "SET += "+source+" must be a map with string keys, got key type "+keyType, map[string]any{"Source": source, "KeyType": keyType})
}

// CypherMutationsSetMergeMapRequired identifies a non-map SET += source.
func CypherMutationsSetMergeMapRequired(source, valueType string) Message {
	return cypherMutationsMessage(MessageCypherMutationsSetMergeMapRequired, "SET += "+source+" must be a map, got type "+valueType, map[string]any{"Source": source, "ValueType": valueType})
}

// CypherMutationsRemoveMatchRequired identifies REMOVE without MATCH.
func CypherMutationsRemoveMatchRequired() Message {
	return cypherMutationsMessage(MessageCypherMutationsRemoveMatchRequired, "REMOVE requires a MATCH clause first (e.g., MATCH (n) REMOVE n.property)", nil)
}

// CypherMutationsUnwindKeysUnsupported identifies unsupported keys() use with UNWIND.
func CypherMutationsUnwindKeysUnsupported() Message {
	return cypherMutationsMessage(MessageCypherMutationsUnwindKeysUnsupported, "keys() function with UNWIND is not supported in this context", nil)
}

// CypherMutationsUnwindParameterNameRequired identifies an empty UNWIND parameter name.
func CypherMutationsUnwindParameterNameRequired() Message {
	return cypherMutationsMessage(MessageCypherMutationsUnwindParameterNameRequired, "UNWIND requires a valid parameter name after $", nil)
}

// CypherMutationsUnwindParametersRequired identifies missing UNWIND parameters.
func CypherMutationsUnwindParametersRequired(parameter string) Message {
	return cypherMutationsMessage(MessageCypherMutationsUnwindParametersRequired, "UNWIND parameter $"+parameter+" requires parameters to be provided", map[string]any{"Parameter": parameter})
}

// CypherMutationsUnwindParameterNotFound identifies an unknown UNWIND parameter.
func CypherMutationsUnwindParameterNotFound(parameter string) Message {
	return cypherMutationsMessage(MessageCypherMutationsUnwindParameterNotFound, "UNWIND parameter $"+parameter+" not found in provided parameters", map[string]any{"Parameter": parameter})
}

// CypherMutationsUnwindMutationFailed identifies a wrapped UNWIND mutation failure.
func CypherMutationsUnwindMutationFailed(cause error) Message {
	return cypherMutationsCauseMessage(MessageCypherMutationsUnwindMutationFailed, "UNWIND mutation failed: ", cause)
}

// CypherMutationsUnwindMatchFailed identifies a wrapped UNWIND MATCH failure.
func CypherMutationsUnwindMatchFailed(cause error) Message {
	return cypherMutationsCauseMessage(MessageCypherMutationsUnwindMatchFailed, "UNWIND MATCH failed: ", cause)
}

// CypherMutationsUnwindMergeCreateFailed identifies a wrapped UNWIND MERGE node create failure.
func CypherMutationsUnwindMergeCreateFailed(cause error) Message {
	return cypherMutationsCauseMessage(MessageCypherMutationsUnwindMergeCreateFailed, "UNWIND MERGE chain create failed: ", cause)
}

// CypherMutationsUnwindMergeUpdateFailed identifies a wrapped UNWIND MERGE node update failure.
func CypherMutationsUnwindMergeUpdateFailed(cause error) Message {
	return cypherMutationsCauseMessage(MessageCypherMutationsUnwindMergeUpdateFailed, "UNWIND MERGE chain update failed: ", cause)
}

// CypherMutationsUnwindMatchUpdateFailed identifies a wrapped UNWIND MATCH update failure.
func CypherMutationsUnwindMatchUpdateFailed(cause error) Message {
	return cypherMutationsCauseMessage(MessageCypherMutationsUnwindMatchUpdateFailed, "UNWIND MATCH chain update failed: ", cause)
}

// CypherMutationsUnwindRelationshipLookupFailed identifies a wrapped relationship lookup failure.
func CypherMutationsUnwindRelationshipLookupFailed(cause error) Message {
	return cypherMutationsCauseMessage(MessageCypherMutationsUnwindRelationshipLookupFailed, "UNWIND MERGE chain relationship lookup failed: ", cause)
}

// CypherMutationsUnwindRelationshipAssignmentFailed identifies a wrapped relationship assignment failure.
func CypherMutationsUnwindRelationshipAssignmentFailed(cause error) Message {
	return cypherMutationsCauseMessage(MessageCypherMutationsUnwindRelationshipAssignmentFailed, "UNWIND MERGE chain relationship assignment failed: ", cause)
}

// CypherMutationsUnwindRelationshipCreateFailed identifies a wrapped relationship creation failure.
func CypherMutationsUnwindRelationshipCreateFailed(cause error) Message {
	return cypherMutationsCauseMessage(MessageCypherMutationsUnwindRelationshipCreateFailed, "UNWIND MERGE chain relationship create failed: ", cause)
}

// CypherMutationsUnwindRelationshipUpdateFailed identifies a wrapped relationship update failure.
func CypherMutationsUnwindRelationshipUpdateFailed(cause error) Message {
	return cypherMutationsCauseMessage(MessageCypherMutationsUnwindRelationshipUpdateFailed, "UNWIND MERGE chain relationship update failed: ", cause)
}

// CypherMutationsUnwindFixedChainFailed identifies a wrapped fixed-chain merge failure.
func CypherMutationsUnwindFixedChainFailed(cause error) Message {
	return cypherMutationsCauseMessage(MessageCypherMutationsUnwindFixedChainFailed, "UNWIND fixed-chain merge failed: ", cause)
}

// CypherMutationsUnwindClauseMissing identifies a missing UNWIND clause.
func CypherMutationsUnwindClauseMissing() Message {
	return cypherMutationsMessage(MessageCypherMutationsUnwindClauseMissing, "UNWIND clause not found", nil)
}

// CypherMutationsUnwindFirstASRequired identifies a first UNWIND without AS.
func CypherMutationsUnwindFirstASRequired() Message {
	return cypherMutationsMessage(MessageCypherMutationsUnwindFirstASRequired, "first UNWIND requires AS clause", nil)
}

// CypherMutationsUnwindDoubleMalformed identifies malformed double UNWIND syntax.
func CypherMutationsUnwindDoubleMalformed() Message {
	return cypherMutationsMessage(MessageCypherMutationsUnwindDoubleMalformed, "malformed double UNWIND", nil)
}

// CypherMutationsUnwindSecondExpected identifies a missing second UNWIND.
func CypherMutationsUnwindSecondExpected() Message {
	return cypherMutationsMessage(MessageCypherMutationsUnwindSecondExpected, "expected second UNWIND", nil)
}

// CypherMutationsUnwindSecondASRequired identifies a second UNWIND without AS.
func CypherMutationsUnwindSecondASRequired() Message {
	return cypherMutationsMessage(MessageCypherMutationsUnwindSecondASRequired, "second UNWIND requires AS clause", nil)
}

// CypherMutationsWithOptionalReturnRequired identifies an incomplete WITH/OPTIONAL MATCH pipeline.
func CypherMutationsWithOptionalReturnRequired() Message {
	return cypherMutationsMessage(MessageCypherMutationsWithOptionalReturnRequired, "WITH, OPTIONAL MATCH, and RETURN clauses required", nil)
}

// CypherMutationsWithReturnRequired identifies WITH aggregation without RETURN.
func CypherMutationsWithReturnRequired() Message {
	return cypherMutationsMessage(MessageCypherMutationsWithReturnRequired, "RETURN clause required after WITH", nil)
}

// CypherMutationsForeachParenthesesRequired identifies FOREACH without parentheses.
func CypherMutationsForeachParenthesesRequired() Message {
	return cypherMutationsMessage(MessageCypherMutationsForeachParenthesesRequired, "FOREACH requires parentheses (e.g., FOREACH (x IN list | SET ...))", nil)
}

// CypherMutationsForeachBalancedParenthesesRequired identifies unbalanced FOREACH parentheses.
func CypherMutationsForeachBalancedParenthesesRequired() Message {
	return cypherMutationsMessage(MessageCypherMutationsForeachBalancedParenthesesRequired, "FOREACH requires balanced parentheses", nil)
}

// CypherMutationsForeachInRequired identifies FOREACH without IN.
func CypherMutationsForeachInRequired() Message {
	return cypherMutationsMessage(MessageCypherMutationsForeachInRequired, "FOREACH requires IN clause (e.g., FOREACH (x IN list | SET ...))", nil)
}

// CypherMutationsForeachSeparatorRequired identifies FOREACH without its update separator.
func CypherMutationsForeachSeparatorRequired() Message {
	return cypherMutationsMessage(MessageCypherMutationsForeachSeparatorRequired, "FOREACH requires | separator", nil)
}
