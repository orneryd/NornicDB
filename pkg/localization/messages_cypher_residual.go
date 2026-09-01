package localization

import "fmt"

const (
	MessageCypherResidualWithClauseNotFound             MessageID = "cypherresidual.with_clause_not_found"
	MessageCypherResidualUnwindClauseNotFound           MessageID = "cypherresidual.unwind_clause_not_found"
	MessageCypherResidualUnionClauseNotFound            MessageID = "cypherresidual.union_clause_not_found"
	MessageCypherResidualUnionAllClauseNotFound         MessageID = "cypherresidual.union_all_clause_not_found"
	MessageCypherResidualUnionBranchFailed              MessageID = "cypherresidual.union_branch_failed"
	MessageCypherResidualUnionColumnCountMismatch       MessageID = "cypherresidual.union_column_count_mismatch"
	MessageCypherResidualOptionalMatchNotFound          MessageID = "cypherresidual.optional_match_not_found"
	MessageCypherResidualCompoundOptionalMatchNotFound  MessageID = "cypherresidual.compound_optional_match_not_found"
	MessageCypherResidualMatchNodePatternParseFailed    MessageID = "cypherresidual.match_node_pattern_parse_failed"
	MessageCypherResidualInitialNodesLookupFailed       MessageID = "cypherresidual.initial_nodes_lookup_failed"
	MessageCypherResidualSumArithmeticTermUnsupported   MessageID = "cypherresidual.sum_arithmetic_term_unsupported"
	MessageCypherResidualSumNumericRequired             MessageID = "cypherresidual.sum_numeric_required"
	MessageCypherResidualReturnClauseRequired           MessageID = "cypherresidual.return_clause_required"
	MessageCypherResidualForeachClauseNotFound          MessageID = "cypherresidual.foreach_clause_not_found"
	MessageCypherResidualLoadCSVUnsupported             MessageID = "cypherresidual.load_csv_unsupported"
	MessageCypherResidualEmptyLabelAfterColon           MessageID = "cypherresidual.empty_label_after_colon"
	MessageCypherResidualPropertyMapSyntaxInvalid       MessageID = "cypherresidual.property_map_syntax_invalid"
	MessageCypherResidualRelationshipConnectorExpected  MessageID = "cypherresidual.relationship_connector_expected"
	MessageCypherResidualCreateRelationshipInvalid      MessageID = "cypherresidual.create_relationship_invalid"
	MessageCypherResidualWithItemInvalid                MessageID = "cypherresidual.with_item_invalid"
	MessageCypherResidualCreateSetScopeEntityRequired   MessageID = "cypherresidual.create_set_scope_entity_required"
	MessageCypherResidualCreateWithExpressionInvalid    MessageID = "cypherresidual.create_with_expression_invalid"
	MessageCypherResidualMergePatternInvalid            MessageID = "cypherresidual.merge_pattern_invalid"
	MessageCypherResidualSetMergeMapOrParameterRequired MessageID = "cypherresidual.set_merge_map_or_parameter_required"
	MessageCypherResidualSetAssignmentInvalid           MessageID = "cypherresidual.set_assignment_invalid"
	MessageCypherResidualSetEntityAssignmentInvalid     MessageID = "cypherresidual.set_entity_assignment_invalid"
	MessageCypherResidualCollectSubquerySyntaxInvalid   MessageID = "cypherresidual.collect_subquery_syntax_invalid"
	MessageCypherResidualCollectSubqueryReturnRequired  MessageID = "cypherresidual.collect_subquery_return_required"
	MessageCypherResidualCollectSubqueryExecutionFailed MessageID = "cypherresidual.collect_subquery_execution_failed"
	MessageCypherResidualPolicyDisallowed               MessageID = "cypherresidual.policy_disallowed"
	MessageCypherResidualPolicyAllowedRequired          MessageID = "cypherresidual.policy_allowed_required"
	MessageCypherResidualCompositeShowTargetRequired    MessageID = "cypherresidual.composite_show_target_required"
	MessageCypherResidualShowAliasesManagerUnavailable  MessageID = "cypherresidual.show_aliases_manager_unavailable"
	MessageCypherResidualShowAliasesSyntaxInvalid       MessageID = "cypherresidual.show_aliases_syntax_invalid"
)

func cypherResidualMessage(id MessageID, fallback string, data map[string]any) Message {
	return Message{ID: id, Fallback: fallback, Data: data}
}

// CypherResidualWithClauseNotFound identifies a routed query missing WITH.
func CypherResidualWithClauseNotFound(query string) Message {
	return cypherResidualMessage(MessageCypherResidualWithClauseNotFound, fmt.Sprintf("WITH clause not found in query: %q", query), map[string]any{"Query": query})
}

// CypherResidualUnwindClauseNotFound identifies a routed query missing UNWIND.
func CypherResidualUnwindClauseNotFound(query string) Message {
	return cypherResidualMessage(MessageCypherResidualUnwindClauseNotFound, fmt.Sprintf("UNWIND clause not found in query: %q", query), map[string]any{"Query": query})
}

// CypherResidualUnionClauseNotFound identifies a routed query missing UNION.
func CypherResidualUnionClauseNotFound(query string) Message {
	return cypherResidualMessage(MessageCypherResidualUnionClauseNotFound, fmt.Sprintf("UNION clause not found in query: %q", query), map[string]any{"Query": query})
}

// CypherResidualUnionAllClauseNotFound identifies a routed query missing UNION ALL.
func CypherResidualUnionAllClauseNotFound(query string) Message {
	return cypherResidualMessage(MessageCypherResidualUnionAllClauseNotFound, fmt.Sprintf("UNION ALL clause not found in query: %q", query), map[string]any{"Query": query})
}

// CypherResidualUnionBranchFailed identifies a wrapped UNION branch failure.
func CypherResidualUnionBranchFailed(branch int, query string, cause error) Message {
	return cypherResidualMessage(MessageCypherResidualUnionBranchFailed, fmt.Sprintf("error in UNION query %d (%q): %s", branch, query, cause), map[string]any{"Branch": branch, "Query": query, "Cause": cause.Error()})
}

// CypherResidualUnionColumnCountMismatch identifies incompatible UNION projections.
func CypherResidualUnionColumnCountMismatch(expected, actual int) Message {
	return cypherResidualMessage(MessageCypherResidualUnionColumnCountMismatch, fmt.Sprintf("UNION queries must return the same number of columns (got %d and %d)", expected, actual), map[string]any{"Expected": expected, "Actual": actual})
}

// CypherResidualOptionalMatchNotFound identifies a routed query missing OPTIONAL MATCH.
func CypherResidualOptionalMatchNotFound(query string) Message {
	return cypherResidualMessage(MessageCypherResidualOptionalMatchNotFound, fmt.Sprintf("OPTIONAL MATCH not found in query: %q", query), map[string]any{"Query": query})
}

// CypherResidualCompoundOptionalMatchNotFound identifies a compound query missing OPTIONAL MATCH.
func CypherResidualCompoundOptionalMatchNotFound(query string) Message {
	return cypherResidualMessage(MessageCypherResidualCompoundOptionalMatchNotFound, fmt.Sprintf("OPTIONAL MATCH not found in compound query: %q", query), map[string]any{"Query": query})
}

// CypherResidualMatchNodePatternParseFailed identifies an invalid MATCH node pattern.
func CypherResidualMatchNodePatternParseFailed(pattern string) Message {
	return cypherResidualMessage(MessageCypherResidualMatchNodePatternParseFailed, fmt.Sprintf("could not parse node pattern from MATCH clause: %q", pattern), map[string]any{"Pattern": pattern})
}

// CypherResidualInitialNodesLookupFailed identifies a wrapped initial-node lookup failure.
func CypherResidualInitialNodesLookupFailed(cause error) Message {
	return cypherResidualMessage(MessageCypherResidualInitialNodesLookupFailed, "failed to get initial nodes: "+cause.Error(), map[string]any{"Cause": cause.Error()})
}

// CypherResidualSumArithmeticTermUnsupported identifies an unsupported SUM arithmetic term.
func CypherResidualSumArithmeticTermUnsupported(term string) Message {
	return cypherResidualMessage(MessageCypherResidualSumArithmeticTermUnsupported, "unsupported SUM arithmetic term: "+term, map[string]any{"Term": term})
}

// CypherResidualSumNumericRequired identifies a non-numeric SUM operand.
func CypherResidualSumNumericRequired(value any, expression string) Message {
	valueType := fmt.Sprintf("%T", value)
	return cypherResidualMessage(MessageCypherResidualSumNumericRequired, fmt.Sprintf("SUM() requires numeric values, got %s in expression %q", valueType, expression), map[string]any{"ValueType": valueType, "Expression": expression})
}

// CypherResidualReturnClauseRequired identifies a missing RETURN clause.
func CypherResidualReturnClauseRequired() Message {
	return cypherResidualMessage(MessageCypherResidualReturnClauseRequired, "RETURN clause required", nil)
}

// CypherResidualForeachClauseNotFound identifies a routed query missing FOREACH.
func CypherResidualForeachClauseNotFound(query string) Message {
	return cypherResidualMessage(MessageCypherResidualForeachClauseNotFound, fmt.Sprintf("FOREACH clause not found in query: %q", query), map[string]any{"Query": query})
}

// CypherResidualLoadCSVUnsupported identifies unsupported embedded LOAD CSV execution.
func CypherResidualLoadCSVUnsupported() Message {
	return cypherResidualMessage(MessageCypherResidualLoadCSVUnsupported, "LOAD CSV is not supported in NornicDB embedded mode", nil)
}

// CypherResidualEmptyLabelAfterColon identifies an empty CREATE label.
func CypherResidualEmptyLabelAfterColon(pattern string) Message {
	return cypherResidualMessage(MessageCypherResidualEmptyLabelAfterColon, "empty label name after colon in pattern: "+pattern, map[string]any{"Pattern": pattern})
}

// CypherResidualPropertyMapSyntaxInvalid identifies an unterminated CREATE property map.
func CypherResidualPropertyMapSyntaxInvalid(pattern string) Message {
	return cypherResidualMessage(MessageCypherResidualPropertyMapSyntaxInvalid, "invalid property map syntax in pattern: "+pattern, map[string]any{"Pattern": pattern})
}

// CypherResidualRelationshipConnectorExpected identifies an invalid relationship connector.
func CypherResidualRelationshipConnectorExpected(connector string) Message {
	return cypherResidualMessage(MessageCypherResidualRelationshipConnectorExpected, "invalid relationship pattern: expected -[ or <-[, got: "+connector, map[string]any{"Connector": connector})
}

// CypherResidualCreateRelationshipInvalid identifies an invalid CREATE relationship pattern.
func CypherResidualCreateRelationshipInvalid(pattern string) Message {
	return cypherResidualMessage(MessageCypherResidualCreateRelationshipInvalid, "invalid relationship pattern in CREATE: "+pattern, map[string]any{"Pattern": pattern})
}

// CypherResidualWithItemInvalid identifies a malformed WITH projection item.
func CypherResidualWithItemInvalid(item string) Message {
	return cypherResidualMessage(MessageCypherResidualWithItemInvalid, fmt.Sprintf("invalid WITH item: %q", item), map[string]any{"Item": item})
}

// CypherResidualCreateSetScopeEntityRequired identifies a non-entity CREATE...SET projection.
func CypherResidualCreateSetScopeEntityRequired(item string) Message {
	return cypherResidualMessage(MessageCypherResidualCreateSetScopeEntityRequired, fmt.Sprintf("WITH item %q does not resolve to a node or relationship in CREATE...SET scope", item), map[string]any{"Item": item})
}

// CypherResidualCreateWithExpressionInvalid identifies an unresolved CREATE...WITH expression.
func CypherResidualCreateWithExpressionInvalid(expression string) Message {
	return cypherResidualMessage(MessageCypherResidualCreateWithExpressionInvalid, fmt.Sprintf("invalid CREATE...WITH query: invalid WITH expression %q", expression), map[string]any{"Expression": expression})
}

// CypherResidualMergePatternInvalid identifies an invalid MERGE node pattern.
func CypherResidualMergePatternInvalid(pattern string) Message {
	return cypherResidualMessage(MessageCypherResidualMergePatternInvalid, "invalid pattern: "+pattern, map[string]any{"Pattern": pattern})
}

// CypherResidualSetMergeMapOrParameterRequired identifies an invalid SET += source.
func CypherResidualSetMergeMapOrParameterRequired(value string) Message {
	return cypherResidualMessage(MessageCypherResidualSetMergeMapOrParameterRequired, fmt.Sprintf("SET += requires a map or parameter (got: %q)", value), map[string]any{"Value": value})
}

// CypherResidualSetAssignmentInvalid identifies malformed SET assignment syntax.
func CypherResidualSetAssignmentInvalid(assignment string) Message {
	return cypherResidualMessage(MessageCypherResidualSetAssignmentInvalid, fmt.Sprintf("invalid SET assignment: %q (expected n.property = value or n:Label)", assignment), map[string]any{"Assignment": assignment})
}

// CypherResidualSetEntityAssignmentInvalid identifies a wrapped invalid entity assignment.
func CypherResidualSetEntityAssignmentInvalid(assignment string, cause error) Message {
	return cypherResidualMessage(MessageCypherResidualSetEntityAssignmentInvalid, fmt.Sprintf("invalid SET assignment: %q (expected variable.property = value or variable = {property: value}): %s", assignment, cause), map[string]any{"Assignment": assignment, "Cause": cause.Error()})
}

// CypherResidualCollectSubquerySyntaxInvalid identifies malformed COLLECT subquery syntax.
func CypherResidualCollectSubquerySyntaxInvalid() Message {
	return cypherResidualMessage(MessageCypherResidualCollectSubquerySyntaxInvalid, "invalid COLLECT subquery syntax", nil)
}

// CypherResidualCollectSubqueryReturnRequired identifies a COLLECT subquery missing RETURN.
func CypherResidualCollectSubqueryReturnRequired() Message {
	return cypherResidualMessage(MessageCypherResidualCollectSubqueryReturnRequired, "COLLECT subquery must have a RETURN clause", nil)
}

// CypherResidualCollectSubqueryExecutionFailed identifies a wrapped COLLECT execution failure.
func CypherResidualCollectSubqueryExecutionFailed(cause error) Message {
	return cypherResidualMessage(MessageCypherResidualCollectSubqueryExecutionFailed, "COLLECT subquery execution failed: "+cause.Error(), map[string]any{"Cause": cause.Error()})
}

// CypherResidualPolicyDisallowed identifies an edge rejected by a DISALLOWED policy.
func CypherResidualPolicyDisallowed(name, sourceLabel, edgeType, targetLabel string) Message {
	return cypherResidualMessage(MessageCypherResidualPolicyDisallowed, fmt.Sprintf("policy constraint %q violated: (%s)-[:%s]->(%s) is DISALLOWED", name, sourceLabel, edgeType, targetLabel), map[string]any{"Name": name, "SourceLabel": sourceLabel, "EdgeType": edgeType, "TargetLabel": targetLabel})
}

// CypherResidualPolicyAllowedRequired identifies an edge lacking an ALLOWED policy match.
func CypherResidualPolicyAllowedRequired(edgeType string) Message {
	return cypherResidualMessage(MessageCypherResidualPolicyAllowedRequired, "policy constraint violated: no ALLOWED policy permits edge of type "+edgeType+" with these endpoint labels", map[string]any{"EdgeType": edgeType})
}

// CypherResidualCompositeShowTargetRequired identifies SHOW against a composite root.
func CypherResidualCompositeShowTargetRequired(command string) Message {
	const code = "Neo.ClientError.Statement.NotAllowed"
	fallback := code + ": " + command + " on composite databases requires a constituent target. Use USE <composite>.<alias> " + command
	return cypherResidualMessage(MessageCypherResidualCompositeShowTargetRequired, fallback, map[string]any{"Code": code, "Command": command})
}

// CypherResidualShowAliasesManagerUnavailable identifies unavailable multi-database support.
func CypherResidualShowAliasesManagerUnavailable() Message {
	return cypherResidualMessage(MessageCypherResidualShowAliasesManagerUnavailable, "database manager not available - SHOW ALIASES requires multi-database support", nil)
}

// CypherResidualShowAliasesSyntaxInvalid identifies malformed SHOW ALIASES syntax.
func CypherResidualShowAliasesSyntaxInvalid() Message {
	return cypherResidualMessage(MessageCypherResidualShowAliasesSyntaxInvalid, "invalid SHOW ALIASES syntax", nil)
}
