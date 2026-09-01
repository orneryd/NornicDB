package localization

import (
	"fmt"
	"strconv"
)

const (
	MessageCypherMergeClauseNotFound                   MessageID = "cyphermerge.clause_not_found"
	MessageCypherMergeCreateNodeFailed                 MessageID = "cyphermerge.create_node_failed"
	MessageCypherMergeInvalidMatchQuery                MessageID = "cyphermerge.invalid_match_query"
	MessageCypherMergeMatchExecutionFailed             MessageID = "cyphermerge.match_execution_failed"
	MessageCypherMergeUnwindASRequired                 MessageID = "cyphermerge.unwind_as_required"
	MessageCypherMergeUnwindParameterNotFound          MessageID = "cyphermerge.unwind_parameter_not_found"
	MessageCypherMergeMatchLabelLookupFailed           MessageID = "cyphermerge.match_label_lookup_failed"
	MessageCypherMergeMatchAllNodesFailed              MessageID = "cyphermerge.match_all_nodes_failed"
	MessageCypherMergeMalformedRelationshipPattern     MessageID = "cyphermerge.malformed_relationship_pattern"
	MessageCypherMergeFindRelationshipFailed           MessageID = "cyphermerge.find_relationship_failed"
	MessageCypherMergeCreateRelationshipFailed         MessageID = "cyphermerge.create_relationship_failed"
	MessageCypherMergeUpdateEdgePropertyFailed         MessageID = "cyphermerge.update_edge_property_failed"
	MessageCypherMergeInitialMergeFailed               MessageID = "cyphermerge.initial_merge_failed"
	MessageCypherMergeForeachFailed                    MessageID = "cyphermerge.foreach_failed"
	MessageCypherMergeSegmentClauseNotFound            MessageID = "cyphermerge.segment_clause_not_found"
	MessageCypherMergeCreateNodeSegmentFailed          MessageID = "cyphermerge.create_node_segment_failed"
	MessageCypherMergeMatchSegmentClauseNotFound       MessageID = "cyphermerge.match_segment_clause_not_found"
	MessageCypherMergeNodePatternParseFailed           MessageID = "cyphermerge.node_pattern_parse_failed"
	MessageCypherMergeRelationshipStartMissing         MessageID = "cyphermerge.relationship_start_missing"
	MessageCypherMergeRelationshipStartParenMissing    MessageID = "cyphermerge.relationship_start_paren_missing"
	MessageCypherMergeRelationshipBracketsMissing      MessageID = "cyphermerge.relationship_brackets_missing"
	MessageCypherMergeRelationshipEndMissing           MessageID = "cyphermerge.relationship_end_missing"
	MessageCypherMergeStartVariableNotBound            MessageID = "cyphermerge.start_variable_not_bound"
	MessageCypherMergeEndVariableNotBound              MessageID = "cyphermerge.end_variable_not_bound"
	MessageCypherMergeFindRelationshipSegmentFailed    MessageID = "cyphermerge.find_relationship_segment_failed"
	MessageCypherMergeRelationshipFailed               MessageID = "cyphermerge.relationship_failed"
	MessageCypherMergeNodeFailed                       MessageID = "cyphermerge.node_failed"
	MessageCypherMergeOptionalMatchFailed              MessageID = "cyphermerge.optional_match_failed"
	MessageCypherMergeMatchFailed                      MessageID = "cyphermerge.match_failed"
	MessageCypherMergeCreateNodeVariableRequired       MessageID = "cyphermerge.create_node_variable_required"
	MessageCypherMergeInvalidLabelName                 MessageID = "cyphermerge.invalid_label_name"
	MessageCypherMergeInvalidPropertyKey               MessageID = "cyphermerge.invalid_property_key"
	MessageCypherMergeInvalidPropertyValue             MessageID = "cyphermerge.invalid_property_value"
	MessageCypherMergePipelineCreateNodeFailed         MessageID = "cyphermerge.pipeline_create_node_failed"
	MessageCypherMergeRelationshipPatternParseFailed   MessageID = "cyphermerge.relationship_pattern_parse_failed"
	MessageCypherMergeVariableNotFound                 MessageID = "cyphermerge.variable_not_found"
	MessageCypherMergeInlineNodeCreateFailed           MessageID = "cyphermerge.inline_node_create_failed"
	MessageCypherMergeSourceResolutionFailed           MessageID = "cyphermerge.source_resolution_failed"
	MessageCypherMergeTargetResolutionFailed           MessageID = "cyphermerge.target_resolution_failed"
	MessageCypherMergeSourceNodeIDEmpty                MessageID = "cyphermerge.source_node_id_empty"
	MessageCypherMergeTargetNodeIDEmpty                MessageID = "cyphermerge.target_node_id_empty"
	MessageCypherMergeRelationshipTypeRequired         MessageID = "cyphermerge.relationship_type_required"
	MessageCypherMergeCreateEdgeFailed                 MessageID = "cyphermerge.create_edge_failed"
	MessageCypherMergeUnknownFunction                  MessageID = "cyphermerge.unknown_function"
	MessageCypherMergePipelineWithRequired             MessageID = "cyphermerge.pipeline_with_required"
	MessageCypherMergeMatchPatternVariableRequired     MessageID = "cyphermerge.match_pattern_variable_required"
	MessageCypherMergeRelationshipCreateCollisions     MessageID = "cyphermerge.relationship_create_collisions"
	MessageCypherMergeRelationshipIdentityExhausted    MessageID = "cyphermerge.relationship_identity_exhausted"
	MessageCypherMergeMapLiteralEnclosureRequired      MessageID = "cyphermerge.map_literal_enclosure_required"
	MessageCypherMergeMapEntryEmpty                    MessageID = "cyphermerge.map_entry_empty"
	MessageCypherMergeMapEntryInvalid                  MessageID = "cyphermerge.map_entry_invalid"
	MessageCypherMergeMapKeyEmpty                      MessageID = "cyphermerge.map_key_empty"
	MessageCypherMergeMapValueEmpty                    MessageID = "cyphermerge.map_value_empty"
	MessageCypherMergeBulkCreateNodesFailed            MessageID = "cyphermerge.bulk_create_nodes_failed"
	MessageCypherMergeBulkCreateEdgesFailed            MessageID = "cyphermerge.bulk_create_edges_failed"
	MessageCypherMergeCreateEdgeStartNotBound          MessageID = "cyphermerge.create_edge_start_not_bound"
	MessageCypherMergeCreateEdgeEndNotBound            MessageID = "cyphermerge.create_edge_end_not_bound"
	MessageCypherMergeRelationshipEndpointsNotBound    MessageID = "cyphermerge.relationship_endpoints_not_bound"
	MessageCypherMergeSelectRelationshipIdentityFailed MessageID = "cyphermerge.select_relationship_identity_failed"
	MessageCypherMergeUpdateEdgeFailed                 MessageID = "cyphermerge.update_edge_failed"
	MessageCypherMergeGetNodesByLabelFailed            MessageID = "cyphermerge.get_nodes_by_label_failed"
	MessageCypherMergeGetEdgesBetweenFailed            MessageID = "cyphermerge.get_edges_between_failed"
)

func cypherMergeMessage(id MessageID, fallback string, data map[string]any) Message {
	return Message{ID: id, Fallback: fallback, Data: data}
}

func cypherMergeCauseMessage(id MessageID, prefix string, cause error) Message {
	return cypherMergeMessage(id, prefix+cause.Error(), map[string]any{"Cause": cause.Error()})
}

func CypherMergeClauseNotFound(query string) Message {
	return cypherMergeMessage(MessageCypherMergeClauseNotFound, fmt.Sprintf("MERGE clause not found in query: %q", query), map[string]any{"Query": query})
}

func CypherMergeCreateNodeFailed(cause error) Message {
	return cypherMergeCauseMessage(MessageCypherMergeCreateNodeFailed, "failed to create node in MERGE: ", cause)
}

func CypherMergeInvalidMatchQuery() Message {
	return cypherMergeMessage(MessageCypherMergeInvalidMatchQuery, "invalid MATCH ... MERGE query", nil)
}

func CypherMergeMatchExecutionFailed(cause error) Message {
	return cypherMergeCauseMessage(MessageCypherMergeMatchExecutionFailed, "failed to execute MATCH: ", cause)
}

func CypherMergeUnwindASRequired() Message {
	return cypherMergeMessage(MessageCypherMergeUnwindASRequired, "UNWIND requires AS clause in MATCH ... UNWIND ... MERGE query", nil)
}

func CypherMergeUnwindParameterNotFound(parameter string) Message {
	return cypherMergeMessage(MessageCypherMergeUnwindParameterNotFound, "UNWIND parameter $"+parameter+" not found or is null", map[string]any{"Parameter": parameter})
}

func CypherMergeMatchLabelLookupFailed(label string, cause error) Message {
	return cypherMergeMessage(MessageCypherMergeMatchLabelLookupFailed, fmt.Sprintf("failed to get nodes by label %q: %s", label, cause), map[string]any{"Label": label, "Cause": cause.Error()})
}

func CypherMergeMatchAllNodesFailed(cause error) Message {
	return cypherMergeCauseMessage(MessageCypherMergeMatchAllNodesFailed, "failed to get all nodes for match: ", cause)
}

func CypherMergeMalformedRelationshipPattern(pattern string) Message {
	return cypherMergeMessage(MessageCypherMergeMalformedRelationshipPattern, "malformed relationship pattern: "+pattern, map[string]any{"Pattern": pattern})
}

func CypherMergeFindRelationshipFailed(cause error) Message {
	return cypherMergeCauseMessage(MessageCypherMergeFindRelationshipFailed, "find relationship for MERGE: ", cause)
}

func CypherMergeCreateRelationshipFailed(cause error) Message {
	return cypherMergeCauseMessage(MessageCypherMergeCreateRelationshipFailed, "failed to create relationship: ", cause)
}

func CypherMergeUpdateEdgePropertyFailed(cause error) Message {
	return cypherMergeCauseMessage(MessageCypherMergeUpdateEdgePropertyFailed, "failed to update edge property: ", cause)
}

func CypherMergeInitialMergeFailed(cause error) Message {
	return cypherMergeCauseMessage(MessageCypherMergeInitialMergeFailed, "initial MERGE failed: ", cause)
}

func CypherMergeForeachFailed(cause error) Message {
	return cypherMergeCauseMessage(MessageCypherMergeForeachFailed, "FOREACH failed: ", cause)
}

func CypherMergeSegmentClauseNotFound() Message {
	return cypherMergeMessage(MessageCypherMergeSegmentClauseNotFound, "MERGE not found in segment", nil)
}

func CypherMergeCreateNodeSegmentFailed(cause error) Message {
	return cypherMergeCauseMessage(MessageCypherMergeCreateNodeSegmentFailed, "failed to create node: ", cause)
}

func CypherMergeMatchSegmentClauseNotFound() Message {
	return cypherMergeMessage(MessageCypherMergeMatchSegmentClauseNotFound, "MATCH not found in segment", nil)
}

func CypherMergeNodePatternParseFailed(pattern string) Message {
	return cypherMergeMessage(MessageCypherMergeNodePatternParseFailed, "could not parse node pattern: "+pattern, map[string]any{"Pattern": pattern})
}

func CypherMergeRelationshipStartMissing(pattern string) Message {
	return cypherMergeMessage(MessageCypherMergeRelationshipStartMissing, fmt.Sprintf("invalid relationship pattern: missing start node in %q", pattern), map[string]any{"Pattern": pattern})
}

func CypherMergeRelationshipStartParenMissing(pattern string) Message {
	return cypherMergeMessage(MessageCypherMergeRelationshipStartParenMissing, fmt.Sprintf("invalid relationship pattern: missing start node closing paren in %q", pattern), map[string]any{"Pattern": pattern})
}

func CypherMergeRelationshipBracketsMissing(pattern string) Message {
	return cypherMergeMessage(MessageCypherMergeRelationshipBracketsMissing, fmt.Sprintf("invalid relationship pattern: missing relationship brackets (expected -[type]-> or -[type]-) in %q", pattern), map[string]any{"Pattern": pattern})
}

func CypherMergeRelationshipEndMissing(pattern string) Message {
	return cypherMergeMessage(MessageCypherMergeRelationshipEndMissing, fmt.Sprintf("invalid relationship pattern: missing end node in %q", pattern), map[string]any{"Pattern": pattern})
}

func CypherMergeStartVariableNotBound(variable string, available any) Message {
	return cypherMergeMessage(MessageCypherMergeStartVariableNotBound, fmt.Sprintf("start node variable '%s' not in context (available: %v)", variable, available), map[string]any{"Variable": variable, "Available": fmt.Sprint(available)})
}

func CypherMergeEndVariableNotBound(variable string, available any) Message {
	return cypherMergeMessage(MessageCypherMergeEndVariableNotBound, fmt.Sprintf("end node variable '%s' not in context (available: %v)", variable, available), map[string]any{"Variable": variable, "Available": fmt.Sprint(available)})
}

func CypherMergeFindRelationshipSegmentFailed(cause error) Message {
	return cypherMergeCauseMessage(MessageCypherMergeFindRelationshipSegmentFailed, "find relationship for MERGE segment: ", cause)
}

func CypherMergeRelationshipFailed(cause error) Message {
	return cypherMergeCauseMessage(MessageCypherMergeRelationshipFailed, "relationship MERGE failed: ", cause)
}

func CypherMergeNodeFailed(cause error) Message {
	return cypherMergeCauseMessage(MessageCypherMergeNodeFailed, "node MERGE failed: ", cause)
}

func CypherMergeOptionalMatchFailed(cause error) Message {
	return cypherMergeCauseMessage(MessageCypherMergeOptionalMatchFailed, "OPTIONAL MATCH failed: ", cause)
}

func CypherMergeMatchFailed(cause error) Message {
	return cypherMergeCauseMessage(MessageCypherMergeMatchFailed, "MATCH failed: ", cause)
}

func CypherMergeCreateNodeVariableRequired() Message {
	return cypherMergeMessage(MessageCypherMergeCreateNodeVariableRequired, "CREATE node must have a variable name", nil)
}

func CypherMergeInvalidLabelName(label string) Message {
	return cypherMergeMessage(MessageCypherMergeInvalidLabelName, fmt.Sprintf("invalid label name: %q", label), map[string]any{"Label": label})
}

func CypherMergeInvalidPropertyKey(property string) Message {
	return cypherMergeMessage(MessageCypherMergeInvalidPropertyKey, fmt.Sprintf("invalid property key: %q", property), map[string]any{"Property": property})
}

func CypherMergeInvalidPropertyValue(property string) Message {
	return cypherMergeMessage(MessageCypherMergeInvalidPropertyValue, fmt.Sprintf("invalid property value for key %q", property), map[string]any{"Property": property})
}

func CypherMergePipelineCreateNodeFailed(cause error) Message {
	return cypherMergeCauseMessage(MessageCypherMergePipelineCreateNodeFailed, "failed to create node: ", cause)
}

func CypherMergeRelationshipPatternParseFailed(cause error) Message {
	return cypherMergeCauseMessage(MessageCypherMergeRelationshipPatternParseFailed, "failed to parse relationship pattern: ", cause)
}

func CypherMergeVariableNotFound(content string) Message {
	return cypherMergeMessage(MessageCypherMergeVariableNotFound, "variable not found in context: "+content, map[string]any{"Content": content})
}

func CypherMergeInlineNodeCreateFailed(cause error) Message {
	return cypherMergeCauseMessage(MessageCypherMergeInlineNodeCreateFailed, "failed to create inline node: ", cause)
}

func CypherMergeSourceResolutionFailed(variable string, cause error) Message {
	return cypherMergeMessage(MessageCypherMergeSourceResolutionFailed, fmt.Sprintf("source %s: %s", variable, cause), map[string]any{"Variable": variable, "Cause": cause.Error()})
}

func CypherMergeTargetResolutionFailed(variable string, cause error) Message {
	return cypherMergeMessage(MessageCypherMergeTargetResolutionFailed, fmt.Sprintf("target %s: %s", variable, cause), map[string]any{"Variable": variable, "Cause": cause.Error()})
}

func CypherMergeSourceNodeIDEmpty(variable string) Message {
	return cypherMergeMessage(MessageCypherMergeSourceNodeIDEmpty, "source node "+variable+" has empty ID", map[string]any{"Variable": variable})
}

func CypherMergeTargetNodeIDEmpty(variable string) Message {
	return cypherMergeMessage(MessageCypherMergeTargetNodeIDEmpty, "target node "+variable+" has empty ID", map[string]any{"Variable": variable})
}

func CypherMergeRelationshipTypeRequired() Message {
	return cypherMergeMessage(MessageCypherMergeRelationshipTypeRequired, "relationship type is required", nil)
}

func CypherMergeCreateEdgeFailed(cause error) Message {
	return cypherMergeCauseMessage(MessageCypherMergeCreateEdgeFailed, "failed to create edge: ", cause)
}

func CypherMergeUnknownFunction(function string) Message {
	return cypherMergeMessage(MessageCypherMergeUnknownFunction, "unknown function: "+function, map[string]any{"Function": function})
}

func CypherMergePipelineWithRequired() Message {
	return cypherMergeMessage(MessageCypherMergePipelineWithRequired, "pipeline requires WITH", nil)
}

func CypherMergeMatchPatternVariableRequired() Message {
	return cypherMergeMessage(MessageCypherMergeMatchPatternVariableRequired, "MATCH pattern must have a variable", nil)
}

func CypherMergeRelationshipCreateCollisions(attempts int) Message {
	return cypherMergeMessage(MessageCypherMergeRelationshipCreateCollisions, fmt.Sprintf("relationship MERGE create failed after %d edge ID collisions", attempts), map[string]any{"Attempts": attempts})
}

func CypherMergeRelationshipIdentityExhausted() Message {
	return cypherMergeMessage(MessageCypherMergeRelationshipIdentityExhausted, "relationship MERGE property identity has no free storage key", nil)
}

func CypherMergeMapLiteralEnclosureRequired() Message {
	return cypherMergeMessage(MessageCypherMergeMapLiteralEnclosureRequired, "map literal must be enclosed in { ... }", nil)
}

func CypherMergeMapEntryEmpty() Message {
	return cypherMergeMessage(MessageCypherMergeMapEntryEmpty, "empty map entry", nil)
}

func CypherMergeMapEntryInvalid(entry string) Message {
	return cypherMergeMessage(MessageCypherMergeMapEntryInvalid, "invalid map entry "+strconv.Quote(entry), map[string]any{"Entry": entry})
}

func CypherMergeMapKeyEmpty() Message {
	return cypherMergeMessage(MessageCypherMergeMapKeyEmpty, "empty map key", nil)
}

func CypherMergeMapValueEmpty(key string) Message {
	return cypherMergeMessage(MessageCypherMergeMapValueEmpty, "empty map value for key "+strconv.Quote(key), map[string]any{"Key": key})
}

func CypherMergeBulkCreateNodesFailed(cause error) Message {
	return cypherMergeCauseMessage(MessageCypherMergeBulkCreateNodesFailed, "BulkCreateNodes: ", cause)
}

func CypherMergeBulkCreateEdgesFailed(cause error) Message {
	return cypherMergeCauseMessage(MessageCypherMergeBulkCreateEdgesFailed, "BulkCreateEdges: ", cause)
}

func CypherMergeCreateEdgeStartNotBound(variable string) Message {
	return cypherMergeMessage(MessageCypherMergeCreateEdgeStartNotBound, fmt.Sprintf("CREATE edge: start variable %q not bound", variable), map[string]any{"Variable": variable})
}

func CypherMergeCreateEdgeEndNotBound(variable string) Message {
	return cypherMergeMessage(MessageCypherMergeCreateEdgeEndNotBound, fmt.Sprintf("CREATE edge: end variable %q not bound", variable), map[string]any{"Variable": variable})
}

func CypherMergeRelationshipEndpointsNotBound(startVariable, endVariable string) Message {
	return cypherMergeMessage(MessageCypherMergeRelationshipEndpointsNotBound, "MERGE relationship endpoints are not bound: "+startVariable+", "+endVariable, map[string]any{"StartVariable": startVariable, "EndVariable": endVariable})
}

func CypherMergeSelectRelationshipIdentityFailed(cause error) Message {
	return cypherMergeCauseMessage(MessageCypherMergeSelectRelationshipIdentityFailed, "select relationship MERGE identity: ", cause)
}

func CypherMergeUpdateEdgeFailed(cause error) Message {
	return cypherMergeCauseMessage(MessageCypherMergeUpdateEdgeFailed, "UpdateEdge: ", cause)
}

func CypherMergeGetNodesByLabelFailed(label string, cause error) Message {
	return cypherMergeMessage(MessageCypherMergeGetNodesByLabelFailed, fmt.Sprintf("GetNodesByLabel(%s): %s", label, cause), map[string]any{"Label": label, "Cause": cause.Error()})
}

func CypherMergeGetEdgesBetweenFailed(startID, endID string, cause error) Message {
	return cypherMergeMessage(MessageCypherMergeGetEdgesBetweenFailed, fmt.Sprintf("GetEdgesBetween(%s,%s): %s", startID, endID, cause), map[string]any{"StartID": startID, "EndID": endID, "Cause": cause.Error()})
}
