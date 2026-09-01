package localization

import "fmt"

const (
	MessageStorageRemoteURIRequired                         MessageID = "storage.remote.uri_required"
	MessageStorageRemoteDatabaseRequired                    MessageID = "storage.remote.database_required"
	MessageStorageRemoteURISchemeUnsupported                MessageID = "storage.remote.uri_scheme_unsupported"
	MessageStorageRemoteBoltDriverCreateFailed              MessageID = "storage.remote.bolt_driver_create_failed"
	MessageStorageRemoteResultKeysFailed                    MessageID = "storage.remote.result_keys_failed"
	MessageStorageRemoteTransactionDecodeFailed             MessageID = "storage.remote.transaction_decode_failed"
	MessageStorageRemoteError                               MessageID = "storage.remote.error"
	MessageStorageRemoteTransactionOpenDecodeFailed         MessageID = "storage.remote.transaction_open_decode_failed"
	MessageStorageRemoteTransactionCommitURLMissing         MessageID = "storage.remote.transaction_commit_url_missing"
	MessageStorageRemoteTransactionClosed                   MessageID = "storage.remote.transaction_closed"
	MessageStorageRemoteNodeMapExpected                     MessageID = "storage.remote.node_map_expected"
	MessageStorageRemoteRelationshipMapExpected             MessageID = "storage.remote.relationship_map_expected"
	MessageStorageRemoteCreateNodeNoRows                    MessageID = "storage.remote.create_node_no_rows"
	MessageStorageRemoteDeletePrefixUnsupported             MessageID = "storage.remote.delete_prefix_unsupported"
	MessageStoragePropertyInvalidValue                      MessageID = "storage.property.invalid_value"
	MessageStoragePropertyInvalidIndex                      MessageID = "storage.property.invalid_index"
	MessageStoragePropertyInvalidMapKey                     MessageID = "storage.property.invalid_map_key"
	MessageStoragePropertyTypeUnsupported                   MessageID = "storage.property.type_unsupported"
	MessageStorageValidationUnknownConstraintType           MessageID = "storage.validation.unknown_constraint_type"
	MessageStorageValidationRefreshUniqueScanFailed         MessageID = "storage.validation.refresh_unique_scan_failed"
	MessageStorageValidationRefreshUniqueFailed             MessageID = "storage.validation.refresh_unique_failed"
	MessageStorageValidationScanEdgesFailed                 MessageID = "storage.validation.scan_edges_failed"
	MessageStorageValidationRelationshipConstraintType      MessageID = "storage.validation.relationship_constraint_type_unsupported"
	MessageStorageValidationRelationshipUniqueDuplicate     MessageID = "storage.validation.relationship_unique_duplicate"
	MessageStorageValidationRelationshipCompositeDuplicate  MessageID = "storage.validation.relationship_composite_duplicate"
	MessageStorageValidationRelationshipPropertyMissing     MessageID = "storage.validation.relationship_property_missing"
	MessageStorageValidationEdgeKeyNull                     MessageID = "storage.validation.edge_key_null"
	MessageStorageValidationTemporalPropertiesAtLeastThree  MessageID = "storage.validation.temporal_properties_at_least_three"
	MessageStorageValidationTemporalCreationFailed          MessageID = "storage.validation.temporal_creation_failed"
	MessageStorageValidationTemporalEdgeInvalid             MessageID = "storage.validation.temporal_edge_invalid"
	MessageStorageValidationTemporalEdgesOverlap            MessageID = "storage.validation.temporal_edges_overlap"
	MessageStorageValidationDomainPropertyCount             MessageID = "storage.validation.domain_property_count"
	MessageStorageValidationDomainAllowedValuesRequired     MessageID = "storage.validation.domain_allowed_values_required"
	MessageStorageValidationScanNodesFailed                 MessageID = "storage.validation.scan_nodes_failed"
	MessageStorageValidationDomainNodeInvalid               MessageID = "storage.validation.domain_node_invalid"
	MessageStorageValidationDomainEdgeInvalid               MessageID = "storage.validation.domain_edge_invalid"
	MessageStorageValidationCardinalityCreationExceeded     MessageID = "storage.validation.cardinality_creation_exceeded"
	MessageStorageValidationDisallowedPolicyCreation        MessageID = "storage.validation.disallowed_policy_creation"
	MessageStorageValidationAllowedPolicyCreation           MessageID = "storage.validation.allowed_policy_creation"
	MessageStorageValidationUniquePropertyCount             MessageID = "storage.validation.unique_property_count"
	MessageStorageValidationNodeUniqueDuplicate             MessageID = "storage.validation.node_unique_duplicate"
	MessageStorageValidationNodeKeyPropertyRequired         MessageID = "storage.validation.node_key_property_required"
	MessageStorageValidationNodeKeyNullCreation             MessageID = "storage.validation.node_key_null_creation"
	MessageStorageValidationNodeKeyDuplicateCreation        MessageID = "storage.validation.node_key_duplicate_creation"
	MessageStorageValidationExistsPropertyCount             MessageID = "storage.validation.exists_property_count"
	MessageStorageValidationNodeExistsMissingCreation       MessageID = "storage.validation.node_exists_missing_creation"
	MessageStorageValidationTemporalPropertiesExactlyThree  MessageID = "storage.validation.temporal_properties_exactly_three"
	MessageStorageValidationTemporalNodeKeyNullCreation     MessageID = "storage.validation.temporal_node_key_null_creation"
	MessageStorageValidationTemporalNodeInvalidCreation     MessageID = "storage.validation.temporal_node_invalid_creation"
	MessageStorageValidationTemporalNodesOverlapCreation    MessageID = "storage.validation.temporal_nodes_overlap_creation"
	MessageStorageValidationRelationshipUniquePropertyCount MessageID = "storage.validation.relationship_unique_property_count"
	MessageStorageValidationRelationshipExistsPropertyCount MessageID = "storage.validation.relationship_exists_property_count"
	MessageStorageValidationRelationshipExistsMissing       MessageID = "storage.validation.relationship_exists_missing"
	MessageStorageValidationExpectedType                    MessageID = "storage.validation.expected_type"
	MessageStorageValidationUnknownPropertyType             MessageID = "storage.validation.unknown_property_type"
	MessageStorageValidationNodePropertyInvalid             MessageID = "storage.validation.node_property_invalid"
	MessageStorageValidationRelationshipPropertyInvalid     MessageID = "storage.validation.relationship_property_invalid"
	MessageStorageTransactionEngineClosed                   MessageID = "storage.transaction.engine_closed"
	MessageStorageTransactionNamespaceRequired              MessageID = "storage.transaction.namespace_required"
	MessageStorageTransactionCrossNamespace                 MessageID = "storage.transaction.cross_namespace"
	MessageStorageTransactionIDNamespaceRequired            MessageID = "storage.transaction.id_namespace_required"
	MessageStorageTransactionStartNodeMissing               MessageID = "storage.transaction.start_node_missing"
	MessageStorageTransactionEndNodeMissing                 MessageID = "storage.transaction.end_node_missing"
	MessageStorageTransactionConstraintViolation            MessageID = "storage.transaction.constraint_violation"
	MessageStorageTransactionCommitNamespaceMissing         MessageID = "storage.transaction.commit_namespace_missing"
	MessageStorageTransactionCommitConflict                 MessageID = "storage.transaction.commit_conflict"
	MessageStorageTransactionMetadataTooLarge               MessageID = "storage.transaction.metadata_too_large"
	MessageStorageTransactionEdgeChanged                    MessageID = "storage.transaction.edge_changed"
	MessageStorageTransactionNodeChanged                    MessageID = "storage.transaction.node_changed"
	MessageStorageTransactionNodeChangedDetailed            MessageID = "storage.transaction.node_changed_detailed"
	MessageStorageTransactionConcurrentModification         MessageID = "storage.transaction.concurrent_modification"
	MessageStorageTransactionEndpointDeleted                MessageID = "storage.transaction.endpoint_deleted"
	MessageStorageTransactionAdjacentEdgeChanged            MessageID = "storage.transaction.adjacent_edge_changed"
	MessageStorageValidationPropertyDomainInvalid           MessageID = "storage.validation.property_domain_invalid"
	MessageStorageValidationPropertyTypeRequired            MessageID = "storage.validation.property_type_required"
	MessageStorageValidationNodeUniqueInTransaction         MessageID = "storage.validation.node_unique_in_transaction"
	MessageStorageValidationNodeUniqueExisting              MessageID = "storage.validation.node_unique_existing"
	MessageStorageValidationNodeUniqueConcurrent            MessageID = "storage.validation.node_unique_concurrent"
	MessageStorageValidationNodeKeyNull                     MessageID = "storage.validation.node_key_null"
	MessageStorageValidationNodeKeyInTransaction            MessageID = "storage.validation.node_key_in_transaction"
	MessageStorageValidationNodeCompositeKeyExisting        MessageID = "storage.validation.node_composite_key_existing"
	MessageStorageValidationRequiredPropertyMissing         MessageID = "storage.validation.required_property_missing"
	MessageStorageValidationTemporalKeyNull                 MessageID = "storage.validation.temporal_key_null"
	MessageStorageValidationTemporalStartInvalid            MessageID = "storage.validation.temporal_start_invalid"
	MessageStorageValidationTemporalNodeRequired            MessageID = "storage.validation.temporal_node_required"
	MessageStorageValidationTemporalNodeOverlap             MessageID = "storage.validation.temporal_node_overlap"
	MessageStorageValidationRelationshipUniqueExisting      MessageID = "storage.validation.relationship_unique_existing"
	MessageStorageValidationRelationshipCompositeExisting   MessageID = "storage.validation.relationship_composite_existing"
	MessageStorageValidationTemporalEdgeOverlap             MessageID = "storage.validation.temporal_edge_overlap"
	MessageStorageValidationCardinalityExceeded             MessageID = "storage.validation.cardinality_exceeded"
	MessageStorageValidationDisallowedPolicy                MessageID = "storage.validation.disallowed_policy"
	MessageStorageValidationAllowedPolicy                   MessageID = "storage.validation.allowed_policy"
	MessageStorageValidationLabelChangeDisallowed           MessageID = "storage.validation.label_change_disallowed"
	MessageStorageValidationLabelChangeAllowed              MessageID = "storage.validation.label_change_allowed"
)

func storageValidationMessage(id MessageID, fallback string, data map[string]any) Message {
	return Message{ID: id, Fallback: fallback, Data: data}
}

func storageErrorText(cause error) string {
	if cause == nil {
		return ""
	}
	return cause.Error()
}

func StorageRemoteURIRequired() Message {
	return storageValidationMessage(MessageStorageRemoteURIRequired, "remote engine URI cannot be empty", nil)
}
func StorageRemoteDatabaseRequired() Message {
	return storageValidationMessage(MessageStorageRemoteDatabaseRequired, "remote engine database cannot be empty", nil)
}
func StorageRemoteURISchemeUnsupported(uri string) Message {
	return storageValidationMessage(MessageStorageRemoteURISchemeUnsupported, fmt.Sprintf("unsupported remote engine URI scheme: %s (expected bolt://, neo4j://, http://, or https://)", uri), map[string]any{"URI": uri})
}
func StorageRemoteBoltDriverCreateFailed(cause error) Message {
	return storageValidationMessage(MessageStorageRemoteBoltDriverCreateFailed, "failed to create Bolt driver: "+storageErrorText(cause), map[string]any{"Cause": storageErrorText(cause)})
}
func StorageRemoteResultKeysFailed(cause error) Message {
	return storageValidationMessage(MessageStorageRemoteResultKeysFailed, "failed to get result keys: "+storageErrorText(cause), map[string]any{"Cause": storageErrorText(cause)})
}
func StorageRemoteTransactionDecodeFailed(status int, cause error) Message {
	return storageValidationMessage(MessageStorageRemoteTransactionDecodeFailed, fmt.Sprintf("remote tx decode failed (status=%d): %s", status, storageErrorText(cause)), map[string]any{"Status": status, "Cause": storageErrorText(cause)})
}
func StorageRemoteError(code, detail string) Message {
	return storageValidationMessage(MessageStorageRemoteError, code+": "+detail, map[string]any{"Code": code, "Detail": detail})
}
func StorageRemoteTransactionOpenDecodeFailed(status int, cause error) Message {
	return storageValidationMessage(MessageStorageRemoteTransactionOpenDecodeFailed, fmt.Sprintf("remote tx open decode failed (status=%d): %s", status, storageErrorText(cause)), map[string]any{"Status": status, "Cause": storageErrorText(cause)})
}
func StorageRemoteTransactionCommitURLMissing() Message {
	return storageValidationMessage(MessageStorageRemoteTransactionCommitURLMissing, "remote tx open returned empty commit URL", nil)
}
func StorageRemoteTransactionClosed() Message {
	return storageValidationMessage(MessageStorageRemoteTransactionClosed, "remote transaction is closed", nil)
}
func StorageRemoteNodeMapExpected(actualType string) Message {
	return storageValidationMessage(MessageStorageRemoteNodeMapExpected, "expected node map, got "+actualType, map[string]any{"ActualType": actualType})
}
func StorageRemoteRelationshipMapExpected(actualType string) Message {
	return storageValidationMessage(MessageStorageRemoteRelationshipMapExpected, "expected relationship map, got "+actualType, map[string]any{"ActualType": actualType})
}
func StorageRemoteCreateNodeNoRows() Message {
	return storageValidationMessage(MessageStorageRemoteCreateNodeNoRows, "remote create node returned no rows", nil)
}
func StorageRemoteDeletePrefixUnsupported(prefix string) Message {
	return storageValidationMessage(MessageStorageRemoteDeletePrefixUnsupported, fmt.Sprintf("DeleteByPrefix is not supported for remote engines (prefix=%s)", prefix), map[string]any{"Prefix": prefix})
}

func StoragePropertyInvalidValue(key string, cause error) Message {
	return storageValidationMessage(MessageStoragePropertyInvalidValue, fmt.Sprintf("invalid property value for key %q: %s", key, storageErrorText(cause)), map[string]any{"Key": key, "Cause": storageErrorText(cause)})
}
func StoragePropertyInvalidIndex(index int, cause error) Message {
	return storageValidationMessage(MessageStoragePropertyInvalidIndex, fmt.Sprintf("index %d: %s", index, storageErrorText(cause)), map[string]any{"Index": index, "Cause": storageErrorText(cause)})
}
func StoragePropertyInvalidMapKey(key string, cause error) Message {
	return storageValidationMessage(MessageStoragePropertyInvalidMapKey, fmt.Sprintf("key %q: %s", key, storageErrorText(cause)), map[string]any{"Key": key, "Cause": storageErrorText(cause)})
}
func StoragePropertyTypeUnsupported(actualType string) Message {
	return storageValidationMessage(MessageStoragePropertyTypeUnsupported, "unsupported property value type "+actualType, map[string]any{"ActualType": actualType})
}

func StorageValidationUnknownConstraintType(constraintType string) Message {
	return storageValidationMessage(MessageStorageValidationUnknownConstraintType, "unknown constraint type: "+constraintType, map[string]any{"ConstraintType": constraintType})
}
func StorageValidationRefreshUniqueScanFailed(cause error) Message {
	return storageValidationMessage(MessageStorageValidationRefreshUniqueScanFailed, "refresh unique constraint values: scan nodes: "+storageErrorText(cause), map[string]any{"Cause": storageErrorText(cause)})
}
func StorageValidationRefreshUniqueFailed(cause error) Message {
	return storageValidationMessage(MessageStorageValidationRefreshUniqueFailed, "refresh unique constraint values: "+storageErrorText(cause), map[string]any{"Cause": storageErrorText(cause)})
}
func StorageValidationScanEdgesFailed(cause error) Message {
	return storageValidationMessage(MessageStorageValidationScanEdgesFailed, "scanning edges: "+storageErrorText(cause), map[string]any{"Cause": storageErrorText(cause)})
}
func StorageValidationRelationshipConstraintTypeUnsupported(constraintType string) Message {
	return storageValidationMessage(MessageStorageValidationRelationshipConstraintType, "unsupported relationship constraint type: "+constraintType, map[string]any{"ConstraintType": constraintType})
}
func StorageValidationRelationshipUniqueDuplicate(firstEdgeID, secondEdgeID, property string, value any) Message {
	return storageValidationMessage(MessageStorageValidationRelationshipUniqueDuplicate, fmt.Sprintf("Cannot create UNIQUE constraint on relationship: edges %s and %s both have %s=%v", firstEdgeID, secondEdgeID, property, value), map[string]any{"FirstEdgeID": firstEdgeID, "SecondEdgeID": secondEdgeID, "Property": property, "Value": value})
}
func StorageValidationRelationshipCompositeDuplicate(firstEdgeID, secondEdgeID string, key any) Message {
	return storageValidationMessage(MessageStorageValidationRelationshipCompositeDuplicate, fmt.Sprintf("Cannot create UNIQUE constraint on relationship: edges %s and %s have duplicate composite key %v", firstEdgeID, secondEdgeID, key), map[string]any{"FirstEdgeID": firstEdgeID, "SecondEdgeID": secondEdgeID, "Key": key})
}
func StorageValidationRelationshipPropertyMissing(edgeID, property string) Message {
	return storageValidationMessage(MessageStorageValidationRelationshipPropertyMissing, fmt.Sprintf("Cannot create constraint on relationship: edge %s is missing required property %s", edgeID, property), map[string]any{"EdgeID": edgeID, "Property": property})
}
func StorageValidationEdgeKeyNull(edgeID, property string) Message {
	return storageValidationMessage(MessageStorageValidationEdgeKeyNull, fmt.Sprintf("edge %s has null %s", edgeID, property), map[string]any{"EdgeID": edgeID, "Property": property})
}
func StorageValidationTemporalPropertiesAtLeastThree() Message {
	return storageValidationMessage(MessageStorageValidationTemporalPropertiesAtLeastThree, "TEMPORAL constraint requires at least 3 properties (key..., valid_from, valid_to)", nil)
}
func StorageValidationTemporalCreationFailed(detail string) Message {
	return storageValidationMessage(MessageStorageValidationTemporalCreationFailed, "Cannot create TEMPORAL constraint: "+detail, map[string]any{"Detail": detail})
}
func StorageValidationTemporalEdgeInvalid(edgeID, property string) Message {
	return storageValidationMessage(MessageStorageValidationTemporalEdgeInvalid, fmt.Sprintf("Cannot create TEMPORAL constraint: edge %s has invalid %s", edgeID, property), map[string]any{"EdgeID": edgeID, "Property": property})
}
func StorageValidationTemporalEdgesOverlap(firstEdgeID, secondEdgeID string) Message {
	return storageValidationMessage(MessageStorageValidationTemporalEdgesOverlap, fmt.Sprintf("Cannot create TEMPORAL constraint: overlap between edges %s and %s", firstEdgeID, secondEdgeID), map[string]any{"FirstEdgeID": firstEdgeID, "SecondEdgeID": secondEdgeID})
}
func StorageValidationDomainPropertyCount(actual int) Message {
	return storageValidationMessage(MessageStorageValidationDomainPropertyCount, fmt.Sprintf("DOMAIN constraint requires exactly 1 property, got %d", actual), map[string]any{"Actual": actual})
}
func StorageValidationDomainAllowedValuesRequired() Message {
	return storageValidationMessage(MessageStorageValidationDomainAllowedValuesRequired, "DOMAIN constraint requires at least one allowed value", nil)
}
func StorageValidationScanNodesFailed(cause error) Message {
	return storageValidationMessage(MessageStorageValidationScanNodesFailed, "scanning nodes: "+storageErrorText(cause), map[string]any{"Cause": storageErrorText(cause)})
}
func StorageValidationDomainNodeInvalid(nodeID, property string, value, allowedValues any) Message {
	return storageValidationMessage(MessageStorageValidationDomainNodeInvalid, fmt.Sprintf("Cannot create DOMAIN constraint: node %s has %s=%v which is not in allowed values %v", nodeID, property, value, allowedValues), map[string]any{"NodeID": nodeID, "Property": property, "Value": value, "AllowedValues": allowedValues})
}
func StorageValidationDomainEdgeInvalid(edgeID, property string, value, allowedValues any) Message {
	return storageValidationMessage(MessageStorageValidationDomainEdgeInvalid, fmt.Sprintf("Cannot create DOMAIN constraint: edge %s has %s=%v which is not in allowed values %v", edgeID, property, value, allowedValues), map[string]any{"EdgeID": edgeID, "Property": property, "Value": value, "AllowedValues": allowedValues})
}
func StorageValidationCardinalityCreationExceeded(nodeID string, count int, direction, relationshipType string, maxCount int) Message {
	return storageValidationMessage(MessageStorageValidationCardinalityCreationExceeded, fmt.Sprintf("Cannot create CARDINALITY constraint: node %s has %d %s %s edges, exceeding max count %d", nodeID, count, direction, relationshipType, maxCount), map[string]any{"NodeID": nodeID, "Count": count, "Direction": direction, "RelationshipType": relationshipType, "MaxCount": maxCount})
}
func StorageValidationDisallowedPolicyCreation(edgeID, startNodeID, sourceLabel, endNodeID, targetLabel, relationshipType string) Message {
	return storageValidationMessage(MessageStorageValidationDisallowedPolicyCreation, fmt.Sprintf("Cannot create DISALLOWED policy: edge %s connects %s (:%s) to %s (:%s) via :%s", edgeID, startNodeID, sourceLabel, endNodeID, targetLabel, relationshipType), map[string]any{"EdgeID": edgeID, "StartNodeID": startNodeID, "SourceLabel": sourceLabel, "EndNodeID": endNodeID, "TargetLabel": targetLabel, "RelationshipType": relationshipType})
}
func StorageValidationAllowedPolicyCreation(edgeID, startNodeID, endNodeID, relationshipType string) Message {
	return storageValidationMessage(MessageStorageValidationAllowedPolicyCreation, fmt.Sprintf("Cannot create ALLOWED policy: existing edge %s connects %s to %s via :%s, which is not covered by any ALLOWED pair", edgeID, startNodeID, endNodeID, relationshipType), map[string]any{"EdgeID": edgeID, "StartNodeID": startNodeID, "EndNodeID": endNodeID, "RelationshipType": relationshipType})
}
func StorageValidationUniquePropertyCount(actual int) Message {
	return storageValidationMessage(MessageStorageValidationUniquePropertyCount, fmt.Sprintf("UNIQUE constraint requires exactly 1 property, got %d", actual), map[string]any{"Actual": actual})
}
func StorageValidationNodeUniqueDuplicate(firstNodeID, secondNodeID, property string, value any) Message {
	return storageValidationMessage(MessageStorageValidationNodeUniqueDuplicate, fmt.Sprintf("Cannot create UNIQUE constraint: nodes %s and %s both have %s=%v", firstNodeID, secondNodeID, property, value), map[string]any{"FirstNodeID": firstNodeID, "SecondNodeID": secondNodeID, "Property": property, "Value": value})
}
func StorageValidationNodeKeyPropertyRequired() Message {
	return storageValidationMessage(MessageStorageValidationNodeKeyPropertyRequired, "NODE KEY constraint requires at least 1 property", nil)
}
func StorageValidationNodeKeyNullCreation(nodeID, property string) Message {
	return storageValidationMessage(MessageStorageValidationNodeKeyNullCreation, fmt.Sprintf("Cannot create NODE KEY constraint: node %s has null value for property %s", nodeID, property), map[string]any{"NodeID": nodeID, "Property": property})
}
func StorageValidationNodeKeyDuplicateCreation(firstNodeID, secondNodeID string, properties, values any) Message {
	return storageValidationMessage(MessageStorageValidationNodeKeyDuplicateCreation, fmt.Sprintf("Cannot create NODE KEY constraint: nodes %s and %s both have composite key %v=%v", firstNodeID, secondNodeID, properties, values), map[string]any{"FirstNodeID": firstNodeID, "SecondNodeID": secondNodeID, "Properties": properties, "Values": values})
}
func StorageValidationExistsPropertyCount(actual int) Message {
	return storageValidationMessage(MessageStorageValidationExistsPropertyCount, fmt.Sprintf("EXISTS constraint requires exactly 1 property, got %d", actual), map[string]any{"Actual": actual})
}
func StorageValidationNodeExistsMissingCreation(nodeID, property string) Message {
	return storageValidationMessage(MessageStorageValidationNodeExistsMissingCreation, fmt.Sprintf("Cannot create EXISTS constraint: node %s is missing required property %s", nodeID, property), map[string]any{"NodeID": nodeID, "Property": property})
}
func StorageValidationTemporalPropertiesExactlyThree() Message {
	return storageValidationMessage(MessageStorageValidationTemporalPropertiesExactlyThree, "TEMPORAL constraint requires 3 properties (key, valid_from, valid_to)", nil)
}
func StorageValidationTemporalNodeKeyNullCreation(nodeID, property string) Message {
	return storageValidationMessage(MessageStorageValidationTemporalNodeKeyNullCreation, fmt.Sprintf("Cannot create TEMPORAL constraint: node %s has null %s", nodeID, property), map[string]any{"NodeID": nodeID, "Property": property})
}
func StorageValidationTemporalNodeInvalidCreation(nodeID, property string) Message {
	return storageValidationMessage(MessageStorageValidationTemporalNodeInvalidCreation, fmt.Sprintf("Cannot create TEMPORAL constraint: node %s has invalid %s", nodeID, property), map[string]any{"NodeID": nodeID, "Property": property})
}
func StorageValidationTemporalNodesOverlapCreation(firstNodeID, secondNodeID string) Message {
	return storageValidationMessage(MessageStorageValidationTemporalNodesOverlapCreation, fmt.Sprintf("Cannot create TEMPORAL constraint: overlap between %s and %s", firstNodeID, secondNodeID), map[string]any{"FirstNodeID": firstNodeID, "SecondNodeID": secondNodeID})
}
func StorageValidationRelationshipUniquePropertyCount() Message {
	return storageValidationMessage(MessageStorageValidationRelationshipUniquePropertyCount, "UNIQUE constraint on relationships requires exactly 1 property", nil)
}
func StorageValidationRelationshipExistsPropertyCount() Message {
	return storageValidationMessage(MessageStorageValidationRelationshipExistsPropertyCount, "EXISTS constraint on relationships requires exactly 1 property", nil)
}
func StorageValidationRelationshipExistsMissing(edgeID, property string) Message {
	return storageValidationMessage(MessageStorageValidationRelationshipExistsMissing, fmt.Sprintf("Cannot create EXISTS constraint on relationship: edge %s is missing required property %s", edgeID, property), map[string]any{"EdgeID": edgeID, "Property": property})
}
func StorageValidationExpectedType(expectedType, actualType string) Message {
	return storageValidationMessage(MessageStorageValidationExpectedType, fmt.Sprintf("expected %s, got %s", expectedType, actualType), map[string]any{"ExpectedType": expectedType, "ActualType": actualType})
}
func StorageValidationUnknownPropertyType(propertyType string) Message {
	return storageValidationMessage(MessageStorageValidationUnknownPropertyType, "unknown property type: "+propertyType, map[string]any{"PropertyType": propertyType})
}
func StorageValidationNodePropertyInvalid(nodeID, property string, cause error) Message {
	return storageValidationMessage(MessageStorageValidationNodePropertyInvalid, fmt.Sprintf("node %s property %s: %s", nodeID, property, storageErrorText(cause)), map[string]any{"NodeID": nodeID, "Property": property, "Cause": storageErrorText(cause)})
}
func StorageValidationRelationshipPropertyInvalid(edgeID, property string, cause error) Message {
	return storageValidationMessage(MessageStorageValidationRelationshipPropertyInvalid, fmt.Sprintf("relationship %s property %s: %s", edgeID, property, storageErrorText(cause)), map[string]any{"EdgeID": edgeID, "Property": property, "Cause": storageErrorText(cause)})
}

func StorageTransactionEngineClosed() Message {
	return storageValidationMessage(MessageStorageTransactionEngineClosed, "engine is closed", nil)
}
func StorageTransactionNamespaceRequired() Message {
	return storageValidationMessage(MessageStorageTransactionNamespaceRequired, "namespace must be non-empty", nil)
}
func StorageTransactionCrossNamespace(pinnedNamespace, attemptedNamespace string) Message {
	return storageValidationMessage(MessageStorageTransactionCrossNamespace, fmt.Sprintf("transaction spans multiple namespaces: pinned to %q, attempted %q", pinnedNamespace, attemptedNamespace), map[string]any{"PinnedNamespace": pinnedNamespace, "AttemptedNamespace": attemptedNamespace})
}
func StorageTransactionIDNamespaceRequired(id string) Message {
	return storageValidationMessage(MessageStorageTransactionIDNamespaceRequired, fmt.Sprintf("ID must be prefixed with namespace (e.g., 'nornic:node-123'), got: %s", id), map[string]any{"ID": id})
}
func StorageTransactionStartNodeMissing(nodeID string) Message {
	return storageValidationMessage(MessageStorageTransactionStartNodeMissing, "start node "+nodeID+" does not exist", map[string]any{"NodeID": nodeID})
}
func StorageTransactionEndNodeMissing(nodeID string) Message {
	return storageValidationMessage(MessageStorageTransactionEndNodeMissing, "end node "+nodeID+" does not exist", map[string]any{"NodeID": nodeID})
}
func StorageTransactionConstraintViolation(cause error) Message {
	return storageValidationMessage(MessageStorageTransactionConstraintViolation, "constraint violation: "+storageErrorText(cause), map[string]any{"Cause": storageErrorText(cause)})
}
func StorageTransactionCommitNamespaceMissing() Message {
	return storageValidationMessage(MessageStorageTransactionCommitNamespaceMissing, "commit: transaction has writes but no pinned namespace", nil)
}
func StorageTransactionCommitConflict(cause error) Message {
	return storageValidationMessage(MessageStorageTransactionCommitConflict, "conflict detected: concurrent transaction modified data before commit: "+storageErrorText(cause), map[string]any{"Cause": storageErrorText(cause)})
}
func StorageTransactionMetadataTooLarge(actual, maximum int) Message {
	return storageValidationMessage(MessageStorageTransactionMetadataTooLarge, fmt.Sprintf("transaction metadata too large: %d chars (max %d)", actual, maximum), map[string]any{"Actual": actual, "Maximum": maximum})
}
func StorageTransactionEdgeChanged(edgeID string) Message {
	return storageValidationMessage(MessageStorageTransactionEdgeChanged, fmt.Sprintf("conflict detected: edge %s changed after transaction start", edgeID), map[string]any{"EdgeID": edgeID})
}
func StorageTransactionNodeChanged(nodeID string) Message {
	return storageValidationMessage(MessageStorageTransactionNodeChanged, fmt.Sprintf("conflict detected: node %s changed after transaction start", nodeID), map[string]any{"NodeID": nodeID})
}
func StorageTransactionNodeChangedDetailed(nodeID, head, readTimestamp string) Message {
	return storageValidationMessage(MessageStorageTransactionNodeChangedDetailed, fmt.Sprintf("conflict detected: node %s changed after transaction start (head=%s, readTS=%s)", nodeID, head, readTimestamp), map[string]any{"NodeID": nodeID, "Head": head, "ReadTimestamp": readTimestamp})
}
func StorageTransactionConcurrentModification() Message {
	return storageValidationMessage(MessageStorageTransactionConcurrentModification, "conflict detected: concurrent transaction modified data before commit", nil)
}
func StorageTransactionEndpointDeleted(nodeID string) Message {
	return storageValidationMessage(MessageStorageTransactionEndpointDeleted, fmt.Sprintf("conflict detected: endpoint node %s was deleted after transaction start", nodeID), map[string]any{"NodeID": nodeID})
}
func StorageTransactionAdjacentEdgeChanged(nodeID, edgeID string) Message {
	return storageValidationMessage(MessageStorageTransactionAdjacentEdgeChanged, fmt.Sprintf("conflict detected: node %s has adjacent edge %s changed after transaction start", nodeID, edgeID), map[string]any{"NodeID": nodeID, "EdgeID": edgeID})
}

func StorageValidationPropertyDomainInvalid(property string, value, allowedValues any) Message {
	return storageValidationMessage(MessageStorageValidationPropertyDomainInvalid, fmt.Sprintf("Property %s value %v is not in allowed values %v", property, value, allowedValues), map[string]any{"Property": property, "Value": value, "AllowedValues": allowedValues})
}
func StorageValidationPropertyTypeRequired(property, expectedType string, cause error) Message {
	return storageValidationMessage(MessageStorageValidationPropertyTypeRequired, fmt.Sprintf("Property %s must be %s (%s)", property, expectedType, storageErrorText(cause)), map[string]any{"Property": property, "ExpectedType": expectedType, "Cause": storageErrorText(cause)})
}
func StorageValidationNodeUniqueInTransaction(property string, value any) Message {
	return storageValidationMessage(MessageStorageValidationNodeUniqueInTransaction, fmt.Sprintf("Node with %s=%v already exists in transaction", property, value), map[string]any{"Property": property, "Value": value})
}
func StorageValidationNodeUniqueExisting(property string, value any, nodeID string) Message {
	return storageValidationMessage(MessageStorageValidationNodeUniqueExisting, fmt.Sprintf("Node with %s=%v already exists (nodeID: %s)", property, value, nodeID), map[string]any{"Property": property, "Value": value, "NodeID": nodeID})
}
func StorageValidationNodeUniqueConcurrent(property string, value any) Message {
	return storageValidationMessage(MessageStorageValidationNodeUniqueConcurrent, fmt.Sprintf("Node with %s=%v already exists (claimed by concurrent commit)", property, value), map[string]any{"Property": property, "Value": value})
}
func StorageValidationNodeKeyNull(property string) Message {
	return storageValidationMessage(MessageStorageValidationNodeKeyNull, "NODE KEY property "+property+" cannot be null", map[string]any{"Property": property})
}
func StorageValidationNodeKeyInTransaction(properties, values any) Message {
	return storageValidationMessage(MessageStorageValidationNodeKeyInTransaction, fmt.Sprintf("Node with key %v=%v already exists in transaction", properties, values), map[string]any{"Properties": properties, "Values": values})
}
func StorageValidationNodeCompositeKeyExisting(properties, values any, nodeID string) Message {
	return storageValidationMessage(MessageStorageValidationNodeCompositeKeyExisting, fmt.Sprintf("Node with composite key %v=%v already exists (nodeID: %s)", properties, values, nodeID), map[string]any{"Properties": properties, "Values": values, "NodeID": nodeID})
}
func StorageValidationRequiredPropertyMissing(property string) Message {
	return storageValidationMessage(MessageStorageValidationRequiredPropertyMissing, "Property "+property+" is required but missing", map[string]any{"Property": property})
}
func StorageValidationTemporalKeyNull(property string) Message {
	return storageValidationMessage(MessageStorageValidationTemporalKeyNull, "TEMPORAL key property "+property+" cannot be null", map[string]any{"Property": property})
}
func StorageValidationTemporalStartInvalid(property string) Message {
	return storageValidationMessage(MessageStorageValidationTemporalStartInvalid, "TEMPORAL start property "+property+" must be a datetime", map[string]any{"Property": property})
}
func StorageValidationTemporalNodeRequired(property, nodeID string) Message {
	return storageValidationMessage(MessageStorageValidationTemporalNodeRequired, fmt.Sprintf("TEMPORAL constraint requires %s for node %s", property, nodeID), map[string]any{"Property": property, "NodeID": nodeID})
}
func StorageValidationTemporalNodeOverlap(nodeID, property string, value any) Message {
	return storageValidationMessage(MessageStorageValidationTemporalNodeOverlap, fmt.Sprintf("TEMPORAL constraint violation: overlap with node %s for %s=%v", nodeID, property, value), map[string]any{"NodeID": nodeID, "Property": property, "Value": value})
}
func StorageValidationRelationshipUniqueExisting(property string, value any, edgeID string) Message {
	return storageValidationMessage(MessageStorageValidationRelationshipUniqueExisting, fmt.Sprintf("Relationship with %s=%v already exists (edgeID: %s)", property, value, edgeID), map[string]any{"Property": property, "Value": value, "EdgeID": edgeID})
}
func StorageValidationRelationshipCompositeExisting(edgeID string) Message {
	return storageValidationMessage(MessageStorageValidationRelationshipCompositeExisting, fmt.Sprintf("Relationship with duplicate composite key already exists (edgeID: %s)", edgeID), map[string]any{"EdgeID": edgeID})
}
func StorageValidationTemporalEdgeOverlap(edgeID string, key any) Message {
	return storageValidationMessage(MessageStorageValidationTemporalEdgeOverlap, fmt.Sprintf("TEMPORAL constraint violation: overlap with edge %s for key=%v", edgeID, key), map[string]any{"EdgeID": edgeID, "Key": key})
}
func StorageValidationCardinalityExceeded(direction string, maxCount int, relationshipType, nodeID string, current int) Message {
	return storageValidationMessage(MessageStorageValidationCardinalityExceeded, fmt.Sprintf("Adding this edge would exceed max %s count of %d for relationship type %s on node %s (current: %d)", direction, maxCount, relationshipType, nodeID, current), map[string]any{"Direction": direction, "MaxCount": maxCount, "RelationshipType": relationshipType, "NodeID": nodeID, "Current": current})
}
func StorageValidationDisallowedPolicy(sourceLabel, relationshipType, targetLabel, constraintName string) Message {
	return storageValidationMessage(MessageStorageValidationDisallowedPolicy, fmt.Sprintf("DISALLOWED policy violation: (:%s)-[:%s]->(:%s) is forbidden (constraint %q)", sourceLabel, relationshipType, targetLabel, constraintName), map[string]any{"SourceLabel": sourceLabel, "RelationshipType": relationshipType, "TargetLabel": targetLabel, "ConstraintName": constraintName})
}
func StorageValidationAllowedPolicy(sourceLabels, relationshipType, targetLabels string) Message {
	return storageValidationMessage(MessageStorageValidationAllowedPolicy, fmt.Sprintf("ALLOWED policy violation: no ALLOWED policy permits (:%s)-[:%s]->(:%s)", sourceLabels, relationshipType, targetLabels), map[string]any{"SourceLabels": sourceLabels, "RelationshipType": relationshipType, "TargetLabels": targetLabels})
}
func StorageValidationLabelChangeDisallowed(sourceLabel, relationshipType, targetLabel, constraintName string) Message {
	return storageValidationMessage(MessageStorageValidationLabelChangeDisallowed, fmt.Sprintf("Label change would violate DISALLOWED policy: (:%s)-[:%s]->(:%s) (constraint %q)", sourceLabel, relationshipType, targetLabel, constraintName), map[string]any{"SourceLabel": sourceLabel, "RelationshipType": relationshipType, "TargetLabel": targetLabel, "ConstraintName": constraintName})
}
func StorageValidationLabelChangeAllowed(sourceLabels, relationshipType, targetLabels string) Message {
	return storageValidationMessage(MessageStorageValidationLabelChangeAllowed, fmt.Sprintf("Label change would violate ALLOWED policy: no ALLOWED policy permits (:%s)-[:%s]->(:%s)", sourceLabels, relationshipType, targetLabels), map[string]any{"SourceLabels": sourceLabels, "RelationshipType": relationshipType, "TargetLabels": targetLabels})
}
