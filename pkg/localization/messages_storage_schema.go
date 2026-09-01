package localization

import (
	"fmt"
	"strconv"
)

const (
	MessageStorageSchemaConstraintAlreadyExists             MessageID = "storageschema.constraint_already_exists"
	MessageStorageSchemaConstraintDifferentAllowedValues    MessageID = "storageschema.constraint_different_allowed_values"
	MessageStorageSchemaConstraintDifferentMaxCount         MessageID = "storageschema.constraint_different_max_count"
	MessageStorageSchemaConstraintDifferentSchemaOrType     MessageID = "storageschema.constraint_different_schema_or_type"
	MessageStorageSchemaConflictingPolicy                   MessageID = "storageschema.conflicting_policy"
	MessageStorageSchemaConflictingDomainConstraint         MessageID = "storageschema.conflicting_domain_constraint"
	MessageStorageSchemaConflictingCardinalityConstraint    MessageID = "storageschema.conflicting_cardinality_constraint"
	MessageStorageSchemaEquivalentConstraintAlreadyExists   MessageID = "storageschema.equivalent_constraint_already_exists"
	MessageStorageSchemaConflictingConstraintAlreadyExists  MessageID = "storageschema.conflicting_constraint_already_exists"
	MessageStorageSchemaUniqueConstraintViolation           MessageID = "storageschema.unique_constraint_violation"
	MessageStorageSchemaCompositeIndexMinProperties         MessageID = "storageschema.composite_index_min_properties"
	MessageStorageSchemaRangeIndexPropertiesRequired        MessageID = "storageschema.range_index_properties_required"
	MessageStorageSchemaRangeIndexNotFound                  MessageID = "storageschema.range_index_not_found"
	MessageStorageSchemaRangeIndexNumericValueRequired      MessageID = "storageschema.range_index_numeric_value_required"
	MessageStorageSchemaIndexNotFound                       MessageID = "storageschema.index_not_found"
	MessageStorageSchemaConstraintNotFound                  MessageID = "storageschema.constraint_not_found"
	MessageStorageSchemaPropertyIndexNotFound               MessageID = "storageschema.property_index_not_found"
	MessageStorageSchemaDecayProfileBundleAlreadyExists     MessageID = "storageschema.decay_profile_bundle_already_exists"
	MessageStorageSchemaDecayProfileBindingAlreadyExists    MessageID = "storageschema.decay_profile_binding_already_exists"
	MessageStorageSchemaDecayProfileBundleNotFound          MessageID = "storageschema.decay_profile_bundle_not_found"
	MessageStorageSchemaDecayProfileBundleReferenced        MessageID = "storageschema.decay_profile_bundle_referenced"
	MessageStorageSchemaDecayProfileNotFound                MessageID = "storageschema.decay_profile_not_found"
	MessageStorageSchemaPromotionProfileAlreadyExists       MessageID = "storageschema.promotion_profile_already_exists"
	MessageStorageSchemaPromotionProfileNotFound            MessageID = "storageschema.promotion_profile_not_found"
	MessageStorageSchemaPromotionProfileReferenced          MessageID = "storageschema.promotion_profile_referenced"
	MessageStorageSchemaPromotionPolicyAlreadyExists        MessageID = "storageschema.promotion_policy_already_exists"
	MessageStorageSchemaPromotionProfileWhenClauseNotFound  MessageID = "storageschema.promotion_profile_when_clause_not_found"
	MessageStorageSchemaPromotionPolicyNotFound             MessageID = "storageschema.promotion_policy_not_found"
	MessageStorageSchemaDecayEdgeBindingConflict            MessageID = "storageschema.decay_edge_binding_conflict"
	MessageStorageSchemaDecayLabelBindingConflict           MessageID = "storageschema.decay_label_binding_conflict"
	MessageStorageSchemaDecayStructuralIndexConflict        MessageID = "storageschema.decay_structural_index_conflict"
	MessageStorageSchemaDecayProfileBundleNameRequired      MessageID = "storageschema.decay_profile_bundle_name_required"
	MessageStorageSchemaInvalidDecayFunction                MessageID = "storageschema.invalid_decay_function"
	MessageStorageSchemaInvalidScoreFromMode                MessageID = "storageschema.invalid_score_from_mode"
	MessageStorageSchemaInvalidScopeType                    MessageID = "storageschema.invalid_scope_type"
	MessageStorageSchemaScoreFromPropertyRequired           MessageID = "storageschema.score_from_property_required"
	MessageStorageSchemaVisibilityThresholdOutOfRange       MessageID = "storageschema.visibility_threshold_out_of_range"
	MessageStorageSchemaScoreFloorOutOfRange                MessageID = "storageschema.score_floor_out_of_range"
	MessageStorageSchemaPromotionProfileNameRequired        MessageID = "storageschema.promotion_profile_name_required"
	MessageStorageSchemaMultiplierNonNegative               MessageID = "storageschema.multiplier_non_negative"
	MessageStorageSchemaScoreCapOutOfRange                  MessageID = "storageschema.score_cap_out_of_range"
	MessageStorageSchemaUnknownOption                       MessageID = "storageschema.unknown_option"
	MessageStorageSchemaConstraintContractAlreadyExists     MessageID = "storageschema.constraint_contract_already_exists"
	MessageStorageSchemaConstraintContractNameConflict      MessageID = "storageschema.constraint_contract_name_conflict"
	MessageStorageSchemaScanNodesFailed                     MessageID = "storageschema.scan_nodes_failed"
	MessageStorageSchemaScanRelationshipsFailed             MessageID = "storageschema.scan_relationships_failed"
	MessageStorageSchemaUnsupportedContractTargetEntityType MessageID = "storageschema.unsupported_contract_target_entity_type"
	MessageStorageSchemaConstraintContractInvalid           MessageID = "storageschema.constraint_contract_invalid"
	MessageStorageSchemaConstraintContractViolated          MessageID = "storageschema.constraint_contract_violated"
	MessageStorageSchemaUnsupportedNodePredicate            MessageID = "storageschema.unsupported_node_predicate"
	MessageStorageSchemaMissingRelationshipEndpoint         MessageID = "storageschema.missing_relationship_endpoint"
	MessageStorageSchemaUnsupportedRelationshipPredicate    MessageID = "storageschema.unsupported_relationship_predicate"
	MessageStorageSchemaUnsupportedConstraintPattern        MessageID = "storageschema.unsupported_constraint_pattern"
	MessageStorageSchemaUnsupportedConstraintLiteral        MessageID = "storageschema.unsupported_constraint_literal"
)

func storageSchemaMessage(id MessageID, fallback string, data map[string]any) Message {
	return Message{ID: id, Fallback: fallback, Data: data}
}

func StorageSchemaConstraintAlreadyExists(name string) Message {
	return storageSchemaMessage(MessageStorageSchemaConstraintAlreadyExists, "constraint "+strconv.Quote(name)+" already exists", map[string]any{"Name": name})
}

func StorageSchemaConstraintDifferentAllowedValues(name string) Message {
	return storageSchemaMessage(MessageStorageSchemaConstraintDifferentAllowedValues, "constraint "+strconv.Quote(name)+" already exists with different allowed values", map[string]any{"Name": name})
}

func StorageSchemaConstraintDifferentMaxCount(name string, existing, requested int) Message {
	fallback := "constraint " + strconv.Quote(name) + " already exists with different max count (" + strconv.Itoa(existing) + " vs " + strconv.Itoa(requested) + ")"
	return storageSchemaMessage(MessageStorageSchemaConstraintDifferentMaxCount, fallback, map[string]any{"Name": name, "ExistingMaxCount": existing, "RequestedMaxCount": requested})
}

func StorageSchemaConstraintDifferentSchemaOrType(name string) Message {
	return storageSchemaMessage(MessageStorageSchemaConstraintDifferentSchemaOrType, "constraint "+strconv.Quote(name)+" already exists with different schema or type", map[string]any{"Name": name})
}

func StorageSchemaConflictingPolicy(sourceLabel, relationshipType, targetLabel, existingName string) Message {
	fallback := "conflicting policy: cannot have both ALLOWED and DISALLOWED for " + sourceLabel + "-[:" + relationshipType + "]->" + targetLabel + " (constraint " + strconv.Quote(existingName) + ")"
	return storageSchemaMessage(MessageStorageSchemaConflictingPolicy, fallback, map[string]any{"SourceLabel": sourceLabel, "RelationshipType": relationshipType, "TargetLabel": targetLabel, "ExistingName": existingName})
}

func StorageSchemaConflictingDomainConstraint(name string) Message {
	return storageSchemaMessage(MessageStorageSchemaConflictingDomainConstraint, "conflicting domain constraint "+strconv.Quote(name)+" already exists on same schema with different allowed values", map[string]any{"Name": name})
}

func StorageSchemaConflictingCardinalityConstraint(name, direction, label string, existing, requested int) Message {
	fallback := "conflicting cardinality constraint " + strconv.Quote(name) + " already exists on " + direction + " " + label + " with max count " + strconv.Itoa(existing) + " (new: " + strconv.Itoa(requested) + ")"
	return storageSchemaMessage(MessageStorageSchemaConflictingCardinalityConstraint, fallback, map[string]any{"Name": name, "Direction": direction, "Label": label, "ExistingMaxCount": existing, "RequestedMaxCount": requested})
}

func StorageSchemaEquivalentConstraintAlreadyExists(name string) Message {
	return storageSchemaMessage(MessageStorageSchemaEquivalentConstraintAlreadyExists, "equivalent constraint "+strconv.Quote(name)+" already exists on same schema", map[string]any{"Name": name})
}

func StorageSchemaConflictingConstraintAlreadyExists(name string) Message {
	return storageSchemaMessage(MessageStorageSchemaConflictingConstraintAlreadyExists, "conflicting constraint "+strconv.Quote(name)+" already exists on same schema", map[string]any{"Name": name})
}

func StorageSchemaUniqueConstraintViolation(label, property string, value any) Message {
	fallback := "Node(" + label + ") already exists with " + property + " = " + fmt.Sprint(value)
	return storageSchemaMessage(MessageStorageSchemaUniqueConstraintViolation, fallback, map[string]any{"Label": label, "Property": property, "Value": value})
}

func StorageSchemaCompositeIndexMinProperties(count int) Message {
	return storageSchemaMessage(MessageStorageSchemaCompositeIndexMinProperties, "composite index requires at least 2 properties, got "+strconv.Itoa(count), map[string]any{"Count": count})
}

func StorageSchemaRangeIndexPropertiesRequired() Message {
	return storageSchemaMessage(MessageStorageSchemaRangeIndexPropertiesRequired, "range index requires at least one property", nil)
}

func StorageSchemaRangeIndexNotFound(name string) Message {
	return storageSchemaMessage(MessageStorageSchemaRangeIndexNotFound, "range index "+name+" not found", map[string]any{"Name": name})
}

func StorageSchemaRangeIndexNumericValueRequired(value any) Message {
	valueType := fmt.Sprintf("%T", value)
	return storageSchemaMessage(MessageStorageSchemaRangeIndexNumericValueRequired, "range index only supports numeric values, got "+valueType, map[string]any{"Type": valueType})
}

func StorageSchemaIndexNotFound(name string) Message {
	return storageSchemaMessage(MessageStorageSchemaIndexNotFound, "index "+strconv.Quote(name)+" does not exist", map[string]any{"Name": name})
}

func StorageSchemaConstraintNotFound(name string) Message {
	return storageSchemaMessage(MessageStorageSchemaConstraintNotFound, "constraint "+strconv.Quote(name)+" does not exist", map[string]any{"Name": name})
}

func StorageSchemaPropertyIndexNotFound(label, property string) Message {
	return storageSchemaMessage(MessageStorageSchemaPropertyIndexNotFound, "property index "+label+":"+property+" not found", map[string]any{"Label": label, "Property": property})
}

func StorageSchemaDecayProfileBundleAlreadyExists(name string) Message {
	return storageSchemaMessage(MessageStorageSchemaDecayProfileBundleAlreadyExists, "decay profile bundle "+strconv.Quote(name)+" already exists", map[string]any{"Name": name})
}

func StorageSchemaDecayProfileBindingAlreadyExists(name string) Message {
	return storageSchemaMessage(MessageStorageSchemaDecayProfileBindingAlreadyExists, "decay profile binding "+strconv.Quote(name)+" already exists", map[string]any{"Name": name})
}

func StorageSchemaDecayProfileBundleNotFound(name string) Message {
	return storageSchemaMessage(MessageStorageSchemaDecayProfileBundleNotFound, "decay profile bundle "+strconv.Quote(name)+" not found", map[string]any{"Name": name})
}

func StorageSchemaDecayProfileBundleReferenced(name string) Message {
	return storageSchemaMessage(MessageStorageSchemaDecayProfileBundleReferenced, "cannot drop decay profile bundle "+strconv.Quote(name)+": referenced by active binding", map[string]any{"Name": name})
}

func StorageSchemaDecayProfileNotFound(name string) Message {
	return storageSchemaMessage(MessageStorageSchemaDecayProfileNotFound, "decay profile "+strconv.Quote(name)+" not found", map[string]any{"Name": name})
}

func StorageSchemaPromotionProfileAlreadyExists(name string) Message {
	return storageSchemaMessage(MessageStorageSchemaPromotionProfileAlreadyExists, "promotion profile "+strconv.Quote(name)+" already exists", map[string]any{"Name": name})
}

func StorageSchemaPromotionProfileNotFound(name string) Message {
	return storageSchemaMessage(MessageStorageSchemaPromotionProfileNotFound, "promotion profile "+strconv.Quote(name)+" not found", map[string]any{"Name": name})
}

func StorageSchemaPromotionProfileReferenced(name string) Message {
	return storageSchemaMessage(MessageStorageSchemaPromotionProfileReferenced, "cannot drop promotion profile "+strconv.Quote(name)+": referenced by active promotion policy", map[string]any{"Name": name})
}

func StorageSchemaPromotionPolicyAlreadyExists(name string) Message {
	return storageSchemaMessage(MessageStorageSchemaPromotionPolicyAlreadyExists, "promotion policy "+strconv.Quote(name)+" already exists", map[string]any{"Name": name})
}

func StorageSchemaPromotionProfileWhenClauseNotFound(name string) Message {
	return storageSchemaMessage(MessageStorageSchemaPromotionProfileWhenClauseNotFound, "promotion profile "+strconv.Quote(name)+" not found (referenced in WHEN clause)", map[string]any{"Name": name})
}

func StorageSchemaPromotionPolicyNotFound(name string) Message {
	return storageSchemaMessage(MessageStorageSchemaPromotionPolicyNotFound, "promotion policy "+strconv.Quote(name)+" not found", map[string]any{"Name": name})
}

func StorageSchemaDecayEdgeBindingConflict(edgeType, bindingName string) Message {
	fallback := "edge type " + strconv.Quote(edgeType) + " already has a decay profile binding " + strconv.Quote(bindingName)
	return storageSchemaMessage(MessageStorageSchemaDecayEdgeBindingConflict, fallback, map[string]any{"EdgeType": edgeType, "BindingName": bindingName})
}

func StorageSchemaDecayLabelBindingConflict(labels any, bindingName string) Message {
	fallback := "label set " + fmt.Sprint(labels) + " already has a decay profile binding " + strconv.Quote(bindingName)
	return storageSchemaMessage(MessageStorageSchemaDecayLabelBindingConflict, fallback, map[string]any{"Labels": labels, "BindingName": bindingName})
}

func StorageSchemaDecayStructuralIndexConflict(property string) Message {
	return storageSchemaMessage(MessageStorageSchemaDecayStructuralIndexConflict, "property "+strconv.Quote(property)+" is in a structural index and cannot have a decay rule", map[string]any{"Property": property})
}

func StorageSchemaDecayProfileBundleNameRequired() Message {
	return storageSchemaMessage(MessageStorageSchemaDecayProfileBundleNameRequired, "decay profile bundle name is required", nil)
}

func StorageSchemaInvalidDecayFunction(function any) Message {
	return storageSchemaMessage(MessageStorageSchemaInvalidDecayFunction, "invalid decay function: "+strconv.Quote(fmt.Sprint(function)), map[string]any{"Function": function})
}

func StorageSchemaInvalidScoreFromMode(mode any) Message {
	return storageSchemaMessage(MessageStorageSchemaInvalidScoreFromMode, "invalid score-from mode: "+strconv.Quote(fmt.Sprint(mode)), map[string]any{"Mode": mode})
}

func StorageSchemaInvalidScopeType(scope any) Message {
	return storageSchemaMessage(MessageStorageSchemaInvalidScopeType, "invalid scope type: "+strconv.Quote(fmt.Sprint(scope)), map[string]any{"Scope": scope})
}

func StorageSchemaScoreFromPropertyRequired() Message {
	return storageSchemaMessage(MessageStorageSchemaScoreFromPropertyRequired, "scoreFromProperty is required when scoreFrom is CUSTOM", nil)
}

func StorageSchemaVisibilityThresholdOutOfRange(value float64) Message {
	return storageSchemaMessage(MessageStorageSchemaVisibilityThresholdOutOfRange, fmt.Sprintf("visibilityThreshold must be between 0 and 1, got %f", value), map[string]any{"Value": value})
}

func StorageSchemaScoreFloorOutOfRange(value float64) Message {
	return storageSchemaMessage(MessageStorageSchemaScoreFloorOutOfRange, fmt.Sprintf("scoreFloor must be between 0 and 1, got %f", value), map[string]any{"Value": value})
}

func StorageSchemaPromotionProfileNameRequired() Message {
	return storageSchemaMessage(MessageStorageSchemaPromotionProfileNameRequired, "promotion profile name is required", nil)
}

func StorageSchemaMultiplierNonNegative(value float64) Message {
	return storageSchemaMessage(MessageStorageSchemaMultiplierNonNegative, fmt.Sprintf("multiplier must be non-negative, got %f", value), map[string]any{"Value": value})
}

func StorageSchemaScoreCapOutOfRange(value float64) Message {
	return storageSchemaMessage(MessageStorageSchemaScoreCapOutOfRange, fmt.Sprintf("scoreCap must be between 0 and 1, got %f", value), map[string]any{"Value": value})
}

func StorageSchemaUnknownOption(option string) Message {
	return storageSchemaMessage(MessageStorageSchemaUnknownOption, "unknown option: "+strconv.Quote(option), map[string]any{"Option": option})
}

func StorageSchemaConstraintContractAlreadyExists(name string) Message {
	return storageSchemaMessage(MessageStorageSchemaConstraintContractAlreadyExists, "constraint contract "+strconv.Quote(name)+" already exists", map[string]any{"Name": name})
}

func StorageSchemaConstraintContractNameConflict(name string) Message {
	return storageSchemaMessage(MessageStorageSchemaConstraintContractNameConflict, "constraint contract "+strconv.Quote(name)+" conflicts with an existing constraint name", map[string]any{"Name": name})
}

func StorageSchemaScanNodesFailed(cause error) Message {
	return storageSchemaMessage(MessageStorageSchemaScanNodesFailed, "scanning nodes: "+cause.Error(), map[string]any{"Cause": cause.Error()})
}

func StorageSchemaScanRelationshipsFailed(cause error) Message {
	return storageSchemaMessage(MessageStorageSchemaScanRelationshipsFailed, "scanning relationships: "+cause.Error(), map[string]any{"Cause": cause.Error()})
}

func StorageSchemaUnsupportedContractTargetEntityType(entityType string) Message {
	return storageSchemaMessage(MessageStorageSchemaUnsupportedContractTargetEntityType, "unsupported constraint contract target entity type: "+entityType, map[string]any{"EntityType": entityType})
}

func StorageSchemaConstraintContractInvalid(name, predicate string, cause error) Message {
	fallback := "constraint contract " + name + " invalid: predicate " + strconv.Quote(predicate) + ": " + cause.Error()
	return storageSchemaMessage(MessageStorageSchemaConstraintContractInvalid, fallback, map[string]any{"Name": name, "Predicate": predicate, "Cause": cause.Error()})
}

func StorageSchemaConstraintContractViolated(name, predicate string) Message {
	fallback := "constraint contract " + name + " violated: predicate " + strconv.Quote(predicate) + " evaluated to false"
	return storageSchemaMessage(MessageStorageSchemaConstraintContractViolated, fallback, map[string]any{"Name": name, "Predicate": predicate})
}

func StorageSchemaUnsupportedNodePredicate() Message {
	return storageSchemaMessage(MessageStorageSchemaUnsupportedNodePredicate, "unsupported node predicate", nil)
}

func StorageSchemaMissingRelationshipEndpoint() Message {
	return storageSchemaMessage(MessageStorageSchemaMissingRelationshipEndpoint, "missing relationship endpoint", nil)
}

func StorageSchemaUnsupportedRelationshipPredicate() Message {
	return storageSchemaMessage(MessageStorageSchemaUnsupportedRelationshipPredicate, "unsupported relationship predicate", nil)
}

func StorageSchemaUnsupportedConstraintPattern(pattern string) Message {
	return storageSchemaMessage(MessageStorageSchemaUnsupportedConstraintPattern, "unsupported pattern "+strconv.Quote(pattern), map[string]any{"Pattern": pattern})
}

func StorageSchemaUnsupportedConstraintLiteral(literal string) Message {
	return storageSchemaMessage(MessageStorageSchemaUnsupportedConstraintLiteral, "unsupported literal "+strconv.Quote(literal), map[string]any{"Literal": literal})
}
