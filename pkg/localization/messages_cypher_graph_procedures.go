package localization

const (
	MessageCypherGraphProceduresStreamNodesFailed              MessageID = "cyphergraphprocedures.stream_nodes_failed"
	MessageCypherGraphProceduresStreamEdgesFailed              MessageID = "cyphergraphprocedures.stream_edges_failed"
	MessageCypherGraphProceduresBuildGraphFailed               MessageID = "cyphergraphprocedures.build_graph_failed"
	MessageCypherGraphProceduresSourceNodeRequired             MessageID = "cyphergraphprocedures.source_node_required"
	MessageCypherGraphProceduresConfigValueOutOfRange          MessageID = "cyphergraphprocedures.config_value_out_of_range"
	MessageCypherGraphProceduresGraphAlreadyExists             MessageID = "cyphergraphprocedures.graph_already_exists"
	MessageCypherGraphProceduresNodeLabelsNotFound             MessageID = "cyphergraphprocedures.node_labels_not_found"
	MessageCypherGraphProceduresRelationshipTypesNotFound      MessageID = "cyphergraphprocedures.relationship_types_not_found"
	MessageCypherGraphProceduresNodePropertiesNotFound         MessageID = "cyphergraphprocedures.node_properties_not_found"
	MessageCypherGraphProceduresRelationshipPropertiesNotFound MessageID = "cyphergraphprocedures.relationship_properties_not_found"
	MessageCypherGraphProceduresUnsupportedOrientation         MessageID = "cyphergraphprocedures.unsupported_orientation"
	MessageCypherGraphProceduresUnsupportedAggregation         MessageID = "cyphergraphprocedures.unsupported_aggregation"
	MessageCypherGraphProceduresInvalidProjection              MessageID = "cyphergraphprocedures.invalid_projection"
	MessageCypherGraphProceduresConfigUnexpectedKey            MessageID = "cyphergraphprocedures.config_unexpected_key"
	MessageCypherGraphProceduresConfigValueMissing             MessageID = "cyphergraphprocedures.config_value_missing"
	MessageCypherGraphProceduresConfigValueType                MessageID = "cyphergraphprocedures.config_value_type"
	MessageCypherGraphProceduresConfigValueNegative            MessageID = "cyphergraphprocedures.config_value_negative"
	MessageCypherGraphProceduresIterationWeightType            MessageID = "cyphergraphprocedures.iteration_weight_type"
	MessageCypherGraphProceduresIterationWeightsEmpty          MessageID = "cyphergraphprocedures.iteration_weights_empty"
	MessageCypherGraphProceduresFeaturePropertiesRequired      MessageID = "cyphergraphprocedures.feature_properties_required"
	MessageCypherGraphProceduresFeaturePropertiesMissing       MessageID = "cyphergraphprocedures.feature_properties_missing"
	MessageCypherGraphProceduresFeaturePropertyInvalid         MessageID = "cyphergraphprocedures.feature_property_invalid"
	MessageCypherGraphProceduresWeightPropertyNotFound         MessageID = "cyphergraphprocedures.weight_property_not_found"
	MessageCypherGraphProceduresRelationshipPropertyMissing    MessageID = "cyphergraphprocedures.relationship_property_missing"
	MessageCypherGraphProceduresNodeLabelsNotInGraph           MessageID = "cyphergraphprocedures.node_labels_not_in_graph"
	MessageCypherGraphProceduresRelationshipTypesNotInGraph    MessageID = "cyphergraphprocedures.relationship_types_not_in_graph"
	MessageCypherGraphProceduresGraphNotFound                  MessageID = "cyphergraphprocedures.graph_not_found"
	MessageCypherGraphProceduresNodePropertyNotInGraph         MessageID = "cyphergraphprocedures.node_property_not_in_graph"
	MessageCypherGraphProceduresStarPropertyRequiresCount      MessageID = "cyphergraphprocedures.star_property_requires_count"
	MessageCypherGraphProceduresJobIDType                      MessageID = "cyphergraphprocedures.job_id_type"
	MessageCypherGraphProceduresSourceNodeNotFound             MessageID = "cyphergraphprocedures.source_node_not_found"
)

// CypherGraphProceduresStreamNodesFailed identifies a wrapped projection node-stream failure.
func CypherGraphProceduresStreamNodesFailed(cause error) Message {
	return Message{ID: MessageCypherGraphProceduresStreamNodesFailed, Fallback: "failed to stream nodes: " + cause.Error(), Data: map[string]any{"Cause": cause.Error()}}
}

// CypherGraphProceduresStreamEdgesFailed identifies a wrapped projection edge-stream failure.
func CypherGraphProceduresStreamEdgesFailed(cause error) Message {
	return Message{ID: MessageCypherGraphProceduresStreamEdgesFailed, Fallback: "failed to stream edges: " + cause.Error(), Data: map[string]any{"Cause": cause.Error()}}
}

// CypherGraphProceduresBuildGraphFailed identifies a wrapped link-prediction graph build failure.
func CypherGraphProceduresBuildGraphFailed(cause error) Message {
	return Message{ID: MessageCypherGraphProceduresBuildGraphFailed, Fallback: "failed to build graph: " + cause.Error(), Data: map[string]any{"Cause": cause.Error()}}
}

// CypherGraphProceduresConfigValueOutOfRange is GDS's message for a config
// value outside its range (FastRP's propertyRatio).
func CypherGraphProceduresConfigValueOutOfRange(key, value, valueRange string) Message {
	return Message{ID: MessageCypherGraphProceduresConfigValueOutOfRange, Fallback: "Value for `" + key + "` was `" + value + "`, but must be within the range " + valueRange + ".", Data: map[string]any{"Key": key, "Value": value, "Range": valueRange}}
}

// CypherGraphProceduresSourceNodeRequired identifies a missing link-prediction sourceNode parameter.
func CypherGraphProceduresSourceNodeRequired() Message {
	return Message{ID: MessageCypherGraphProceduresSourceNodeRequired, Fallback: "sourceNode parameter required"}
}

// CypherGraphProceduresGraphAlreadyExists is GDS's message for projecting a graph name already in use.
func CypherGraphProceduresGraphAlreadyExists(graph string) Message {
	return Message{ID: MessageCypherGraphProceduresGraphAlreadyExists, Fallback: "A graph with name '" + graph + "' already exists.", Data: map[string]any{"Graph": graph}}
}

// CypherGraphProceduresNodeLabelsNotFound is GDS's message for a node projection naming a label no node has.
func CypherGraphProceduresNodeLabelsNotFound(labels string) Message {
	return Message{ID: MessageCypherGraphProceduresNodeLabelsNotFound, Fallback: "Invalid node projection, one or more labels not found: '" + labels + "'", Data: map[string]any{"Labels": labels}}
}

// CypherGraphProceduresRelationshipTypesNotFound is GDS's message for a relationship projection naming a type no relationship has.
func CypherGraphProceduresRelationshipTypesNotFound(types string) Message {
	return Message{ID: MessageCypherGraphProceduresRelationshipTypesNotFound, Fallback: "Invalid relationship projection, one or more relationship types not found: '" + types + "'", Data: map[string]any{"Types": types}}
}

// CypherGraphProceduresNodePropertiesNotFound is GDS's message for projecting node properties no projected node has.
func CypherGraphProceduresNodePropertiesNotFound(properties string) Message {
	return Message{ID: MessageCypherGraphProceduresNodePropertiesNotFound, Fallback: "Node properties not found: '" + properties + "'", Data: map[string]any{"Properties": properties}}
}

// CypherGraphProceduresRelationshipPropertiesNotFound is GDS's message for projecting relationship properties no projected relationship has.
func CypherGraphProceduresRelationshipPropertiesNotFound(properties string) Message {
	return Message{ID: MessageCypherGraphProceduresRelationshipPropertiesNotFound, Fallback: "Relationship properties not found: '" + properties + "' (if you meant to count parallel relationships, use `property:'*'`).", Data: map[string]any{"Properties": properties}}
}

// CypherGraphProceduresUnsupportedOrientation is GDS's message for an unknown relationship orientation.
func CypherGraphProceduresUnsupportedOrientation(orientation string) Message {
	return Message{ID: MessageCypherGraphProceduresUnsupportedOrientation, Fallback: "Orientation `" + orientation + "` is not supported. Must be one of: [NATURAL, REVERSE, UNDIRECTED].", Data: map[string]any{"Orientation": orientation}}
}

// CypherGraphProceduresUnsupportedAggregation is GDS's message for an unknown relationship aggregation.
func CypherGraphProceduresUnsupportedAggregation(aggregation string) Message {
	return Message{ID: MessageCypherGraphProceduresUnsupportedAggregation, Fallback: "Aggregation `" + aggregation + "` is not supported. Must be one of: [DEFAULT, NONE, SINGLE, SUM, MIN, MAX, COUNT].", Data: map[string]any{"Aggregation": aggregation}}
}

// CypherGraphProceduresInvalidProjection is GDS's message for a node or relationship projection of a type it can't read.
func CypherGraphProceduresInvalidProjection(kind, typeName string) Message {
	return Message{ID: MessageCypherGraphProceduresInvalidProjection, Fallback: "Cannot construct a " + kind + " projection out of a " + typeName, Data: map[string]any{"Kind": kind, "Type": typeName}}
}

// CypherGraphProceduresConfigUnexpectedKey is GDS's message for a configuration key the procedure doesn't take.
func CypherGraphProceduresConfigUnexpectedKey(key string) Message {
	return Message{ID: MessageCypherGraphProceduresConfigUnexpectedKey, Fallback: "Unexpected configuration key: " + key, Data: map[string]any{"Key": key}}
}

// CypherGraphProceduresConfigValueMissing is GDS's message for a missing mandatory configuration value.
func CypherGraphProceduresConfigValueMissing(key string) Message {
	return Message{ID: MessageCypherGraphProceduresConfigValueMissing, Fallback: "No value specified for the mandatory configuration parameter `" + key + "`", Data: map[string]any{"Key": key}}
}

// CypherGraphProceduresConfigValueType is GDS's message for a configuration value of the wrong type.
func CypherGraphProceduresConfigValueType(key, expected, actual string) Message {
	return Message{ID: MessageCypherGraphProceduresConfigValueType, Fallback: "The value of `" + key + "` must be of type `" + expected + "` but was `" + actual + "`.", Data: map[string]any{"Key": key, "Expected": expected, "Actual": actual}}
}

// CypherGraphProceduresConfigValueNegative is GDS's message for a negative configuration value that may not be.
func CypherGraphProceduresConfigValueNegative(key string) Message {
	return Message{ID: MessageCypherGraphProceduresConfigValueNegative, Fallback: "The value of `" + key + "` may not be negative.", Data: map[string]any{"Key": key}}
}

// CypherGraphProceduresIterationWeightType is GDS's message for an iteration weight that isn't a number.
func CypherGraphProceduresIterationWeightType(value, typeName string) Message {
	return Message{ID: MessageCypherGraphProceduresIterationWeightType, Fallback: "Iteration weights must be numbers, but found `" + value + "` of type `" + typeName + "`", Data: map[string]any{"Value": value, "Type": typeName}}
}

// CypherGraphProceduresIterationWeightsEmpty is GDS's message for FastRP with no iteration weights and no self-influence.
func CypherGraphProceduresIterationWeightsEmpty() Message {
	return Message{ID: MessageCypherGraphProceduresIterationWeightsEmpty, Fallback: "The value of `iterationWeights` must not be empty if `nodeSelfInfluence` is zero."}
}

// CypherGraphProceduresFeaturePropertiesRequired is GDS's message for a property ratio without feature properties.
func CypherGraphProceduresFeaturePropertiesRequired() Message {
	return Message{ID: MessageCypherGraphProceduresFeaturePropertiesRequired, Fallback: "When `propertyRatio` is non-zero, `featureProperties` may not be empty."}
}

// CypherGraphProceduresFeaturePropertiesMissing is GDS's message for feature properties the graph doesn't hold for every label.
func CypherGraphProceduresFeaturePropertiesMissing(properties, labels, available string) Message {
	return Message{ID: MessageCypherGraphProceduresFeaturePropertiesMissing, Fallback: "The feature properties " + properties + " are not present for all requested labels. Requested labels: " + labels + ". Properties available on all requested labels: " + available, Data: map[string]any{"Properties": properties, "Labels": labels, "Available": available}}
}

// CypherGraphProceduresFeaturePropertyInvalid is GDS's message for a node without a value for a feature property.
func CypherGraphProceduresFeaturePropertyInvalid(node, property string) Message {
	return Message{ID: MessageCypherGraphProceduresFeaturePropertyInvalid, Fallback: "Node with ID `" + node + "` has invalid feature property value `NaN` for property `" + property + "`", Data: map[string]any{"Node": node, "Property": property}}
}

// CypherGraphProceduresWeightPropertyNotFound is GDS's message for a relationship weight property the graph doesn't hold.
func CypherGraphProceduresWeightPropertyNotFound(property, types, available string) Message {
	return Message{ID: MessageCypherGraphProceduresWeightPropertyNotFound, Fallback: "Relationship weight property `" + property + "` not found in relationship types " + types + ". Properties existing on all relationship types: " + available, Data: map[string]any{"Property": property, "Types": types, "Available": available}}
}

// CypherGraphProceduresRelationshipPropertyMissing is GDS's message for a relationship without a value for the weight property.
func CypherGraphProceduresRelationshipPropertyMissing(property, source, target string) Message {
	return Message{ID: MessageCypherGraphProceduresRelationshipPropertyMissing, Fallback: "Missing relationship property `" + property + "` on relationship between nodes with ids `" + source + "` and `" + target + "`.", Data: map[string]any{"Property": property, "Source": source, "Target": target}}
}

// CypherGraphProceduresNodeLabelsNotInGraph is GDS's message for an algorithm's nodeLabels the graph doesn't hold.
func CypherGraphProceduresNodeLabelsNotInGraph(labels, available string) Message {
	return Message{ID: MessageCypherGraphProceduresNodeLabelsNotInGraph, Fallback: "Could not find the specified `nodeLabels` of " + labels + ". Available labels are " + available + ".", Data: map[string]any{"Labels": labels, "Available": available}}
}

// CypherGraphProceduresRelationshipTypesNotInGraph is GDS's message for an algorithm's relationshipTypes the graph doesn't hold.
func CypherGraphProceduresRelationshipTypesNotInGraph(types, available string) Message {
	return Message{ID: MessageCypherGraphProceduresRelationshipTypesNotInGraph, Fallback: "Could not find the specified `relationshipTypes` of " + types + ". Available relationship types are " + available + ".", Data: map[string]any{"Types": types, "Available": available}}
}

// CypherGraphProceduresSourceNodeNotFound identifies a link prediction sourceNode the graph doesn't hold.
func CypherGraphProceduresSourceNodeNotFound(node string) Message {
	return Message{ID: MessageCypherGraphProceduresSourceNodeNotFound, Fallback: "sourceNode `" + node + "` does not exist in the graph", Data: map[string]any{"Node": node}}
}

// CypherGraphProceduresGraphNotFound is GDS's message for a graph name the
// database's catalog doesn't hold.
func CypherGraphProceduresGraphNotFound(graph, database string) Message {
	return Message{ID: MessageCypherGraphProceduresGraphNotFound, Fallback: "Graph with name `" + graph + "` does not exist on database `" + database + "`. It might exist on another database.", Data: map[string]any{"Graph": graph, "Database": database}}
}

// CypherGraphProceduresNodePropertyNotInGraph is GDS's message for
// gds.util.nodeProperty of a property the graph doesn't hold.
func CypherGraphProceduresNodePropertyNotInGraph(property string) Message {
	return Message{ID: MessageCypherGraphProceduresNodePropertyNotInGraph, Fallback: "No node projection with property '" + property + "' exists.", Data: map[string]any{"Property": property}}
}

// CypherGraphProceduresStarPropertyRequiresCount is GDS's message for a '*'
// relationship property without COUNT aggregation.
func CypherGraphProceduresStarPropertyRequiresCount() Message {
	return Message{ID: MessageCypherGraphProceduresStarPropertyRequiresCount, Fallback: "A '*' property key can only be used in combination with count aggregation."}
}

// CypherGraphProceduresJobIDType is GDS's message for a jobId that isn't a
// STRING.
func CypherGraphProceduresJobIDType(typeName string) Message {
	return Message{ID: MessageCypherGraphProceduresJobIDType, Fallback: "Expected JobId or String. Got " + typeName + ".", Data: map[string]any{"Type": typeName}}
}
