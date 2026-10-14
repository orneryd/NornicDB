package localization

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/text/language"
)

func TestCypherGraphProceduresDescriptorsPreserveExactEnglish(t *testing.T) {
	cause := errors.New("forced graph failure")
	testCases := []struct {
		message Message
		text    string
	}{
		{CypherGraphProceduresStreamNodesFailed(cause), "failed to stream nodes: forced graph failure"},
		{CypherGraphProceduresStreamEdgesFailed(cause), "failed to stream edges: forced graph failure"},
		{CypherGraphProceduresGraphNotFound("missing", "neo4j"), "Graph with name `missing` does not exist on database `neo4j`. It might exist on another database."},
		{CypherGraphProceduresRelationshipEndNotProjected("source", "n1"), "Failed to load a relationship because its source-node with id n1 is not part of the node query or projection. To ignore the relationship, set the configuration parameter `validateRelationships` to false."},
		{CypherGraphProceduresUnsupportedArrayType("StringArray"), "Loading of values of type StringArray is currently not supported"},
		{CypherGraphProceduresUnsafeConversion("DoubleArray[1.0, 2.0]", "a Double"), "Cannot safely convert DoubleArray[1.0, 2.0] into a Double."},
		{CypherGraphProceduresArrayLengthsDiffer("v", "2", "1"), "The property `v` contains arrays of differing lengths `2` and `1`."},
		{CypherGraphProceduresUnsafeLongConversion("2.50"), "Cannot safely convert 2.50 into an long value"},
		{CypherGraphProceduresRelationshipTypeEmpty(), "Relationship type cannot be empty"},
		{CypherGraphProceduresInverseTypesNotProjected("[NOPE, X]", "['R']"), "Specified inverseIndexedRelationshipTypes `[NOPE, X]` were not projected in the graph. Projected types are: `['R']`."},
		{CypherGraphProceduresConcurrencyBelowOne("0"), "Valid values for Concurrency are int[1..], Value provided was `0`."},
		{CypherGraphProceduresUnsafeLongArrayConversion("DoubleArray[1.5]", "1.50"), "Cannot safely convert DoubleArray[1.5] into a Long Array. Cannot safely convert 1.50 into an long value"},
		{CypherGraphProceduresNodePropertyNotInGraph("nope"), "No node projection with property 'nope' exists."},
		{CypherGraphProceduresStarPropertyRequiresCount(), "A '*' property key can only be used in combination with count aggregation."},
		{CypherGraphProceduresJobIDType("Long"), "Expected JobId or String. Got Long."},
		{CypherGraphProceduresNodeNotNodeOrInteger("NO_VALUE"), "The node has to be either a NODE or an INTEGER, but got NO_VALUE"},
		{CypherGraphProceduresConfigKeyMissingPair("targetNodeLabels", "sourceNodeLabels"), "The configuration key 'targetNodeLabels' is missing, but 'sourceNodeLabels' is provided. If you really meant to only provide `sourceNodeLabels` with no value for `targetNodeLabels`, you can set `targetNodeLabels` to `NULL`."},
		{CypherGraphProceduresRelationshipTypeNotString("NUMBER"), "The value of `relationshipType` must be `String`, but was `NUMBER`."},
		{CypherGraphProceduresUndirectedTypesNotProjected("[NOPE]", "['__ALL__']"), "Specified undirectedRelationshipTypes `[NOPE]` were not projected in the graph. Projected types are: `['__ALL__']`."},
		{CypherGraphProceduresLabelsTrueDeprecated("sourceNodeLabels", "s"), "Using `true` to load all labels is deprecated, use `{ sourceNodeLabels: labels(s) }` instead"},
		{CypherGraphProceduresUnsupportedConcurrency("String"), "Unsupported Concurrency input of type String"},
		{CypherGraphProceduresConfigParametersMerged(), "The parameters for `nodesConfig` and `relationshipsConfig` have been merged. Update your query by merging the 4th and 5th parameter into one parameter."},
		{CypherGraphProceduresGraphExistsAggregation("a3"), "Graph a3 already exists"},
		{CypherGraphProceduresPropertyTypeUnknown(), "Cannot infer type of property"},
		{CypherGraphProceduresUnsupportedGDSValue("String"), "Unsupported conversion to GDS Value from Neo4j Value with type `String`."},
		{CypherGraphProceduresBuildGraphFailed(cause), "failed to build graph: forced graph failure"},
		{CypherGraphProceduresSourceNodeRequired(), "sourceNode parameter required"},
		{CypherGraphProceduresConfigValueOutOfRange("propertyRatio", "2", "[0.00, 1.00]"), "Value for `propertyRatio` was `2`, but must be within the range [0.00, 1.00]."},
		{CypherGraphProceduresGraphAlreadyExists("g"), "A graph with name 'g' already exists."},
		{CypherGraphProceduresNodeLabelsNotFound("P, Q"), "Invalid node projection, one or more labels not found: 'P, Q'"},
		{CypherGraphProceduresRelationshipTypesNotFound("K|L"), "Invalid relationship projection, one or more relationship types not found: 'K|L'"},
		{CypherGraphProceduresNodePropertiesNotFound("nope"), "Node properties not found: 'nope'"},
		{CypherGraphProceduresRelationshipPropertiesNotFound("nope"), "Relationship properties not found: 'nope' (if you meant to count parallel relationships, use `property:'*'`)."},
		{CypherGraphProceduresUnsupportedOrientation("FOO"), "Orientation `FOO` is not supported. Must be one of: [NATURAL, REVERSE, UNDIRECTED]."},
		{CypherGraphProceduresUnsupportedAggregation("FOO"), "Aggregation `FOO` is not supported. Must be one of: [DEFAULT, NONE, SINGLE, SUM, MIN, MAX, COUNT]."},
		{CypherGraphProceduresInvalidProjection("node", "java.lang.Long"), "Cannot construct a node projection out of a java.lang.Long"},
		{CypherGraphProceduresConfigUnexpectedKey("bogus"), "Unexpected configuration key: bogus"},
		{CypherGraphProceduresConfigValueMissing("embeddingDimension"), "No value specified for the mandatory configuration parameter `embeddingDimension`"},
		{CypherGraphProceduresConfigValueType("topK", "Integer", "String"), "The value of `topK` must be of type `Integer` but was `String`."},
		{CypherGraphProceduresConfigValueNegative("nodeSelfInfluence"), "The value of `nodeSelfInfluence` may not be negative."},
		{CypherGraphProceduresIterationWeightType("x", "String"), "Iteration weights must be numbers, but found `x` of type `String`"},
		{CypherGraphProceduresIterationWeightsEmpty(), "The value of `iterationWeights` must not be empty if `nodeSelfInfluence` is zero."},
		{CypherGraphProceduresFeaturePropertiesRequired(), "When `propertyRatio` is non-zero, `featureProperties` may not be empty."},
		{CypherGraphProceduresFeaturePropertiesMissing("['age']", "['P']", "[]"), "The feature properties ['age'] are not present for all requested labels. Requested labels: ['P']. Properties available on all requested labels: []"},
		{CypherGraphProceduresFeaturePropertyInvalid("2", "age", "`NaN`"), "Node with ID `2` has invalid feature property value `NaN` for property `age`"},
		{CypherGraphProceduresFeaturePropertyInvalid("2", "age", "NaN"), "Node with ID `2` has invalid feature property value NaN for property `age`"},
		{CypherGraphProceduresPropertyReadType("LONG", "DOUBLE"), "Tried to retrieve a value of type LONG value from properties of type DOUBLE"},
		{CypherGraphProceduresRelationshipValueType("NUMBER_ARRAY", "LongArray[1]"), "Unsupported type [NUMBER_ARRAY] of value LongArray[1]. Please use a numeric property."},
		{CypherGraphProceduresDefaultPropertyType("long[]", "Double"), "Expected type of default value to be `long[]`. But got `Double`."},
		{CypherGraphProceduresWeightPropertyNotFound("nope", "['K']", "['weight']"), "Relationship weight property `nope` not found in relationship types ['K']. Properties existing on all relationship types: ['weight']"},
		{CypherGraphProceduresRelationshipPropertyMissing("weight", "1", "2"), "Missing relationship property `weight` on relationship between nodes with ids `1` and `2`."},
		{CypherGraphProceduresNodeLabelsNotInGraph("['Nope']", "['P']"), "Could not find the specified `nodeLabels` of ['Nope']. Available labels are ['P']."},
		{CypherGraphProceduresRelationshipTypesNotInGraph("['Nope']", "['K']"), "Could not find the specified `relationshipTypes` of ['Nope']. Available relationship types are ['K']."},
		{CypherGraphProceduresSourceNodeNotFound("x"), "sourceNode `x` does not exist in the graph"},
	}

	require.Len(t, testCases, 54)
	manager, err := NewManager([]language.Tag{language.AmericanEnglish}, nil)
	require.NoError(t, err)
	for _, testCase := range testCases {
		require.Equal(t, testCase.text, testCase.message.Fallback, testCase.message.ID)
		rendered, tag, err := manager.Render(WithPreferences(context.Background(), language.AmericanEnglish), testCase.message)
		require.NoError(t, err, testCase.message.ID)
		require.Equal(t, language.AmericanEnglish, tag, testCase.message.ID)
		require.Equal(t, testCase.text, rendered, testCase.message.ID)
	}
}

func TestCypherGraphProceduresLocalizedErrorsPreserveCauseIdentity(t *testing.T) {
	cause := errors.New("forced graph failure")
	messages := []Message{
		CypherGraphProceduresStreamNodesFailed(cause),
		CypherGraphProceduresStreamEdgesFailed(cause),
		CypherGraphProceduresBuildGraphFailed(cause),
	}

	for _, message := range messages {
		err := NewLocalizedError(string(message.ID), message, cause)
		require.ErrorIs(t, err, cause, message.ID)
		var localizedErr *LocalizedError
		require.ErrorAs(t, err, &localizedErr, message.ID)
		require.Equal(t, message.ID, localizedErr.Message.ID)
		require.Equal(t, cause.Error(), localizedErr.Message.Data["Cause"])
	}
}

func TestCypherGraphProceduresCatalogRendering(t *testing.T) {
	manager, err := NewManager([]language.Tag{language.AmericanEnglish}, nil)
	require.NoError(t, err)
	message := CypherGraphProceduresGraphNotFound("missing", "neo4j")

	spanish, tag, err := manager.Render(WithPreferences(context.Background(), language.EuropeanSpanish), message)
	require.NoError(t, err)
	require.Equal(t, language.EuropeanSpanish, tag)
	require.Equal(t, "El grafo con el nombre `missing` no existe en la base de datos `neo4j`. Podría existir en otra base de datos.", spanish)

	pseudoTag := language.MustParse("en-XA")
	pseudo, tag, err := manager.Render(WithPreferences(context.Background(), pseudoTag), message)
	require.NoError(t, err)
	require.Equal(t, pseudoTag, tag)
	require.Equal(t, "[!! Graph with name `missing` does not exist on database `neo4j`. It might exist on another database. !!]", pseudo)
}
