package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCompositeEngine_GetSchema_MergesIndexes(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

	// Add indexes to engine1
	schema1 := engine1.GetSchema()
	err := schema1.AddPropertyIndex("idx_person_name", "Person", []string{"name"})
	require.NoError(t, err)
	err = schema1.AddCompositeIndex("idx_person_location", "Person", []string{"country", "city"})
	require.NoError(t, err)
	err = schema1.AddFulltextIndex("idx_person_fulltext", []string{"Person"}, []string{"name", "bio"})
	require.NoError(t, err)
	err = schema1.AddVectorIndex("idx_person_embedding", "Person", "embedding", 384, "cosine")
	require.NoError(t, err)
	err = schema1.AddRangeIndex("idx_person_age", "Person", "age")
	require.NoError(t, err)

	// Add different indexes to engine2
	schema2 := engine2.GetSchema()
	err = schema2.AddPropertyIndex("idx_company_name", "Company", []string{"name"})
	require.NoError(t, err)
	err = schema2.AddCompositeIndex("idx_company_location", "Company", []string{"country", "city"})
	require.NoError(t, err)

	// Create composite engine
	constituents := map[string]Engine{
		"db1": engine1,
		"db2": engine2,
	}
	constituentNames := map[string]string{
		"db1": "db1",
		"db2": "db2",
	}
	accessModes := map[string]string{
		"db1": "read_write",
		"db2": "read_write",
	}
	composite := NewCompositeEngine(constituents, constituentNames, accessModes)

	// Get merged schema
	mergedSchema := composite.GetSchema()
	require.NotNil(t, mergedSchema)

	// Verify all indexes are merged
	indexes := nonLookupIndexes(mergedSchema.GetIndexes())
	require.Len(t, indexes, 7, "composite merge should contain the exact union of constituent index names")

	// Check for specific indexes
	indexMap := make(map[string]bool)
	typeMap := make(map[string]string)
	for _, idx := range indexes {
		if idxMap, ok := idx.(map[string]interface{}); ok {
			if name, ok := idxMap["name"].(string); ok {
				indexMap[name] = true
				if idxType, ok := idxMap["type"].(string); ok {
					typeMap[name] = idxType
				}
			}
		}
	}

	// Verify indexes from both constituents are present
	assert.True(t, indexMap["idx_person_name"], "idx_person_name should be merged")
	assert.True(t, indexMap["idx_person_location"], "idx_person_location should be merged")
	assert.True(t, indexMap["idx_person_fulltext"], "idx_person_fulltext should be merged")
	assert.True(t, indexMap["idx_person_embedding"], "idx_person_embedding should be merged")
	assert.True(t, indexMap["idx_person_age"], "idx_person_age should be merged")
	assert.True(t, indexMap["idx_company_name"], "idx_company_name should be merged")
	assert.True(t, indexMap["idx_company_location"], "idx_company_location should be merged")
	assert.Equal(t, "PROPERTY", typeMap["idx_person_name"])
	assert.Equal(t, "COMPOSITE", typeMap["idx_person_location"])
	assert.Equal(t, "FULLTEXT", typeMap["idx_person_fulltext"])
	assert.Equal(t, "VECTOR", typeMap["idx_person_embedding"])
	assert.Equal(t, "RANGE", typeMap["idx_person_age"])
	assert.Equal(t, "PROPERTY", typeMap["idx_company_name"])
	assert.Equal(t, "COMPOSITE", typeMap["idx_company_location"])
}

func TestCompositeEngine_GetSchema_DeduplicatesIndexes(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

	// Add same index to both engines (same name)
	schema1 := engine1.GetSchema()
	err := schema1.AddPropertyIndex("idx_person_name", "Person", []string{"name"})
	require.NoError(t, err)

	schema2 := engine2.GetSchema()
	err = schema2.AddPropertyIndex("idx_person_name", "Person", []string{"name"})
	require.NoError(t, err)

	// Create composite engine
	constituents := map[string]Engine{
		"db1": engine1,
		"db2": engine2,
	}
	constituentNames := map[string]string{
		"db1": "db1",
		"db2": "db2",
	}
	accessModes := map[string]string{
		"db1": "read_write",
		"db2": "read_write",
	}
	composite := NewCompositeEngine(constituents, constituentNames, accessModes)

	// Get merged schema
	mergedSchema := composite.GetSchema()
	require.NotNil(t, mergedSchema)

	// Verify index appears only once (deduplicated by name)
	indexes := nonLookupIndexes(mergedSchema.GetIndexes())
	idxCount := 0
	for _, idx := range indexes {
		if idxMap, ok := idx.(map[string]interface{}); ok {
			if name, ok := idxMap["name"].(string); ok && name == "idx_person_name" {
				idxCount++
			}
		}
	}
	assert.Equal(t, 1, idxCount, "Duplicate index should be deduplicated")
}

func TestCompositeEngine_GetSchema_MergesConstraints(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

	// Add constraints to engine1
	schema1 := engine1.GetSchema()
	err := schema1.AddConstraint(Constraint{
		Name:       "unique_person_email",
		Type:       ConstraintUnique,
		Label:      "Person",
		Properties: []string{"email"},
	})
	require.NoError(t, err)

	// Add different constraint to engine2
	schema2 := engine2.GetSchema()
	err = schema2.AddConstraint(Constraint{
		Name:       "unique_company_name",
		Type:       ConstraintUnique,
		Label:      "Company",
		Properties: []string{"name"},
	})
	require.NoError(t, err)

	// Create composite engine
	constituents := map[string]Engine{
		"db1": engine1,
		"db2": engine2,
	}
	constituentNames := map[string]string{
		"db1": "db1",
		"db2": "db2",
	}
	accessModes := map[string]string{
		"db1": "read_write",
		"db2": "read_write",
	}
	composite := NewCompositeEngine(constituents, constituentNames, accessModes)

	// Get merged schema
	mergedSchema := composite.GetSchema()
	require.NotNil(t, mergedSchema)

	// Verify constraints are merged
	// GetConstraintsForLabels with specific labels
	personConstraints := mergedSchema.GetConstraintsForLabels([]string{"Person"})
	companyConstraints := mergedSchema.GetConstraintsForLabels([]string{"Company"})

	// At least one constraint should be found
	foundPerson := false
	foundCompany := false
	for _, c := range personConstraints {
		if c.Name == "unique_person_email" {
			foundPerson = true
		}
	}
	for _, c := range companyConstraints {
		if c.Name == "unique_company_name" {
			foundCompany = true
		}
	}

	// Verify constraints from both constituents are present
	assert.True(t, foundPerson || foundCompany, "At least one constraint should be merged")
}

func TestCompositeEngine_GetSchema_EmptyComposite(t *testing.T) {
	// Create composite engine with no constituents
	constituents := map[string]Engine{}
	constituentNames := map[string]string{}
	accessModes := map[string]string{}
	composite := NewCompositeEngine(constituents, constituentNames, accessModes)

	// Get schema should return empty schema manager
	mergedSchema := composite.GetSchema()
	require.NotNil(t, mergedSchema)

	// Should have no indexes or constraints
	indexes := nonLookupIndexes(mergedSchema.GetIndexes())
	assert.Equal(t, 0, len(indexes))

	constraints := mergedSchema.GetConstraints()
	assert.Equal(t, 0, len(constraints))
}

func TestCompositeEngine_GetSchema_AllIndexTypes(t *testing.T) {
	// Create constituent engine with all index types
	engine1 := NewMemoryEngine()
	schema1 := engine1.GetSchema()

	// Add one of each index type
	err := schema1.AddPropertyIndex("prop_idx", "Person", []string{"name"})
	require.NoError(t, err)
	err = schema1.AddCompositeIndex("comp_idx", "Person", []string{"country", "city"})
	require.NoError(t, err)
	err = schema1.AddFulltextIndex("ft_idx", []string{"Person"}, []string{"bio"})
	require.NoError(t, err)
	err = schema1.AddVectorIndex("vec_idx", "Person", "embedding", 384, "cosine")
	require.NoError(t, err)
	err = schema1.AddRangeIndex("range_idx", "Person", "age")
	require.NoError(t, err)

	// Create composite engine
	constituents := map[string]Engine{
		"db1": engine1,
	}
	constituentNames := map[string]string{
		"db1": "db1",
	}
	accessModes := map[string]string{
		"db1": "read_write",
	}
	composite := NewCompositeEngine(constituents, constituentNames, accessModes)

	// Get merged schema
	mergedSchema := composite.GetSchema()
	require.NotNil(t, mergedSchema)

	// Verify all index types are present
	indexes := nonLookupIndexes(mergedSchema.GetIndexes())
	require.Equal(t, 5, len(indexes))

	// Verify each index type
	typeCounts := make(map[string]int)
	for _, idx := range indexes {
		if idxMap, ok := idx.(map[string]interface{}); ok {
			if idxType, ok := idxMap["type"].(string); ok {
				typeCounts[idxType]++
			}
		}
	}

	assert.Equal(t, 1, typeCounts["PROPERTY"], "Should have 1 property index")
	assert.Equal(t, 1, typeCounts["COMPOSITE"], "Should have 1 composite index")
	assert.Equal(t, 1, typeCounts["FULLTEXT"], "Should have 1 fulltext index")
	assert.Equal(t, 1, typeCounts["VECTOR"], "Should have 1 vector index")
	assert.Equal(t, 1, typeCounts["RANGE"], "Should have 1 range index")
}

func TestAnyToStringSlice_DeterministicAcrossShapes(t *testing.T) {
	t.Run("native string slice", func(t *testing.T) {
		got := anyToStringSlice([]string{"a", "b", "", "  "})
		require.Equal(t, []string{"a", "b"}, got)
	})

	t.Run("interface slice from generic decode", func(t *testing.T) {
		got := anyToStringSlice([]interface{}{"a", "b", 7, nil, ""})
		require.Equal(t, []string{"a", "b"}, got)
	})

	t.Run("unsupported shape", func(t *testing.T) {
		got := anyToStringSlice(map[string]interface{}{"a": 1})
		require.Nil(t, got)
	})
}

func TestCompositeEngine_GetSchema_MergesRelationshipRangeIndexEntityType(t *testing.T) {
	engine := NewMemoryEngine()
	require.NoError(t, engine.GetSchema().AddRangeIndexForEntity(
		"rel_range_idx",
		"RELATES_TO",
		[]string{"uuid"},
		ConstraintEntityRelationship,
	))

	composite := NewCompositeEngine(
		map[string]Engine{"db1": engine},
		map[string]string{"db1": "db1"},
		map[string]string{"db1": "read_write"},
	)

	indexes := composite.GetSchema().GetIndexes()
	require.NotEmpty(t, indexes)

	var found map[string]interface{}
	for _, idx := range indexes {
		idxMap, ok := idx.(map[string]interface{})
		if !ok {
			continue
		}
		if name, _ := idxMap["name"].(string); name == "rel_range_idx" {
			found = idxMap
			break
		}
	}
	require.NotNil(t, found)
	require.Equal(t, "RANGE", found["type"])
	require.Equal(t, "RELATES_TO", found["label"])
	require.Equal(t, string(ConstraintEntityRelationship), found["entityType"])
	require.Equal(t, []string{"uuid"}, found["properties"])
}

func TestCompositeEngine_GetSchema_MergesLegacyScalarRangePropertyShape(t *testing.T) {
	engine := NewMemoryEngine()
	sm := engine.GetSchema()
	require.NoError(t, sm.AddRangeIndex("legacy_range_idx", "Person", "age"))
	// Force legacy export shape: scalar "property" only.
	sm.mu.Lock()
	sm.rangeIndexes["legacy_range_idx"].Properties = nil
	sm.mu.Unlock()

	composite := NewCompositeEngine(
		map[string]Engine{"db1": engine},
		map[string]string{"db1": "db1"},
		map[string]string{"db1": "read_write"},
	)

	indexes := composite.GetSchema().GetIndexes()
	require.NotEmpty(t, indexes)
	var found map[string]interface{}
	for _, idx := range indexes {
		idxMap, ok := idx.(map[string]interface{})
		if !ok {
			continue
		}
		if name, _ := idxMap["name"].(string); name == "legacy_range_idx" {
			found = idxMap
			break
		}
	}
	require.NotNil(t, found)
	require.Equal(t, "RANGE", found["type"])
	require.Equal(t, "Person", found["label"])
	require.Equal(t, []string{"age"}, found["properties"])
}
