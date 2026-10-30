package storage

// Author: #1667 implementation lane (Codex).

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSchemaRangeDefinitionPropertyIsolation(t *testing.T) {
	for _, direction := range []string{"export", "import"} {
		t.Run(direction, func(t *testing.T) {
			sm := NewSchemaManager()
			var def *SchemaDefinition
			if direction == "export" {
				require.NoError(t, sm.AddTypedIndexForEntity(IndexKindText, "p_text", "P", []string{"k"}, ConstraintEntityNode))
				def = sm.ExportDefinition()
			} else {
				def = &SchemaDefinition{Version: 1, RangeIndexes: []SchemaRangeIndexDef{{Name: "p_text", Kind: IndexKindText, Label: "P", Property: "k", Properties: []string{"k"}, EntityType: ConstraintEntityNode}}}
				require.NoError(t, sm.ReplaceFromDefinition(def))
			}
			require.Len(t, def.RangeIndexes, 1)
			def.RangeIndexes[0].Properties[0] = "changed_by_caller"
			index, ok := sm.GetRangeIndex("p_text")
			require.True(t, ok)
			t.Logf("direction=%s retained property=%s fields=%v", direction, index.Property, index.Properties)
			require.Equal(t, []string{"k"}, index.Properties, "a caller-owned definition must not mutate the retained schema descriptor")
		})
	}
}
