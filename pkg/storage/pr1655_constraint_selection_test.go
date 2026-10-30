package storage

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestPR1655ConstraintSelectionPreservesOrderAndLabels(t *testing.T) {
	schema := NewSchemaManager()
	for _, c := range []Constraint{
		{Name: "z_unique", Type: ConstraintUnique, Label: "Doc", Properties: []string{"id"}},
		{Name: "a_exists", Type: ConstraintExists, Label: "Doc", Properties: []string{"name"}},
		{Name: "other", Type: ConstraintUnique, Label: "Other", Properties: []string{"id"}},
	} {
		require.NoError(t, schema.AddConstraint(c))
	}
	for _, tc := range []struct {
		name  string
		types []ConstraintType
		want  []string
	}{
		{"all", nil, []string{"a_exists", "z_unique"}},
		{"unique", []ConstraintType{ConstraintUnique}, []string{"z_unique"}},
		{"multiple", []ConstraintType{ConstraintUnique, ConstraintExists}, []string{"a_exists", "z_unique"}},
		{"absent", []ConstraintType{ConstraintTemporal}, []string{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := schema.GetConstraintsForLabels([]string{"Doc", "Doc"}, tc.types...)
			names := make([]string, 0, len(got))
			for _, c := range got {
				names = append(names, c.Name)
				require.Equal(t, "Doc", c.Label)
			}
			require.Equal(t, tc.want, names)
		})
	}
	require.Len(t, schema.GetConstraintsForLabels([]string{"Doc", "Other"}, ConstraintUnique), 2)
}
