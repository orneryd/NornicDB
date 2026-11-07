package cypher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConstraintPropertyReferenceUsesQuotedBoundaries(t *testing.T) {
	for _, tc := range []struct {
		input, qualifier, property string
		valid                      bool
	}{
		{"n.name", "n", "name", true},
		{"n.`external.id`", "n", "external.id", true},
		{"(n.`external.id`)", "n", "external.id", true},
		{"n.`external``id`", "n", "external`id", true},
		{"`n.dot`.`external.id`", "`n.dot`", "external.id", true},
		{"n.`  `", "n", "  ", true},
		{"n", "", "", false},
		{"n.name.other", "", "", false},
		{"n.name, n.other", "", "", false},
		{"n.`unterminated", "", "", false},
	} {
		t.Run(tc.input, func(t *testing.T) {
			qualifier, property, valid := parseConstraintQualifiedProperty(tc.input)
			require.Equal(t, tc.valid, valid)
			require.Equal(t, tc.qualifier, qualifier)
			require.Equal(t, tc.property, property)
		})
	}
}
