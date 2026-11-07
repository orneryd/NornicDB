package cypher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProjectedPropertiesRetainReferenceBoundaries(t *testing.T) {
	for _, tc := range []struct {
		variable, expression string
		properties           []string
	}{
		{"n", "n.`external.id`", []string{"external.id"}},
		{"n", "n.`a,b`", []string{"a,b"}},
		{"n", "n.profile.id", []string{"profile"}},
		{"n", "n.`a``.b`", []string{"a`.b"}},
		{"n.dot", "`n.dot`.`external.id`", []string{"external.id"}},
		{"n", "n", nil},
		{"n", "other.`external.id` + n.name", []string{"name"}},
		{"n", "n.`external.id` = 'literal.a.b'", []string{"external.id"}},
		{"n", "n.name + n.name", []string{"name"}},
	} {
		t.Run(tc.expression, func(t *testing.T) {
			require.Equal(t, tc.properties, pipelineNodePredicateProperties(tc.variable, tc.expression, nil))
		})
	}
}
