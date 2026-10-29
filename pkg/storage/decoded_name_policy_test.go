package storage

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/stretchr/testify/require"
)

func TestDecodedNamePolicyLabelSetIdentity(t *testing.T) {
	compound := knowledgepolicy.DecayProfileBinding{Name: "compound", TargetLabels: []string{"A\x00B"}, NoDecay: true}
	split := knowledgepolicy.DecayProfileBinding{Name: "split", TargetLabels: []string{"A", "B"}, NoDecay: true}
	t.Run("schema_admission", func(t *testing.T) {
		schema := NewSchemaManager()
		require.NoError(t, schema.CreateDecayProfileBinding(compound))
		require.NoError(t, schema.CreateDecayProfileBinding(split), "distinct exact label sets must not conflict")
		require.Error(t, schema.CreateDecayProfileBinding(knowledgepolicy.DecayProfileBinding{Name: "duplicate", TargetLabels: []string{"B", "A"}, NoDecay: true}), "reordering an actual duplicate remains a conflict")
	})
	t.Run("runtime_resolution", func(t *testing.T) {
		table, err := knowledgepolicy.BuildBindingTable(nil, map[string]*knowledgepolicy.DecayProfileBinding{"compound": &compound, "split": &split}, nil, nil)
		require.NoError(t, err)
		resolver := knowledgepolicy.NewResolver(table, nil)
		for _, tc := range []struct {
			labels []string
			name   string
		}{{[]string{"A\x00B"}, "compound"}, {[]string{"B", "A"}, "split"}, {[]string{"Other", "A", "B"}, "split"}} {
			binding := resolver.ResolveNode(tc.labels)
			require.NotNil(t, binding)
			require.NotNil(t, binding.DecayBinding)
			require.Equal(t, tc.name, binding.DecayBinding.Name)
		}
		require.Nil(t, resolver.ResolveNode([]string{"unrelated"}))
	})
	t.Run("promotion_target", func(t *testing.T) {
		policy := knowledgepolicy.PromotionPolicyDef{Name: "split_policy", TargetLabels: []string{"A", "B"}, Enabled: true}
		table, err := knowledgepolicy.BuildBindingTable(nil, map[string]*knowledgepolicy.DecayProfileBinding{"compound": &compound}, nil, map[string]*knowledgepolicy.PromotionPolicyDef{"split_policy": &policy})
		require.NoError(t, err)
		binding := knowledgepolicy.NewResolver(table, nil).ResolveNode([]string{"A\x00B"})
		require.NotNil(t, binding)
		require.Nil(t, binding.PromotionPolicy, "a promotion for two labels cannot attach to a single delimiter-bearing label")
	})
}

func TestDecodedNamePolicyStructuralIndexIdentity(t *testing.T) {
	for _, tc := range []struct {
		name, indexName, label, property string
		reject                           bool
	}{
		{"named range index", "ordinary_index", "Doc", "value", true},
		{"index name is not a target tuple", "Doc:value", "Other", "different", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			schema := NewSchemaManager()
			require.NoError(t, schema.AddRangeIndex(tc.indexName, tc.label, tc.property))
			err := schema.CreateDecayProfileBinding(knowledgepolicy.DecayProfileBinding{Name: "binding", TargetLabels: []string{"Doc"}, NoDecay: true, PropertyRules: []knowledgepolicy.DecayProfilePropertyRule{{PropertyPath: "value", NoDecay: true}}})
			if tc.reject {
				require.ErrorContains(t, err, "structural index")
			} else {
				require.NoError(t, err)
			}
		})
	}
}
