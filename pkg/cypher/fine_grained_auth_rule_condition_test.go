package cypher

import (
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/stretchr/testify/require"
)

func TestFineGrainedAuthRuleCondition(t *testing.T) {
	input := auth.FineGrainedAuthRuleInput{
		NativeTags: []string{"finance", "auditor"},
		Claims: map[string]any{
			"department":           "finance",
			"citizenshipCountries": []any{"c3", "c5"},
		},
		Now: time.Date(2026, time.November, 20, 10, 0, 0, 0, time.UTC),
	}
	conditions := []struct {
		expr string
		want bool
	}{
		{"'finance' IN abac.native.user_tags()", true},
		{"all(tag IN ['finance', 'auditor'] WHERE tag IN abac.native.user_tags())", true},
		{"any(country IN abac.oidc.user_attribute('citizenshipCountries') WHERE country IN ['c1', 'c5'])", true},
		{"abac.oidc.user_attribute('department') = 'finance' AND time.transaction('UTC').hour >= 9", true},
		{"'auditor' IN abac.native.user_tags() AND (time.transaction('UTC').hour < 9 OR time.transaction('UTC').hour >= 17)", false},
		{"NOT abac.oidc.user_attribute('unknown') = 'finance'", false},
		{"abac.oidc.user_attribute('unknown') IS NULL", true},
		{"abac.oidc.user_attribute('unknown') IS NOT NULL", false},
		{"false AND abac.oidc.user_attribute('unknown') = 'finance'", false},
		{"true OR abac.oidc.user_attribute('unknown') = 'finance'", true},
		{"size(abac.native.user_tags()) = 2", true},
		{"toLower('AUDITOR') IN abac.native.user_tags()", true},
		{"abs(-3) = 3", true},
		{"reduce(total = 0, n IN [1, 2, 3] | total + n) = 6", true},
		{"abac.oidc.user_attribute('department') STARTS WITH 'fin'", true},
		{"(1 + 2 * 3) = 7", true},
	}
	for _, tc := range conditions {
		t.Run(tc.expr, func(t *testing.T) {
			condition, err := CompileFineGrainedAuthRuleCondition(tc.expr)
			require.NoError(t, err)
			require.Equal(t, tc.want, condition(input))
		})
	}

	for _, invalid := range []string{"", "n.title", "time.transaction('Invalid/Zone').hour > 0", "'a' IN abac.native.user_tags() ignored"} {
		_, err := CompileFineGrainedAuthRuleCondition(invalid)
		require.Error(t, err, invalid)
	}
}
