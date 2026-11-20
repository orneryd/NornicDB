package cypher

import (
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/stretchr/testify/require"
)

// fineGrainedAuthRuleCase is one authorization rule condition expression and
// the grant outcome it must produce for a fixed identity input.
type fineGrainedAuthRuleCase struct {
	expr string
	want bool
}

// requireFineGrainedAuthRuleConditions asserts each condition's grant outcome.
// An authorization rule grants its roles only when the condition evaluates to
// true, so want=false also covers a null or non-boolean result.
func requireFineGrainedAuthRuleConditions(t *testing.T, input auth.FineGrainedAuthRuleInput, conditions []fineGrainedAuthRuleCase) {
	t.Helper()
	for _, tc := range conditions {
		t.Run(tc.expr, func(t *testing.T) {
			condition, err := CompileFineGrainedAuthRuleCondition(tc.expr)
			require.NoError(t, err)
			require.Equal(t, tc.want, condition(input))
		})
	}
}

func TestFineGrainedAuthRuleCondition(t *testing.T) {
	input := auth.FineGrainedAuthRuleInput{
		NativeTags: []string{"finance", "auditor"},
		Claims: map[string]any{
			"department":           "finance",
			"citizenshipCountries": []any{"c3", "c5"},
		},
		Now: time.Date(2026, time.November, 20, 10, 0, 0, 0, time.UTC),
	}
	conditions := []fineGrainedAuthRuleCase{
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
	requireFineGrainedAuthRuleConditions(t, input, conditions)

	for _, invalid := range []string{"", "n.title", "abac.oidc.user_attribute(department) = 'finance'", "time.transaction('Invalid/Zone').hour > 0", "'a' IN abac.native.user_tags() ignored"} {
		_, err := CompileFineGrainedAuthRuleCondition(invalid)
		require.Error(t, err, invalid)
	}
}

// TestFineGrainedAuthRuleConditionClaims covers claims that are absent, null or
// not booleans. Null claims follow Cypher's three-valued logic, and no
// condition that fails to produce true grants the rule.
func TestFineGrainedAuthRuleConditionClaims(t *testing.T) {
	input := auth.FineGrainedAuthRuleInput{
		Claims: map[string]any{
			"department": "finance",
			"manager":    nil,
			"seniority":  7,
			"active":     false,
			"emptyText":  "",
			"emptyList":  []any{},
			"groups":     []any{"finance", nil},
		},
		Now: time.Date(2026, time.November, 20, 10, 0, 0, 0, time.UTC),
	}
	conditions := []fineGrainedAuthRuleCase{
		// A null and a missing claim both read as null.
		{"abac.oidc.user_attribute('manager') IS NULL", true},
		{"abac.oidc.user_attribute('manager') = 'alice'", false},
		{"abac.oidc.user_attribute('manager') IN ['alice', 'bob']", false},
		{"NOT abac.oidc.user_attribute('manager') = 'alice'", false},
		// Null operands follow three-valued AND, OR and XOR.
		{"abac.oidc.user_attribute('manager') = 'alice' AND true", false},
		{"abac.oidc.user_attribute('manager') = 'alice' OR true", true},
		{"abac.oidc.user_attribute('manager') = 'alice' XOR true", false},
		{"true XOR false", true},
		{"true XOR true", false},
		// A claim that is not a boolean cannot grant the rule, even under NOT.
		{"abac.oidc.user_attribute('department')", false},
		{"NOT abac.oidc.user_attribute('department')", false},
		{"abac.oidc.user_attribute('citizenshipCountries')", false},
		{"abac.oidc.user_attribute('active')", false},
		{"abac.oidc.user_attribute('active') = false", true},
		// Ordinary non-string and empty claims.
		{"abac.oidc.user_attribute('seniority') = 7", true},
		{"abac.oidc.user_attribute('seniority') > 5", true},
		{"abac.oidc.user_attribute('emptyText') = ''", true},
		{"isEmpty(abac.oidc.user_attribute('emptyText'))", true},
		{"size(abac.oidc.user_attribute('emptyList')) = 0", true},
		{"'restricted' IN abac.native.user_tags()", false},
		// A missing claim and a null list entry both stay unknown.
		{"coalesce(abac.oidc.user_attribute('missing'), abac.oidc.user_attribute('department')) = 'finance'", true},
		{"any(group IN abac.oidc.user_attribute('missing') WHERE group = 'finance')", false},
		{"any(group IN abac.oidc.user_attribute('groups') WHERE group = 'finance')", true},
		{"all(group IN abac.oidc.user_attribute('groups') WHERE group IS NOT NULL)", false},
	}
	requireFineGrainedAuthRuleConditions(t, input, conditions)
}

// TestFineGrainedAuthRuleConditionTemporal covers the documented transaction
// time fields and transaction instants that are not already asserted.
func TestFineGrainedAuthRuleConditionTemporal(t *testing.T) {
	requireFineGrainedAuthRuleConditions(t, auth.FineGrainedAuthRuleInput{
		Now: time.Date(2026, time.November, 20, 10, 0, 0, 0, time.UTC),
	}, []fineGrainedAuthRuleCase{
		{"time.transaction('UTC').minute = 0", true},
		{"time.transaction('UTC').second = 0", true},
		{"datetime.transaction('UTC').day = 20", true},
		{"datetime.transaction('UTC').month = 11", true},
		{"datetime.transaction('UTC').year = 2026", true},
		{"datetime.transaction('UTC').dayOfWeek = 5", true},
		{"datetime.transaction('UTC').dayOfWeek IN [6, 7]", false},
		{"datetime.transaction('UTC').hour >= 9 AND datetime.transaction('UTC').hour < 17", true},
		{"datetime.transaction().hour = 10", true},
	})

	// A +02:00 transaction instant has a different wall-clock hour in UTC.
	shifted := time.Date(2026, time.November, 20, 10, 0, 0, 0, time.FixedZone("UTC+2", 2*60*60))
	requireFineGrainedAuthRuleConditions(t, auth.FineGrainedAuthRuleInput{Now: shifted}, []fineGrainedAuthRuleCase{
		{"time.transaction('UTC').hour = 8", true},
		{"time.transaction('UTC').hour = 10", false},
		{"datetime.transaction('UTC').day = 20", true},
		{"time.transaction().hour = 10", true},
	})
}

// TestFineGrainedAuthRuleConditionScalarAndList covers representative scalar
// and list functions documented for auth rule conditions, plus list-edge
// quantifier results.
func TestFineGrainedAuthRuleConditionScalarAndList(t *testing.T) {
	input := auth.FineGrainedAuthRuleInput{
		NativeTags: []string{"finance", "auditor"},
		Claims: map[string]any{
			"department":           "finance",
			"citizenshipCountries": []any{"c3", "c5"},
		},
		Now: time.Date(2026, time.November, 20, 10, 0, 0, 0, time.UTC),
	}
	conditions := []fineGrainedAuthRuleCase{
		{"toUpper(abac.oidc.user_attribute('department')) = 'FINANCE'", true},
		{"trim('  finance  ') = 'finance'", true},
		{"left('finance', 3) = 'fin'", true},
		{"right('finance', 5) = 'nance'", true},
		{"substring('finance', 0, 3) = 'fin'", true},
		{"replace('finance', 'fin', 'ops') = 'opsance'", true},
		{"char_length(abac.oidc.user_attribute('department')) = 7", true},
		{"head(abac.native.user_tags()) = 'finance'", true},
		{"last(abac.native.user_tags()) = 'auditor'", true},
		{"size(split('finance,audit', ',')) = 2", true},
		{"nullIf(abac.oidc.user_attribute('department'), 'finance') IS NULL", true},
		{"toInteger('42') = 42", true},
		{"toFloat('1.5') = 1.5", true},
		{"toString(42) = '42'", true},
		{"toBoolean('true')", true},
		{"toBoolean('not-a-boolean') IS NULL", true},
		{"round(2.4) = 2", true},
		{"ceil(2.1) = 3", true},
		{"floor(2.9) = 2", true},
		{"sign(-4) = -1", true},
		{"abs(-2.5) = 2.5", true},
		{"'c5' IN abac.oidc.user_attribute('citizenshipCountries')", true},
		{"'c1' IN abac.oidc.user_attribute('citizenshipCountries')", false},
		{"range(1, 3) = [1, 2, 3]", true},
		{"reverse([1, 2, 3]) = [3, 2, 1]", true},
		{"tail([1, 2, 3]) = [2, 3]", true},
		{"toIntegerList(['1', '2']) = [1, 2]", true},
		{"all(n IN [] WHERE n > 0)", true},
		{"any(n IN [] WHERE n > 0)", false},
		{"'a' IN []", false},
		{"none(n IN [1, 2, 3] WHERE n > 5)", true},
		{"single(n IN [1, 2, 3] WHERE n > 2)", true},
		{"single(n IN [1, 2, 3] WHERE n > 1)", false},
	}
	requireFineGrainedAuthRuleConditions(t, input, conditions)
}

func TestFineGrainedAuthRuleTransactionClockTypes(t *testing.T) {
	input := auth.FineGrainedAuthRuleInput{
		Now: time.Date(2026, time.November, 20, 23, 30, 0, 0, time.UTC),
	}
	for _, expression := range []string{
		"date.transaction() = date('2026-11-20')",
		"date.transaction('Asia/Tokyo') = date('2026-11-21')",
		"localdatetime.transaction('UTC').hour = 23",
		"localtime.transaction('UTC') = localtime('23:30')",
		"time.transaction('UTC') >= time('23:00Z')",
		"datetime.transaction('UTC').year = 2026",
		"date.truncate('year') = date('2026-01-01')",
	} {
		t.Run(expression, func(t *testing.T) {
			condition, err := CompileFineGrainedAuthRuleCondition(expression)
			require.NoError(t, err)
			require.True(t, condition(input))
		})
	}
}

func TestFineGrainedAuthRuleRequiresProviderInputs(t *testing.T) {
	condition, err := CompileFineGrainedAuthRuleCondition("isEmpty(abac.native.user_tags()) AND abac.oidc.user_attribute('department') IS NULL")
	require.NoError(t, err)
	require.False(t, condition(auth.FineGrainedAuthRuleInput{}))
	require.False(t, condition(auth.FineGrainedAuthRuleInput{NativeTags: []string{}}))
	require.False(t, condition(auth.FineGrainedAuthRuleInput{Claims: map[string]any{}}))
	require.True(t, condition(auth.FineGrainedAuthRuleInput{NativeTags: []string{}, Claims: map[string]any{}}))
}
