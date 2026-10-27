package cypher

import (
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"strconv"
	"testing"

	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ── Non-knowledge-policy statements ─────────────────────────────────────────

// ── CREATE DECAY PROFILE … OPTIONS (bundle) ────────────────────────────────

func TestParseDDL_CreateDecayProfileBundle_AllFunctions(t *testing.T) {
	for _, fn := range []string{"exponential", "linear", "step", "none"} {
		stmt := `CREATE DECAY PROFILE fn_test OPTIONS {
			halfLifeSeconds: 100,
			function: '` + fn + `',
			scope: 'NODE',
			scoreFrom: 'CREATED'
		}`
		cmd, ok, err := ParseKnowledgePolicyDDL(t.Context(), stmt, RawSource)
		require.NoError(t, err, "function=%s", fn)
		require.True(t, ok)
		c := cmd.(*CreateDecayProfileBundleCmd)
		assert.Equal(t, knowledgepolicy.DecayFunction(fn), c.Bundle.Function)
	}
}

// ── CREATE DECAY PROFILE … FOR (binding) ───────────────────────────────────

// ── ALTER DECAY PROFILE ─────────────────────────────────────────────────────

// ── DROP DECAY PROFILE ──────────────────────────────────────────────────────

// ── SHOW DECAY PROFILES ─────────────────────────────────────────────────────

// ── CREATE PROMOTION PROFILE ────────────────────────────────────────────────

// ── ALTER PROMOTION PROFILE ─────────────────────────────────────────────────

// ── DROP PROMOTION PROFILE ──────────────────────────────────────────────────

// ── SHOW PROMOTION PROFILES ─────────────────────────────────────────────────

// ── CREATE PROMOTION POLICY ─────────────────────────────────────────────────

// ── WITH KALMAN in ON ACCESS blocks ─────────────────────────────────────────

// ── ALTER PROMOTION POLICY ──────────────────────────────────────────────────

func TestParseDDL_KnowledgeProfileAndPolicyTerminators(t *testing.T) {
	for _, statement := range []string{
		"CREATE DECAY PROFILE d OPTIONS { halfLifeSeconds: 3600, function: 'exponential', scope: 'NODE', scoreFrom: 'CREATED' }",
		"CREATE DECAY PROFILE binding FOR (n:Fact) APPLY { DECAY PROFILE 'd' }",
		"ALTER DECAY PROFILE binding FOR (n:Fact) APPLY { DECAY PROFILE 'd' }",
		"CREATE PROMOTION PROFILE p OPTIONS { multiplier: 1.5 }",
		"ALTER PROMOTION PROFILE p SET OPTIONS { multiplier: 1.5 }",
		"ALTER DECAY PROFILE d SET OPTIONS { halfLifeSeconds: 3600 }",
		"CREATE PROMOTION POLICY pg FOR (n:Fact) APPLY { ON ACCESS { SET n.c = 1 } }",
		"ALTER PROMOTION POLICY pg SET OPTIONS { enabled: true } DISABLE",
	} {
		_, ok, err := ParseKnowledgePolicyDDL(t.Context(), statement+"; ", RawSource)
		require.NoError(t, err, statement)
		require.True(t, ok, statement)
		_, _, err = ParseKnowledgePolicyDDL(t.Context(), statement+"; GARBAGE", RawSource)
		require.Error(t, err, statement)
	}
}

func TestParseDDL_AlterPromotionPolicyRejectsUnconsumedClauses(t *testing.T) {
	_, _, err := ParseKnowledgePolicyDDL(t.Context(), "ALTER PROMOTION POLICY", RawSource)
	require.Error(t, err)
	for _, suffix := range []string{
		"DISABEL",
		"FROBNICATE",
		"SET",
		"SET ENABLE",
		"SET OPTION { enabled: false }",
		"SET OPTIONS {",
		"FOR (n:Fact) APPLY {",
		"FOR (n:Fact) GARBAGE",
		"FOR (n:Fact) APPLY { ON ACCESS { SET n.c = 1 } } GARBAGE",
		"ENABLE GARBAGE",
		"SET OPTIONS { enabled: true } trailing words",
		"ENABLE DISABLE",
	} {
		t.Run(suffix, func(t *testing.T) {
			_, _, err := ParseKnowledgePolicyDDL(t.Context(), "ALTER PROMOTION POLICY pg "+suffix, RawSource)
			require.Error(t, err)
		})
	}
}

// ── DROP PROMOTION POLICY ───────────────────────────────────────────────────

// ── SHOW PROMOTION POLICIES ─────────────────────────────────────────────────

// ── Edge cases and error handling ───────────────────────────────────────────

// ── Scanner helper edge cases ───────────────────────────────────────────────

func TestKpScanDecimal_Negative(t *testing.T) {
	for _, tc := range []struct {
		name     string
		input    string
		floating bool
		want     any
		end      int
	}{
		{name: "floating", input: "-3.14", floating: true, want: float64(-3.14), end: 5},
		{name: "integer", input: "-99", want: int64(-99), end: 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var value any
			var end int
			var ok bool
			if tc.floating {
				value, end, ok = kpScanDecimal[float64](tc.input, 0)
			} else {
				value, end, ok = kpScanDecimal[int64](tc.input, 0)
			}
			require.True(t, ok)
			assert.Equal(t, tc.want, value)
			assert.Equal(t, tc.end, end)
		})
	}
}

func TestKpScanNumber_Integer(t *testing.T) {
	f, j, ok := kpScanDecimal[float64]("42", 0)
	require.True(t, ok)
	assert.Equal(t, 42.0, f)
	assert.Equal(t, 2, j)
}

func TestKpScanNumber_InvalidInput(t *testing.T) {
	_, _, ok := kpScanDecimal[float64]("abc", 0)
	assert.False(t, ok)
}

func TestKpScanInt_Valid(t *testing.T) {
	n, j, ok := kpScanDecimal[int64]("12345", 0)
	require.True(t, ok)
	assert.Equal(t, int64(12345), n)
	assert.Equal(t, 5, j)
}

func TestKpScanBool_True(t *testing.T) {
	b, j, ok := kpScanBool("TRUE rest", 0)
	require.True(t, ok)
	assert.True(t, b)
	assert.Equal(t, 4, j)
}

func TestKpScanBool_False(t *testing.T) {
	b, j, ok := kpScanBool("FALSE rest", 0)
	require.True(t, ok)
	assert.False(t, b)
	assert.Equal(t, 5, j)
}

func TestKpScanBool_Invalid(t *testing.T) {
	_, _, ok := kpScanBool("MAYBE", 0)
	assert.False(t, ok)
}

func TestKpScanQuotedString_SingleQuote(t *testing.T) {
	val, j := kpScanQuotedString("'hello world'", 0)
	assert.Equal(t, "hello world", val)
	assert.Equal(t, 13, j)
}

func TestKpScanQuotedString_DoubleQuote(t *testing.T) {
	val, j := kpScanQuotedString(`"hello"`, 0)
	assert.Equal(t, "hello", val)
	assert.Equal(t, 7, j)
}

func TestKpScanQuotedString_Unterminated(t *testing.T) {
	_, j := kpScanQuotedString("'unterminated", 0)
	assert.Equal(t, -1, j)
}

func TestKpScanBraceBlock_Balanced(t *testing.T) {
	inner, j := kpScanBraceBlock("{ a: 1, b: { c: 2 } }", 0)
	assert.Equal(t, " a: 1, b: { c: 2 } ", inner)
	assert.Equal(t, 21, j)
}

func TestKpScanBraceBlock_NoTrailingWhitespace(t *testing.T) {
	inner, j := kpScanBraceBlock("{abc}", 0)
	assert.Equal(t, "abc", inner)
	assert.Equal(t, 5, j)
}

func TestKpScanBraceBlock_Unbalanced(t *testing.T) {
	_, j := kpScanBraceBlock("{ a: 1, b: { c: 2 }", 0)
	assert.Equal(t, -1, j)
}

func TestKpMatchKeywordAt_CaseInsensitive(t *testing.T) {
	assert.Greater(t, cyphertext.KeywordEnd("create", 0, "CREATE", cyphertext.CypherWord), 0)
	assert.Greater(t, cyphertext.KeywordEnd("CREATE", 0, "create", cyphertext.CypherWord), 0)
	assert.Greater(t, cyphertext.KeywordEnd("CrEaTe", 0, "CREATE", cyphertext.CypherWord), 0)
}

func TestKpMatchKeywordAt_NoPartialMatch(t *testing.T) {
	assert.Equal(t, -1, cyphertext.KeywordEnd("CREATING", 0, "CREATE", cyphertext.CypherWord))
}

func TestKpScanIdent_ValidIdent(t *testing.T) {
	name, j := kpScanIdent("my_ident123 rest", 0)
	assert.Equal(t, "my_ident123", name)
	assert.Equal(t, 11, j)
}

func TestKpScanIdent_StartsWithDigit(t *testing.T) {
	name, j := kpScanIdent("123abc", 0)
	assert.Equal(t, "", name)
	assert.Equal(t, 0, j)
}

func TestKpScanIdent_Empty(t *testing.T) {
	name, j := kpScanIdent("", 0)
	assert.Equal(t, "", name)
	assert.Equal(t, 0, j)
}

// ── Plan-syntax DDL examples ──────────────────────────────────────────────────
// Each test below corresponds to a DDL example from the plan / user guide docs.

// TestPlanDDL_SharedAlterDropShapes pins the error texts and shapes shared by
// the alter and drop parsers through kpParseKnowledgeName / kpParseSetOptions /
// kpParseDrop: one helper bug would show up across all six statements.
func TestPlanDDL_SharedAlterDropShapes(t *testing.T) {
	tests := []struct {
		name       string
		stmt       string
		wantErr    string
		wantIFDrop bool
		wantCmd    string
	}{
		{name: "alter decay missing SET OPTIONS", stmt: "ALTER DECAY PROFILE p", wantErr: "SET OPTIONS"},
		{name: "alter promotion missing SET OPTIONS", stmt: "ALTER PROMOTION PROFILE p", wantErr: "SET OPTIONS"},
		{name: "alter policy bare name is valid", stmt: "ALTER PROMOTION POLICY p", wantCmd: "alter_promotion_policy"},
		{name: "drop decay missing name", stmt: "DROP DECAY PROFILE", wantErr: "name"},
		{name: "drop promotion missing name", stmt: "DROP PROMOTION PROFILE", wantErr: "name"},
		{name: "drop policy missing name", stmt: "DROP PROMOTION POLICY", wantErr: "name"},
		{name: "drop decay if exists", stmt: "DROP DECAY PROFILE IF EXISTS dp", wantIFDrop: true, wantCmd: "drop_decay"},
		{name: "alter decay set options parses", stmt: "ALTER DECAY PROFILE dp SET OPTIONS {halfLifeSeconds: 3600}", wantCmd: "alter_decay"},
		{name: "alter promotion set options parses", stmt: "ALTER PROMOTION PROFILE pp SET OPTIONS {multiplier: 2.0}", wantCmd: "alter_promotion"},
		{name: "alter policy enable", stmt: "ALTER PROMOTION POLICY pol ENABLE", wantCmd: "alter_promotion_policy"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cmd, ok, err := ParseKnowledgePolicyDDL(t.Context(), tc.stmt, RawSource)
			if tc.wantErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.wantErr)
				require.False(t, ok)
				return
			}
			require.NoError(t, err)
			require.True(t, ok)
			switch tc.wantCmd {
			case "alter_decay":
				require.IsType(t, &AlterDecayProfileCmd{}, cmd)
			case "alter_promotion":
				require.IsType(t, &AlterPromotionProfileCmd{}, cmd)
			case "alter_promotion_policy":
				require.IsType(t, &AlterPromotionPolicyCmd{}, cmd)
			case "drop_decay":
				require.IsType(t, &DropDecayProfileCmd{}, cmd)
				require.True(t, cmd.(*DropDecayProfileCmd).IfExists == tc.wantIFDrop)
			}
		})
	}
}

// BenchmarkParseKnowledgePolicyDDL_AlterSetOptions pins the shared alter-parser
// path (kpParseKnowledgeName + kpParseSetOptions).
func BenchmarkParseKnowledgePolicyDDL_AlterSetOptions(b *testing.B) {
	stmt := "ALTER DECAY PROFILE dp SET OPTIONS {halfLifeSeconds: 3600, visibilityThreshold: 0.05}"
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cmd, ok, err := ParseKnowledgePolicyDDL(b.Context(), stmt, RawSource)
		if err != nil || !ok {
			b.Fatalf("parse failed: %v", err)
		}
		_ = cmd
	}
}

func TestKpDecimalPreservesIntegerPrecisionAndPolicy(t *testing.T) {
	for _, raw := range []string{"9223372036854775807", "-9223372036854775808", "9007199254740993"} {
		expected, err := strconv.ParseInt(raw, 10, 64)
		require.NoError(t, err)
		value, end, ok := kpScanDecimal[int64](raw, 0)
		require.True(t, ok, raw)
		require.Equal(t, expected, value)
		require.Equal(t, len(raw), end)
	}
	for _, raw := range []string{"9223372036854775808", "-9223372036854775809", "1.5", "1e2", "1_000", "0x10", "42n"} {
		_, end, ok := kpScanDecimal[int64](raw, 0)
		require.False(t, ok, raw)
		require.Zero(t, end, raw)
	}
	value, end, ok := kpScanDecimal[float64]("1e2", 0)
	require.True(t, ok)
	require.Equal(t, 100.0, value)
	require.Equal(t, 3, end)
}

// kpDDLProductCase keeps one exact original input with its typed deep checks.
// The single public parse and concrete product assertion are owned by the runner.
type kpDDLProductCase[T any] struct {
	name, query string
	stage       SourceStage
	check       func(*testing.T, T)
}

func runKPProductCases[T any](t *testing.T, cases []kpDDLProductCase[T]) {
	t.Helper()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cmd, ok, err := ParseKnowledgePolicyDDL(t.Context(), tc.query, tc.stage)
			require.NoError(t, err)
			require.True(t, ok)
			typed, present := cmd.(T)
			var expected T
			require.True(t, present, "expected %T, got %T", expected, cmd)
			if tc.check != nil {
				tc.check(t, typed)
			}
		})
	}
}

type kpDDLErrorCase struct {
	name, query string
	stage       SourceStage
	fragments   []string
}

func TestKnowledgePolicyDDLErrors(t *testing.T) {
	cases := []kpDDLErrorCase{
		{name: "TestParseDDL_CreateDecayProfileBundle_InvalidFunction", query: `CREATE DECAY PROFILE bad_fn OPTIONS {
		function: 'quadratic',
		scope: 'NODE',
		scoreFrom: 'CREATED'
	}`, stage: RawSource, fragments: []string{"invalid decay function"}},
		{name: "TestParseDDL_CreateDecayProfileBundle_InvalidScope", query: `CREATE DECAY PROFILE bad_scope OPTIONS {
		function: 'exponential',
		scope: 'UNIVERSE',
		scoreFrom: 'CREATED'
	}`, stage: RawSource, fragments: []string{"invalid scope"}},
		{name: "TestParseDDL_CreateDecayProfileBundle_InvalidScoreFrom", query: `CREATE DECAY PROFILE bad_sf OPTIONS {
		function: 'exponential',
		scope: 'NODE',
		scoreFrom: 'NEVER'
	}`, stage: RawSource, fragments: []string{"invalid scoreFrom"}},
		{name: "TestParseDDL_CreateDecayProfileBundle_UnknownOption", query: `CREATE DECAY PROFILE bad_opt OPTIONS {
		function: 'exponential',
		scope: 'NODE',
		scoreFrom: 'CREATED',
		bogusField: 42
	}`, stage: RawSource, fragments: []string{"unknown option"}},
		{name: "TestParseDDL_CreateDecayProfileBundle_MissingName", query: `CREATE DECAY PROFILE OPTIONS { function: 'exponential' }`, stage: RawSource, fragments: []string{"expected"}},
		{name: "TestParseDDL_CreateDecayProfile_MissingOptionsOrFor", query: `CREATE DECAY PROFILE orphan_name SOMETHING_ELSE`, stage: RawSource, fragments: []string{"expected OPTIONS or FOR"}},
		{name: "TestParseDDL_AlterDecayProfile_MissingSetOptions", query: `ALTER DECAY PROFILE slow_decay SOMETHING_ELSE`, stage: RawSource, fragments: []string{"expected SET OPTIONS"}},
		{name: "TestParseDDL_AlterDecayProfile_MissingName", query: `ALTER DECAY PROFILE SET OPTIONS { enabled: false }`, stage: RawSource, fragments: []string{}},
		{name: "TestParseDDL_DropDecayProfile_MissingName", query: `DROP DECAY PROFILE`, stage: RawSource, fragments: []string{"expected profile name"}},
		{name: "TestParseDDL_CreatePromotionProfile_InvalidScope", query: `CREATE PROMOTION PROFILE bad OPTIONS { scope: 'GALAXY', multiplier: 1.0, scoreCap: 1.0 }`, stage: RawSource, fragments: []string{"invalid scope"}},
		{name: "TestParseDDL_CreatePromotionProfile_UnknownOption", query: `CREATE PROMOTION PROFILE bad OPTIONS { scope: 'NODE', foo: 123 }`, stage: RawSource, fragments: []string{"unknown option"}},
		{name: "TestParseDDL_CreatePromotionProfile_MissingOptions", query: `CREATE PROMOTION PROFILE bare_name`, stage: RawSource, fragments: []string{"expected OPTIONS"}},
		{name: "TestParseDDL_CreatePromotionProfile_MissingName", query: `CREATE PROMOTION PROFILE OPTIONS { scope: 'NODE' }`, stage: RawSource, fragments: []string{}},
		{name: "TestParseDDL_AlterPromotionProfile_MissingSetOptions", query: `ALTER PROMOTION PROFILE boost SOMETHING`, stage: RawSource, fragments: []string{"expected SET OPTIONS"}},
		{name: "TestParseDDL_DropPromotionProfile_MissingName", query: `DROP PROMOTION PROFILE`, stage: RawSource, fragments: []string{"expected profile name"}},
		{name: "TestParseDDL_CreatePromotionPolicy_MissingName", query: `CREATE PROMOTION POLICY`, stage: RawSource, fragments: []string{"expected policy name"}},
		{name: "TestParseDDL_OnAccess_WithKalmanUnknownKey", query: `CREATE PROMOTION POLICY kalman_bad FOR (n:Fact) APPLY {
		ON ACCESS {
			WITH KALMAN{q: 0.1, bogusKey: 42} SET n.score = $val
		}
	}`, stage: RawSource, fragments: []string{"unknown Kalman config key"}},
		{name: "TestParseDDL_CreatePromotionPolicyRequiresFor/0", query: "CREATE PROMOTION POLICY nofor", stage: RawSource, fragments: []string{}},
		{name: "TestParseDDL_CreatePromotionPolicyRequiresFor/1", query: "CREATE PROMOTION POLICY nofor APPLY { ON ACCESS { SET n.c = 1 } }", stage: RawSource, fragments: []string{}},
		{name: "TestParseDDL_CreatePromotionPolicyRequiresFor/2", query: "CREATE PROMOTION POLICY IF NOT EXISTS nofor APPLY { ON ACCESS { SET n.c = 1 } }", stage: RawSource, fragments: []string{}},
		{name: "TestParseDDL_CreatePromotionPolicyRequiresFor/3", query: "CREATE PROMOTION POLICY nofor IF NOT EXISTS APPLY { ON ACCESS { SET n.c = 1 } }", stage: RawSource, fragments: []string{}},
		{name: "TestParseDDL_KnowledgePolicyForRequiresTarget/0", query: "CREATE PROMOTION POLICY p FOR", stage: RawSource, fragments: []string{}},
		{name: "TestParseDDL_KnowledgePolicyForRequiresTarget/1", query: "CREATE PROMOTION POLICY p FOR APPLY { ON ACCESS { SET n.c = 1 } }", stage: RawSource, fragments: []string{}},
		{name: "TestParseDDL_KnowledgePolicyForRequiresTarget/2", query: "ALTER PROMOTION POLICY p FOR", stage: RawSource, fragments: []string{}},
		{name: "TestParseDDL_KnowledgePolicyForRequiresTarget/3", query: "CREATE DECAY PROFILE d FOR", stage: RawSource, fragments: []string{}},
		{name: "TestParseDDL_KnowledgePolicyForRequiresTarget/4", query: "ALTER DECAY PROFILE d FOR", stage: RawSource, fragments: []string{}},
		{name: "TestParseDDL_KnowledgeProfileRejectsTrailingText/0", query: "CREATE DECAY PROFILE d OPTIONS { halfLifeSeconds: 3600, function: 'exponential', scope: 'NODE', scoreFrom: 'CREATED' } GARBAGE", stage: RawSource, fragments: []string{}},
		{name: "TestParseDDL_KnowledgeProfileRejectsTrailingText/1", query: "CREATE DECAY PROFILE binding FOR (n:Fact) APPLY { DECAY PROFILE 'd' } GARBAGE", stage: RawSource, fragments: []string{}},
		{name: "TestParseDDL_KnowledgeProfileRejectsTrailingText/2", query: "ALTER DECAY PROFILE binding FOR (n:Fact) APPLY { DECAY PROFILE 'd' } GARBAGE", stage: RawSource, fragments: []string{}},
		{name: "TestParseDDL_KnowledgeProfileRejectsTrailingText/3", query: "CREATE PROMOTION PROFILE p OPTIONS { multiplier: 1.5 } GARBAGE", stage: RawSource, fragments: []string{}},
		{name: "TestParseDDL_KnowledgeProfileRejectsTrailingText/4", query: "ALTER PROMOTION PROFILE p SET OPTIONS { multiplier: 1.5 } GARBAGE", stage: RawSource, fragments: []string{}},
		{name: "TestParseDDL_KnowledgeProfileRejectsTrailingText/5", query: "ALTER DECAY PROFILE d SET OPTIONS { halfLifeSeconds: 3600 } GARBAGE", stage: RawSource, fragments: []string{}},
		{name: "TestParseDDL_DropPromotionPolicy_MissingName", query: `DROP PROMOTION POLICY`, stage: RawSource, fragments: []string{"expected policy name"}},
		{name: "TestParseDDL_CreateDecayProfileBinding_MissingApplyBraces", query: `CREATE DECAY PROFILE broken_bind FOR (n:Fact) APPLY`, stage: RawSource, fragments: []string{"expected {"}},
		{name: "TestParseDDL_OnAccess_MissingBraces", query: `CREATE PROMOTION POLICY broken_oa FOR (n:Fact) APPLY {
		ON ACCESS
	}`, stage: RawSource, fragments: []string{"expected {"}},
		{name: "TestParseDDL_WhenClause_MissingApplyProfile", query: `CREATE PROMOTION POLICY bad_when FOR (n:Fact) APPLY {
		WHEN n.accessCount > 10
	}`, stage: RawSource, fragments: []string{"expected APPLY"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := ParseKnowledgePolicyDDL(t.Context(), tc.query, tc.stage)
			require.Error(t, err, tc.query)
			for _, fragment := range tc.fragments {
				assert.Contains(t, err.Error(), fragment)
			}
		})
	}
}

func TestKnowledgePolicyDDLNotOwned(t *testing.T) {
	cases := []struct {
		name, query string
		stage       SourceStage
	}{
		{name: "TestParseDDL_NonKnowledgePolicyStatement/0", query: "MATCH (n) RETURN n", stage: RawSource},
		{name: "TestParseDDL_NonKnowledgePolicyStatement/1", query: "CREATE (n:Person {name: 'Alice'})", stage: RawSource},
		{name: "TestParseDDL_NonKnowledgePolicyStatement/2", query: "CREATE INDEX ON :Person(name)", stage: RawSource},
		{name: "TestParseDDL_NonKnowledgePolicyStatement/3", query: "", stage: RawSource},
		{name: "TestParseDDL_NonKnowledgePolicyStatement/4", query: "   ", stage: RawSource},
		{name: "TestParseDDL_UnrelatedCreateStatement", query: `CREATE INDEX ON :Person(name)`, stage: RawSource},
		{name: "TestParseDDL_UnrelatedAlterStatement", query: `ALTER INDEX foo`, stage: RawSource},
		{name: "TestParseDDL_UnrelatedDropStatement", query: `DROP INDEX foo`, stage: RawSource},
		{name: "TestParseDDL_UnrelatedShowStatement", query: `SHOW INDEXES`, stage: RawSource},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cmd, ok, err := ParseKnowledgePolicyDDL(t.Context(), tc.query, tc.stage)
			assert.Nil(t, cmd)
			assert.False(t, ok)
			assert.NoError(t, err)
		})
	}
}

func TestKnowledgePolicyDDLProducts_CreateDecayProfileBundleCmd(t *testing.T) {
	runKPProductCases(t, []kpDDLProductCase[*CreateDecayProfileBundleCmd]{
		{name: "TestParseDDL_CreateDecayProfileBundle", query: `CREATE DECAY PROFILE slow_decay OPTIONS {
		halfLifeSeconds: 604800,
		visibilityThreshold: 0.10,
		scoreFloor: 0.05,
		function: 'exponential',
		scope: 'NODE',
		decayEnabled: true,
		scoreFrom: 'CREATED',
		enabled: true
	}`, stage: RawSource, check: func(t *testing.T, c *CreateDecayProfileBundleCmd) {
			assert.Equal(t, "slow_decay", c.Bundle.Name)
			assert.Equal(t, int64(604800), c.Bundle.HalfLifeSeconds)
			assert.Equal(t, 0.10, c.Bundle.VisibilityThreshold)
			assert.Equal(t, 0.05, c.Bundle.ScoreFloor)
			assert.Equal(t, knowledgepolicy.DecayFunctionExponential, c.Bundle.Function)
			assert.Equal(t, knowledgepolicy.ScopeNode, c.Bundle.Scope)
			assert.True(t, c.Bundle.DecayEnabled)
			assert.Equal(t, knowledgepolicy.ScoreFromCreated, c.Bundle.ScoreFrom)
			assert.True(t, c.Bundle.Enabled)

		}},
		{name: "TestParseDDL_CreateDecayProfileBundle_QuotedName", query: `CREATE DECAY PROFILE 'my-profile' OPTIONS {
		halfLifeSeconds: 3600,
		function: 'linear',
		scope: 'EDGE',
		scoreFrom: 'VERSION'
	}`, stage: RawSource, check: func(t *testing.T, c *CreateDecayProfileBundleCmd) {
			assert.Equal(t, "my-profile", c.Bundle.Name)
			assert.Equal(t, int64(3600), c.Bundle.HalfLifeSeconds)
			assert.Equal(t, knowledgepolicy.DecayFunctionLinear, c.Bundle.Function)
			assert.Equal(t, knowledgepolicy.ScopeEdge, c.Bundle.Scope)
			assert.Equal(t, knowledgepolicy.ScoreFromVersion, c.Bundle.ScoreFrom)

		}},
		{name: "TestParseDDL_CreateDecayProfileBundle_CustomScoreFrom", query: `CREATE DECAY PROFILE custom_anchor OPTIONS {
		halfLifeSeconds: 86400,
		function: 'step',
		scope: 'NODE',
		scoreFrom: 'CUSTOM',
		scoreFromProperty: 'myTimestamp'
	}`, stage: RawSource, check: func(t *testing.T, c *CreateDecayProfileBundleCmd) {
			assert.Equal(t, knowledgepolicy.ScoreFromCustom, c.Bundle.ScoreFrom)
			assert.Equal(t, "myTimestamp", c.Bundle.ScoreFromProperty)

		}},
		{name: "TestParseDDL_CreateDecayProfileBundle_EnabledDefaultsTrue", query: `CREATE DECAY PROFILE minimal OPTIONS {
		halfLifeSeconds: 100,
		function: 'none',
		scope: 'NODE',
		scoreFrom: 'CREATED'
	}`, stage: RawSource, check: func(t *testing.T, c *CreateDecayProfileBundleCmd) {
			assert.True(t, c.Bundle.Enabled, "enabled should default to true")

		}},
		{name: "TestParseDDL_CreateDecayProfileBundle_CaseInsensitiveKeywords", query: `create decay profile ci_test options {
		halfLifeSeconds: 100,
		function: 'exponential',
		scope: 'NODE',
		scoreFrom: 'CREATED'
	}`, stage: RawSource, check: func(t *testing.T, c *CreateDecayProfileBundleCmd) {
			assert.Equal(t, "ci_test", c.Bundle.Name)

		}},
		{name: "TestParseDDL_ExtraWhitespace", query: `   CREATE   DECAY   PROFILE   ws_test   OPTIONS   {
		halfLifeSeconds:   100  ,
		function:   'exponential'  ,
		scope:  'NODE'  ,
		scoreFrom:  'CREATED'
	}  `, stage: RawSource, check: func(t *testing.T, c *CreateDecayProfileBundleCmd) {
			assert.Equal(t, "ws_test", c.Bundle.Name)

		}},
		{name: "TestParseDDL_EqualsSignSeparator", query: `CREATE DECAY PROFILE eq_test OPTIONS {
		halfLifeSeconds = 200,
		function = 'linear',
		scope = 'NODE',
		scoreFrom = 'CREATED'
	}`, stage: RawSource, check: func(t *testing.T, c *CreateDecayProfileBundleCmd) {
			assert.Equal(t, int64(200), c.Bundle.HalfLifeSeconds)
			assert.Equal(t, knowledgepolicy.DecayFunctionLinear, c.Bundle.Function)

		}},
		{name: "TestParseDDL_SemicolonSeparator", query: `CREATE DECAY PROFILE semi_test OPTIONS {
		halfLifeSeconds: 300;
		function: 'step';
		scope: 'NODE';
		scoreFrom: 'CREATED'
	}`, stage: RawSource, check: func(t *testing.T, c *CreateDecayProfileBundleCmd) {
			assert.Equal(t, int64(300), c.Bundle.HalfLifeSeconds)

		}},
		{name: "TestPlanDDL_WorkingMemoryBundle", query: `CREATE DECAY PROFILE working_memory OPTIONS {
		halfLifeSeconds: 604800,
		function: 'exponential',
		visibilityThreshold: 0.10,
		scoreFloor: 0.01
	}`, stage: RawSource, check: func(t *testing.T, c *CreateDecayProfileBundleCmd) {
			assert.Equal(t, "working_memory", c.Bundle.Name)
			assert.Equal(t, int64(604800), c.Bundle.HalfLifeSeconds)
			assert.Equal(t, knowledgepolicy.DecayFunctionExponential, c.Bundle.Function)
			assert.Equal(t, 0.10, c.Bundle.VisibilityThreshold)
			assert.Equal(t, 0.01, c.Bundle.ScoreFloor)

		}},
	})
}

func TestKnowledgePolicyDDLProducts_CreateDecayProfileBindingCmd(t *testing.T) {
	runKPProductCases(t, []kpDDLProductCase[*CreateDecayProfileBindingCmd]{
		{name: "TestParseDDL_CreateDecayProfileBinding_NodeLabel", query: `CREATE DECAY PROFILE fact_binding FOR (n:KnowledgeFact) APPLY {
		DECAY PROFILE 'slow_decay'
		DECAY VISIBILITY THRESHOLD 0.10
		n.content NO DECAY
	}`, stage: RawSource, check: func(t *testing.T, c *CreateDecayProfileBindingCmd) {
			assert.Equal(t, "fact_binding", c.Binding.Name)
			assert.Equal(t, []string{"KnowledgeFact"}, c.Binding.TargetLabels)
			assert.False(t, c.Binding.IsEdge)
			assert.False(t, c.Binding.IsWildcard)
			assert.Equal(t, "slow_decay", c.Binding.ProfileRef)
			require.NotNil(t, c.Binding.VisibilityThreshold)
			assert.Equal(t, 0.10, *c.Binding.VisibilityThreshold)
			require.Len(t, c.Binding.PropertyRules, 1)
			assert.Equal(t, "content", c.Binding.PropertyRules[0].PropertyPath)
			assert.True(t, c.Binding.PropertyRules[0].NoDecay)

		}},
		{name: "TestParseDDL_CreateDecayProfileBinding_MultiLabel", query: `CREATE DECAY PROFILE multi_bind FOR (n:KnowledgeFact:MemoryEpisode) APPLY {
		DECAY PROFILE 'slow_decay'
	}`, stage: RawSource, check: func(t *testing.T, c *CreateDecayProfileBindingCmd) {
			assert.Equal(t, []string{"KnowledgeFact", "MemoryEpisode"}, c.Binding.TargetLabels)

		}},
		{name: "TestParseDDL_CreateDecayProfileBinding_Wildcard", query: `CREATE DECAY PROFILE wildcard_bind FOR () APPLY {
		NO DECAY
	}`, stage: RawSource, check: func(t *testing.T, c *CreateDecayProfileBindingCmd) {
			assert.True(t, c.Binding.IsWildcard)
			assert.True(t, c.Binding.NoDecay)

		}},
		{name: "TestParseDDL_CreateDecayProfileBinding_EdgePattern", query: `CREATE DECAY PROFILE edge_bind FOR ()-[r:RELATES_TO]-() APPLY {
		DECAY PROFILE 'edge_profile'
	}`, stage: RawSource, check: func(t *testing.T, c *CreateDecayProfileBindingCmd) {
			assert.True(t, c.Binding.IsEdge)
			assert.Equal(t, "RELATES_TO", c.Binding.TargetEdgeType)
			assert.Equal(t, "edge_profile", c.Binding.ProfileRef)

		}},
		{name: "TestParseDDL_CreateDecayProfileBinding_PropertyRuleWithProfileRef", query: `CREATE DECAY PROFILE prop_rule_bind FOR (n:User) APPLY {
		DECAY PROFILE 'base_decay'
		n.score DECAY PROFILE 'slow_decay'
		n.score DECAY HALF LIFE 3600
		n.score DECAY FLOOR 0.01
	}`, stage: RawSource, check: func(t *testing.T, c *CreateDecayProfileBindingCmd) {
			require.Len(t, c.Binding.PropertyRules, 1)
			rule := c.Binding.PropertyRules[0]
			assert.Equal(t, "score", rule.PropertyPath)
			assert.Equal(t, "slow_decay", rule.ProfileRef)
			assert.Equal(t, int64(3600), rule.HalfLifeSeconds)
			assert.Equal(t, 0.01, rule.ScoreFloor)
			assert.Equal(t, 0, rule.Order)

		}},
		{name: "TestParseDDL_CreateDecayProfileBinding_MultiplePropertyRules", query: `CREATE DECAY PROFILE multi_prop FOR (n:Doc) APPLY {
		DECAY PROFILE 'base'
		n.metadata NO DECAY
		n.confidence DECAY HALF LIFE 7200
	}`, stage: RawSource, check: func(t *testing.T, c *CreateDecayProfileBindingCmd) {
			require.Len(t, c.Binding.PropertyRules, 2)
			assert.Equal(t, "metadata", c.Binding.PropertyRules[0].PropertyPath)
			assert.True(t, c.Binding.PropertyRules[0].NoDecay)
			assert.Equal(t, 0, c.Binding.PropertyRules[0].Order)
			assert.Equal(t, "confidence", c.Binding.PropertyRules[1].PropertyPath)
			assert.Equal(t, int64(7200), c.Binding.PropertyRules[1].HalfLifeSeconds)
			assert.Equal(t, 1, c.Binding.PropertyRules[1].Order)

		}},
		{name: "TestPlanDDL_SessionRecordRetention", query: `CREATE DECAY PROFILE session_record_retention
FOR (n:SessionRecord)
APPLY {
  DECAY PROFILE 'working_memory'
  DECAY VISIBILITY THRESHOLD 0.10
  n.summary DECAY PROFILE 'session_summary'
  n.lastConversationSummary DECAY HALF LIFE 2592000
  n.tenantId NO DECAY
}`, stage: RawSource, check: func(t *testing.T, c *CreateDecayProfileBindingCmd) {
			assert.Equal(t, "session_record_retention", c.Binding.Name)
			assert.Equal(t, []string{"SessionRecord"}, c.Binding.TargetLabels)
			assert.False(t, c.Binding.IsEdge)
			assert.Equal(t, "working_memory", c.Binding.ProfileRef)
			require.NotNil(t, c.Binding.VisibilityThreshold)
			assert.Equal(t, 0.10, *c.Binding.VisibilityThreshold)

			require.Len(t, c.Binding.PropertyRules, 3)
			assert.Equal(t, "summary", c.Binding.PropertyRules[0].PropertyPath)
			assert.Equal(t, "session_summary", c.Binding.PropertyRules[0].ProfileRef)

			assert.Equal(t, "lastConversationSummary", c.Binding.PropertyRules[1].PropertyPath)
			assert.Equal(t, int64(2592000), c.Binding.PropertyRules[1].HalfLifeSeconds)

			assert.Equal(t, "tenantId", c.Binding.PropertyRules[2].PropertyPath)
			assert.True(t, c.Binding.PropertyRules[2].NoDecay)

		}},
		{name: "TestPlanDDL_CoaccessRetention", query: `CREATE DECAY PROFILE coaccess_retention
FOR ()-[r:CO_ACCESSED]-()
APPLY {
  DECAY HALF LIFE 1209600
  r.signalScore DECAY HALF LIFE 1209600
  r.signalScore DECAY FLOOR 0.15
  r.externalId NO DECAY
}`, stage: RawSource, check: func(t *testing.T, c *CreateDecayProfileBindingCmd) {
			assert.Equal(t, "coaccess_retention", c.Binding.Name)
			assert.True(t, c.Binding.IsEdge)
			assert.Equal(t, "CO_ACCESSED", c.Binding.TargetEdgeType)
			assert.Equal(t, int64(1209600), c.Binding.HalfLifeSeconds)

			require.Len(t, c.Binding.PropertyRules, 2)
			assert.Equal(t, "signalScore", c.Binding.PropertyRules[0].PropertyPath)
			assert.Equal(t, int64(1209600), c.Binding.PropertyRules[0].HalfLifeSeconds)
			assert.Equal(t, 0.15, c.Binding.PropertyRules[0].ScoreFloor)

			assert.Equal(t, "externalId", c.Binding.PropertyRules[1].PropertyPath)
			assert.True(t, c.Binding.PropertyRules[1].NoDecay)

		}},
		{name: "TestPlanDDL_CanonicalLinkRetention", query: `CREATE DECAY PROFILE canonical_link_retention
FOR ()-[r:CANONICAL_LINK]-()
APPLY {
  NO DECAY
  r.externalId NO DECAY
  r.sourceSystem NO DECAY
}`, stage: RawSource, check: func(t *testing.T, c *CreateDecayProfileBindingCmd) {
			assert.Equal(t, "canonical_link_retention", c.Binding.Name)
			assert.True(t, c.Binding.IsEdge)
			assert.Equal(t, "CANONICAL_LINK", c.Binding.TargetEdgeType)
			assert.True(t, c.Binding.NoDecay)

			require.Len(t, c.Binding.PropertyRules, 2)
			assert.Equal(t, "externalId", c.Binding.PropertyRules[0].PropertyPath)
			assert.True(t, c.Binding.PropertyRules[0].NoDecay)
			assert.Equal(t, "sourceSystem", c.Binding.PropertyRules[1].PropertyPath)
			assert.True(t, c.Binding.PropertyRules[1].NoDecay)

		}},
		{name: "TestPlanDDL_ReviewLinkRetention", query: `CREATE DECAY PROFILE review_link_retention
FOR ()-[r:REVIEWED_WITH]-()
APPLY {
  DECAY HALF LIFE 604800
  r.confidence DECAY HALF LIFE 86400
  r.confidence DECAY FLOOR 0.25
}`, stage: RawSource, check: func(t *testing.T, c *CreateDecayProfileBindingCmd) {
			assert.Equal(t, "review_link_retention", c.Binding.Name)
			assert.True(t, c.Binding.IsEdge)
			assert.Equal(t, "REVIEWED_WITH", c.Binding.TargetEdgeType)
			assert.Equal(t, int64(604800), c.Binding.HalfLifeSeconds)

			require.Len(t, c.Binding.PropertyRules, 1)
			assert.Equal(t, "confidence", c.Binding.PropertyRules[0].PropertyPath)
			assert.Equal(t, int64(86400), c.Binding.PropertyRules[0].HalfLifeSeconds)
			assert.Equal(t, 0.25, c.Binding.PropertyRules[0].ScoreFloor)

		}},
		{name: "TestPlanDDL_MultiLabelTarget", query: `CREATE DECAY PROFILE multi_label
FOR (n:SessionRecord:MemoryEpisode)
APPLY {
  DECAY PROFILE 'working_memory'
}`, stage: RawSource, check: func(t *testing.T, c *CreateDecayProfileBindingCmd) {
			assert.Equal(t, []string{"SessionRecord", "MemoryEpisode"}, c.Binding.TargetLabels)

		}},
		{name: "TestPlanDDL_EntityLevelDecayHalfLife", query: `CREATE DECAY PROFILE short_lived
FOR (n:Temp)
APPLY {
  DECAY HALF LIFE 86400
}`, stage: RawSource, check: func(t *testing.T, c *CreateDecayProfileBindingCmd) {
			assert.Equal(t, int64(86400), c.Binding.HalfLifeSeconds)

		}},
		{name: "TestPlanDDL_EntityLevelDecayFloor", query: `CREATE DECAY PROFILE floored
FOR (n:Archive)
APPLY {
  DECAY HALF LIFE 604800
  DECAY FLOOR 0.05
}`, stage: RawSource, check: func(t *testing.T, c *CreateDecayProfileBindingCmd) {
			assert.Equal(t, int64(604800), c.Binding.HalfLifeSeconds)
			assert.Equal(t, 0.05, c.Binding.ScoreFloor)

		}},
		{name: "TestPlanDDL_PropertyDecayProfile", query: `CREATE DECAY PROFILE prop_ref
FOR (n:Doc)
APPLY {
  n.summary DECAY PROFILE 'session_summary'
}`, stage: RawSource, check: func(t *testing.T, c *CreateDecayProfileBindingCmd) {
			require.Len(t, c.Binding.PropertyRules, 1)
			assert.Equal(t, "summary", c.Binding.PropertyRules[0].PropertyPath)
			assert.Equal(t, "session_summary", c.Binding.PropertyRules[0].ProfileRef)

		}},
		{name: "TestPlanDDL_PropertyDecayHalfLife", query: `CREATE DECAY PROFILE prop_hl
FOR (n:Doc)
APPLY {
  n.content DECAY HALF LIFE 1209600
}`, stage: RawSource, check: func(t *testing.T, c *CreateDecayProfileBindingCmd) {
			require.Len(t, c.Binding.PropertyRules, 1)
			assert.Equal(t, "content", c.Binding.PropertyRules[0].PropertyPath)
			assert.Equal(t, int64(1209600), c.Binding.PropertyRules[0].HalfLifeSeconds)

		}},
		{name: "TestPlanDDL_PropertyDecayFloor", query: `CREATE DECAY PROFILE prop_floor
FOR (n:Doc)
APPLY {
  n.summary DECAY HALF LIFE 1209600
  n.summary DECAY FLOOR 0.10
}`, stage: RawSource, check: func(t *testing.T, c *CreateDecayProfileBindingCmd) {
			require.Len(t, c.Binding.PropertyRules, 1)
			assert.Equal(t, "summary", c.Binding.PropertyRules[0].PropertyPath)
			assert.Equal(t, int64(1209600), c.Binding.PropertyRules[0].HalfLifeSeconds)
			assert.Equal(t, 0.10, c.Binding.PropertyRules[0].ScoreFloor)

		}},
	})
}

func TestKnowledgePolicyDDLProducts_AlterDecayProfileCmd(t *testing.T) {
	runKPProductCases(t, []kpDDLProductCase[*AlterDecayProfileCmd]{
		{name: "TestParseDDL_AlterDecayProfile", query: `ALTER DECAY PROFILE slow_decay SET OPTIONS {
		halfLifeSeconds: 1209600,
		visibilityThreshold: 0.05
	}`, stage: RawSource, check: func(t *testing.T, c *AlterDecayProfileCmd) {
			assert.Equal(t, "slow_decay", c.Name)
			assert.Equal(t, int64(1209600), c.Updates["halfLifeSeconds"])
			assert.Equal(t, 0.05, c.Updates["visibilityThreshold"])

		}},
		{name: "TestParseDDL_AlterDecayProfile_QuotedName", query: `ALTER DECAY PROFILE 'my-profile' SET OPTIONS { enabled: false }`, stage: RawSource, check: func(t *testing.T, c *AlterDecayProfileCmd) {
			assert.Equal(t, "my-profile", c.Name)
			assert.Equal(t, false, c.Updates["enabled"])

		}},
	})
}

func TestKnowledgePolicyDDLProducts_AlterDecayProfileBindingCmd(t *testing.T) {
	runKPProductCases(t, []kpDDLProductCase[*AlterDecayProfileBindingCmd]{
		{name: "TestParseDDL_AlterDecayProfileBinding", query: `ALTER DECAY PROFILE fact_binding FOR (n:KnowledgeFact:Reviewed) APPLY {
		DECAY PROFILE 'slow_decay'
		DECAY VISIBILITY THRESHOLD 0.25
		n.summary NO DECAY
	}`, stage: RawSource, check: func(t *testing.T, c *AlterDecayProfileBindingCmd) {
			assert.Equal(t, "fact_binding", c.Binding.Name)
			assert.Equal(t, []string{"KnowledgeFact", "Reviewed"}, c.Binding.TargetLabels)
			assert.Equal(t, "slow_decay", c.Binding.ProfileRef)
			require.NotNil(t, c.Binding.VisibilityThreshold)
			assert.Equal(t, 0.25, *c.Binding.VisibilityThreshold)
			require.Len(t, c.Binding.PropertyRules, 1)
			assert.Equal(t, "summary", c.Binding.PropertyRules[0].PropertyPath)
			assert.True(t, c.Binding.PropertyRules[0].NoDecay)

		}},
	})
}

func TestKnowledgePolicyDDLProducts_DropDecayProfileCmd(t *testing.T) {
	runKPProductCases(t, []kpDDLProductCase[*DropDecayProfileCmd]{
		{name: "TestParseDDL_DropDecayProfile", query: `DROP DECAY PROFILE slow_decay`, stage: RawSource, check: func(t *testing.T, c *DropDecayProfileCmd) {
			assert.Equal(t, "slow_decay", c.Name)
			assert.False(t, c.IfExists)

		}},
		{name: "TestParseDDL_DropDecayProfile_IfExists", query: `DROP DECAY PROFILE IF EXISTS slow_decay`, stage: RawSource, check: func(t *testing.T, c *DropDecayProfileCmd) {
			assert.Equal(t, "slow_decay", c.Name)
			assert.True(t, c.IfExists)

		}},
		{name: "TestParseDDL_DropDecayProfile_QuotedName", query: `DROP DECAY PROFILE 'my-profile'`, stage: RawSource, check: func(t *testing.T, c *DropDecayProfileCmd) {
			assert.Equal(t, "my-profile", c.Name)

		}},
		{name: "TestPlanDDL_DropDecayProfileByName", query: `DROP DECAY PROFILE session_record_retention;`, stage: RawSource, check: func(t *testing.T, c *DropDecayProfileCmd) {
			assert.Equal(t, "session_record_retention", c.Name)

		}},
	})
}

func TestKnowledgePolicyDDLProducts_ShowDecayProfilesCmd(t *testing.T) {
	runKPProductCases(t, []kpDDLProductCase[*ShowDecayProfilesCmd]{
		{name: "TestParseDDL_ShowDecayProfiles", query: `SHOW DECAY PROFILES`, stage: RawSource, check: func(t *testing.T, c *ShowDecayProfilesCmd) {

		}},
		{name: "TestParseDDL_ShowDecayProfiles_CaseInsensitive", query: `show decay profiles`, stage: RawSource, check: func(t *testing.T, c *ShowDecayProfilesCmd) {

		}},
		{name: "TestPlanDDL_ShowDecayProfiles", query: `SHOW DECAY PROFILES;`, stage: RawSource, check: func(t *testing.T, c *ShowDecayProfilesCmd) {

		}},
	})
}

func TestKnowledgePolicyDDLProducts_CreatePromotionProfileCmd(t *testing.T) {
	runKPProductCases(t, []kpDDLProductCase[*CreatePromotionProfileCmd]{
		{name: "TestParseDDL_CreatePromotionProfile", query: `CREATE PROMOTION PROFILE boost OPTIONS {
		scope: 'NODE',
		multiplier: 2.0,
		scoreFloor: 0.3,
		scoreCap: 1.0,
		enabled: true
	}`, stage: RawSource, check: func(t *testing.T, c *CreatePromotionProfileCmd) {
			assert.Equal(t, "boost", c.Profile.Name)
			assert.Equal(t, knowledgepolicy.ScopeNode, c.Profile.Scope)
			assert.Equal(t, 2.0, c.Profile.Multiplier)
			assert.Equal(t, 0.3, c.Profile.ScoreFloor)
			assert.Equal(t, 1.0, c.Profile.ScoreCap)
			assert.True(t, c.Profile.Enabled)

		}},
		{name: "TestParseDDL_CreatePromotionProfile_QuotedName", query: `CREATE PROMOTION PROFILE 'heavy-boost' OPTIONS {
		scope: 'EDGE',
		multiplier: 3.0,
		scoreCap: 1.0
	}`, stage: RawSource, check: func(t *testing.T, c *CreatePromotionProfileCmd) {
			assert.Equal(t, "heavy-boost", c.Profile.Name)
			assert.Equal(t, knowledgepolicy.ScopeEdge, c.Profile.Scope)

		}},
		{name: "TestParseDDL_CreatePromotionProfile_EnabledDefaultsTrue", query: `CREATE PROMOTION PROFILE default_enabled OPTIONS {
		scope: 'NODE',
		multiplier: 1.5,
		scoreCap: 1.0
	}`, stage: RawSource, check: func(t *testing.T, c *CreatePromotionProfileCmd) {
			assert.True(t, c.Profile.Enabled, "enabled should default to true")

		}},
		{name: "TestPlanDDL_ReinforcedTierPromotionProfile", query: `CREATE PROMOTION PROFILE reinforced_tier OPTIONS {
		multiplier: 1.5,
		scoreFloor: 0.3,
		scoreCap: 1.0
	}`, stage: RawSource, check: func(t *testing.T, c *CreatePromotionProfileCmd) {
			assert.Equal(t, "reinforced_tier", c.Profile.Name)
			assert.Equal(t, 1.5, c.Profile.Multiplier)
			assert.Equal(t, 0.3, c.Profile.ScoreFloor)
			assert.Equal(t, 1.0, c.Profile.ScoreCap)

		}},
	})
}

func TestKnowledgePolicyDDLProducts_AlterPromotionProfileCmd(t *testing.T) {
	runKPProductCases(t, []kpDDLProductCase[*AlterPromotionProfileCmd]{
		{name: "TestParseDDL_AlterPromotionProfile", query: `ALTER PROMOTION PROFILE boost SET OPTIONS {
		multiplier: 3.0,
		scoreCap: 0.95
	}`, stage: RawSource, check: func(t *testing.T, c *AlterPromotionProfileCmd) {
			assert.Equal(t, "boost", c.Name)
			assert.Equal(t, 3.0, c.Updates["multiplier"])
			assert.Equal(t, 0.95, c.Updates["scoreCap"])

		}},
	})
}

func TestKnowledgePolicyDDLProducts_DropPromotionProfileCmd(t *testing.T) {
	runKPProductCases(t, []kpDDLProductCase[*DropPromotionProfileCmd]{
		{name: "TestParseDDL_DropPromotionProfile", query: `DROP PROMOTION PROFILE boost`, stage: RawSource, check: func(t *testing.T, c *DropPromotionProfileCmd) {
			assert.Equal(t, "boost", c.Name)
			assert.False(t, c.IfExists)

		}},
		{name: "TestParseDDL_DropPromotionProfile_IfExists", query: `DROP PROMOTION PROFILE IF EXISTS boost`, stage: RawSource, check: func(t *testing.T, c *DropPromotionProfileCmd) {
			assert.Equal(t, "boost", c.Name)
			assert.True(t, c.IfExists)

		}},
		{name: "TestPlanDDL_DropPromotionProfile", query: `DROP PROMOTION PROFILE reinforced_tier;`, stage: RawSource, check: func(t *testing.T, c *DropPromotionProfileCmd) {
			assert.Equal(t, "reinforced_tier", c.Name)

		}},
	})
}

func TestKnowledgePolicyDDLProducts_ShowPromotionProfilesCmd(t *testing.T) {
	runKPProductCases(t, []kpDDLProductCase[*ShowPromotionProfilesCmd]{
		{name: "TestParseDDL_ShowPromotionProfiles", query: `SHOW PROMOTION PROFILES`, stage: RawSource, check: func(t *testing.T, c *ShowPromotionProfilesCmd) {

		}},
	})
}

func TestKnowledgePolicyDDLProducts_CreatePromotionPolicyCmd(t *testing.T) {
	runKPProductCases(t, []kpDDLProductCase[*CreatePromotionPolicyCmd]{
		{name: "TestParseDDL_CreatePromotionPolicy_WithOnAccess", query: `CREATE PROMOTION POLICY fact_promo FOR (n:KnowledgeFact) APPLY {
		ON ACCESS {
			SET n.accessCount = n.accessCount + 1
		}
	}`, stage: RawSource, check: func(t *testing.T, c *CreatePromotionPolicyCmd) {
			assert.Equal(t, "fact_promo", c.Policy.Name)
			assert.Equal(t, []string{"KnowledgeFact"}, c.Policy.TargetLabels)
			assert.False(t, c.Policy.IsEdge)
			assert.False(t, c.Policy.IsWildcard)
			assert.True(t, c.Policy.Enabled)
			require.NotNil(t, c.Policy.OnAccess)
			require.Len(t, c.Policy.OnAccess.Mutations, 1)
			assert.Equal(t, "n.accessCount = n.accessCount + 1", c.Policy.OnAccess.Mutations[0].Expression)
			assert.Nil(t, c.Policy.OnAccess.Mutations[0].Kalman)

		}},
		{name: "TestParseDDL_CreatePromotionPolicy_WithWhenClauses", query: `CREATE PROMOTION POLICY when_promo FOR (n:KnowledgeFact) APPLY {
		WHEN n.accessCount > 10 APPLY PROFILE boost
		WHEN n.mutationCount > 50 APPLY PROFILE heavy_boost
	}`, stage: RawSource, check: func(t *testing.T, c *CreatePromotionPolicyCmd) {
			require.Len(t, c.Policy.WhenClauses, 2)
			assert.Equal(t, "n.accessCount > 10", c.Policy.WhenClauses[0].Predicate)
			assert.Equal(t, "boost", c.Policy.WhenClauses[0].ProfileRef)
			assert.Equal(t, 0, c.Policy.WhenClauses[0].Order)
			assert.Equal(t, "n.mutationCount > 50", c.Policy.WhenClauses[1].Predicate)
			assert.Equal(t, "heavy_boost", c.Policy.WhenClauses[1].ProfileRef)
			assert.Equal(t, 1, c.Policy.WhenClauses[1].Order)

		}},
		{name: "TestParseDDL_CreatePromotionPolicy_WithWhenClause_QuotedProfileRef", query: `CREATE PROMOTION POLICY qref_promo FOR (n:User) APPLY {
		WHEN n.score > 0.5 APPLY PROFILE 'my-boost'
	}`, stage: RawSource, check: func(t *testing.T, c *CreatePromotionPolicyCmd) {
			require.Len(t, c.Policy.WhenClauses, 1)
			assert.Equal(t, "my-boost", c.Policy.WhenClauses[0].ProfileRef)

		}},
		{name: "TestParseDDL_CreatePromotionPolicy_EdgeTarget", query: `CREATE PROMOTION POLICY edge_promo FOR ()-[r:SUPERSEDES]-() APPLY {
		ON ACCESS {
			SET r.traversalCount = r.traversalCount + 1
		}
	}`, stage: RawSource, check: func(t *testing.T, c *CreatePromotionPolicyCmd) {
			assert.True(t, c.Policy.IsEdge)
			assert.Equal(t, "SUPERSEDES", c.Policy.TargetEdgeType)

		}},
		{name: "TestParseDDL_CreatePromotionPolicy_WildcardTarget", query: `CREATE PROMOTION POLICY wild_promo FOR () APPLY {
		ON ACCESS {
			SET n.accessCount = n.accessCount + 1
		}
	}`, stage: RawSource, check: func(t *testing.T, c *CreatePromotionPolicyCmd) {
			assert.True(t, c.Policy.IsWildcard)

		}},
		{name: "TestParseDDL_OnAccess_PlainSetNoKalman", query: `CREATE PROMOTION POLICY plain_set FOR (n:Fact) APPLY {
		ON ACCESS {
			SET n.accessCount = n.accessCount + 1
		}
	}`, stage: RawSource, check: func(t *testing.T, c *CreatePromotionPolicyCmd) {
			require.NotNil(t, c.Policy.OnAccess)
			require.Len(t, c.Policy.OnAccess.Mutations, 1)
			assert.Equal(t, "n.accessCount = n.accessCount + 1", c.Policy.OnAccess.Mutations[0].Expression)
			assert.Nil(t, c.Policy.OnAccess.Mutations[0].Kalman, "plain SET should have nil Kalman")

		}},
		{name: "TestParseDDL_OnAccess_WithKalmanAutoDefaults", query: `CREATE PROMOTION POLICY kalman_auto FOR (n:Fact) APPLY {
		ON ACCESS {
			WITH KALMAN SET n.confidenceScore = $evaluatedConfidence
		}
	}`, stage: RawSource, check: func(t *testing.T, c *CreatePromotionPolicyCmd) {
			require.NotNil(t, c.Policy.OnAccess)
			require.Len(t, c.Policy.OnAccess.Mutations, 1)

			mut := c.Policy.OnAccess.Mutations[0]
			assert.Equal(t, "n.confidenceScore = $evaluatedConfidence", mut.Expression)
			require.NotNil(t, mut.Kalman, "WITH KALMAN should produce non-nil KalmanConfig")
			assert.Equal(t, knowledgepolicy.KalmanModeAuto, mut.Kalman.Mode)
			assert.Equal(t, 0.1, mut.Kalman.Q)
			assert.Equal(t, 88.0, mut.Kalman.R)
			assert.Equal(t, 10.0, mut.Kalman.VarianceScale)
			assert.Equal(t, 32, mut.Kalman.WindowSize)

		}},
		{name: "TestParseDDL_OnAccess_WithKalmanManualMode", query: `CREATE PROMOTION POLICY kalman_manual FOR (n:Fact) APPLY {
		ON ACCESS {
			WITH KALMAN{q: 0.1, r: 88.0} SET n.score = $rawScore
		}
	}`, stage: RawSource, check: func(t *testing.T, c *CreatePromotionPolicyCmd) {
			mut := c.Policy.OnAccess.Mutations[0]
			assert.Equal(t, "n.score = $rawScore", mut.Expression)
			require.NotNil(t, mut.Kalman)
			assert.Equal(t, knowledgepolicy.KalmanModeManual, mut.Kalman.Mode)
			assert.Equal(t, 0.1, mut.Kalman.Q)
			assert.Equal(t, 88.0, mut.Kalman.R)

		}},
		{name: "TestParseDDL_OnAccess_WithKalmanAutoCustomQ", query: `CREATE PROMOTION POLICY kalman_auto_q FOR (n:Fact) APPLY {
		ON ACCESS {
			WITH KALMAN{q: 0.05} SET n.confidence = $conf
		}
	}`, stage: RawSource, check: func(t *testing.T, c *CreatePromotionPolicyCmd) {
			mut := c.Policy.OnAccess.Mutations[0]
			require.NotNil(t, mut.Kalman)
			assert.Equal(t, knowledgepolicy.KalmanModeAuto, mut.Kalman.Mode, "no r provided, should be auto")
			assert.Equal(t, 0.05, mut.Kalman.Q)
			assert.Equal(t, 88.0, mut.Kalman.R, "R should keep default for auto mode")
			assert.Equal(t, 10.0, mut.Kalman.VarianceScale)
			assert.Equal(t, 32, mut.Kalman.WindowSize)

		}},
		{name: "TestParseDDL_OnAccess_WithKalmanAllConfigKeys", query: `CREATE PROMOTION POLICY kalman_full FOR (n:Fact) APPLY {
		ON ACCESS {
			WITH KALMAN{q: 0.05, r: 50.0, varianceScale: 5.0, windowSize: 64} SET n.metric = $val
		}
	}`, stage: RawSource, check: func(t *testing.T, c *CreatePromotionPolicyCmd) {
			mut := c.Policy.OnAccess.Mutations[0]
			require.NotNil(t, mut.Kalman)
			assert.Equal(t, knowledgepolicy.KalmanModeManual, mut.Kalman.Mode)
			assert.Equal(t, 0.05, mut.Kalman.Q)
			assert.Equal(t, 50.0, mut.Kalman.R)
			assert.Equal(t, 5.0, mut.Kalman.VarianceScale)
			assert.Equal(t, 64, mut.Kalman.WindowSize)

		}},
		{name: "TestParseDDL_OnAccess_MultipleMutations", query: `CREATE PROMOTION POLICY multi_mut FOR (n:Fact) APPLY {
		ON ACCESS {
			SET n.accessCount = n.accessCount + 1
			WITH KALMAN SET n.confidence = $conf
			WITH KALMAN{q: 0.05, r: 50.0} SET n.score = $rawScore
		}
	}`, stage: RawSource, check: func(t *testing.T, c *CreatePromotionPolicyCmd) {
			require.NotNil(t, c.Policy.OnAccess)
			require.Len(t, c.Policy.OnAccess.Mutations, 3)

			assert.Nil(t, c.Policy.OnAccess.Mutations[0].Kalman)
			assert.Equal(t, "n.accessCount = n.accessCount + 1", c.Policy.OnAccess.Mutations[0].Expression)

			require.NotNil(t, c.Policy.OnAccess.Mutations[1].Kalman)
			assert.Equal(t, knowledgepolicy.KalmanModeAuto, c.Policy.OnAccess.Mutations[1].Kalman.Mode)

			require.NotNil(t, c.Policy.OnAccess.Mutations[2].Kalman)
			assert.Equal(t, knowledgepolicy.KalmanModeManual, c.Policy.OnAccess.Mutations[2].Kalman.Mode)

		}},
		{name: "TestParseDDL_OnAccess_MixedOnAccessAndWhen", query: `CREATE PROMOTION POLICY mixed FOR (n:Fact) APPLY {
		ON ACCESS {
			SET n.accessCount = n.accessCount + 1
		}
		WHEN n.accessCount > 10 APPLY PROFILE boost
	}`, stage: RawSource, check: func(t *testing.T, c *CreatePromotionPolicyCmd) {
			require.NotNil(t, c.Policy.OnAccess)
			require.Len(t, c.Policy.OnAccess.Mutations, 1)
			require.Len(t, c.Policy.WhenClauses, 1)

		}},
		{name: "TestParseDDL_OnAccess_QueryContextVarsInExpression", query: `CREATE PROMOTION POLICY ctx_vars FOR (n:Fact) APPLY {
		ON ACCESS {
			SET n.lastSessionId = $_session
			SET n.lastAgent = $_agent
			WITH KALMAN SET n.confidence = CASE WHEN n._lastSessionId <> $_session THEN $evaluatedConfidence ELSE n.confidence END
		}
	}`, stage: RawSource, check: func(t *testing.T, c *CreatePromotionPolicyCmd) {
			require.NotNil(t, c.Policy.OnAccess)
			require.Len(t, c.Policy.OnAccess.Mutations, 3)

			assert.Contains(t, c.Policy.OnAccess.Mutations[0].Expression, "$_session")
			assert.Nil(t, c.Policy.OnAccess.Mutations[0].Kalman)

			assert.Contains(t, c.Policy.OnAccess.Mutations[1].Expression, "$_agent")
			assert.Nil(t, c.Policy.OnAccess.Mutations[1].Kalman)

			assert.Contains(t, c.Policy.OnAccess.Mutations[2].Expression, "$_session")
			require.NotNil(t, c.Policy.OnAccess.Mutations[2].Kalman)
			assert.Equal(t, knowledgepolicy.KalmanModeAuto, c.Policy.OnAccess.Mutations[2].Kalman.Mode)

		}},
		{name: "TestPlanDDL_SessionRecordTiering", query: `CREATE PROMOTION POLICY session_record_tiering
FOR (n:SessionRecord)
APPLY {
  ON ACCESS {
    SET n.accessCount = coalesce(n.accessCount, 0) + 1
    SET n.lastAccessedAt = timestamp()
  }

  WHEN n.accessCount >= 3
    APPLY PROFILE 'reinforced_tier'

  WHEN n.accessCount >= 5 AND n.sourceAgreement >= 0.95
    APPLY PROFILE 'canonical_tier'
}`, stage: RawSource, check: func(t *testing.T, c *CreatePromotionPolicyCmd) {
			assert.Equal(t, "session_record_tiering", c.Policy.Name)
			assert.Equal(t, []string{"SessionRecord"}, c.Policy.TargetLabels)
			assert.False(t, c.Policy.IsEdge)

			require.NotNil(t, c.Policy.OnAccess)
			require.Len(t, c.Policy.OnAccess.Mutations, 2)
			assert.Equal(t, "n.accessCount = coalesce(n.accessCount, 0) + 1", c.Policy.OnAccess.Mutations[0].Expression)
			assert.Equal(t, "n.lastAccessedAt = timestamp()", c.Policy.OnAccess.Mutations[1].Expression)

			require.Len(t, c.Policy.WhenClauses, 2)
			assert.Equal(t, "n.accessCount >= 3", c.Policy.WhenClauses[0].Predicate)
			assert.Equal(t, "reinforced_tier", c.Policy.WhenClauses[0].ProfileRef)
			assert.Contains(t, c.Policy.WhenClauses[1].Predicate, "n.accessCount >= 5")
			assert.Equal(t, "canonical_tier", c.Policy.WhenClauses[1].ProfileRef)

		}},
		{name: "TestPlanDDL_EpisodicRecallQuality", query: `CREATE PROMOTION POLICY episodic_recall_quality
FOR (n:MemoryEpisode)
APPLY {
  ON ACCESS {
    SET n.accessCount = coalesce(n.accessCount, 0) + 1
    SET n.lastAccessedAt = timestamp()
    WITH KALMAN{q: 0.05, r: 50.0} SET n.confidenceScore = $evaluatedConfidence
    WITH KALMAN SET n.crossSessionAccessRate =
      CASE WHEN n._lastSessionId <> $_session
        THEN coalesce(n.crossSessionAccessRate, 0) + 1
        ELSE n.crossSessionAccessRate
      END
    SET n._lastSessionId = $_session
    SET n._lastAgentId = $_agent
  }

  WHEN n.accessCount >= 5 AND n.confidenceScore >= 0.8
    APPLY PROFILE 'high_confidence_tier'

  WHEN n.accessCount >= 3
    APPLY PROFILE 'reinforced_tier'
}`, stage: RawSource, check: func(t *testing.T, c *CreatePromotionPolicyCmd) {
			assert.Equal(t, "episodic_recall_quality", c.Policy.Name)
			assert.Equal(t, []string{"MemoryEpisode"}, c.Policy.TargetLabels)

			require.NotNil(t, c.Policy.OnAccess)
			require.Len(t, c.Policy.OnAccess.Mutations, 6)

			assert.Nil(t, c.Policy.OnAccess.Mutations[0].Kalman)
			assert.Nil(t, c.Policy.OnAccess.Mutations[1].Kalman)

			require.NotNil(t, c.Policy.OnAccess.Mutations[2].Kalman)
			assert.Equal(t, knowledgepolicy.KalmanModeManual, c.Policy.OnAccess.Mutations[2].Kalman.Mode)
			assert.Equal(t, 0.05, c.Policy.OnAccess.Mutations[2].Kalman.Q)
			assert.Equal(t, 50.0, c.Policy.OnAccess.Mutations[2].Kalman.R)
			assert.Contains(t, c.Policy.OnAccess.Mutations[2].Expression, "$evaluatedConfidence")

			require.NotNil(t, c.Policy.OnAccess.Mutations[3].Kalman)
			assert.Equal(t, knowledgepolicy.KalmanModeAuto, c.Policy.OnAccess.Mutations[3].Kalman.Mode)

			assert.Contains(t, c.Policy.OnAccess.Mutations[4].Expression, "$_session")
			assert.Nil(t, c.Policy.OnAccess.Mutations[4].Kalman)

			assert.Contains(t, c.Policy.OnAccess.Mutations[5].Expression, "$_agent")
			assert.Nil(t, c.Policy.OnAccess.Mutations[5].Kalman)

			require.Len(t, c.Policy.WhenClauses, 2)
			assert.Equal(t, "high_confidence_tier", c.Policy.WhenClauses[0].ProfileRef)
			assert.Equal(t, "reinforced_tier", c.Policy.WhenClauses[1].ProfileRef)

		}},
	})
}

func TestKnowledgePolicyDDLProducts_AlterPromotionPolicyCmd(t *testing.T) {
	runKPProductCases(t, []kpDDLProductCase[*AlterPromotionPolicyCmd]{
		{name: "TestParseDDL_AlterPromotionPolicy_Enable", query: `ALTER PROMOTION POLICY fact_promo ENABLE`, stage: RawSource, check: func(t *testing.T, c *AlterPromotionPolicyCmd) {
			assert.Equal(t, "fact_promo", c.Name)
			assert.Equal(t, true, c.Updates["enabled"])

		}},
		{name: "TestParseDDL_AlterPromotionPolicy_Disable", query: `ALTER PROMOTION POLICY fact_promo DISABLE`, stage: RawSource, check: func(t *testing.T, c *AlterPromotionPolicyCmd) {
			assert.Equal(t, "fact_promo", c.Name)
			assert.Equal(t, false, c.Updates["enabled"])

		}},
		{name: "TestParseDDL_AlterPromotionPolicy_MissingName", query: `ALTER PROMOTION POLICY ENABLE`, stage: RawSource, check: func(t *testing.T, c *AlterPromotionPolicyCmd) {
			assert.Equal(t, "ENABLE", c.Name)

		}},
	})
}

func TestKnowledgePolicyDDLProducts_AlterPromotionPolicyDefinitionCmd(t *testing.T) {
	runKPProductCases(t, []kpDDLProductCase[*AlterPromotionPolicyDefinitionCmd]{
		{name: "TestParseDDL_AlterPromotionPolicyDefinition", query: `ALTER PROMOTION POLICY fact_promo FOR ()-[r:REFERENCES]-() APPLY {
		ON ACCESS { SET r.accessCount = coalesce(r.accessCount, 0) + 1 }
		WHEN r.accessCount >= 5 APPLY PROFILE 'boost'
	}`, stage: RawSource, check: func(t *testing.T, c *AlterPromotionPolicyDefinitionCmd) {
			assert.Equal(t, "fact_promo", c.Policy.Name)
			assert.True(t, c.Policy.IsEdge)
			assert.Equal(t, "REFERENCES", c.Policy.TargetEdgeType)
			require.NotNil(t, c.Policy.OnAccess)
			require.Len(t, c.Policy.OnAccess.Mutations, 1)
			require.Len(t, c.Policy.WhenClauses, 1)
			assert.Equal(t, "boost", c.Policy.WhenClauses[0].ProfileRef)

		}},
		{name: "TestParseDDL_AlterPromotionPolicyTargetOnly", query: `ALTER PROMOTION POLICY fact_promo FOR (n:KnowledgeFact)`, stage: RawSource, check: func(t *testing.T, c *AlterPromotionPolicyDefinitionCmd) {
			assert.Equal(t, "fact_promo", c.Policy.Name)
			assert.Equal(t, []string{"KnowledgeFact"}, c.Policy.TargetLabels)
			assert.Nil(t, c.Policy.OnAccess)
			assert.Empty(t, c.Policy.WhenClauses)

		}},
	})
}

func TestKnowledgePolicyDDLProducts_DropPromotionPolicyCmd(t *testing.T) {
	runKPProductCases(t, []kpDDLProductCase[*DropPromotionPolicyCmd]{
		{name: "TestParseDDL_DropPromotionPolicy", query: `DROP PROMOTION POLICY fact_promo`, stage: RawSource, check: func(t *testing.T, c *DropPromotionPolicyCmd) {
			assert.Equal(t, "fact_promo", c.Name)
			assert.False(t, c.IfExists)

		}},
		{name: "TestParseDDL_DropPromotionPolicy_IfExists", query: `DROP PROMOTION POLICY IF EXISTS fact_promo`, stage: RawSource, check: func(t *testing.T, c *DropPromotionPolicyCmd) {
			assert.Equal(t, "fact_promo", c.Name)
			assert.True(t, c.IfExists)

		}},
		{name: "TestPlanDDL_DropPromotionPolicy", query: `DROP PROMOTION POLICY session_record_tiering;`, stage: RawSource, check: func(t *testing.T, c *DropPromotionPolicyCmd) {
			assert.Equal(t, "session_record_tiering", c.Name)

		}},
	})
}

func TestKnowledgePolicyDDLProducts_ShowPromotionPoliciesCmd(t *testing.T) {
	runKPProductCases(t, []kpDDLProductCase[*ShowPromotionPoliciesCmd]{
		{name: "TestParseDDL_ShowPromotionPolicies", query: `SHOW PROMOTION POLICIES`, stage: RawSource, check: func(t *testing.T, c *ShowPromotionPoliciesCmd) {

		}},
		{name: "TestPlanDDL_ShowPromotionPolicies", query: `SHOW PROMOTION POLICIES;`, stage: RawSource, check: func(t *testing.T, c *ShowPromotionPoliciesCmd) {

		}},
	})
}
