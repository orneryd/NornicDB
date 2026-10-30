package cypher

import (
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func oldDistinctArgumentContract(text string) (string, bool) {
	if strings.EqualFold(strings.TrimSpace(text), "DISTINCT") {
		return "", true
	}
	body, distinct := cutDistinct(originalBraceView(text))
	return body.text(), distinct
}

// distinctAfter adapts the view-taking DISTINCT-as-name reader to the lexical
// producer's rest callback, as braceView.argumentSource does.
func distinctAfter(input string) func(string) bool {
	view := acceptedBraceView(input)
	end := len(strings.TrimRightFunc(input, cyphertext.IsWhitespace))
	return func(rest string) bool { return distinctFollowsAsName(view.window(end-len(rest), end)) }
}
func TestFunctionArgumentSourceDistinctConsumerParity(t *testing.T) {
	bodies := []string{"", "ALL", "all", "aLl", "DISTINCT", "distinct", "DiStInCt", "DISTINCT all", "DISTINCT x", "DISTINCT x.y", "DISTINCT `all`", "DISTINCT(x)", "DISTINCT{a:1}", "DISTINCT .5", "DISTINCT +1", "DISTINCT -1", "DISTINCT [1]", "distinct.x", "distinct[0]", "distinct, x", "distinct AS x", "distinct AND x", "distinct OR x", "distinct IS NULL", "distinct:Label", "distinct / 2", "distinct % 2", "distinct * x", "distinct{.x}", "distinct ORDER BY x", "distinctName", "ALL.x", "all+1", "all[0]", "(all)", "`ALL`", "'ALL'", "\"ALL\"", "all, x", "日本", "DİSTINCT", "ALL日本"}
	checked := 0
	var deltas []string
	for _, head := range []string{"", " ", "/*head*/", " //head\n"} {
		for _, tail := range []string{"", " ", "/*tail*/", " //tail\n"} {
			for _, body := range bodies {
				input := head + body + tail
				want, distinct := oldDistinctArgumentContract(input)
				got := cyphertext.ReadFunctionArgumentSource(input, cyphertext.DistinctArgumentModifier, distinctAfter(input))
				value := input[got.Arguments.Start:got.Arguments.End]
				flag := got.Modifier == cyphertext.DistinctArgumentModifier
				if value != want || flag != distinct {
					require.True(t, strings.EqualFold(strings.TrimSpace(body), "DISTINCT") && head+tail != "", input)
					require.Empty(t, value, input)
					require.True(t, flag, input)
					deltas = append(deltas, input)
				}
				checked++
			}
		}
	}
	t.Logf("checked=%d explicitcomment-lone-DISTINCT corrections=%d", checked, len(deltas))
}
func TestFunctionArgumentSourceMasksAndOffsets(t *testing.T) {
	for _, tc := range []struct {
		input, body string
		kind        cyphertext.FunctionArgumentModifier
	}{{" ALL ", "", cyphertext.AllArgumentModifier}, {"/*h*/aLl/*t*/", "", cyphertext.AllArgumentModifier}, {" DISTINCT all ", "all", cyphertext.DistinctArgumentModifier}, {"DISTINCT `all`", "`all`", cyphertext.DistinctArgumentModifier}, {"ALL.x", "ALL.x", cyphertext.NoArgumentModifier}, {"all+1", "all+1", cyphertext.NoArgumentModifier}, {"all,x", "all,x", cyphertext.NoArgumentModifier}, {"`ALL`", "`ALL`", cyphertext.NoArgumentModifier}, {"'ALL'", "'ALL'", cyphertext.NoArgumentModifier}, {"ALL日本", "ALL日本", cyphertext.NoArgumentModifier}, {"/*unfinished", "/*unfinished", cyphertext.NoArgumentModifier}} {
		got := cyphertext.ReadFunctionArgumentSource(tc.input, cyphertext.AllArgumentModifier|cyphertext.DistinctArgumentModifier, distinctAfter(tc.input))
		require.Equal(t, tc.kind, got.Modifier, tc.input)
		require.Equal(t, tc.body, tc.input[got.Arguments.Start:got.Arguments.End], tc.input)
		if got.Modifier != cyphertext.NoArgumentModifier {
			require.True(t, equalFoldASCII(tc.input[got.ModifierSpan.Start:got.ModifierSpan.End], map[cyphertext.FunctionArgumentModifier]string{cyphertext.AllArgumentModifier: "ALL", cyphertext.DistinctArgumentModifier: "DISTINCT"}[tc.kind]))
		}
	}
	for _, input := range []string{"ALL", "DISTINCT all", "DISTINCT `a``b`", "/*h*/all/*t*/"} {
		require.Zero(t, testing.AllocsPerRun(100, func() {
			_ = cyphertext.ReadFunctionArgumentSource(input, cyphertext.AllArgumentModifier|cyphertext.DistinctArgumentModifier, distinctAfter(input))
		}), input)
	}
}

func TestDistinctOnlyArgumentConsumerProducts(t *testing.T) {
	for _, tc := range []struct {
		input, body, variable, property string
		distinct                        bool
	}{{"SUM(DISTINCT all)", "all", "all", "", true}, {"SUM(all)", "all", "all", "", false}, {"SUM(DISTINCT x)", "x", "x", "", true}, {"SUM(distinct.x)", "distinct.x", "distinct", "x", false}, {"SUM(DISTINCT x.y)", "x.y", "x", "y", true}} {
		t.Run(tc.input, func(t *testing.T) {
			aggregate := ParseAggregation(tc.input)
			require.NotNil(t, aggregate)
			require.Equal(t, tc.variable, aggregate.Variable)
			require.Equal(t, tc.property, aggregate.Property)
			require.Equal(t, tc.distinct, aggregate.Distinct)
			_, inner, distinct, ok := parsePipelineAggregate(originalBraceView(tc.input))
			require.True(t, ok)
			require.Equal(t, tc.body, inner)
			require.Equal(t, tc.distinct, distinct)
		})
	}
}
func TestASTFunctionArgumentOriginalExtent(t *testing.T) {
	for _, source := range []string{"WITH 7 AS all RETURN sum(DISTINCT all ) AS k", "WITH 7 AS all RETURN sum( DISTINCT all  ) AS k"} {
		ast, err := NewASTBuilder().Build(source)
		require.NoError(t, err)
		require.Len(t, ast.Clauses, 2)
		require.NotNil(t, ast.Clauses[1].Return)
		call := ast.Clauses[1].Return.Items[0].Expression.Function
		require.NotNil(t, call)
		require.True(t, call.Distinct)
		require.Len(t, call.Arguments, 1)
		require.Equal(t, "all", call.Arguments[0].Variable)
	}
}
