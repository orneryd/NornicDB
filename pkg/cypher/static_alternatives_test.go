package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// staticAlternativeInputs are values of several possible types, of each
// origin: an operator signature's result (n.k * 1.5), CASE unions, and a
// union holding a temporal value.
func staticAlternativeInputs() map[string]staticOperand {
	return map[string]staticOperand{
		"Float or Duration":          signatureOperand([]string{"Float", "Duration"}),
		"Integer or String":          typedOperand("Integer or String").proved(),
		"Boolean or Integer":         typedOperand("Boolean or Integer").proved(),
		"Date or Integer":            typedOperand("Date or Integer").proved(),
		"Duration or String":         typedOperand("Duration or String").proved(),
		"Float, Integer or Duration": signatureOperand([]string{"Float", "Integer", "Duration"}),
	}
}

// staticValueConstructors give back one of v's values: every alternative of
// v must survive them. staticListConstructors build a list of v's values:
// List<X> must survive for every alternative X.
var (
	staticValueConstructors = []string{
		"v", "coalesce(v)", "coalesce(v, v)", "head([v])", "last([v])", "[v][0]",
		"[x IN [v] | x][0]", "head([x IN [v] | x])", "head([x IN [v] WHERE true])", "head(collect(v))",
		"min(v)", "max(v)", "head(tail([v]))", "head(reverse([v]))", "[v][0..1][0]",
		"CASE WHEN true THEN v END", "CASE WHEN true THEN v ELSE v END", "head([[v]][0])",
	}
	staticListConstructors = []string{"[v]", "collect(v)", "[x IN [v] | x]", "[x IN [1] | v]", "tail([v])", "reverse([v])", "[v][0..1]"}
	// staticCollapsingConstructors are Neo4j's own collapses, each with its
	// reason; TestStaticAlternativesNeverCollapse checks they are the
	// documented ones.
	staticCollapsingConstructors = map[string]string{
		"[v, v]": "Neo4j types a list literal of several elements by the least upper bound of each pair of their alternatives (rule R8); a pair bounded by Any leaves the elements unknown",
	}
)

// staticAlternativesOf is the set of alternatives an operand's type names.
func staticAlternativesOf(operand staticOperand) map[string]bool {
	set := map[string]bool{}
	for _, choice := range staticTypeChoices(operand.typeName()) {
		set[choice] = true
	}
	return set
}

// No constructor or derived function collapses a value of several types
// into fewer: each alternative of the input is an alternative of the
// result (of its elements, for a list), whatever produced the input. Every
// function of staticFunctionDerivations is exercised here.
func TestStaticAlternativesNeverCollapse(t *testing.T) {
	for name, input := range staticAlternativeInputs() {
		scope := typeScope{"v": input}.static(false)
		checker := staticOperatorChecker{scope: scope}
		for _, expression := range staticValueConstructors {
			result, err := checker.check(expression)
			require.NoError(t, err, "%s for v of %s", expression, name)
			got := staticAlternativesOf(result)
			for _, choice := range staticTypeChoices(input.typeName()) {
				require.True(t, got[choice], "%s for v of %s lost %s: %q", expression, name, choice, result.typeName())
			}
		}
		for _, expression := range staticListConstructors {
			result, err := checker.check(expression)
			require.NoError(t, err, "%s for v of %s", expression, name)
			got := staticAlternativesOf(result)
			for _, choice := range staticTypeChoices(input.typeName()) {
				require.True(t, got["List<"+choice+">"], "%s for v of %s lost List<%s>: %q", expression, name, choice, result.typeName())
			}
		}
	}
	for expression := range staticCollapsingConstructors {
		result, err := (staticOperatorChecker{scope: typeScope{"v": staticAlternativeInputs()["Integer or String"]}.static(false)}).check(expression)
		require.NoError(t, err, expression)
		require.Equal(t, "List<T>", result.typeName(), "%s is a documented collapse", expression)
	}
	for function := range staticFunctionDerivations {
		exercised := false
		for _, expression := range append(append([]string(nil), staticValueConstructors...), staticListConstructors...) {
			exercised = exercised || strings.Contains(expression, function+"(")
		}
		require.True(t, exercised, "staticFunctionDerivations[%q] has no case in TestStaticAlternativesNeverCollapse", function)
	}
}

// The one place a union's list alternatives lose their element types is
// Neo4j's own: a CASE whose branches mix a temporal value or duration with a
// list of other values ("Duration or List<T>").
func TestStaticCaseTemporalListWidening(t *testing.T) {
	checker := staticOperatorChecker{scope: typeScope{}.static(false)}
	for expression, want := range map[string]string{
		"CASE WHEN true THEN duration('PT1S') ELSE [1] END":        "Duration or List<T>",
		"CASE WHEN true THEN [duration('PT1S')] ELSE [1] END":      "List<T>",
		"CASE WHEN true THEN [duration('PT1S')] ELSE [date()] END": "List<Duration> or List<Date>",
		"[CASE WHEN true THEN 1.5 ELSE duration('PT1S') END]":      "List<Float> or List<Duration>",
	} {
		result, err := checker.check(expression)
		require.NoError(t, err, expression)
		require.Equal(t, want, result.typeName(), expression)
	}
}

// Round-5 review: an operator's result keeps its alternatives through list
// construction to every consumer (Neo4j 5.26.30 rejects each: n.k * 1.5 is a
// Float or a Duration), in auto-commit and in an explicit transaction.
func TestOperatorResultAlternativesReachConsumers(t *testing.T) {
	ctx := context.Background()
	for _, query := range []string{
		"UNWIND [] AS r MATCH (n) WITH n, head([n.k * 1.5]) AS v RETURN toUpper(v) AS a",
		"UNWIND [] AS r MATCH (n) WITH n, head([n.k * 1.5]) AS v SET n += v RETURN 1 AS a",
		"UNWIND [] AS r MATCH (n) WITH n, head([n.k * 1.5]) AS v RETURN [x IN [v] | toUpper(x)] AS a",
		"UNWIND [] AS r MATCH (n) WITH n, last([n.k * 1.5]) AS v RETURN toUpper(v) AS a",
		"UNWIND [] AS r MATCH (n) WITH n, last([n.k * 1.5]) AS v SET n += v RETURN 1 AS a",
		"UNWIND [] AS r MATCH (n) WITH n, last([n.k * 1.5]) AS v RETURN [x IN [v] | toUpper(x)] AS a",
		"UNWIND [] AS r MATCH (n) WITH n, coalesce(n.k * 1.5) AS v RETURN [x IN [v] | toUpper(x)] AS a",
		"UNWIND [] AS r MATCH (n) WITH n, [x IN [n.k * 1.5] | x][0] AS v RETURN toUpper(v) AS a",
		"UNWIND [] AS r MATCH (n) WITH n, [x IN [n.k * 1.5] | x][0] AS v SET n += v RETURN 1 AS a",
		"UNWIND [] AS r MATCH (n) WITH n, [x IN [n.k * 1.5] | x][0] AS v RETURN [x IN [v] | toUpper(x)] AS a",
		"UNWIND [] AS r MATCH (n) WITH n, CASE WHEN true THEN n.k * 1.5 END AS v RETURN [x IN [v] | toUpper(x)] AS a",
	} {
		for _, mode := range []string{"auto", "tx"} {
			t.Run(mode+"/"+query, func(t *testing.T) {
				exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "operator_alternatives"))
				if mode == "tx" {
					_, err := exec.Execute(ctx, "BEGIN", nil)
					require.NoError(t, err)
					defer func() { _, _ = exec.Execute(ctx, "ROLLBACK", nil) }()
				}
				_, err := exec.Execute(ctx, query, nil)
				requireStatusCode(t, err, "Neo.ClientError.Statement.SyntaxError")
			})
		}
	}
}
