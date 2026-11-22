// Close-out lane L8 (v1.5.2 verification campaign, 2026-11-22): owner runs
// on the pin for the leads in zz_closeout_l8_public_test.go. Each test calls
// the function named as the owner with the lead's input and logs the
// decisive value (L8_OWNER lines), next to a control input that takes the
// right path; where a package variable is the seam, the test corrects it
// test-side and shows the public failure disappear. Test-only, not for merge.
package cypher

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"runtime/pprof"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/pprof/profile"
	"github.com/orneryd/nornicdb/pkg/config"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
)

func l8Owner(t *testing.T, lead, step string, fields map[string]any) {
	t.Helper()
	fields["lead"], fields["step"], fields["parser"] = lead, step, config.GetParserType()
	data, _ := json.Marshal(fields)
	t.Logf("L8_OWNER %s", data)
}

func l8ErrFields(err error) map[string]any {
	if err == nil {
		return map[string]any{"err": nil}
	}
	code, _ := nornicerrors.Neo4jStatus(err)
	return map[string]any{"err": err.Error(), "code": code}
}

// L-H4-007: patternHasRelationship decides whether the CREATE validator
// checks the relationship's direction.
func TestL8Owner_H4_007(t *testing.T) {
	for _, pattern := range []string{"()--()", "(a)--(b)", "()-->()", "()<--()", "()-[:LINK]-()"} {
		fields := l8ErrFields(validateRelationshipMutation(pattern, createRelationshipMutation))
		fields["pattern"], fields["patternHasRelationship"] = pattern, patternHasRelationship(pattern)
		l8Owner(t, "L-H4-007", "patternHasRelationship+validateRelationshipMutation", fields)
	}
	if patternHasRelationship("()--()") {
		t.Errorf("patternHasRelationship(()--()) = true: the owner is elsewhere")
	}
	if validateRelationshipMutation("()--()", createRelationshipMutation) == nil {
		t.Errorf("validateRelationshipMutation accepts ()--(): the owner is elsewhere")
	}
}

// L-H4-008: applyTruncationFields writes the override into time.Date, which
// normalises an out-of-range day or nanosecond instead of refusing it.
func TestL8Owner_H4_008(t *testing.T) {
	base := time.Date(1984, 10, 11, 12, 31, 14, 645876123, time.UTC)
	ldt := CypherLocalDateTime{Time: base}
	for _, c := range []struct {
		kind, unit string
		fields     map[string]interface{}
	}{
		{"date", "month", map[string]interface{}{"day": int64(-2)}},
		{"datetime", "day", map[string]interface{}{"nanosecond": int64(-2)}},
		{"date", "month", map[string]interface{}{"day": int64(5)}},
	} {
		value, ok, err := truncateTemporalValue(c.kind, c.unit, ldt, c.fields)
		fields := l8ErrFields(err)
		fields["kind"], fields["unit"], fields["fields"], fields["value"], fields["ok"] = c.kind, c.unit, fmt.Sprint(c.fields), fmt.Sprint(value), ok
		truncated, _ := truncateTimeAtUnit(base, c.unit)
		fields["applyTruncationFields"] = applyTruncationFields(truncated, c.fields).Format(time.RFC3339Nano)
		l8Owner(t, "L-H4-008", "truncateTemporalValue", fields)
	}
}

// L-H4-011: validateMatchWhereSimpleOperands reports a literal left operand
// of IN as an undefined variable.
func TestL8Owner_H4_011(t *testing.T) {
	exec := l8Exec(t)
	scope := matchSemanticScope{"n": matchBindingNode, "m": matchBindingNode, "r": matchBindingRelationship}
	for _, where := range []string{"false IN [1, 2, 'a', null]", "true IN [1, 2]", "null IN [1]", "1 IN [1, 2, 'a', null]", "n.x IN [1]", "false = n.x", "true STARTS WITH 'a'"} {
		fields := l8ErrFields(exec.validateMatchWhereSimpleOperands(scope, where))
		fields["where"] = where
		l8Owner(t, "L-H4-011", "validateMatchWhereSimpleOperands", fields)
	}
}

// L-H4-012: abs's static result type "Float or Integer" is one type name
// that is neither numeric nor a list, so + takes the List<T> branch. The seam
// (staticFunctionResultTypes) is corrected test-side to show the failure go.
func TestL8Owner_H4_012(t *testing.T) {
	ctx := context.Background()
	for _, function := range []string{"abs", "sign"} {
		operand := provenOperand(staticFunctionResultType(function, "1"))
		l8Owner(t, "L-H4-012", "staticFunctionResultType", map[string]any{"function": function, "typeName": operand.typeName(), "known": operand.known(), "numeric": operand.numeric(), "list": operand.list()})
	}
	for _, expression := range []string{"abs(1) + 1", "abs(1) + 'a'", "sign(1) + 1", "'a' + abs(1)"} {
		checker := staticOperatorChecker{checked: make(map[string]staticCheckResult)}
		result, err := checker.check(ctx, expression)
		fields := l8ErrFields(err)
		fields["expression"], fields["result"] = expression, result.typeName()
		l8Owner(t, "L-H4-012", "staticOperatorChecker.check", fields)
	}
	previous := staticFunctionResultTypes["abs"]
	staticFunctionResultTypes["abs"] = "Integer"
	defer func() { staticFunctionResultTypes["abs"] = previous }()
	forEachParser(t, func(t *testing.T) {
		exec := l8Exec(t)
		o := l8Run(t, exec, "RETURN abs(2) + 2 AS seam", nil)
		l8Owner(t, "L-H4-012", "seam abs=Integer", map[string]any{"outcome": o})
		o2 := l8Run(t, exec, "RETURN [abs(toInteger(-0.0)) + 'a', 'ab'] AS seam2", nil)
		l8Owner(t, "L-H4-012", "seam abs=Integer", map[string]any{"outcome": o2})
	})
}

// L-H4-014: which evaluator reports "/ by zero" for null % 0.
func TestL8Owner_H4_014(t *testing.T) {
	for _, c := range []struct {
		left, right interface{}
	}{{nil, int64(0)}, {int64(5), int64(0)}, {1.5, int64(0)}} {
		l8Owner(t, "L-H4-014", "divisionByZero", map[string]any{"left": fmt.Sprint(c.left), "right": fmt.Sprint(c.right), "divisionByZero": divisionByZero('%', c.left, c.right)})
	}
	exec := l8Exec(t)
	for _, expression := range []string{"null % 0", "x % 0", "toInteger(x) % 0", "1 - x % 0"} {
		value, ok, err := exec.evaluateRowValue(context.Background(), expression, map[string]interface{}{"x": nil})
		fields := l8ErrFields(err)
		fields["expression"], fields["value"], fields["ok"] = expression, fmt.Sprint(value), ok
		l8Owner(t, "L-H4-014", "evaluateRowValue", fields)
	}
}

// L-H4-017: validatePercentileCalls checks the percentile of every row
// before (and whatever) the aggregated values are.
func TestL8Owner_H4_017(t *testing.T) {
	exec := l8Exec(t)
	for _, c := range []struct {
		expression string
		row        pipelineRow
	}{
		{"percentileDisc(null, deg)", pipelineRow{"deg": int64(3)}},
		{"percentileDisc(null, deg)", pipelineRow{"deg": 0.5}},
		{"percentileDisc(x, 3)", pipelineRow{"x": nil}},
	} {
		fields := l8ErrFields(exec.validatePercentileCalls(context.Background(), c.expression, c.row))
		fields["expression"], fields["row"] = c.expression, fmt.Sprint(c.row)
		l8Owner(t, "L-H4-017", "validatePercentileCalls", fields)
	}
}

// L-H4-018: the error each member's owner returns for the member's input.
func TestL8Owner_H4_018(t *testing.T) {
	ctx := context.Background()
	exec := l8Exec(t)
	for _, p := range []paginationExpression{{"LIMIT", "-null"}, {"LIMIT", "toInteger(ceil(null))"}, {"SKIP", "-2"}, {"SKIP", "-null"}} {
		fields := l8ErrFields(exec.validateStaticPaginationExpressions(ctx, p))
		fields["keyword"], fields["value"] = p.keyword, p.value
		l8Owner(t, "L-H4-018", "validateStaticPaginationExpressions", fields)
	}
	for _, arguments := range [][]interface{}{{int64(2), nil, int64(0)}, {nil, int64(8), int64(0)}, {int64(2), int64(5), int64(0)}, {int64(2), nil, int64(1)}} {
		_, err := newCypherRange(arguments)
		fields := l8ErrFields(err)
		fields["arguments"] = fmt.Sprint(arguments)
		l8Owner(t, "L-H4-018", "newCypherRange", fields)
	}
	for _, inner := range []string{"x IN ['Clara', 'Bob', null, 'Alice'] WHERE x % 2 = 0", "x IN [false, null] WHERE x % 2 = 0", "x IN [1, 2] WHERE x % 2 = 0"} {
		fields := l8ErrFields(validateStaticQuantifierCall(ctx, inner))
		fields["inner"] = inner
		l8Owner(t, "L-H4-018", "validateStaticQuantifierCall", fields)
	}
	for _, c := range []struct {
		function string
		input    map[string]interface{}
	}{
		{"date", map[string]interface{}{"date": nil, "week": int64(2), "dayOfWeek": int64(3)}},
		{"localdatetime", map[string]interface{}{"date": nil, "week": int64(2)}},
		{"date", map[string]interface{}{"year": int64(1984), "month": int64(13)}},
	} {
		fields := l8ErrFields(temporalConstructorError(c.function, c.input))
		fields["function"], fields["input"] = c.function, fmt.Sprint(c.input)
		l8Owner(t, "L-H4-018", "temporalConstructorError", fields)
	}
}

// L-H2-010: the comprehension and quantifier loops do not observe a canceled
// context; reduce (the fix's form) does. Called directly with a context that
// is already canceled.
func TestL8Owner_H2_010(t *testing.T) {
	exec := l8Exec(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	values := map[string]interface{}{"items": l8Items(50000)}
	for _, expression := range []string{
		"[x IN $items | size(randomUUID())]",
		"[x IN $items WHERE size(randomUUID()) > 0]",
		"any(x IN $items WHERE size(randomUUID()) < 0)",
		"all(x IN $items WHERE size(randomUUID()) > 0)",
		"reduce(a = 0, x IN $items | a + size(randomUUID()))",
		"size(randomUUID())",
	} {
		bound := map[string]interface{}{}
		for k, v := range values {
			bound[k] = v
		}
		params := withParams(ctx, values)
		start := time.Now()
		value, ok, err := exec.evaluateRowValue(params, expression, bound)
		elapsed := time.Since(start)
		fields := l8ErrFields(err)
		size := -1
		if list, isList := value.([]interface{}); isList {
			size = len(list)
		}
		fields["expression"], fields["ok"], fields["result_list_len"], fields["elapsed_ms"] = expression, ok, size, elapsed.Milliseconds()
		if size < 0 {
			fields["value"] = fmt.Sprint(value)
		}
		l8Owner(t, "L-H2-010", "evaluateRowValue(canceled ctx)", fields)
	}
}

// L-H2-011: the physical hint the terminal RETURN gives the traversal.
func TestL8Owner_H2_011(t *testing.T) {
	exec := l8Exec(t)
	ctx := context.Background()
	for _, clauses := range [][]pipelineClause{
		{{kind: pipelineClauseReturn, text: "RETURN p.id SKIP 1 LIMIT 1"}},
		{{kind: pipelineClauseReturn, text: "RETURN p.id SKIP 0 LIMIT 1"}},
		{{kind: pipelineClauseReturn, text: "RETURN p.id LIMIT 1"}},
		{{kind: pipelineClauseWith, text: "WITH p SKIP 1 LIMIT 1"}, {kind: pipelineClauseReturn, text: "RETURN p.id"}},
	} {
		hint := exec.pipelineMatchHint(ctx, clauses)
		texts := []string{}
		for _, c := range clauses {
			texts = append(texts, c.text)
		}
		l8Owner(t, "L-H2-011", "pipelineMatchHint", map[string]any{"clauses": texts, "limit": hint.limit, "earlyLimit": hint.earlyLimit})
	}
}

// L-H2-013: where the time before the first cancellation check goes. CPU
// profile of the long statements run with an already-expired deadline.
func TestL8Owner_H2_013(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := l8Exec(t)
		or := "UNWIND [1] AS y WITH y WHERE " + strings.TrimSuffix(strings.Repeat("y = 2 OR ", 722), " OR ") + " RETURN y"
		plus := "RETURN " + strings.TrimSuffix(strings.Repeat("$s + ", 723), " + ") + " AS v"
		params := map[string]interface{}{"s": "secret"}
		for shape, q := range map[string]string{"or": or, "plus": plus} {
			var buf bytes.Buffer
			if err := pprof.StartCPUProfile(&buf); err != nil {
				t.Logf("profile: %v", err)
				continue
			}
			start := time.Now()
			runs := 0
			for time.Since(start) < 3*time.Second {
				expired, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
				_, _ = exec.Execute(expired, q, params)
				cancel()
				runs++
			}
			pprof.StopCPUProfile()
			l8ProfileTop(t, "L-H2-013", shape, runs, time.Since(start), buf.Bytes())
		}
	})
}

func l8ProfileTop(t *testing.T, lead, shape string, runs int, elapsed time.Duration, data []byte) {
	t.Helper()
	p, err := profile.Parse(bytes.NewReader(data))
	if err != nil {
		t.Logf("profile parse: %v", err)
		return
	}
	cumulative, flat := map[string]int64{}, map[string]int64{}
	var total int64
	for _, sample := range p.Sample {
		v := sample.Value[len(sample.Value)-1]
		total += v
		seen := map[string]bool{}
		for i, location := range sample.Location {
			for _, line := range location.Line {
				if line.Function == nil {
					continue
				}
				name := strings.TrimPrefix(line.Function.Name, "github.com/orneryd/nornicdb/")
				if i == 0 {
					flat[name] += v
				}
				if !seen[name] {
					seen[name] = true
					cumulative[name] += v
				}
			}
		}
	}
	top := func(m map[string]int64, k int) []string {
		type kv struct {
			k string
			v int64
		}
		list := []kv{}
		for name, v := range m {
			if strings.HasPrefix(name, "pkg/") {
				list = append(list, kv{name, v})
			}
		}
		sort.Slice(list, func(i, j int) bool { return list[i].v > list[j].v || list[i].v == list[j].v && list[i].k < list[j].k })
		out := []string{}
		for i := 0; i < len(list) && i < k; i++ {
			pct := 0.0
			if total > 0 {
				pct = 100 * float64(list[i].v) / float64(total)
			}
			out = append(out, fmt.Sprintf("%.1f%% %s", pct, list[i].k))
		}
		return out
	}
	l8Owner(t, lead, "cpu-profile expired-deadline "+shape, map[string]any{"runs": runs, "ms_per_run": float64(elapsed.Milliseconds()) / float64(max(runs, 1)), "cumulative_top": top(cumulative, 25), "flat_top": top(flat, 10)})
}

// L-H9-012: the declared result header of the CSV export procedures.
func TestL8Owner_H9_012(t *testing.T) {
	for _, name := range []string{"apoc.export.csv.all", "apoc.export.csv.query", "apoc.export.json.all"} {
		l8Owner(t, "L-H9-012", "legacyProcedureColumns", map[string]any{"procedure": name, "columns": legacyProcedureColumns(name)})
	}
}
