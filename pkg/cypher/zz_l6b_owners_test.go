package cypher

// Lane L6b (closeout, partial root causes of the issues list): runs at the
// owners that were only read in source. Each test calls the named owner
// directly with the lead's input and a control, and where possible a
// test-side corrected step.
//   C13 L-H4-001 compileRowPredicateLeaf            TestL6bIsNullPrecedenceOwner
//   C15 L-H4-002 temporalOptionalInt/temporalFloat  TestL6bTemporalNullFieldOwner
//   C16 L-H4-003 buildTemporalValue (time date reset), L-H9-005 ResolveNamedZoneLocalTime
//                                                   TestL6bTemporalZoneOffsetOwner
//   C14 L-H4-005 cypherRange.values                 TestL6bRangeStepDirectionOwner
//   C21 L-H4-012 abs result type in addition         TestL6bAbsAdditionTypeOwner
//   C22 L-H4-011 validateMatchWhereSimpleOperands   TestL6bWhereLiteralInOperandOwner
//   C12 L-H9-001 shouldHideSystemNodes/isSystemNode TestL6bInternalLabelHiddenOwner

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func TestL6bIsNullPrecedenceOwner(t *testing.T) {
	ctx := context.Background()
	for _, leaf := range []string{
		"n.x <> n.x IS NULL",
		"(n.x <> n.x) IS NULL",
		"n.x <> (n.x IS NULL)",
		"n.k < true IS NULL",
		"n.k < (true IS NULL)",
	} {
		node := compileRowPredicateLeaf(ctx, leaf)
		t.Logf("L6B_C13 compileRowPredicateLeaf(row_predicate_program.go:200, suffix test :232-237 before comparison :239) leaf=%q -> %T %+v", leaf, node, node)
	}
	bare := fmt.Sprintf("%T", compileRowPredicateLeaf(ctx, "n.x <> n.x IS NULL"))
	left := fmt.Sprintf("%T", compileRowPredicateLeaf(ctx, "(n.x <> n.x) IS NULL"))
	right := fmt.Sprintf("%T", compileRowPredicateLeaf(ctx, "n.x <> (n.x IS NULL)"))
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "l6bc13"))
	if _, err := exec.Execute(ctx, "CREATE (:B {x: 1, v: 1}), (:B {x: 2, v: 1})", nil); err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{
		"MATCH (n:B) WHERE n.x <> n.x IS NULL RETURN count(n) AS c",
		"MATCH (n:B) WHERE (n.x <> n.x) IS NULL RETURN count(n) AS c",
		"MATCH (n:B) WHERE n.x <> (n.x IS NULL) RETURN count(n) AS c",
		"MATCH (n:B) RETURN n.x <> n.x IS NULL AS projected ORDER BY projected LIMIT 1",
	} {
		res, err := exec.Execute(ctx, q, nil)
		var rows any
		if res != nil {
			rows = res.Rows
		}
		t.Logf("L6B_C13 EXEC %q -> %v err=%v", q, rows, err)
	}
	if bare == left && bare != right {
		t.Errorf("CAUSE C13: the bare leaf compiles to %s, the node of (a <> b) IS NULL, not the comparison of a with (b IS NULL) (%s)", bare, right)
	}
}

func TestL6bTemporalNullFieldOwner(t *testing.T) {
	nullWeek := map[string]interface{}{"year": int64(1817), "week": nil}
	oneWeek := map[string]interface{}{"year": int64(1817), "week": int64(1)}
	noWeek := map[string]interface{}{"year": int64(1817)}
	for _, tc := range []struct {
		name   string
		fields map[string]interface{}
	}{{"week:null", nullWeek}, {"week:1", oneWeek}, {"week absent", noWeek}} {
		v, present := temporalOptionalInt(tc.fields, "week")
		built, ok := buildTemporalValue("date", tc.fields)
		t.Logf("L6B_C15 temporalOptionalInt(temporal_values.go:892) %s -> value=%d present=%v | buildTemporalValue(date)=%v ok=%v", tc.name, v, present, built, ok)
	}
	dayNull := temporalFieldInt(map[string]interface{}{"day": nil}, "day", 7)
	dayOne := temporalFieldInt(map[string]interface{}{"day": int64(1)}, "day", 7)
	t.Logf("L6B_C15 temporalFieldInt(:883) day:null fallback 7 -> %d | day:1 -> %d", dayNull, dayOne)
	weeksNull := temporalFloat(map[string]interface{}{"weeks": nil}, "weeks")
	weeksOne := temporalFloat(map[string]interface{}{"weeks": int64(1)}, "weeks")
	durNull, _ := buildTemporalValue("duration", map[string]interface{}{"weeks": nil})
	t.Logf("L6B_C15 temporalFloat(:905) weeks:null -> %v | weeks:1 -> %v | buildDurationFromFields(weeks:null)=%v", weeksNull, weeksOne, durNull)
	vNull, presentNull := temporalOptionalInt(nullWeek, "week")
	builtNull, _ := buildTemporalValue("date", nullWeek)
	builtAbsent, _ := buildTemporalValue("date", noWeek)
	builtOne, _ := buildTemporalValue("date", oneWeek)
	t.Logf("L6B_C15 summary null==absent:%v null==week1(Neo4j's answer 1816-12-30):%v value=%d present=%v",
		fmt.Sprint(builtNull) == fmt.Sprint(builtAbsent), fmt.Sprint(builtNull) == fmt.Sprint(builtOne), vNull, presentNull)
	if !presentNull && fmt.Sprint(builtNull) == fmt.Sprint(builtAbsent) {
		t.Errorf("CAUSE C15: temporalOptionalInt reports a null map value as an absent key, so date({year:1817, week:null}) = %v (the absent-key value), not %v", builtNull, builtOne)
	}
	if weeksNull == 0 {
		t.Errorf("CAUSE C15: temporalFloat turns null into 0, so duration({weeks:null}) = %v instead of an error", durNull)
	}
}

func TestL6bTemporalZoneOffsetOwner(t *testing.T) {
	ctx := context.Background()
	zone, ok := loadTemporalLocationAt("Europe/Stockholm", time.Time{})
	if !ok {
		t.Fatal("zone")
	}
	at1970 := time.Date(1970, 1, 1, 12, 0, 42, 0, zone).Format("-07:00")
	atSource := time.Date(1984, 9, 11, 12, 0, 42, 0, zone).Format("-07:00")
	t.Logf("L6B_C16 L-H4-003 step temporal_values.go:564-565 resets the date to 1970-01-01 for kind time; offset of 12:00:42 Europe/Stockholm on 1970-01-01=%s, on the source date 1984-09-11=%s", at1970, atSource)
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "l6bc16"))
	res, err := exec.Execute(ctx, "RETURN datetime({year: 1984, month: 9, day: 11, hour: 12, timezone: 'Europe/Stockholm'}) AS d", nil)
	if err != nil || res == nil || len(res.Rows) == 0 {
		t.Fatalf("datetime: %v", err)
	}
	source := res.Rows[0][0]
	built, okBuilt := buildTemporalValue("time", map[string]interface{}{"time": source, "second": int64(42)})
	t.Logf("L6B_C16 buildTemporalValue(time, {time: %v, second: 42}) = %v ok=%v (Neo4j 12:00:42+02:00)", source, built, okBuilt)
	if at1970 == "+01:00" && atSource == "+02:00" && fmt.Sprint(built) != "" && !containsOffset(fmt.Sprint(built), "+02:00") {
		t.Errorf("CAUSE C16 L-H4-003: kind time resolves the zone offset at 1970-01-01 (%s), the source instant has %s; built %v", at1970, atSource, built)
	}

	ny, ok := loadTemporalLocationAt("America/New_York", time.Time{})
	if !ok {
		t.Fatal("ny")
	}
	local := time.Date(2024, 11, 3, 1, 30, 0, 0, time.UTC)
	later := ResolveNamedZoneLocalTime(local, ny, true)
	earlier := ResolveNamedZoneLocalTime(local, ny, false)
	mapBuilt, _ := buildTemporalValue("datetime", map[string]interface{}{"year": int64(2024), "month": int64(11), "day": int64(3), "hour": int64(1), "minute": int64(30), "timezone": "America/New_York"})
	t.Logf("L6B_C16 L-H9-005 ResolveNamedZoneLocalTime(temporal_parse.go:253) preferLater=true (map constructor, temporal_values.go:596) -> %s epoch=%d | preferLater=false (string form, temporal_parse.go:245) -> %s epoch=%d | buildTemporalValue(datetime map)=%v (Neo4j -04:00 epoch 1730611800)",
		later.Format(time.RFC3339), later.Unix(), earlier.Format(time.RFC3339), earlier.Unix(), mapBuilt)
	if later.Unix() == 1730615400 && earlier.Unix() == 1730611800 {
		t.Errorf("CAUSE C16 L-H9-005: preferLater=true picks the later overlap instant (-05:00); false gives Neo4j's -04:00")
	}
}

func containsOffset(s, offset string) bool {
	for i := 0; i+len(offset) <= len(s); i++ {
		if s[i:i+len(offset)] == offset {
			return true
		}
	}
	return false
}

func TestL6bRangeStepDirectionOwner(t *testing.T) {
	for _, args := range [][]interface{}{
		{int64(-1), int64(1), int64(-123)},
		{int64(0), int64(1), int64(-2)},
		{int64(1), int64(-1), int64(-1)},
		{int64(0), int64(5), int64(2)},
		{int64(5), int64(1), int64(1)},
	} {
		r, err := newCypherRange(args)
		var got []int64
		if err == nil {
			for v := range r.values() {
				got = append(got, v)
			}
		}
		t.Logf("L6B_C14 cypherRange.values(range_semantics.go:69-81, loop condition :71) range(%v,%v,%v) -> %v err=%v", args[0], args[1], args[2], got, err)
	}
	r, _ := newCypherRange([]interface{}{int64(-1), int64(1), int64(-123)})
	count := 0
	for range r.values() {
		count++
	}
	if count == 0 {
		t.Errorf("CAUSE C14: the loop condition is false at once when step < 0 and start < end, so range(-1, 1, -123) yields nothing (Neo4j: [-1])")
	}
}

func TestL6bAbsAdditionTypeOwner(t *testing.T) {
	ctx := context.Background()
	scope := staticTypeScope{}
	for _, fn := range []struct{ name, args string }{{"abs", "1"}, {"abs", "-1.5"}, {"sign", "1"}, {"toInteger", "-0.0"}} {
		t.Logf("L6B_C21 staticFunctionResultType(static_operator_types.go:80) %s(%s) = %q", fn.name, fn.args, staticFunctionResultType(fn.name, fn.args))
	}
	str := scope.operandOf(ctx, "'a'")
	for _, expr := range []string{"abs(1)", "abs(toInteger(-0.0))", "sign(1)", "toInteger(-0.0)"} {
		op := scope.operandOf(ctx, expr)
		sum, err := checkOperator('+', op, str, false)
		t.Logf("L6B_C21 operand %q typeName=%q numeric(static_operand.go:610)=%v additionResultTypes(:312, op, 'a')=%q checkOperator('+')=%q err=%v",
			expr, op.typeName(), op.numeric(), additionResultTypes(op, str), sum.typeName(), err)
	}
	abs := scope.operandOf(ctx, "abs(1)")
	integer := scope.operandOf(ctx, "toInteger(1)")
	_, absErr := checkOperator('+', abs, str, false)
	_, intErr := checkOperator('+', integer, str, false)
	if absErr != nil && intErr == nil && !abs.numeric() {
		t.Errorf("CAUSE C21: abs's result type %q is not numeric() to the checker, so addition falls through to List<T> (%v); an Integer operand is accepted", abs.typeName(), absErr)
	}
}

func TestL6bWhereLiteralInOperandOwner(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "l6bc22"))
	scope := matchSemanticScope{"n": matchBindingNode, "r": matchBindingRelationship, "m": matchBindingNode}
	results := map[string]error{}
	for _, where := range []string{
		"false IN [1, 2, 'a', null]",
		"true IN [1, 2]",
		"null IN [1, 2]",
		"1 IN [1, 2, 'a', null]",
		"n.id IN [1, 2, 'a', null]",
		"false STARTS WITH 'a'",
		"x IN [1, 2]",
	} {
		err := exec.validateMatchWhereSimpleOperands(scope, where)
		results[where] = err
		t.Logf("L6B_C22 validateMatchWhereSimpleOperands(match_semantic_validation.go:914, IN check :984-998) where=%q -> %v", where, err)
	}
	if results["false IN [1, 2, 'a', null]"] != nil && results["1 IN [1, 2, 'a', null]"] == nil {
		t.Errorf("CAUSE C22: the boolean literal left operand of IN is reported as an undefined variable: %v", results["false IN [1, 2, 'a', null]"])
	}
}

func TestL6bInternalLabelHiddenOwner(t *testing.T) {
	ctx := context.Background()
	base := newTestMemoryEngine(t)
	for _, db := range []string{"nornic", "system"} {
		engine := storage.NewNamespacedEngine(base, db)
		exec := NewStorageExecutor(engine)
		if _, err := exec.Execute(ctx, "CREATE (:_Migrated {_neo4j_id: 'x1', k: 1})", nil); err != nil {
			t.Fatalf("%s create: %v", db, err)
		}
		stored, err := engine.GetNodesByLabel(ctx, "_Migrated")
		hidden := false
		if len(stored) > 0 {
			hidden = isSystemNode(stored[0])
		}
		res, qerr := exec.Execute(ctx, "MATCH (n:_Migrated) RETURN count(n) AS c", nil)
		var c any
		if res != nil && len(res.Rows) > 0 {
			c = res.Rows[0][0]
		}
		t.Logf("L6B_C12 db=%s storage GetNodesByLabel(_Migrated)=%d err=%v | isInternalLabel(_Migrated)=%v isSystemNode=%v shouldHideSystemNodes(match_multi.go:977)=%v | MATCH count=%v err=%v",
			db, len(stored), err, isInternalLabel("_Migrated"), hidden, shouldHideSystemNodes(engine), c, qerr)
		if db == "nornic" && len(stored) == 1 && fmt.Sprint(c) == "0" && shouldHideSystemNodes(engine) && hidden {
			t.Errorf("CAUSE C12: the node is stored, but the reader hides it because its label starts with _ (isSystemNode) on a non-system database")
		}
	}
}
