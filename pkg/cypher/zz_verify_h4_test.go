// Verification tests for hunt leads L-H4-001, -002, -003, -005, -011, -012
// and the duplicate check of L-H4-013 (v1.5.2 verification campaign, H4
// verifier lane, 2026-11-21). Test-only, not for merge. Each lead test
// asserts the Neo4j 5.26.30 result recorded in the lead (or in its hunt-log
// row's HUNT_LEAD line) and fails exactly at the disagreement; each control
// next to it is expected to pass. Diag tests only log (no assertion), except
// the L-H4-013 owner check, which passes when the N152-92 splitter cut is
// present.
package cypher

import (
	"context"
	"fmt"
	"math"
	"os"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func verifyH4Executor(t *testing.T, setup []string) *StorageExecutor {
	t.Helper()
	engine := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = engine.Close() })
	exec := NewStorageExecutor(storage.NewNamespacedEngine(engine, "nornic"))
	for _, statement := range setup {
		_, err := exec.Execute(context.Background(), statement, nil)
		require.NoError(t, err, statement)
	}
	return exec
}

// verifyH4Rows runs query and returns its rows printed with %v, logging them.
func verifyH4Rows(t *testing.T, exec *StorageExecutor, query string) (string, error) {
	t.Helper()
	result, err := exec.Execute(context.Background(), query, nil)
	if err != nil {
		t.Logf("VERIFY_H4 parser=%q %q -> ERROR %v", os.Getenv("NORNICDB_PARSER"), query, err)
		return "", err
	}
	printed := fmt.Sprintf("%v", result.Rows)
	t.Logf("VERIFY_H4 parser=%q %q -> rows=%d %s", os.Getenv("NORNICDB_PARSER"), query, len(result.Rows), printed)
	return printed, nil
}

func verifyH4Expect(t *testing.T, exec *StorageExecutor, query, want string) {
	t.Helper()
	got, err := verifyH4Rows(t, exec, query)
	if assert.NoError(t, err, query) {
		assert.Equal(t, want, got, query)
	}
}

func verifyH4ExpectError(t *testing.T, exec *StorageExecutor, query string) {
	t.Helper()
	got, err := verifyH4Rows(t, exec, query)
	assert.Error(t, err, "Neo4j refuses %q; NornicDB returned %s", query, got)
}

// ---------------------------------------------------------------- L-H4-001
// Neo4j reads a <op> b IS NULL as a <op> (b IS NULL).

func TestVerifyH4_001_IsNullPrecedenceG2(t *testing.T) {
	exec := verifyH4Executor(t, verifyH4SetupG2)
	// Neo4j 5.26.30: 0 rows (lead L-H4-001, H4-gen-G2-0348).
	verifyH4Expect(t, exec, "MATCH (n) WHERE n.k < true IS NULL WITH n.id AS g, count(*) AS a RETURN g, a", "[]")
}

func TestVerifyH4_001_IsNullPrecedenceG4(t *testing.T) {
	exec := verifyH4Executor(t, verifyH4SetupG4)
	// Neo4j 5.26.30: [[null, 103]] (lead L-H4-001, H4-gen-G4-1928).
	verifyH4Expect(t, exec, "MATCH (n:B) WHERE n.x <> n.x IS NULL WITH n.v AS g, sum(toInteger(n.x)) AS a RETURN g, a", "[[<nil> 103]]")
}

// Control: the same predicates with the grouping written out.
func TestVerifyH4_001_Control_Parenthesized(t *testing.T) {
	g2 := verifyH4Executor(t, verifyH4SetupG2)
	verifyH4Expect(t, g2, "MATCH (n) WHERE n.k < (true IS NULL) WITH n.id AS g, count(*) AS a RETURN g, a", "[]")
	g4 := verifyH4Executor(t, verifyH4SetupG4)
	verifyH4Expect(t, g4, "MATCH (n:B) WHERE n.x <> (n.x IS NULL) WITH n.v AS g, sum(toInteger(n.x)) AS a RETURN g, a", "[[<nil> 103]]")
	verifyH4Expect(t, g4, "MATCH (n:B) WHERE (n.x <> n.x) IS NULL WITH n.v AS g, sum(toInteger(n.x)) AS a RETURN g, a", "[[<nil> 0]]")
}

// Diag: which predicate node the compiled WHERE program builds for the leaf,
// and how a projection reads the same shape (logged only).
func TestVerifyH4_001_Diag(t *testing.T) {
	for _, leaf := range []string{"n.x <> n.x IS NULL", "n.k < true IS NULL"} {
		node := compileRowPredicateLeaf(context.Background(), leaf)
		t.Logf("VERIFY_H4_DIAG leaf %q -> %T", leaf, node)
	}
	exec := verifyH4Executor(t, nil)
	for _, query := range []string{
		"RETURN 1 <> 1 IS NULL AS r",
		"RETURN 1 <> (1 IS NULL) AS r",
		"WITH 1 AS x WHERE x <> x IS NULL RETURN x",
		"UNWIND [1, null] AS x WITH x WHERE x <> x IS NULL RETURN collect(x) AS r",
	} {
		_, _ = verifyH4Rows(t, exec, query)
	}
}

// ---------------------------------------------------------------- L-H4-002
// Temporal and duration constructors with a null map value.

func TestVerifyH4_002_TemporalNullMapValue(t *testing.T) {
	exec := verifyH4Executor(t, nil)
	t.Run("date-week-null", func(t *testing.T) {
		_, _ = verifyH4Rows(t, exec, "RETURN date({year: 1817, week: null}) AS d")
		// Neo4j 5.26.30: 1816-12-30 (H4-mut-H1-tck-03099.1-2).
		verifyH4Expect(t, exec, "RETURN toString(date({year: 1817, week: null})) AS d", "[[1816-12-30]]")
	})
	t.Run("date-year-null", func(t *testing.T) {
		_, _ = verifyH4Rows(t, exec, "RETURN date({year: null, month: 10, day: 11}) AS result")
		// Neo4j 5.26.30: 0000-10-11 (H4-mut-H1-tck-03142.1-1).
		verifyH4Expect(t, exec, "RETURN toString(date({year: null, month: 10, day: 11})) AS result", "[[0000-10-11]]")
	})
	t.Run("duration-weeks-null", func(t *testing.T) {
		// Neo4j 5.26.30: error "weeks must be a number value, but was a NoValue" (H4-mut-H1-tck-03294.1-1).
		verifyH4ExpectError(t, exec, "RETURN duration({weeks: null}) AS result")
	})
	t.Run("create-duration-seconds-null", func(t *testing.T) {
		// Neo4j 5.26.30 refuses it (H4-mut-H1-tck-03686.1-1).
		verifyH4ExpectError(t, exec, "CREATE ({created: duration({seconds: null})})")
	})
	t.Run("date-from-date-day-null", func(t *testing.T) {
		_, _ = verifyH4Rows(t, exec, "WITH date({year: 1984, month: 11, day: 11}) AS other RETURN date({date: other, day: null}) AS result")
		// Neo4j 5.26.30: 1984-11-01 (H4-mut-H1-tck-03491.1-2).
		verifyH4Expect(t, exec, "WITH date({year: 1984, month: 11, day: 11}) AS other RETURN toString(date({date: other, day: null})) AS result", "[[1984-11-01]]")
	})
}

// Control: the same constructors with the value present.
func TestVerifyH4_002_Control_ValuePresent(t *testing.T) {
	exec := verifyH4Executor(t, nil)
	verifyH4Expect(t, exec, "RETURN toString(date({year: 1817, week: 1})) AS d", "[[1816-12-30]]")
	verifyH4Expect(t, exec, "RETURN toString(date({year: 1817})) AS d", "[[1817-01-01]]")
	verifyH4Expect(t, exec, "RETURN toString(date({year: 1984, month: 10, day: 11})) AS result", "[[1984-10-11]]")
	verifyH4Expect(t, exec, "RETURN toString(duration({weeks: 1})) AS result", "[[P7D]]")
	verifyH4Expect(t, exec, "WITH date({year: 1984, month: 11, day: 11}) AS other RETURN toString(date({date: other, day: 1})) AS result", "[[1984-11-01]]")
}

// ---------------------------------------------------------------- L-H4-003
// time({time: <zoned datetime>, second: 42}) keeps the datetime's offset.

func TestVerifyH4_003_TimeFromSummerDateTimeOffset(t *testing.T) {
	exec := verifyH4Executor(t, nil)
	_, _ = verifyH4Rows(t, exec, "WITH datetime({year: 1984, month: 9, day: 11, hour: 12, timezone: 'Europe/Stockholm'}) AS other\nRETURN time({time: other, second: 42}) AS result")
	// Neo4j 5.26.30: 12:00:42+02:00 (lead L-H4-003, H4-mut-H1-tck-03539.1-2).
	verifyH4Expect(t, exec, "WITH datetime({year: 1984, month: 9, day: 11, hour: 12, timezone: 'Europe/Stockholm'}) AS other\nRETURN toString(time({time: other, second: 42})) AS result", "[[12:00:42+02:00]]")
}

// Control: the datetime itself carries +02:00; a winter datetime gives +01:00.
func TestVerifyH4_003_Control(t *testing.T) {
	exec := verifyH4Executor(t, nil)
	verifyH4Expect(t, exec, "WITH datetime({year: 1984, month: 9, day: 11, hour: 12, timezone: 'Europe/Stockholm'}) AS other RETURN other.offset AS o", "[[+02:00]]")
	verifyH4Expect(t, exec, "WITH datetime({year: 1984, month: 1, day: 11, hour: 12, timezone: 'Europe/Stockholm'}) AS other\nRETURN toString(time({time: other, second: 42})) AS result", "[[12:00:42+01:00]]")
}

// ---------------------------------------------------------------- L-H4-005
// range() with a negative step and start below end.

func TestVerifyH4_005_RangeNegativeStepStartBelowEnd(t *testing.T) {
	exec := verifyH4Executor(t, nil)
	// Neo4j 5.26.30: [-1] and [0] (H4-mut-H1-tck-01920.1-1, -01921.1-2).
	verifyH4Expect(t, exec, "RETURN range(-1, 1, -123) AS list", "[[[-1]]]")
	verifyH4Expect(t, exec, "RETURN range(0, 1, -2) AS list", "[[[0]]]")
}

func TestVerifyH4_005_Control(t *testing.T) {
	exec := verifyH4Executor(t, nil)
	verifyH4Expect(t, exec, "RETURN range(1, -1, -1) AS list", "[[[1 0 -1]]]")
	verifyH4Expect(t, exec, "RETURN range(0, 5, 2) AS list", "[[[0 2 4]]]")
	_, _ = verifyH4Rows(t, exec, "RETURN range(5, 1, 1) AS list")
}

// ---------------------------------------------------------------- L-H4-011
// A literal true/false/null as the left operand of IN in a WHERE.

func TestVerifyH4_011_LiteralInListWhere(t *testing.T) {
	exec := verifyH4Executor(t, verifyH4SetupG3)
	// Neo4j 5.26.30: 0 rows (H4-gen-G3-2293); true/null forms: 0 rows in
	// H4-gen-G1-2509 / H4-gen-G4-2656 (the predicate is null whatever the graph).
	for _, literal := range []string{"false", "true", "null"} {
		verifyH4Expect(t, exec, "MATCH (n)<-[r:S]-(m:C) WHERE "+literal+" IN [1, 2, 'a', null] RETURN n.x AS e0", "[]")
	}
}

func TestVerifyH4_011_Control(t *testing.T) {
	exec := verifyH4Executor(t, verifyH4SetupG3)
	_, err := verifyH4Rows(t, exec, "MATCH (n)<-[r:S]-(m:C) WHERE n.id IN [1, 2, 'a', null] RETURN n.x AS e0")
	assert.NoError(t, err)
	verifyH4Expect(t, exec, "RETURN false IN [1, 2, 'a', null] AS r", "[[<nil>]]")
}

func TestVerifyH4_011_Diag(t *testing.T) {
	t.Logf("VERIFY_H4_DIAG expressionFreeVariables(false IN [1, 2, 'a', null]) = %q", expressionFreeVariables(context.Background(), "false IN [1, 2, 'a', null]"))
	exec := verifyH4Executor(t, verifyH4SetupG3)
	for _, query := range []string{
		"MATCH (n) WHERE false IN [1, 2, 'a', null] RETURN count(n) AS c",
		"MATCH (n) WHERE false IN [1, 2] RETURN count(n) AS c",
		"MATCH (n) WHERE false IN [false] RETURN count(n) AS c",
		"MATCH (n)<-[r:S]-(m:C) WHERE false IN [false] RETURN count(n) AS c",
		"MATCH (n)<-[r:S]-(m:C) WHERE 1 IN [1, 2, 'a', null] RETURN count(n) AS c",
		"WITH 1 AS x WHERE false IN [1, 2] RETURN x",
	} {
		_, _ = verifyH4Rows(t, exec, query)
	}
}

// ---------------------------------------------------------------- L-H4-012
// abs(...) + <string> refused by the static type check.

func TestVerifyH4_012_AbsPlusString(t *testing.T) {
	exec := verifyH4Executor(t, nil)
	// Neo4j 5.26.30: [[1, ['0a', 'ab']]] (H4-gen-G5-1134).
	verifyH4Expect(t, exec, "RETURN size(toString(4)) AS c0, [abs(toInteger(-0.0)) + 'a', 'ab'] AS c1", "[[1 [0a ab]]]")
}

func TestVerifyH4_012_Control(t *testing.T) {
	exec := verifyH4Executor(t, nil)
	verifyH4Expect(t, exec, "RETURN size(toString(4)) AS c0, [toInteger(-0.0) + 'a', 'ab'] AS c1", "[[1 [0a ab]]]")
}

func TestVerifyH4_012_Diag(t *testing.T) {
	exec := verifyH4Executor(t, nil)
	for _, query := range []string{
		"RETURN abs(1) + 'a' AS r",
		"RETURN 'a' + abs(1) AS r",
		"RETURN abs(1) + 1 AS r",
		"RETURN abs(1) - 1 AS r",
		"RETURN abs(-1.5) + 'a' AS r",
		"RETURN abs(1) + [2] AS r",
		"RETURN sign(1) + 'a' AS r",
	} {
		_, _ = verifyH4Rows(t, exec, query)
	}
}

// ---------------------------------------------------------------- L-H4-013
// Duplicate check against N152-92 (row-arithmetic splitter and CASE).

func TestVerifyH4_013_CaseOperandModulo(t *testing.T) {
	exec := verifyH4Executor(t, nil)
	query := "RETURN toInteger('') AS c0, -0.0 % CASE WHEN 'a' >= -0.0 THEN 1 ELSE -0.0 END AS c1"
	result, err := exec.Execute(context.Background(), query, nil)
	// Neo4j 5.26.30: [[null, NaN]] (H4-gen-G3-0671).
	if !assert.NoError(t, err, query) {
		t.Logf("VERIFY_H4 parser=%q %q -> ERROR %v", os.Getenv("NORNICDB_PARSER"), query, err)
		return
	}
	t.Logf("VERIFY_H4 parser=%q %q -> %v", os.Getenv("NORNICDB_PARSER"), query, result.Rows)
	require.Len(t, result.Rows, 1)
	assert.Nil(t, result.Rows[0][0])
	f, ok := result.Rows[0][1].(float64)
	assert.True(t, ok && math.IsNaN(f), "c1 = %#v, want NaN", result.Rows[0][1])
}

func TestVerifyH4_013_Control_Parenthesized(t *testing.T) {
	exec := verifyH4Executor(t, nil)
	query := "RETURN toInteger('') AS c0, -0.0 % (CASE WHEN 'a' >= -0.0 THEN 1 ELSE -0.0 END) AS c1"
	result, err := exec.Execute(context.Background(), query, nil)
	require.NoError(t, err, query)
	t.Logf("VERIFY_H4 parser=%q %q -> %v", os.Getenv("NORNICDB_PARSER"), query, result.Rows)
	require.Len(t, result.Rows, 1)
	f, ok := result.Rows[0][1].(float64)
	assert.True(t, ok && math.IsNaN(f), "c1 = %#v, want NaN", result.Rows[0][1])
}

// Owner check: the product splitter (N152-92's owner) cuts the lead's
// expression inside the CASE. Passes when that is so.
func TestVerifyH4_013_OwnerIsN152_92Splitter(t *testing.T) {
	expr := "-0.0 % CASE WHEN 'a' >= -0.0 THEN 1 ELSE -0.0 END"
	left, right, operator, ok := splitRowArithmeticTier(expr, "+-")
	t.Logf("VERIFY_H4_DIAG splitRowArithmeticTier(%q, \"+-\") = (%q, %q, %q, %v)", expr, left, string(operator), right, ok)
	assert.True(t, ok)
	assert.True(t, strings.Contains(left, "CASE") && !strings.Contains(left, "END"), "left half ends inside the CASE: %q", left)
	exec := verifyH4Executor(t, nil)
	_, _ = verifyH4Rows(t, exec, "RETURN 10 + CASE WHEN true THEN 5 - 2 ELSE 0 END AS r")
}
