// Close-out lane L8, round 2 (2026-11-22). Test-only, not for merge.
//   - L-H4-014 / -015: the hunter's exact statements on the corpus graphs G3
//     and G2 (l8SetupG3 / l8SetupG2, copied verbatim).
//   - L-H4-018 member 01075.1-3 as written (SKIP of a run-time expression).
//   - L-H2-010 owner: the compiled list programs evaluated with a canceled
//     context, next to the compiled reduce.
//   - L-H2-013 owner: CPU profile of the long chains run under a 20 ms deadline.
package cypher

import (
	"bytes"
	"context"
	"fmt"
	"runtime/pprof"
	"strings"
	"testing"
	"time"
)

func TestL8_H4_014_ExactG3(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := l8Exec(t, l8SetupG3...)
		q := "MATCH (n:C)-[*1..2]-(m) WHERE NOT (abs(toInteger(true)) = m.x) OPTIONAL MATCH (n)<-[q:R]-(o:C) RETURN n.id AS e0, substring(toString(m.x), 1) AS e1, m.id - toInteger(o.id) % abs(toInteger(coalesce(false, n.w))) AS e2 ORDER BY e0, e1, e2 SKIP 3 LIMIT 2"
		o := l8Run(t, exec, q, nil)
		// Neo4j 5.26.30 (lead): [[1, "", null], [1, "", null]].
		if o.Err {
			t.Errorf("Neo4j gives 2 rows with null; NornicDB refused: %s %s", o.Code, o.Msg)
		}
		// Diag: the divisor and its parts per row.
		l8Run(t, exec, "MATCH (n:C)-[*1..2]-(m) WHERE NOT (abs(toInteger(true)) = m.x) OPTIONAL MATCH (n)<-[q:R]-(o:C) RETURN DISTINCT n.w AS w, coalesce(false, n.w) AS c, toInteger(coalesce(false, n.w)) AS ti, o.id AS oid, toInteger(o.id) AS toid", nil)
		l8Run(t, exec, "MATCH (n:C)-[*1..2]-(m) WHERE NOT (abs(toInteger(true)) = m.x) OPTIONAL MATCH (n)<-[q:R]-(o:C) RETURN count(*) AS rows, count(o) AS withO", nil)
		l8Run(t, exec, "MATCH (n:C)-[*1..2]-(m) WHERE NOT (abs(toInteger(true)) = m.x) OPTIONAL MATCH (n)<-[q:R]-(o:C) RETURN m.id - toInteger(o.id) % abs(toInteger(coalesce(false, n.w))) AS e2 LIMIT 3", nil)
		l8Run(t, exec, "MATCH (n:C)-[*1..2]-(m) WHERE NOT (abs(toInteger(true)) = m.x) OPTIONAL MATCH (n)<-[q:R]-(o:C) RETURN toInteger(o.id) % abs(toInteger(coalesce(false, n.w))) AS e2 LIMIT 3", nil)
		l8Run(t, exec, "MATCH (n:C) OPTIONAL MATCH (n)<-[q:R]-(o:C) RETURN toInteger(o.id) % 0 AS v LIMIT 3", nil)
		l8Run(t, exec, "MATCH (n:C) OPTIONAL MATCH (n)<-[q:R]-(o:C) WITH n, o WHERE o IS NOT NULL RETURN toInteger(o.id) % 0 AS v LIMIT 3", nil)
		l8Run(t, exec, "MATCH (n:C {id: 3}) RETURN coalesce(false, n.w) AS c, toInteger(coalesce(false, n.w)) AS ti", nil)
	})
}

func TestL8_H4_015_ExactG2(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := l8Exec(t, l8SetupG2...)
		q := "MATCH (n)-[r:S]->(m:B) WHERE (m.w = 'a') AND (n.z IS NULL <> n.x + true) RETURN n.x AS e0, '' AS e1"
		o := l8Run(t, exec, q, nil)
		// Neo4j 5.26.30 (lead): [] (no row has m.w = 'a').
		if o.Err {
			t.Errorf("Neo4j gives []; NornicDB refused: %s %s", o.Code, o.Msg)
		}
		l8Run(t, exec, "MATCH (n)-[r:S]->(m:B) WHERE m.w = 'a' RETURN count(*) AS c", nil)
		l8Run(t, exec, "MATCH (n)-[r:S]->(m:B) WHERE (n.z IS NULL <> n.x + true) RETURN count(*) AS c", nil)
		l8Run(t, exec, "MATCH (n)-[r:S]->(m:B) WHERE (m.w = 'a') AND (n.z IS NULL <> n.x + 1) RETURN n.x AS e0", nil)
		l8Run(t, exec, "MATCH (n)-[r:S]->(m:B) WITH n, m WHERE (m.w = 'a') AND (n.z IS NULL <> n.x + true) RETURN n.x AS e0", nil)
		l8Run(t, exec, "MATCH (m:B)<-[r:S]-(n) WHERE (m.w = 'a') AND (n.z IS NULL <> n.x + true) RETURN n.x AS e0", nil)
		l8Run(t, exec, "MATCH (n)-[r:S]->(m:B) WHERE (n.z IS NULL <> n.x + true) AND (m.w = 'a') RETURN n.x AS e0", nil)
		l8Run(t, exec, "MATCH (n)-[r:S]->(m:B) WHERE m.w = 'a' AND n.x + true RETURN n.x AS e0", nil)
	})
}

func TestL8_H4_018_Member01075(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := l8Exec(t, "UNWIND range(1, 10) AS i CREATE ({nr: i})")
		// Neo4j 5.26.30 (lead): ArgumentError "SKIP: Invalid input. '-2' is not a valid value".
		l8WantCode(t, exec, "MATCH (n) WITH n SKIP toInteger(rand()*-9) WITH count(*) AS count RETURN count > 0 AS nonEmpty", "Neo.ClientError.Statement.ArgumentError")
	})
}

// L-H2-010 owner: compile each list form (rowValueProgramFor), then evaluate
// its program with a context that is already canceled. The compiled reduce
// (rowReduceLoop observes ctx) is the control.
func TestL8Owner_H2_010_CompiledPrograms(t *testing.T) {
	exec := l8Exec(t)
	params := map[string]interface{}{"items": l8Items(500000)}
	base := withParams(context.Background(), params)
	for _, expr := range []string{
		"[x IN $items | size(randomUUID())]",
		"[x IN $items WHERE size(randomUUID()) > 0]",
		"any(x IN $items WHERE size(randomUUID()) < 0)",
		"all(x IN $items WHERE size(randomUUID()) > 0)",
		"reduce(a = 0, x IN $items | a + size(randomUUID()))",
	} {
		values := map[string]interface{}{}
		program := rowValueProgramFor(base, expr, values)
		fields := map[string]any{"expression": expr, "compiled": program != nil}
		if program != nil {
			canceled, cancel := context.WithCancel(base)
			cancel()
			start := time.Now()
			value, ok, err := program.root.eval(canceled, exec, values, nil)
			fields["elapsed_ms"], fields["ok"] = time.Since(start).Milliseconds(), ok
			for k, v := range l8ErrFields(err) {
				fields[k] = v
			}
			if list, isList := value.([]interface{}); isList {
				fields["result_list_len"] = len(list)
			} else {
				fields["value"] = fmt.Sprint(value)
			}
		}
		// The text path for comparison (evaluateRowValueText, no program).
		canceled, cancel := context.WithCancel(base)
		cancel()
		start := time.Now()
		_, _, textErr := runtimeRowResult(exec.evaluateRowValueText(canceled, expr, values))
		fields["text_path_elapsed_ms"], fields["text_path_err"] = time.Since(start).Milliseconds(), fmt.Sprint(textErr)
		l8Owner(t, "L-H2-010", "compiled program eval with canceled ctx", fields)
	}
}

// L-H2-013 owner: where the time after the deadline goes. Each run has a
// 20 ms deadline; the profile covers the whole runs, so with the overrun
// (about 130-200 ms) dominating, the top frames are the post-deadline work.
func TestL8Owner_H2_013_DeadlineProfile(t *testing.T) {
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
			var overrun time.Duration
			for time.Since(start) < 4*time.Second {
				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
				began := time.Now()
				_, _ = exec.Execute(ctx, q, params)
				overrun += time.Since(began) - 20*time.Millisecond
				cancel()
				runs++
			}
			pprof.StopCPUProfile()
			l8Owner(t, "L-H2-013", "mean overrun "+shape, map[string]any{"runs": runs, "mean_overrun_ms": float64(overrun.Milliseconds()) / float64(max(runs, 1))})
			l8ProfileTop(t, "L-H2-013", "deadline-20ms "+shape, runs, time.Since(start), buf.Bytes())
		}
	})
}
