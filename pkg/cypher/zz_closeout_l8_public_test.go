// Close-out lane L8 (v1.5.2 verification campaign, 2026-11-22): public
// statement tests for hunt leads L-H4-007, -008, -011, -012, -014 to -020,
// L-H2-010, -011, -013. Test-only, not for merge. Each lead test asserts the
// Neo4j 5.26.30 result recorded in the lead (or the openCypher TCK's where
// the lead's premise depends on it) and fails exactly at the disagreement;
// the control next to it is expected to pass. Every statement logs one
// L8_STMT line with its outcome so the values can be read from the job.
package cypher

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
)

func l8Exec(t *testing.T, setup ...string) *StorageExecutor {
	t.Helper()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nornic"))
	for _, statement := range setup {
		if _, err := exec.Execute(context.Background(), statement, nil); err != nil {
			t.Fatalf("setup %q: %v", statement, err)
		}
	}
	return exec
}

type l8Outcome struct {
	Rows string `json:"rows,omitempty"`
	N    int    `json:"n"`
	Code string `json:"code,omitempty"`
	Msg  string `json:"msg,omitempty"`
	Err  bool   `json:"err"`
}

func l8Run(t *testing.T, exec *StorageExecutor, query string, params map[string]interface{}) l8Outcome {
	t.Helper()
	result, err := exec.Execute(context.Background(), query, params)
	var o l8Outcome
	if err != nil {
		o.Err = true
		o.Code, _ = nornicerrors.Neo4jStatus(err)
		o.Msg = err.Error()
	} else {
		o.Rows = fmt.Sprintf("%v", result.Rows)
		o.N = len(result.Rows)
	}
	data, _ := json.Marshal(map[string]any{"parser": config.GetParserType(), "query": query, "outcome": o})
	t.Logf("L8_STMT %s", data)
	return o
}

func l8WantRows(t *testing.T, exec *StorageExecutor, query, want string) {
	t.Helper()
	o := l8Run(t, exec, query, nil)
	if o.Err {
		t.Errorf("%q: Neo4j gives %s; NornicDB refused: %s %s", query, want, o.Code, o.Msg)
		return
	}
	if o.Rows != want {
		t.Errorf("%q: Neo4j gives %s; NornicDB %s", query, want, o.Rows)
	}
}

func l8WantCode(t *testing.T, exec *StorageExecutor, query, code string) {
	t.Helper()
	o := l8Run(t, exec, query, nil)
	if !o.Err {
		t.Errorf("%q: Neo4j fails with %s; NornicDB returned %s", query, code, o.Rows)
		return
	}
	if o.Code != code {
		t.Errorf("%q: Neo4j fails with %s; NornicDB with %s (%s)", query, code, o.Code, o.Msg)
	}
}

// ---------------------------------------------------------------- L-H4-007
// Neo4j 5.26.30: CREATE ()--() is SyntaxError "Only directed relationships
// are supported in CREATE" (lead); the TCK's RequiresDirectedRelationship.
func TestL8_H4_007_UndirectedCreate(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := l8Exec(t)
		l8WantCode(t, exec, "CREATE ()--()", "Neo.ClientError.Statement.SyntaxError")
		// What it wrote, when it was accepted.
		l8Run(t, exec, "MATCH (n) RETURN count(n) AS nodes", nil)
		l8Run(t, exec, "MATCH ()-[r]->() RETURN count(r) AS rels, collect(type(r)) AS types", nil)
		l8WantCode(t, l8Exec(t), "CREATE (a)--(b) RETURN a, b", "Neo.ClientError.Statement.SyntaxError")
	})
}

// Control: the bracketed undirected form is refused (existing behaviour).
func TestL8_H4_007_Control(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		l8WantCode(t, l8Exec(t), "CREATE ()-[:LINK]-()", "Neo.ClientError.Statement.SyntaxError")
		l8WantRows(t, l8Exec(t), "CREATE ()-[:LINK]->() RETURN 1 AS x", "[[1]]")
	})
}

// ---------------------------------------------------------------- L-H4-008
// Neo4j 5.26.30: ArgumentError "Invalid value for DayOfMonth ... -2" and
// "Invalid value for Nanosecond: -2" (lead).
func TestL8_H4_008_TruncateOutOfRangeFields(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := l8Exec(t)
		ldt := "localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123})"
		l8WantCode(t, exec, "RETURN date.truncate('month', "+ldt+", {day: -2}) AS result", "Neo.ClientError.Statement.ArgumentError")
		l8WantCode(t, exec, "RETURN datetime.truncate('day', "+ldt+", {nanosecond: -2}) AS result", "Neo.ClientError.Statement.ArgumentError")
		// Not established by the lead: the plain constructor with the same field.
		l8Run(t, exec, "RETURN date({year: 1984, month: 10, day: -2}) AS d", nil)
		l8Run(t, exec, "RETURN date.truncate('month', date({year: 1984, month: 10, day: 11}), {day: 40}) AS d", nil)
	})
}

// Control: an in-range field override is applied.
func TestL8_H4_008_Control(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		ldt := "localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123})"
		l8WantRows(t, l8Exec(t), "RETURN toString(date.truncate('month', "+ldt+", {day: 5})) AS result", "[[1984-10-05]]")
	})
}

// ---------------------------------------------------------------- L-H4-011 / -012
// Already confirmed by the H4 verifier; repeated here next to the owner run.
func TestL8_H4_011_LiteralInListWhere(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := l8Exec(t, "CREATE (:C {x: 1})-[:S]->(:C {x: 2})")
		for _, literal := range []string{"false", "true", "null"} {
			l8WantRows(t, exec, "MATCH (n)<-[r:S]-(m:C) WHERE "+literal+" IN [1, 2, 'a', null] RETURN n.x AS e0", "[]")
		}
	})
}

func TestL8_H4_011_Control(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := l8Exec(t, "CREATE (:C {x: 1})-[:S]->(:C {x: 2})")
		l8WantRows(t, exec, "MATCH (n)<-[r:S]-(m:C) WHERE 1 IN [1, 2, 'a', null] RETURN n.x AS e0", "[[2]]")
	})
}

func TestL8_H4_012_AbsPlus(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := l8Exec(t)
		l8WantRows(t, exec, "RETURN size(toString(4)) AS c0, [abs(toInteger(-0.0)) + 'a', 'ab'] AS c1", "[[1 [0a ab]]]")
		l8WantRows(t, exec, "RETURN abs(1) + 1 AS v", "[[2]]")
	})
}

func TestL8_H4_012_Control(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := l8Exec(t)
		l8WantRows(t, exec, "RETURN sign(1) + 1 AS v", "[[2]]")
		l8WantRows(t, exec, "RETURN 'a' + abs(1) AS v", "[[a1]]")
	})
}

// ---------------------------------------------------------------- L-H4-014
// Neo4j 5.26.30: null % 0 is null (H4-gen-G3-0643: rows with null).
func TestL8_H4_014_NullModuloZero(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := l8Exec(t, "CREATE (:C {id: 1, x: 5, w: 0.5})-[:R]->(:C {id: 2, x: 7})")
		l8WantRows(t, exec, "RETURN null % 0 AS v", "[[<nil>]]")
		l8WantRows(t, exec, "WITH null AS x RETURN x % 0 AS v", "[[<nil>]]")
		l8WantRows(t, exec, "OPTIONAL MATCH (o:Missing) RETURN toInteger(o.id) % 0 AS v", "[[<nil>]]")
		l8WantRows(t, exec, "OPTIONAL MATCH (o:Missing) RETURN toInteger(o.id) % abs(toInteger(coalesce(false, 1))) AS v", "[[<nil>]]")
		l8WantRows(t, exec, "MATCH (n:C {id: 1}) OPTIONAL MATCH (n)<-[q:R]-(o:C) RETURN n.id - toInteger(o.id) % abs(toInteger(coalesce(false, n.w))) AS e2", "[[<nil>]]")
		// Diag: the other null operand orders and operators.
		l8Run(t, exec, "RETURN null / 0 AS v", nil)
		l8Run(t, exec, "RETURN 0 % null AS v", nil)
		l8Run(t, exec, "WITH null AS x RETURN 1 - x % 0 AS v", nil)
	})
}

// Control: an integer % 0 is Neo4j's "/ by zero".
func TestL8_H4_014_Control(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		l8WantCode(t, l8Exec(t), "WITH 5 AS x RETURN x % 0 AS v", "Neo.ClientError.Statement.ArithmeticError")
		l8WantRows(t, l8Exec(t), "WITH null AS x RETURN x % 2 AS v", "[[<nil>]]")
	})
}

// ---------------------------------------------------------------- L-H4-015
// Neo4j 5.26.30: 0 rows for H4-gen-G2-0226 (m.w = 'a' holds for no row, so
// the other conjunct is never evaluated). Minimal graph: one S path whose
// end node's w is not 'a' and whose start node's x is an integer.
func TestL8_H4_015_AndEvaluationOrder(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := l8Exec(t, "CREATE (:A {x: 1})-[:S]->(:B {w: 'b'})")
		l8WantRows(t, exec, "MATCH (n)-[r:S]->(m:B) WHERE (m.w = 'a') AND (n.z IS NULL <> n.x + true) RETURN n.x AS e0, '' AS e1", "[]")
		// Diag: the same conjuncts on one variable, swapped, and on a WITH row.
		l8Run(t, exec, "MATCH (n)-[r:S]->(m:B) WHERE (n.z IS NULL <> n.x + true) AND (m.w = 'a') RETURN n.x AS e0", nil)
		l8Run(t, exec, "MATCH (n)-[r:S]->(m:B) WHERE (m.w = 'a') AND (m.z IS NULL <> n.x + true) RETURN n.x AS e0", nil)
		l8Run(t, exec, "MATCH (n:A) WHERE n.x = 2 AND (n.z IS NULL <> n.x + true) RETURN n.x AS e0", nil)
		l8Run(t, exec, "UNWIND [{w: 'b', x: 1}] AS m WITH m WHERE m.w = 'a' AND (m.z IS NULL <> m.x + true) RETURN m.x AS e0", nil)
		l8Run(t, exec, "MATCH (n)-[r:S]->(m:B) WITH n, m WHERE (m.w = 'a') AND (n.z IS NULL <> n.x + true) RETURN n.x AS e0", nil)
	})
}

// Control: with m.w = 'a' true the conjunct is evaluated and fails on Neo4j too.
func TestL8_H4_015_Control(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := l8Exec(t, "CREATE (:A {x: 1})-[:S]->(:B {w: 'a'})")
		l8WantCode(t, exec, "MATCH (n)-[r:S]->(m:B) WHERE (m.w = 'a') AND (n.z IS NULL <> n.x + true) RETURN n.x AS e0", "Neo.ClientError.Statement.TypeError")
	})
}

// ---------------------------------------------------------------- L-H4-016
// Lead: Neo4j 5.26.30 returns 0 rows for MATCH (a)-[r]-()-[r]->(a) RETURN r;
// NornicDB refuses. The openCypher TCK (Match3 [29]) requires the refusal
// for the directed original; this run records both forms and the Cypher 25
// match-mode form that the code reads as Neo4j does.
func TestL8_H4_016_RepeatedRelationshipVariable(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := l8Exec(t, "CREATE (a:A)-[:T]->(b:B)-[:T]->(a)")
		tck := l8Run(t, exec, "MATCH (a)-[r]->()-[r]->(a) RETURN r", nil)
		mutant := l8Run(t, exec, "MATCH (a)-[r]-()-[r]->(a) RETURN r", nil)
		mode := l8Run(t, exec, "CYPHER 25 MATCH DIFFERENT RELATIONSHIPS (a)-[r]-()-[r]->(a) RETURN r", nil)
		if !tck.Err || tck.Code != "Neo.ClientError.Statement.SyntaxError" {
			t.Errorf("TCK Match3 [29] requires SyntaxError for the directed original; got %+v", tck)
		}
		if !mutant.Err {
			t.Errorf("mutant accepted: %+v", mutant)
		}
		_ = mode
	})
}

// ---------------------------------------------------------------- L-H4-017
// Neo4j 5.26.30: percentileDisc(null, deg) per deg group is null (9 rows).
func TestL8_H4_017_PercentileOfNullValues(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := l8Exec(t, "UNWIND range(0, 10) AS i CREATE (s:S) WITH s, i UNWIND range(0, i) AS j CREATE (s)-[:REL]->()")
		o := l8Run(t, exec, "MATCH (n:S) WITH n, size([(n)-->() | 1]) AS deg WHERE deg > 2 WITH deg LIMIT 100 RETURN percentileDisc(null, deg), deg", nil)
		if o.Err {
			t.Errorf("Neo4j gives 9 rows of null; NornicDB refused: %s %s", o.Code, o.Msg)
		} else if o.N != 9 {
			t.Errorf("Neo4j gives 9 rows; NornicDB %d: %s", o.N, o.Rows)
		}
		// Diag: which values the percentile check reads.
		l8Run(t, exec, "UNWIND [3] AS p RETURN percentileDisc(null, p) AS v", nil)
		l8Run(t, exec, "UNWIND [null] AS x RETURN percentileDisc(x, 3) AS v", nil)
		l8Run(t, exec, "UNWIND [1] AS x RETURN percentileDisc(x, 3) AS v", nil)
		l8Run(t, exec, "UNWIND [] AS x RETURN percentileDisc(x, 3) AS v", nil)
	})
}

// Control: an in-range percentile over null values is null.
func TestL8_H4_017_Control(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		l8WantRows(t, l8Exec(t), "UNWIND [null, null] AS x RETURN percentileDisc(x, 0.5) AS v", "[[<nil>]]")
	})
}

// ---------------------------------------------------------------- L-H4-018
// Error class of the 22 members (Neo4j codes recorded in the lead's log).
var l8H4018 = []struct{ id, setup, query, neo string }{
	{"01075.1-3", "UNWIND range(1, 10) AS i CREATE ({nr: i})", "MATCH (n) WITH n SKIP -2 WITH count(*) AS count RETURN count > 0 AS nonEmpty", "Neo.ClientError.Statement.SyntaxError"},
	{"01079.1-2", "CREATE (s:Person {name: 'Steven'}), (c:Person {name: 'Craig'})", "MATCH (p:Person) RETURN p.name AS name SKIP -null", "Neo.ClientError.Statement.TypeError"},
	{"01083.1-2", "", "MATCH (n) RETURN n   SKIP -null", "Neo.ClientError.Statement.TypeError"},
	{"01089.1-1", "UNWIND range(1, 3) AS i CREATE ({nr: i})", "MATCH (n) WITH n LIMIT toInteger(ceil(null)) RETURN count(*) AS count", "Neo.ClientError.Statement.TypeError"},
	{"01095.1-1", "", "MATCH (n) RETURN n   LIMIT -null", "Neo.ClientError.Statement.TypeError"},
	{"01096.1-1", "CREATE (s:Person {name: 'Steven'}), (c:Person {name: 'Craig'})", "MATCH (p:Person) RETURN p.name AS name LIMIT -null", "Neo.ClientError.Statement.TypeError"},
	{"01937.1-2", "", "RETURN range(2, null, 0)", "Neo.ClientError.Statement.ArgumentError"},
	{"01940.1-2", "", "RETURN range(null, 8, 0)", "Neo.ClientError.Statement.ArgumentError"},
	{"02565.1-1", "", "RETURN none(x IN ['Clara', 'Bob', null, 'Alice'] WHERE x % 2 = 0) AS result", "Neo.ClientError.Statement.TypeError"},
	{"02717.1-3", "", "RETURN single(x IN [false, null] WHERE x % 2 = 0) AS result", "Neo.ClientError.Statement.TypeError"},
	{"02718.1-1", "", "RETURN single(x IN ['Clara', null, 'Dave', 'Alice'] WHERE x % 2 = 0) AS result", "Neo.ClientError.Statement.TypeError"},
	{"02928.1-2", "", "RETURN all(x IN ['Clara', 'Bob', null, 'Alice'] WHERE x % 2 = 0) AS result", "Neo.ClientError.Statement.TypeError"},
	{"03109.1-1", "", "RETURN date({date: date(null), week: 2, dayOfWeek: 3}) AS d", "Neo.ClientError.Statement.ArgumentError"},
	{"03124.1-1", "", "RETURN localdatetime({date: date(null), week: 2, dayOfWeek: 3}) AS d", "Neo.ClientError.Statement.ArgumentError"},
	{"03125.1-2", "", "RETURN localdatetime({date: date(null), week: 2}) AS d", "Neo.ClientError.Statement.ArgumentError"},
}

func TestL8_H4_018_ErrorClass(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		for _, m := range l8H4018 {
			setup := []string{}
			if m.setup != "" {
				setup = append(setup, m.setup)
			}
			l8WantCode(t, l8Exec(t, setup...), m.query, m.neo)
		}
	})
}

// Disambiguation: each member's single error alone (one error per statement).
func TestL8_H4_018_SingleErrorVariants(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := l8Exec(t, "UNWIND range(1, 3) AS i CREATE ({nr: i})")
		for _, q := range []string{
			"RETURN range(2, 5, 0) AS r",
			"RETURN range(2, null, 1) AS r",
			"RETURN range(null, 8, 1) AS r",
			"UNWIND [1] AS x RETURN x LIMIT -1",
			"UNWIND [1] AS x RETURN x SKIP -1",
			"UNWIND [1] AS x RETURN x LIMIT null",
			"UNWIND [1] AS x RETURN x LIMIT $p",
			"RETURN all(x IN ['a'] WHERE x % 2 = 0) AS r",
			"WITH ['a'] AS l RETURN all(x IN l WHERE x % 2 = 0) AS r",
			"RETURN 'a' % 2 AS r",
			"RETURN date(null) AS d",
			"RETURN date({date: null}) AS d",
			"RETURN date({week: 2}) AS d",
			"CALL db.labels() RETURN label",
			"WITH null AS label CALL db.labels() YIELD label RETURN *",
			"CALL db.labels() YIELD * RETURN label",
			"MATCH (n) WITH size(n) AS s RETURN s",
			"CALL test.my.proc(0) YIELD out RETURN out",
		} {
			params := map[string]interface{}{"p": nil}
			l8Run(t, exec, q, params)
		}
	})
}

// ---------------------------------------------------------------- L-H4-019 / -020
// Lead: rows differ only in order (no ORDER BY; order within a pattern
// comprehension and of labels() is not specified by Cypher). The tests
// compare as multisets, in which form Neo4j's recorded rows and NornicDB's
// must agree; they also log NornicDB's own order.
func l8Canon(rows [][]interface{}) []string {
	out := make([]string, 0, len(rows))
	for _, row := range rows {
		parts := make([]string, 0, len(row))
		for _, value := range row {
			parts = append(parts, l8CanonValue(value))
		}
		out = append(out, strings.Join(parts, "|"))
	}
	sort.Strings(out)
	return out
}

func l8CanonValue(value interface{}) string {
	switch typed := value.(type) {
	case []interface{}:
		items := make([]string, 0, len(typed))
		for _, item := range typed {
			items = append(items, l8CanonValue(item))
		}
		sort.Strings(items)
		return "[" + strings.Join(items, ",") + "]"
	case []string:
		items := append([]string(nil), typed...)
		sort.Strings(items)
		return "[" + strings.Join(items, ",") + "]"
	case map[string]interface{}:
		if nodes, ok := typed["nodes"]; ok {
			return "path(" + l8CanonPathNodes(nodes) + ")"
		}
		keys := make([]string, 0, len(typed))
		for k := range typed {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		parts := []string{}
		for _, k := range keys {
			parts = append(parts, k+"="+l8CanonValue(typed[k]))
		}
		return "{" + strings.Join(parts, ",") + "}"
	case *storage.Node:
		labels := append([]string(nil), typed.Labels...)
		sort.Strings(labels)
		return "node" + strings.Join(labels, ":")
	case storage.Node:
		return l8CanonValue(&typed)
	case *PathResult:
		parts := []string{}
		for _, n := range typed.Nodes {
			parts = append(parts, l8CanonValue(n))
		}
		return "path(" + strings.Join(parts, ">") + ")"
	}
	return fmt.Sprintf("%v", value)
}

func l8CanonPathNodes(nodes interface{}) string {
	parts := []string{}
	switch typed := nodes.(type) {
	case []interface{}:
		for _, n := range typed {
			parts = append(parts, l8CanonValue(n))
		}
	case []*storage.Node:
		for _, n := range typed {
			parts = append(parts, l8CanonValue(n))
		}
	default:
		parts = append(parts, fmt.Sprintf("%T", nodes))
	}
	return strings.Join(parts, ">")
}

func TestL8_H4_019_PatternComprehensionOrder(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := l8Exec(t, "CREATE (a:A), (b:B) CREATE (a)-[:T]->(b), (b)-[:T]->(:C)")
		// Neo4j 5.26.30 (lead, start->end labels per path, per row):
		// [A>B] ; [B>A, B>C] ; [C>B]
		want := []string{"[path(nodeA>nodeB)]", "[path(nodeB>nodeA),path(nodeB>nodeC)]", "[path(nodeC>nodeB)]"}
		sort.Strings(want)
		result, err := exec.Execute(context.Background(), "MATCH (n) RETURN [p = (n)--() | p] AS list", nil)
		if err != nil {
			t.Fatalf("refused: %v", err)
		}
		got := l8Canon(result.Rows)
		data, _ := json.Marshal(map[string]any{"parser": config.GetParserType(), "rows_as_returned": fmt.Sprintf("%v", result.Rows), "canonical": got, "want": want})
		t.Logf("L8_ORDER %s", data)
		if strings.Join(got, ";") != strings.Join(want, ";") {
			t.Errorf("as multisets: Neo4j %v, NornicDB %v", want, got)
		}
		// Diag: does NornicDB's own order vary between runs?
		orders := map[string]int{}
		for i := 0; i < 20; i++ {
			again, err := exec.Execute(context.Background(), "MATCH (n) RETURN [p = (n)--() | p] AS list", nil)
			if err == nil {
				orders[fmt.Sprintf("%v", again.Rows)]++
			}
		}
		t.Logf("L8_ORDER_RUNS distinct=%d", len(orders))
	})
}

func TestL8_H4_020_LabelOrder(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := l8Exec(t, "CREATE (:L:A {num: 42})")
		l8Run(t, exec, "MERGE (test:L:B {num: 41}) RETURN labels(test) AS labels", nil)
		result, err := exec.Execute(context.Background(), "MATCH (n) RETURN labels(n) AS labels, properties(n) AS properties", nil)
		if err != nil {
			t.Fatalf("refused: %v", err)
		}
		got := l8Canon(result.Rows)
		// Neo4j 5.26.30: [A, L] {num: 42} and [B, L] {num: 41}.
		want := []string{"[A,L]|{num=42}", "[B,L]|{num=41}"}
		data, _ := json.Marshal(map[string]any{"parser": config.GetParserType(), "rows_as_returned": fmt.Sprintf("%v", result.Rows), "canonical": got, "want": want})
		t.Logf("L8_ORDER %s", data)
		if strings.Join(got, ";") != strings.Join(want, ";") {
			t.Errorf("as sets of labels: Neo4j %v, NornicDB %v", want, got)
		}
		l8Run(t, exec, "MATCH (n) WHERE n:A AND n:L RETURN n.num AS num", nil)
	})
}

// ---------------------------------------------------------------- L-H2-010
// The fix c9cb917a9's budget: a canceled statement ends within 100 ms of
// the deadline. 500,000 integers, 20 ms deadline (lead).
func l8CancelOverrun(t *testing.T, exec *StorageExecutor, query string, params map[string]interface{}, deadline time.Duration) (time.Duration, error) {
	ctx, cancel := context.WithTimeout(context.Background(), deadline)
	defer cancel()
	start := time.Now()
	_, err := exec.Execute(ctx, query, params)
	return time.Since(start) - deadline, err
}

func l8Items(n int) []interface{} {
	items := make([]interface{}, n)
	for i := range items {
		items[i] = int64(i)
	}
	return items
}

func l8CancelCase(t *testing.T, exec *StorageExecutor, shape, query string, params map[string]interface{}, assert bool) {
	t.Helper()
	runs := []string{}
	worst := time.Duration(0)
	var lastErr error
	for i := 0; i < 3; i++ {
		overrun, err := l8CancelOverrun(t, exec, query, params, 20*time.Millisecond)
		lastErr = err
		if overrun > worst {
			worst = overrun
		}
		runs = append(runs, fmt.Sprintf("%s err=%v", overrun.Round(time.Millisecond), err))
	}
	data, _ := json.Marshal(map[string]any{"parser": config.GetParserType(), "shape": shape, "overruns": runs})
	t.Logf("L8_CANCEL %s", data)
	if !assert {
		return
	}
	if !errors.Is(lastErr, context.DeadlineExceeded) {
		t.Errorf("%s: canceled statement reported %v, not the deadline", shape, lastErr)
	}
	if worst > statementCancellationBudget {
		t.Errorf("%s: backend kept working %s after the deadline (budget %s)", shape, worst, statementCancellationBudget)
	}
}

func TestL8_H2_010_ListLoopsCancellation(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := l8Exec(t)
		params := map[string]interface{}{"items": l8Items(500000)}
		for shape, query := range map[string]string{
			"comprehension-projection": "RETURN [x IN $items | size(randomUUID())] AS v",
			"comprehension-filter":     "RETURN [x IN $items WHERE size(randomUUID()) > 0] AS v",
			"any":                      "RETURN any(x IN $items WHERE size(randomUUID()) < 0) AS v",
			"all":                      "RETURN all(x IN $items WHERE size(randomUUID()) > 0) AS v",
			"none":                     "RETURN none(x IN $items WHERE size(randomUUID()) < 0) AS v",
			"single":                   "RETURN single(x IN $items WHERE size(randomUUID()) < 0) AS v",
		} {
			l8CancelCase(t, exec, shape, query, params, true)
		}
	})
}

// Control: the fix's reduce over the same list stops within the budget.
func TestL8_H2_010_Control(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := l8Exec(t)
		params := map[string]interface{}{"items": l8Items(500000)}
		l8CancelCase(t, exec, "reduce", "RETURN reduce(a = 0, x IN $items | a + size(randomUUID())) AS v", params, true)
		// Diag: how long the full comprehension takes without a deadline.
		start := time.Now()
		_, err := exec.Execute(context.Background(), "RETURN size([x IN $items | size(randomUUID())]) AS v", params)
		t.Logf("L8_CANCEL_FULL parser=%s comprehension_full=%s err=%v", config.GetParserType(), time.Since(start).Round(time.Millisecond), err)
	})
}

// ---------------------------------------------------------------- L-H2-011
// The fix 10e4a18a9's bound applied with SKIP: SKIP 1 LIMIT 1 needs at most
// 2 adjacency reads.
func TestL8_H2_011_SelfLoopSkipLimit(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		for _, c := range []struct {
			q     string
			max   int
			rows  int
			check bool
		}{
			{"MATCH (p:P)-[:SELF]->(p) RETURN p.id SKIP 1 LIMIT 1", 2, 1, true},
			{"MATCH (p:P)-[:SELF]->(p) RETURN p.id SKIP 0 LIMIT 1", 1, 1, false},
			{"MATCH (p:P)-[:SELF]->(p) RETURN p.id LIMIT 1", 1, 1, false},
			{"MATCH (p:P)-[:SELF]->(p) WITH p SKIP 1 LIMIT 1 RETURN p.id", 2, 1, false},
			{"MATCH (a:P)-[:SELF]->(b) RETURN a.id SKIP 1 LIMIT 1", 2, 1, false},
		} {
			spy := newRepeatedEndpointReadSpy(repeatedEndpointStore(t, true))
			exec := NewStorageExecutor(spy, QueryCachePolicy{MaxEntries: 0, TTL: 0})
			result, err := exec.Execute(context.Background(), c.q, nil)
			rows := -1
			if err == nil {
				rows = len(result.Rows)
			}
			data, _ := json.Marshal(map[string]any{"parser": config.GetParserType(), "query": c.q, "adjacency_reads": spy.calls, "rows": rows, "err": fmt.Sprint(err)})
			t.Logf("L8_SELFLOOP %s", data)
			if c.check {
				if err != nil || rows != c.rows {
					t.Errorf("%q: rows %d err %v", c.q, rows, err)
				}
				if spy.calls > c.max {
					t.Errorf("%q: %d adjacency reads, bound %d", c.q, spy.calls, c.max)
				}
			}
		}
	})
}

// ---------------------------------------------------------------- L-H2-013
func TestL8_H2_013_LongChainsCancellation(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := l8Exec(t)
		or := "UNWIND [1] AS y WITH y WHERE " + strings.TrimSuffix(strings.Repeat("y = 2 OR ", 722), " OR ") + " RETURN y"
		plus := "RETURN " + strings.TrimSuffix(strings.Repeat("$s + ", 723), " + ") + " AS v"
		params := map[string]interface{}{"s": "secret"}
		for i := 0; i < 2; i++ {
			l8CancelCase(t, exec, "comparison-or-chain-in-where", or, nil, i == 1)
			l8CancelCase(t, exec, "parameter-chain", plus, params, i == 1)
		}
		// Diag: the time the same statements take with an already-expired
		// deadline (work before the first cancellation check) and without one.
		for shape, q := range map[string]string{"or": or, "plus": plus} {
			expired, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
			time.Sleep(time.Millisecond)
			start := time.Now()
			_, err := exec.Execute(expired, q, params)
			pre := time.Since(start)
			cancel()
			start = time.Now()
			_, errFull := exec.Execute(context.Background(), q, params)
			full := time.Since(start)
			t.Logf("L8_CANCEL_PHASE parser=%s shape=%s expired_return=%s err=%v full=%s errFull=%v", config.GetParserType(), shape, pre.Round(time.Millisecond), err, full.Round(time.Millisecond), errFull)
		}
	})
}

// Control: the fix's own literal chain of the same length stays in budget.
func TestL8_H2_013_Control(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		l8CancelCase(t, l8Exec(t), "literal-chain", "RETURN "+expressionChainText(722)+" AS v", nil, true)
	})
}
