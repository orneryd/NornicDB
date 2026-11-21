package server

// Untracked probe (earlier-issues batch 1): report rows L500-L505 over Bolt
// auto-commit, Bolt explicit transaction and HTTP. Assertions state Neo4j 5.26
// behaviour; a failure means the issue exists on the build under test.

import (
	"fmt"
	"strings"
	"testing"
)

func ei1Rows(rows [][]any) string { return fmt.Sprintf("%v", rows) }

// L500
func TestEI1RoutesDottedConstraint(t *testing.T) {
	ei1Routes(t, func(t *testing.T, run func(string, map[string]any) ([][]any, error)) {
		if _, err := run("CREATE CONSTRAINT ei1_dot FOR (n:EIDot) REQUIRE n.`external.id` IS UNIQUE", nil); err != nil {
			t.Fatalf("create constraint: %v", err)
		}
		rows, err := run("SHOW CONSTRAINTS YIELD name, properties RETURN name, properties", nil)
		t.Logf("constraints: %s err=%v", ei1Rows(rows), err)
		if _, err := run("CREATE (:EIDot {`external.id`:'dup'})", nil); err != nil {
			t.Fatalf("first create: %v", err)
		}
		_, err = run("CREATE (:EIDot {`external.id`:'dup'})", nil)
		t.Logf("second create err=%v", err)
		if err == nil || !strings.Contains(err.Error(), "ConstraintValidationFailed") {
			t.Errorf("expected ConstraintValidationFailed, got %v", err)
		}
		rows, _ = run("MATCH (n:EIDot) RETURN count(n) AS c", nil)
		t.Logf("count: %s", ei1Rows(rows))
	})
}

// L501
func TestEI1RoutesBracketLabel(t *testing.T) {
	ei1Routes(t, func(t *testing.T, run func(string, map[string]any) ([][]any, error)) {
		_, err := run("CREATE INDEX ei1_bracket FOR (n:`Document[archived]`) ON (n.id)", nil)
		t.Logf("create index err=%v", err)
		if err != nil {
			t.Errorf("L501 EXISTS: %v", err)
			return
		}
		rows, err := run("SHOW INDEXES YIELD name, labelsOrTypes RETURN name, labelsOrTypes", nil)
		t.Logf("indexes: %s err=%v", ei1Rows(rows), err)
	})
}

// L502, L503, L504 and L505 case 1-3 over the routes.
func TestEI1RoutesExpressions(t *testing.T) {
	type tc struct {
		issue, q, want string // want: rows text, or "ERR:<code substring>"
	}
	cases := []tc{
		{"L502", "WITH [7] AS `a b` RETURN `a b`[0] AS v", "[[7]]"},
		{"L502", "WITH {`a b`:[7]} AS m RETURN m.`a b`[0] AS v", "[[7]]"},
		{"L503", "CREATE (n:EIDel {p:1}) DELETE n RETURN labels(n) AS v", "ERR:EntityNotFound"},
		{"L503", "CREATE (n:EIDel {p:1}) DELETE n RETURN labels(head([n])) AS v", "ERR:EntityNotFound"},
		{"L503", "CREATE (n:EIDel {p:1}) DELETE n RETURN labels((n)) AS v", "ERR:EntityNotFound"},
		{"L503", "CREATE (n:EIDel {p:1}) DELETE n RETURN n['p'] AS v", "ERR:EntityNotFound"},
		{"L503", "CREATE (n:EIDel {p:1}) DELETE n RETURN [x IN [n] | x.p] AS v", "ERR:EntityNotFound"},
		{"L504", "UNWIND [1,2] AS x RETURN {c:`count`(x)} AS v", "[[map[c:2]]]"},
		{"L505", "UNWIND [] AS row MATCH (a)-->(b {key: a.key,}) RETURN b AS out", "ERR:SyntaxError"},
		{"L505", "WITH {name: 'Ada'} AS n, 42 AS score RETURN n{.*, score, active: true} AS out", "[[map[active:true name:Ada score:42]]]"},
		{"L505", "UNWIND [] AS row RETURN {,} AS out", "ERR:SyntaxError"},
	}
	ei1Routes(t, func(t *testing.T, run func(string, map[string]any) ([][]any, error)) {
		for _, c := range cases {
			rows, err := run(c.q, nil)
			got := ei1Rows(rows)
			if err != nil {
				got = "ERR:" + err.Error()
			}
			ok := got == c.want
			if strings.HasPrefix(c.want, "ERR:") {
				ok = err != nil && strings.Contains(err.Error(), strings.TrimPrefix(c.want, "ERR:"))
			}
			t.Logf("%s ok=%v query=%s got=%s", c.issue, ok, c.q, got)
			if !ok {
				t.Errorf("%s EXISTS on this route: %s want %s got %s", c.issue, c.q, c.want, got)
			}
		}
	})
}
