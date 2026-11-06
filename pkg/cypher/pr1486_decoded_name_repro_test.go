package cypher

import (
	"context"
	"testing"
)

// Reproduction of PR #1486 (NOR-46) on the release/v1.4.4 head.
// Original failing cases: a quoted relationship type A|B must survive
// COUNT/EXISTS subqueries, and names differing only by whitespace must stay distinct.
func TestPR1486_QuotedRelationshipTypeSurvivesCountExists(t *testing.T) {
	exec, _ := newTestExecutor(t)
	ctx := context.Background()
	if _, err := exec.Execute(ctx, "CREATE (:ReviewStart)-[:`A|B`]->()", nil); err != nil {
		t.Fatal(err)
	}
	res, err := exec.Execute(ctx, "MATCH (n:ReviewStart) RETURN COUNT { (n)-[:`A|B`]->() } AS c, EXISTS { (n)-[:`A|B`]->() } AS e", nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Rows) != 1 || res.Rows[0][0] != int64(1) || res.Rows[0][1] != true {
		t.Fatalf("want c=1,e=true; got %v", res.Rows)
	}
}

func TestPR1486_WhitespaceNamesStayDistinct(t *testing.T) {
	exec, _ := newTestExecutor(t)
	ctx := context.Background()
	for _, q := range []string{
		"CREATE (:`a b`)-[:` `]->(:X)",
		"CREATE (:`a  b`)-[:`  `]->(:X)",
	} {
		if _, err := exec.Execute(ctx, q, nil); err != nil {
			t.Fatal(err)
		}
	}
	for _, c := range []struct {
		q    string
		want int64
	}{
		{"MATCH (n:`a b`) RETURN count(n)", 1},
		{"MATCH (n:`a  b`) RETURN count(n)", 1},
		{"MATCH ()-[r:` `]->() RETURN count(r)", 1},
		{"MATCH ()-[r:`  `]->() RETURN count(r)", 1},
	} {
		res, err := exec.Execute(ctx, c.q, nil)
		if err != nil {
			t.Fatalf("%s: %v", c.q, err)
		}
		if res.Rows[0][0] != c.want {
			t.Errorf("%s: got %v want %d", c.q, res.Rows[0][0], c.want)
		}
	}
}
