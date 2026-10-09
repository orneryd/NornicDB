package antlr

import (
	"testing"

	"github.com/antlr4-go/antlr/v4"
)

func TestANTLRReadPipelineClauses(t *testing.T) {
	queries := []struct {
		name  string
		query string
	}{
		{"let", "LET x = 1 RETURN x"},
		{"let multiple bindings", "LET x = 1, y = x + 2 RETURN x, y"},
		{"let nested expressions", "LET x = {items: [1, 2]}, y = coalesce($value, x.items[0]) RETURN y"},
		{"filter", "UNWIND [1, 2] AS x FILTER x > 1 RETURN x"},
		{"filter where", "MATCH (n) FILTER WHERE n.active = true RETURN n"},
		{"leading filter", "FILTER true RETURN 1"},
		{"for", "FOR x IN [1, 2] RETURN x"},
		{"for parameter", "FOR x IN $items RETURN x"},
		{"for expression", "WITH [1, 2] AS items FOR x IN items + [3] RETURN x"},
		{"chained pipeline", "FOR x IN [1, 2] LET y = x + 1 FILTER WHERE y > 2 RETURN y"},
		{"before and after with", "LET x = 1 FILTER x > 0 WITH x FOR y IN [x] LET z = y + 1 RETURN z"},
		{"before match", "LET x = 1 MATCH (n) FILTER n.id = x RETURN n"},
		{"after optional match", "OPTIONAL MATCH (n) LET x = n.id FILTER x IS NOT NULL RETURN x"},
		{"before update", "FOR x IN [1, 2] LET y = x + 1 FILTER y > 1 CREATE (n {value: y}) RETURN n"},
		{"after update with", "CREATE (n) WITH n LET x = n FILTER x IS NOT NULL RETURN x"},
		{"procedure", "CALL db.labels() YIELD label LET x = label FILTER x IS NOT NULL RETURN x"},
		{"unscoped call", "LET x = 1 CALL { LET y = x + 1 FILTER y > 1 RETURN y } RETURN y"},
		{"scoped call", "LET x = [1, 2] CALL (x) { FOR y IN x LET z = y + 1 FILTER WHERE z > 2 RETURN z } RETURN z"},
		{"exists subquery", "MATCH (n) WHERE EXISTS { FOR x IN n.items FILTER x > 0 RETURN x } RETURN n"},
		{"count subquery", "RETURN COUNT { FOR x IN [1, 2] LET y = x FILTER y > 1 }"},
		{"collect subquery", "RETURN COLLECT { FOR x IN [1, 2] LET y = x FILTER WHERE y > 1 RETURN y }"},
		{"union", "LET x = 1 RETURN x UNION ALL FOR x IN [2] FILTER x > 0 RETURN x"},
		{"use explain", "EXPLAIN USE neo4j LET x = 1 FOR y IN [x] RETURN y"},
		{"lowercase", "for x in [1, 2] let y = x + 1 filter where y > 2 return y"},
		{"comments", "FOR /* source */ x IN [1, 2] LET /* binding */ y = x FILTER /* predicate */ WHERE y > 1 RETURN y"},
	}
	for _, tt := range queries {
		t.Run(tt.name, func(t *testing.T) {
			result, err := Parse(tt.query)
			if err != nil {
				t.Errorf("Parse(%q): %v", tt.query, err)
			} else if result == nil || result.Tree == nil {
				t.Error("Parse returned no tree")
			}
			if err := Validate(tt.query); err != nil {
				t.Errorf("Validate(%q): %v", tt.query, err)
			}
		})
	}
}

func TestANTLRReadPipelineKeywordIdentifiers(t *testing.T) {
	queries := []string{
		"MATCH (let:Let {filter: 1, for: 2}) RETURN let.filter AS for",
		"WITH 1 AS let, 2 AS filter, 3 AS for RETURN let + filter + for",
		"LET let = 1, filter = let + 1, for = filter + 1 RETURN let, filter, for",
		"FOR for IN [1, 2] LET let = for FILTER let > 1 RETURN for, let",
		"FOR filter IN [true, false] FILTER filter RETURN filter",
		"LET `let` = 1 RETURN `let`",
		"RETURN filter([1, 2])",
		"CREATE INDEX idx FOR (n:Let) ON (n.let)",
	}
	for _, query := range queries {
		t.Run(query, func(t *testing.T) {
			if _, err := Parse(query); err != nil {
				t.Errorf("Parse: %v", err)
			}
			if err := Validate(query); err != nil {
				t.Errorf("Validate: %v", err)
			}
		})
	}
}

func TestANTLRReadPipelineInvalidSyntax(t *testing.T) {
	queries := []string{
		"LET RETURN 1",
		"LET x RETURN x",
		"LET x = RETURN x",
		"LET x = 1, RETURN x",
		"LET n.x = 1 RETURN n",
		"FILTER RETURN 1",
		"FILTER WHERE ) RETURN 1",
		"FOR IN [1] RETURN 1",
		"FOR x [1] RETURN x",
		"FOR x IN RETURN x",
	}
	for _, query := range queries {
		t.Run(query, func(t *testing.T) {
			if _, err := Parse(query); err == nil {
				t.Error("Parse accepted invalid syntax")
			}
			if err := Validate(query); err == nil {
				t.Error("Validate accepted invalid syntax")
			}
		})
	}
}

func TestANTLRReadPipelineContexts(t *testing.T) {
	result := parseScriptForTest(t, "FOR x IN [1, 2] LET y = x + 1, z = y FILTER WHERE z > 1 RETURN z")
	listener := &readPipelineListener{BaseCypherParserListener: &BaseCypherParserListener{}}
	antlr.ParseTreeWalkerDefault.Walk(listener, result.Tree)
	if len(listener.lets) != 1 || len(listener.filters) != 1 || len(listener.fors) != 1 {
		t.Fatalf("clause counts: LET=%d FILTER=%d FOR=%d", len(listener.lets), len(listener.filters), len(listener.fors))
	}
	items := listener.lets[0].AllLetItem()
	if len(items) != 2 {
		t.Fatalf("LET bindings: got %d, want 2", len(items))
	}
	for i, want := range []struct{ symbol, expression string }{{"y", "x+1"}, {"z", "y"}} {
		if got := items[i].Symbol().GetText(); got != want.symbol {
			t.Errorf("binding %d symbol = %q, want %q", i, got, want.symbol)
		}
		if got := items[i].Expression().GetText(); got != want.expression {
			t.Errorf("binding %d expression = %q, want %q", i, got, want.expression)
		}
	}
	if ctx := listener.filters[0]; ctx.WHERE() == nil || ctx.Expression().GetText() != "z>1" {
		t.Errorf("unexpected FILTER context: %s", ctx.GetText())
	}
	if ctx := listener.fors[0]; ctx.Symbol().GetText() != "x" || ctx.Expression().GetText() != "[1,2]" {
		t.Errorf("unexpected FOR context: %s", ctx.GetText())
	}

	result = parseScriptForTest(t, "FILTER true RETURN 1")
	listener = &readPipelineListener{BaseCypherParserListener: &BaseCypherParserListener{}}
	antlr.ParseTreeWalkerDefault.Walk(listener, result.Tree)
	if len(listener.filters) != 1 || listener.filters[0].WHERE() != nil || listener.filters[0].Expression().GetText() != "true" {
		t.Error("FILTER without WHERE did not preserve its expression")
	}
}

type readPipelineListener struct {
	*BaseCypherParserListener
	lets    []*LetStContext
	filters []*FilterStContext
	fors    []*ForStContext
}

func (l *readPipelineListener) EnterLetSt(ctx *LetStContext) {
	l.lets = append(l.lets, ctx)
}

func (l *readPipelineListener) EnterFilterSt(ctx *FilterStContext) {
	l.filters = append(l.filters, ctx)
}

func (l *readPipelineListener) EnterForSt(ctx *ForStContext) {
	l.fors = append(l.fors, ctx)
}

func TestANTLRPreambleAcceptedDirectly(t *testing.T) {
	for _, query := range []string{
		"CYPHER 5 RETURN 1",
		"CYPHER 25 RETURN 1",
		"CYPHER 25 runtime=slotted RETURN 1",
		"CYPHER 5.0 RETURN 1",
		"EXPLAIN CYPHER 25 RETURN 1",
		"CYPHER RETURN 1",
	} {
		if err := Validate(query); err != nil {
			t.Errorf("Validate(%q): %v", query, err)
		}
	}
}
