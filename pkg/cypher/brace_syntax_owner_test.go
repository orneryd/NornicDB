package cypher

import (
	"fmt"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"strings"
	"testing"
)

// The brace corpus labels every brace by hand, independently of the owner:
// «L{ a map literal, «P{ a map projection, «T{ a pattern's property map, «S{ a
// query body and «Q{ a quantifier. The tests below check what each consumer
// of the brace decision actually does with the labelled brace, in every
// trivia spelling: they never pass a kind back into the owner.
var braceOwnerCorpus = []string{
	// Map literals.
	"RETURN «L{k: 1} AS m",
	"WITH 1 AS a RETURN «L{k: «L{x: a}} AS m",
	"UNWIND [«L{k: 1}] AS m RETURN m",
	"RETURN CASE WHEN true THEN «L{k: 1} ELSE «L{k: 2} END AS m",
	"MATCH (n) WHERE n.x IN [«L{k: 1}] RETURN n",
	"RETURN 1 AS x UNION RETURN «L{k: 1} AS x",
	"MATCH (n) SET n += «L{k: 1}",
	"MATCH (n) SET n = «L{k: 1}",
	"RETURN «L{count: 1, match: 2, `a-b`: 3, a$b: 4} AS m",
	"RETURN «L{} AS m",
	// Map projections, including variables spelled like keywords.
	"WITH «L{a: 1} AS n, 2 AS b RETURN n «P{.a, b, c: 3, .*} AS m",
	"WITH «L{a: 1} AS count RETURN count «P{.a} AS m",
	"WITH «L{a: 1} AS limit RETURN limit «P{.a} AS m",
	"WITH «L{a: 1} AS distinct RETURN distinct «P{.a} AS m",
	"WITH «L{a: 1} AS `n-m` RETURN `n-m` «P{.a} AS m",
	"WITH «L{a: 1} AS n RETURN [x IN [1] | n «P{.a}] AS m",
	"WITH «L{a: 1} AS n WHERE (n «P{.a}).a = 1 RETURN n",
	"WITH «L{a: 1} AS n RETURN n «P{.a, `b-c`: 2} AS m",
	// Pattern property maps.
	"MATCH (n «T{k: 1}) RETURN n",
	"MATCH (n:Label «T{k: 1})-[r:R «T{k: 2}]->(m «T{k: 3}) RETURN n",
	"CREATE (n «T{k: 1}) RETURN n",
	"MERGE (n:L «T{k: 1}) RETURN n",
	"MATCH (n) WHERE EXISTS((n)-->(m «T{k: 1})) RETURN n",
	"MATCH p = shortestPath((a «T{k: 1})-[*]-(b)) RETURN p",
	"MATCH (a)-[«T{k: 1}]->(b) RETURN a",
	"MATCH («T{k: 1}) RETURN 1 AS x",
	"MATCH (count «T{k: 1}) RETURN count",
	"MATCH (n:Count «T{k: 1}) RETURN n",
	"RETURN [(a «T{k: 1})-->(b) | a] AS out",
	"WITH [(a «T{k: 1})-->(b) | a] AS out RETURN out",
	"RETURN [(a)-[r «T{k: 1}]->(b) | r] AS out",
	"WITH «L{k: 1} AS n RETURN (n «P{.k}).k AS out",
	// Query bodies under every introducer, nested blocks and query parts.
	"CALL «S{ RETURN 1 AS x } RETURN x",
	"CALL () «S{ CREATE (n «T{k: 1}) } RETURN 1 AS x",
	"WITH 1 AS a CALL (a) «S{ RETURN a AS x } RETURN x",
	"MATCH (n) WHERE EXISTS «S{ MATCH (n)-->(m «T{k: 1}) } RETURN n",
	"MATCH (n) WHERE EXISTS «S{ p = (n)-->() } RETURN n",
	"RETURN COUNT «S{ (n)-->() } AS c",
	"RETURN COLLECT «S{ MATCH (n) RETURN n.k AS k } AS c",
	"RETURN «L{k: COUNT «S{ MATCH (n) RETURN n } } AS m",
	"CALL «S{ CREATE (n:X «T{k: 1}) RETURN n } RETURN n",
	"CALL «S{ MERGE (n:X «T{k: 1}) } RETURN 1 AS x",
	"CALL «S{ CALL «S{ RETURN «L{k: 1} AS m } RETURN m } RETURN m",
	"CYPHER 25 RETURN COUNT «S{ «S{ RETURN 1 AS x } } AS c",
	"CYPHER 25 RETURN EXISTS «S{ «S{ RETURN 1 AS x } } AS c",
	"CYPHER 25 RETURN COLLECT «S{ «S{ RETURN 1 AS x } } AS c",
	"CYPHER 25 CALL () «S{ «S{ RETURN «L{k: 1} AS x } } RETURN x",
	"«S{ RETURN «L{k: 1} AS x } UNION «S{ RETURN 2 AS x }",
	"«S{ RETURN 1 AS x } UNION ALL «S{ RETURN 2 AS x }",
	"RETURN 1 AS x NEXT «S{ RETURN x AS y }",
	// Quantifiers.
	"MATCH ((a)-[:R]->(b))«Q{1,3} RETURN a",
	"MATCH (a)-[:R]->«Q{1,3}(b) RETURN a",
	"MATCH (a)-[r]-«Q{2}(b) RETURN a",
	"MATCH (a)-->«Q{1,}(b) RETURN a",
	"MATCH (a)<--«Q{,3}(b) RETURN a",
}

type braceSlot struct {
	kind  byte
	open  int
	close int
	// base is the template token before the brace (a projection's variable).
	base string
}

// expandBraceTemplate removes the labels, joins tokens with gap instead of the
// template's single spaces, and returns the query with its labelled braces.
func expandBraceTemplate(template, gap string) (string, []braceSlot) {
	var builder strings.Builder
	type label struct {
		kind byte
		at   int
		base string
	}
	var labels []label
	for index := 0; index < len(template); {
		if strings.HasPrefix(template[index:], "«") {
			fields := strings.Fields(template[:index])
			base := ""
			if len(fields) > 0 {
				base = strings.TrimLeft(fields[len(fields)-1], "([,|")
			}
			index += len("«")
			labels = append(labels, label{template[index], builder.Len(), base})
			index++
			continue
		}
		if template[index] == ' ' {
			builder.WriteString(gap)
		} else {
			builder.WriteByte(template[index])
		}
		index++
	}
	query := builder.String()
	slots := make([]braceSlot, 0, len(labels))
	for _, label := range labels {
		close := cyphertext.FindMatchingDelimiter(query, label.at, '{', '}', cyphertext.OpeningDelimiter)
		slots = append(slots, braceSlot{kind: label.kind, open: label.at, close: close, base: label.base})
	}
	return query, slots
}

var braceOwnerGaps = []string{" ", "\t", "\n", " /* { } : , CALL ( ) */ ", " // } { : ,\n", "\r\n/* ' ` */ "}

func TestBraceOwnerCorpusLabels(t *testing.T) {
	for _, template := range braceOwnerCorpus {
		for gapIndex, gap := range braceOwnerGaps {
			query, slots := expandBraceTemplate(template, gap)
			t.Run(fmt.Sprintf("%s/gap%d", template, gapIndex), func(t *testing.T) {
				if err := validateStaticMapKeys(query); err != nil {
					t.Fatalf("valid statement rejected: %v\n%s", err, query)
				}
				context := newMapPatternContext(query)
				forward := newSubqueryBraceScanner(query, 0)
				rewriter := &labelExpressionRewriter{query: query, braceView: originalBraceView(query)}
				for _, slot := range slots {
					if slot.close < 0 {
						t.Fatalf("unbalanced template slot at %d", slot.open)
					}
					body := slot.kind == 'S'
					checks := map[string]bool{
						"precedingSubqueryExpressionKeyword": precedingSubqueryExpressionKeyword(query, slot.open),
						"scanner.at":                         forward.at(slot.open),
						"mapPatternContext.subqueries":       context.subqueries[slot.open],
						"labelExpressionRewriter":            rewriter.opensSubquery(slot.open),
					}
					for consumer, opens := range checks {
						if opens != body {
							t.Errorf("%s: brace %c at %d opens a body=%v\n%s", consumer, slot.kind, slot.open, opens, query)
						}
					}
					pattern := context.insidePatternDelimiter(query, slot.open)
					if pattern != (slot.kind == 'T') {
						t.Errorf("pattern context: brace %c at %d pattern=%v\n%s", slot.kind, slot.open, pattern, query)
					}
					if slot.kind == 'P' {
						// The evaluator reads the projection's window of the statement,
						// where the brace has the statement's role.
						start := strings.LastIndex(query[:slot.open], slot.base)
						expression := query[start : slot.close+1]
						if _, _, ok := rowMapProjectionSplit(originalBraceView(query).window(start, slot.close+1)); !ok {
							t.Errorf("projection evaluator does not read %q", expression)
						}
					}
				}
			})
		}
	}
}

// Each malformed entry replaces one labelled brace's entries; every other
// brace stays valid. The statement must be rejected with that brace's
// diagnostic, so the precedence between pattern and literal messages and the
// projection grammar are checked at each labelled position.
func TestBraceOwnerCorpusMalformedSiblings(t *testing.T) {
	const (
		literal = "map literal keys must be valid symbolic names"
		pattern = "pattern property maps require key-value entries"
		valid   = ""
	)
	entries := []struct {
		text                            string
		literal, projection, properties string
	}{
		{"bad", literal, valid, pattern},
		{"1", literal, literal, pattern},
		{".k", literal, valid, pattern},
		{".*", literal, valid, pattern},
		{"1bad: 1", literal, literal, literal},
		{"k: 1, bad", literal, valid, literal},
		{"k:", literal, literal, literal},
		{"k: 1,", literal, literal, literal},
		{",", literal, literal, pattern},
		{"$p", literal, literal, pattern},
		{"a.b", literal, literal, pattern},
	}
	for _, template := range braceOwnerCorpus {
		for gapIndex, gap := range braceOwnerGaps {
			query, slots := expandBraceTemplate(template, gap)
			for slotIndex, slot := range slots {
				if slot.kind != 'L' && slot.kind != 'P' && slot.kind != 'T' {
					continue
				}
				for _, entry := range entries {
					malformed := query[:slot.open+1] + " " + entry.text + " " + query[slot.close:]
					want := map[byte]string{'L': entry.literal, 'P': entry.projection, 'T': entry.properties}[slot.kind]
					t.Run(fmt.Sprintf("%s/gap%d/slot%d/%s", template, gapIndex, slotIndex, entry.text), func(t *testing.T) {
						err := validateStaticMapKeys(malformed)
						if want == "" {
							if err != nil {
								t.Fatalf("valid entry rejected: %v\n%s", err, malformed)
							}
							return
						}
						if err == nil || !strings.Contains(err.Error(), want) {
							t.Fatalf("error %v, want %q\n%s", err, want, malformed)
						}
						requireSemanticDetail(t, err, "UnexpectedSyntax")
					})
				}
			}
		}
	}
}

// A malformed map inside a query body is rejected wherever the body is,
// including a nested block, and a quantifier never reads as a map.
func TestBraceOwnerBodiesValidateTheirMaps(t *testing.T) {
	for _, template := range braceOwnerCorpus {
		query, slots := expandBraceTemplate(template, " ")
		for _, slot := range slots {
			if slot.kind != 'S' {
				continue
			}
			// The first entry-bearing brace of the body is replaced; if there
			// is none, a malformed literal is projected at the body's end.
			body := query[slot.open+1 : slot.close]
			if !strings.Contains(body, "RETURN") {
				continue
			}
			malformed := query[:slot.open+1] + strings.Replace(body, "RETURN", "WITH 1 AS bad RETURN {bad} AS bad2,", 1) + query[slot.close:]
			t.Run(malformed, func(t *testing.T) {
				err := validateStaticMapKeys(malformed)
				if err == nil || !strings.Contains(err.Error(), "map literal keys must be valid symbolic names") {
					t.Fatalf("malformed map inside a body accepted: %v", err)
				}
			})
		}
	}
}

// Edge positions of the owner: a consumer asking about an earlier brace, a
// brace inside a string, a keyword that is never a projection's variable,
// and braces after arrows or groups that hold no quantifier.
func TestBraceOwnerEdgePositions(t *testing.T) {
	query := "RETURN COUNT { RETURN 1 } AS a, '{' AS b, COUNT { RETURN 2 } AS c"
	first, quoted, last := strings.Index(query, "{"), strings.Index(query, "'{'")+1, strings.LastIndex(query, "{")
	scanner := newSubqueryBraceScanner(query, 0)
	if !scanner.at(last) || !scanner.at(first) || scanner.at(quoted) || scanner.at(-1) {
		t.Fatal("scanner answers differ when asked out of order")
	}
	for _, query := range []string{
		"MATCH (a)-->{x}(b) RETURN a",
		"MATCH (a)--{1 2}(b) RETURN a",
		"MATCH (a)<-{,}(b) RETURN a",
		"MATCH ((a)){1} RETURN a",
		"MATCH ((a) x){1} RETURN a",
	} {
		if err := validateStaticMapKeys(query); err == nil {
			t.Errorf("%s: malformed brace accepted", query)
		}
	}
	for _, query := range []string{
		"MATCH (n) WHERE EXISTS { NODETACH DELETE n } RETURN n",
		"MATCH ((a)<-[:R]-(b)){1,2} RETURN a",
		"WITH {a: 1} AS distinct RETURN distinct{.a} AS v",
		"WITH {a: 1} AS distinct WITH distinct {.a} AS v RETURN v",
	} {
		if err := validateStaticMapKeys(query); err != nil {
			t.Errorf("%s: %v", query, err)
		}
	}
}
