package cypher

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"os"
	"strings"
	"testing"
)

type relationshipOwnerOracleStep struct {
	Query  string                 `json:"query"`
	Output []string               `json:"output"`
	Params map[string]interface{} `json:"params,omitempty"`
}

// Run with tnrun --ref neo-ref and NORNICDB_GENERATE_RELATIONSHIP_ORACLE=1.
// Each added statement executes against Neo4j 5.26.30; rows and status codes
// are recorded directly. ORACLE is the complete fixture, while CYPHER and
// EXPECTED retain readable reproductions, including intentionally failing ones.
func TestGenerateRelationshipOwnerOracle(t *testing.T) {
	if os.Getenv("NORNICDB_GENERATE_RELATIONSHIP_ORACLE") != "1" {
		t.Skip("explicit reference generation only")
	}
	var steps []relationshipOwnerOracleStep
	add := func(q string, output ...string) {
		steps = append(steps, relationshipOwnerOracleStep{Query: q, Output: output})
	}
	add("CREATE (:R73Degree)")
	types := []string{"`A|B`", "`A``B`", "`A:中|B`", "`D]'Z`", "`D\"Z]-[`", "`日本語`", "A", "B"}
	for _, typ := range types {
		add("MATCH (a:R73Degree) CREATE (a)-[:" + typ + "]->(), ()-[:" + typ + "]->(a)")
	}
	for _, typ := range types {
		for _, v := range []string{"a", "rélation", "`a a`", "`a``a`"} {
			prefix := "MATCH (" + v + ":R73Degree) RETURN "
			add(prefix+"COUNT { ("+v+")-[:"+typ+"]->() } AS c", "c", "1")
			add(prefix+"COUNT { ()-[:"+typ+"]->("+v+") } AS c", "c", "1")
			add(prefix+"EXISTS { ("+v+")-[:"+typ+"]->() } AS c", "c", "TRUE")
			add(prefix+"COUNT { MATCH ("+v+")-[:"+typ+"]->(b WHERE true) } AS c", "c", "1")
			add(prefix+"COUNT { ("+v+")-[`r``r`:"+typ+"]->() } + 1 AS c", "c", "2")
			add("MATCH ("+v+":R73Degree)-[:"+typ+"]->() RETURN count(*) AS c", "c", "1")
		}
		for _, v := range []string{"r", "rélation", "`r r`", "`r``r`", "where"} {
			add("MATCH (a:R73Degree) RETURN COUNT { (a)-["+v+":"+typ+"]->() } AS c", "c", "1")
			add("MATCH ((a:R73Degree)-["+v+":"+typ+"]->(b)){1} RETURN count(*) AS c", "c", "1")
			add("MATCH ((a:R73Degree)-["+v+":"+typ+" WHERE true]->(b)){1} RETURN count(*) AS c", "c", "1")
			add("MATCH (a:R73Degree)-[:" + typ + "]->(b) RETURN (a) - [" + v + ":" + typ + "] -> (b) AS value")
			add("MATCH (a:R73Degree) RETURN [(a) - [" + v + ":" + typ + "] -> (b) | type(" + v + ")] AS xs")
		}
	}
	add("CREATE (:R73Range)-[:R]->()-[:S]->()")
	add(`CREATE (:R73SlashA)-[:$('R\\')]->(:R73SlashB)-[:S]->(:R73SlashC)`)
	add(`MATCH (:R73SlashA)-[:$('R\\')]->()-->(:R73SlashMissing) RETURN count(*) AS c`, "c", "0")
	add(`MATCH (:R73SlashA)-[:$('R\\')]->()-[:S]->(:R73SlashMissing) RETURN count(*) AS c`, "c", "0")
	values := []interface{}{"", "NOPE", nil, int64(7), float64(7.5), true, []interface{}{}, []interface{}{"", int64(7)}, map[string]interface{}{"odd": "value"}}
	for _, value := range values {
		for _, form := range []string{"$($t)", "$any($t)"} {
			for _, prefix := range []string{"MATCH", "PROFILE MATCH", "OPTIONAL MATCH"} {
				steps = append(steps, relationshipOwnerOracleStep{Query: prefix + " (:R73Range)-[r:" + form + "*1..2]->() RETURN count(r) AS c", Output: []string{"c", "2"}, Params: map[string]interface{}{"t": value}})
			}
			for _, kind := range []string{"COUNT", "EXISTS", "PROFILE COUNT"} {
				prefix, expression, want := "MATCH", kind, "2"
				if kind == "EXISTS" {
					want = "TRUE"
				}
				if kind == "PROFILE COUNT" {
					prefix, expression = "PROFILE MATCH", "COUNT"
				}
				steps = append(steps, relationshipOwnerOracleStep{Query: prefix + " (a:R73Range) RETURN " + expression + " { (a)-[r:" + form + "*1..2]->() } AS c", Output: []string{"c", want}, Params: map[string]interface{}{"t": value}})
			}
		}
	}
	add("DROP INDEX index_f7700477 IF EXISTS")
	for i, v := range []string{"r", "rélation", "关系", "`r r`", "`r``r`"} {
		for j, arrow := range []string{"-", "->", "<-"} {
			pattern := "()-[" + v + "]-()"
			if arrow == "->" {
				pattern = "()-[" + v + "]->()"
			}
			if arrow == "<-" {
				pattern = "()<-[" + v + "]-()"
			}
			name := fmt.Sprintf("r73_lookup_%d_%d", i, j)
			add("CREATE LOOKUP INDEX " + name + " FOR " + pattern + " ON EACH type(" + v + ")")
			add("DROP INDEX " + name)
		}
	}
	add("CREATE (:R73Keyword)-[:KeywordType]->()")
	for _, keyword := range strings.Fields("is where as any all none single not and or xor in contains starts ends null true false count exists case when") {
		for _, name := range []string{keyword, strings.ToUpper(keyword)} {
			reference := "`" + name + "`"
			for _, variable := range []string{name, reference} {
				prefix := "MATCH (:R73Keyword)-[" + variable
				add(prefix + "]->() RETURN type(" + reference + ") AS t")
				add(prefix + ":KeywordType]->() RETURN type(" + reference + ") AS t")
				add(prefix + " IS KeywordType]->() RETURN type(" + reference + ") AS t")
				add(prefix + " WHERE type(" + reference + ") = 'KeywordType']->() RETURN type(" + reference + ") AS t")
				add(prefix + "*1..1]->() RETURN size(" + reference + ") AS n")
				add("MATCH (a:R73Keyword) RETURN COUNT { (a)-[" + variable + "]->() } AS n")
				add("MATCH (a:R73Keyword) RETURN EXISTS { (a)-[" + variable + "]->() } AS b")
				indexName := fmt.Sprintf("r73_keyword_%d", len(steps))
				add("CREATE LOOKUP INDEX " + indexName + " FOR ()-[" + variable + "]-() ON EACH type(" + reference + ")")
				add("DROP INDEX " + indexName)
			}
		}
	}
	for _, variable := range []string{"is", "where", "true", "false", "null"} {
		reference := "`" + variable + "`"
		nested := "(a)-[" + variable + "]->() WHERE type(" + reference + ") = 'KeywordType'"
		add("MATCH (a:R73Keyword)-[outer WHERE EXISTS { " + nested + " }]->() RETURN type(outer) AS t")
		add("MATCH (a:R73Keyword)-[outer {missing: COUNT { " + nested + " }}]->() RETURN count(*) AS n")
	}
	for _, variable := range []string{"true", "false", "null"} {
		add("MATCH (:R73Keyword)-[" + variable + "]->() RETURN " + variable + " AS literal, type(`" + variable + "`) AS t")
		add("MATCH (:R73Keyword)-[" + variable + "]->() RETURN `" + variable + "`{.*} AS props")
	}
	// An IS marker followed by the type IS must remain anonymous; only an
	// additional token makes the second IS a marker after a variable named is.
	add("CREATE (:R73IS)-[:IS]->()")
	for _, declaration := range []string{"IS IS", "is IS", "IS is", "is is", "IS IS*1..1", "IS `IS`"} {
		add("MATCH (:R73IS)-[" + declaration + "]->() RETURN count(*) AS n")
	}
	add("MATCH (:R73IS)-[is IS IS]->() RETURN type(`is`) AS t")
	fulltextVariables := []string{"r", "rélation", "关系", "`r r`", "`r``r`", "`true`", "`is`"}
	fulltextVariables = append(fulltextVariables, strings.Fields("is where as any all none single not and or xor in contains starts ends null true false count exists case when")...)
	for i, variable := range fulltextVariables {
		for j, types := range []string{"`A|B`", "`A``B`", "`关系`", "`A|B`|`C:D`"} {
			name := fmt.Sprintf("r73_fulltext_%d_%d", i, j)
			add("CREATE FULLTEXT INDEX " + name + " FOR ()-[" + variable + ":" + types + "]-() ON EACH [" + variable + ".text]")
			add("SHOW INDEXES YIELD name, labelsOrTypes WHERE name = '" + name + "' RETURN labelsOrTypes")
			add("DROP INDEX " + name)
		}
	}
	// Whitespace inside quotes belongs to the name, not to FOR-clause gaps.
	for i, variable := range fulltextVariables {
		for j, types := range []string{"` `", "` R `"} {
			name := fmt.Sprintf("r73_fulltext_space_%d_%d", i, j)
			add("CREATE FULLTEXT INDEX " + name + " FOR ()-[" + variable + ":" + types + "]-() ON EACH [" + variable + ".text]")
			add("SHOW INDEXES YIELD name, labelsOrTypes WHERE name = '" + name + "' RETURN labelsOrTypes")
			add("DROP INDEX " + name)
		}
	}
	for _, punctuation := range []string{"&", "!", "%", "("} {
		for _, literal := range []string{"`A" + punctuation + "B`", "`A``" + punctuation + "B`"} {
			for _, types := range []string{literal + "|R", "R|" + literal} {
				add("MATCH (n:R73SyntaxA:R73SyntaxB)-[:" + types + "]->() RETURN count(*) AS n")
			}
		}
	}
	for _, types := range []string{"R&S", "!R", "%", "(R|S)"} {
		add("MATCH (n:R73SyntaxA:R73SyntaxB)-[:" + types + "]->() RETURN count(*) AS n")
	}
	for i, variable := range fulltextVariables {
		name := fmt.Sprintf("r73_fulltext_bracket_%d", i)
		add("CREATE FULLTEXT INDEX " + name + " FOR ()-[" + variable + ":`A[B]`]-() ON EACH [" + variable + ".text]")
		add("SHOW INDEXES YIELD name, labelsOrTypes WHERE name = '" + name + "' RETURN labelsOrTypes")
		add("DROP INDEX " + name)
	}
	for i, label := range []string{"`A[B]`", "`A]B`", "`A->B`", "`A|B`"} {
		name := fmt.Sprintf("r73_fulltext_node_control_%d", i)
		add("CREATE FULLTEXT INDEX " + name + " FOR (n:" + label + ") ON EACH [n.text]")
		add("SHOW INDEXES YIELD name, labelsOrTypes WHERE name = '" + name + "' RETURN labelsOrTypes")
		add("DROP INDEX " + name)
	}
	for _, variable := range strings.Fields("is where as any all none single not and or xor in contains starts ends null true false count exists case when") {
		add("MATCH (:R73Keyword)-[" + variable + "]->() RETURN " + variable + " IS NOT NULL AS p")
		add("MATCH (:R73Keyword)-[" + variable + "]->() RETURN type(" + variable + ") AS t")
	}
	var script, expected strings.Builder
	ctx := context.Background()
	// Decoded spellings are data, not dynamic-token or range syntax. Cover
	// every path consumer as well as the ordinary/degree MATCH readers.
	for index, name := range []string{"$(R)", "$any(R)", "$all(R)", "$(R|S)", "$(中`文)", "$(R)*1..2"} {
		quoted := "`" + strings.ReplaceAll(name, "`", "``") + "`"
		start, end := fmt.Sprintf("R73DeltaStart%d", index), fmt.Sprintf("R73DeltaEnd%d", index)
		add("CREATE (:" + start + ")-[:" + quoted + "]->(:" + end + ")")
		add("MATCH p = ANY SHORTEST (a:"+start+")-[:"+quoted+"]->(b:"+end+") RETURN length(p) AS n", "n", "1")
		add("MATCH (a:"+start+"), (b:"+end+") MATCH p = shortestPath((a)-[:"+quoted+"*]->(b)) RETURN length(p) AS n", "n", "1")
		add("MATCH (a:"+start+") RETURN COUNT { (a)-[:"+quoted+"]->() } AS n", "n", "1")
		add("MATCH (a:"+start+") RETURN EXISTS { (a)-[:"+quoted+"]->() } AS n", "n", "TRUE")
		add("MATCH (:"+start+")-[r:"+quoted+"]->(:"+end+") RETURN type(r) AS t", "t", cypherShellPlainValue(name))
	}
	uri := os.Getenv("NORNICDB_NEO4J_REFERENCE_URI")
	if uri == "" {
		t.Fatal("reference generation requires tnrun --ref neo-ref")
	}
	driver, err := neo4j.NewDriverWithContext(uri, neo4j.BasicAuth("neo4j", "unused", ""))
	if err != nil {
		t.Fatal(err)
	}
	defer driver.Close(ctx)
	session := driver.NewSession(ctx, neo4j.SessionConfig{})
	defer session.Close(ctx)
	wipe, err := session.Run(ctx, "MATCH (n) DETACH DELETE n", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = wipe.Consume(ctx); err != nil {
		t.Fatal(err)
	}
	for i := range steps {
		step := &steps[i]
		result, err := session.Run(ctx, step.Query, step.Params)
		var lines []string
		if err == nil {
			keys, keyErr := result.Keys()
			if keyErr != nil {
				t.Fatal(keyErr)
			}
			records, readErr := result.Collect(ctx)
			err = readErr
			if err == nil && len(records) > 0 {
				lines = append(lines, strings.Join(keys, ", "))
				for _, record := range records {
					values := make([]string, len(record.Values))
					for j, value := range record.Values {
						values[j] = cypherShellPlainValue(value)
					}
					lines = append(lines, strings.Join(values, ", "))
				}
			}
		}
		if err != nil {
			neoErr, ok := err.(*neo4j.Neo4jError)
			if !ok {
				t.Fatal(err)
			}
			lines = []string{"error: " + neoErr.Code}
			t.Logf("reference statement %d params=%v => %s", i+1, step.Params, neoErr.Code)
		}
		step.Output = lines
	}
	for i, step := range steps {
		if step.Params != nil {
			fmt.Fprintf(&script, ":param t => %s\n", relationshipReferenceLiteral(step.Params["t"]))
		}
		fmt.Fprintf(&script, "RETURN %d AS stmt;\n%s;\n", i+1, step.Query)
		fmt.Fprintf(&expected, "stmt\n%d\n", i+1)
		for _, line := range step.Output {
			fmt.Fprintln(&expected, line)
		}
	}
	base, err := os.ReadFile("testdata/relationship_pattern_oracle.json")
	if err != nil {
		t.Fatal(err)
	}
	var original []relationshipOwnerOracleStep
	if err = json.Unmarshal(base, &original); err != nil {
		t.Fatal(err)
	}
	// The original stream is kept intact; regeneration is repeatable.
	if len(original) > 415 {
		original = original[:415]
	}
	pinned, err := json.MarshalIndent(append(original, steps...), "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	for name, bytes := range map[string][]byte{"CYPHER": []byte(script.String()), "EXPECTED": []byte(expected.String()), "ORACLE": pinned} {
		fmt.Printf("RELATIONSHIP_%s %s\n", name, base64.StdEncoding.EncodeToString(bytes))
	}
	t.Logf("415 original + %d generated reference statements", len(steps))
}

func relationshipReferenceLiteral(value interface{}) string {
	switch v := value.(type) {
	case nil:
		return "null"
	case string:
		return cypherStringLiteral(v)
	case int64:
		return fmt.Sprint(v)
	case float64:
		return fmt.Sprint(v)
	case bool:
		return fmt.Sprint(v)
	case []interface{}:
		parts := make([]string, len(v))
		for i, x := range v {
			parts[i] = relationshipReferenceLiteral(x)
		}
		return "[" + strings.Join(parts, ",") + "]"
	case map[string]interface{}:
		return "{odd:'value'}"
	}
	panic("unsupported reference fixture")
}

func BenchmarkRelationshipOwnerShortcuts(b *testing.B) {
	for name, body := range map[string]string{"degree_plain": "(a)-[:R]->()", "degree_quoted": "(a)-[:`A|B`]->()", "degree_escaped": "(a)-[:`A``B`]->()"} {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				parseBoundDegreeShapeText(body)
			}
		})
	}
	b.Run("lookup_unicode", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			parseCreateLookupIndex("CREATE LOOKUP INDEX ix FOR ()-[rélation]-() ON EACH type(rélation)")
		}
	})
	for name, query := range map[string]string{"plain_rewrite": "MATCH (a)-[:R]->(b) RETURN b", "dynamic_range": "MATCH (a)-[r:$($t)*1..2]->(b) RETURN b", "inline_where": "MATCH (a)-[r:R WHERE r.k=1]->(b) RETURN b"} {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			params := map[string]interface{}{"t": "NOPE"}
			for i := 0; i < b.N; i++ {
				if _, _, err := desugarLabelExpressions(query, params, false); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
