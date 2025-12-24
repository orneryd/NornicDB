package antlr

import (
	"testing"

	"github.com/antlr4-go/antlr/v4"
)

// TestANTLRParserBasicQueries tests that ANTLR can parse basic Cypher queries
func TestANTLRParserBasicQueries(t *testing.T) {
	queries := []struct {
		name  string
		query string
	}{
		// Basic MATCH
		{"simple match", "MATCH (n) RETURN n"},
		{"match with label", "MATCH (n:Person) RETURN n"},
		{"match with properties", "MATCH (n:Person {name: 'Alice'}) RETURN n"},
		{"match with variable", "MATCH (n:Person {name: $name}) RETURN n"},

		// MATCH with WHERE
		{"match where equals", "MATCH (n:Person) WHERE n.name = 'Alice' RETURN n"},
		{"match where gt", "MATCH (n:Person) WHERE n.age > 21 RETURN n"},
		{"match where and", "MATCH (n:Person) WHERE n.age > 21 AND n.city = 'NYC' RETURN n"},
		{"match where or", "MATCH (n:Person) WHERE n.age > 21 OR n.active = true RETURN n"},
		{"match where not", "MATCH (n:Person) WHERE NOT n.active RETURN n"},
		{"match where is null", "MATCH (n:Person) WHERE n.email IS NULL RETURN n"},
		{"match where is not null", "MATCH (n:Person) WHERE n.email IS NOT NULL RETURN n"},
		{"match where in", "MATCH (n:Person) WHERE n.city IN ['NYC', 'LA', 'SF'] RETURN n"},
		{"match where starts with", "MATCH (n:Person) WHERE n.name STARTS WITH 'A' RETURN n"},
		{"match where contains", "MATCH (n:Person) WHERE n.name CONTAINS 'li' RETURN n"},

		// Relationships
		{"match relationship", "MATCH (a)-[r]->(b) RETURN a, b"},
		{"match typed relationship", "MATCH (a)-[r:KNOWS]->(b) RETURN a, b"},
		{"match relationship with props", "MATCH (a)-[r:KNOWS {since: 2020}]->(b) RETURN a, b"},
		{"match variable length", "MATCH (a)-[r*1..3]->(b) RETURN b"},
		{"match variable length unbounded", "MATCH (a)-[r*]->(b) RETURN b"},
		{"match variable length min only", "MATCH (a)-[r*2..]->(b) RETURN b"},
		{"match variable length max only", "MATCH (a)-[r*..5]->(b) RETURN b"},
		{"match reverse relationship", "MATCH (a)<-[r:KNOWS]-(b) RETURN a, b"},
		{"match undirected", "MATCH (a)-[r:KNOWS]-(b) RETURN a, b"},

		// CREATE
		{"create node", "CREATE (n:Person {name: 'Alice'})"},
		{"create with return", "CREATE (n:Person {name: 'Alice'}) RETURN n"},
		{"create relationship", "CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})"},
		{"match create", "MATCH (a:Person {name: 'Alice'}) CREATE (a)-[:KNOWS]->(b:Person {name: 'Bob'})"},

		// MERGE
		{"merge node", "MERGE (n:Person {name: 'Alice'})"},
		{"merge with on create", "MERGE (n:Person {name: 'Alice'}) ON CREATE SET n.created = timestamp()"},
		{"merge with on match", "MERGE (n:Person {name: 'Alice'}) ON MATCH SET n.lastSeen = timestamp()"},
		{"merge relationship", "MATCH (a:Person), (b:Person) MERGE (a)-[:KNOWS]->(b)"},

		// SET
		{"set property", "MATCH (n:Person {name: 'Alice'}) SET n.age = 30"},
		{"set multiple", "MATCH (n:Person {name: 'Alice'}) SET n.age = 30, n.city = 'NYC'"},
		{"set label", "MATCH (n:Person {name: 'Alice'}) SET n:Employee"},

		// DELETE
		{"delete node", "MATCH (n:Person {name: 'Alice'}) DELETE n"},
		{"detach delete", "MATCH (n:Person {name: 'Alice'}) DETACH DELETE n"},

		// RETURN variations
		{"return star", "MATCH (n) RETURN *"},
		{"return alias", "MATCH (n:Person) RETURN n.name AS name"},
		{"return distinct", "MATCH (n:Person) RETURN DISTINCT n.city"},
		{"return limit", "MATCH (n:Person) RETURN n LIMIT 10"},
		{"return skip", "MATCH (n:Person) RETURN n SKIP 5"},
		{"return order by", "MATCH (n:Person) RETURN n ORDER BY n.name"},
		{"return order desc", "MATCH (n:Person) RETURN n ORDER BY n.age DESC"},

		// WITH clause
		{"with simple", "MATCH (n:Person) WITH n.name AS name RETURN name"},
		{"with where", "MATCH (n:Person) WITH n WHERE n.age > 21 RETURN n"},
		{"with aggregation", "MATCH (n:Person) WITH n.city AS city, COUNT(n) AS cnt RETURN city, cnt"},

		// Aggregations
		{"count all", "MATCH (n:Person) RETURN COUNT(*)"},
		{"count nodes", "MATCH (n:Person) RETURN COUNT(n)"},
		{"sum", "MATCH (n:Person) RETURN SUM(n.age)"},
		{"avg", "MATCH (n:Person) RETURN AVG(n.age)"},
		{"min max", "MATCH (n:Person) RETURN MIN(n.age), MAX(n.age)"},
		{"collect", "MATCH (n:Person) RETURN COLLECT(n.name)"},

		// Functions
		{"function upper", "MATCH (n:Person) RETURN toUpper(n.name)"},
		{"function lower", "MATCH (n:Person) RETURN toLower(n.name)"},
		{"function size", "MATCH (n:Person) RETURN SIZE(n.friends)"},
		{"function coalesce", "MATCH (n:Person) RETURN COALESCE(n.nickname, n.name)"},

		// UNWIND
		{"unwind list", "UNWIND [1, 2, 3] AS x RETURN x"},
		{"unwind with match", "MATCH (n:Person) UNWIND n.friends AS friend RETURN friend"},

		// OPTIONAL MATCH
		{"optional match", "MATCH (a:Person) OPTIONAL MATCH (a)-[:KNOWS]->(b) RETURN a, b"},

		// UNION
		{"union", "MATCH (n:Person) RETURN n.name AS name UNION MATCH (c:Company) RETURN c.name AS name"},
		{"union all", "MATCH (n:Person) RETURN n.name UNION ALL MATCH (c:Company) RETURN c.name"},

		// CASE
		{"case when", "MATCH (n:Person) RETURN CASE WHEN n.age < 18 THEN 'minor' ELSE 'adult' END"},
		{"case simple", "MATCH (n:Person) RETURN CASE n.status WHEN 'active' THEN 1 WHEN 'inactive' THEN 0 END"},

		// Complex patterns
		{"multi-hop", "MATCH (a:Person)-[r:KNOWS*2..4]->(b:Person) RETURN a, b"},
		{"path variable", "MATCH path = (a:Person)-[:KNOWS*]->(b:Person) RETURN path"},
		{"multiple patterns", "MATCH (a:Person), (b:Company) WHERE a.employer = b.name RETURN a, b"},

		// CALL
		{"call procedure", "CALL db.labels()"},
		{"call with yield", "CALL db.labels() YIELD label RETURN label"},
	}

	for _, tt := range queries {
		t.Run(tt.name, func(t *testing.T) {
			// Create ANTLR input stream
			input := antlr.NewInputStream(tt.query)

			// Create lexer
			lexer := NewCypherLexer(input)

			// Create token stream
			tokens := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)

			// Create parser
			parser := NewCypherParser(tokens)

			// Collect errors
			errorListener := &testErrorListener{}
			parser.RemoveErrorListeners()
			parser.AddErrorListener(errorListener)

			// Parse
			tree := parser.Script()

			// Check for errors
			if len(errorListener.errors) > 0 {
				t.Errorf("Parse errors: %v", errorListener.errors)
			}

			// Verify we got a valid tree
			if tree == nil {
				t.Error("Got nil parse tree")
			}
		})
	}
}

type testErrorListener struct {
	*antlr.DefaultErrorListener
	errors []string
}

func (e *testErrorListener) SyntaxError(recognizer antlr.Recognizer, offendingSymbol interface{}, line, column int, msg string, ex antlr.RecognitionException) {
	e.errors = append(e.errors, msg)
}

// BenchmarkANTLRParser measures ANTLR parser performance
func BenchmarkANTLRParser(b *testing.B) {
	query := "MATCH (n:Person {name: 'Alice'})-[r:KNOWS*1..3]->(m:Person) WHERE m.age > 21 AND m.city IN ['NYC', 'LA'] WITH m, COUNT(r) AS cnt ORDER BY cnt DESC LIMIT 10 RETURN m.name, m.age, cnt"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		input := antlr.NewInputStream(query)
		lexer := NewCypherLexer(input)
		tokens := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)
		parser := NewCypherParser(tokens)
		parser.RemoveErrorListeners()
		_ = parser.Script()
	}
}

func BenchmarkANTLRValidate(b *testing.B) {
	query := "MATCH (n:Person {name: 'Alice'})-[r:KNOWS*1..3]->(m:Person) WHERE m.age > 21 AND m.city IN ['NYC', 'LA'] WITH m, COUNT(r) AS cnt ORDER BY cnt DESC LIMIT 10 RETURN m.name, m.age, cnt"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := Validate(query); err != nil {
			b.Fatal(err)
		}
	}
}
