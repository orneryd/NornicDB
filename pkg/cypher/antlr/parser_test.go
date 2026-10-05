package antlr

import (
	"fmt"
	"reflect"
	"sort"
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
		{"yield alias arithmetic", "WITH 3 AS yield RETURN 1 + yield AS v"},
		{"yield node binding", "MATCH (yield:Person) RETURN yield.name AS name"},
		{"yield procedure binding", "CALL db.labels() YIELD label AS yield RETURN yield"},

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
		{"shell param arrow", ":param key => 'value'"},
		{"shell param map", ":param {a: 1, b: 1 + 1}"},
		{"shell params alias", ":params"},
		{"shell use", ":use system"},
		{"begin transaction", "BEGIN TRANSACTION"},
		{"commit transaction", "COMMIT TRANSACTION"},
		{"rollback transaction", "ROLLBACK TRANSACTION"},
		// Schema type constraints (Neo4j 5)
		{"constraint typed zoned datetime", "CREATE CONSTRAINT event_ts_type IF NOT EXISTS FOR (e:Event) REQUIRE e.ts IS :: ZONED DATETIME"},
		{"constraint typed local datetime", "CREATE CONSTRAINT meeting_start_type IF NOT EXISTS FOR (m:Meeting) REQUIRE m.start IS TYPED LOCAL DATETIME"},
		{"constraint node key", "CREATE CONSTRAINT user_key IF NOT EXISTS FOR (u:User) REQUIRE (u.username, u.domain) IS NODE KEY"},
		{"constraint require options backticks", "CREATE CONSTRAINT `uq order id` IF NOT EXISTS FOR (`n`:`Order`) REQUIRE `n`.`id` IS UNIQUE OPTIONS {indexProvider: 'range-1.0'}"},
		{"create with embedding return", "CREATE (n:Doc {id:'d1', content:'hello world'}) WITH EMBEDDING RETURN count(n) AS c"},
		{"create with embedding no return", "CREATE (n:Doc {id:'d2', content:'hello world'}) WITH EMBEDDING"},
		{"disallowed policy", "CREATE CONSTRAINT forbidden FOR (n:Person)-[r:FORBIDDEN]->(m) REQUIRE DISALLOWED"},
		{"constraint block", "CREATE CONSTRAINT contract FOR (n:Person) REQUIRE { n.id IS UNIQUE n.age IS :: INTEGER n.status IN ['active'] NOT EXISTS { (n)-[:BAD]->() } }"},
		{"show contracts", "SHOW CONSTRAINT CONTRACTS"},
		{"policy words as names", "MATCH (n:DISALLOWED) RETURN n.CONTRACTS AS DISALLOWED"},
		{"negative node label", "MATCH (n:!Other) RETURN n"},
		{"label wildcard predicate", "MATCH (n) WHERE n IS % RETURN n"},
		{"label expression precedence", "MATCH (n IS (A|B)&!C) RETURN n"},
		{"negative relationship type", "MATCH (a)-[:!R]->(b) RETURN b"},
		{"relationship quantifier", "MATCH (a)-[r:!S]->{1,2}(b) RETURN count(*)"},
		{"relationship plus quantifier", "MATCH (a)-[:R]->+(b) RETURN b"},
		{"escaped backtick label", "MATCH (n:P) SET n:`Quoted``Label` RETURN labels(n)"},
		{"bare index property", "CREATE INDEX okidx FOR (n:L) ON n.q"},
		{"replace database", "CREATE OR REPLACE DATABASE db"},
		{"composite aliases", "CREATE COMPOSITE DATABASE comp ALIAS first FOR DATABASE db1 ALIAS second FOR DATABASE db2"},
		{"show users", "SHOW USERS YIELD user RETURN user ORDER BY user"},
		{"show current user", "SHOW CURRENT USER"},
		{"admin words as identifiers", "MATCH (n:USER) RETURN n.CURRENT AS REPLACE"},
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

func parseScriptForTest(t *testing.T, query string) *ParseResult {
	t.Helper()

	result, err := Parse(query)
	if err != nil {
		t.Fatalf("Parse(%q) failed: %v", query, err)
	}
	if result == nil || result.Tree == nil {
		t.Fatalf("Parse(%q) returned nil tree", query)
	}
	return result
}

func parseExpressionForTest(t *testing.T, expr string) IExpressionContext {
	t.Helper()

	input := antlr.NewInputStream(expr)
	lexer := NewCypherLexer(input)
	tokens := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)
	parser := NewCypherParser(tokens)

	errorListener := &testErrorListener{}
	parser.RemoveErrorListeners()
	parser.AddErrorListener(errorListener)
	lexer.RemoveErrorListeners()
	lexer.AddErrorListener(errorListener)

	tree := parser.Expression()
	if len(errorListener.errors) > 0 {
		t.Fatalf("Expression parse errors for %q: %v", expr, errorListener.errors)
	}
	if tree == nil {
		t.Fatalf("Expression parse returned nil tree for %q", expr)
	}
	return tree
}

func newParserForSnippet(snippet string) *CypherParser {
	input := antlr.NewInputStream(snippet)
	lexer := NewCypherLexer(input)
	tokens := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)
	return NewCypherParser(tokens)
}

func parseNodePatternForTest(t *testing.T, snippet string) INodePatternContext {
	t.Helper()
	ctx := newParserForSnippet(snippet).NodePattern()
	if ctx == nil {
		t.Fatalf("NodePattern parse returned nil for %q", snippet)
	}
	return ctx
}

func parseRelationshipPatternForTest(t *testing.T, snippet string) IRelationshipPatternContext {
	t.Helper()
	ctx := newParserForSnippet(snippet).RelationshipPattern()
	if ctx == nil {
		t.Fatalf("RelationshipPattern parse returned nil for %q", snippet)
	}
	return ctx
}

func parsePropertyExpressionForTest(t *testing.T, snippet string) IPropertyExpressionContext {
	t.Helper()
	ctx := newParserForSnippet(snippet).PropertyExpression()
	if ctx == nil {
		t.Fatalf("PropertyExpression parse returned nil for %q", snippet)
	}
	return ctx
}

func parseProjectionItemForTest(t *testing.T, snippet string) IProjectionItemContext {
	t.Helper()
	ctx := newParserForSnippet(snippet).ProjectionItem()
	if ctx == nil {
		t.Fatalf("ProjectionItem parse returned nil for %q", snippet)
	}
	return ctx
}

func parseInvocationNameForTest(t *testing.T, snippet string) IInvocationNameContext {
	t.Helper()
	ctx := newParserForSnippet(snippet).InvocationName()
	if ctx == nil {
		t.Fatalf("InvocationName parse returned nil for %q", snippet)
	}
	return ctx
}

func parseOrderStForTest(t *testing.T, snippet string) IOrderStContext {
	t.Helper()
	ctx := newParserForSnippet(snippet).OrderSt()
	if ctx == nil {
		t.Fatalf("OrderSt parse returned nil for %q", snippet)
	}
	return ctx
}

func parseExpressionChainForTest(t *testing.T, snippet string) IExpressionChainContext {
	t.Helper()
	ctx := newParserForSnippet(snippet).ExpressionChain()
	if ctx == nil {
		t.Fatalf("ExpressionChain parse returned nil for %q", snippet)
	}
	return ctx
}

func parseCreateStForTest(t *testing.T, snippet string) ICreateStContext {
	t.Helper()
	ctx := newParserForSnippet(snippet).CreateSt()
	if ctx == nil {
		t.Fatalf("CreateSt parse returned nil for %q", snippet)
	}
	return ctx
}

func parseDeleteStForTest(t *testing.T, snippet string) IDeleteStContext {
	t.Helper()
	ctx := newParserForSnippet(snippet).DeleteSt()
	if ctx == nil {
		t.Fatalf("DeleteSt parse returned nil for %q", snippet)
	}
	return ctx
}

func parseRemoveStForTest(t *testing.T, snippet string) IRemoveStContext {
	t.Helper()
	ctx := newParserForSnippet(snippet).RemoveSt()
	if ctx == nil {
		t.Fatalf("RemoveSt parse returned nil for %q", snippet)
	}
	return ctx
}

func parseStandaloneCallForTest(t *testing.T, snippet string) IStandaloneCallContext {
	t.Helper()
	ctx := newParserForSnippet(snippet).StandaloneCall()
	if ctx == nil {
		t.Fatalf("StandaloneCall parse returned nil for %q", snippet)
	}
	return ctx
}

func parseReturnStForTest(t *testing.T, snippet string) IReturnStContext {
	t.Helper()
	ctx := newParserForSnippet(snippet).ReturnSt()
	if ctx == nil {
		t.Fatalf("ReturnSt parse returned nil for %q", snippet)
	}
	return ctx
}

func parseUnwindStForTest(t *testing.T, snippet string) IUnwindStContext {
	t.Helper()
	ctx := newParserForSnippet(snippet).UnwindSt()
	if ctx == nil {
		t.Fatalf("UnwindSt parse returned nil for %q", snippet)
	}
	return ctx
}

func parseLimitStForTest(t *testing.T, snippet string) ILimitStContext {
	t.Helper()
	ctx := newParserForSnippet(snippet).LimitSt()
	if ctx == nil {
		t.Fatalf("LimitSt parse returned nil for %q", snippet)
	}
	return ctx
}

func parseSkipStForTest(t *testing.T, snippet string) ISkipStContext {
	t.Helper()
	ctx := newParserForSnippet(snippet).SkipSt()
	if ctx == nil {
		t.Fatalf("SkipSt parse returned nil for %q", snippet)
	}
	return ctx
}

func parseQueryCallStForTest(t *testing.T, snippet string) IQueryCallStContext {
	t.Helper()
	ctx := newParserForSnippet(snippet).QueryCallSt()
	if ctx == nil {
		t.Fatalf("QueryCallSt parse returned nil for %q", snippet)
	}
	return ctx
}

func atomicFromExpr(t *testing.T, expr string) IAtomicExpressionContext {
	t.Helper()
	parsed := parseExpressionForTest(t, expr)
	xor := parsed.AllXorExpression()[0]
	and := xor.AllAndExpression()[0]
	not := and.AllNotExpression()[0]
	comp := not.ComparisonExpression()
	add := comp.AllAddSubExpression()[0]
	mult := add.AllMultDivExpression()[0]
	power := mult.AllPowerExpression()[0]
	unary := power.AllUnaryAddSubExpression()[0]
	atomic := unary.AtomicExpression()
	if atomic == nil {
		t.Fatalf("no atomic expression found in %q", expr)
	}
	return atomic
}

func atomFromExpr(t *testing.T, expr string) IAtomContext {
	t.Helper()
	atomic := atomicFromExpr(t, expr)
	prop := atomic.PropertyOrLabelExpression()
	if prop == nil || prop.PropertyExpression() == nil || prop.PropertyExpression().Atom() == nil {
		t.Fatalf("no atom found in %q", expr)
	}
	return prop.PropertyExpression().Atom()
}

func functionInvocationFromExpr(t *testing.T, expr string) IFunctionInvocationContext {
	t.Helper()
	atom := atomFromExpr(t, expr)
	fn := atom.FunctionInvocation()
	if fn == nil {
		t.Fatalf("no function invocation found in %q", expr)
	}
	return fn
}

func literalFromExpr(t *testing.T, expr string) ILiteralContext {
	t.Helper()
	atom := atomFromExpr(t, expr)
	lit := atom.Literal()
	if lit == nil {
		t.Fatalf("no literal found in %q", expr)
	}
	return lit
}

func addSubFromExpr(t *testing.T, expr string) IAddSubExpressionContext {
	t.Helper()
	return parseExpressionForTest(t, expr).AllXorExpression()[0].AllAndExpression()[0].AllNotExpression()[0].ComparisonExpression().AllAddSubExpression()[0]
}

func multDivFromExpr(t *testing.T, expr string) IMultDivExpressionContext {
	t.Helper()
	return addSubFromExpr(t, expr).AllMultDivExpression()[0]
}

func powerFromExpr(t *testing.T, expr string) IPowerExpressionContext {
	t.Helper()
	return multDivFromExpr(t, expr).AllPowerExpression()[0]
}

func unaryFromExpr(t *testing.T, expr string) IUnaryAddSubExpressionContext {
	t.Helper()
	return powerFromExpr(t, expr).AllUnaryAddSubExpression()[0]
}

func comparisonSignFromExpr(t *testing.T, expr string) IComparisonSignsContext {
	t.Helper()
	comp := parseExpressionForTest(t, expr).AllXorExpression()[0].AllAndExpression()[0].AllNotExpression()[0].ComparisonExpression()
	signs := comp.AllComparisonSigns()
	if len(signs) == 0 {
		t.Fatalf("no comparison sign found in %q", expr)
	}
	return signs[0]
}

func TestANTLRParserAdvancedQueries(t *testing.T) {
	queries := []struct {
		name  string
		query string
	}{
		{"explain db procedure", "EXPLAIN CALL db.labels() YIELD label RETURN label"},
		{"profile shortest path", "PROFILE MATCH p = shortestPath((a:Person)-[:KNOWS*]->(b:Person)) RETURN p"},
		{"all shortest paths", "MATCH p = allShortestPaths((a:Person)-[:KNOWS*]->(b:Person)) RETURN p"},
		{"show procedures", "SHOW PROCEDURES"},
		{"show functions", "SHOW FUNCTIONS"},
		{"show constraints", "SHOW CONSTRAINTS"},
		{"show databases", "SHOW DATABASES"},
		{"create index", "CREATE INDEX person_name IF NOT EXISTS FOR (n:Person) ON (n.name)"},
		{"drop index", "DROP INDEX person_name IF EXISTS"},
		{"create fulltext index", "CREATE FULLTEXT INDEX doc_search IF NOT EXISTS FOR (n:Doc) ON EACH [n.title, n.content]"},
		{"create vector index", "CREATE VECTOR INDEX doc_embedding IF NOT EXISTS FOR (n:Doc) ON (n.embedding) OPTIONS {indexConfig: {dimensions: 3}}"},
		{"create constraint node key", "CREATE CONSTRAINT user_key IF NOT EXISTS FOR (u:User) REQUIRE (u.username, u.domain) IS NODE KEY"},
		{"create constraint options backticks", "CREATE CONSTRAINT `uq order id` IF NOT EXISTS FOR (`n`:`Order`) REQUIRE `n`.`id` IS UNIQUE OPTIONS {indexProvider: 'range-1.0'}"},
		{"drop constraint", "DROP CONSTRAINT person_id IF EXISTS"},
		{"create with embedding return", "CREATE (n:Doc {id:'d1', content:'hello world'}) WITH EMBEDDING RETURN count(n) AS c"},
		{"create with embedding no return", "CREATE (n:Doc {id:'d2', content:'hello world'}) WITH EMBEDDING"},
		{"call subquery", "CALL { MATCH (n:Person) RETURN n LIMIT 1 } RETURN n"},
		{"exists subquery", "MATCH (n:Person) WHERE EXISTS { MATCH (n)-[:KNOWS]->(m:Person) WHERE m.age > 18 } RETURN n"},
		{"count subquery", "MATCH (n:Person) RETURN COUNT { MATCH (n)-[:KNOWS]->(m:Person) } AS cnt"},
		{"list comprehension", "MATCH (n:Person) RETURN [x IN [1, 2, 3] WHERE x > 1 | x * 2] AS xs"},
		{"pattern comprehension", "MATCH (n:Person) RETURN [(n)-[:KNOWS]->(m:Person) | m.name] AS names"},
		{"reduce expression", "MATCH (n:Person) RETURN REDUCE(total = 0, x IN [1, 2, 3] | total + x) AS total"},
		{"foreach clause", "FOREACH (x IN [1, 2, 3] | CREATE (:Number {value: x}))"},
		{"call yield order skip limit", "CALL db.labels() YIELD label WITH label ORDER BY label SKIP 1 LIMIT 2 RETURN label"},
		{"union with call", "CALL db.labels() YIELD label RETURN label UNION ALL MATCH (n:Person) RETURN n.name AS label"},
	}

	for _, tt := range queries {
		t.Run(tt.name, func(t *testing.T) {
			parseScriptForTest(t, tt.query)
			if err := Validate(tt.query); err != nil {
				t.Fatalf("Validate(%q) failed: %v", tt.query, err)
			}
		})
	}
}

func TestANTLRParseAndValidateErrors(t *testing.T) {
	if _, err := Parse("   "); err == nil {
		t.Fatal("Parse should reject empty queries")
	}
	if err := Validate("   "); err == nil {
		t.Fatal("Validate should reject empty queries")
	}
	if err := Validate("MATCH (n RETURN n"); err == nil {
		t.Fatal("Validate should reject malformed queries")
	}
	if err := Validate("MATCH (n {name: 'Ångstrom'}) RETURN n"); err != nil {
		t.Fatalf("Validate should accept non-ASCII queries: %v", err)
	}
}

func TestANTLRClauseExtraction(t *testing.T) {
	query := "MATCH (n:Person)-[:KNOWS]->(m:Person) WHERE n.age >= 21 WITH n, m.name AS friendName ORDER BY friendName SKIP 2 LIMIT 5 RETURN n.name AS name, friendName"
	parseResult := parseScriptForTest(t, query)

	info := ExtractClauses(query, parseResult)
	if info.MatchPattern == "" || info.MatchFull == "" {
		t.Fatalf("expected MATCH info, got %+v", info)
	}
	if info.WhereCondition != "n.age >= 21" {
		t.Fatalf("unexpected where condition: %q", info.WhereCondition)
	}
	if info.WithItems != "n, m.name AS friendName" {
		t.Fatalf("unexpected WITH items: %q", info.WithItems)
	}
	if info.OrderByItems != "friendName" {
		t.Fatalf("unexpected ORDER BY items: %q", info.OrderByItems)
	}
	if info.SkipValue != "2" {
		t.Fatalf("unexpected SKIP value: %q", info.SkipValue)
	}
	if info.LimitValue != "5" {
		t.Fatalf("unexpected LIMIT value: %q", info.LimitValue)
	}
	if info.ReturnItems != "n.name AS name, friendName" {
		t.Fatalf("unexpected RETURN items: %q", info.ReturnItems)
	}
}

func TestANTLRClauseExtraction_MergeAndCall(t *testing.T) {
	t.Run("merge actions and variables", func(t *testing.T) {
		query := "MERGE (n:Person {id: $id}) ON CREATE SET n.created = timestamp() ON MATCH SET n.lastSeen = timestamp()"
		parseResult := parseScriptForTest(t, query)

		info := ExtractClauses(query, parseResult)
		if info.MergePattern == "" || len(info.MergePatterns) != 1 {
			t.Fatalf("expected merge pattern, got %+v", info)
		}
		if info.OnCreateSet != "n.created = timestamp()" {
			t.Fatalf("unexpected ON CREATE SET: %q", info.OnCreateSet)
		}
		if info.OnMatchSet != "n.lastSeen = timestamp()" {
			t.Fatalf("unexpected ON MATCH SET: %q", info.OnMatchSet)
		}
		if len(info.Variables) == 0 {
			t.Fatalf("expected extracted variables, got %+v", info)
		}
	})

	t.Run("standalone call and unwind", func(t *testing.T) {
		query := "UNWIND [1, 2, 3] AS x CALL db.labels()"
		parseResult := parseScriptForTest(t, query)

		info := ExtractClauses(query, parseResult)
		if info.UnwindExpr != "[1, 2, 3]" || info.UnwindAs != "x" {
			t.Fatalf("unexpected unwind extraction: %+v", info)
		}
		if info.CallProcedure != "db.labels()" {
			t.Fatalf("unexpected call extraction: %q", info.CallProcedure)
		}
	})
}

func TestANTLRClauseExtraction_MutationsAndStandaloneClauses(t *testing.T) {
	t.Run("create clause", func(t *testing.T) {
		info := ExtractClauses("CREATE (n:Person {name: 'Alice'})", parseScriptForTest(t, "CREATE (n:Person {name: 'Alice'})"))
		if info.CreatePattern != "(n:Person {name: 'Alice'})" {
			t.Fatalf("unexpected create pattern: %q", info.CreatePattern)
		}
		if info.CreateFull != "CREATE (n:Person {name: 'Alice'})" {
			t.Fatalf("unexpected create full text: %q", info.CreateFull)
		}
	})

	t.Run("delete and remove clauses", func(t *testing.T) {
		info := ExtractClauses(
			"MATCH (n:Person) REMOVE n.legacy DETACH DELETE n",
			parseScriptForTest(t, "MATCH (n:Person) REMOVE n.legacy DETACH DELETE n"),
		)
		if info.RemoveItems != "n.legacy" {
			t.Fatalf("unexpected remove items: %q", info.RemoveItems)
		}
		if !info.DetachDelete || info.DeleteTargets != "n" {
			t.Fatalf("unexpected delete extraction: %+v", info)
		}
	})

	t.Run("standalone call extraction", func(t *testing.T) {
		info := ExtractClauses("CALL db.labels()", parseScriptForTest(t, "CALL db.labels()"))
		if info.CallProcedure != "db.labels()" {
			t.Fatalf("unexpected standalone call extraction: %q", info.CallProcedure)
		}
	})
}

func TestANTLRQueryAnalyzer(t *testing.T) {
	analyzer := NewQueryAnalyzer()

	t.Run("db procedure explain is read only", func(t *testing.T) {
		query := "EXPLAIN CALL db.labels() YIELD label RETURN label"
		info := analyzer.Analyze(query, parseScriptForTest(t, query))
		if !info.HasExplain || !info.HasCall || !info.CallIsDbProcedure {
			t.Fatalf("expected explain db call flags, got %+v", info)
		}
		if !info.IsReadOnly || info.IsWriteQuery {
			t.Fatalf("expected read-only db procedure call, got %+v", info)
		}
	})

	t.Run("write query is compound", func(t *testing.T) {
		query := "MATCH (n:Person) SET n.active = true RETURN n"
		info := analyzer.Analyze(query, parseScriptForTest(t, query))
		if !info.HasMatch || !info.HasSet || !info.HasReturn {
			t.Fatalf("expected match/set/return flags, got %+v", info)
		}
		if !info.IsWriteQuery || info.IsReadOnly || !info.IsCompoundQuery {
			t.Fatalf("expected compound write query, got %+v", info)
		}
		if info.FirstClause != ClauseMatch {
			t.Fatalf("expected first clause MATCH, got %v", info.FirstClause)
		}
	})

	t.Run("show schema query", func(t *testing.T) {
		query := "SHOW CONSTRAINTS"
		info := analyzer.Analyze(query, parseScriptForTest(t, query))
		if !info.HasShow || !info.HasSchema || !info.IsSchemaQuery {
			t.Fatalf("expected schema/show flags, got %+v", info)
		}
		if info.FirstClause != ClauseShow {
			t.Fatalf("expected first clause SHOW, got %v", info.FirstClause)
		}
	})

	t.Run("shortest path collects labels", func(t *testing.T) {
		query := "MATCH p = shortestPath((a:Person)-[:KNOWS]->(b:Person)) RETURN p"
		info := analyzer.Analyze(query, parseScriptForTest(t, query))
		if !info.HasShortestPath || !info.HasMatch || !info.HasReturn {
			t.Fatalf("expected shortest path read query, got %+v", info)
		}
		if len(info.Labels) == 0 {
			t.Fatalf("expected collected labels, got %+v", info)
		}
	})

	cached := analyzer.Analyze("SHOW CONSTRAINTS", parseScriptForTest(t, "SHOW CONSTRAINTS"))
	if cached != analyzer.Analyze("SHOW CONSTRAINTS", nil) {
		t.Fatal("expected analyzer to return cached QueryInfo for repeated query")
	}

	analyzer.ClearCache()
	refreshed := analyzer.Analyze("SHOW CONSTRAINTS", parseScriptForTest(t, "SHOW CONSTRAINTS"))
	if refreshed == cached {
		t.Fatal("expected ClearCache to drop cached pointer")
	}
}

func TestANTLRQueryAnalyzer_WriteAndRoutingVariants(t *testing.T) {
	analyzer := NewQueryAnalyzer()

	tests := []struct {
		name  string
		query string
		check func(t *testing.T, info *QueryInfo)
	}{
		{
			name:  "create query",
			query: "CREATE (n:Person {name: 'Alice'})",
			check: func(t *testing.T, info *QueryInfo) {
				if !info.HasCreate || info.FirstClause != ClauseCreate || !info.IsWriteQuery {
					t.Fatalf("unexpected create analysis: %+v", info)
				}
			},
		},
		{
			name:  "merge query",
			query: "MERGE (n:Person {id: 1})",
			check: func(t *testing.T, info *QueryInfo) {
				if !info.HasMerge || info.MergeCount != 1 || info.FirstClause != ClauseMerge {
					t.Fatalf("unexpected merge analysis: %+v", info)
				}
			},
		},
		{
			name:  "detach delete query",
			query: "MATCH (n:Person) DETACH DELETE n",
			check: func(t *testing.T, info *QueryInfo) {
				if !info.HasDelete || !info.HasDetachDelete || info.FirstClause != ClauseMatch {
					t.Fatalf("unexpected delete analysis: %+v", info)
				}
			},
		},
		{
			name:  "remove query",
			query: "MATCH (n:Person) REMOVE n.legacy",
			check: func(t *testing.T, info *QueryInfo) {
				if !info.HasRemove || !info.IsWriteQuery {
					t.Fatalf("unexpected remove analysis: %+v", info)
				}
			},
		},
		{
			name:  "with query",
			query: "MATCH (n:Person) WITH n RETURN n",
			check: func(t *testing.T, info *QueryInfo) {
				if !info.HasWith || !info.IsReadOnly {
					t.Fatalf("unexpected WITH analysis: %+v", info)
				}
			},
		},
		{
			name:  "unwind query",
			query: "UNWIND [1, 2] AS x RETURN x",
			check: func(t *testing.T, info *QueryInfo) {
				if !info.HasUnwind || info.FirstClause != ClauseUnwind || !info.IsReadOnly {
					t.Fatalf("unexpected UNWIND analysis: %+v", info)
				}
			},
		},
		{
			name:  "standalone call query",
			query: "CALL db.labels()",
			check: func(t *testing.T, info *QueryInfo) {
				if !info.HasCall || info.FirstClause != ClauseCall {
					t.Fatalf("unexpected CALL analysis: %+v", info)
				}
			},
		},
		{
			name:  "call subquery query",
			query: "CALL { MATCH (n:Person) RETURN n } RETURN n",
			check: func(t *testing.T, info *QueryInfo) {
				if !info.HasCall || !info.HasReturn {
					t.Fatalf("unexpected CALL subquery analysis: %+v", info)
				}
			},
		},
		{
			name:  "schema create query",
			query: "CREATE INDEX person_name IF NOT EXISTS FOR (n:Person) ON (n.name)",
			check: func(t *testing.T, info *QueryInfo) {
				if !info.HasSchema || !info.IsSchemaQuery || info.FirstClause != ClauseCreate {
					t.Fatalf("unexpected schema create analysis: %+v", info)
				}
			},
		},
		{
			name:  "schema drop query",
			query: "DROP INDEX person_name IF EXISTS",
			check: func(t *testing.T, info *QueryInfo) {
				if !info.HasSchema || info.FirstClause != ClauseDrop {
					t.Fatalf("unexpected schema drop analysis: %+v", info)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			info := analyzer.Analyze(tt.query, parseScriptForTest(t, tt.query))
			tt.check(t, info)
		})
	}
}

func TestANTLRExpressionEvaluator(t *testing.T) {
	evaluator := NewExpressionEvaluator(
		map[string]interface{}{"age": int64(21), "city": "NYC"},
		map[string]interface{}{"tags": []interface{}{"neo4j", "go"}},
	)
	evaluator.SetRow(map[string]interface{}{
		"n": map[string]interface{}{
			"age":    int64(21),
			"name":   "Alice",
			"email":  nil,
			"city":   "NYC",
			"active": true,
		},
		"flag": true,
	})

	tests := []struct {
		name string
		expr string
		want bool
	}{
		{"and or precedence", "n.age = 21 AND n.city = 'NYC' OR n.name = 'Bob'", true},
		{"xor", "n.age = 21 XOR n.city = 'LA'", true},
		{"not", "NOT n.city = 'LA'", true},
		{"is null", "n.email IS NULL", true},
		{"is not null", "n.name IS NOT NULL", true},
		{"contains", "n.name CONTAINS 'lic'", true},
		{"starts with", "n.name STARTS WITH 'Al'", true},
		{"ends with", "n.name ENDS WITH 'ce'", true},
		{"in list from variables", "'go' IN tags", true},
		{"parenthesized", "(n.age = 21 AND n.city = 'NYC')", true},
		{"params", "n.age >= $age AND n.city = $city", true},
		{"truthy fallback", "flag", true},
		{"arithmetic", "1 + 2 * 3 = 7", true},
		{"modulo", "10 % 3 = 1", true},
		{"unary minus", "-5 < 0", true},
		{"chain comparison", "1 < 2 < 3", true},
		{"false branch", "n.city = 'LA'", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr := parseExpressionForTest(t, tt.expr)
			got := evaluator.EvaluateWhere(expr)
			if got != tt.want {
				t.Fatalf("%s => %v, want %v", tt.expr, got, tt.want)
			}
		})
	}

	if !evaluator.EvaluateWhere(nil) {
		t.Fatal("nil WHERE expression should evaluate to true")
	}
}

func TestANTLRASCIICharStream(t *testing.T) {
	if !isASCII("MATCH (n) RETURN n") {
		t.Fatal("expected ASCII query to be detected as ASCII")
	}
	if isASCII("MATCH (n {name: 'Ångstrom'}) RETURN n") {
		t.Fatal("expected non-ASCII query to be detected")
	}

	s := newASCIICharStream("MATCH")
	if s.LA(1) != int('M') || s.LA(2) != int('A') {
		t.Fatalf("unexpected lookahead values: %d %d", s.LA(1), s.LA(2))
	}
	if s.LA(0) != 0 {
		t.Fatalf("LA(0) should be 0, got %d", s.LA(0))
	}
	if s.LA(-1) != antlr.TokenEOF {
		t.Fatalf("LA(-1) from start should be EOF, got %d", s.LA(-1))
	}

	s.Consume()
	if s.Index() != 1 {
		t.Fatalf("expected index 1 after consume, got %d", s.Index())
	}
	if s.LA(-1) != int('M') {
		t.Fatalf("expected prior character after consume, got %d", s.LA(-1))
	}
	s.Seek(2)
	if s.Index() != 2 || s.LA(1) != int('T') {
		t.Fatalf("unexpected seek state: idx=%d la=%d", s.Index(), s.LA(1))
	}
	if s.Size() != 5 {
		t.Fatalf("unexpected size: %d", s.Size())
	}
	if got := s.GetText(1, 3); got != "ATC" {
		t.Fatalf("unexpected GetText slice: %q", got)
	}
	if got := s.GetText(-2, 1); got != "MA" {
		t.Fatalf("unexpected clamped GetText slice: %q", got)
	}
	if got := s.GetText(10, 12); got != "" {
		t.Fatalf("expected empty out-of-range GetText, got %q", got)
	}
	if got := s.GetTextFromInterval(antlr.Interval{Start: 0, Stop: 4}); got != "MATCH" {
		t.Fatalf("unexpected interval text: %q", got)
	}
	if s.Mark() != -1 {
		t.Fatal("expected Mark() to return -1")
	}
	s.Release(1)

	defer func() {
		if recover() == nil {
			t.Fatal("expected Consume at EOF to panic")
		}
	}()
	eof := newASCIICharStream("")
	eof.Consume()
}

func TestANTLRExpressionEvaluator_BuiltinsAndHelpers(t *testing.T) {
	e := NewExpressionEvaluator(nil, nil)

	FunctionLookup = func(name string) (interface{}, bool) {
		if name == "custom.add" {
			return func(a int64, b float64) float64 { return float64(a) + b }, true
		}
		if name == "custom.concat" {
			return func(args []interface{}) interface{} { return args[0].(string) + args[1].(string) }, true
		}
		return nil, false
	}
	defer func() { FunctionLookup = nil }()

	cases := []struct {
		name string
		expr string
		want interface{}
	}{
		{"map literal", "{name: 'Alice', age: 21}", map[string]interface{}{"name": "Alice", "age": int64(21)}},
		{"list literal", "[1, 2, 3]", []interface{}{int64(1), int64(2), int64(3)}},
		{"plugin typed conversion", "custom.add(2, 3)", float64(5)},
		{"plugin slice args", "custom.concat('neo', '4j')", "neo4j"},
		{"builtin tostring", "toString(42)", "42"},
		{"builtin tointeger", "toInteger('42')", int64(42)},
		{"builtin tofloat", "toFloat('4.5')", float64(4.5)},
		{"builtin toboolean", "toBoolean('true')", true},
		{"builtin size map", "size({a: 1, b: 2})", int64(2)},
		{"builtin reverse", "reverse([1, 2, 3])", []interface{}{int64(3), int64(2), int64(1)}},
		{"builtin range negative", "range(3, 1, -1)", []interface{}{int64(3), int64(2), int64(1)}},
		{"builtin replace", "replace('abcabc', 'a', 'z')", "zbczbc"},
		{"builtin substring", "substring('abcdef', 2, 3)", "cde"},
		{"builtin left", "left('abcdef', 2)", "ab"},
		{"builtin right", "right('abcdef', 2)", "ef"},
		{"builtin split", "split('a,b,c', ',')", []interface{}{"a", "b", "c"}},
		{"apoc coll sum", "apoc.coll.sum([1, 2, 3])", int64(6)},
		{"apoc coll avg", "apoc.coll.avg([1, 2, 3])", float64(2)},
		{"apoc coll min", "apoc.coll.min([3, 2, 4])", int64(2)},
		{"apoc coll max", "apoc.coll.max([3, 2, 4])", int64(4)},
		{"apoc coll reverse", "apoc.coll.reverse([1, 2])", []interface{}{int64(2), int64(1)}},
		{"graph type", "type({_type: 'KNOWS'})", "KNOWS"},
		{"graph id", "id({_nodeId: 'n-1'})", "n-1"},
		{"graph labels", "labels({_labels: ['Person']})", []interface{}{"Person"}},
		{"graph keys", "keys({_nodeId: 'n-1', name: 'Alice', age: 1})", []interface{}{"name", "age"}},
		{"graph properties", "properties({_nodeId: 'n-1', name: 'Alice'})", map[string]interface{}{"name": "Alice"}},
		{"exists builtin", "exists('value')", true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := e.Evaluate(parseExpressionForTest(t, tc.expr))
			if gotSlice, ok := got.([]interface{}); ok {
				if wantSlice, ok := tc.want.([]interface{}); ok {
					got = sortedInterfaces(gotSlice)
					tc.want = sortedInterfaces(wantSlice)
				}
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("%s => %#v, want %#v", tc.expr, got, tc.want)
			}
		})
	}

	if got := e.evaluateBuiltInFunction("timestamp", nil); got == nil {
		t.Fatal("timestamp() should return a value")
	}
	if got := e.evaluateBuiltInFunction("date", nil); got == nil {
		t.Fatal("date() should return a value")
	}
	if got := e.evaluateBuiltInFunction("datetime", nil); got == nil {
		t.Fatal("datetime() should return a value")
	}
	if got := e.evaluateBuiltInFunction("unknown.func", []interface{}{1}); got != nil {
		t.Fatalf("unknown builtin should return nil, got %#v", got)
	}

	if got := e.toInt64("9"); got != 9 {
		t.Fatalf("toInt64 string => %d", got)
	}
	if got := e.toBool(int64(0)); got {
		t.Fatal("toBool(0) should be false")
	}
	if got := e.size(map[string]interface{}{"a": 1}); got != 1 {
		t.Fatalf("size(map) => %d", got)
	}
	if !e.isTruthy("x") || e.isTruthy("") {
		t.Fatal("unexpected truthiness behavior for strings")
	}
	if !e.valuesEqual(int64(2), float64(2)) || e.valuesEqual("2", int64(2)) {
		t.Fatal("unexpected equality behavior")
	}
	if e.compareValues("b", "a") <= 0 || e.compareValues(int64(1), float64(2)) >= 0 {
		t.Fatal("unexpected compareValues behavior")
	}
	if e.toFloat64(float32(3.5)) != 3.5 {
		t.Fatal("unexpected toFloat64 conversion")
	}
	if v, ok := e.toNumericOk("x"); ok || v != 0 {
		t.Fatal("toNumericOk should fail for strings")
	}
	if maybeInt64(2.0) != int64(2) {
		t.Fatal("maybeInt64 should collapse whole numbers")
	}
	if maybeInt64(2.5) != 2.5 {
		t.Fatal("maybeInt64 should preserve fractional values")
	}

	if got := convertToType(int64(3), reflect.TypeOf(float64(0))); got.Interface().(float64) != 3 {
		t.Fatalf("unexpected convertToType int64->float64: %#v", got.Interface())
	}
	if got := convertToType(nil, reflect.TypeOf("")); got.Interface().(string) != "" {
		t.Fatalf("unexpected convertToType nil zero value: %#v", got.Interface())
	}
}

func TestANTLRExtractionHelpers(t *testing.T) {
	e := NewExpressionEvaluator(map[string]interface{}{"id": int64(7)}, nil)

	varName, props := ExtractPropertyAccess(parsePropertyExpressionForTest(t, "n.name.first"))
	if varName != "n" || !reflect.DeepEqual(props, []string{"name", "first"}) {
		t.Fatalf("unexpected property access extraction: %q %#v", varName, props)
	}

	nodeInfo := ExtractNodePattern(parseNodePatternForTest(t, "(n:Person:Employee {id: $id, active: true})"), e)
	if nodeInfo.Variable != "n" || !reflect.DeepEqual(nodeInfo.Labels, []string{"Person", "Employee"}) {
		t.Fatalf("unexpected node pattern: %+v", nodeInfo)
	}
	if nodeInfo.Properties["id"] != int64(7) || nodeInfo.Properties["active"] != true {
		t.Fatalf("unexpected node properties: %+v", nodeInfo.Properties)
	}

	relInfo := ExtractRelationshipPattern(parseRelationshipPatternForTest(t, "<-[r:KNOWS {since: 2020}]-"), e)
	if relInfo.Variable != "r" || relInfo.Type != "KNOWS" || relInfo.IsForward {
		t.Fatalf("unexpected relationship pattern: %+v", relInfo)
	}
	if relInfo.Properties["since"] != int64(2020) {
		t.Fatalf("unexpected relationship properties: %+v", relInfo.Properties)
	}

	proj := ExtractProjectionItem(parseProjectionItemForTest(t, "COUNT(DISTINCT n) AS total"))
	if !proj.IsAggregation || proj.AggFunc != "COUNT" || !proj.IsDistinct || proj.Alias != "total" {
		t.Fatalf("unexpected projection info: %+v", proj)
	}

	aggCountAll := ExtractAggregation(parseExpressionForTest(t, "COUNT(*)"))
	if !aggCountAll.IsAggregation || !aggCountAll.IsCountAll || aggCountAll.FuncName != "COUNT" {
		t.Fatalf("unexpected count(*) aggregation: %+v", aggCountAll)
	}

	aggCollect := ExtractAggregation(parseExpressionForTest(t, "COLLECT(n.name)"))
	if !aggCollect.IsAggregation || aggCollect.FuncName != "COLLECT" || len(aggCollect.Args) != 1 {
		t.Fatalf("unexpected collect aggregation: %+v", aggCollect)
	}

	proc := ExtractProcedureName(parseInvocationNameForTest(t, "db.labels"))
	if proc.Name != "db.labels" || !proc.IsDbProc || proc.Namespace != "db" {
		t.Fatalf("unexpected procedure info: %+v", proc)
	}

	showParse := parseScriptForTest(t, "SHOW FUNCTIONS")
	if got := ExtractShowCommandType(showParse.Tree.Query(0).ShowCommand()); got != ShowFunctions {
		t.Fatalf("unexpected show command type: %v", got)
	}

	sorts := ExtractSortItems(parseOrderStForTest(t, "ORDER BY n.name DESC, n.age ASC"))
	if len(sorts) != 2 || !sorts[0].IsDesc || sorts[1].IsDesc {
		t.Fatalf("unexpected sort extraction: %+v", sorts)
	}

	chainVars := ExtractVariablesFromExpressionChain(parseExpressionChainForTest(t, "n, m.name, count(x)"))
	if !reflect.DeepEqual(chainVars, []string{"n", "m", "x"}) {
		t.Fatalf("unexpected expression chain variables: %#v", chainVars)
	}
	if got := ExtractVariableFromExpression(parseExpressionForTest(t, "n")); got != "n" {
		t.Fatalf("unexpected variable extraction: %q", got)
	}
	if got := ExtractVariableFromExpression(parseExpressionForTest(t, "n + 1")); got != "" {
		t.Fatalf("compound expression should not extract single variable, got %q", got)
	}
}

func TestANTLRAggregationHelpers(t *testing.T) {
	e := NewExpressionEvaluator(nil, nil)
	rows := []map[string]interface{}{
		{"group": "a", "score": int64(1), "name": "alice"},
		{"group": "a", "score": int64(2), "name": "bob"},
		{"group": "b", "score": int64(3), "name": "carol"},
	}

	grouped := GroupRows(rows, []IExpressionContext{parseExpressionForTest(t, "group")}, e)
	if len(grouped) != 2 || len(grouped[0]) != 2 || len(grouped[1]) != 1 {
		t.Fatalf("unexpected grouped rows: %#v", grouped)
	}

	if got := ComputeAggregation("COUNT", nil, true, grouped[0], e); got != int64(2) {
		t.Fatalf("COUNT(*) => %#v", got)
	}
	args := []IExpressionContext{parseExpressionForTest(t, "score")}
	if got := ComputeAggregation("COUNT", args, false, grouped[0], e); got != int64(2) {
		t.Fatalf("COUNT(score) => %#v", got)
	}
	if got := ComputeAggregation("SUM", args, false, grouped[0], e); got != int64(3) {
		t.Fatalf("SUM(score) => %#v", got)
	}
	if got := ComputeAggregation("AVG", args, false, grouped[0], e); got != float64(1.5) {
		t.Fatalf("AVG(score) => %#v", got)
	}
	if got := ComputeAggregation("MIN", args, false, grouped[0], e); got != int64(1) {
		t.Fatalf("MIN(score) => %#v", got)
	}
	if got := ComputeAggregation("MAX", args, false, grouped[0], e); got != int64(2) {
		t.Fatalf("MAX(score) => %#v", got)
	}
	if got := ComputeAggregation("COLLECT", []IExpressionContext{parseExpressionForTest(t, "name")}, false, grouped[0], e); !reflect.DeepEqual(got, []interface{}{"alice", "bob"}) {
		t.Fatalf("COLLECT(name) => %#v", got)
	}
	if got := ComputeAggregation("AVG", args, false, nil, e); got != nil {
		t.Fatalf("AVG(empty) should be nil, got %#v", got)
	}

	marker, ok := IsAggregation(&AggregationMarker{FuncName: "SUM"})
	if !ok || marker.FuncName != "SUM" {
		t.Fatalf("unexpected IsAggregation result: %#v %v", marker, ok)
	}
	if marker, ok := IsAggregation("nope"); ok || marker != nil {
		t.Fatalf("expected non-marker to return false, got %#v %v", marker, ok)
	}
	if CompareValues(int64(1), int64(2)) >= 0 || !ValuesEqual("x", "x") || ToFloat64(int64(4)) != 4 {
		t.Fatal("unexpected exported helper behavior")
	}
}

func TestANTLRClauseWalker_DirectEntryPoints(t *testing.T) {
	t.Run("direct create delete remove and standalone call", func(t *testing.T) {
		w := &clauseWalker{info: &ClauseInfo{}}
		w.EnterCreateSt(parseCreateStForTest(t, "CREATE (n:Person {name: 'Alice'})").(*CreateStContext))
		w.EnterRemoveSt(parseRemoveStForTest(t, "REMOVE n.legacy").(*RemoveStContext))
		w.EnterDeleteSt(parseDeleteStForTest(t, "DETACH DELETE n").(*DeleteStContext))
		w.EnterStandaloneCall(parseStandaloneCallForTest(t, "CALL db.labels()").(*StandaloneCallContext))

		if w.info.CreatePattern == "" || w.info.RemoveItems != "n.legacy" || w.info.DeleteTargets != "n" || !w.info.DetachDelete || w.info.CallProcedure != "db.labels()" {
			t.Fatalf("unexpected direct clause walker extraction: %+v", w.info)
		}
	})

	t.Run("nil-safe child/full text helpers", func(t *testing.T) {
		if getChildText(nil) != "" || getFullText(nil) != "" {
			t.Fatal("nil parser contexts should yield empty strings")
		}
		empty := NewEmptyScriptContext()
		if getChildText(empty) != "" || getFullText(empty) != "" {
			t.Fatal("empty contexts without tokens should yield empty strings")
		}
		if info := NewClauseExtractor().Extract(&ParseResult{}); info == nil {
			t.Fatal("empty parse result should still return ClauseInfo")
		}
	})

	t.Run("direct remaining clause entrypoints", func(t *testing.T) {
		w := &clauseWalker{info: &ClauseInfo{}}
		w.EnterReturnSt(parseReturnStForTest(t, "RETURN n.name").(*ReturnStContext))
		w.EnterUnwindSt(parseUnwindStForTest(t, "UNWIND [1, 2] AS x").(*UnwindStContext))
		w.EnterOrderSt(parseOrderStForTest(t, "ORDER BY n.name DESC").(*OrderStContext))
		w.EnterLimitSt(parseLimitStForTest(t, "LIMIT 5").(*LimitStContext))
		w.EnterSkipSt(parseSkipStForTest(t, "SKIP 2").(*SkipStContext))
		w.EnterQueryCallSt(parseQueryCallStForTest(t, "CALL db.labels()").(*QueryCallStContext))
		if w.info.ReturnItems != "n.name" || w.info.UnwindExpr != "[1, 2]" || w.info.UnwindAs != "x" || w.info.OrderByItems != "n.name DESC" || w.info.LimitValue != "5" || w.info.SkipValue != "2" || w.info.CallProcedure != "db.labels()" {
			t.Fatalf("unexpected direct clause coverage extraction: %+v", w.info)
		}
	})

	t.Run("extractor nil parse result", func(t *testing.T) {
		extractor := NewClauseExtractor()
		if info := extractor.Extract(nil); info == nil {
			t.Fatal("nil parse result should still return ClauseInfo")
		}
	})

	t.Run("clause walker early returns", func(t *testing.T) {
		w := &clauseWalker{info: &ClauseInfo{
			RemoveItems:   "already",
			ReturnItems:   "already",
			UnwindExpr:    "already",
			OrderByItems:  "already",
			LimitValue:    "already",
			SkipValue:     "already",
			CallProcedure: "already",
		}}
		w.EnterRemoveSt(parseRemoveStForTest(t, "REMOVE n.legacy").(*RemoveStContext))
		w.EnterReturnSt(parseReturnStForTest(t, "RETURN n.name").(*ReturnStContext))
		w.EnterUnwindSt(parseUnwindStForTest(t, "UNWIND [1] AS x").(*UnwindStContext))
		w.EnterOrderSt(parseOrderStForTest(t, "ORDER BY n.name").(*OrderStContext))
		w.EnterLimitSt(parseLimitStForTest(t, "LIMIT 1").(*LimitStContext))
		w.EnterSkipSt(parseSkipStForTest(t, "SKIP 1").(*SkipStContext))
		w.EnterStandaloneCall(parseStandaloneCallForTest(t, "CALL db.labels()").(*StandaloneCallContext))
		w.EnterQueryCallSt(parseQueryCallStForTest(t, "CALL db.labels()").(*QueryCallStContext))
		if w.info.RemoveItems != "already" || w.info.ReturnItems != "already" || w.info.UnwindExpr != "already" || w.info.OrderByItems != "already" || w.info.LimitValue != "already" || w.info.SkipValue != "already" || w.info.CallProcedure != "already" {
			t.Fatalf("early return branches should preserve existing info: %+v", w.info)
		}
	})
}

func TestANTLRExpressionEvaluator_DirectHelpers(t *testing.T) {
	e := NewExpressionEvaluator(nil, nil)

	t.Run("builtin direct coverage", func(t *testing.T) {
		type testCase struct {
			name string
			args []interface{}
			want interface{}
		}
		tests := []testCase{
			{"head", []interface{}{[]interface{}{int64(1), int64(2)}}, int64(1)},
			{"tail", []interface{}{[]interface{}{int64(1), int64(2)}}, []interface{}{int64(2)}},
			{"last", []interface{}{[]interface{}{int64(1), int64(2)}}, int64(2)},
			{"coalesce", []interface{}{nil, "x"}, "x"},
			{"abs", []interface{}{int64(-3)}, int64(3)},
			{"ceil", []interface{}{float64(4.1)}, int64(5)},
			{"floor", []interface{}{float64(4.9)}, int64(4)},
			{"round", []interface{}{float64(4.5)}, int64(5)},
			{"trim", []interface{}{" hi "}, "hi"},
			{"ltrim", []interface{}{" hi "}, "hi "},
			{"rtrim", []interface{}{" hi "}, " hi"},
			{"toupper", []interface{}{"neo"}, "NEO"},
			{"tolower", []interface{}{"NEO"}, "neo"},
			{"exists", []interface{}{nil}, false},
		}
		for _, tc := range tests {
			if got := e.evaluateBuiltInFunction(tc.name, tc.args); !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("%s => %#v, want %#v", tc.name, got, tc.want)
			}
		}
	})

	t.Run("conversion helpers", func(t *testing.T) {
		if got := convertToType(float64(4.9), reflect.TypeOf(int64(0))); got.Interface().(int64) != 4 {
			t.Fatalf("unexpected float64->int64 conversion: %#v", got.Interface())
		}
		if got := convertToType(int64(4), reflect.TypeOf(int(0))); got.Interface().(int) != 4 {
			t.Fatalf("unexpected int64->int conversion: %#v", got.Interface())
		}
		if got := convertToType("x", reflect.TypeOf(int64(0))); got.Interface().(int64) != 0 {
			t.Fatalf("unexpected invalid conversion fallback: %#v", got.Interface())
		}
		if e.toInt64(float32(5)) != 5 || e.toInt64("bad") != 0 {
			t.Fatal("unexpected toInt64 helper behavior")
		}
		if !e.toBool("true") || !e.toBool(float64(1)) || e.toBool("false") {
			t.Fatal("unexpected toBool helper behavior")
		}
		if e.size([]interface{}{1, 2}) != 2 || e.size("abc") != 3 || e.size(nil) != 0 {
			t.Fatal("unexpected size helper behavior")
		}
		if !e.isTruthy(int64(1)) || !e.isTruthy(float64(1)) || e.isTruthy(nil) {
			t.Fatal("unexpected truthiness helper behavior")
		}
		if !e.valuesEqual(true, true) || e.valuesEqual(false, true) {
			t.Fatal("unexpected boolean equality behavior")
		}
		if v, ok := e.toNumericOk(float32(2)); !ok || v != 2 {
			t.Fatal("unexpected toNumericOk(float32) behavior")
		}
	})

	t.Run("show command variants", func(t *testing.T) {
		cases := []struct {
			query string
			want  ShowCommandType
		}{
			{"SHOW INDEXES", ShowIndexes},
			{"SHOW CONSTRAINTS", ShowConstraints},
			{"SHOW PROCEDURES", ShowProcedures},
			{"SHOW FUNCTIONS", ShowFunctions},
			{"SHOW DATABASE", ShowDatabase},
			{"SHOW DATABASES", ShowDatabases},
		}
		for _, tc := range cases {
			parse := parseScriptForTest(t, tc.query)
			if got := ExtractShowCommandType(parse.Tree.Query(0).ShowCommand()); got != tc.want {
				t.Fatalf("%s => %v, want %v", tc.query, got, tc.want)
			}
		}
		if got := ExtractShowCommandType(nil); got != ShowUnknown {
			t.Fatalf("nil show command should be unknown, got %v", got)
		}
	})

	t.Run("ascii stream token text", func(t *testing.T) {
		stream := newASCIICharStream("MATCH")
		lexer := NewCypherLexer(stream)
		tokens := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)
		tokens.Fill()
		if got := stream.GetSourceName(); got != "<empty>" {
			t.Fatalf("unexpected source name: %q", got)
		}
		if got := stream.GetTextFromTokens(tokens.Get(0), tokens.Get(0)); got != "MATCH" {
			t.Fatalf("unexpected token text: %q", got)
		}
		if got := stream.GetTextFromTokens(nil, tokens.Get(0)); got != "" {
			t.Fatalf("nil start token should produce empty text, got %q", got)
		}
		if got := stream.GetTextFromTokens(tokens.Get(0), nil); got != "" {
			t.Fatalf("nil end token should produce empty text, got %q", got)
		}
		if got := stream.GetText(3, 99); got != "CH" {
			t.Fatalf("stop beyond length should clamp, got %q", got)
		}
	})

	t.Run("evaluate direct and aggregation markers", func(t *testing.T) {
		if got := e.Evaluate(parseExpressionForTest(t, "1 OR 2")); got != nil {
			t.Fatalf("multi-xor Evaluate should return nil, got %#v", got)
		}
		aggExprs := []struct {
			expr string
			name string
		}{
			{"COUNT(n)", "COUNT"},
			{"SUM(n)", "SUM"},
			{"AVG(n)", "AVG"},
			{"MIN(n)", "MIN"},
			{"MAX(n)", "MAX"},
			{"COLLECT(n)", "COLLECT"},
		}
		e.SetRow(map[string]interface{}{"n": int64(1)})
		for _, tc := range aggExprs {
			got := e.Evaluate(parseExpressionForTest(t, tc.expr))
			marker, ok := got.(*AggregationMarker)
			if !ok || marker.FuncName != tc.name {
				t.Fatalf("%s => %#v", tc.expr, got)
			}
		}
	})

	t.Run("property access non-map fallback", func(t *testing.T) {
		e2 := NewExpressionEvaluator(nil, nil)
		e2.SetRow(map[string]interface{}{"n": "alice"})
		if got := e2.evaluatePropertyOrLabel(atomicFromExpr(t, "n.name").PropertyOrLabelExpression()); got != nil {
			t.Fatalf("non-map property access should return nil, got %#v", got)
		}
	})
}

func TestANTLRExpressionEvaluator_AtomicAndLiteralInternals(t *testing.T) {
	e := NewExpressionEvaluator(
		map[string]interface{}{"age": int64(42)},
		map[string]interface{}{"tags": []interface{}{"go", "db"}},
	)
	e.SetRow(map[string]interface{}{
		"n": map[string]interface{}{
			"name":  "Alice",
			"email": nil,
			"score": int64(7),
			"meta":  map[string]interface{}{"city": "NYC"},
		},
		"rel": map[string]interface{}{"_type": "KNOWS"},
	})

	t.Run("evaluateAtomic variants", func(t *testing.T) {
		if got := e.evaluateAtomic(atomicFromExpr(t, "n.email IS NULL")); got != true {
			t.Fatalf("IS NULL => %#v", got)
		}
		if got := e.evaluateAtomic(atomicFromExpr(t, "n.name STARTS WITH 'Al'")); got != true {
			t.Fatalf("STARTS WITH => %#v", got)
		}
		if got := e.evaluateAtomic(atomicFromExpr(t, "n.name ENDS WITH 'ce'")); got != true {
			t.Fatalf("ENDS WITH => %#v", got)
		}
		if got := e.evaluateAtomic(atomicFromExpr(t, "n.name CONTAINS 'lic'")); got != true {
			t.Fatalf("CONTAINS => %#v", got)
		}
		if got := e.evaluateAtomic(atomicFromExpr(t, "'go' IN tags")); got != true {
			t.Fatalf("IN => %#v", got)
		}
		if got := e.evaluateAtomic(atomicFromExpr(t, "n.meta.city")); got != "NYC" {
			t.Fatalf("property access => %#v", got)
		}
	})

	t.Run("evaluateAtomicAsBool and comparison sign", func(t *testing.T) {
		if got, handled := e.evaluateAtomicAsBool(atomicFromExpr(t, "n.email IS NOT NULL")); handled != true || got != false {
			t.Fatalf("IS NOT NULL bool => %v %v", got, handled)
		}
		if got, handled := e.evaluateAtomicAsBool(atomicFromExpr(t, "n.name STARTS WITH 1")); handled != true || got != false {
			t.Fatalf("non-string STARTS WITH => %v %v", got, handled)
		}
		if !e.evaluateComparisonSign(int64(1), nil, int64(2)) {
			t.Fatal("nil comparison sign should default true")
		}
	})

	t.Run("evaluateAtom and literals", func(t *testing.T) {
		if got := e.evaluateAtom(atomFromExpr(t, "$age")); got != int64(42) {
			t.Fatalf("parameter atom => %#v", got)
		}
		if got := e.evaluateAtom(atomFromExpr(t, "$missing")); got != nil {
			t.Fatalf("missing parameter atom => %#v", got)
		}
		if got := e.evaluateAtom(atomFromExpr(t, "n")); got == nil {
			t.Fatal("symbol atom should resolve row variable")
		}
		if got := NewExpressionEvaluator(nil, map[string]interface{}{"v": "from-vars"}).evaluateAtom(atomFromExpr(t, "v")); got != "from-vars" {
			t.Fatalf("variable fallback atom => %#v", got)
		}
		if got := NewExpressionEvaluator(nil, nil).evaluateAtom(atomFromExpr(t, "missing")); got != nil {
			t.Fatalf("missing symbol atom => %#v", got)
		}
		if got := e.evaluateAtom(atomFromExpr(t, "(1 + 2)")); got != int64(3) {
			t.Fatalf("parenthesized atom => %#v", got)
		}
		if got := e.evaluateAtom(atomFromExpr(t, "COUNT(*)")); got != "COUNT(*)" {
			t.Fatalf("count all atom => %#v", got)
		}

		if got := e.evaluateLiteral(literalFromExpr(t, "false")); got != false {
			t.Fatalf("bool literal => %#v", got)
		}
		if got := e.evaluateLiteral(literalFromExpr(t, "null")); got != nil {
			t.Fatalf("null literal => %#v", got)
		}
		if got := e.evaluateLiteral(literalFromExpr(t, "3.5")); got != float64(3.5) {
			t.Fatalf("float literal => %#v", got)
		}
		if got := e.evaluateLiteral(literalFromExpr(t, "'c'")); got != "c" {
			t.Fatalf("char/string literal => %#v", got)
		}
		if got := e.evaluateLiteral(literalFromExpr(t, "[]")); !reflect.DeepEqual(got, []interface{}{}) {
			t.Fatalf("empty list literal => %#v", got)
		}
		if got := e.evaluateLiteral(literalFromExpr(t, "{a: 1}")); !reflect.DeepEqual(got, map[string]interface{}{"a": int64(1)}) {
			t.Fatalf("map literal => %#v", got)
		}
	})

	t.Run("function invocation helpers", func(t *testing.T) {
		if got := e.evaluateFunctionInvocation(functionInvocationFromExpr(t, "toString(3)")); got != "3" {
			t.Fatalf("builtin invocation => %#v", got)
		}
		if got := e.evaluateFunctionInvocation(functionInvocationFromExpr(t, "COUNT(n)")); !IsAggregationMarkerNamed(got, "COUNT") {
			t.Fatalf("count invocation => %#v", got)
		}
	})
}

func IsAggregationMarkerNamed(v interface{}, name string) bool {
	m, ok := v.(*AggregationMarker)
	return ok && m.FuncName == name
}

func sortedInterfaces(vals []interface{}) []interface{} {
	out := append([]interface{}(nil), vals...)
	sort.Slice(out, func(i, j int) bool {
		return fmt.Sprint(out[i]) < fmt.Sprint(out[j])
	})
	return out
}

func TestANTLRExpressionEvaluator_NilAndOperatorEdges(t *testing.T) {
	e := NewExpressionEvaluator(nil, nil)

	if !e.evaluateXor(nil) || !e.evaluateAnd(nil) || !e.evaluateNot(nil) || !e.evaluateComparison(nil) {
		t.Fatal("nil logical helpers should default true")
	}
	if e.findAtomicInAddSub(nil) != nil || e.findParenthesizedExprInAddSub(nil) != nil {
		t.Fatal("nil add/sub helpers should return nil")
	}
	if e.evaluateAddSub(nil) != nil || e.evaluateMultDiv(nil) != nil || e.evaluatePower(nil) != nil || e.evaluateUnary(nil) != nil {
		t.Fatal("nil arithmetic helpers should return nil")
	}
	if e.evaluateAtomic(nil) != nil || e.evaluatePropertyOrLabel(nil) != nil || e.evaluateAtom(nil) != nil || e.evaluateLiteral(nil) != nil || e.evaluateFunctionInvocation(nil) != nil {
		t.Fatal("nil atomic helpers should return nil")
	}
	if got, handled := e.evaluateAtomicAsBool(nil); got != true || handled {
		t.Fatalf("nil evaluateAtomicAsBool => %v %v", got, handled)
	}
	if e.Evaluate(nil) != nil {
		t.Fatal("Evaluate(nil) should be nil")
	}
	if got := e.callPluginFunction(123, []interface{}{1}); got != nil {
		t.Fatalf("non-function plugin call should return nil, got %#v", got)
	}
	if got := e.callPluginFunction(func(args ...interface{}) interface{} { return len(args) }, []interface{}{1, 2, 3}); got != 3 {
		t.Fatalf("variadic plugin call => %#v", got)
	}

	if !e.evaluateComparisonSign(int64(1), comparisonSignFromExpr(t, "1 = 1"), int64(1)) {
		t.Fatal("= comparison should be true")
	}
	if !e.evaluateComparisonSign(int64(1), comparisonSignFromExpr(t, "1 <> 2"), int64(2)) {
		t.Fatal("<> comparison should be true")
	}
	if !e.evaluateComparisonSign(int64(1), comparisonSignFromExpr(t, "1 < 2"), int64(2)) {
		t.Fatal("< comparison should be true")
	}
	if !e.evaluateComparisonSign(int64(2), comparisonSignFromExpr(t, "2 > 1"), int64(1)) {
		t.Fatal("> comparison should be true")
	}
	if !e.evaluateComparisonSign(int64(2), comparisonSignFromExpr(t, "2 <= 2"), int64(2)) {
		t.Fatal("<= comparison should be true")
	}
	if !e.evaluateComparisonSign(int64(2), comparisonSignFromExpr(t, "2 >= 2"), int64(2)) {
		t.Fatal(">= comparison should be true")
	}

	if got := e.evaluatePower(powerFromExpr(t, "2 ^ 3")); got != int64(2) {
		t.Fatalf("power expression currently returns first unary value, got %#v", got)
	}
	if got := e.toFloat64(e.evaluateUnary(unaryFromExpr(t, "-3"))); got != float64(-3) {
		t.Fatalf("unary minus => %#v", got)
	}
	if e.findAtomicInAddSub(addSubFromExpr(t, "1 + 2")) != nil {
		t.Fatal("multi-term add/sub should not expose a single atomic expression")
	}
	if e.findParenthesizedExprInAddSub(addSubFromExpr(t, "(1 = 1)")) == nil {
		t.Fatal("parenthesized expression should be found")
	}

	if !e.valuesEqual("x", "x") || !e.valuesEqual(false, false) || e.valuesEqual(true, false) {
		t.Fatal("unexpected equality helper behavior")
	}
	if e.toInt64(float64(4)) != 4 || e.toBool(bool(true)) != true || e.toBool(float64(0)) != false {
		t.Fatal("unexpected conversion helper behavior")
	}
}

func TestANTLRExpressionEvaluator_ExtractionEdgeCases(t *testing.T) {
	if info := ExtractAggregation(nil); info.IsAggregation {
		t.Fatalf("nil aggregation should be empty: %+v", info)
	}
	if info := ExtractAggregation(parseExpressionForTest(t, "toString(n)")); info.IsAggregation {
		t.Fatalf("non-aggregation function misdetected: %+v", info)
	}
	if got := ExtractVariableFromExpression(nil); got != "" {
		t.Fatalf("nil variable extraction => %q", got)
	}
	if got := ExtractVariableFromExpression(parseExpressionForTest(t, "n.name")); got != "" {
		t.Fatalf("property expression should not return simple variable, got %q", got)
	}
	if got, props := ExtractPropertyAccess(nil); got != "" || props != nil {
		t.Fatalf("nil property access var => %q", got)
	}
	if props := ExtractProperties(nil, NewExpressionEvaluator(nil, nil)); len(props) != 0 {
		t.Fatalf("nil properties should be empty, got %#v", props)
	}
	if proc := ExtractProcedureName(nil); proc.Name != "" || proc.IsDbProc {
		t.Fatalf("nil procedure extraction => %+v", proc)
	}
	if items := ExtractSortItems(nil); items != nil {
		t.Fatalf("nil sort items should be nil, got %#v", items)
	}
	if groups := GroupRows([]map[string]interface{}{{"x": 1}}, nil, NewExpressionEvaluator(nil, nil)); len(groups) != 1 || len(groups[0]) != 1 {
		t.Fatalf("group rows without keys => %#v", groups)
	}
	if got := ComputeAggregation("UNKNOWN", nil, false, nil, NewExpressionEvaluator(nil, nil)); got != nil {
		t.Fatalf("unknown aggregation should be nil, got %#v", got)
	}
	if vars := ExtractVariablesFromExpressionChain(nil); vars != nil {
		t.Fatalf("nil expression chain vars should be nil, got %#v", vars)
	}
}

func TestANTLRExpressionEvaluator_ConversionAndExtractionCompleteness(t *testing.T) {
	t.Run("convertToType numeric branches", func(t *testing.T) {
		type namedInt int
		if got := convertToType("neo", reflect.TypeOf("")); got.Interface().(string) != "neo" {
			t.Fatalf("assignable conversion => %#v", got.Interface())
		}
		if got := convertToType(namedInt(5), reflect.TypeOf(int(0))); got.Interface().(int) != 5 {
			t.Fatalf("convertible named int => %#v", got.Interface())
		}
		if got := convertToType(int(2), reflect.TypeOf(float64(0))); got.Interface().(float64) != 2 {
			t.Fatalf("int->float64 => %#v", got.Interface())
		}
		if got := convertToType(int64(2), reflect.TypeOf(float64(0))); got.Interface().(float64) != 2 {
			t.Fatalf("int64->float64 => %#v", got.Interface())
		}
		if got := convertToType(float32(2), reflect.TypeOf(float64(0))); got.Interface().(float64) != 2 {
			t.Fatalf("float32->float64 => %#v", got.Interface())
		}
		if got := convertToType(int(2), reflect.TypeOf(int64(0))); got.Interface().(int64) != 2 {
			t.Fatalf("int->int64 => %#v", got.Interface())
		}
		if got := convertToType(float64(2), reflect.TypeOf(int64(0))); got.Interface().(int64) != 2 {
			t.Fatalf("float64->int64 => %#v", got.Interface())
		}
		if got := convertToType(int64(2), reflect.TypeOf(int64(0))); got.Interface().(int64) != 2 {
			t.Fatalf("int64->int64 => %#v", got.Interface())
		}
		if got := convertToType(int(2), reflect.TypeOf(int(0))); got.Interface().(int) != 2 {
			t.Fatalf("int->int => %#v", got.Interface())
		}
		if got := convertToType(int64(2), reflect.TypeOf(int(0))); got.Interface().(int) != 2 {
			t.Fatalf("int64->int => %#v", got.Interface())
		}
		if got := convertToType(float64(2), reflect.TypeOf(int(0))); got.Interface().(int) != 2 {
			t.Fatalf("float64->int => %#v", got.Interface())
		}
		if got := convertToType(nil, reflect.TypeOf(int64(0))); got.Interface().(int64) != 0 {
			t.Fatalf("nil->int64 zero => %#v", got.Interface())
		}
		if got := convertToType(true, reflect.TypeOf(int64(0))); got.Interface().(int64) != 0 {
			t.Fatalf("unsupported bool->int64 should zero, got %#v", got.Interface())
		}
	})

	t.Run("value equality and float helpers", func(t *testing.T) {
		e := NewExpressionEvaluator(nil, nil)
		if !e.valuesEqual(float64(2), int64(2)) || !e.valuesEqual(float64(2), int(2)) {
			t.Fatal("float equality branches should match equivalent ints")
		}
		if e.valuesEqual(float64(2), "2") {
			t.Fatal("float should not equal string")
		}
		if e.toFloat64(int(3)) != 3 || e.toFloat64(int64(4)) != 4 {
			t.Fatal("integer toFloat64 branches should convert")
		}
	})

	t.Run("aggregation detection variants", func(t *testing.T) {
		cases := []struct {
			expr string
			want string
		}{
			{"SUM(n)", "SUM"},
			{"AVG(n)", "AVG"},
			{"MIN(n)", "MIN"},
			{"MAX(n)", "MAX"},
			{"COLLECT(n)", "COLLECT"},
		}
		for _, tc := range cases {
			info := ExtractAggregation(parseExpressionForTest(t, tc.expr))
			if !info.IsAggregation || info.FuncName != tc.want || len(info.Args) != 1 {
				t.Fatalf("%s => %+v", tc.expr, info)
			}
		}
	})

	t.Run("atom and variable extraction helpers", func(t *testing.T) {
		if atom := findAtomInExpression(parseExpressionForTest(t, "n")); atom == nil {
			t.Fatal("simple symbol should expose atom")
		}
		if atom := findAtomInExpression(parseExpressionForTest(t, "toString(n)")); atom == nil {
			t.Fatal("function expression should still expose atom")
		}
		if atom := findAtomInExpression(parseExpressionForTest(t, "COUNT(*)")); atom == nil {
			t.Fatal("count(*) should expose an atom")
		}
		if atom := findAtomInExpression(parseExpressionForTest(t, "1 OR 2")); atom != nil {
			t.Fatalf("compound boolean expression should not expose single atom: %#v", atom)
		}
		if got := ExtractVariableFromExpression(parseExpressionForTest(t, "n")); got != "n" {
			t.Fatalf("simple variable extraction => %q", got)
		}
		if got := ExtractVariableFromExpression(parseExpressionForTest(t, "toString(n)")); got != "" {
			t.Fatalf("function expression should not extract simple variable, got %q", got)
		}
		if got := ExtractVariableFromExpression(parseExpressionForTest(t, "COUNT(*)")); got != "" {
			t.Fatalf("count(*) should not extract simple variable, got %q", got)
		}
	})

	t.Run("collectVariablesFromTree interface branches", func(t *testing.T) {
		seen := map[string]struct{}{}
		var vars []string
		collectVariablesFromTree(atomicFromExpr(t, "n.name"), seen, &vars)
		collectVariablesFromTree(functionInvocationFromExpr(t, "toString(m)"), seen, &vars)
		collectVariablesFromTree(parseExpressionForTest(t, "(x)"), seen, &vars)
		collectVariablesFromTree(atomicFromExpr(t, "n.name STARTS WITH prefix"), seen, &vars)
		collectVariablesFromTree(atomicFromExpr(t, "item IN tags"), seen, &vars)
		if !reflect.DeepEqual(vars, []string{"n", "m", "x", "prefix", "item", "tags"}) {
			t.Fatalf("unexpected collected vars: %#v", vars)
		}
	})
}

func TestANTLRExpressionEvaluator_BuiltinEdgeCases(t *testing.T) {
	e := NewExpressionEvaluator(nil, nil)

	cases := []struct {
		name string
		args []interface{}
		want interface{}
	}{
		{"head", []interface{}{[]interface{}{}}, nil},
		{"tail", []interface{}{[]interface{}{int64(1)}}, nil},
		{"last", []interface{}{[]interface{}{}}, nil},
		{"reverse", []interface{}{"not-a-list"}, nil},
		{"range", []interface{}{int64(1), int64(3), int64(0)}, []interface{}{int64(1), int64(2), int64(3)}},
		{"coalesce", []interface{}{nil, nil}, nil},
		{"substring", []interface{}{"abcdef", int64(-2)}, "abcdef"},
		{"substring", []interface{}{"abcdef", int64(20)}, ""},
		{"substring", []interface{}{"abcdef", int64(2), int64(99)}, "cdef"},
		{"left", []interface{}{"abcdef", int64(99)}, "abcdef"},
		{"right", []interface{}{"abcdef", int64(99)}, "abcdef"},
		{"keys", []interface{}{"not-a-map"}, nil},
		{"properties", []interface{}{"not-a-map"}, nil},
		{"exists", nil, false},
	}

	for _, tc := range cases {
		if got := e.evaluateBuiltInFunction(tc.name, tc.args); !reflect.DeepEqual(got, tc.want) {
			t.Fatalf("%s(%#v) => %#v, want %#v", tc.name, tc.args, got, tc.want)
		}
	}

	if got := e.evaluateBuiltInFunction("id", []interface{}{map[string]interface{}{"_edgeId": "e-1"}}); got != "e-1" {
		t.Fatalf("id(_edgeId) => %#v", got)
	}
}

func TestANTLRExpressionEvaluator_AdditionalExtractionAndEvaluateBranches(t *testing.T) {
	e := NewExpressionEvaluator(nil, nil)

	if got := e.Evaluate(parseExpressionForTest(t, "1 AND 2")); got != nil {
		t.Fatalf("Evaluate on AND expression should return nil, got %#v", got)
	}
	if got := e.Evaluate(parseExpressionForTest(t, "1 < 2")); got != nil {
		t.Fatalf("Evaluate on comparison expression should return nil, got %#v", got)
	}

	proj := ExtractProjectionItem(parseProjectionItemForTest(t, "n.name"))
	if proj.Alias != "n.name" || proj.IsAggregation {
		t.Fatalf("unexpected simple projection info: %+v", proj)
	}

	if atom := findAtomInExpression(nil); atom != nil {
		t.Fatalf("nil expression should not expose atom: %#v", atom)
	}
	if atom := findAtomInExpression(parseExpressionForTest(t, "n.name")); atom == nil {
		t.Fatal("property expression should expose atom")
	}

	if got := ExtractVariableFromExpression(parseExpressionForTest(t, "1 AND 2")); got != "" {
		t.Fatalf("AND expression should not extract simple variable, got %q", got)
	}

	proc := ExtractProcedureName(parseInvocationNameForTest(t, "labels"))
	if proc.Name != "labels" || proc.IsDbProc || proc.Namespace != "" {
		t.Fatalf("unexpected non-db procedure info: %+v", proc)
	}
}

func TestANTLRExpressionEvaluator_ArithmeticAndConversionEdges(t *testing.T) {
	e := NewExpressionEvaluator(nil, nil)

	if got := e.Evaluate(parseExpressionForTest(t, "5 - 2")); got != int64(3) {
		t.Fatalf("subtraction => %#v", got)
	}
	if got := e.Evaluate(parseExpressionForTest(t, "8 / 2")); got != int64(4) {
		t.Fatalf("division => %#v", got)
	}
	if got := e.Evaluate(parseExpressionForTest(t, "8 / 0")); got != int64(8) {
		t.Fatalf("division by zero should leave lhs unchanged, got %#v", got)
	}
	if got := e.Evaluate(parseExpressionForTest(t, "8 % 0")); got != int64(8) {
		t.Fatalf("modulo by zero should leave lhs unchanged, got %#v", got)
	}
	if got := e.evaluateLiteral(literalFromExpr(t, "42")); got != int64(42) {
		t.Fatalf("integer literal => %#v", got)
	}

	if !e.valuesEqual(int64(2), int(2)) || !e.valuesEqual(float64(2), float64(2)) || e.valuesEqual(int64(2), "2") {
		t.Fatal("unexpected additional equality behavior")
	}
	if e.compareValues("a", "a") != 0 || e.compareValues(true, false) != 0 {
		t.Fatal("unexpected compareValues equality/fallback behavior")
	}
	if e.toFloat64("3.25") != 3.25 {
		t.Fatal("string toFloat64 conversion should parse")
	}
	if v, ok := e.toNumericOk(int64(2)); !ok || v != 2 {
		t.Fatal("toNumericOk(int64) should succeed")
	}
	if v, ok := e.toNumericOk(int(3)); !ok || v != 3 {
		t.Fatal("toNumericOk(int) should succeed")
	}
}

// ---------------------------------------------------------------------------
// QueryAnalyzer – standalone CALL routing and db-procedure classification
// ---------------------------------------------------------------------------

func TestANTLRQueryAnalyzer_StandaloneCallDbProc(t *testing.T) {
	// Per openCypher/Neo4j: standalone CALL db.labels() is a read-only
	// metadata procedure invocation.
	analyzer := NewQueryAnalyzer()
	query := "CALL db.labels()"
	info := analyzer.Analyze(query, parseScriptForTest(t, query))

	if !info.HasCall {
		t.Fatal("standalone CALL must set HasCall")
	}
	if info.FirstClause != ClauseCall {
		t.Fatalf("FirstClause = %v, want ClauseCall", info.FirstClause)
	}
	if !info.CallIsDbProcedure {
		t.Fatal("CALL db.labels() must set CallIsDbProcedure")
	}
	if !info.IsReadOnly {
		t.Fatal("CALL db.labels() must be read-only per Neo4j semantics")
	}
	if info.IsWriteQuery {
		t.Fatal("CALL db.labels() must not be a write query")
	}
}

func TestANTLRQueryAnalyzer_StandaloneCallNonDbProc(t *testing.T) {
	// Non-db.* procedures are not guaranteed read-only and must not be cached.
	analyzer := NewQueryAnalyzer()
	query := "CALL apoc.help('match')"
	info := analyzer.Analyze(query, parseScriptForTest(t, query))

	if !info.HasCall {
		t.Fatal("standalone CALL must set HasCall")
	}
	if info.CallIsDbProcedure {
		t.Fatal("apoc.help is not a db.* procedure")
	}
	if info.IsReadOnly {
		t.Fatal("standalone CALL to non-db.* procedure must not be read-only")
	}
}

// ---------------------------------------------------------------------------
// QueryAnalyzer – PROFILE prefix (distinct from EXPLAIN)
// ---------------------------------------------------------------------------

func TestANTLRQueryAnalyzer_ProfilePrefix(t *testing.T) {
	analyzer := NewQueryAnalyzer()
	query := "PROFILE MATCH (n:Person) RETURN n"
	info := analyzer.Analyze(query, parseScriptForTest(t, query))

	if !info.HasProfile {
		t.Fatal("PROFILE prefix must set HasProfile")
	}
	if info.HasExplain {
		t.Fatal("PROFILE prefix must not set HasExplain")
	}
}

// ---------------------------------------------------------------------------
// QueryAnalyzer – OPTIONAL MATCH first-clause routing
// ---------------------------------------------------------------------------

func TestANTLRQueryAnalyzer_OptionalMatchAsFirstClause(t *testing.T) {
	analyzer := NewQueryAnalyzer()
	query := "OPTIONAL MATCH (n:Person)-[:KNOWS]->(m) RETURN n, m"
	info := analyzer.Analyze(query, parseScriptForTest(t, query))

	if !info.HasOptionalMatch {
		t.Fatal("OPTIONAL MATCH must set HasOptionalMatch")
	}
	if info.FirstClause != ClauseOptionalMatch {
		t.Fatalf("FirstClause = %v, want ClauseOptionalMatch", info.FirstClause)
	}
	if !info.IsReadOnly {
		t.Fatal("OPTIONAL MATCH + RETURN is read-only")
	}
}

func TestANTLRQueryAnalyzer_OptionalMatchFollowedByMatch(t *testing.T) {
	analyzer := NewQueryAnalyzer()
	query := "OPTIONAL MATCH (n:Person) WITH n MATCH (n)-[:KNOWS]->(m) RETURN m"
	info := analyzer.Analyze(query, parseScriptForTest(t, query))

	if !info.HasOptionalMatch {
		t.Fatal("expected HasOptionalMatch")
	}
	if !info.HasMatch {
		t.Fatal("expected HasMatch for the regular MATCH clause")
	}
	if info.FirstClause != ClauseOptionalMatch {
		t.Fatalf("FirstClause = %v, want ClauseOptionalMatch", info.FirstClause)
	}
}

// ---------------------------------------------------------------------------
// QueryAnalyzer – multiple MERGE compound detection
// ---------------------------------------------------------------------------

func TestANTLRQueryAnalyzer_MultipleMerge(t *testing.T) {
	analyzer := NewQueryAnalyzer()
	query := "MERGE (a:Person {id: 1}) MERGE (b:Person {id: 2}) MERGE (a)-[:KNOWS]->(b)"
	info := analyzer.Analyze(query, parseScriptForTest(t, query))

	if info.MergeCount != 3 {
		t.Fatalf("MergeCount = %d, want 3", info.MergeCount)
	}
	if !info.IsCompoundQuery {
		t.Fatal("multiple MERGEs must set IsCompoundQuery")
	}
	if !info.IsWriteQuery {
		t.Fatal("MERGE is a write query")
	}
}

// ---------------------------------------------------------------------------
// QueryAnalyzer – nil/empty parse result defensive paths
// ---------------------------------------------------------------------------

func TestANTLRQueryAnalyzer_NilParseResult(t *testing.T) {
	analyzer := NewQueryAnalyzer()
	info := analyzer.Analyze("RETURN 1", nil)
	if info.IsWriteQuery || info.IsReadOnly || info.IsSchemaQuery {
		t.Fatal("nil parse result should leave all flags false")
	}
	// Must return cached value on second call
	if analyzer.Analyze("RETURN 1", nil) != info {
		t.Fatal("analyzer must cache by query string")
	}
}

func TestANTLRQueryAnalyzer_NilTree(t *testing.T) {
	analyzer := NewQueryAnalyzer()
	info := analyzer.Analyze("RETURN 1", &ParseResult{Tree: nil})
	if info.IsWriteQuery || info.IsReadOnly || info.IsSchemaQuery {
		t.Fatal("nil tree should leave all flags false")
	}
}

// ---------------------------------------------------------------------------
// QueryAnalyzer – CREATE CONSTRAINT schema routing
// ---------------------------------------------------------------------------

func TestANTLRQueryAnalyzer_CreateConstraint(t *testing.T) {
	analyzer := NewQueryAnalyzer()
	query := "CREATE CONSTRAINT person_id IF NOT EXISTS FOR (n:Person) REQUIRE n.id IS UNIQUE"
	info := analyzer.Analyze(query, parseScriptForTest(t, query))

	if !info.HasSchema || !info.IsSchemaQuery {
		t.Fatal("CREATE CONSTRAINT must be a schema query")
	}
	if info.FirstClause != ClauseCreate {
		t.Fatalf("FirstClause = %v, want ClauseCreate", info.FirstClause)
	}
}

// ---------------------------------------------------------------------------
// convertToType – numeric conversion branches
// ---------------------------------------------------------------------------

func TestConvertToType_Branches(t *testing.T) {
	tests := []struct {
		name   string
		val    interface{}
		target reflect.Type
		want   interface{}
	}{
		{"int→float64", int(7), reflect.TypeOf(float64(0)), float64(7)},
		{"float32→float64", float32(2.5), reflect.TypeOf(float64(0)), float64(2.5)},
		{"int→int64", int(9), reflect.TypeOf(int64(0)), int64(9)},
		{"float64→int", float64(3.7), reflect.TypeOf(int(0)), int(3)},
		{"int64→int", int64(42), reflect.TypeOf(int(0)), int(42)},
		{"int→int (identity)", int(5), reflect.TypeOf(int(0)), int(5)},
		{"unconvertible→zero", []int{1, 2}, reflect.TypeOf(""), ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := convertToType(tt.val, tt.target).Interface()
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("convertToType(%T, %v) = %#v, want %#v", tt.val, tt.target, got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// findAtomInExpression – path coverage
// ---------------------------------------------------------------------------

func TestFindAtomInExpression(t *testing.T) {
	t.Run("simple variable", func(t *testing.T) {
		atom := findAtomInExpression(parseExpressionForTest(t, "n"))
		if atom == nil || atom.Symbol() == nil || atom.Symbol().GetText() != "n" {
			t.Fatal("should find atom 'n' in simple variable expression")
		}
	})

	t.Run("nil expression", func(t *testing.T) {
		if findAtomInExpression(nil) != nil {
			t.Fatal("nil expression must return nil")
		}
	})

	t.Run("compound OR expression", func(t *testing.T) {
		// Multiple XorExpression children → early return nil
		if findAtomInExpression(parseExpressionForTest(t, "a OR b")) != nil {
			t.Fatal("compound expression must return nil")
		}
	})

	t.Run("arithmetic expression", func(t *testing.T) {
		// "1 + 2" has multiple AddSub children → early nil
		if findAtomInExpression(parseExpressionForTest(t, "1 + 2")) != nil {
			t.Fatal("arithmetic expression must return nil")
		}
	})
}

// ---------------------------------------------------------------------------
// EvaluateWhere – OR branch evaluation per openCypher
// ---------------------------------------------------------------------------

func TestANTLRExpressionEvaluator_OrBranches(t *testing.T) {
	e := NewExpressionEvaluator(nil, nil)
	e.SetRow(map[string]interface{}{
		"n": map[string]interface{}{"age": int64(10), "city": "LA"},
	})

	t.Run("all OR branches false", func(t *testing.T) {
		expr := parseExpressionForTest(t, "n.age > 100 OR n.city = 'NYC'")
		if e.EvaluateWhere(expr) {
			t.Fatal("both OR branches false → EvaluateWhere must return false")
		}
	})

	t.Run("second OR branch true", func(t *testing.T) {
		e.SetRow(map[string]interface{}{
			"n": map[string]interface{}{"age": int64(10), "city": "NYC"},
		})
		expr := parseExpressionForTest(t, "n.age > 100 OR n.city = 'NYC'")
		if !e.EvaluateWhere(expr) {
			t.Fatal("second OR branch true → EvaluateWhere must return true")
		}
	})
}

// ---------------------------------------------------------------------------
// EvaluateWhere – XOR chaining per openCypher
// ---------------------------------------------------------------------------

func TestANTLRExpressionEvaluator_XorChained(t *testing.T) {
	e := NewExpressionEvaluator(nil, nil)
	e.SetRow(map[string]interface{}{
		"a": true, "b": false, "c": true,
	})
	// openCypher: true XOR false XOR true = (true XOR false) XOR true = true XOR true = false
	expr := parseExpressionForTest(t, "a XOR b XOR c")
	if e.EvaluateWhere(expr) {
		t.Fatal("true XOR false XOR true = false per openCypher")
	}
}

// ---------------------------------------------------------------------------
// Comparison operators – <= >= <> per openCypher
// ---------------------------------------------------------------------------

func TestANTLRExpressionEvaluator_ComparisonOperators(t *testing.T) {
	e := NewExpressionEvaluator(nil, nil)

	tests := []struct {
		expr string
		want bool
	}{
		{"1 <= 1", true},
		{"1 <= 2", true},
		{"2 <= 1", false},
		{"2 >= 1", true},
		{"1 >= 1", true},
		{"1 >= 2", false},
		{"1 <> 2", true},
		{"1 <> 1", false},
	}
	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			if got := e.EvaluateWhere(parseExpressionForTest(t, tt.expr)); got != tt.want {
				t.Fatalf("%s = %v, want %v per openCypher", tt.expr, got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// String predicates – false cases per openCypher
// ---------------------------------------------------------------------------

func TestANTLRExpressionEvaluator_StringPredicateFalsePaths(t *testing.T) {
	e := NewExpressionEvaluator(nil, nil)

	tests := []struct {
		name string
		row  map[string]interface{}
		expr string
	}{
		{"STARTS WITH false", map[string]interface{}{"n": map[string]interface{}{"name": "Bob"}}, "n.name STARTS WITH 'Al'"},
		{"ENDS WITH false", map[string]interface{}{"n": map[string]interface{}{"name": "Alice"}}, "n.name ENDS WITH 'ob'"},
		{"CONTAINS false", map[string]interface{}{"n": map[string]interface{}{"name": "Alice"}}, "n.name CONTAINS 'xyz'"},
		{"IN false", map[string]interface{}{"n": map[string]interface{}{"city": "Chicago"}}, "n.city IN ['NYC', 'LA', 'SF']"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e.SetRow(tt.row)
			if e.EvaluateWhere(parseExpressionForTest(t, tt.expr)) {
				t.Fatalf("%s must be false per openCypher", tt.expr)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Numeric helpers – uncovered type branches
// ---------------------------------------------------------------------------

func TestANTLRExpressionEvaluator_NumericHelperEdges(t *testing.T) {
	e := NewExpressionEvaluator(nil, nil)

	// toInt64
	if e.toInt64(int(99)) != 99 {
		t.Fatal("toInt64(int) should convert")
	}
	if e.toInt64(true) != 0 {
		t.Fatal("toInt64(bool) should return 0")
	}

	// toBool
	if e.toBool(int64(0)) {
		t.Fatal("toBool(int64(0)) must be false per Neo4j")
	}
	if e.toBool(float64(0)) {
		t.Fatal("toBool(float64(0)) must be false per Neo4j")
	}
	if e.toBool("FALSE") {
		t.Fatal("toBool('FALSE') must be false (case-insensitive)")
	}

	// toFloat64
	if e.toFloat64("3.14") != 3.14 {
		t.Fatal("toFloat64 should parse valid numeric string")
	}
	if e.toFloat64("abc") != 0 {
		t.Fatal("toFloat64 of non-numeric string should return 0")
	}
	if e.toFloat64(int(7)) != 7.0 {
		t.Fatal("toFloat64(int) should convert")
	}
	if e.toFloat64(nil) != 0 {
		t.Fatal("toFloat64(nil) should return 0")
	}

	// compareValues
	if e.compareValues("abc", "abc") != 0 {
		t.Fatal("compareValues equal strings must return 0")
	}
	if e.compareValues("a", "b") >= 0 {
		t.Fatal("compareValues 'a' < 'b'")
	}

	// isTruthy – lists and maps fall through to return true
	if !e.isTruthy([]interface{}{}) {
		t.Fatal("empty list falls through to truthy in current implementation")
	}
	if !e.isTruthy(map[string]interface{}{"a": 1}) {
		t.Fatal("non-empty map is truthy")
	}
}

// ---------------------------------------------------------------------------
// Power expression evaluation – evaluatePower only evaluates first operand
// (exponentiation not yet implemented; this covers the len(unarys)==0 guard)
// ---------------------------------------------------------------------------

func TestANTLRExpressionEvaluator_PowerExpression(t *testing.T) {
	e := NewExpressionEvaluator(nil, nil)
	// evaluatePower currently returns the first operand only
	got := e.Evaluate(parseExpressionForTest(t, "2 ^ 3"))
	if got != int64(2) {
		t.Fatalf("2 ^ 3 (power not implemented) should return first operand 2, got %#v", got)
	}
}

// ---------------------------------------------------------------------------
// Builtin function edge cases
// ---------------------------------------------------------------------------

func TestANTLRExpressionEvaluator_BuiltinFunctionEdges(t *testing.T) {
	e := NewExpressionEvaluator(nil, nil)

	t.Run("range default step", func(t *testing.T) {
		got := e.Evaluate(parseExpressionForTest(t, "range(1, 5)"))
		want := []interface{}{int64(1), int64(2), int64(3), int64(4), int64(5)}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("range(1,5) = %#v, want %#v", got, want)
		}
	})

	t.Run("substring no length", func(t *testing.T) {
		got := e.Evaluate(parseExpressionForTest(t, "substring('abcdef', 3)"))
		if got != "def" {
			t.Fatalf("substring('abcdef', 3) = %#v, want 'def'", got)
		}
	})

	t.Run("head empty list returns nil", func(t *testing.T) {
		if got := e.evaluateBuiltInFunction("head", []interface{}{[]interface{}{}}); got != nil {
			t.Fatalf("head([]) must return nil per Neo4j, got %#v", got)
		}
	})

	t.Run("last empty list returns nil", func(t *testing.T) {
		if got := e.evaluateBuiltInFunction("last", []interface{}{[]interface{}{}}); got != nil {
			t.Fatalf("last([]) must return nil per Neo4j, got %#v", got)
		}
	})

	t.Run("tail empty list returns nil", func(t *testing.T) {
		// Implementation returns nil when list has <2 elements
		if got := e.evaluateBuiltInFunction("tail", []interface{}{[]interface{}{}}); got != nil {
			t.Fatalf("tail([]) = %#v, want nil", got)
		}
	})

	t.Run("coalesce all nil returns nil", func(t *testing.T) {
		if got := e.evaluateBuiltInFunction("coalesce", []interface{}{nil, nil}); got != nil {
			t.Fatalf("coalesce(null, null) must return nil per Neo4j, got %#v", got)
		}
	})
}

// ---------------------------------------------------------------------------
// ExtractProjectionItem – non-aggregation, alias fallback
// ---------------------------------------------------------------------------

func TestANTLRExtractProjectionItem_NonAggregation(t *testing.T) {
	proj := ExtractProjectionItem(parseProjectionItemForTest(t, "n.name AS personName"))
	if proj.IsAggregation {
		t.Fatal("n.name is not an aggregation")
	}
	if proj.Alias != "personName" {
		t.Fatalf("Alias = %q, want 'personName'", proj.Alias)
	}
}

func TestANTLRExtractProjectionItem_NoExplicitAlias(t *testing.T) {
	// When no AS alias is given, the expression text is used as alias
	proj := ExtractProjectionItem(parseProjectionItemForTest(t, "n.name"))
	if proj.Alias != "n.name" {
		t.Fatalf("Alias = %q, want 'n.name' (expression text fallback)", proj.Alias)
	}
}

// ---------------------------------------------------------------------------
// ExtractProcedureName – namespace extraction
// ---------------------------------------------------------------------------

func TestANTLRExtractProcedureName_NestedNamespace(t *testing.T) {
	proc := ExtractProcedureName(parseInvocationNameForTest(t, "apoc.coll.sum"))
	if proc.Name != "apoc.coll.sum" {
		t.Fatalf("Name = %q, want 'apoc.coll.sum'", proc.Name)
	}
	// Namespace is only set for db.* procedures
	if proc.IsDbProc {
		t.Fatal("apoc.coll.sum is not a db.* procedure")
	}
}

// ---------------------------------------------------------------------------
// ExtractRelationshipPattern – direction semantics
// ---------------------------------------------------------------------------

func TestANTLRExtractRelationshipPattern_ForwardWithProps(t *testing.T) {
	e := NewExpressionEvaluator(nil, nil)
	rel := ExtractRelationshipPattern(parseRelationshipPatternForTest(t, "-[r:LIKES {weight: 0.5}]->"), e)
	if !rel.IsForward {
		t.Fatal("forward pattern (-[]->) must be IsForward")
	}
	if rel.Type != "LIKES" {
		t.Fatalf("Type = %q, want 'LIKES'", rel.Type)
	}
	if rel.Properties["weight"] != float64(0.5) {
		t.Fatalf("weight = %v, want 0.5", rel.Properties["weight"])
	}
}

func TestANTLRExtractRelationshipPattern_Undirected(t *testing.T) {
	e := NewExpressionEvaluator(nil, nil)
	// Undirected -[r:KNOWS]- has no < token, so IsForward defaults to true
	rel := ExtractRelationshipPattern(parseRelationshipPatternForTest(t, "-[r:KNOWS]-"), e)
	if !rel.IsForward {
		t.Fatal("undirected pattern defaults to IsForward=true in implementation")
	}
	if rel.Type != "KNOWS" {
		t.Fatalf("Type = %q, want 'KNOWS'", rel.Type)
	}
}

// ---------------------------------------------------------------------------
// ExtractNodePattern – bare variable
// ---------------------------------------------------------------------------

func TestANTLRExtractNodePattern_VariableOnly(t *testing.T) {
	e := NewExpressionEvaluator(nil, nil)
	node := ExtractNodePattern(parseNodePatternForTest(t, "(n)"), e)
	if node.Variable != "n" {
		t.Fatalf("Variable = %q, want 'n'", node.Variable)
	}
	if len(node.Labels) != 0 {
		t.Fatalf("expected no labels, got %v", node.Labels)
	}
	if len(node.Properties) != 0 {
		t.Fatalf("expected no properties, got %v", node.Properties)
	}
}

// ---------------------------------------------------------------------------
// collectVariablesFromTree – deeper expression structures
// ---------------------------------------------------------------------------

func TestANTLRCollectVariablesFromTree_Nested(t *testing.T) {
	chainVars := ExtractVariablesFromExpressionChain(parseExpressionChainForTest(t, "count(a), sum(b)"))
	sort.Strings(chainVars)
	if !reflect.DeepEqual(chainVars, []string{"a", "b"}) {
		t.Fatalf("got %v, want [a b]", chainVars)
	}
}

func TestANTLRCollectVariablesFromTree_StringExpression(t *testing.T) {
	chainVars := ExtractVariablesFromExpressionChain(parseExpressionChainForTest(t, "n.name STARTS WITH prefix"))
	if len(chainVars) < 2 {
		t.Fatalf("expected at least variables n and prefix, got %v", chainVars)
	}
}

func TestANTLRCollectVariablesFromTree_Nil(t *testing.T) {
	if ExtractVariablesFromExpressionChain(nil) != nil {
		t.Fatal("nil chain must return nil")
	}
}

// ---------------------------------------------------------------------------
// Validate – SLL→LL fallback and error paths
// ---------------------------------------------------------------------------

func TestANTLRValidate_ComplexQuery(t *testing.T) {
	query := "MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.age > 21 AND EXISTS { MATCH (b)-[:WORKS_AT]->(c:Company) WHERE c.name = 'CVS' } RETURN a, b"
	if err := Validate(query); err != nil {
		t.Fatalf("valid complex query must pass: %v", err)
	}
}

func TestANTLRValidate_RejectsGarbage(t *testing.T) {
	if err := Validate("!!!! not cypher at all !!!!"); err == nil {
		t.Fatal("garbage input must be rejected")
	}
}

// ---------------------------------------------------------------------------
// Parse – error reporting
// ---------------------------------------------------------------------------

func TestANTLRParse_ErrorIncludesDetails(t *testing.T) {
	_, err := Parse("MATCH (n RETURN")
	if err == nil {
		t.Fatal("malformed query must return error")
	}
	if len(err.Error()) == 0 {
		t.Fatal("error string should not be empty")
	}
}

// ---------------------------------------------------------------------------
// asciiCharStream – edge cases
// ---------------------------------------------------------------------------

func TestANTLRASCIICharStream_Edges(t *testing.T) {
	t.Run("GetText stop before start", func(t *testing.T) {
		s := newASCIICharStream("MATCH")
		if got := s.GetText(3, 1); got != "" {
			t.Fatalf("GetText(3,1) = %q, want empty", got)
		}
	})

	t.Run("LA past end is EOF", func(t *testing.T) {
		s := newASCIICharStream("M")
		if s.LA(2) != -1 {
			t.Fatalf("LA(2) on 1-char stream = %d, want TokenEOF(-1)", s.LA(2))
		}
	})

	t.Run("LA negative offsets", func(t *testing.T) {
		s := newASCIICharStream("ABC")
		s.Consume()
		s.Consume()
		if s.LA(-1) != int('B') {
			t.Fatalf("LA(-1) from pos 2 = %d, want 'B'", s.LA(-1))
		}
		if s.LA(-2) != int('A') {
			t.Fatalf("LA(-2) from pos 2 = %d, want 'A'", s.LA(-2))
		}
	})
}
