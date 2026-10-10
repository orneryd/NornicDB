package antlr

import (
	"fmt"
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
		{"create vector index on a bare property", "CREATE VECTOR INDEX doc_embedding IF NOT EXISTS FOR (n:Doc) ON n.embedding OPTIONS {indexConfig: {dimensions: 3}}"},
		{"create relationship vector index on a bare property", "CREATE VECTOR INDEX likes_emb FOR ()-[r:LIKES]-() ON r.emb OPTIONS {indexConfig: {`vector.dimensions`: 3}}"},
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

func sortedInterfaces(vals []interface{}) []interface{} {
	out := append([]interface{}(nil), vals...)
	sort.Slice(out, func(i, j int) bool {
		return fmt.Sprint(out[i]) < fmt.Sprint(out[j])
	})
	return out
}

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
