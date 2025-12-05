package cypher

import (
	"testing"
)

func TestASTBuilder_Build(t *testing.T) {
	builder := NewASTBuilder()

	tests := []struct {
		name         string
		query        string
		wantClauses  int
		wantType     QueryType
		wantReadOnly bool
		checkFunc    func(t *testing.T, ast *AST)
	}{
		{
			name:         "simple MATCH RETURN",
			query:        "MATCH (n:Person) RETURN n.name",
			wantClauses:  2,
			wantType:     QueryMatch,
			wantReadOnly: true,
			checkFunc: func(t *testing.T, ast *AST) {
				if ast.Clauses[0].Type != ASTClauseMatch {
					t.Errorf("First clause should be MATCH, got %v", ast.Clauses[0].Type)
				}
				if ast.Clauses[0].Match == nil {
					t.Error("Match clause should be populated")
				}
				if ast.Clauses[1].Type != ASTClauseReturn {
					t.Errorf("Second clause should be RETURN, got %v", ast.Clauses[1].Type)
				}
			},
		},
		{
			name:         "MATCH with WHERE",
			query:        "MATCH (n:Person) WHERE n.age > 21 RETURN n",
			wantClauses:  3,
			wantType:     QueryMatch,
			wantReadOnly: true,
			checkFunc: func(t *testing.T, ast *AST) {
				if ast.Clauses[1].Type != ASTClauseWhere {
					t.Errorf("Second clause should be WHERE, got %v", ast.Clauses[1].Type)
				}
				if ast.Clauses[1].Where == nil {
					t.Error("Where clause should be populated")
				}
			},
		},
		{
			name:         "CREATE node",
			query:        "CREATE (n:Person {name: 'Alice', age: 30})",
			wantClauses:  1,
			wantType:     QueryCreate,
			wantReadOnly: false,
			checkFunc: func(t *testing.T, ast *AST) {
				if ast.Clauses[0].Type != ASTClauseCreate {
					t.Errorf("First clause should be CREATE, got %v", ast.Clauses[0].Type)
				}
				if ast.Clauses[0].Create == nil {
					t.Error("Create clause should be populated")
				}
				if len(ast.Clauses[0].Create.Patterns) != 1 {
					t.Error("Should have one pattern")
				}
			},
		},
		{
			name:         "MERGE with ON CREATE SET",
			query:        "MERGE (n:Person {id: 1}) ON CREATE SET n.created = timestamp()",
			wantClauses:  1,
			wantType:     QueryMerge,
			wantReadOnly: false,
			checkFunc: func(t *testing.T, ast *AST) {
				if ast.Clauses[0].Merge == nil {
					t.Error("Merge clause should be populated")
				}
				if len(ast.Clauses[0].Merge.OnCreate) == 0 {
					t.Error("OnCreate should have items")
				}
			},
		},
		{
			name:         "DELETE",
			query:        "MATCH (n:Temp) DELETE n",
			wantClauses:  2,
			wantType:     QueryMatch,
			wantReadOnly: false,
			checkFunc: func(t *testing.T, ast *AST) {
				if ast.Clauses[1].Type != ASTClauseDelete {
					t.Errorf("Second clause should be DELETE, got %v", ast.Clauses[1].Type)
				}
				if ast.Clauses[1].Delete == nil {
					t.Error("Delete clause should be populated")
				}
				if len(ast.Clauses[1].Delete.Variables) != 1 || ast.Clauses[1].Delete.Variables[0] != "n" {
					t.Error("Delete should have variable 'n'")
				}
			},
		},
		{
			name:         "DETACH DELETE",
			query:        "MATCH (n:Temp) DETACH DELETE n",
			wantClauses:  2,
			wantType:     QueryMatch,
			wantReadOnly: false,
			checkFunc: func(t *testing.T, ast *AST) {
				if ast.Clauses[1].Type != ASTClauseDetachDelete {
					t.Errorf("Second clause should be DETACH DELETE, got %v", ast.Clauses[1].Type)
				}
				if !ast.Clauses[1].Delete.Detach {
					t.Error("Delete should have Detach=true")
				}
			},
		},
		{
			name:         "SET",
			query:        "MATCH (n:Person) SET n.updated = true, n.count = n.count + 1",
			wantClauses:  2,
			wantType:     QueryMatch,
			wantReadOnly: false,
			checkFunc: func(t *testing.T, ast *AST) {
				if ast.Clauses[1].Set == nil {
					t.Error("Set clause should be populated")
				}
				if len(ast.Clauses[1].Set.Items) != 2 {
					t.Errorf("Set should have 2 items, got %d", len(ast.Clauses[1].Set.Items))
				}
			},
		},
		{
			name:         "WITH clause",
			query:        "MATCH (n:Person) WITH n.name AS name, COUNT(*) AS cnt RETURN name, cnt",
			wantClauses:  3,
			wantType:     QueryMatch,
			wantReadOnly: true,
			checkFunc: func(t *testing.T, ast *AST) {
				if ast.Clauses[1].Type != ASTClauseWith {
					t.Errorf("Second clause should be WITH, got %v", ast.Clauses[1].Type)
				}
				if ast.Clauses[1].With == nil {
					t.Error("With clause should be populated")
				}
			},
		},
		{
			name:         "UNWIND",
			query:        "UNWIND [1, 2, 3] AS x RETURN x",
			wantClauses:  2,
			wantType:     QueryMatch, // Default when first clause isn't a write
			wantReadOnly: true,
			checkFunc: func(t *testing.T, ast *AST) {
				if ast.Clauses[0].Type != ASTClauseUnwind {
					t.Errorf("First clause should be UNWIND, got %v", ast.Clauses[0].Type)
				}
				if ast.Clauses[0].Unwind == nil {
					t.Error("Unwind clause should be populated")
				}
				if ast.Clauses[0].Unwind.Variable != "x" {
					t.Errorf("Unwind variable should be 'x', got '%s'", ast.Clauses[0].Unwind.Variable)
				}
			},
		},
		{
			name:         "ORDER BY LIMIT SKIP",
			query:        "MATCH (n:Person) RETURN n.name ORDER BY n.age DESC SKIP 10 LIMIT 5",
			wantClauses:  5,
			wantType:     QueryMatch,
			wantReadOnly: true,
			checkFunc: func(t *testing.T, ast *AST) {
				// Find ORDER BY clause
				var orderBy *ASTOrderBy
				var limit, skip *int64
				for _, c := range ast.Clauses {
					if c.OrderBy != nil {
						orderBy = c.OrderBy
					}
					if c.Limit != nil {
						limit = c.Limit
					}
					if c.Skip != nil {
						skip = c.Skip
					}
				}
				if orderBy == nil {
					t.Error("Should have ORDER BY")
				} else if len(orderBy.Items) != 1 || !orderBy.Items[0].Descending {
					t.Error("ORDER BY should be DESC")
				}
				if limit == nil || *limit != 5 {
					t.Errorf("LIMIT should be 5, got %v", limit)
				}
				if skip == nil || *skip != 10 {
					t.Errorf("SKIP should be 10, got %v", skip)
				}
			},
		},
		{
			name:         "CALL procedure",
			query:        "CALL db.labels() YIELD label RETURN label",
			wantClauses:  2,
			wantType:     QueryMatch,
			wantReadOnly: true,
			checkFunc: func(t *testing.T, ast *AST) {
				if ast.Clauses[0].Type != ASTClauseCall {
					t.Errorf("First clause should be CALL, got %v", ast.Clauses[0].Type)
				}
				if ast.Clauses[0].Call == nil {
					t.Error("Call clause should be populated")
				}
				if ast.Clauses[0].Call.Procedure != "db.labels" {
					t.Errorf("Procedure should be 'db.labels', got '%s'", ast.Clauses[0].Call.Procedure)
				}
				if len(ast.Clauses[0].Call.Yield) != 1 || ast.Clauses[0].Call.Yield[0] != "label" {
					t.Errorf("Yield should be ['label'], got %v", ast.Clauses[0].Call.Yield)
				}
			},
		},
		{
			name:         "OPTIONAL MATCH",
			query:        "MATCH (a:Person) OPTIONAL MATCH (a)-[:KNOWS]->(b) RETURN a, b",
			wantClauses:  3,
			wantType:     QueryMatch,
			wantReadOnly: true,
			checkFunc: func(t *testing.T, ast *AST) {
				if ast.Clauses[1].Type != ASTClauseOptionalMatch {
					t.Errorf("Second clause should be OPTIONAL MATCH, got %v", ast.Clauses[1].Type)
				}
				if ast.Clauses[1].Match == nil || !ast.Clauses[1].Match.Optional {
					t.Error("Match should have Optional=true")
				}
			},
		},
		{
			name:         "compound query",
			query:        "MATCH (a:Person), (b:Person) WHERE a.id = 1 AND b.id = 2 CREATE (a)-[:KNOWS]->(b) RETURN a, b",
			wantClauses:  4,
			wantType:     QueryMatch,
			wantReadOnly: false,
			checkFunc: func(t *testing.T, ast *AST) {
				if !ast.IsCompound {
					t.Error("Should be compound query")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ast, err := builder.Build(tt.query)
			if err != nil {
				t.Fatalf("Build() error = %v", err)
			}

			if len(ast.Clauses) != tt.wantClauses {
				t.Errorf("Got %d clauses, want %d", len(ast.Clauses), tt.wantClauses)
				for i, c := range ast.Clauses {
					t.Logf("  Clause %d: type=%v raw=%q", i, c.Type, c.RawText)
				}
			}

			if ast.QueryType != tt.wantType {
				t.Errorf("QueryType = %v, want %v", ast.QueryType, tt.wantType)
			}

			if ast.IsReadOnly != tt.wantReadOnly {
				t.Errorf("IsReadOnly = %v, want %v", ast.IsReadOnly, tt.wantReadOnly)
			}

			if tt.checkFunc != nil {
				tt.checkFunc(t, ast)
			}
		})
	}
}

func TestASTBuilder_ParseExpression(t *testing.T) {
	builder := NewASTBuilder()

	tests := []struct {
		name     string
		expr     string
		wantType ASTExprType
		check    func(t *testing.T, expr ASTExpression)
	}{
		{
			name:     "integer literal",
			expr:     "42",
			wantType: ASTExprLiteral,
			check: func(t *testing.T, expr ASTExpression) {
				if expr.Literal != int64(42) {
					t.Errorf("Expected int64(42), got %v (%T)", expr.Literal, expr.Literal)
				}
			},
		},
		{
			name:     "float literal",
			expr:     "3.14",
			wantType: ASTExprLiteral,
			check: func(t *testing.T, expr ASTExpression) {
				if expr.Literal != 3.14 {
					t.Errorf("Expected 3.14, got %v", expr.Literal)
				}
			},
		},
		{
			name:     "string literal",
			expr:     "'hello world'",
			wantType: ASTExprLiteral,
			check: func(t *testing.T, expr ASTExpression) {
				if expr.Literal != "hello world" {
					t.Errorf("Expected 'hello world', got %v", expr.Literal)
				}
			},
		},
		{
			name:     "boolean true",
			expr:     "true",
			wantType: ASTExprLiteral,
			check: func(t *testing.T, expr ASTExpression) {
				if expr.Literal != true {
					t.Errorf("Expected true, got %v", expr.Literal)
				}
			},
		},
		{
			name:     "null",
			expr:     "NULL",
			wantType: ASTExprLiteral,
			check: func(t *testing.T, expr ASTExpression) {
				if expr.Literal != nil {
					t.Errorf("Expected nil, got %v", expr.Literal)
				}
			},
		},
		{
			name:     "parameter",
			expr:     "$name",
			wantType: ASTExprParameter,
			check: func(t *testing.T, expr ASTExpression) {
				if expr.Parameter != "name" {
					t.Errorf("Expected 'name', got %v", expr.Parameter)
				}
			},
		},
		{
			name:     "property access",
			expr:     "n.name",
			wantType: ASTExprProperty,
			check: func(t *testing.T, expr ASTExpression) {
				if expr.Property == nil {
					t.Error("Property should not be nil")
				} else if expr.Property.Variable != "n" || expr.Property.Property != "name" {
					t.Errorf("Expected n.name, got %s.%s", expr.Property.Variable, expr.Property.Property)
				}
			},
		},
		{
			name:     "function call",
			expr:     "count(n)",
			wantType: ASTExprFunction,
			check: func(t *testing.T, expr ASTExpression) {
				if expr.Function == nil {
					t.Error("Function should not be nil")
				} else if expr.Function.Name != "count" {
					t.Errorf("Expected 'count', got %v", expr.Function.Name)
				}
			},
		},
		{
			name:     "function with DISTINCT",
			expr:     "count(DISTINCT n.type)",
			wantType: ASTExprFunction,
			check: func(t *testing.T, expr ASTExpression) {
				if expr.Function == nil {
					t.Error("Function should not be nil")
				} else if !expr.Function.Distinct {
					t.Error("Function should have Distinct=true")
				}
			},
		},
		{
			name:     "list literal",
			expr:     "[1, 2, 3]",
			wantType: ASTExprList,
			check: func(t *testing.T, expr ASTExpression) {
				if len(expr.List) != 3 {
					t.Errorf("Expected 3 items, got %d", len(expr.List))
				}
			},
		},
		{
			name:     "variable",
			expr:     "myVar",
			wantType: ASTExprVariable,
			check: func(t *testing.T, expr ASTExpression) {
				if expr.Variable != "myVar" {
					t.Errorf("Expected 'myVar', got %v", expr.Variable)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr := builder.parseExpression(tt.expr)

			if expr.Type != tt.wantType {
				t.Errorf("Type = %v, want %v", expr.Type, tt.wantType)
			}

			if tt.check != nil {
				tt.check(t, expr)
			}
		})
	}
}

func TestQueryInfo_GetAST(t *testing.T) {
	analyzer := NewQueryAnalyzer(100)

	t.Run("lazy AST building", func(t *testing.T) {
		info := analyzer.Analyze("MATCH (n:Person) WHERE n.age > 21 RETURN n.name")

		// AST should not be built yet
		if info.HasAST() {
			t.Error("AST should not be built before GetAST() call")
		}

		// Get AST
		ast := info.GetAST()
		if ast == nil {
			t.Fatal("AST should not be nil")
		}

		// AST should be built now
		if !info.HasAST() {
			t.Error("AST should be built after GetAST() call")
		}

		// Verify AST content
		if len(ast.Clauses) != 3 {
			t.Errorf("Expected 3 clauses, got %d", len(ast.Clauses))
		}

		// Second call should return same cached AST
		ast2 := info.GetAST()
		if ast != ast2 {
			t.Error("Second GetAST() should return cached AST")
		}
	})

	t.Run("cached analysis doesn't rebuild AST", func(t *testing.T) {
		query := "MATCH (n:Test) RETURN n"

		// First analysis
		info1 := analyzer.Analyze(query)
		ast1 := info1.GetAST()

		// Second analysis (should be cached)
		info2 := analyzer.Analyze(query)

		// Same QueryInfo should be returned
		if info1 != info2 {
			t.Error("Same query should return cached QueryInfo")
		}

		// AST should still be cached
		ast2 := info2.GetAST()
		if ast1 != ast2 {
			t.Error("AST should be cached in QueryInfo")
		}
	})
}

func BenchmarkASTBuilder_Build(b *testing.B) {
	builder := NewASTBuilder()
	queries := []string{
		"MATCH (n:Person) RETURN n.name",
		"MATCH (n:Person) WHERE n.age > 21 RETURN n.name, n.age ORDER BY n.age DESC LIMIT 10",
		"MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.name = 'Alice' RETURN b.name",
		"CREATE (n:Person {name: 'Bob', age: 30}) RETURN n",
		"MERGE (n:Person {id: 1}) ON CREATE SET n.created = timestamp() ON MATCH SET n.updated = timestamp()",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		query := queries[i%len(queries)]
		_, _ = builder.Build(query)
	}
}

func BenchmarkQueryInfo_GetAST_Cold(b *testing.B) {
	analyzer := NewQueryAnalyzer(100)
	query := "MATCH (n:Person) WHERE n.age > 21 RETURN n.name, n.age ORDER BY n.age DESC LIMIT 10"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Clear cache to force fresh analysis
		analyzer.ClearCache()
		info := analyzer.Analyze(query)
		_ = info.GetAST()
	}
}

func BenchmarkQueryInfo_GetAST_Warm(b *testing.B) {
	analyzer := NewQueryAnalyzer(100)
	query := "MATCH (n:Person) WHERE n.age > 21 RETURN n.name, n.age ORDER BY n.age DESC LIMIT 10"

	// Warm up
	info := analyzer.Analyze(query)
	_ = info.GetAST()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		info := analyzer.Analyze(query)
		_ = info.GetAST()
	}
}

// TestQueryAnalyzer_CacheConsistency verifies cache behaves correctly
func TestQueryAnalyzer_CacheConsistency(t *testing.T) {
	analyzer := NewQueryAnalyzer(100)

	t.Run("same query returns cached QueryInfo", func(t *testing.T) {
		query := "MATCH (n:Test) RETURN n"
		info1 := analyzer.Analyze(query)
		info2 := analyzer.Analyze(query)

		// Should be same pointer
		if info1 != info2 {
			t.Error("Same query should return cached QueryInfo")
		}
	})

	t.Run("whitespace normalized queries share cache", func(t *testing.T) {
		query1 := "MATCH (n:Test) RETURN n"
		query2 := "MATCH  (n:Test)  RETURN  n" // Extra spaces

		info1 := analyzer.Analyze(query1)
		info2 := analyzer.Analyze(query2)

		// Should be same after normalization
		if info1 != info2 {
			t.Error("Whitespace-normalized queries should share cache")
		}
	})

	t.Run("different queries have different cache entries", func(t *testing.T) {
		query1 := "MATCH (n:Person) RETURN n"
		query2 := "MATCH (n:Company) RETURN n"

		info1 := analyzer.Analyze(query1)
		info2 := analyzer.Analyze(query2)

		if info1 == info2 {
			t.Error("Different queries should have different QueryInfo")
		}

		// But metadata should differ
		if info1.Labels[0] == info2.Labels[0] {
			t.Error("Different labels should be detected")
		}
	})

	t.Run("cache eviction works", func(t *testing.T) {
		smallAnalyzer := NewQueryAnalyzer(3) // Very small cache

		// Fill cache
		smallAnalyzer.Analyze("MATCH (a:A) RETURN a")
		smallAnalyzer.Analyze("MATCH (b:B) RETURN b")
		smallAnalyzer.Analyze("MATCH (c:C) RETURN c")

		if smallAnalyzer.CacheSize() != 3 {
			t.Errorf("Cache should have 3 entries, got %d", smallAnalyzer.CacheSize())
		}

		// Add one more, should evict
		smallAnalyzer.Analyze("MATCH (d:D) RETURN d")

		if smallAnalyzer.CacheSize() != 3 {
			t.Errorf("Cache should still have 3 entries after eviction, got %d", smallAnalyzer.CacheSize())
		}
	})

	t.Run("ClearCache empties cache", func(t *testing.T) {
		localAnalyzer := NewQueryAnalyzer(100)
		localAnalyzer.Analyze("MATCH (n) RETURN n")
		localAnalyzer.Analyze("CREATE (n:Node)")

		if localAnalyzer.CacheSize() != 2 {
			t.Errorf("Expected 2 cached entries, got %d", localAnalyzer.CacheSize())
		}

		localAnalyzer.ClearCache()

		if localAnalyzer.CacheSize() != 0 {
			t.Errorf("Cache should be empty after clear, got %d", localAnalyzer.CacheSize())
		}
	})
}

// TestAST_ConcurrentAccess verifies thread safety
func TestAST_ConcurrentAccess(t *testing.T) {
	analyzer := NewQueryAnalyzer(100)
	query := "MATCH (n:Person) WHERE n.age > 21 RETURN n.name"

	// Pre-analyze to get cached QueryInfo
	info := analyzer.Analyze(query)

	// Concurrent GetAST calls
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func() {
			ast := info.GetAST()
			if ast == nil {
				t.Error("AST should not be nil")
			}
			if len(ast.Clauses) != 3 {
				t.Errorf("Expected 3 clauses, got %d", len(ast.Clauses))
			}
			done <- true
		}()
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}

	// Verify AST was only built once
	if !info.HasAST() {
		t.Error("AST should be built")
	}
}

// TestASTBuilder_EdgeCases tests edge cases in parsing
func TestASTBuilder_EdgeCases(t *testing.T) {
	builder := NewASTBuilder()

	tests := []struct {
		name  string
		query string
		check func(t *testing.T, ast *AST)
	}{
		{
			name:  "empty pattern",
			query: "MATCH () RETURN count(*)",
			check: func(t *testing.T, ast *AST) {
				if len(ast.Clauses) < 1 {
					t.Error("Should have at least 1 clause")
				}
			},
		},
		{
			name:  "multiple labels",
			query: "MATCH (n:Person:Employee:Manager) RETURN n",
			check: func(t *testing.T, ast *AST) {
				if ast.Clauses[0].Match == nil {
					t.Fatal("Match should be populated")
				}
				patterns := ast.Clauses[0].Match.Patterns
				if len(patterns) == 0 || len(patterns[0].Nodes) == 0 {
					t.Fatal("Should have node patterns")
				}
				// Check multiple labels parsed
				labels := patterns[0].Nodes[0].Labels
				if len(labels) < 2 {
					t.Errorf("Expected multiple labels, got %v", labels)
				}
			},
		},
		{
			name:  "relationship with variable",
			query: "MATCH (a)-[r:KNOWS]->(b) RETURN r",
			check: func(t *testing.T, ast *AST) {
				if ast.Clauses[0].Match == nil {
					t.Fatal("Match should be populated")
				}
				patterns := ast.Clauses[0].Match.Patterns
				if len(patterns) == 0 {
					t.Fatal("Should have patterns")
				}
				// Check relationship parsed
				rels := patterns[0].Relationships
				if len(rels) == 0 {
					t.Error("Should have relationships")
				}
			},
		},
		{
			name:  "RETURN with multiple aliases",
			query: "MATCH (n) RETURN n.name AS name, n.age AS age, count(*) AS total",
			check: func(t *testing.T, ast *AST) {
				for _, c := range ast.Clauses {
					if c.Return != nil {
						if len(c.Return.Items) != 3 {
							t.Errorf("Expected 3 return items, got %d", len(c.Return.Items))
						}
						// Check aliases
						aliases := []string{}
						for _, item := range c.Return.Items {
							if item.Alias != "" {
								aliases = append(aliases, item.Alias)
							}
						}
						if len(aliases) != 3 {
							t.Errorf("Expected 3 aliases, got %v", aliases)
						}
						return
					}
				}
				t.Error("RETURN clause not found")
			},
		},
		{
			name:  "nested function calls",
			query: "RETURN toUpper(substring(n.name, 0, 5))",
			check: func(t *testing.T, ast *AST) {
				for _, c := range ast.Clauses {
					if c.Return != nil && len(c.Return.Items) > 0 {
						expr := c.Return.Items[0].Expression
						if expr.Type != ASTExprFunction {
							t.Error("Expected function expression")
						}
						if expr.Function == nil || expr.Function.Name != "toUpper" {
							t.Error("Expected toUpper function")
						}
						return
					}
				}
				t.Error("RETURN clause not found")
			},
		},
		{
			name:  "CASE expression in RETURN",
			query: "RETURN CASE WHEN n.age > 18 THEN 'adult' ELSE 'minor' END",
			check: func(t *testing.T, ast *AST) {
				// Just verify it parses without error
				if len(ast.Clauses) < 1 {
					t.Error("Should have at least 1 clause")
				}
			},
		},
		{
			name:  "property with special characters in string",
			query: "MATCH (n) WHERE n.email = 'test@example.com' RETURN n",
			check: func(t *testing.T, ast *AST) {
				for _, c := range ast.Clauses {
					if c.Where != nil {
						if c.Where.RawText == "" {
							t.Error("WHERE raw text should be captured")
						}
						return
					}
				}
				t.Error("WHERE clause not found")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ast, err := builder.Build(tt.query)
			if err != nil {
				t.Fatalf("Build() error = %v", err)
			}
			tt.check(t, ast)
		})
	}
}
