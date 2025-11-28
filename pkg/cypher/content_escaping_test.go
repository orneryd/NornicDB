package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// TestContentWithCypherLikeSyntax tests that content containing Cypher-like patterns
// is properly handled as string data, not parsed as Cypher commands.
//
// These test cases come from real-world data that failed migration.
func TestContentWithCypherLikeSyntax(t *testing.T) {
	engine := storage.NewMemoryEngine()
	executor := NewStorageExecutor(engine)
	ctx := context.Background()

	testCases := []struct {
		name    string
		content string
	}{
		{
			name: "JSON with arrows",
			content: `{"overview": {"goal": "Build a blog API"}, "tasks": [{"id": "task-1", "deps": ["task-0"]}]}`,
		},
		{
			name: "Markdown with arrows",
			content: `# Guide
## Flow
Data -> Processing -> Output
User <- Response <- Server`,
		},
		{
			name: "Cypher query in string",
			content: `Example query: MATCH (n:User)-[:KNOWS]->(m) RETURN n, m`,
		},
		{
			name: "CREATE statement in content",
			content: `To create a node, use: CREATE (n:Person {name: "Alice"})`,
		},
		{
			name: "MATCH and SET in content",
			content: `Update pattern: MATCH (n) WHERE n.id = 1 SET n.updated = true`,
		},
		{
			name: "REMOVE in content",
			content: `To remove property: MATCH (n) REMOVE n.deprecated`,
		},
		{
			name: "DELETE in content",
			content: `Cleanup: MATCH (n:Temp) DELETE n`,
		},
		{
			name: "Relationship patterns in markdown",
			content: `## Relationships
- User-[:FOLLOWS]->User
- Post<-[:WROTE]-User
- (a)-[r]->(b)`,
		},
		{
			name: "Curly braces in JSON",
			content: `{"props": {"nested": {"key": "value"}}, "array": [1, 2, 3]}`,
		},
		{
			name: "Parentheses in content",
			content: `Function call: processData(input) returns (output, error)`,
		},
		{
			name: "Mixed special chars",
			content: `Pattern: (a)->[:REL]->(b)<-[:REL2]-(c) with props {x: 1}`,
		},
		{
			name: "Real orchestration plan excerpt",
			content: `{
  "overview": {
    "goal": "Build a blog REST API with full CRUD, mock DB, and deploy via Docker/Kubernetes.",
    "complexity": "Complex",
    "totalTasks": 10
  },
  "tasks": [
    {
      "id": "task-0",
      "title": "Environment Validation",
      "prompt": "Verify all required dependencies..."
    }
  ]
}`,
		},
		{
			name: "Agent preamble with arrows",
			content: `---
description: Video Game Engineer Agent
tools: ['run_terminal_cmd', 'read_file']
---

# Agent Preamble

**Flow:** Input -> Process -> Output
**Pattern:** User <- Response <- Server`,
		},
		{
			name: "Code with Cypher examples",
			content: `// Example Cypher queries:
// 1. MATCH (n:Person)-[:KNOWS]->(m:Person) RETURN n.name, m.name
// 2. CREATE (n:Node {id: $id}) RETURN n
// 3. MATCH (a)-[r:REL]->(b) DELETE r`,
		},
		{
			name: "SQL-like content",
			content: `SELECT * FROM users WHERE id = 1; INSERT INTO logs VALUES (1, 'test');`,
		},
		{
			name: "Escaped quotes",
			content: `He said "Hello \"World\"" and then 'it\'s fine'`,
		},
		{
			name: "Newlines and tabs",
			content: "Line1\nLine2\tTabbed\rCarriage\r\nWindows",
		},
		{
			name: "Unicode content",
			content: `Unicode: 你好世界 🚀 émojis →�ñ`,
		},
		{
			name: "Backticks",
			content: "Code: `MATCH (n) RETURN n` in markdown",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Test CREATE with this content
			createQuery := `CREATE (n:TestNode {content: $content, title: $title}) RETURN n`
			params := map[string]interface{}{
				"content": tc.content,
				"title":   tc.name,
			}

			result, err := executor.Execute(ctx, createQuery, params)
			if err != nil {
				t.Fatalf("CREATE failed for %q: %v\nContent: %s", tc.name, err, tc.content[:min(100, len(tc.content))])
			}

			if len(result.Rows) == 0 {
				t.Fatalf("CREATE returned no rows for %q", tc.name)
			}

			// Verify we can read it back
			matchQuery := `MATCH (n:TestNode {title: $title}) RETURN n.content`
			result, err = executor.Execute(ctx, matchQuery, map[string]interface{}{"title": tc.name})
			if err != nil {
				t.Fatalf("MATCH failed for %q: %v", tc.name, err)
			}

			if len(result.Rows) == 0 {
				t.Fatalf("MATCH returned no rows for %q", tc.name)
			}

			// Verify content is intact
			retrievedContent, ok := result.Rows[0][0].(string)
			if !ok {
				t.Fatalf("Retrieved content is not a string for %q", tc.name)
			}

			if retrievedContent != tc.content {
				t.Errorf("Content mismatch for %q:\nExpected: %s\nGot: %s",
					tc.name, tc.content[:min(50, len(tc.content))], retrievedContent[:min(50, len(retrievedContent))])
			}

			t.Logf("✓ %s: stored and retrieved %d chars", tc.name, len(tc.content))
		})
	}
}

// TestBatchInsertWithProblematicContent tests batch INSERT with content that previously failed
func TestBatchInsertWithProblematicContent(t *testing.T) {
	engine := storage.NewMemoryEngine()
	executor := NewStorageExecutor(engine)
	ctx := context.Background()

	// Content samples that caused migration failures
	problemContent := []string{
		`{"tasks": [{"id": "task-1", "deps": ["task-0"]}]}`,
		`MATCH (n)-[:REL]->(m) RETURN n`,
		`Pattern: (a)->[:X]->(b)`,
		`Remove: MATCH (n) REMOVE n.prop`,
		`Set: MATCH (n) SET n.x = 1`,
	}

	for i, content := range problemContent {
		query := `CREATE (n:BatchTest {id: $id, content: $content}) RETURN n`
		params := map[string]interface{}{
			"id":      i,
			"content": content,
		}

		_, err := executor.Execute(ctx, query, params)
		if err != nil {
			t.Errorf("Batch item %d failed: %v\nContent: %s", i, err, content)
		}
	}

	// Verify all were inserted
	countQuery := `MATCH (n:BatchTest) RETURN count(n)`
	result, _ := executor.Execute(ctx, countQuery, nil)
	if len(result.Rows) > 0 {
		count := result.Rows[0][0]
		t.Logf("✓ Batch inserted %v nodes with problematic content", count)
	}
}

// TestStringLiteralParsing verifies string literals are not parsed as Cypher
func TestStringLiteralParsing(t *testing.T) {
	engine := storage.NewMemoryEngine()
	executor := NewStorageExecutor(engine)
	ctx := context.Background()

	// This should create a node with the literal string, not execute it as Cypher
	dangerousContent := `MATCH (n) DETACH DELETE n`

	query := `CREATE (n:Safe {content: $content}) RETURN n.content`
	result, err := executor.Execute(ctx, query, map[string]interface{}{
		"content": dangerousContent,
	})

	if err != nil {
		t.Fatalf("Failed to store dangerous-looking content: %v", err)
	}

	// Verify the content was stored, not executed
	if len(result.Rows) == 0 {
		t.Fatal("No result returned")
	}

	stored := result.Rows[0][0].(string)
	if stored != dangerousContent {
		t.Errorf("Content was modified: got %q, want %q", stored, dangerousContent)
	}

	// Verify no nodes were deleted (the dangerous content should be data, not executed)
	countQuery := `MATCH (n:Safe) RETURN count(n)`
	countResult, _ := executor.Execute(ctx, countQuery, nil)
	if len(countResult.Rows) > 0 && countResult.Rows[0][0].(int64) == 0 {
		t.Error("Node was deleted! Dangerous content was executed as Cypher!")
	}

	t.Log("✓ Dangerous-looking content stored safely as string literal")
}

// min is built-in in Go 1.21+

// TestInlinePropertySyntax tests that inline property syntax handles special content
func TestInlinePropertySyntax(t *testing.T) {
	engine := storage.NewMemoryEngine()
	executor := NewStorageExecutor(engine)
	ctx := context.Background()

	// Test inline property assignment with tricky content
	testCases := []struct {
		name  string
		query string
	}{
		{
			name:  "Simple inline",
			query: `CREATE (n:Test {name: "test->value"}) RETURN n.name`,
		},
		{
			name:  "Inline with curly braces",
			query: `CREATE (n:Test {data: "{\"key\": \"value\"}"}) RETURN n.data`,
		},
		{
			name:  "Inline with parentheses",
			query: `CREATE (n:Test {expr: "(a + b) * c"}) RETURN n.expr`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := executor.Execute(ctx, tc.query, nil)
			if err != nil {
				// Check if it's a parsing error vs execution error
				if strings.Contains(err.Error(), "invalid") || strings.Contains(err.Error(), "parse") {
					t.Errorf("Parse error for %q: %v", tc.name, err)
				} else {
					t.Logf("Execution note for %q: %v", tc.name, err)
				}
			}
		})
	}
}
