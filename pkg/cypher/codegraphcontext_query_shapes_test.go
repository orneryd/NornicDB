package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupCodeGraphContextRepositoryGraph(t *testing.T, exec *StorageExecutor, ctx context.Context) string {
	t.Helper()

	repoPath := "/tmp/codegraphcontext-repo"
	queries := []struct {
		query  string
		params map[string]any
	}{
		{
			query:  `CREATE (r:Repository {path: $repo_path, name: 'repo'})`,
			params: map[string]any{"repo_path": repoPath},
		},
		{
			query: `
MATCH (r:Repository {path: $repo_path})
CREATE (d:Directory {path: $dir_path, name: 'src'})
CREATE (r)-[:CONTAINS]->(d)
`,
			params: map[string]any{"repo_path": repoPath, "dir_path": repoPath + "/src"},
		},
		{
			query: `
MATCH (d:Directory {path: $dir_path})
CREATE (f:File {path: $file_path, name: 'service.py', relative_path: 'src/service.py'})
CREATE (d)-[:CONTAINS]->(f)
`,
			params: map[string]any{"dir_path": repoPath + "/src", "file_path": repoPath + "/src/service.py"},
		},
		{
			query: `
MATCH (f:File {path: $file_path})
CREATE (fn:Function {name: 'render_graph', path: $file_path, line_number: 10, source: 'def render_graph(): pass', docstring: 'render graph'})
CREATE (cls:Class {name: 'GraphRenderer', path: $file_path, line_number: 2, source: 'class GraphRenderer: pass', docstring: 'renderer'})
CREATE (f)-[:CONTAINS]->(fn)
CREATE (f)-[:CONTAINS]->(cls)
`,
			params: map[string]any{"file_path": repoPath + "/src/service.py"},
		},
		{
			query: `
MATCH (f:File {path: $file_path})
CREATE (m:Module {name: 'neo4j', full_import_name: 'neo4j', lang: 'python'})
CREATE (f)-[:IMPORTS {imported_name: 'neo4j', line_number: 1}]->(m)
`,
			params: map[string]any{"file_path": repoPath + "/src/service.py"},
		},
	}

	for _, step := range queries {
		_, err := exec.Execute(ctx, strings.TrimSpace(step.query), step.params)
		require.NoError(t, err)
	}

	return repoPath
}

// TestCodeGraphContextQueriesParseWithExplain validates exact Cypher statement
// shapes emitted by CodeGraphContext against both parsers. This provides a
// compatibility contract without needing every query to execute end-to-end.
func TestCodeGraphContextQueriesParseWithExplain(t *testing.T) {
	// Not parallel: its subtests switch the process-wide parser.

	store := newTestMemoryEngine(t)
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	repoPath := "/tmp/codegraphcontext-repo"
	searchTerm := "graph"
	formattedSearchTerm := "name:graph"

	testCases := []struct {
		name   string
		query  string
		params map[string]any
	}{
		{
			name: "function fulltext search scoped to repo path",
			query: `
CALL db.index.fulltext.queryNodes("code_search_index", $search_term) YIELD node, score
WITH node, score
WHERE node:Function AND node.path STARTS WITH $repo_path
RETURN node.name as name, node.path as path, node.line_number as line_number,
       node.source as source, node.docstring as docstring, node.is_dependency as is_dependency
ORDER BY score DESC
LIMIT 20
`,
			params: map[string]any{"search_term": formattedSearchTerm, "repo_path": repoPath},
		},
		{
			name: "content fulltext search with CASE projection and parent file match",
			query: `
CALL db.index.fulltext.queryNodes("code_search_index", $search_term) YIELD node, score
WITH node, score
WHERE (node:Function OR node:Class OR node:Variable) AND node.path STARTS WITH $repo_path
MATCH (node)<-[:CONTAINS]-(f:File)
RETURN
    CASE
        WHEN node:Function THEN 'function'
        WHEN node:Class THEN 'class'
        ELSE 'variable'
    END as type,
    node.name as name, f.path as path,
    node.line_number as line_number, node.source as source,
    node.docstring as docstring, node.is_dependency as is_dependency
ORDER BY score DESC
LIMIT 20
`,
			params: map[string]any{"search_term": searchTerm, "repo_path": repoPath},
		},
		{
			name: "repository scoped visualization query",
			query: `
MATCH (r:Repository {path: $repo_path})
OPTIONAL MATCH (r)-[:CONTAINS*0..]->(n)
WITH DISTINCT n
WHERE n IS NOT NULL
OPTIONAL MATCH (n)-[rel]->(m)
RETURN n, rel, m
`,
			params: map[string]any{"repo_path": repoPath},
		},
		{
			name:   "repository file count query",
			query:  `MATCH (r:Repository {path: $path})-[:CONTAINS*]->(f:File) RETURN count(f) as c`,
			params: map[string]any{"path": repoPath},
		},
		{
			name:   "repository import count query",
			query:  `MATCH (r:Repository {path: $path})-[:CONTAINS*]->(f:File)-[:IMPORTS]->(m:Module) RETURN count(DISTINCT m) as c`,
			params: map[string]any{"path": repoPath},
		},
		{
			name: "function parameter merge query",
			query: `
MATCH (fn:Function {name: $func_name, path: $path, line_number: $line_number})
MERGE (p:Parameter {name: $arg_name, path: $path, function_line_number: $line_number})
MERGE (fn)-[:HAS_PARAMETER]->(p)
`,
			params: map[string]any{
				"func_name":   "render_graph",
				"path":        repoPath + "/src/service.py",
				"line_number": int64(10),
				"arg_name":    "repo_path",
			},
		},
	}

	run := func(t *testing.T) {
		for _, tc := range testCases {
			tc := tc
			t.Run(tc.name, func(t *testing.T) {
				_, err := exec.Execute(ctx, "EXPLAIN "+strings.TrimSpace(tc.query), tc.params)
				require.NoError(t, err)
			})
		}
	}

	t.Run("nornic_parser", func(t *testing.T) {
		cleanup := config.WithNornicParser()
		defer cleanup()
		run(t)
	})

	t.Run("antlr_parser", func(t *testing.T) {
		cleanup := config.WithANTLRParser()
		defer cleanup()
		run(t)
	})
}

func TestCodeGraphContextRepositoryStatsQueries(t *testing.T) {
	t.Parallel()

	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	repoPath := setupCodeGraphContextRepositoryGraph(t, exec, ctx)

	testCases := []struct {
		name     string
		query    string
		expected int64
	}{
		{
			name:     "counts files in repository tree",
			query:    `MATCH (r:Repository {path: $path})-[:CONTAINS*]->(f:File) RETURN count(f) as c`,
			expected: 1,
		},
		{
			name:     "counts functions in repository tree",
			query:    `MATCH (r:Repository {path: $path})-[:CONTAINS*]->(func:Function) RETURN count(func) as c`,
			expected: 1,
		},
		{
			name:     "counts classes in repository tree",
			query:    `MATCH (r:Repository {path: $path})-[:CONTAINS*]->(cls:Class) RETURN count(cls) as c`,
			expected: 1,
		},
		{
			name:     "counts distinct imported modules in repository tree",
			query:    `MATCH (r:Repository {path: $path})-[:CONTAINS*]->(f:File)-[:IMPORTS]->(m:Module) RETURN count(DISTINCT m) as c`,
			expected: 1,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			result, err := exec.Execute(ctx, tc.query, map[string]any{"path": repoPath})
			require.NoError(t, err)
			require.Len(t, result.Rows, 1)
			require.Len(t, result.Rows[0], 1)

			count, ok := result.Rows[0][0].(int64)
			require.True(t, ok, "count should be int64, got %T", result.Rows[0][0])
			assert.Equal(t, tc.expected, count)
		})
	}
}

func TestCodeGraphContextParameterMergeQuery(t *testing.T) {
	t.Parallel()

	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	repoPath := setupCodeGraphContextRepositoryGraph(t, exec, ctx)
	filePath := repoPath + "/src/service.py"

	query := `
MATCH (fn:Function {name: $func_name, path: $path, line_number: $line_number})
MERGE (p:Parameter {name: $arg_name, path: $path, function_line_number: $line_number})
MERGE (fn)-[:HAS_PARAMETER]->(p)
`
	params := map[string]any{
		"func_name":   "render_graph",
		"path":        filePath,
		"line_number": int64(10),
		"arg_name":    "repo_path",
	}

	_, err := exec.Execute(ctx, strings.TrimSpace(query), params)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, strings.TrimSpace(query), params)
	require.NoError(t, err, "MERGE form should remain idempotent on repeated execution")

	countResult, err := exec.Execute(ctx, `
MATCH (:Function {name: $func_name, path: $path, line_number: $line_number})-[:HAS_PARAMETER]->(p:Parameter)
RETURN count(p) as c
`, params)
	require.NoError(t, err)
	require.Len(t, countResult.Rows, 1)

	count, ok := countResult.Rows[0][0].(int64)
	require.True(t, ok, "count should be int64, got %T", countResult.Rows[0][0])
	assert.Equal(t, int64(1), count)

	parameterResult, err := exec.Execute(ctx, `
MATCH (:Function {name: $func_name, path: $path, line_number: $line_number})-[:HAS_PARAMETER]->(p:Parameter)
RETURN p.name as name, p.function_line_number as function_line_number
`, params)
	require.NoError(t, err)
	require.Len(t, parameterResult.Rows, 1)
	require.Len(t, parameterResult.Rows[0], 2)
	assert.Equal(t, "repo_path", parameterResult.Rows[0][0])
	assert.Equal(t, int64(10), parameterResult.Rows[0][1])
}

func TestCodeGraphContextRepoVisualizationQuery(t *testing.T) {
	t.Parallel()

	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	repoPath := setupCodeGraphContextRepositoryGraph(t, exec, ctx)
	assert.NotEmpty(t, repoPath)

	result, err := exec.Execute(ctx, strings.TrimSpace(`
MATCH (r:Repository {path: $repo_path})
OPTIONAL MATCH (r)-[:CONTAINS*0..]->(n)
WITH DISTINCT n
WHERE n IS NOT NULL
OPTIONAL MATCH (n)-[rel]->(m)
RETURN n, rel, m
`), map[string]any{"repo_path": repoPath})
	require.NoError(t, err)
	assert.Equal(t, []string{"n", "rel", "m"}, result.Columns)
	assert.NotEmpty(t, result.Rows, "repo visualization query should return repository subgraph rows")

	nonNilNodes := 0
	labelsSeen := map[string]bool{}
	for _, row := range result.Rows {
		require.Len(t, row, 3)
		if row[0] != nil {
			node, ok := row[0].(*storage.Node)
			assert.True(t, ok, "n should be a node when present, got %T", row[0])
			if ok {
				nonNilNodes++
				for _, label := range node.Labels {
					labelsSeen[label] = true
				}
			}
		}
		if row[1] != nil {
			_, ok := row[1].(*storage.Edge)
			assert.True(t, ok, "rel should be an edge when present, got %T", row[1])
		}
		if row[2] != nil {
			node, ok := row[2].(*storage.Node)
			assert.True(t, ok, "m should be a node when present, got %T", row[2])
			if ok {
				nonNilNodes++
				for _, label := range node.Labels {
					labelsSeen[label] = true
				}
			}
		}
	}

	assert.GreaterOrEqual(t, nonNilNodes, 4, "expected repository visualization query to surface multiple nodes")
	assert.True(t, labelsSeen["Directory"], "expected visualization query to include directory nodes")
	assert.True(t, labelsSeen["File"], "expected visualization query to include file nodes")
}
