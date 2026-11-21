package buildinfo

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Source-based qualification for ownership and release-tracking rows.
// Runtime capability checks live with the packages that expose them.
func TestCloseoutL5Ownership(t *testing.T) {
	root := filepath.Join("..", "..")
	fset := token.NewFileSet()
	read := func(t *testing.T, path string) string {
		t.Helper()
		src, err := os.ReadFile(filepath.Join(root, path))
		require.NoError(t, err)
		return string(src)
	}
	find := func(t *testing.T, path, anchor string) {
		t.Helper()
		for i, line := range strings.Split(read(t, path), "\n") {
			if strings.Contains(line, anchor) {
				t.Logf("%s:%d %s", path, i+1, strings.TrimSpace(line))
				return
			}
		}
		t.Fatalf("%s does not contain %q", path, anchor)
	}
	for _, tc := range []struct {
		id      string
		anchors [][2]string
	}{
		{"P-N152-137", [][2]string{
			{"pkg/cypher/cache.go", "cypherantlr.ParseSyntax("},
			{"pkg/cypher/query_body_structure.go", "cypherantlr.ParseSyntax("},
			{"pkg/cypher/executor_query_routing.go", "antlr.ParseSyntax("},
		}},
		{"P-N152-127", [][2]string{
			{"pkg/storage/query_result_projection.go", ") NodeProperties("},
			{"pkg/storage/query_result_projection.go", ") EdgeProperties("},
			{"pkg/cypher/cache.go", "func transportResultShell("},
			{"pkg/storage/query_projection_scope.go", "func BeginQueryResultProjectionContext("},
		}},
		{"P-N152-126", [][2]string{
			{"internal/jsontext/append_string.go", "func AppendString("},
			{"pkg/observability/logging.go", "func appendEscapedJSONString("},
		}},
		{"P-N152-125", [][2]string{
			{"pkg/storage/projected_node_decoder.go", "func SplitNodeEnvelope("},
			{"pkg/storage/projected_node_decoder.go", "envelope, err := SplitNodeEnvelope("},
			{"pkg/storage/badger_helpers.go", "envelope, err := SplitNodeEnvelope("},
		}},
		{"P-N152-124", [][2]string{
			{"pkg/cypher/merge_node_writes.go", "func withMergeNodeWrites("},
			{"pkg/storage/merge_key_prepare.go", ") prepareMergeNode("},
		}},
		{"H-69", [][2]string{
			{"pkg/cypher/executor_mutations_where_eval.go", ") evaluateWhere("},
			{"pkg/cypher/functions_eval_math.go", ") evaluateQuantifierPredicate("},
			{"pkg/cypher/case_evaluation.go", "func runCaseExpression("},
		}},
		{"H-89", [][2]string{
			{"pkg/cypher/transaction_memory.go", "func TransactionMemoryPoolUsage("},
			{"pkg/cypher/transaction_memory.go", ") beginTransactionMemory("},
		}},
		{"H-67", [][2]string{
			{"pkg/nornicdb/embed_queue.go", ") embeddingPaused("},
			{"pkg/nornicdb/db_admin.go", ") PauseLifecycle("},
			{"pkg/server/server_router.go", "{\"/nornicdb/embed/stats\""},
		}},
		{"P-N151-34", [][2]string{
			{"pkg/config/config.go", "EmbeddingGPULayers int"},
			{"pkg/config/dbconfig/settings.go", "db.nornic.embedding.gpu.layers"},
		}},
		{"P-N152-1", [][2]string{
			{"CHANGELOG.md", "## [1.5.0]"},
			{"CHANGELOG.md", "## [1.5.1]"},
		}},
	} {
		t.Run(tc.id, func(t *testing.T) {
			for _, anchor := range tc.anchors {
				find(t, anchor[0], anchor[1])
			}
		})
	}
	t.Run("P-NGEN-15", func(t *testing.T) {
		owners := map[string]string{
			"pkg/cypher/call.go":                          "cloneStringInterfaceMap",
			"pkg/cypher/executor_mutations_where_eval.go":  "cloneStringAnyMap",
			"pkg/cypher/merge.go":                         "cloneNodePropertiesMap",
			"pkg/server/server_graph.go":                  "cloneInterfaceMap",
		}
		for path, name := range owners {
			f, err := parser.ParseFile(fset, path, read(t, path), 0)
			require.NoError(t, err)
			obj := f.Scope.Lookup(name)
			require.NotNil(t, obj)
			fn := obj.Decl.(*ast.FuncDecl)
			var rangeCopies int
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				if _, ok := n.(*ast.RangeStmt); ok {
					rangeCopies++
				}
				return true
			})
			t.Logf("%s %s range-copy-loops=%d", fset.Position(fn.Pos()), name, rangeCopies)
		}
	})
}

func TestCloseoutL5NamedSurfaces(t *testing.T) {
	root := filepath.Join("..", "..")
	// Scope: the shipping backend's named Go surfaces. Match exact
	// identifiers or a documented namespace. Test, docs, and generated
	// parser source cannot supply a positive product match.
	seen := make(map[string][]string)
	fset := token.NewFileSet()
	for _, dir := range []string{"cmd", "pkg", "internal", "apoc"} {
		err := filepath.WalkDir(filepath.Join(root, dir), func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			f, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution|parser.ParseComments)
			if err != nil {
				return err
			}
			if ast.IsGenerated(f) {
				return nil
			}
			handle := func(name string, position token.Pos) {
				key := strings.ToLower(name)
				if len(seen[key]) < 3 {
					seen[key] = append(seen[key], fset.Position(position).String())
				}
			}
			ast.Inspect(f, func(node ast.Node) bool {
				switch n := node.(type) {
				case *ast.FuncDecl:
					handle(n.Name.Name, n.Pos())
				case *ast.TypeSpec:
					handle(n.Name.Name, n.Pos())
				case *ast.BasicLit:
					if n.Kind == token.STRING && len(n.Value) < 90 {
						handle(strings.Trim(n.Value, "\"\x60"), n.Pos())
					}
				}
				return true
			})
			return nil
		})
		require.NoError(t, err)
	}
	require.Contains(t, seen, "db.index.fulltext.querynodes")
	require.Contains(t, seen, "/mcp/tools/list")
	require.Contains(t, seen, "gds.graph.project")
	require.Contains(t, seen, "executecypher")
	for _, tc := range []struct {
		id    string
		names []string
	}{
		{"P-N152-155", []string{"derivedsubgraph", "publishderivedsubgraph", "create derived subgraph"}},
		{"P-N152-154", []string{"create materialized view", "creatematerializedview", "incrementalaggregate"}},
		{"P-N152-144", []string{"okf", "importokf", "exportokf", "/okf/import", "/okf/export"}},
		{"P-N152-105", []string{"create graph view", "creategraphview", "graphviewbinding"}},
		{"P-N152-104", []string{"diffbykey", "historybykey", "db.temporal.diff", "db.temporal.asof"}},
		{"P-N151-34", []string{"nornicdb_embedding_split_mode", "nornicdb_heimdall_split_mode", "nornicdb_embedding_main_gpu", "nornicdb_embedding_tensor_split"}},
		{"P-N151-31", []string{"sdkfixture", "openfixture", "newtestfixture", "executecypher"}},
		{"P-N151-30", []string{"bindview", "diffview", "viewbinding"}},
		{"P-N151-29", []string{"repositorycapture", "capturerepository", "refreshrepository"}},
		{"P-N151-27", []string{"batchlifecycle", "pausebatch", "retrybatch"}},
		{"P-N151-26", []string{"selectiveexport", "upgradeproof", "apoc.export.json.query"}},
		{"P-N151-25", []string{"reviewrecord", "submitreview", "recordreview"}},
		{"P-N151-24", []string{"maintainsummary", "refreshsummary", "graphsummary"}},
		{"P-N151-23", []string{"blobstore", "ingestblob", "putblob"}},
		{"P-N151-22", []string{"publishsource", "sourcepublication", "sourcebinding"}},
		{"P-N151-21", []string{"recordcorrection", "correctionlifecycle", "revisionchecked"}},
		{"P-N151-20", []string{"identity.match", "resolveidentity", "apoc.refactor.mergenodes"}},
		{"P-N151-19", []string{"durablejob", "workflowlease", "joblease"}},
		{"H-67", []string{"db.embedding.pause", "db.embedding.resume", "/nornicdb/embed/pause", "/nornicdb/embed/resume", "/nornicdb/embed/stats"}},
		{"H-243", []string{"sourcespan", "placecitations", "batchcitations"}},
		{"H-254", []string{"mergechainterminals", "mergeterminals", "terminalsforids"}},
		{"H-261", []string{"text.intervals", "text.lines", "intervalindex", "textspanindex"}},
		{"H-262", []string{"keptworkclosure", "expandkeptworkclosure", "keptworkreferences"}},
	} {
		t.Run(tc.id, func(t *testing.T) {
			var present, missing []string
			for _, name := range tc.names {
				if len(seen[name]) > 0 {
					present = append(present, name+"="+strings.Join(seen[name], ","))
				} else {
					missing = append(missing, name)
				}
			}
			sort.Strings(present)
			t.Logf("L5 %s named surface present=[%s] absent=%v", tc.id, strings.Join(present, " "), missing)
		})
	}
}
