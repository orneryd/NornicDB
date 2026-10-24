package fabric

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPlan_UnionPlanningEntries(t *testing.T) {
	p := NewFabricPlanner(plannerUseCatalog())
	for _, op := range []string{"UNION", "UNION ALL"} {
		body := "USE cmp.a RETURN 'left' AS value " + op + " USE cmp.b RETURN 'right' AS value"
		for _, entry := range []struct {
			name   string
			query  string
			multi  bool
			shared bool
		}{
			{name: "Plan", query: body},
			{name: "planQuery", query: body, shared: true},
			{name: "CALL", query: "CALL { " + body + " } RETURN value"},
			{name: "nested_CALL", query: "CALL { CALL { " + body + " } RETURN value } RETURN value"},
			{name: "planMultiGraph", query: "CALL { " + body + " } RETURN value", multi: true},
			{name: "importing_WITH", query: "WITH 'seed' AS seed CALL { WITH seed USE cmp.a RETURN 'left' AS value " + op + " WITH seed USE cmp.b RETURN 'right' AS value } RETURN value"},
			{name: "USE_then_import", query: "WITH 'seed' AS seed CALL { USE cmp.a WITH seed RETURN 'left' AS value " + op + " USE cmp.b WITH seed RETURN 'right' AS value } RETURN value"},
		} {
			t.Run(op+"/"+entry.name, func(t *testing.T) {
				p.SetCallImportReader(nil)
				if entry.name == "importing_WITH" || entry.name == "USE_then_import" {
					read, _ := recordedCallImports("seed")
					p.SetCallImportReader(read)
				}
				var fragment Fragment
				var err error
				if entry.multi {
					blocks, extractErr := extractTopLevelCallBlocks(entry.query, nil)
					require.NoError(t, extractErr)
					fragment, err = p.planMultiGraph(planTarget{name: "cmp"}, "cmp", entry.query, blocks)
				} else if entry.shared {
					fragment, err = p.planQuery(entry.query, planTarget{name: "cmp"}, "cmp", true)
				} else {
					fragment, err = p.Plan(entry.query, "cmp")
				}
				require.NoError(t, err)
				var unions []*FragmentUnion
				var walk func(Fragment)
				walk = func(fragment Fragment) {
					switch f := fragment.(type) {
					case *FragmentApply:
						walk(f.Input)
						walk(f.Inner)
					case *FragmentUnion:
						unions = append(unions, f)
						walk(f.LHS)
						walk(f.RHS)
					}
				}
				walk(fragment)
				require.Len(t, unions, 1, "each entry must split the union before graph routing")
				u := unions[0]
				require.Equal(t, op == "UNION", u.Distinct)
				lhs := collectPlannedExecs(u.LHS)
				rhs := collectPlannedExecs(u.RHS)
				require.Len(t, lhs, 1)
				require.Len(t, rhs, 1)
				require.Equal(t, "cmp.a", lhs[0].GraphName)
				require.Equal(t, "cmp.b", rhs[0].GraphName)
				if entry.name == "importing_WITH" || entry.name == "USE_then_import" {
					for _, exec := range append(lhs, rhs...) {
						require.Equal(t, []string{"seed"}, exec.Input.(*FragmentInit).ImportColumns)
					}
				}
			})
		}
	}
}

func TestPlan_UnionBranchContext(t *testing.T) {
	p := NewFabricPlanner(plannerUseCatalog())
	for _, wrap := range []string{"%s", "CALL { %s } RETURN value"} {
		body := "USE cmp.a RETURN 'left' AS value UNION ALL RETURN 'inherited' AS value"
		query := strings.Replace(wrap, "%s", body, 1)
		fragment, err := p.Plan(query, "cmp")
		require.NoError(t, err)
		execs := collectPlannedExecs(fragment)
		require.GreaterOrEqual(t, len(execs), 2)
		require.Equal(t, "cmp.a", execs[0].GraphName)
		require.Equal(t, "cmp", execs[1].GraphName, "the first branch's USE must not change its sibling's default graph")
	}
}

func TestPlan_UnionUseContinuations(t *testing.T) {
	p := NewFabricPlanner(plannerUseCatalog())
	fragment, err := p.Plan("WITH 'left' AS value USE cmp.a RETURN value UNION ALL WITH 'right' AS value USE cmp.b RETURN value", "cmp")
	require.NoError(t, err)
	u, ok := fragment.(*FragmentUnion)
	require.True(t, ok, "expected union, got %T", fragment)
	for i, branch := range []Fragment{u.LHS, u.RHS} {
		execs := collectPlannedExecs(branch)
		require.Len(t, execs, 2)
		require.Equal(t, "cmp", execs[0].GraphName)
		require.Equal(t, []string{"cmp.a", "cmp.b"}[i], execs[1].GraphName)
	}
}

// A new recursive entry must not call the single-branch planner directly.
// Behavioral tests above establish that the shared entry actually splits unions;
// this guard prevents a future caller from quietly bypassing that boundary.
func TestPlannerUnionOwner_NoBypass(t *testing.T) {
	require.Empty(t, unionOwnerViolations(t, "."))
}

func unionOwnerViolations(t *testing.T, dir string) []string {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(dir, "*.go"))
	require.NoError(t, err)
	var violations []string
	fset := token.NewFileSet()
	splits := map[string]int{}
	owners := map[string]string{
		"NewConcatRowIterator":   "mergeUnion",
		"NewDistinctRowIterator": "mergeUnion",
		"splitTopLevelUnion":     "planQuery|singleGraphStatement",
		"planSingleQuery":        "planQuery|planCallBranch",
		"planCallBranch":         "planQuery",
		"ProjectionColumns":      "ResultColumns|validateUnionSchemas",
		"planMultiGraph":         "planSingleQuery",
		"UnionColumnOrder":       "validateUnionSchemas|mergeUnionRows",
	}
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, name, nil, 0)
		require.NoError(t, err)
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				var callee string
				switch fun := call.Fun.(type) {
				case *ast.SelectorExpr:
					callee = fun.Sel.Name
				case *ast.Ident:
					callee = fun.Name
				}
				if callee == "splitTopLevelUnion" {
					splits[fn.Name.Name]++
				}
				if owner, guarded := owners[callee]; guarded && !strings.Contains("|"+owner+"|", "|"+fn.Name.Name+"|") {
					violations = append(violations, fn.Name.Name+" calls "+callee+" outside "+owner)
				}
				return true
			})
		}
	}
	if splits["planQuery"] != 1 || splits["singleGraphStatement"] != 1 || len(splits) != 2 {
		violations = append(violations, "UNION splitting must use the shared splitter once in each planning and validation entry")
	}
	return violations
}

func TestPlannerUnionOwner_GuardMutations(t *testing.T) {
	source, err := os.ReadFile("planner.go")
	require.NoError(t, err)
	for _, mutation := range []struct{ name, before, after string }{
		{"public", "p.planQuery(trimmed, session, scope, false)", "p.planSingleQuery(trimmed, session, scope, false)"},
		{"call", "p.planQuery(subRest, sub, scope, true)", "p.planSingleQuery(block.Body, top, scope, true)"},
		{"use", "p.planQuery(subRest, sub, scope, true)", "p.planSingleQuery(subRest, sub, scope, true)"},
	} {
		t.Run(mutation.name, func(t *testing.T) {
			require.Contains(t, string(source), mutation.before)
			dir := t.TempDir()
			files, err := filepath.Glob("*.go")
			require.NoError(t, err)
			for _, path := range files {
				if strings.HasSuffix(path, "_test.go") {
					continue
				}
				data, err := os.ReadFile(path)
				require.NoError(t, err)
				if path == "planner.go" {
					data = []byte(strings.Replace(string(data), mutation.before, mutation.after, 1))
				}
				require.NoError(t, os.WriteFile(filepath.Join(dir, path), data, 0600))
			}
			require.NotEmpty(t, unionOwnerViolations(t, dir), "reintroduced entry bypass must be rejected")
		})
	}
}
