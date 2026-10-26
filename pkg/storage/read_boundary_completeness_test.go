package storage_test

import (
	"fmt"
	"go/ast"
	"go/constant"
	"go/token"
	"go/types"
	"sort"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/internal/capabilitycensus"
	"github.com/stretchr/testify/require"
)

type declaredBoundaryMethod struct {
	method      string
	disposition int64
	scope       string
}

// Check the compiled method set, including promoted methods, rather than an
// inventory of known reads. This is deliberately stronger than checking named
// interfaces: it also covers anonymous and combined consumer capabilities and
// a new method before its first consumer lands. Core Engine operations and the
// discovery protocol itself have distinct contracts.
func boundaryPolicyProblems(methods *types.MethodSet, core *types.Interface, declarations []declaredBoundaryMethod) []string {
	protocol := map[string]bool{"ReadCapabilitySource": true, "ReadCapabilityScoped": true, "ReadCapabilityAvailable": true, "ReadCapabilityPolicy": true, "ReadCapabilityBinding": true, "PublishReadCapabilityBinding": true, "CapabilityOwnerSource": true, "StorageMembership": true, "EngineCapabilityPolicy": true}
	expected := map[string]bool{}
	for i := 0; i < methods.Len(); i++ {
		method := methods.At(i).Obj().(*types.Func)
		if !method.Exported() || protocol[method.Name()] {
			continue
		}
		ordinary := false
		for j := 0; j < core.NumMethods(); j++ {
			if method.Name() == core.Method(j).Name() && types.Identical(method.Type(), core.Method(j).Type()) {
				ordinary = true
				break
			}
		}
		if !ordinary {
			expected[method.Name()] = true
		}
	}
	counts := map[string]int{}
	var problems []string
	for _, declaration := range declarations {
		counts[declaration.method]++
		if !expected[declaration.method] {
			problems = append(problems, "stale declaration: "+declaration.method)
		}
		if declaration.disposition < 1 || declaration.disposition > 3 || strings.TrimSpace(declaration.scope) == "" {
			problems = append(problems, "missing disposition/scope/reason: "+declaration.method)
		}
	}
	for method := range expected {
		if counts[method] != 1 {
			problems = append(problems, fmt.Sprintf("%s: expected exactly one declaration, got %d", method, counts[method]))
		}
	}
	sort.Strings(problems)
	return problems
}

func TestReadBoundaryPolicyCompleteness(t *testing.T) {
	result, err := capabilitycensus.Discover("../..")
	require.NoError(t, err)
	loaded, fset, err := capabilitycensus.Packages("../..")
	require.NoError(t, err)
	require.NotEmpty(t, loaded)
	var source, core *types.Interface
	for _, pkg := range loaded {
		require.Empty(t, pkg.Errors, "complete production types required: %s", pkg.PkgPath)
		if pkg.PkgPath == "github.com/orneryd/nornicdb/pkg/storage" {
			source = pkg.Types.Scope().Lookup("ReadCapabilitySource").Type().Underlying().(*types.Interface)
			core = pkg.Types.Scope().Lookup("Engine").Type().Underlying().(*types.Interface)
		}
	}
	require.NotNil(t, source)
	require.NotNil(t, core)
	boundaries := 0
	for _, pkg := range loaded {
		initializers := map[types.Object]ast.Expr{}
		policies := map[*types.Named]*ast.FuncDecl{}
		positions := map[*types.Named]token.Pos{}
		for _, member := range result.Inventory.Members {
			if member.Syntax == nil || member.Syntax.Info != pkg.TypesInfo || member.Kind != "declaration" {
				continue
			}
			node := member.Syntax.Node
			switch n := node.(type) {
			case *ast.ValueSpec:
				for i, id := range n.Names {
					if i < len(n.Values) {
						initializers[pkg.TypesInfo.Defs[id]] = n.Values[i]
					}
				}
			case *ast.TypeSpec:
				if obj := pkg.TypesInfo.Defs[n.Name]; obj != nil {
					if named, ok := obj.Type().(*types.Named); ok {
						positions[named] = n.Pos()
					}
				}
			case *ast.FuncDecl:
				if n.Name.Name == "ReadCapabilityPolicy" && n.Recv != nil {
					obj := pkg.TypesInfo.Defs[n.Name].(*types.Func)
					receiver := obj.Type().(*types.Signature).Recv().Type()
					if ptr, ok := receiver.(*types.Pointer); ok {
						receiver = ptr.Elem()
					}
					policies[receiver.(*types.Named)] = n
				}
			}
		}

		for named, position := range positions {
			pointer := types.NewPointer(named)
			if !types.Implements(pointer, source) || !types.Implements(pointer, core) {
				continue
			}
			boundaries++
			t.Run(pkg.PkgPath+"."+named.Obj().Name(), func(t *testing.T) {
				fn := policies[named]
				require.NotNil(t, fn, "every declared read boundary/decorator needs its own policy")
				require.Equal(t, fset.Position(position).Filename, fset.Position(fn.Pos()).Filename, "declare the policy beside the boundary type")
				require.Len(t, fn.Body.List, 1)
				ret, ok := fn.Body.List[0].(*ast.ReturnStmt)
				require.True(t, ok)
				require.Len(t, ret.Results, 1)
				expression := ret.Results[0]
				if call, ok := expression.(*ast.CallExpr); ok {
					selector, ok := call.Fun.(*ast.SelectorExpr)
					require.True(t, ok)
					require.Equal(t, "Copy", selector.Sel.Name)
					expression = selector.X
				}
				if id, ok := expression.(*ast.Ident); ok {
					expression = initializers[pkg.TypesInfo.Uses[id]]
				}
				literal, ok := expression.(*ast.CompositeLit)
				require.True(t, ok, "policy must be a static declaration")
				var declarations []declaredBoundaryMethod
				for _, element := range literal.Elts {
					rule, ok := element.(*ast.CompositeLit)
					require.True(t, ok)
					var methods, scope string
					var disposition int64
					for _, field := range rule.Elts {
						pair, ok := field.(*ast.KeyValueExpr)
						require.True(t, ok)
						value := pkg.TypesInfo.Types[pair.Value].Value
						require.NotNil(t, value, "declaration fields must be constants")
						switch pair.Key.(*ast.Ident).Name {
						case "Methods":
							methods = constant.StringVal(value)
						case "Scope":
							scope = constant.StringVal(value)
						case "Disposition":
							disposition, ok = constant.Int64Val(value)
							require.True(t, ok)
						default:
							t.Fatal("unknown declaration field")
						}
					}
					require.NotEmpty(t, strings.Fields(methods), "empty declaration")
					for _, method := range strings.Fields(methods) {
						declarations = append(declarations, declaredBoundaryMethod{method, disposition, scope})
					}
				}
				methodSet := types.NewMethodSet(pointer)
				require.Empty(t, boundaryPolicyProblems(methodSet, core, declarations))
				t.Logf("%d declared optional methods", len(declarations))
				// Production mutations preserve the two omissions as guard controls.
				for _, forgotten := range []string{"StreamEdgesByType", "ConstituentStorage"} {
					var mutated []declaredBoundaryMethod
					for _, d := range declarations {
						if d.method != forgotten {
							mutated = append(mutated, d)
						}
					}
					if len(mutated) != len(declarations) {
						require.NotEmpty(t, boundaryPolicyProblems(methodSet, core, mutated), "must detect omitted %s", forgotten)
					}
				}
				if len(declarations) > 0 {
					require.NotEmpty(t, boundaryPolicyProblems(methodSet, core, append(declarations, declarations[0])), "duplicate declarations must fail")
				}
			})
		}
	}
	require.GreaterOrEqual(t, boundaries, 11, "production boundary population must be loaded")
}

func TestReadBoundaryPolicyNewMethodControl(t *testing.T) {
	pkg := types.NewPackage("boundary-control", "control")
	boundary := types.NewNamed(types.NewTypeName(token.NoPos, pkg, "Boundary", nil), types.NewStruct(nil, nil), nil)
	boundary.AddMethod(types.NewFunc(token.NoPos, pkg, "NewCapability", types.NewSignatureType(types.NewVar(token.NoPos, pkg, "", types.NewPointer(boundary)), nil, nil, types.NewTuple(), types.NewTuple(), false)))
	require.NotEmpty(t, boundaryPolicyProblems(types.NewMethodSet(types.NewPointer(boundary)), types.NewInterfaceType(nil, nil).Complete(), nil), "a newly implemented capability needs a declaration")
}
