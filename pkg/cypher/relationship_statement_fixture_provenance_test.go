package cypher

import (
	"bytes"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"os/exec"
	"sort"
	"strings"
	"testing"
)

// Required kernel parameters make the Go compiler the caller census. This
// backstop protects construction of those values and the original reader
// boundary; it does not infer admission from contexts, rows or observed panics.
func relationshipFixtureSource(t *testing.T, extra string, race bool, extraFilename ...string) (*token.FileSet, []*ast.File, *types.Info, []types.Error) {
	t.Helper()
	args := []string{"list", "-deps", "-test", "-json"}
	if race {
		args = append(args, "-race")
	}
	command := exec.Command("go", append(args, ".")...)
	var stdout, stderr bytes.Buffer
	command.Stdout, command.Stderr = &stdout, &stderr
	if err := command.Run(); err != nil {
		t.Fatalf("fixture source listing: %v: %s", err, &stderr)
	}
	packages := map[string]*relationshipBoundaryListedPackage{}
	const path = "github.com/orneryd/nornicdb/pkg/cypher"
	var tests []string
	decoder := json.NewDecoder(&stdout)
	for decoder.More() {
		var listed struct {
			relationshipBoundaryListedPackage
			TestGoFiles []string
		}
		if err := decoder.Decode(&listed); err != nil {
			t.Fatal(err)
		}
		copy := listed.relationshipBoundaryListedPackage
		packages[copy.ImportPath] = &copy
		if copy.ImportPath == path {
			tests = append(tests, listed.TestGoFiles...)
		}
	}
	listed := packages[path]
	if listed == nil || len(tests) == 0 {
		t.Fatal("complete compiler-selected fixture population missing")
	}
	listed.GoFiles = append(append([]string{}, listed.GoFiles...), tests...)
	fset := token.NewFileSet()
	files, err := relationshipBoundaryParseFiles(fset, listed, "")
	if err != nil {
		t.Fatal(err)
	}
	if extra != "" {
		name := "zz_relationship_fixture_guard_control_test.go"
		if len(extraFilename) != 0 {
			name = extraFilename[0]
		}
		file, err := parser.ParseFile(fset, name, extra, 0)
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, file)
	}
	info := &types.Info{Types: map[ast.Expr]types.TypeAndValue{}, Defs: map[*ast.Ident]types.Object{}, Uses: map[*ast.Ident]types.Object{}, Selections: map[*ast.SelectorExpr]*types.Selection{}}
	imp := &relationshipBoundaryImporter{fset: fset, packages: packages, loaded: map[string]*types.Package{}}
	var problems []types.Error
	config := types.Config{FakeImportC: true, Importer: imp.from(listed), Error: func(err error) {
		if problem, ok := err.(types.Error); ok {
			problems = append(problems, problem)
		} else {
			t.Errorf("unexpected compiler diagnostic: %v", err)
		}
	}}
	// Check the actual package, including function bodies. Eval of a function
	// literal does not establish that its body was checked.
	_, _ = config.Check(path, fset, files, info)
	if len(imp.errors) != 0 {
		t.Fatalf("complete dependency type information required: %v", imp.errors)
	}
	return fset, files, info, problems
}

func relationshipFixtureTypedSource(t *testing.T, extra string, race bool, extraFilename ...string) (*token.FileSet, []*ast.File, *types.Info) {
	t.Helper()
	fset, files, info, problems := relationshipFixtureSource(t, extra, race, extraFilename...)
	if len(problems) != 0 {
		t.Fatalf("complete fixture type information required: %v", problems)
	}
	return fset, files, info
}

func relationshipAdmissionCompileErrors(t *testing.T, declarations string) []types.Error {
	t.Helper()
	_, _, _, problems := relationshipFixtureSource(t, "package cypher\nimport \"context\"\n"+declarations, false)
	for _, problem := range problems {
		if problem.Fset.Position(problem.Pos).Filename != "zz_relationship_fixture_guard_control_test.go" {
			t.Fatalf("baseline package does not compile: %v", problem)
		}
	}
	return problems
}

func relationshipFixtureAdmissionViolations(fset *token.FileSet, files []*ast.File, info *types.Info) ([]string, int) {
	required := map[string]bool{
		"StorageExecutor.relationshipExecutionFor":               false,
		"StorageExecutor.relationshipExecutionForQuery":          false,
		"StorageExecutor.relationshipAccessFor":                  false,
		"StorageExecutor.relationshipAccessForIdentity":          false,
		"StorageExecutor.relationshipTemplateAccess":             false,
		"StorageExecutor.selectPaths":                            false,
		"StorageExecutor.runCorrelatedSubquery":                  false,
		"StorageExecutor.executeFragment":                        false,
		"StorageExecutor.executeAdmittedWithoutTransaction":      false,
		"StorageExecutor.executeAdmittedImplicitAsync":           false,
		"StorageExecutor.executeAdmittedWithImplicitTransaction": false,
		"StorageExecutor.executeAdmittedInTransaction":           false,
		"StorageExecutor.executeAdmittedQueryAgainstStorage":     false,
	}
	var violations []string
	fail := func(node ast.Node, message string) {
		violations = append(violations, fmt.Sprintf("%s: %s", fset.Position(node.Pos()), message))
	}
	isAdmission := func(kind types.Type) bool {
		return relationshipBoundaryNamedType(kind) == "admittedRelationshipStatement"
	}
	// Existing fixtures sometimes predeclare the compiler's second result. A
	// zero declaration alone is not a source: require assignment before its use.
	firstAssignment := map[types.Object]token.Pos{}
	assignedNames := map[*ast.Ident]bool{}
	for _, file := range files {
		ast.Inspect(file, func(node ast.Node) bool {
			assignment, ok := node.(*ast.AssignStmt)
			if !ok {
				return true
			}
			for _, lhs := range assignment.Lhs {
				name, ok := lhs.(*ast.Ident)
				if !ok {
					continue
				}
				assignedNames[name] = true
				object := info.Uses[name]
				if object == nil {
					object = info.Defs[name]
				}
				if object != nil && isAdmission(object.Type()) && (firstAssignment[object] == token.NoPos || assignment.End() < firstAssignment[object]) {
					firstAssignment[object] = assignment.End()
				}
			}
			return true
		})
	}
	for _, file := range files {
		production := !strings.HasSuffix(fset.Position(file.Pos()).Filename, "_test.go")
		for _, declaration := range file.Decls {
			owner := ""
			if function, ok := declaration.(*ast.FuncDecl); ok {
				object, _ := info.Defs[function.Name].(*types.Func)
				if object != nil {
					owner = relationshipBoundaryFunctionName(object)
					if production && owner == "relationshipStaticTraversal" {
						required[owner] = false
					}
					if _, needed := required[owner]; needed {
						signature := object.Type().(*types.Signature)
						required[owner] = true
						if signature.Params().Len() == 0 || !isAdmission(signature.Params().At(0).Type()) {
							fail(function, owner+" must require admittedRelationshipStatement as its first input")
						}
					}
				}
				if function.Name.Name == "relationshipProgramFromContext" {
					fail(function, "a private kernel cannot recover admission from ambient context")
				}
			}
			constructor := production && (owner == "compileRelationshipStatement" || owner == "relationshipAccessExecution.statement")
			// Binding an exact-text prepared source is the cached public admission
			// owner. Its explicit source-override check is not kernel recovery.
			// Transparent dispatch still requires the typed input above.
			publicAdmission := production && (owner == "compileRelationshipStatement" || owner == "compileRelationshipProcedureEntry" || owner == "preparedRelationshipStatement.bind")
			ast.Inspect(declaration, func(node ast.Node) bool {
				switch value := node.(type) {
				case *ast.CompositeLit:
					if isAdmission(info.TypeOf(value)) && !constructor {
						fail(value, owner+" fabricates admission; use the actual source compiler or common fixture helpers")
					}
				case *ast.CallExpr:
					if signature, ok := info.TypeOf(value.Fun).(*types.Signature); ok {
						admitted := false
						for index := 0; index < signature.Params().Len(); index++ {
							admitted = admitted || isAdmission(signature.Params().At(index).Type())
						}
						for index := 0; index < signature.Results().Len(); index++ {
							admitted = admitted || isAdmission(signature.Results().At(index).Type())
						}
						if admitted {
							for index, argument := range value.Args {
								if index >= signature.Params().Len() {
									break
								}
								if name, ok := argument.(*ast.Ident); ok && info.Uses[name] == types.Universe.Lookup("nil") && signature.Params().At(index).Type().String() == "context.Context" {
									fail(value, owner+" supplies nil context to an admitted statement boundary")
								}
							}
						}
					}
					if kind := info.Types[value.Fun]; kind.IsType() && isAdmission(kind.Type) {
						fail(value, owner+" converts an admission value outside its owner")
					}
					if name, ok := value.Fun.(*ast.Ident); ok {
						if builtin, ok := info.Uses[name].(*types.Builtin); ok && builtin.Name() == "new" && len(value.Args) == 1 && isAdmission(info.TypeOf(value.Args[0])) {
							fail(value, owner+" allocates a zero admission value")
						}
					}
					if selector, ok := value.Fun.(*ast.SelectorExpr); ok && selector.Sel.Name == "Value" && len(value.Args) == 1 && production && !publicAdmission {
						key := relationshipBoundaryNamedType(info.TypeOf(value.Args[0]))
						if key == "relationshipStatementKey" || key == "relationshipAccessProgramKey" {
							fail(value, owner+" reads ambient admission outside the public admission owner")
						}
					}
				case *ast.TypeAssertExpr:
					if isAdmission(info.TypeOf(value)) {
						fail(value, owner+" recovers admission from an untyped value")
					}
				case *ast.AssignStmt:
					for _, lhs := range value.Lhs {
						if selector, ok := lhs.(*ast.SelectorExpr); ok && isAdmission(info.TypeOf(selector.X)) && !constructor {
							fail(selector, owner+" mutates an admitted statement")
						}
					}
				case *ast.ValueSpec:
					if len(value.Values) == 0 {
						for _, name := range value.Names {
							object := info.Defs[name]
							if object == nil || !isAdmission(object.Type()) {
								continue
							}
							assigned := firstAssignment[object]
							if assigned == token.NoPos {
								fail(name, owner+" declares admission without any source assignment")
								continue
							}
							for use, target := range info.Uses {
								if target == object && !assignedNames[use] && use.Pos() < assigned {
									fail(use, owner+" uses admission before source assignment")
								}
							}
						}
					}
				}
				return true
			})
		}
	}
	for kernel, found := range required {
		if !found {
			violations = append(violations, "required relationship kernel disappeared: "+kernel)
		}
	}
	sort.Strings(violations)
	return violations, len(required)
}

func TestRelationshipStatementFixtureAdmission(t *testing.T) {
	for _, race := range []bool{false, true} {
		t.Run(fmt.Sprintf("race=%t", race), func(t *testing.T) {
			fset, files, info := relationshipFixtureTypedSource(t, "", race)
			violations, kernels := relationshipFixtureAdmissionViolations(fset, files, info)
			t.Logf("ADMISSION_TYPED_BACKSTOP race=%t files=%d mandatoryKernels=%d violations=%d; compiler enumerates callers", race, len(files), kernels, len(violations))
			if len(violations) != 0 {
				t.Fatal(strings.Join(violations, "\n"))
			}
		})
	}
}

func TestRelationshipStatementFixtureAdmissionControls(t *testing.T) {
	const expression = `COLLECT { MATCH (p)-[:R]->(q) RETURN q }`
	extra := `package cypher
import "context"
func relationshipGuardMissingProjection(e *StorageExecutor) { e.resolveReturnItem(context.Background(),returnItem{expr:"` + expression + `"},"p",nil) }
func relationshipGuardMissingCancelled(e *StorageExecutor) { ctx,cancel:=context.WithCancel(context.Background());cancel();e.pipelineApplyShortestPathMatch(ctx,nil,&shortestPathMatch{},false) }
func relationshipGuardMissingCallback(e *StorageExecutor) { read:=e.resolveReturnItem;read(context.Background(),returnItem{expr:"` + expression + `"},"p",nil) }
func relationshipGuardMissingRow(e *StorageExecutor) { e.evaluateExpressionFromValues("` + expression + `",map[string]interface{}{}) }
func relationshipGuardMissingOtherContext(e *StorageExecutor) { ctx,_:=expressionTestContext(context.Background(),"` + expression + `");e.resolveReturnItem(ctx,returnItem{expr:"` + expression + `"},"p",nil) }
func relationshipGuardTypedHelper(statement admittedRelationshipStatement,e *StorageExecutor,row map[string]interface{}) { e.evaluateExpressionFromValues(statement,"` + expression + `",row) }
func relationshipGuardMissingRowViaHelper(e *StorageExecutor) { _,_ = expressionTestContext(context.Background(),"RETURN 1");relationshipGuardTypedHelper(e,map[string]interface{}{}) }
type relationshipGuardCarrier struct {ctx context.Context; statement admittedRelationshipStatement}
func relationshipGuardMissingCarrier(e *StorageExecutor,carrier relationshipGuardCarrier) { e.resolveReturnItem(carrier.ctx,returnItem{expr:"` + expression + `"},"p",nil) }
func relationshipGuardMissingNil(e *StorageExecutor) { e.relationshipAccessFor(nil,context.Background(),"()-[r:R]->()","") }
func relationshipGuardCompilerBody() { _ = func(){var invalid int = "body was not checked";_ = invalid} }
func relationshipGuardAdmitted(e *StorageExecutor) { ctx,statement:=expressionTestContext(context.Background(),"` + expression + `");ctx,cancel:=context.WithCancel(ctx);cancel();read:=e.resolveReturnItem;read(statement,ctx,returnItem{expr:"` + expression + `"},"p",nil) }
func relationshipGuardAdmittedRow(e *StorageExecutor) { row,statement:=expressionTestRow(context.Background(),"` + expression + `",nil);e.evaluateExpressionFromValues(statement,"` + expression + `",row) }
func relationshipGuardAdmittedCarrier(e *StorageExecutor) { ctx,statement:=expressionTestContext(context.Background(),"` + expression + `");carrier:=relationshipGuardCarrier{ctx,statement};e.resolveReturnItem(carrier.statement,carrier.ctx,returnItem{expr:"` + expression + `"},"p",nil) }
func relationshipGuardAdmittedWithoutRowContext(e *StorageExecutor) { _,statement:=expressionTestContext(context.Background(),"` + expression + `");e.evaluateExpressionFromValues(statement,"` + expression + `",map[string]interface{}{}) }
`
	fset, files, _, problems := relationshipFixtureSource(t, extra, false)
	missing := map[string]bool{}
	for _, problem := range problems {
		if fset.Position(problem.Pos).Filename != "zz_relationship_fixture_guard_control_test.go" {
			t.Fatalf("baseline package does not compile: %v", problem)
		}
		for _, declaration := range files[len(files)-1].Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok || problem.Pos < function.Pos() || problem.Pos > function.End() {
				continue
			}
			if strings.HasPrefix(function.Name.Name, "relationshipGuardAdmitted") || function.Name.Name == "relationshipGuardTypedHelper" {
				t.Errorf("valid admission rejected: %v", problem)
			}
			missing[function.Name.Name] = true
		}
	}
	for _, name := range []string{"Projection", "Cancelled", "Callback", "Row", "OtherContext", "RowViaHelper", "Carrier", "Nil"} {
		if !missing["relationshipGuardMissing"+name] {
			t.Errorf("compiler accepted missing %s admission", name)
		}
	}
	if !missing["relationshipGuardCompilerBody"] {
		t.Fatal("negative controls did not check function literal bodies")
	}
	t.Log("real package compiler rejects all eight missing-input controls and the body sentinel; all four admitted controls compile")
}

func TestRelationshipStatementFixtureConstructionControls(t *testing.T) {
	extra := `package cypher
import "context"
func relationshipGuardFake(e *StorageExecutor) { e.relationshipAccessFor(admittedRelationshipStatement{},context.Background(),"()-[r:R]->()","") }
func relationshipGuardZero(e *StorageExecutor) { var statement admittedRelationshipStatement;e.relationshipAccessFor(statement,context.Background(),"()-[r:R]->()","") }
func relationshipGuardNew(e *StorageExecutor) { e.relationshipAccessFor(*new(admittedRelationshipStatement),context.Background(),"()-[r:R]->()","") }
func relationshipGuardMutation(statement admittedRelationshipStatement) { statement.program=relationshipProgramFor("RETURN 1") }
func relationshipGuardNilContext(e *StorageExecutor,statement admittedRelationshipStatement) { e.relationshipExecutionFor(statement,nil,"()-[r:R]->()","") }
func relationshipGuardAmbientRead(ctx context.Context) { _ = ctx.Value(relationshipAccessProgramKey{}) }
func relationshipGuardGood(e *StorageExecutor) { ctx,statement:=traversalTestContext(context.Background(),"()-[r:R]->()","",nil);e.relationshipAccessFor(statement,ctx,"()-[r:R]->()","") }
`
	// The reader restriction applies to production kernels. Parse these
	// construction controls as production so an ambient-reader exemption
	// cannot accidentally make that negative control pass as a test fixture.
	fset, files, info := relationshipFixtureTypedSource(t, extra, false, "zz_relationship_fixture_guard_control.go")
	violations, _ := relationshipFixtureAdmissionViolations(fset, files, info)
	found := map[string]bool{}
	for _, violation := range violations {
		if !strings.Contains(violation, "zz_relationship_fixture_guard_control.go:") {
			t.Error(violation)
			continue
		}
		for _, name := range []string{"Fake", "Zero", "New", "Mutation", "NilContext", "AmbientRead"} {
			if strings.Contains(violation, "relationshipGuard"+name) {
				found[name] = true
			}
		}
		if strings.Contains(violation, "relationshipGuardGood") {
			t.Error(violation)
		}
	}
	if len(found) != 6 {
		t.Fatalf("construction backstop missed fabricated or zero source: %v", found)
	}
	t.Log("construction backstop rejects literal, zero, new, mutation, nil-context and ambient-read controls; common fixture admission accepted")
}
