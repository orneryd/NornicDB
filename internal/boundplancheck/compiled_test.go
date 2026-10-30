//go:build boundplan_census_operator

// Author: #1640 lane (Codex). Analysis-only client; frozen engine supplied by the operator overlay.
package boundplancheck

import (
	"archive/zip"
	"bytes"
	"encoding/base64"
	"encoding/json"
	"go/ast"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/internal/workcensus"
	"golang.org/x/tools/go/ssa"
	"golang.org/x/tools/go/types/typeutil"
)

// Author: #1640 lane (Codex). Membership is DIRECT use of a named mechanism,
// never reachability, a fact-typed signature, or general diagnostic calls.
// The shared engine owns package loading, syntax, declaration IDs and digests.
const boundModule = "github.com/orneryd/nornicdb/"

// Exact APIs whose implementations decide scope/header/import membership from
// text, row keys, or a second name set. Canonical callers are recorded too, so
// their read-based dispositions can distinguish syntax input from a bypass.
// Callers of returnProjectionPlanFor are members only without its retained
// clause argument; the retained branch consumes the admitted program.
var boundDirectAPIs = map[string][]string{
	"pkg/cypher": {
		"returnProjectionPlanFor", "parseReturnProjectionPlan", "projectionPlanScope",
		"pipelineWildcardColumns", "pipelineScopeColumns", "pipelineReturnSourceColumns",
		"callTailWithProjectionColumns", "buildColumnsFromReturnItems",
		"callSubqueryImportedVariables", "importingWithVariables", "importingWithSyntax", "importingWithNode",
		"callScopeClauseVariables", "readCallScopeClause", "callWithImports",
		"projectTypeScope", "projectTypeScopeIn", "projectLocalTypeScope", "forEachProjectedBinding",
		"callSubqueryReturnTypes", "localVariableShadowsAt", "withoutLocalVariables", "expressionLocalBindings",
		"localBindingOf", "addMatchPatternBindingKinds", "validateReturnSemanticScopeIn",
		"validateQuantifiedGroupPredicates", "projectionAliasScope", "quantifiedGroupVariables",
	},
	"pkg/fabric": {
		"callBodyReturns", "ensureRowProducingPrefix", "trailingWithAliases", "hasTopLevelReturnClause",
		"SetCallImportReader", "readCallImports", "bindLeadingImportColumns", "bindScopeImports",
		"parseSimpleBatchedLookupReturnItems", "parseSimpleBatchedCountReturnItems",
		"aliasesFromReturnItems", "aliasesFromCountReturnItems", "parseSimpleCollectReturnItem",
		"importColumnsFromFragment", "rewriteFragmentWithRuntimeImports", "importedNames",
		"combineColumns", "executeApplyInMemoryProjectionColumns", "executeCollectedMapJoinFlatProjectionColumns",
	},
	"pkg/cypher/resultmetadata": {"KnownScope", "KnownBindingScope", "KnownColumns", "KnownColumnAlternatives", "KnownFacts"},
}

// These exact private representations and the public resultmetadata product
// define the query's type boundary. The compiler supplies their fields and
// methods, so a new field, reader or writer cannot escape a frozen field list.
// Other tuple layouts and same-spelling fixture fields are not this product.
var boundDirectFactTypes = []string{
	"pkg/cypher.statementBinding",
	"pkg/cypher.statementBindingUse",
	"pkg/cypher.boundBranchInput",
	"pkg/cypher.boundBranchSeed",
	"pkg/cypher.boundBody",
	"pkg/cypher.boundScopeAdmission",
	"pkg/cypher.projectedScopeFacts",
	"pkg/cypher.boundTypeValues",
	"pkg/cypher.semanticDependencyScope",
	"pkg/cypher.semanticQueryScope",
	"pkg/cypher.returnProjectionPlan",
	"pkg/cypher.returnProjection",
	"pkg/cypher.pipelineCallMetadata",
	"pkg/cypher.pipelineRowWith",
	"pkg/cypher.boundBranch",
	"pkg/cypher.boundClause",
	"pkg/cypher.boundStatementProgram",
	"pkg/cypher.staticTypeScope",
	"pkg/cypher.semanticBodyScope",
	"pkg/fabric.Fragment",
	"pkg/fabric.FragmentInit",
	"pkg/fabric.FragmentLeaf",
	"pkg/fabric.FragmentExec",
	"pkg/fabric.FragmentApply",
	"pkg/fabric.FragmentUnion",
}

type boundCompiledReceipt struct {
	Author                  string               `json:"author"`
	Inventory               workcensus.Inventory `json:"inventory"`
	Methods                 map[string][]string  `json:"methods"`
	Sites                   map[string][]string  `json:"sites"`
	Counts                  map[string]int       `json:"counts"`
	CompiledSubjects        []string             `json:"compiledSubjects"`
	NoncompiledSubjects     []string             `json:"noncompiledSubjects"`
	ExcludedFixtureSubjects []string             `json:"excludedFixtureSubjects"`
	Limitations             []string             `json:"limitations"`
	program                 *workcensus.Program
}

func boundDirectAPI(fn *types.Func) string {
	if fn == nil || fn.Pkg() == nil {
		return ""
	}
	pkg := strings.TrimPrefix(fn.Pkg().Path(), boundModule)
	for _, name := range boundDirectAPIs[pkg] {
		if fn.Name() == name {
			return pkg + "." + name
		}
	}
	return ""
}

func boundDirectFactType(typ types.Type) string {
	if typ == nil {
		return ""
	}
	typ = types.Unalias(typ)
	if ptr, ok := typ.(*types.Pointer); ok {
		typ = types.Unalias(ptr.Elem())
	}
	named, ok := typ.(*types.Named)
	if !ok || named.Obj().Pkg() == nil {
		return ""
	}
	key := strings.TrimPrefix(named.Obj().Pkg().Path(), boundModule) + "." + named.Obj().Name()
	if named.Obj().Pkg().Path() == boundModule+"pkg/cypher/resultmetadata" {
		switch named.Underlying().(type) {
		case *types.Struct, *types.Interface:
			return key
		}
	}
	for _, factType := range boundDirectFactTypes {
		if key == factType {
			return key
		}
	}
	return ""
}

// A product may be retained by an arbitrarily named record. Read/write the
// fact-bearing FIELD of that record directly; do not select every field or
// method of a containing record, or callers merely reachable from its owner.
// This catches admission.program and future transport envelopes without making
// a same-spelling Columns []string field a fact.
func boundFactPayload(typ types.Type) bool {
	seen := map[types.Type]bool{}
	var contains func(types.Type) bool
	contains = func(typ types.Type) bool {
		if typ == nil {
			return false
		}
		typ = types.Unalias(typ)
		if seen[typ] {
			return false
		}
		seen[typ] = true
		if boundDirectFactType(typ) != "" {
			return true
		}
		// Stop at another named record: a field holding an executor, session or
		// server that eventually retains a fact is containment reachability. The
		// function reading the inner fact field is the direct member.
		if named, ok := typ.(*types.Named); ok {
			if _, record := named.Underlying().(*types.Struct); record {
				return false
			}
		}
		switch value := typ.Underlying().(type) {
		case *types.Pointer:
			return contains(value.Elem())
		case *types.Slice:
			return contains(value.Elem())
		case *types.Array:
			return contains(value.Elem())
		case *types.Map:
			return contains(value.Key()) || contains(value.Elem())
		case *types.Chan:
			return contains(value.Elem())
		case *types.Struct:
			for i := 0; i < value.NumFields(); i++ {
				if contains(value.Field(i).Type()) {
					return true
				}
			}
		}
		// Interface/function signatures alone do not prove direct fact use.
		return false
	}
	return contains(typ)
}

func boundDirectField(typ types.Type, name string) string {
	if key := boundDirectFactType(typ); key != "" {
		return key + "." + name
	}
	if typ == nil {
		return ""
	}
	typ = types.Unalias(typ)
	if pointer, ok := typ.(*types.Pointer); ok {
		typ = types.Unalias(pointer.Elem())
	}
	structure, ok := typ.Underlying().(*types.Struct)
	if !ok {
		return ""
	}
	for i := 0; i < structure.NumFields(); i++ {
		field := structure.Field(i)
		if field.Name() == name && boundFactPayload(field.Type()) {
			key := strings.TrimPrefix(types.TypeString(typ, func(pkg *types.Package) string { return pkg.Path() }), boundModule)
			return key + "." + name
		}
	}
	return ""
}

// Resolve a write through an indexed fact container to that exact field. This
// includes map membership edits, rather than just replacing the map itself.
func boundWrittenField(expr ast.Expr) *ast.SelectorExpr {
	switch expr := expr.(type) {
	case *ast.SelectorExpr:
		return expr
	case *ast.IndexExpr:
		return boundWrittenField(expr.X)
	case *ast.StarExpr:
		return boundWrittenField(expr.X)
	case *ast.ParenExpr:
		return boundWrittenField(expr.X)
	}
	return nil
}

func boundFixture(file string) bool {
	// Executable corpus/fixture drivers exercise the product; they do not own
	// admission. This boundary is explicit and reported separately from members.
	for _, part := range strings.Split(filepath.ToSlash(file), "/") {
		if part == "testdata" || part == "fixtures" || part == "fixture" || part == "corpus" {
			return true
		}
	}
	return false
}

func boundCompiledQuery(t *testing.T, config workcensus.Config) boundCompiledReceipt {
	t.Helper()
	config.Patterns = []string{"./..."}
	config.IncludeProgram = true
	config.IncludeSyntax = true
	all, err := workcensus.Extract(config)
	if err != nil {
		t.Fatal(err)
	}
	out := boundCompiledReceipt{Author: "#1640 lane (Codex)", Methods: map[string][]string{}, Sites: map[string][]string{}, Counts: map[string]int{"source-subjects": len(all.Members)}, Limitations: all.Limitations}
	out.Inventory.Sources = all.Sources
	out.program = all.Program
	bySyntax := map[ast.Node]workcensus.Member{}
	byObject := map[*types.Func]workcensus.Member{}
	byField := map[types.Object]string{}
	addFields := func(typ types.Type) {
		for {
			typ = types.Unalias(typ)
			pointer, ok := typ.(*types.Pointer)
			if !ok {
				break
			}
			typ = pointer.Elem()
		}
		if structure, ok := typ.Underlying().(*types.Struct); ok {
			for i := 0; i < structure.NumFields(); i++ {
				field := structure.Field(i)
				if key := boundDirectField(typ, field.Name()); key != "" {
					byField[field] = key
				}
			}
		}
	}
	for _, pkg := range all.Program.Packages {
		if pkg.Types == nil {
			continue
		}
		for _, name := range pkg.Types.Scope().Names() {
			if named, ok := pkg.Types.Scope().Lookup(name).(*types.TypeName); ok {
				addFields(named.Type())
			}
		}
		if pkg.TypesInfo != nil {
			for _, instance := range pkg.TypesInfo.Instances {
				addFields(instance.Type)
			}
		}
	}
	selected := map[string]workcensus.Member{}
	addMember := func(member workcensus.Member, method, site string) {
		selected[member.ID] = member
		out.Methods[member.ID] = append(out.Methods[member.ID], method)
		out.Sites[member.ID] = append(out.Sites[member.ID], site)
	}
	for _, member := range all.Members {
		if !member.Typed || member.State != "compiled" || member.Syntax == nil || boundFixture(member.File) {
			continue
		}
		switch node := member.Syntax.Node.(type) {
		case *ast.FuncDecl:
			bySyntax[node] = member
			if object, ok := member.Syntax.Info.Defs[node.Name].(*types.Func); ok {
				byObject[object.Origin()] = member
			}
		case *ast.FuncLit:
			bySyntax[node] = member
		}
	}
	for _, member := range all.Members {
		if !member.Typed || member.State != "compiled" {
			out.NoncompiledSubjects = append(out.NoncompiledSubjects, member.ID)
			continue
		}
		out.CompiledSubjects = append(out.CompiledSubjects, member.ID)
		if boundFixture(member.File) {
			out.ExcludedFixtureSubjects = append(out.ExcludedFixtureSubjects, member.ID)
			continue
		}
		if member.Syntax == nil {
			continue
		}
		switch member.Syntax.Node.(type) {
		case *ast.FuncDecl, *ast.FuncLit:
		default:
			continue
		}
		source := member.Syntax
		info := source.Info
		methods, sites := map[string]bool{}, map[string]bool{}
		add := func(method, fact string, at token.Pos) {
			methods[method] = true
			pos := source.FileSet.Position(at)
			sites[method+" "+fact+" @"+filepath.ToSlash(member.File)+":"+strconv.Itoa(pos.Line)] = true
		}
		// Include the mechanism's body itself, even when it currently has no
		// caller. An unused independent implementation is still a second owner.
		if decl, ok := source.Node.(*ast.FuncDecl); ok {
			if fn, ok := info.Defs[decl.Name].(*types.Func); ok {
				if api := boundDirectAPI(fn); api != "" {
					add("mechanism-definition", api, decl.Pos())
				}
			}
		}
		// This is a domain query on the shared engine's typed function handle.
		// It does not discover functions, generate IDs, or walk arbitrary files.
		ast.Inspect(source.Node, func(node ast.Node) bool {
			if node == nil {
				return false
			}
			if _, nested := node.(*ast.FuncLit); nested && node != source.Node {
				return false
			}
			switch node := node.(type) {
			case *ast.SelectorExpr:
				selected := info.Selections[node]
				if selected == nil {
					break
				}
				if selected.Kind() == types.FieldVal {
					field := byField[selected.Obj()]
					if field == "" {
						field = boundDirectField(selected.Recv(), selected.Obj().Name())
					}
					if field != "" {
						add("direct-fact-field-reference", field, node.Pos())
					}
				} else if fn, ok := selected.Obj().(*types.Func); ok {
					if signature, ok := fn.Type().(*types.Signature); ok && signature.Recv() != nil {
						key := boundDirectFactType(signature.Recv().Type())
						if key == "" {
							break
						}
						add("direct-fact-method-reference", key+"."+fn.Name(), node.Pos())
						if implementation, ok := byObject[fn.Origin()]; ok {
							addMember(implementation, "direct-fact-method-implementation", key+"."+fn.Name())
						}
					}
				}
			case *ast.CallExpr:
				// An actual factory invocation is a direct fact use even when
				// its result stays opaque. A signature alone is not membership.
				factResult := func(typ types.Type) {
					if key := boundDirectFactType(typ); key != "" {
						add("direct-fact-returning-call", key, node.Pos())
					}
				}
				if tuple, ok := info.TypeOf(node).(*types.Tuple); ok {
					for i := 0; i < tuple.Len(); i++ {
						factResult(tuple.At(i).Type())
					}
				} else {
					factResult(info.TypeOf(node))
				}
				if fn := typeutil.StaticCallee(info, node); fn != nil {
					if api := boundDirectAPI(fn); api != "" {
						if fn.Name() != "returnProjectionPlanFor" || len(node.Args) < 3 {
							add("direct-mechanism-call", api, node.Pos())
						}
					}
				}
			case *ast.Ident:
				// Taking an old inference callback as a function value is also a direct
				// use; this covers Fabric's installed CallImportReader, not its callers.
				if fn, ok := info.Uses[node].(*types.Func); ok {
					if api := boundDirectAPI(fn); api != "" && fn.Name() != "returnProjectionPlanFor" {
						add("direct-mechanism-reference", api, node.Pos())
					}
				}
			case *ast.AssignStmt:
				for _, lhs := range node.Lhs {
					if selector := boundWrittenField(lhs); selector != nil {
						if selected := info.Selections[selector]; selected != nil && selected.Kind() == types.FieldVal {
							field := byField[selected.Obj()]
							if field == "" {
								field = boundDirectField(selected.Recv(), selected.Obj().Name())
							}
							if field != "" {
								add("direct-fact-field-write", field, lhs.Pos())
							}
						}
					}
				}
			case *ast.CompositeLit:
				if key := boundDirectFactType(info.TypeOf(node)); key != "" {
					add("direct-fact-value-construction", key, node.Pos())
				}
				for i, element := range node.Elts {
					name := ""
					if kv, ok := element.(*ast.KeyValueExpr); ok {
						if ident, ok := kv.Key.(*ast.Ident); ok {
							name = ident.Name
						}
					} else {
						typ := info.TypeOf(node)
						if typ != nil {
							if structure, ok := typ.Underlying().(*types.Struct); ok && i < structure.NumFields() {
								name = structure.Field(i).Name()
							}
						}
					}
					if field := boundDirectField(info.TypeOf(node), name); field != "" {
						add("direct-fact-field-construction", field, element.Pos())
					}
				}
			}
			return true
		})
		if len(methods) == 0 {
			continue
		}
		selected[member.ID] = member
		for method := range methods {
			out.Methods[member.ID] = append(out.Methods[member.ID], method)
		}
		for site := range sites {
			out.Sites[member.ID] = append(out.Sites[member.ID], site)
		}
	}
	// These exact public ports admit or publish the canonical product. Query
	// implementations through go/types method sets, not an interface-name guess
	// or all functions reachable through the implementation.
	for _, pkg := range all.Program.Packages {
		if pkg.Types == nil || pkg.PkgPath != boundModule+"pkg/cypher/resultmetadata" {
			continue
		}
		for _, name := range []string{"FactsProvider", "ProjectionProvider", "Provider"} {
			object := pkg.Types.Scope().Lookup(name)
			if object == nil {
				t.Fatalf("missing bound contract %s", name)
			}
			contract, ok := object.Type().Underlying().(*types.Interface)
			if !ok {
				t.Fatalf("bound contract %s is not an interface", name)
			}
			for _, implementation := range all.Program.Packages {
				if implementation.Types == nil || !strings.HasPrefix(implementation.PkgPath, boundModule) {
					continue
				}
				for _, typeName := range implementation.Types.Scope().Names() {
					declared, ok := implementation.Types.Scope().Lookup(typeName).(*types.TypeName)
					if !ok {
						continue
					}
					named, ok := types.Unalias(declared.Type()).(*types.Named)
					if !ok {
						continue
					}
					candidate := types.NewPointer(named)
					if !types.Implements(candidate, contract) {
						continue
					}
					methods := types.NewMethodSet(candidate)
					for i := 0; i < contract.NumMethods(); i++ {
						required := contract.Method(i)
						method := methods.Lookup(required.Pkg(), required.Name())
						if method == nil {
							continue
						}
						fn, ok := method.Obj().(*types.Func)
						if !ok {
							continue
						}
						if member, ok := byObject[fn.Origin()]; ok {
							addMember(member, "direct-bound-port-implementation", name+"."+required.Name())
						}
					}
				}
			}
		}
	}
	// A callback's body is a direct implementation of the old import-inference
	// port, even if it uses an arbitrary name. Follow only the argument at that
	// exact installation call, never a transitive name/value/reachability graph.
	for _, fn := range all.Program.Functions {
		for _, block := range fn.Blocks {
			for _, instruction := range block.Instrs {
				call, ok := instruction.(ssa.CallInstruction)
				if !ok {
					continue
				}
				common := call.Common()
				callee := common.StaticCallee()
				if callee == nil || callee.Pkg == nil || callee.Pkg.Pkg.Path() != boundModule+"pkg/fabric" || callee.Name() != "SetCallImportReader" {
					continue
				}
				if len(common.Args) < 2 {
					continue
				}
				value := common.Args[len(common.Args)-1]
				if change, ok := value.(*ssa.ChangeType); ok {
					value = change.X
				}
				var callback *ssa.Function
				switch value := value.(type) {
				case *ssa.Function:
					callback = value
				case *ssa.MakeClosure:
					callback, _ = value.Fn.(*ssa.Function)
				}
				if callback == nil {
					continue
				}
				if original := callback.Origin(); original != nil {
					callback = original
				}
				if member, ok := bySyntax[callback.Syntax()]; ok {
					addMember(member, "direct-legacy-callback-implementation", "pkg/fabric.CallImportReader installed by "+fn.String())
				}
			}
		}
	}
	for _, member := range selected {
		out.Inventory.Members = append(out.Inventory.Members, member)
		out.Methods[member.ID] = boundUniqueStrings(out.Methods[member.ID])
		out.Sites[member.ID] = boundUniqueStrings(out.Sites[member.ID])
	}
	sort.Slice(out.Inventory.Members, func(i, j int) bool { return out.Inventory.Members[i].ID < out.Inventory.Members[j].ID })
	out.Counts["compiled-subjects"] = len(out.CompiledSubjects)
	out.Counts["noncompiled-subjects"] = len(out.NoncompiledSubjects)
	out.Counts["fixture-corpus-subjects-excluded"] = len(out.ExcludedFixtureSubjects)
	out.Counts["direct-mechanism-functions"] = len(out.Inventory.Members)
	return out
}

func boundUniqueStrings(values []string) []string {
	sort.Strings(values)
	var out []string
	for _, value := range values {
		if len(out) == 0 || out[len(out)-1] != value {
			out = append(out, value)
		}
	}
	return out
}

func TestBoundPlanCompiledPopulationInventory(t *testing.T) {
	receipt := boundCompiledQuery(t, boundPlanCensusConfig())
	raw, err := json.Marshal(receipt)
	if err != nil {
		t.Fatal(err)
	}
	var compressed bytes.Buffer
	archive := zip.NewWriter(&compressed)
	entry, err := archive.Create("compiled-population.json")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = entry.Write(raw); err != nil {
		t.Fatal(err)
	}
	if err = archive.Close(); err != nil {
		t.Fatal(err)
	}
	t.Logf("BOUND_PLAN_COMPILED_COUNTS %v", receipt.Counts)
	t.Logf("BOUND_PLAN_COMPILED_ZIP %s", base64.StdEncoding.EncodeToString(compressed.Bytes()))
}

func TestBoundPlanProductionPopulation(t *testing.T) {
	config := boundPlanCensusConfig()
	actual := boundCompiledQuery(t, config)
	raw, err := os.ReadFile(filepath.Join(config.Dir, "internal/boundplancheck/testdata/population.json"))
	if err != nil {
		t.Fatal(err)
	}
	var approved workcensus.Inventory
	if err = json.Unmarshal(raw, &approved); err != nil {
		t.Fatal(err)
	}
	if failures := workcensus.Validate(actual.Inventory, approved, boundPlanCensusPolicy()); len(failures) > 0 {
		t.Fatal(strings.Join(failures, "\n"))
	}
}

func TestBoundPlanPopulationDetectsIndependentDeciders(t *testing.T) {
	config := boundPlanCensusConfig()
	baseline := boundCompiledQuery(t, config)
	planted := filepath.Join(config.Dir, "pkg/server/r73_planted_decl.go")
	config.Overlay = map[string][]byte{planted: []byte(`package server
import "github.com/orneryd/nornicdb/pkg/cypher/resultmetadata"
func arbitraryProducer(row map[string]any) resultmetadata.Statement {
 var names []string
 for name:=range row { names=append(names,name) }
 return resultmetadata.Statement{Columns:resultmetadata.KnownColumns(names)}
}
func arbitraryNamesProducer(row map[string]any) resultmetadata.Names {
 var names []string
 for name:=range row { names=append(names,name) }
 return resultmetadata.Known(names)
}
func arbitraryConsumer(statement resultmetadata.Statement) resultmetadata.Columns {
 return statement.Columns
}
func arbitraryMethodConsumer(columns resultmetadata.Columns) {
 columns.Visit(func([]string){},func(){})
}
type wrappedStatement struct { resultmetadata.Statement }
func arbitraryPromotedConsumer(statement wrappedStatement) resultmetadata.Columns {
 return statement.Columns
}
func arbitraryPortConsumer(provider resultmetadata.Provider) resultmetadata.Names {
 return provider.CachedResultColumns(nil,"",nil)
}
type arbitraryEnvelope struct { accepted *resultmetadata.Statement; label string }
func arbitraryEnvelopeReader(value arbitraryEnvelope) *resultmetadata.Statement {
 return value.accepted
}
func arbitraryEnvelopeWriter(value *arbitraryEnvelope, statement *resultmetadata.Statement) {
 value.accepted=statement
}
// The same row-key mechanism without a bound-fact sink is context, not a member.
func unrelatedFixture(row map[string]any) []string {
 var names []string
 for name:=range row {names=append(names,name)}
 return names
}
// A non-fact field of a fact-bearing envelope is still a decoy.
func unrelatedEnvelopeLabel(value arbitraryEnvelope) string { return value.label }
// A record that only holds another fact-bearing record is not a fact field.
type arbitraryHolder struct { envelope *arbitraryEnvelope }
func unrelatedTransitiveHolder(value arbitraryHolder) *arbitraryEnvelope { return value.envelope }
type unrelatedFields struct { Columns []string }
func unrelatedConsumer(value unrelatedFields) []string { return value.Columns }
`)}
	actual := boundCompiledQuery(t, config)
	wanted := []struct{ owner, method string }{
		{".arbitraryProducer", "direct-fact-field-construction"},
		{".arbitraryNamesProducer", "direct-fact-returning-call"},
		{".arbitraryConsumer", "direct-fact-field-reference"},
		{".arbitraryMethodConsumer", "direct-fact-method-reference"},
		{".arbitraryPromotedConsumer", "direct-fact-field-reference"},
		{".arbitraryPortConsumer", "direct-fact-method-reference"},
		{".arbitraryEnvelopeReader", "direct-fact-field-reference"},
		{".arbitraryEnvelopeWriter", "direct-fact-field-write"},
	}
	plants := map[string]string{}
	for _, member := range actual.Inventory.Members {
		if strings.HasPrefix(member.ID, "pkg/server/r73_planted_decl.go:") {
			matched := false
			for _, want := range wanted {
				if strings.Contains(member.Owner, want.owner) {
					matched = true
					plants[want.owner] = member.ID
					if !strings.Contains(strings.Join(actual.Methods[member.ID], ","), want.method) {
						t.Fatalf("plant %s missed typed mechanism %s", want.owner, want.method)
					}
				}
			}
			if !matched {
				t.Fatalf("non-fact fixture entered direct population: %s", member.ID)
			}
		}
	}
	if len(plants) != len(wanted) || len(actual.Inventory.Members) != len(baseline.Inventory.Members)+len(wanted) {
		t.Fatal("outside-package producer/consumer population mismatch", plants)
	}
	policy := boundPlanCensusPolicy()
	policy.RequireSources = false
	failures := strings.Join(workcensus.Validate(actual.Inventory, baseline.Inventory, policy), "\n")
	for owner, id := range plants {
		if !strings.Contains(failures, "UNLISTED "+id) {
			t.Fatal("new direct producer/consumer escaped shared approval", owner, failures)
		}
	}
	config.Overlay = nil
	removed := boundCompiledQuery(t, config)
	if len(removed.Inventory.Members) != len(baseline.Inventory.Members) {
		t.Fatal("removed plant changed baseline population")
	}
	for i, member := range removed.Inventory.Members {
		if member.ID != baseline.Inventory.Members[i].ID || member.Digest != baseline.Inventory.Members[i].Digest {
			t.Fatal("plant removal failed to restore exact direct population")
		}
	}
	t.Logf("DIRECT_PLANT appears=%v disappears=true decoys=excluded baseline=%d planted=%d restored=%d", plants, len(baseline.Inventory.Members), len(actual.Inventory.Members), len(removed.Inventory.Members))
}
