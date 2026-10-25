package storage

import (
	"bytes"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
)

// storagePackageSources is this package's production source, parsed once, and
// the compiler export data of its dependencies.
type storagePackageSources struct {
	importPath string
	fset       *token.FileSet
	files      []*ast.File
	names      []string
	exports    map[string]string
	err        error
}

var loadStoragePackageSources = sync.OnceValue(func() *storagePackageSources {
	sources := &storagePackageSources{fset: token.NewFileSet(), exports: map[string]string{}}
	command := exec.Command("go", "list", "-deps", "-export", "-json", ".")
	var stderr bytes.Buffer
	command.Stderr = &stderr
	output, err := command.Output()
	if err != nil {
		sources.err = fmt.Errorf("go list: %v\n%s", err, stderr.String())
		return sources
	}
	type listed struct {
		ImportPath, Dir, Export string
		GoFiles, CgoFiles       []string
	}
	var self listed
	decoder := json.NewDecoder(bytes.NewReader(output))
	for {
		var pkg listed
		if err := decoder.Decode(&pkg); err == io.EOF {
			break
		} else if err != nil {
			sources.err = err
			return sources
		}
		sources.exports[pkg.ImportPath] = pkg.Export
		if pkg.ImportPath == "github.com/orneryd/nornicdb/pkg/storage" {
			self = pkg
		}
	}
	sources.importPath = self.ImportPath
	for _, name := range append(append([]string{}, self.GoFiles...), self.CgoFiles...) {
		file, err := parser.ParseFile(sources.fset, filepath.Join(self.Dir, name), nil, parser.SkipObjectResolution)
		if err != nil {
			sources.err = err
			return sources
		}
		sources.files = append(sources.files, file)
		sources.names = append(sources.names, name)
	}
	return sources
})

// checkedStorage is this package type-checked with an optional overlay.
type checkedStorage struct {
	pkg   *types.Package
	info  *types.Info
	files []*ast.File
	names []string
}

// typeCheckStorage type-checks this package's production source plus one
// in-memory overlay source, if given, and returns the type-checking error.
func typeCheckStorage(overlay string) (*checkedStorage, error) {
	return typeCheckStorageWith("overlay.go", overlay)
}

// typeCheckStorageWith type-checks this package's production source with
// source as the file name: it replaces the production file of that name, or
// is added to the package. An empty source changes nothing.
func typeCheckStorageWith(name, source string) (*checkedStorage, error) {
	sources := loadStoragePackageSources()
	if sources.err != nil {
		return nil, sources.err
	}
	checked := &checkedStorage{
		info: &types.Info{Defs: map[*ast.Ident]types.Object{}, Uses: map[*ast.Ident]types.Object{},
			Selections: map[*ast.SelectorExpr]*types.Selection{}, Types: map[ast.Expr]types.TypeAndValue{}},
	}
	for index, file := range sources.files {
		if source == "" || sources.names[index] != name {
			checked.files = append(checked.files, file)
			checked.names = append(checked.names, sources.names[index])
		}
	}
	if source != "" {
		file, err := parser.ParseFile(sources.fset, name, source, parser.SkipObjectResolution)
		if err != nil {
			return nil, err
		}
		checked.files = append(checked.files, file)
		checked.names = append(checked.names, name)
	}
	lookup := func(path string) (io.ReadCloser, error) {
		if sources.exports[path] == "" {
			return nil, fmt.Errorf("no export data for %s", path)
		}
		return os.Open(sources.exports[path])
	}
	config := types.Config{Importer: importer.ForCompiler(sources.fset, "gc", lookup), FakeImportC: true, Sizes: types.SizesFor("gc", "amd64")}
	pkg, err := config.Check(sources.importPath, sources.fset, checked.files, checked.info)
	if err != nil {
		return nil, err
	}
	checked.pkg = pkg
	return checked, nil
}

func checkStoragePackage(t *testing.T, overlay string) *checkedStorage {
	t.Helper()
	checked, err := typeCheckStorage(overlay)
	if err != nil {
		t.Fatalf("type-check: %v", err)
	}
	return checked
}

// exportedAPIRoot is one exported package-level object and its type.
type exportedAPIRoot struct {
	name string
	typ  types.Type
}

// exportedAPIRoots returns every exported package-level object of pkg:
// functions, variables, constants and type names, aliases included.
func exportedAPIRoots(pkg *types.Package) []exportedAPIRoot {
	var roots []exportedAPIRoot
	for _, name := range pkg.Scope().Names() {
		if object := pkg.Scope().Lookup(name); object.Exported() {
			roots = append(roots, exportedAPIRoot{name: name, typ: object.Type()})
		}
	}
	return roots
}

// maxExportedClosureTypes bounds the closure. Each reached type is visited
// once, but instantiating generic types can in principle create new types
// without end; hitting the bound fails instead of stopping the walk early.
const maxExportedClosureTypes = 200000

// exportedClosure computes every type an external package can reach from
// roots, as a fixed point: it holds every type named by a root, and every type
// reachable from one in the closure through any part of it a caller can use,
// to any depth. A caller can use pointer, slice, array and channel elements,
// map keys and values, exported and embedded (promoted) struct fields, every
// parameter and result of a signature and of each interface method, type
// arguments, the type parameters (with their constraints) of generic types,
// aliases and functions, union terms, and the exported
// methods of each reached type and of a pointer to it, whether or not the
// type itself is exported. The result maps each reached type to the path that
// reaches it. A types.Type kind it does not know is an error: an unhandled
// kind must fail the check, never pass it unexamined.
//
// A named type declared in another package, without type arguments, is a
// leaf. That package is a dependency of pkg and cannot name pkg's types, so
// nothing reachable through it can be one of them; types of pkg can enter a
// dependency's type only as type arguments, which are followed.
func exportedClosure(pkg *types.Package, roots []exportedAPIRoot) (map[types.Type]string, error) {
	reached := map[types.Type]string{}
	type pending struct {
		typ  types.Type
		path string
	}
	var queue []pending
	add := func(typ types.Type, path string) {
		if typ == nil {
			return
		}
		if _, seen := reached[typ]; !seen {
			reached[typ] = path
			queue = append(queue, pending{typ, path})
		}
	}
	addTuple := func(tuple *types.Tuple, path string) {
		for index := 0; index < tuple.Len(); index++ {
			add(tuple.At(index).Type(), fmt.Sprintf("%s.%d", path, index))
		}
	}
	addTypeParams := func(params *types.TypeParamList, path string) {
		for index := 0; index < params.Len(); index++ {
			add(params.At(index), fmt.Sprintf("%s[T%d]", path, index))
		}
	}
	addMethods := func(typ types.Type, path string) {
		for _, receiver := range []types.Type{typ, types.NewPointer(typ)} {
			methods := types.NewMethodSet(receiver)
			for index := 0; index < methods.Len(); index++ {
				if method := methods.At(index); method.Obj().Exported() {
					add(method.Type(), path+"."+method.Obj().Name()+"()")
				}
			}
		}
	}
	for _, root := range roots {
		add(root.typ, root.name)
	}
	for len(queue) > 0 {
		if len(reached) > maxExportedClosureTypes {
			return reached, fmt.Errorf("exported closure exceeded %d types", maxExportedClosureTypes)
		}
		next := queue[0]
		queue = queue[1:]
		switch typ := next.typ.(type) {
		case *types.Basic:
		case *types.Alias:
			add(types.Unalias(typ), next.path)
			addTypeParams(typ.TypeParams(), next.path)
			for index := 0; index < typ.TypeArgs().Len(); index++ {
				add(typ.TypeArgs().At(index), fmt.Sprintf("%s[%d]", next.path, index))
			}
		case *types.Named:
			args := typ.TypeArgs()
			if typ.Obj().Pkg() != pkg && args.Len() == 0 {
				continue
			}
			addTypeParams(typ.TypeParams(), next.path)
			for index := 0; index < args.Len(); index++ {
				add(args.At(index), fmt.Sprintf("%s[%d]", next.path, index))
			}
			add(typ.Underlying(), next.path)
			addMethods(typ, next.path)
		case *types.Pointer:
			add(typ.Elem(), next.path+".*")
		case *types.Slice:
			add(typ.Elem(), next.path+".[]")
		case *types.Array:
			add(typ.Elem(), next.path+".[n]")
		case *types.Chan:
			add(typ.Elem(), next.path+".<-")
		case *types.Map:
			add(typ.Key(), next.path+".key")
			add(typ.Elem(), next.path+".value")
		case *types.Struct:
			for index := 0; index < typ.NumFields(); index++ {
				if field := typ.Field(index); field.Exported() || field.Embedded() {
					add(field.Type(), next.path+"."+field.Name())
				}
			}
			addMethods(typ, next.path)
		case *types.Signature:
			addTypeParams(typ.TypeParams(), next.path)
			addTuple(typ.Params(), next.path+".param")
			addTuple(typ.Results(), next.path+".result")
		case *types.Interface:
			for index := 0; index < typ.NumMethods(); index++ {
				if method := typ.Method(index); method.Exported() {
					add(method.Type(), next.path+"."+method.Name()+"()")
				}
			}
			for index := 0; index < typ.NumEmbeddeds(); index++ {
				add(typ.EmbeddedType(index), next.path+".embedded")
			}
		case *types.TypeParam:
			add(typ.Constraint(), next.path+".constraint")
		case *types.Union:
			for index := 0; index < typ.Len(); index++ {
				add(typ.Term(index).Type(), fmt.Sprintf("%s.term%d", next.path, index))
			}
		case *types.Tuple:
			addTuple(typ, next.path)
		default:
			return reached, fmt.Errorf("exported closure: unknown type kind %T at %s", next.typ, next.path)
		}
	}
	return reached, nil
}

// planMemory is the memory a revision's closures keep: the entries and the
// plans compiled from them, read from the type graph through every field
// (private ones included), element, map key or value and pointer target.
// Interface and function values are leaves.
type planMemory struct {
	named   map[*types.TypeName]string // each type's path from its root
	structs []*types.Struct
}

func planMemoryOf(roots ...types.Type) (*planMemory, error) {
	memory := &planMemory{named: map[*types.TypeName]string{}}
	seen := map[types.Type]bool{}
	var walk func(types.Type, string) error
	walk = func(typ types.Type, path string) error {
		if seen[typ] {
			return nil
		}
		seen[typ] = true
		switch value := typ.(type) {
		case *types.Basic, *types.Interface, *types.Signature:
			return nil
		case *types.Alias:
			return walk(types.Unalias(value), path)
		case *types.Named:
			if value.Obj().Pkg() == nil {
				return nil // error
			}
			memory.named[value.Obj()] = path
			return walk(value.Underlying(), path)
		case *types.Struct:
			memory.structs = append(memory.structs, value)
			for index := 0; index < value.NumFields(); index++ {
				field := value.Field(index)
				if err := walk(field.Type(), path+"."+field.Name()); err != nil {
					return err
				}
			}
			return nil
		case *types.Pointer:
			return walk(value.Elem(), path+".*")
		case *types.Slice:
			return walk(value.Elem(), path+".[]")
		case *types.Array:
			return walk(value.Elem(), path+".[n]")
		case *types.Chan:
			return walk(value.Elem(), path+".<-")
		case *types.Map:
			if err := walk(value.Key(), path+".key"); err != nil {
				return err
			}
			return walk(value.Elem(), path+".value")
		default:
			return fmt.Errorf("plan memory: unknown type kind %T at %s", typ, path)
		}
	}
	for _, root := range roots {
		if err := walk(root, root.String()); err != nil {
			return memory, err
		}
	}
	return memory, nil
}

// isMemoryType reports whether typ is one of the memory types, or any type a
// Go conversion can make from one: a struct whose underlying type is
// identical to a memory struct's, struct tags ignored, as the language
// compares them for conversions.
func (memory *planMemory) isMemoryType(typ types.Type) bool {
	typ = types.Unalias(typ)
	if named, ok := typ.(*types.Named); ok {
		if _, ok := memory.named[named.Obj()]; ok {
			return true
		}
	}
	if value, ok := typ.Underlying().(*types.Struct); ok {
		for _, owned := range memory.structs {
			if types.IdenticalIgnoreTags(value, owned) {
				return true
			}
		}
	}
	return false
}

// holds reports whether a value of typ itself, not memory it only refers to,
// contains plan memory: it is a memory type or a view of one, or holds one in
// a field, private ones included, or an array element, at any depth. The
// closure follows pointers, slices, maps and channels itself. A types.Type
// kind it does not know is an error.
func (memory *planMemory) holds(typ types.Type) (bool, error) {
	if memory.isMemoryType(typ) {
		return true, nil
	}
	switch value := typ.(type) {
	case *types.Alias:
		return memory.holds(types.Unalias(value))
	case *types.Named:
		return memory.holds(value.Underlying())
	case *types.Struct:
		for index := 0; index < value.NumFields(); index++ {
			if holds, err := memory.holds(value.Field(index).Type()); holds || err != nil {
				return holds, err
			}
		}
		return false, nil
	case *types.Array:
		return memory.holds(value.Elem())
	case *types.Basic, *types.Pointer, *types.Slice, *types.Map, *types.Chan, *types.Signature, *types.Interface,
		*types.TypeParam, *types.Union, *types.Tuple:
		return false, nil
	default:
		return false, fmt.Errorf("plan memory: unknown type kind %T", typ)
	}
}

func storagePlanMemory(t *testing.T, checked *checkedStorage) *planMemory {
	t.Helper()
	var roots []types.Type
	for _, name := range []string{"constraintContractRevisionEntry", "constraintContractEntryPlan"} {
		object := checked.pkg.Scope().Lookup(name)
		if object == nil {
			t.Fatalf("%s not found", name)
		}
		roots = append(roots, object.Type())
	}
	memory, err := planMemoryOf(roots...)
	if err != nil {
		t.Fatal(err)
	}
	return memory
}

// planMemoryLeaks returns the path to every type reachable through the
// exported API that holds plan memory.
func planMemoryLeaks(checked *checkedStorage, memory *planMemory) ([]string, error) {
	reached, err := exportedClosure(checked.pkg, exportedAPIRoots(checked.pkg))
	if err != nil {
		return nil, err
	}
	var leaks []string
	for typ, path := range reached {
		holds, err := memory.holds(typ)
		if err != nil {
			return nil, fmt.Errorf("%w at %s", err, path)
		}
		if holds {
			leaks = append(leaks, path)
		}
	}
	// A view and its underlying struct share a path; report it once.
	sort.Strings(leaks)
	return slices.Compact(leaks), nil
}

// An installed revision is a value that holds no pointer, slice, map, channel
// or interface: only scalars and closures. The schema stores revisions by
// value and a handle holds its revision by value, so every holder has its own
// copy and nothing it does reaches what the schema enforces.
func TestConstraintContractRevisionHoldsNoReferences(t *testing.T) {
	checked := checkStoragePackage(t, "")
	scope := checked.pkg.Scope()
	revision := scope.Lookup("constraintContractRevision").Type()
	fields := revision.Underlying().(*types.Struct)
	for index := 0; index < fields.NumFields(); index++ {
		field := fields.Field(index)
		switch field.Type().Underlying().(type) {
		case *types.Basic, *types.Signature:
		default:
			t.Errorf("constraintContractRevision.%s is a %s: a revision must hold only scalars and closures", field.Name(), field.Type())
		}
	}
	handle := scope.Lookup("CompiledConstraintContract").Type().Underlying().(*types.Struct)
	if handle.NumFields() != 1 || handle.Field(0).Exported() || !types.Identical(handle.Field(0).Type(), revision) {
		t.Errorf("CompiledConstraintContract must hold one private revision by value, got %s", handle)
	}
	schema := scope.Lookup("SchemaManager").Type().Underlying().(*types.Struct)
	for index := 0; index < schema.NumFields(); index++ {
		if field := schema.Field(index); field.Name() == "constraintContracts" {
			stored, ok := field.Type().(*types.Map)
			if !ok || !types.Identical(stored.Elem(), revision) {
				t.Errorf("SchemaManager.constraintContracts must store revisions by value, got %s", field.Type())
			}
			return
		}
	}
	t.Error("SchemaManager.constraintContracts not found")
}

// Plan memory is private: every storage type in it is unexported, every type
// from another package in it has only private fields, and it keeps no error
// value, which a caller could reach through a returned error's causes: a
// compile error is kept as immutable data (retainedError) and rebuilt on each
// report.
func TestConstraintContractPlanMemoryIsPrivate(t *testing.T) {
	checked := checkStoragePackage(t, "")
	memory := storagePlanMemory(t, checked)
	errorType := types.Universe.Lookup("error").Type()
	for _, structure := range memory.structs {
		for index := 0; index < structure.NumFields(); index++ {
			if field := structure.Field(index); types.Identical(field.Type(), errorType) {
				t.Errorf("plan memory keeps the error field %s; keep it as retainedError data", field.Name())
			}
		}
	}
	for object, path := range memory.named {
		if object.Pkg() == checked.pkg && object.Exported() {
			t.Errorf("plan memory holds the exported type %s at %s", object.Name(), path)
		}
		if structure, ok := object.Type().Underlying().(*types.Struct); ok && object.Pkg() != checked.pkg {
			for index := 0; index < structure.NumFields(); index++ {
				if structure.Field(index).Exported() {
					t.Errorf("plan memory holds %s.%s, whose field %s storage can select", object.Pkg().Name(), object.Name(), structure.Field(index).Name())
				}
			}
		}
	}
}

// No exported storage API lets a caller reach plan memory under any type, and
// the opaque handle has one private field.
func TestConstraintContractRevisionStaysInternal(t *testing.T) {
	checked := checkStoragePackage(t, "")
	leaks, err := planMemoryLeaks(checked, storagePlanMemory(t, checked))
	if err != nil {
		t.Fatal(err)
	}
	if len(leaks) > 0 {
		t.Fatalf("exported APIs reach plan memory:\n%s", strings.Join(leaks, "\n"))
	}
	handle := reflect.TypeOf(CompiledConstraintContract{})
	if handle.NumField() != 1 || handle.Field(0).IsExported() {
		t.Fatalf("CompiledConstraintContract must be an opaque handle with one private field, got %d fields", handle.NumField())
	}
}

// Each planted API is type-checked on its own. A pointer view of an installed
// revision cannot be written at all, whatever its tags: the handle and the
// schema hold revisions by value. A route to plan memory is found by the
// closure, struct tags ignored. Views of a revision value compile but reach
// only a copy, which TestConstraintContractRevisionCopiesCannotChangeEnforcement
// exercises at run time.
func TestConstraintContractRevisionGuardMutations(t *testing.T) {
	const closure, compile, none = "closure", "compile", "none"
	type planted struct{ name, want, source string }
	tagged := func(layout string) string {
		return strings.Replace(layout, "string\n", "string `review:\"tagged-view\"`\n", 1)
	}
	oldRevision := "struct {\n\tname              string\n\ttargetEntityType  string\n\ttargetLabelOrType string\n\tdefinition        string\n\tentries           []constraintContractRevisionEntry\n}"
	revisionLayout := "struct {\n\tname              string\n\ttargetEntityType  string\n\ttargetLabelOrType string\n\tentries           int\n\tinvalid           func() error\n\tprimitive         func(index int) bool\n\tcheck             func(view constraintGraphView, kind string, node *Node, edge *Edge) error\n\tdescribe          func() ConstraintContract\n}"
	planLayout := "struct {\n\tnode         contractNodePredicate\n\trelationship contractRelationshipPredicate\n\terr          error\n}"
	cases := []planted{
		// The round-6 review's tagged pointer views, named and anonymous, and the
		// earlier untagged ones: there is no pointer to convert.
		{"review-r6-tagged-named-view", compile, "type reviewR6TaggedRevisionView " + tagged(oldRevision) + "\n\nfunc ReviewR6TaggedView(h CompiledConstraintContract) *reviewR6TaggedRevisionView { return (*reviewR6TaggedRevisionView)(h.revision) }"},
		{"review-r6-tagged-anonymous-view", compile, "func ReviewR6AnonymousView(h CompiledConstraintContract) *" + tagged(oldRevision) + " { return (*" + tagged(oldRevision) + ")(h.revision) }"},
		{"current-layout-tagged-view", compile, "type plantedView " + tagged(revisionLayout) + "\n\nfunc PlantedView(h CompiledConstraintContract) *plantedView { return (*plantedView)(h.revision) }"},
		{"review-r5-view", compile, "type reviewR5RevisionView constraintContractRevision\n\nfunc ReviewR5View(h CompiledConstraintContract) *reviewR5RevisionView { return (*reviewR5RevisionView)(h.revision) }"},
		{"review-d5b0-description", compile, `func ReviewD5B0Description(h CompiledConstraintContract) *ConstraintContract { return &h.revision.contract }`},
		{"review-d5b0-entries", compile, `func ReviewD5B0Entries(h CompiledConstraintContract) *[]ConstraintContractEntry { return &h.revision.contract.Entries }`},
		{"entries-pointer", compile, `func PlantedEntries(h CompiledConstraintContract) *[]constraintContractRevisionEntry { return &h.revision.entries }`},
		{"revision-pointer", compile, `func PlantedRevision(h CompiledConstraintContract) *constraintContractRevision { return h.revision }`},
		// Routes to plan memory, through the type graph, whatever the tags.
		{"plan-pointer", closure, `func PlantedPlan(plan constraintContractEntryPlan) *constraintContractEntryPlan { return &plan }`},
		{"entry-value", closure, `func PlantedEntry() constraintContractRevisionEntry { return constraintContractRevisionEntry{} }`},
		{"predicate-callback", closure, `func PlantedVisit(visit func(*contractNodePredicate)) {}`},
		{"pattern-result", closure, `func PlantedPattern() *contractPattern { return nil }`},
		{"tagged-plan-view", closure, "type plantedPlanView " + strings.Replace(planLayout, "contractNodePredicate\n", "contractNodePredicate `review:\"tagged\"`\n", 1) + "\n\nfunc PlantedPlanView() *plantedPlanView { return nil }"},
		{"tagged-anonymous-plan-view", closure, "func PlantedAnonymousPlanView() *" + strings.Replace(planLayout, "error\n", "error `review:\"tagged\"`\n", 1) + " { return nil }"},
		{"plan-alias", closure, "type PlantedPlanAlias = constraintContractEntryPlan"},
		{"plan-var", closure, `var PlantedPlans []constraintContractEntryPlan`},
		{"plan-generic", closure, "import \"sync/atomic\"\n\nfunc PlantedAtomicPlan() *atomic.Pointer[constraintContractEntryPlan] { return nil }"},
		{"plan-constraint", closure, `func PlantedPlanConstraint[T interface{ ~*constraintContractEntryPlan }](value T) {}`},
		{"plan-field", closure, `type PlantedHolder struct{ Plan *constraintContractEntryPlan }`},
		{"plan-embedded", closure, "type plantedInner struct{ Plan constraintContractEntryPlan }\ntype PlantedOuter struct{ plantedInner }"},
		{"plan-interface-method", closure, `type PlantedReader interface{ Plan() *constraintContractEntryPlan }`},
		// A revision value, a pointer to a holder's own copy and views of that
		// copy compile and reach no installed memory.
		{"revision-value", none, `func PlantedRevisionValue(h CompiledConstraintContract) constraintContractRevision { return h.revision }`},
		{"own-copy-pointer", none, `func PlantedCopyPointer(h CompiledConstraintContract) *constraintContractRevision { return &h.revision }`},
		{"own-copy-tagged-view", none, "type plantedCopyView " + tagged(revisionLayout) + "\n\nfunc PlantedCopyView(h CompiledConstraintContract) *plantedCopyView { return (*plantedCopyView)(&h.revision) }"},
		{"own-copy-name", none, `func PlantedName(h CompiledConstraintContract) *string { return &h.revision.name }`},
		{"description-copy", none, `func PlantedCopy(h CompiledConstraintContract) ConstraintContract { return h.Contract() }`},
		{"private-plan-function", none, `func plantedPlanOf(plan constraintContractEntryPlan) *constraintContractEntryPlan { return &plan }`},
	}
	for _, test := range cases {
		t.Run(test.want+"/"+test.name, func(t *testing.T) {
			checked, err := typeCheckStorage("package storage\n\n" + test.source + "\n")
			if test.want == compile {
				if err == nil {
					t.Fatalf("the planted API compiled")
				}
				t.Logf("does not compile: %v", err)
				return
			}
			if err != nil {
				t.Fatalf("type-check: %v", err)
			}
			leaks, err := planMemoryLeaks(checked, storagePlanMemory(t, checked))
			if err != nil {
				t.Fatal(err)
			}
			switch {
			case test.want == closure && len(leaks) == 0:
				t.Fatalf("the closure missed the planted route to plan memory")
			case test.want == none && len(leaks) > 0:
				t.Fatalf("a route to a copy was reported: %v", leaks)
			}
			t.Logf("closure: %v", leaks)
		})
	}
}

// taggedRevisionView is a tagged view of the current revision layout: storage
// code can write it, but only over a holder's own copy.
type taggedRevisionView struct {
	name              string `review:"tagged-view"`
	targetEntityType  string
	targetLabelOrType string
	entries           int
	invalid           func() error
	primitive         func(index int) bool
	check             func(view constraintGraphView, kind string, node *Node, edge *Edge) error
	describe          func() ConstraintContract
}

func zeroValue[T any](pointer *T) {
	var zero T
	*pointer = zero
}

// Every revision value storage code can get, from a handle, the handles the
// schema returns, the revisions it looks up for a target and a description,
// can be cleared, retargeted or replaced through any pointer or tagged view
// of it, and the installed contract still rejects the same writes.
func TestConstraintContractRevisionCopiesCannotChangeEnforcement(t *testing.T) {
	engine, err := NewBadgerEngineInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	schema := engine.GetSchemaForNamespace("test")
	compiled, err := CompileConstraintContract(ConstraintContract{Name: "copies", TargetEntityType: string(ConstraintEntityNode), TargetLabelOrType: "Copies",
		Entries: []ConstraintContractEntry{{Kind: ConstraintContractKindBooleanNode, Expression: "n.age >= 10"}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := schema.AddCompiledConstraintContractBundle(compiled, nil, nil, false); err != nil {
		t.Fatal(err)
	}
	commit := func(id string, age int64) error {
		tx, err := engine.BeginTransaction()
		if err != nil {
			return err
		}
		defer tx.Rollback()
		if err := tx.SetNamespace("test"); err != nil {
			return err
		}
		if _, err := tx.CreateNode(&Node{ID: NodeID("test:" + id), Labels: []string{"Copies"}, Properties: map[string]any{"age": age}}); err != nil {
			return err
		}
		return tx.Commit()
	}
	if commit("before", 5) == nil {
		t.Fatal("the installed contract does not reject age 5")
	}
	handles := append([]CompiledConstraintContract{compiled}, schema.CompiledConstraintContracts()...)
	for index := range handles {
		zeroValue((*taggedRevisionView)(&handles[index].revision))
		handles[index].revision.targetLabelOrType = "Elsewhere"
		zeroValue(&handles[index].revision)
		zeroValue(&handles[index])
	}
	revisions := schema.constraintContractRevisionsForTarget(ConstraintEntityNode, "Copies")
	for index := range revisions {
		zeroValue((*taggedRevisionView)(&revisions[index]))
	}
	description := schema.GetAllConstraintContracts()[0]
	zeroValue(&description)
	if commit("after", 5) == nil {
		t.Fatal("clearing a copy changed what the installed contract enforces")
	}
	if err := commit("adult", 15); err != nil {
		t.Fatal(err)
	}
	if stored := schema.GetAllConstraintContracts()[0]; stored.TargetLabelOrType != "Copies" || stored.Entries[0].Expression != "n.age >= 10" {
		t.Fatalf("clearing a copy changed the installed description: %+v", stored)
	}
}

// Descriptions are detached in both directions: editing the contract a schema
// installed, or any description it hands out, changes neither later
// descriptions nor what commits enforce.
func TestConstraintContractDescriptionsAreDetached(t *testing.T) {
	engine, err := NewBadgerEngineInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	schema := engine.GetSchemaForNamespace("test")
	contract := ConstraintContract{Name: "detached", TargetEntityType: string(ConstraintEntityNode), TargetLabelOrType: "Detached",
		Entries: []ConstraintContractEntry{{Kind: ConstraintContractKindBooleanNode, Expression: "n.age >= 10", Properties: []string{"age"}}}}
	if err := schema.AddConstraintContractBundle(contract, nil, nil, false); err != nil {
		t.Fatal(err)
	}
	commit := func(age int64) error {
		tx, err := engine.BeginTransaction()
		if err != nil {
			return err
		}
		defer tx.Rollback()
		if err := tx.SetNamespace("test"); err != nil {
			return err
		}
		if _, err := tx.CreateNode(&Node{ID: NodeID(fmt.Sprintf("test:%d", age)), Labels: []string{"Detached"}, Properties: map[string]any{"age": age}}); err != nil {
			return err
		}
		return tx.Commit()
	}
	edit := func(description *ConstraintContract) {
		description.Name, description.TargetLabelOrType = "edited", "Other"
		description.Entries[0].Expression, description.Entries[0].Properties[0] = "n.age >= 0", "edited"
		description.Entries = description.Entries[:0]
	}
	edit(&contract)
	compiled := schema.CompiledConstraintContracts()[0]
	for _, description := range []ConstraintContract{
		schema.GetAllConstraintContracts()[0], schema.GetConstraintContractsForTarget(ConstraintEntityNode, "Detached")[0],
		schema.ExportDefinition().ConstraintContracts[0], compiled.Contract(),
	} {
		edit(&description)
	}
	stored := schema.GetAllConstraintContracts()[0]
	if stored.Name != "detached" || stored.TargetLabelOrType != "Detached" || stored.Entries[0].Expression != "n.age >= 10" || stored.Entries[0].Properties[0] != "age" {
		t.Fatalf("an edited description changed the installed one: %+v", stored)
	}
	if commit(5) == nil {
		t.Fatal("the installed contract no longer rejects age 5")
	}
	if err := commit(15); err != nil {
		t.Fatal(err)
	}
}

// unknownTypeKind is a types.Type kind the guard does not know.
type unknownTypeKind struct{}

func (unknownTypeKind) Underlying() types.Type { return unknownTypeKind{} }
func (unknownTypeKind) String() string         { return "unknownTypeKind" }

// A type kind the guard does not know fails the check, naming that kind and
// where it was found, instead of passing unexamined: in the closure as a root
// or nested in a signature, in the plan memory walk and in its check of what
// a value holds.
func TestConstraintContractRevisionClosureRejectsUnknownKinds(t *testing.T) {
	result := types.NewVar(token.NoPos, nil, "result", unknownTypeKind{})
	nested := types.NewSignatureType(nil, nil, nil, nil, types.NewTuple(result), false)
	for name, test := range map[string]struct {
		typ  types.Type
		want string
	}{
		"root":   {unknownTypeKind{}, "unknown type kind storage.unknownTypeKind at Root"},
		"nested": {types.NewSlice(nested), "unknown type kind storage.unknownTypeKind at Root.[].result.0"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := exportedClosure(types.NewPackage("example.com/p", "p"), []exportedAPIRoot{{name: "Root", typ: test.typ}})
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("closure error %v, want %q", err, test.want)
			}
		})
	}
	field := types.NewField(token.NoPos, nil, "value", unknownTypeKind{}, false)
	inline := types.NewStruct([]*types.Var{field}, nil)
	if _, err := planMemoryOf(types.NewPointer(inline)); err == nil || !strings.Contains(err.Error(), "unknown type kind storage.unknownTypeKind") {
		t.Fatalf("plan memory walk error %v, want the unknown kind named", err)
	}
	empty := &planMemory{named: map[*types.TypeName]string{}}
	if _, err := empty.holds(inline); err == nil || !strings.Contains(err.Error(), "unknown type kind storage.unknownTypeKind") {
		t.Fatalf("plan memory check error %v, want the unknown kind named", err)
	}
}
