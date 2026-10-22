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
	}
	return sources
})

// checkStoragePackage type-checks this package's production source plus one
// in-memory overlay source, if given.
func checkStoragePackage(t *testing.T, overlay string) *types.Package {
	t.Helper()
	sources := loadStoragePackageSources()
	if sources.err != nil {
		t.Fatal(sources.err)
	}
	files := append([]*ast.File{}, sources.files...)
	if overlay != "" {
		file, err := parser.ParseFile(sources.fset, "overlay.go", overlay, parser.SkipObjectResolution)
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, file)
	}
	lookup := func(path string) (io.ReadCloser, error) {
		if sources.exports[path] == "" {
			return nil, fmt.Errorf("no export data for %s", path)
		}
		return os.Open(sources.exports[path])
	}
	config := types.Config{Importer: importer.ForCompiler(sources.fset, "gc", lookup), FakeImportC: true, Sizes: types.SizesFor("gc", "amd64")}
	pkg, err := config.Check(sources.importPath, sources.fset, files, nil)
	if err != nil {
		t.Fatalf("type-check: %v", err)
	}
	return pkg
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
// arguments, type parameter constraints and union terms, and the exported
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
			if args := typ.TypeArgs(); args != nil {
				for index := 0; index < args.Len(); index++ {
					add(args.At(index), fmt.Sprintf("%s[%d]", next.path, index))
				}
			}
		case *types.Named:
			args := typ.TypeArgs()
			if typ.Obj().Pkg() != pkg && args.Len() == 0 {
				continue
			}
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
			if params := typ.TypeParams(); params != nil {
				for index := 0; index < params.Len(); index++ {
					add(params.At(index), fmt.Sprintf("%s[T%d]", next.path, index))
				}
			}
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

// constraintContractRevisionLeaks type-checks this package plus an optional
// overlay source and returns, for each way the exported API's closure reaches
// constraintContractRevision, the path that reaches it.
func constraintContractRevisionLeaks(t *testing.T, overlay string) []string {
	t.Helper()
	pkg := checkStoragePackage(t, overlay)
	revision := pkg.Scope().Lookup("constraintContractRevision")
	if revision == nil {
		t.Fatal("constraintContractRevision not found")
	}
	reached, err := exportedClosure(pkg, exportedAPIRoots(pkg))
	if err != nil {
		t.Fatal(err)
	}
	var leaks []string
	for typ, path := range reached {
		if named, ok := typ.(*types.Named); ok && named.Obj() == revision {
			leaks = append(leaks, path)
		}
	}
	sort.Strings(leaks)
	return leaks
}

// No exported storage API lets a caller reach an installed contract revision:
// installed schema state is reachable only through the opaque handle.
func TestConstraintContractRevisionStaysInternal(t *testing.T) {
	if leaks := constraintContractRevisionLeaks(t, ""); len(leaks) > 0 {
		t.Fatalf("exported APIs reach constraintContractRevision:\n%s", strings.Join(leaks, "\n"))
	}
	handle := reflect.TypeOf(CompiledConstraintContract{})
	if handle.NumField() != 1 || handle.Field(0).IsExported() {
		t.Fatalf("CompiledConstraintContract must be an opaque handle with one private field, got %d fields", handle.NumField())
	}
}

// Each planted API is type-checked on its own, so one leak cannot hide a path
// the guard misses. Leaks must be found; the opaque shapes must not be.
func TestConstraintContractRevisionGuardMutations(t *testing.T) {
	carrier := `type plantedCarrier struct{ revision *constraintContractRevision }

func (c plantedCarrier) Revision() *constraintContractRevision { return c.revision }
`
	leaks := map[string]string{
		"direct-result":    `func PlantedResult(h CompiledConstraintContract) *constraintContractRevision { return h.revision }`,
		"interface-result": carrier + `func PlantedInterface(h CompiledConstraintContract) interface{ Revision() *constraintContractRevision } { return plantedCarrier{h.revision} }`,
		"direct-callback":  `func PlantedCallback(h CompiledConstraintContract, visit func(*constraintContractRevision)) { visit(h.revision) }`,
		"embedded-carrier": carrier + `type PlantedEmbedded struct{ plantedCarrier }`,
		"alias-result": `type PlantedAlias = constraintContractRevision

func PlantedAliasResult(h CompiledConstraintContract) *PlantedAlias { return h.revision }`,
		"private-carrier-result": carrier + `func PlantedCarrierResult(h CompiledConstraintContract) plantedCarrier { return plantedCarrier{h.revision} }`,
		"callback-struct":        `func PlantedCallbackStruct(h CompiledConstraintContract, callbacks struct{ Visit func(*constraintContractRevision) }) { callbacks.Visit(h.revision) }`,
		"callback-slice":         `func PlantedCallbackSlice(h CompiledConstraintContract, callbacks []func(*constraintContractRevision)) { callbacks[0](h.revision) }`,
		"callback-map":           `func PlantedCallbackMap(h CompiledConstraintContract, callbacks map[string]func(*constraintContractRevision)) { callbacks["visit"](h.revision) }`,
		// The production-shaped carrier the round-4 review planted.
		"review-carrier": `type reviewR4Carrier struct{ revision *constraintContractRevision }

func (c reviewR4Carrier) Revision() *constraintContractRevision { return c.revision }
func ReviewR4Carrier(h CompiledConstraintContract) reviewR4Carrier { return reviewR4Carrier{h.revision} }`,
		"alias-callback": `type PlantedVisit = func(*constraintContractRevision)

func PlantedAliasCallback(h CompiledConstraintContract, visit PlantedVisit) { visit(h.revision) }`,
		"alias-callback-map": `type plantedVisits = map[string]func(*constraintContractRevision)

func PlantedAliasCallbackMap(h CompiledConstraintContract, visits plantedVisits) {}`,
		"callback-array":    `func PlantedCallbackArray(h CompiledConstraintContract, callbacks [1]func(*constraintContractRevision)) {}`,
		"callback-chan":     `func PlantedCallbackChan(h CompiledConstraintContract, callbacks chan func(*constraintContractRevision)) {}`,
		"variadic-callback": `func PlantedVariadic(h CompiledConstraintContract, callbacks ...func(*constraintContractRevision)) {}`,
		"nested-callback":   `func PlantedNested(register func(func(*constraintContractRevision))) {}`,
		"callback-pointer-struct": `type plantedHooks struct{ Visit func(*constraintContractRevision) }

func PlantedHooks(hooks *plantedHooks) {}`,
		"exported-var":   `var PlantedHook func(*constraintContractRevision)`,
		"exported-field": `type PlantedHolder struct{ Revision *constraintContractRevision }`,
		"promoted-field": `type plantedInner struct{ Revision *constraintContractRevision }
type PlantedOuter struct{ *plantedInner }`,
		"method-result": `func (compiled CompiledConstraintContract) PlantedRevision() *constraintContractRevision { return compiled.revision }`,
		"pointer-method-result": `type plantedBox struct{ revision *constraintContractRevision }

func (b *plantedBox) Revision() *constraintContractRevision { return b.revision }
func PlantedBox() plantedBox { return plantedBox{} }`,
		"generic-container": `type plantedList[T any] struct{ items []T }

func (l plantedList[T]) First() T { return l.items[0] }
func PlantedGeneric() plantedList[*constraintContractRevision] { return plantedList[*constraintContractRevision]{} }`,
		"foreign-generic":       `func PlantedAtomic() *atomic.Pointer[constraintContractRevision] { return nil }`,
		"type-param-constraint": `func PlantedConstraint[T interface{ ~*constraintContractRevision }](value T) {}`,
		"interface-embedding": `type plantedReader interface{ Revision() *constraintContractRevision }
type PlantedReader interface{ plantedReader }`,
		"func-type-result": `type PlantedLoader func() *constraintContractRevision`,
	}
	opaque := map[string]string{
		"private-function": `func plantedPrivate(h CompiledConstraintContract) *constraintContractRevision { return h.revision }`,
		"private-field":    `type PlantedOpaque struct{ revision *constraintContractRevision }`,
		"private-method":   `func (compiled CompiledConstraintContract) plantedRevision() *constraintContractRevision { return compiled.revision }`,
		"unexported-carrier": carrier[:strings.Index(carrier, "\n")+1] + `func (c plantedCarrier) revisionOf() *constraintContractRevision { return c.revision }
func PlantedOpaqueCarrier(h CompiledConstraintContract) plantedCarrier { return plantedCarrier{h.revision} }`,
		"private-type": carrier + `func plantedUse(c plantedCarrier) {}`,
	}
	run := func(name, source string, wantLeak bool) {
		t.Run(name, func(t *testing.T) {
			if strings.Contains(source, "atomic.") {
				source = "import \"sync/atomic\"\n\n" + source
			}
			found := constraintContractRevisionLeaks(t, "package storage\n\n"+source+"\n")
			switch {
			case wantLeak && len(found) == 0:
				t.Fatalf("guard missed the planted leak")
			case !wantLeak && len(found) > 0:
				t.Fatalf("guard reported an opaque shape: %v", found)
			}
			t.Logf("reported: %v", found)
		})
	}
	for name, source := range leaks {
		run("leak/"+name, source, true)
	}
	for name, source := range opaque {
		run("opaque/"+name, source, false)
	}
}

// unknownTypeKind is a types.Type kind the closure does not know.
type unknownTypeKind struct{}

func (unknownTypeKind) Underlying() types.Type { return unknownTypeKind{} }
func (unknownTypeKind) String() string         { return "unknownTypeKind" }

// A type kind the closure does not know fails the check, as a root or nested
// in a signature, instead of passing unexamined.
func TestConstraintContractRevisionClosureRejectsUnknownKinds(t *testing.T) {
	result := types.NewVar(token.NoPos, nil, "result", unknownTypeKind{})
	nested := types.NewSignatureType(nil, nil, nil, nil, types.NewTuple(result), false)
	for name, typ := range map[string]types.Type{"root": unknownTypeKind{}, "nested": types.NewSlice(nested)} {
		t.Run(name, func(t *testing.T) {
			_, err := exportedClosure(types.NewPackage("example.com/p", "p"), []exportedAPIRoot{{name: "Root", typ: typ}})
			if err == nil || !strings.Contains(err.Error(), "unknown type kind") {
				t.Fatalf("closure accepted an unknown type kind: %v", err)
			}
		})
	}
}
