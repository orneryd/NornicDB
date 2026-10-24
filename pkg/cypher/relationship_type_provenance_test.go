package cypher

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
	"strings"
	"testing"
)

// Use the compiler's field/function identities, not local variable spellings
// or a delimiter list. Export metadata comes from this worker's cached build;
// no test binary is run and no source is generated or changed by go list.
func relationshipTypeImporter(t *testing.T) (types.Importer, []string) {
	t.Helper()
	command := exec.Command("go", "list", "-deps", "-export", "-json", ".")
	output, err := command.Output()
	if err != nil {
		t.Fatalf("relationship type boundary export metadata: %v", err)
	}
	exports := map[string]string{}
	var files []string
	decoder := json.NewDecoder(bytes.NewReader(output))
	for {
		var pkg struct {
			ImportPath, Export, Dir string
			GoFiles, CgoFiles       []string
		}
		if err := decoder.Decode(&pkg); err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		exports[pkg.ImportPath] = pkg.Export
		if pkg.ImportPath == "github.com/orneryd/nornicdb/pkg/cypher" {
			for _, name := range append(pkg.GoFiles, pkg.CgoFiles...) {
				files = append(files, filepath.Join(pkg.Dir, name))
			}
		}
	}
	if len(files) == 0 {
		t.Fatal("missing actual Cypher build files")
	}
	return importer.ForCompiler(token.NewFileSet(), "gc", func(path string) (io.ReadCloser, error) {
		file := exports[path]
		if file == "" {
			return nil, fmt.Errorf("missing export for %s", path)
		}
		return os.Open(file)
	}), files
}

func relationshipTypedOwnerViolations(sources map[string]string, imports types.Importer) []string {
	fset := token.NewFileSet()
	var files []*ast.File
	for name, source := range sources {
		file, err := parser.ParseFile(fset, name, source, 0)
		if err != nil {
			return []string{err.Error()}
		}
		files = append(files, file)
	}
	info := &types.Info{Types: map[ast.Expr]types.TypeAndValue{}, Defs: map[*ast.Ident]types.Object{}, Uses: map[*ast.Ident]types.Object{}, Selections: map[*ast.SelectorExpr]*types.Selection{}}
	var violations []string
	config := types.Config{Importer: imports, Error: func(err error) { violations = append(violations, err.Error()) }}
	checked, err := config.Check("github.com/orneryd/nornicdb/pkg/cypher", fset, files, info)
	if err != nil {
		return violations
	}
	owner := func(position token.Pos) bool {
		return filepath.Base(fset.Position(position).Filename) == "relationship_declaration.go"
	}
	opaque := func(typ types.Type) bool {
		if typ == nil {
			return false
		}
		if pointer, ok := typ.(*types.Pointer); ok {
			typ = pointer.Elem()
		}
		named, ok := types.Unalias(typ).(*types.Named)
		return ok && named.Obj().Pkg() != nil && named.Obj().Pkg().Path() == "github.com/orneryd/nornicdb/pkg/cypher" && (named.Obj().Name() == "relationshipTypes" || named.Obj().Name() == "relationshipTypeFacts")
	}
	object := func(expr ast.Expr) types.Object {
		switch value := expr.(type) {
		case *ast.Ident:
			if obj := info.Uses[value]; obj != nil {
				return obj
			}
			return info.Defs[value]
		case *ast.SelectorExpr:
			if selection := info.Selections[value]; selection != nil {
				return selection.Obj()
			}
			return info.Uses[value.Sel]
		}
		return nil
	}
	privateFields := map[types.Object]bool{}
	for _, obj := range info.Defs {
		if named, ok := obj.(*types.TypeName); ok && opaque(named.Type()) {
			if shape, ok := named.Type().Underlying().(*types.Struct); ok {
				for index := 0; index < shape.NumFields(); index++ {
					privateFields[shape.Field(index)] = true
				}
			}
		}
	}
	functions := map[*types.Func]*ast.FuncDecl{}
	for _, file := range files {
		for _, decl := range file.Decls {
			if fn, ok := decl.(*ast.FuncDecl); ok {
				if obj, ok := info.Defs[fn.Name].(*types.Func); ok {
					functions[obj] = fn
				}
			}
		}
	}
	var carriesFacts func(types.Type, map[types.Type]bool) bool
	carriesFacts = func(typ types.Type, seen map[types.Type]bool) bool {
		if typ == nil || seen[typ] {
			return false
		}
		if opaque(typ) {
			return true
		}
		seen[typ] = true
		switch value := typ.Underlying().(type) {
		case *types.Pointer:
			return carriesFacts(value.Elem(), seen)
		case *types.Slice:
			return carriesFacts(value.Elem(), seen)
		case *types.Array:
			return carriesFacts(value.Elem(), seen)
		case *types.Struct:
			for index := 0; index < value.NumFields(); index++ {
				if carriesFacts(value.Field(index).Type(), seen) {
					return true
				}
			}
		}
		return false
	}
	factTypes, checkedFactTypes := map[types.Type]bool{}, map[types.Type]bool{}
	containsFacts := func(typ types.Type) bool {
		if !checkedFactTypes[typ] {
			factTypes[typ] = carriesFacts(typ, map[types.Type]bool{})
			checkedFactTypes[typ] = true
		}
		return factTypes[typ]
	}
	decoded := map[types.Object]bool{}
	terminalFields := map[types.Object]bool{}
	if result := checked.Scope().Lookup("ExecuteResult"); result != nil {
		if packet, ok := result.Type().Underlying().(*types.Struct); ok {
			for index := 0; index < packet.NumFields(); index++ {
				if field := packet.Field(index); field.Exported() {
					terminalFields[field] = true
				}
			}
		}
	}
	results := map[*types.Func]map[int]bool{}
	for fn, decl := range functions {
		if !owner(decl.Pos()) {
			continue
		}
		signature := fn.Type().(*types.Signature)
		if signature.Recv() != nil && opaque(signature.Recv().Type()) {
			for index := 0; index < signature.Results().Len(); index++ {
				if relationshipStringCarrier(signature.Results().At(index).Type()) {
					if results[fn] == nil {
						results[fn] = map[int]bool{}
					}
					results[fn][index] = true
				}
			}
		}
	}
	var tainted func(ast.Expr, int) bool
	tainted = func(expr ast.Expr, result int) bool {
		if obj := object(expr); obj != nil && decoded[obj] {
			return result == 0
		}
		switch value := expr.(type) {
		case *ast.ParenExpr:
			return tainted(value.X, result)
		case *ast.UnaryExpr:
			return value.Op == token.AND && tainted(value.X, result)
		case *ast.StarExpr:
			return tainted(value.X, result)
		case *ast.IndexExpr:
			return result == 0 && tainted(value.X, 0)
		case *ast.SliceExpr:
			return result == 0 && tainted(value.X, 0)
		case *ast.TypeAssertExpr:
			return result == 0 && tainted(value.X, 0)
		case *ast.CompositeLit:
			if typ := info.TypeOf(value); typ != nil && relationshipStringCarrier(typ) {
				for _, element := range value.Elts {
					if tainted(element, 0) {
						return true
					}
				}
			}
		case *ast.KeyValueExpr:
			return result == 0 && (tainted(value.Key, 0) || tainted(value.Value, 0))
		case *ast.BinaryExpr:
			return result == 0 && value.Op == token.ADD && (tainted(value.X, 0) || tainted(value.Y, 0))
		case *ast.CallExpr:
			if selection, ok := value.Fun.(*ast.SelectorExpr); ok && relationshipCopyBuffer(info.TypeOf(selection.X)) && tainted(selection.X, 0) {
				// Append-only serialization preserves origin through String/Bytes.
				return result == 0 && relationshipStringCarrier(info.TypeOf(value))
			}
			if fn, ok := object(value.Fun).(*types.Func); ok {
				signature := fn.Type().(*types.Signature)
				if results[fn][result] || result < signature.Results().Len() && decoded[signature.Results().At(result)] {
					return true
				}
			}
			if result == 0 {
				// Conversions and text transformations retain origin. A storage
				// query's newly returned entities are data, not aliases of its
				// type-name argument.
				_, local := functions[funcObject(object(value.Fun))]
				transform := info.Types[value.Fun].IsType()
				if builtin, ok := object(value.Fun).(*types.Builtin); ok {
					transform = builtin.Name() == "append"
				}
				if fn, ok := object(value.Fun).(*types.Func); ok && fn.Pkg() != nil {
					switch fn.Pkg().Path() {
					case "strings", "bytes", "fmt", "unicode/utf8":
						transform = true
					}
				}
				if !local && transform {
					for _, arg := range value.Args {
						if tainted(arg, 0) {
							return true
						}
					}
				}
			}
		}
		return false
	}
	for changed := true; changed; {
		changed = false
		mark := func(obj types.Object) {
			// Entity fields belong to the storage value contract. Likewise a
			// public result packet contains client data, not pattern facts.
			if terminalFields[obj] || obj != nil && obj.Pkg() != nil && obj.Pkg().Path() != "github.com/orneryd/nornicdb/pkg/cypher" {
				return
			}
			if obj != nil && relationshipStringCarrier(obj.Type()) && !decoded[obj] {
				decoded[obj] = true
				changed = true
			}
		}
		assign := func(left []ast.Expr, right []ast.Expr) {
			for i, target := range left {
				if len(right) == 1 && tainted(right[0], i) || i < len(right) && tainted(right[i], 0) {
					mark(object(target))
					if index, ok := target.(*ast.IndexExpr); ok {
						mark(object(index.X))
					}
				}
				// A pointer to a serialization buffer shares its content. Propagate
				// both ways, including writes made through an address alias.
				if i < len(right) && relationshipCopyBuffer(info.TypeOf(target)) {
					source := right[i]
					if address, ok := source.(*ast.UnaryExpr); ok && address.Op == token.AND {
						source = address.X
					} else if _, pointer := info.TypeOf(source).(*types.Pointer); !pointer {
						continue
					}
					if tainted(target, 0) {
						mark(object(source))
					}
					if tainted(source, 0) {
						mark(object(target))
					}
				}
			}
		}
		for fn, decl := range functions {
			owned := owner(decl.Pos())
			ast.Inspect(decl.Body, func(node ast.Node) bool {
				switch value := node.(type) {
				case *ast.AssignStmt:
					assign(value.Lhs, value.Rhs)
				case *ast.ValueSpec:
					var left []ast.Expr
					for _, name := range value.Names {
						left = append(left, name)
					}
					assign(left, value.Values)
				case *ast.RangeStmt:
					if tainted(value.X, 0) {
						mark(object(value.Value))
						mark(object(value.Key))
					}
				case *ast.CompositeLit:
					typ := info.TypeOf(value)
					if typ == nil {
						break
					}
					shape, ok := typ.Underlying().(*types.Struct)
					if !ok {
						break
					}
					for index, element := range value.Elts {
						fieldIndex, item := index, element
						if key, ok := element.(*ast.KeyValueExpr); ok {
							item = key.Value
							fieldIndex = -1
							if name, ok := key.Key.(*ast.Ident); ok {
								for j := 0; j < shape.NumFields(); j++ {
									if shape.Field(j).Name() == name.Name {
										fieldIndex = j
										break
									}
								}
							}
						}
						if fieldIndex >= 0 && fieldIndex < shape.NumFields() && tainted(item, 0) {
							mark(shape.Field(fieldIndex))
						}
					}
				case *ast.CallExpr:
					if selection, ok := value.Fun.(*ast.SelectorExpr); ok && relationshipCopyBuffer(info.TypeOf(selection.X)) {
						for _, arg := range value.Args {
							if tainted(arg, 0) {
								mark(object(selection.X))
							}
						}
					}
					// Ownership includes the owner's canonical identifier/type
					// operations. Calling one there must not turn its generic
					// implementation into an unowned pattern consumer.
					if owned {
						break
					}
					if called, ok := object(value.Fun).(*types.Func); ok {
						if _, local := functions[called]; local {
							signature := called.Type().(*types.Signature)
							for index, arg := range value.Args {
								if index < signature.Params().Len() && tainted(arg, 0) {
									mark(signature.Params().At(index))
								}
							}
						}
					}
				case *ast.ReturnStmt:
					count := len(value.Results)
					if count == 1 {
						if tuple, ok := info.TypeOf(value.Results[0]).(*types.Tuple); ok {
							count = tuple.Len()
						}
					}
					for index := 0; index < count; index++ {
						match := len(value.Results) == 1 && tainted(value.Results[0], index) || index < len(value.Results) && tainted(value.Results[index], 0)
						if match && !results[fn][index] {
							if results[fn] == nil {
								results[fn] = map[int]bool{}
							}
							results[fn][index] = true
							changed = true
						}
					}
				}
				return true
			})
		}
	}
	add := func(node ast.Node, why string) {
		violations = append(violations, fset.Position(node.Pos()).String()+": "+why)
	}
	for _, file := range files {
		if owner(file.Pos()) {
			continue
		}
		ast.Inspect(file, func(node ast.Node) bool {
			switch value := node.(type) {
			case *ast.SelectorExpr:
				if selection := info.Selections[value]; selection != nil && privateFields[selection.Obj()] {
					add(node, "opaque relationship type field opened")
				}
				if selection := info.Selections[value]; selection != nil && opaque(selection.Recv()) {
					if method, ok := selection.Obj().(*types.Func); ok {
						if _, pointer := method.Type().(*types.Signature).Recv().Type().(*types.Pointer); pointer {
							add(node, "relationship type facts mutated")
						}
					}
				}
			case *ast.CompositeLit:
				if opaque(info.TypeOf(value)) && len(value.Elts) > 0 {
					add(node, "relationship type facts constructed outside owner")
				}
			case *ast.FuncDecl:
				if value.Recv != nil && opaque(info.TypeOf(value.Recv.List[0].Type)) {
					add(node, "relationship type method added outside owner")
				}
			case *ast.TypeSpec:
				if opaque(info.TypeOf(value.Type)) {
					add(node, "relationship type representation aliased outside owner")
				}
			case *ast.CallExpr:
				// A decoded token cannot be turned back into raw pattern text,
				// even through a function-value alias of an owner entry point.
				if signature, ok := info.TypeOf(value.Fun).(*types.Signature); ok && signature.Recv() == nil {
					constructs := false
					for index := 0; index < signature.Results().Len(); index++ {
						constructs = constructs || containsFacts(signature.Results().At(index).Type())
					}
					if constructs {
						for _, arg := range value.Args {
							if tainted(arg, 0) {
								add(node, "decoded relationship type reparsed into pattern facts")
								break
							}
						}
					}
				}
				if called, ok := object(value.Fun).(*types.Func); ok && called.Pkg() != nil {
					path, name := called.Pkg().Path(), called.Name()
					inspect := path == "regexp" || path == "strings" || path == "bytes" || path == "unicode/utf8"
					if (path == "strings" || path == "bytes") && (name == "Equal" || name == "EqualFold" || name == "Compare" || name == "Clone") {
						inspect = false
					}
					if signature := called.Type().(*types.Signature); signature.Recv() != nil && relationshipCopyBuffer(signature.Recv().Type()) {
						// These operations copy bytes or expose the copied payload;
						// they never interpret a relationship declaration. Origin is
						// retained, so inspection of their output is still forbidden.
						switch name {
						case "Write", "WriteString", "WriteByte", "WriteRune", "Grow", "Len", "Cap", "Reset", "String", "Bytes":
							inspect = false
						}
					}
					if inspect {
						if selection, ok := value.Fun.(*ast.SelectorExpr); ok && tainted(selection.X, 0) {
							add(node, "decoded relationship type reclassified by receiver operation")
						}
						for _, arg := range value.Args {
							if tainted(arg, 0) {
								add(node, "decoded relationship type reclassified by string operation")
								break
							}
						}
					}
				}
			case *ast.IndexExpr:
				if tainted(value.X, 0) && isRelationshipString(info.TypeOf(value.X)) {
					add(node, "decoded relationship type indexed as syntax")
				}
			case *ast.SliceExpr:
				if tainted(value.X, 0) && isRelationshipString(info.TypeOf(value.X)) {
					add(node, "decoded relationship type sliced as syntax")
				}
			case *ast.RangeStmt:
				if tainted(value.X, 0) && isRelationshipString(info.TypeOf(value.X)) {
					add(node, "decoded relationship type scanned as syntax")
				}
			}
			return true
		})
	}
	return violations
}

func relationshipStringCarrier(typ types.Type) bool {
	if typ == nil {
		return false
	}
	if relationshipCopyBuffer(typ) {
		return true
	}
	switch value := typ.Underlying().(type) {
	case *types.Basic:
		return value.Info()&types.IsString != 0
	case *types.Slice:
		return isRelationshipCharacter(value.Elem()) || relationshipStringCarrier(value.Elem())
	case *types.Array:
		return isRelationshipCharacter(value.Elem()) || relationshipStringCarrier(value.Elem())
	case *types.Map:
		return relationshipStringCarrier(value.Key()) || relationshipStringCarrier(value.Elem())
	case *types.Interface:
		return true
	}
	return false
}

// Recognize standard append/copy containers by compiler type identity. This
// describes a data operation, never an exception for a pattern consumer.
func relationshipCopyBuffer(typ types.Type) bool {
	if pointer, ok := typ.(*types.Pointer); ok {
		typ = pointer.Elem()
	}
	if typ == nil {
		return false
	}
	named, ok := types.Unalias(typ).(*types.Named)
	if !ok || named.Obj().Pkg() == nil {
		return false
	}
	path, name := named.Obj().Pkg().Path(), named.Obj().Name()
	return path == "strings" && name == "Builder" || path == "bytes" && name == "Buffer"
}
func funcObject(object types.Object) *types.Func { fn, _ := object.(*types.Func); return fn }
func isRelationshipString(typ types.Type) bool {
	if typ == nil {
		return false
	}
	switch container := typ.Underlying().(type) {
	case *types.Slice:
		return isRelationshipCharacter(container.Elem())
	case *types.Array:
		return isRelationshipCharacter(container.Elem())
	}
	basic, ok := typ.Underlying().(*types.Basic)
	return ok && basic.Info()&types.IsString != 0
}

func isRelationshipCharacter(typ types.Type) bool {
	basic, ok := typ.Underlying().(*types.Basic)
	return ok && (basic.Kind() == types.Uint8 || basic.Kind() == types.Int32)
}

func TestRelationshipOwnerTypedProvenanceBoundary(t *testing.T) {
	imports, files := relationshipTypeImporter(t)
	sources := map[string]string{}
	for _, file := range files {
		data, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		sources[filepath.Base(file)] = string(data)
	}
	if failures := relationshipTypedOwnerViolations(sources, imports); len(failures) != 0 {
		t.Fatal(strings.Join(failures, "\n"))
	}
	const owner = `package cypher
type relationshipTypeFacts struct{name string;quoted,dynamic bool}
type relationshipTypes struct{first relationshipTypeFacts}
func(t relationshipTypes)at(int)relationshipTypeFacts{return t.first}
func(t relationshipTypeFacts)storageName()string{return t.name}
type relationshipDeclaration struct{types relationshipTypes}
func(d relationshipDeclaration)staticTypeName()string{return d.types.at(0).storageName()}
func(d relationshipDeclaration)fulltextTypes()([]string,bool){return []string{d.types.at(0).storageName()},true}`
	for _, body := range []string{
		`func bad(d relationshipDeclaration){strings.Split(d.staticTypeName(),"|")}`,
		`func bad(d relationshipDeclaration){name:=d.staticTypeName();parse(name)};func parse(raw string){for _,c:=range raw{switch c{case '|':return}}}`,
		"func bad(d relationshipDeclaration){names,_:=d.fulltextTypes();strings.IndexByte(names[0],'`')}",
		`func bad(d relationshipDeclaration){strings.HasPrefix(d.types.at(0).storageName(),"$(")}`,
		`func bad(d relationshipDeclaration){name:=d.types.at(0).storageName();strings.HasPrefix(name,"x")}`,
		`func bad(d relationshipDeclaration){name:=d.types.at(0).storageName();parse(name)};func parse(value string){strings.HasPrefix(value,"$(")}`,
		`type plan struct{types relationshipTypes};func bad(p plan)bool{return strings.HasPrefix(p.types.at(0).storageName(),"$(")}`,
		`type plan struct{types relationshipTypes};type erased struct{name string};func bad(p plan)bool{e:=erased{name:p.types.at(0).storageName()};return strings.HasPrefix(e.name,"anything")}`,
		`type plan struct{types relationshipTypes};type erased struct{name string};func erase(p plan)erased{return erased{name:p.types.at(0).storageName()}};func bad(p plan)bool{return strings.HasPrefix(erase(p).name,"$(")}`,
		`func bad(value relationshipTypeFacts)bool{alias:=value;return strings.HasPrefix(alias.name,"$(")}`,
		`type wrapper struct{relationshipTypeFacts};func bad(value wrapper)bool{return strings.HasPrefix(value.name,"$(")}`,
		`func bad(value relationshipTypeFacts)bool{return inspect(value.storageName())};func inspect(name string)bool{for _,c:=range name{if c=='$'{return true}};return false}`,
		`func erase(value relationshipTypeFacts)(name string){name=value.storageName();return};func bad(value relationshipTypeFacts)bool{return strings.HasPrefix(erase(value),"$(")}`,
		`func bad(value relationshipTypeFacts)bool{var boxed interface{}=value.storageName();name:=boxed.(string);return strings.HasPrefix(name,"$(")}`,
		`func bad(value relationshipTypeFacts)bool{names:=make([]string,1);names[0]=value.storageName();return strings.HasPrefix(names[0],"$(")}`,
		`func bad(value relationshipTypeFacts)bool{names:=[]string{value.storageName()};return strings.HasPrefix(names[0],"$(")}`,
		`func bad(value relationshipTypeFacts)bool{raw:=[]byte(value.storageName());return strings.HasPrefix(string(raw),"$(")}`,
		`func bad(value relationshipTypeFacts)bool{var b strings.Builder;b.WriteString(value.storageName());return strings.HasPrefix(b.String(),"$(")}`,
		`func bad(value relationshipTypeFacts)bool{var b strings.Builder;alias:=&b;alias.WriteString(value.storageName());return strings.HasPrefix(b.String(),"$(")}`,
		`func bad(value relationshipTypeFacts)bool{var b bytes.Buffer;b.WriteString(value.storageName());return strings.HasPrefix(string(b.Bytes()),"$(")}`,
		`func parse(raw string)relationshipTypes{return relationshipTypes{}};func bad(value relationshipTypeFacts){reader:=parse;_ = reader(":"+value.storageName())}`,
	} {
		failures := relationshipTypedOwnerViolations(map[string]string{"relationship_declaration.go": owner, "consumer.go": "package cypher;import(\"strings\";\"bytes\");var _=strings.HasPrefix;var _ bytes.Buffer;" + body}, imports)
		caught := false
		for _, failure := range failures {
			caught = caught || strings.Contains(failure, "decoded relationship type") || strings.Contains(failure, "opaque relationship type")
		}
		if !caught {
			t.Errorf("missed typed provenance mutation: %s; diagnostics: %v", body, failures)
		}
	}
	const serialization = `package cypher;import "strings"
func key(value relationshipTypeFacts)string{var b strings.Builder;b.WriteString("cache:");b.WriteString(value.storageName());return b.String()}
func same(a,b relationshipTypeFacts)bool{return a.storageName()==b.storageName()}`
	if failures := relationshipTypedOwnerViolations(map[string]string{"relationship_declaration.go": owner, "consumer.go": serialization}, imports); len(failures) != 0 {
		t.Fatalf("byte-preserving serialization or equality rejected: %s", strings.Join(failures, "\n"))
	}
}
