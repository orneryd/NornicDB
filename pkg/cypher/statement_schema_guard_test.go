package cypher

import (
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func statementSchemaFactViolations(sources map[string]string, imports types.Importer) []string {
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
	var failures []string
	config := types.Config{Importer: imports, Error: func(err error) { failures = append(failures, err.Error()) }}
	pkg, err := config.Check("github.com/orneryd/nornicdb/pkg/cypher", fset, files, info)
	if err != nil {
		return failures
	}
	owner := func(pos token.Pos) bool {
		return filepath.Base(fset.Position(pos).Filename) == "statement_schema_facts.go"
	}
	protected := map[types.Object]bool{}
	var factsTypes []types.Type
	for _, name := range []string{"statementSchemaFacts", "statementSchemaBuilder", "statementSchemaReference"} {
		if named, ok := pkg.Scope().Lookup(name).(*types.TypeName); ok {
			factsTypes = append(factsTypes, named.Type())
			if fields, ok := named.Type().Underlying().(*types.Struct); ok {
				for index := 0; index < fields.NumFields(); index++ {
					protected[fields.Field(index)] = true
				}
			}
		}
	}
	facts := func(typ types.Type) bool {
		if pointer, ok := typ.(*types.Pointer); ok {
			typ = pointer.Elem()
		}
		for _, factsType := range factsTypes {
			if typ != nil && types.Identical(typ, factsType) {
				return true
			}
		}
		return false
	}
	// Explicit-name factories/writers are only the owner's public data boundary.
	// Internal producers cannot feed a text-scanned []string into them.
	dataBoundary := map[types.Object]bool{}
	for _, file := range files {
		for _, declaration := range file.Decls {
			fn, ok := declaration.(*ast.FuncDecl)
			if !ok || !owner(fn.Pos()) {
				continue
			}
			obj, ok := info.Defs[fn.Name].(*types.Func)
			if !ok {
				continue
			}
			signature := obj.Type().(*types.Signature)
			stringsInput := false
			for index := 0; index < signature.Params().Len(); index++ {
				if slice, ok := signature.Params().At(index).Type().(*types.Slice); ok && types.Identical(slice.Elem(), types.Typ[types.String]) {
					stringsInput = true
				}
			}
			if stringsInput && (signature.Recv() != nil || signature.Results().Len() > 0 && facts(signature.Results().At(0).Type())) {
				dataBoundary[obj] = true
			}
		}
	}
	report := func(pos token.Pos, message string) {
		failures = append(failures, fset.Position(pos).String()+": schema producer boundary: "+message)
	}
	for _, file := range files {
		ast.Inspect(file, func(n ast.Node) bool {
			if n == nil || owner(n.Pos()) {
				return true
			}
			switch value := n.(type) {
			case *ast.SelectorExpr:
				if selection := info.Selections[value]; selection != nil && protected[selection.Obj()] {
					report(value.Pos(), "dependency representation opened outside owner")
				}
			case *ast.CompositeLit:
				if facts(info.TypeOf(value)) {
					report(value.Pos(), "dependency facts constructed outside owner")
				}
			case *ast.Ident:
				if dataBoundary[info.Uses[value]] {
					report(value.Pos(), "explicit-name data boundary used by internal query consumer")
				}
			case *ast.FuncDecl:
				if value.Recv != nil && len(value.Recv.List) > 0 && facts(info.TypeOf(value.Recv.List[0].Type)) {
					report(value.Pos(), "dependency method added outside owner")
				}
			}
			return true
		})
		for _, declaration := range file.Decls {
			fn, ok := declaration.(*ast.FuncDecl)
			if !ok || fn.Body == nil || owner(fn.Pos()) {
				continue
			}
			// Whole-statement schema scanners have no bracket provenance. Look
			// at their actual string results and delimiter inspection instead.
			obj, _ := info.Defs[fn.Name].(*types.Func)
			if obj == nil {
				continue
			}
			signature := obj.Type().(*types.Signature)
			textOutput := false
			for index := 0; index < signature.Results().Len(); index++ {
				typ := signature.Results().At(index).Type()
				if slice, ok := typ.(*types.Slice); ok {
					typ = slice.Elem()
				}
				textOutput = textOutput || types.Identical(typ, types.Typ[types.String])
			}
			if !textOutput {
				continue
			}
			colon, pipe, inspect := false, false, false
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				switch value := n.(type) {
				case *ast.BasicLit:
					if value.Kind == token.CHAR || value.Kind == token.STRING {
						text, _ := strconv.Unquote(value.Value)
						colon = colon || strings.Contains(text, ":")
						pipe = pipe || strings.Contains(text, "|")
					}
				case *ast.IndexExpr, *ast.SliceExpr, *ast.RangeStmt:
					inspect = true
				case *ast.CallExpr:
					if selector, ok := value.Fun.(*ast.SelectorExpr); ok {
						if object, ok := selector.X.(*ast.Ident); ok && (object.Name == "strings" || object.Name == "regexp") {
							inspect = true
							colon = colon || object.Name == "regexp"
						}
					}
				}
				return true
			})
			grammar := filepath.Base(fset.Position(fn.Pos()).Filename)
			if textOutput && inspect && colon && pipe && statementSchemaRawNameResult(fn, info) && grammar != "relationship_declaration.go" && grammar != "label_expression.go" {
				report(fn.Pos(), "whole-text name producer requires canonical syntax facts: "+fn.Name.Name)
			}
		}
	}
	return failures
}

// statementSchemaRawNameResult tracks actual substring/split/regexp extraction
// from string parameters to returned names. Rewritten query text and formatted
// plans are constructed strings, not extracted names. Canonical token/grammar
// results are separate provenance, even when their decoded value is a string.
// This supplements the closed facts type: no bracket origin or function name
// is needed to detect the reviewer's whole-statement scanner.
func statementSchemaRawNameResult(fn *ast.FuncDecl, info *types.Info) bool {
	const source, extracted = uint8(1), uint8(2)
	origins := map[types.Object]uint8{}
	object := func(id *ast.Ident) types.Object {
		if obj := info.Defs[id]; obj != nil {
			return obj
		}
		return info.Uses[id]
	}
	for _, field := range fn.Type.Params.List {
		if types.Identical(info.TypeOf(field.Type), types.Typ[types.String]) {
			for _, name := range field.Names {
				origins[object(name)] = source
			}
		}
	}
	var origin func(ast.Expr) uint8
	origin = func(expr ast.Expr) uint8 {
		switch value := expr.(type) {
		case *ast.Ident:
			return origins[object(value)]
		case *ast.ParenExpr:
			return origin(value.X)
		case *ast.SliceExpr:
			from := origin(value.X)
			if types.Identical(info.TypeOf(value.X), types.Typ[types.String]) && from&source != 0 {
				from |= extracted
			}
			return from
		case *ast.IndexExpr:
			return origin(value.X)
		case *ast.CallExpr:
			if id, ok := value.Fun.(*ast.Ident); ok && id.Name == "append" {
				var from uint8
				for _, arg := range value.Args {
					from |= origin(arg)
				}
				return from
			}
			selector, ok := value.Fun.(*ast.SelectorExpr)
			if !ok {
				return 0
			}
			callee, _ := info.Uses[selector.Sel].(*types.Func)
			if callee == nil || callee.Pkg() == nil || len(value.Args) == 0 {
				return 0
			}
			from := origin(value.Args[0])
			switch callee.Pkg().Path() {
			case "strings":
				switch callee.Name() {
				case "Split", "SplitN", "SplitAfter", "Fields", "FieldsFunc", "TrimPrefix", "TrimSuffix":
					if from&source != 0 {
						from |= extracted
					}
					return from
				case "TrimSpace", "Trim", "TrimLeft", "TrimRight", "ToLower", "ToUpper", "Clone":
					return from
				}
			case "regexp":
				if strings.HasPrefix(callee.Name(), "Find") && strings.Contains(callee.Name(), "String") && from&source != 0 {
					return from | extracted
				}
			}
		}
		return 0
	}
	set := func(expr ast.Expr, from uint8) bool {
		id, ok := expr.(*ast.Ident)
		if !ok {
			return false
		}
		obj := object(id)
		before := origins[obj]
		origins[obj] |= from
		return before != origins[obj]
	}
	for changed := true; changed; {
		changed = false
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			switch value := n.(type) {
			case *ast.AssignStmt:
				if len(value.Lhs) == len(value.Rhs) {
					for index, rhs := range value.Rhs {
						changed = set(value.Lhs[index], origin(rhs)) || changed
					}
				}
			case *ast.ValueSpec:
				if len(value.Names) == len(value.Values) {
					for index, rhs := range value.Values {
						changed = set(value.Names[index], origin(rhs)) || changed
					}
				}
			case *ast.RangeStmt:
				if value.Value != nil {
					changed = set(value.Value, origin(value.X)) || changed
				}
			}
			return true
		})
	}
	found := false
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		if result, ok := n.(*ast.ReturnStmt); ok {
			for _, expr := range result.Results {
				found = found || origin(expr)&extracted != 0
			}
			if len(result.Results) == 0 && fn.Type.Results != nil {
				for _, field := range fn.Type.Results.List {
					for _, name := range field.Names {
						found = found || origins[object(name)]&extracted != 0
					}
				}
			}
		}
		return true
	})
	return found
}

func TestStatementSchemaProducerBoundary(t *testing.T) {
	imports, files := relationshipTypeImporter(t)
	sources := map[string]string{}
	for _, path := range files {
		source, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		sources[path] = string(source)
	}
	if failures := statementSchemaFactViolations(sources, imports); len(failures) != 0 {
		t.Fatalf("schema producer boundary violations:\n%s", strings.Join(failures, "\n"))
	}
}

func TestStatementSchemaProducerGuardMutations(t *testing.T) {
	imports, _ := relationshipTypeImporter(t)
	owner := `package cypher; type statementSchemaFacts struct { names []string }; type statementSchemaReference struct { query string; facts *statementSchemaFacts }; func statementSchemaFactsOf(q string) *statementSchemaFacts { return &statementSchemaFacts{} }`
	for _, source := range []string{
		`package cypher; func renamedWholeStatement(q string) []string { var out []string; for i:=0;i<len(q);i++ { if q[i]==':' || q[i]=='|' { out=append(out,q[i+1:]) } }; return out }`,
		`package cypher; import "strings"; func indirectWholeStatement(q string) []string { return strings.Split(strings.TrimPrefix(q, ":"), "|") }`,
		`package cypher; import "regexp"; func regexWholeStatement(q string) []string { return regexp.MustCompile(":([^|]+)").FindStringSubmatch(q) }`,
		`package cypher; func erasePlan(q string) *statementSchemaFacts { return &statementSchemaFacts{names: []string{q}} }`,
		`package cypher; func reopenPlan(q string) []string { return statementSchemaFactsOf(q).names }`,
		`package cypher; func eraseDeferred(q string) statementSchemaReference { return statementSchemaReference{query:q} }`,
		`package cypher; func reopenDeferred(r statementSchemaReference) string { return r.query }`,
		`package cypher; func (r statementSchemaReference) foreignResolve() *statementSchemaFacts { return statementSchemaFactsOf(r.query) }`,
		`package cypher; func scalarWholeStatement(q string) string { for i:=0;i<len(q);i++ { if q[i]==':' || q[i]=='|' { return q[i+1:] } }; return "" }`,
		`package cypher; import "strings"; func aliasesWholeStatement(q string) []string { raw := strings.TrimSpace(q); parts := strings.Split(strings.TrimPrefix(raw, ":"), "|"); var out []string; for _, name := range parts { out = append(out, name) }; return out }`,
	} {
		failures := statementSchemaFactViolations(map[string]string{"statement_schema_facts.go": owner, "new_consumer.go": source}, imports)
		if len(failures) == 0 || !strings.Contains(strings.Join(failures, "\n"), "schema producer boundary:") {
			t.Fatalf("mutation escaped, or failed only at compilation: %v\n%s", failures, source)
		}
	}
	// A serializer contains schema punctuation but returns constructed text.
	// A variable reader obtains tokens from its distinct lexical owner. Neither
	// is a query-to-schema producer; no consumer/file exemption is involved.
	for _, source := range []string{
		`package cypher; import "strings"; func serializer(q string) string { return "MATCH (n:" + strings.TrimSpace(q) + "|Other) RETURN n" }`,
		`package cypher; import "strings"; func token(s string) string { return s }; func references(q string) []string { if strings.ContainsAny(q, ":|") { return []string{token(q)} }; return nil }`,
	} {
		if failures := statementSchemaFactViolations(map[string]string{"statement_schema_facts.go": owner, "new_consumer.go": source}, imports); len(failures) != 0 {
			t.Fatalf("distinct data/grammar boundary rejected: %v\n%s", failures, source)
		}
	}
}
