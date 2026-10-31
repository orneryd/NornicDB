// Author: declared-list-owners lane (Claude), 2026-10-30.

package cypher

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// headerSite is one place in module source that declares or writes a
// result header: a signature's YIELD list, a []ProcedureColumn literal, or a
// []string literal.
type headerSite struct {
	names    []string
	position string
	kind     string
	// header is set where a []string literal is used as a result header: a
	// Columns value or assignment, a columns variable, or a value returned by
	// the header owner file.
	header bool
	// legacyOwner is set inside legacyProcedureColumns, the closed result
	// schema declaration of the compatibility procedures it names.
	legacyOwner bool
}

// declaredProcedureHeaders maps each header a built-in procedure declares
// through its registration (ResultSchemas, or its YIELD columns) to the
// procedures declaring it. The built-ins are registered first, so the
// census does not depend on which test ran before it.
func declaredProcedureHeaders() map[string][]string {
	ensureBuiltInProceduresRegistered()
	declared := map[string][]string{}
	for _, spec := range globalProcedureRegistry.ListBuiltIns() {
		var headers [][]string
		if spec.ResultSchemas != nil {
			headers = spec.ResultSchemas()
		} else if len(spec.Returns) > 0 {
			names := make([]string, len(spec.Returns))
			for i, column := range spec.Returns {
				names[i] = column.Name
			}
			headers = [][]string{names}
		}
		for _, header := range headers {
			key := strings.Join(header, ",")
			declared[key] = append(declared[key], spec.Name)
		}
	}
	return declared
}

// collectHeaderSites lists every header declaration site in the files.
func collectHeaderSites(t *testing.T, fset *token.FileSet, files map[string]*ast.File) []headerSite {
	var sites []headerSite
	for name, file := range files {
		owner := name == "pkg/cypher/procedure_result_columns.go"
		for _, decl := range file.Decls {
			legacy := false
			if function, ok := decl.(*ast.FuncDecl); ok {
				legacy = owner && function.Name.Name == "legacyProcedureColumns"
			}
			headerUses := map[*ast.CompositeLit]bool{}
			mark := func(expr ast.Expr) {
				if lit, ok := expr.(*ast.CompositeLit); ok {
					headerUses[lit] = true
				}
			}
			ast.Inspect(decl, func(node ast.Node) bool {
				switch n := node.(type) {
				case *ast.KeyValueExpr:
					if isIdent(n.Key, "Columns") {
						mark(n.Value)
					}
				case *ast.AssignStmt:
					for i, lhs := range n.Lhs {
						if i < len(n.Rhs) && namesColumns(lhs) {
							mark(n.Rhs[i])
						}
					}
				case *ast.ValueSpec:
					for i, ident := range n.Names {
						if i < len(n.Values) && strings.Contains(strings.ToLower(ident.Name), "column") {
							mark(n.Values[i])
						}
					}
				case *ast.ReturnStmt:
					if owner {
						for _, result := range n.Results {
							mark(result)
						}
					}
				}
				return true
			})
			ast.Inspect(decl, func(node ast.Node) bool {
				switch n := node.(type) {
				case *ast.BasicLit:
					if n.Kind != token.STRING || !strings.Contains(n.Value, ") :: (") {
						return true
					}
					text, err := strconv.Unquote(n.Value)
					require.NoError(t, err)
					if names := signatureYieldNames(text); len(names) > 0 {
						sites = append(sites, headerSite{names: names, position: fset.Position(n.Pos()).String(), kind: "signature YIELD list"})
					}
				case *ast.CompositeLit:
					array, ok := n.Type.(*ast.ArrayType)
					if !ok || array.Len != nil || len(n.Elts) == 0 {
						return true
					}
					switch {
					case isIdent(array.Elt, "string"):
						if values, ok := stringLiterals(t, n.Elts); ok {
							sites = append(sites, headerSite{names: values, position: fset.Position(n.Pos()).String(), kind: "[]string literal", header: headerUses[n], legacyOwner: legacy})
						}
					case isIdent(array.Elt, "ProcedureColumn"):
						if names, ok := procedureColumnLiteralNames(t, n.Elts); ok {
							sites = append(sites, headerSite{names: names, position: fset.Position(n.Pos()).String(), kind: "[]ProcedureColumn literal"})
						}
					}
				}
				return true
			})
		}
	}
	return sites
}

// signatureYieldNames is the ordered names of a signature literal's YIELD
// list, or nil when the literal declares none.
func signatureYieldNames(signature string) []string {
	start := strings.LastIndex(signature, ") :: (")
	if start < 0 || !strings.HasSuffix(signature, ")") {
		return nil
	}
	var names []string
	for _, output := range splitTopLevelComma(signature[start+6 : len(signature)-1]) {
		name, _, ok := strings.Cut(output, "::")
		if !ok {
			return nil
		}
		names = append(names, strings.TrimSpace(name))
	}
	return names
}

func stringLiterals(t *testing.T, elements []ast.Expr) ([]string, bool) {
	values := make([]string, 0, len(elements))
	for _, element := range elements {
		basic, ok := element.(*ast.BasicLit)
		if !ok || basic.Kind != token.STRING {
			return nil, false
		}
		value, err := strconv.Unquote(basic.Value)
		require.NoError(t, err)
		values = append(values, value)
	}
	return values, true
}

func procedureColumnLiteralNames(t *testing.T, elements []ast.Expr) ([]string, bool) {
	names := make([]string, 0, len(elements))
	for _, element := range elements {
		column, ok := element.(*ast.CompositeLit)
		if !ok {
			return nil, false
		}
		named := false
		for _, field := range column.Elts {
			if pair, ok := field.(*ast.KeyValueExpr); ok && isIdent(pair.Key, "Name") {
				basic, ok := pair.Value.(*ast.BasicLit)
				if !ok || basic.Kind != token.STRING {
					return nil, false
				}
				name, err := strconv.Unquote(basic.Value)
				require.NoError(t, err)
				names, named = append(names, name), true
			}
		}
		if !named {
			return nil, false
		}
	}
	return names, true
}

// nameBoundHeaders lists header expressions that resolve a header by
// procedure name through the registry, where a user registration can shadow
// the declaration that governs the handler.
func nameBoundHeaders(fset *token.FileSet, files map[string]*ast.File) []string {
	var bound []string
	for _, file := range files {
		ast.Inspect(file, func(node ast.Node) bool {
			var header ast.Expr
			switch n := node.(type) {
			case *ast.KeyValueExpr:
				if isIdent(n.Key, "Columns") {
					header = n.Value
				}
			case *ast.AssignStmt:
				for i, lhs := range n.Lhs {
					if i < len(n.Rhs) && namesColumns(lhs) {
						header = n.Rhs[i]
					}
				}
			}
			if header != nil && resolvesThroughRegistry(header) {
				bound = append(bound, fset.Position(header.Pos()).String())
			}
			return true
		})
	}
	sort.Strings(bound)
	return bound
}

func resolvesThroughRegistry(expr ast.Expr) bool {
	found := false
	ast.Inspect(expr, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return !found
		}
		switch fn := call.Fun.(type) {
		case *ast.Ident:
			found = found || fn.Name == "procedureResultColumns" || fn.Name == "procedureResultSchemas"
		case *ast.SelectorExpr:
			found = found || (fn.Sel.Name == "Get" && isIdent(fn.X, "globalProcedureRegistry"))
		}
		return !found
	})
	return found
}

// resultHeaderCensus lists every second declaration of a result header and
// every header bound to its declaration by name:
//   - a header of two or more columns declared at more than one site
//     (signature YIELD lists, []ProcedureColumn literals, header literals);
//   - a []string literal copy of a header a built-in procedure declares;
//   - a header expression that resolves its declaration by procedure name
//     through the registry.
func resultHeaderCensus(t *testing.T, fset *token.FileSet, files map[string]*ast.File, declared map[string][]string) []string {
	var members []string
	declarations := map[string][]string{}
	for _, site := range collectHeaderSites(t, fset, files) {
		key := strings.Join(site.names, ",")
		if len(site.names) > 1 && (site.kind != "[]string literal" || site.header) {
			declarations[key] = append(declarations[key], site.kind+" at "+site.position)
		}
		if site.kind != "[]string literal" || site.legacyOwner {
			continue
		}
		if procedures, found := declared[key]; found && (site.header || len(site.names) > 1) {
			members = append(members, "copy of the header "+key+" declared by "+strings.Join(procedures, ", ")+" at "+site.position)
		}
	}
	for header, places := range declarations {
		if len(places) > 1 {
			sort.Strings(places)
			members = append(members, "header "+header+" declared at "+strings.Join(places, "; "))
		}
	}
	for _, position := range nameBoundHeaders(fset, files) {
		members = append(members, "header resolved by procedure name through the registry at "+position)
	}
	sort.Strings(members)
	return members
}

func isIdent(expr ast.Expr, name string) bool {
	ident, ok := expr.(*ast.Ident)
	return ok && ident.Name == name
}

func namesColumns(expr ast.Expr) bool {
	switch e := expr.(type) {
	case *ast.SelectorExpr:
		return e.Sel.Name == "Columns"
	case *ast.Ident:
		return strings.Contains(strings.ToLower(e.Name), "column")
	}
	return false
}

// parseModuleSources parses every non-test Go file of the module except the
// generated ANTLR parser, keyed by module-relative path.
func parseModuleSources(t *testing.T, fset *token.FileSet) map[string]*ast.File {
	root := filepath.Join("..", "..")
	files := map[string]*ast.File{}
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if entry.IsDir() {
			if rel != "." && (strings.HasPrefix(entry.Name(), ".") || entry.Name() == "vendor" || entry.Name() == "testdata" || entry.Name() == "node_modules") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(rel, ".go") || strings.HasSuffix(rel, "_test.go") || strings.HasPrefix(rel, "pkg/cypher/antlr/cypher_") {
			return nil
		}
		src, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		file, err := parser.ParseFile(fset, rel, src, 0)
		if err != nil {
			return err
		}
		files[rel] = file
		return nil
	})
	require.NoError(t, err)
	return files
}

// TestProcedureResultHeadersHaveOneDeclaration proves that every result
// header of two or more columns is declared once, that no literal copies a
// built-in procedure's header, and that no handler binds its header by
// procedure name. Each planted second declaration, in a new file, must be
// caught.
func TestProcedureResultHeadersHaveOneDeclaration(t *testing.T) {
	declared := declaredProcedureHeaders()
	require.Contains(t, declared, "section,status,data")
	require.Contains(t, declared, "txId,db,kind,seq,timestamp,payload")
	fset := token.NewFileSet()
	files := parseModuleSources(t, fset)
	require.Empty(t, resultHeaderCensus(t, fset, files, declared))

	plants := map[string]struct {
		source  string
		members int
	}{
		// A factory writing a registered header beside its declaration.
		"pkg/cypher/planted_status_columns.go": {`package cypher
func plantedStatusColumns() []string { return []string{"section", "status", "data"} }`, 1},
		// A sibling registration spelling a shared header in its own signature.
		"pkg/cypher/planted_sibling.go": {`package cypher
var plantedSibling = ProcedureSpec{Name: "planted.entries", Signature: "planted.entries() :: (txId :: STRING, db :: STRING, kind :: STRING, seq :: INTEGER, timestamp :: STRING, payload :: STRING)"}`, 1},
		// A handler header resolved by procedure name through the registry.
		"pkg/cypher/planted_alias_header.go": {`package cypher
func plantedAliasHeader() *ExecuteResult {
	return &ExecuteResult{Columns: procedureResultColumns("kp.info")}
}`, 1},
		// A renamed copy of a header in another package.
		"pkg/fabric/planted_copy.go": {`package fabric
func plantedJoinProjection() *ResultStream {
	return &ResultStream{Columns: []string{"textKey", "textKey128", "texts"}}
}`, 1},
	}
	for name, plant := range plants {
		withPlant := make(map[string]*ast.File, len(files)+1)
		for key, file := range files {
			withPlant[key] = file
		}
		planted, err := parser.ParseFile(fset, name, plant.source, 0)
		require.NoError(t, err)
		withPlant[name] = planted
		members := resultHeaderCensus(t, fset, withPlant, declared)
		require.Len(t, members, plant.members, name)
		for _, member := range members {
			require.Contains(t, member, name)
		}
	}
}
