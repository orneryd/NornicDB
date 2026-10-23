package storage

import (
	"encoding/json"
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

	"github.com/stretchr/testify/require"
)

// Resolve every production type using real build exports; no ignored import or
// type errors may reduce the population. Mutations are checked in that package.
func TestBufferedWALBindingOwner(t *testing.T) {
	cmd := exec.Command("go", "list", "-deps", "-export", "-json", ".")
	output, err := cmd.Output()
	require.NoError(t, err)
	exports := make(map[string]string)
	var dir, pkgPath string
	var names []string
	decoder := json.NewDecoder(strings.NewReader(string(output)))
	for {
		var p struct {
			ImportPath, Dir, Export string
			GoFiles                 []string
		}
		err := decoder.Decode(&p)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		exports[p.ImportPath] = p.Export
		if p.ImportPath == "github.com/orneryd/nornicdb/pkg/storage" {
			dir, pkgPath, names = p.Dir, p.ImportPath, p.GoFiles
		}
	}
	require.NotEmpty(t, names)
	fset := token.NewFileSet()
	var files []*ast.File
	for _, name := range names {
		file, err := parser.ParseFile(fset, filepath.Join(dir, name), nil, parser.SkipObjectResolution)
		require.NoError(t, err)
		files = append(files, file)
	}
	mutation, err := parser.ParseFile(fset, "wal_binding_guard_mutation.go", `package storage
 type bindingGuardAlias = bufferedCommit
 func bindingGuardEmpty(d *WALBufferedCommitData) bufferedCommit {
  c := bufferedCommit{}; c.writes = d.Writes; c.ops = d.Ops; c.recovered = true; return c
 }
 func bindingGuardZero(d *WALBufferedCommitData) bufferedCommit {
  var c bindingGuardAlias; c.writes = d.Writes; return c
 }
 func bindingGuardPointer(c *bindingGuardAlias, d *WALBufferedCommitData) { c.writes = d.Writes }
 func bindingGuardMapAlias(c *bufferedCommit, d *WALBufferedCommitData) {
  writes := c.writes; for key, value := range d.Writes { writes[key] = value }
 }
 func bindingGuardNew() *bufferedCommit { return new(bindingGuardAlias) }
 type bindingGuardEmbedded struct { bufferedCommit }
 func bindingGuardPromoted(c *bindingGuardEmbedded, d *WALBufferedCommitData) { c.writes = d.Writes }
 type bindingGuardCopy bufferedCommit
 func bindingGuardConversion(c bindingGuardCopy) bufferedCommit { return bufferedCommit(c) }
 func bindingGuardDelegate(b *BadgerEngine, d *WALBufferedCommitData) (bufferedCommit, error) { return b.importBufferedWALRecord(d) }
 type bindingGuardUnrelated struct { writes map[string][]byte }
 func bindingGuardUnrelatedWrite(c *bindingGuardUnrelated) { c.writes = nil }
 `, parser.SkipObjectResolution)
	require.NoError(t, err)
	info := &types.Info{Types: make(map[ast.Expr]types.TypeAndValue), Defs: make(map[*ast.Ident]types.Object), Uses: make(map[*ast.Ident]types.Object), Selections: make(map[*ast.SelectorExpr]*types.Selection)}
	conf := types.Config{Importer: importer.ForCompiler(fset, "gc", func(path string) (io.ReadCloser, error) { return os.Open(exports[path]) })}
	pkg, err := conf.Check(pkgPath, fset, append(files, mutation), info)
	require.NoError(t, err, "full production package and mutations must type-check")
	commit := pkg.Scope().Lookup("bufferedCommit").Type()
	owners := map[types.Object]bool{}
	for _, file := range files {
		for _, declaration := range file.Decls {
			if fn, ok := declaration.(*ast.FuncDecl); ok {
				path := filepath.Base(fset.Position(fn.Pos()).Filename)
				if path == "badger_write_behind.go" && fn.Name.Name == "commitBufferedLocked" || path == "wal_property_bindings.go" && fn.Name.Name == "importBufferedWALRecord" {
					owners[info.Defs[fn.Name]] = true
				}
			}
		}
	}
	require.Len(t, owners, 2)
	for _, file := range files {
		require.Empty(t, bufferedBodyProducers(file, info, commit, owners), "physical-body producer bypasses owner: %s", fset.Position(file.Pos()))
	}
	got := bufferedBodyProducers(mutation, info, commit, owners)
	for _, name := range []string{"bindingGuardEmpty", "bindingGuardZero", "bindingGuardPointer", "bindingGuardMapAlias", "bindingGuardNew", "bindingGuardPromoted", "bindingGuardConversion"} {
		require.True(t, got[name], "guard must reject mutation %s", name)
	}
	require.False(t, got["bindingGuardDelegate"], "thin owner calls are allowed")
	require.False(t, got["bindingGuardUnrelatedWrite"], "field spelling is not type identity")
}

func bufferedBodyProducers(file *ast.File, info *types.Info, commit types.Type, owners map[types.Object]bool) map[string]bool {
	out := make(map[string]bool)
	var writesField *types.Var
	fields := commit.Underlying().(*types.Struct)
	for i := 0; i < fields.NumFields(); i++ {
		if field := fields.Field(i); field.Name() == "writes" {
			writesField = field
		}
	}
	isCommit := func(typ types.Type) bool {
		if pointer, ok := typ.(*types.Pointer); ok {
			typ = pointer.Elem()
		}
		return typ != nil && types.Identical(typ, commit)
	}
	physicalField := func(expr ast.Expr) bool {
		selection, ok := expr.(*ast.SelectorExpr)
		if !ok {
			return false
		}
		field := info.Selections[selection]
		return field != nil && field.Obj() == writesField
	}
	for _, declaration := range file.Decls {
		name := "package initialization"
		if fn, ok := declaration.(*ast.FuncDecl); ok {
			if owners[info.Defs[fn.Name]] {
				continue
			}
			name = fn.Name.Name
		}
		aliases := make(map[types.Object]bool)
		ast.Inspect(declaration, func(node ast.Node) bool {
			switch n := node.(type) {
			case *ast.CompositeLit:
				if isCommit(info.TypeOf(n)) {
					out[name] = true
				}
			case *ast.ValueSpec:
				if n.Type != nil && isCommit(info.TypeOf(n.Type)) {
					out[name] = true
				}
			case *ast.CallExpr:
				if info.Types[n.Fun].IsType() && isCommit(info.TypeOf(n)) {
					out[name] = true
				}
				if builtin, ok := n.Fun.(*ast.Ident); ok && info.Uses[builtin] == types.Universe.Lookup("new") && isCommit(info.TypeOf(n)) {
					out[name] = true
				}
			case *ast.AssignStmt:
				for i, lhs := range n.Lhs {
					if physicalField(lhs) {
						out[name] = true
					}
					if index, ok := lhs.(*ast.IndexExpr); ok {
						if physicalField(index.X) {
							out[name] = true
						}
						if id, ok := index.X.(*ast.Ident); ok && aliases[info.Uses[id]] {
							out[name] = true
						}
					}
					if i < len(n.Rhs) && physicalField(n.Rhs[i]) {
						if id, ok := lhs.(*ast.Ident); ok {
							object := info.Defs[id]
							if object == nil {
								object = info.Uses[id]
							}
							aliases[object] = true
						}
					}
				}
			case *ast.UnaryExpr:
				if n.Op == token.AND && physicalField(n.X) {
					out[name] = true
				}
			}
			return true
		})
	}
	return out
}
