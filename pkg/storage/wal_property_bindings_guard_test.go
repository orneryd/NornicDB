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
	"golang.org/x/tools/go/ssa"
)

// Every mutation is type-checked separately with the complete production
// package. SSA erases declaration spelling and follows actual values/aliases.
func TestBufferedWALBindingOwner(t *testing.T) {
	output, err := exec.Command("go", "list", "-deps", "-export", "-json", ".").Output()
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
	imp := importer.ForCompiler(fset, "gc", func(path string) (io.ReadCloser, error) { return os.Open(exports[path]) })
	check := func(t *testing.T, source string) map[string]bool {
		population := append([]*ast.File(nil), files...)
		if source != "" {
			mutation, err := parser.ParseFile(fset, "wal_binding_guard_mutation.go", "package storage\n"+source, parser.SkipObjectResolution)
			require.NoError(t, err)
			population = append(population, mutation)
		}
		info := &types.Info{
			Types: map[ast.Expr]types.TypeAndValue{}, Defs: map[*ast.Ident]types.Object{}, Uses: map[*ast.Ident]types.Object{},
			Implicits: map[ast.Node]types.Object{}, Selections: map[*ast.SelectorExpr]*types.Selection{}, Scopes: map[ast.Node]*types.Scope{},
			Instances: map[*ast.Ident]types.Instance{}, FileVersions: map[*ast.File]string{},
		}
		conf := types.Config{Importer: imp}
		pkg, err := conf.Check(pkgPath, fset, population, info)
		require.NoError(t, err, "production and this mutation must independently type-check")
		prog := ssa.NewProgram(fset, ssa.SanityCheckFunctions)
		seen := map[*types.Package]bool{}
		var imports func(*types.Package)
		imports = func(p *types.Package) {
			if seen[p] {
				return
			}
			seen[p] = true
			for _, dependency := range p.Imports() {
				imports(dependency)
			}
			if p != pkg {
				prog.CreatePackage(p, nil, nil, true)
			}
		}
		imports(pkg)
		ssaPkg := prog.CreatePackage(pkg, population, info, true)
		ssaPkg.Build()
		var functions []*ssa.Function
		owners := map[*ssa.Function]bool{}
		method := func(receiver, name string) *types.Func {
			selection := types.NewMethodSet(types.NewPointer(pkg.Scope().Lookup(receiver).Type())).Lookup(pkg, name)
			require.NotNil(t, selection)
			return selection.Obj().(*types.Func)
		}
		writerObject := method("WAL", "AppendBufferedCommit")
		ownerObjects := map[*types.Func]bool{
			method("BadgerTransaction", "commitBufferedLocked"): true,
			method("BadgerEngine", "importBufferedWALRecord"):   true,
			method("BadgerEngine", "rebindWALNamedProperties"):  true,
			writerObject: true,
			pkg.Scope().Lookup("rewriteWALPropertyBodies").(*types.Func):   true,
			pkg.Scope().Lookup("rewriteWALPropertyPayloads").(*types.Func): true,
		}
		var add func(*ssa.Function, bool)
		add = func(fn *ssa.Function, owner bool) {
			if fn == nil {
				return
			}
			functions = append(functions, fn)
			if owner {
				owners[fn] = true
			}
			for _, nested := range fn.AnonFuncs {
				add(nested, owner)
			}
		}
		for _, file := range population {
			for _, declaration := range file.Decls {
				if fn, ok := declaration.(*ast.FuncDecl); ok {
					owner := ownerObjects[info.Defs[fn.Name].(*types.Func)]
					add(prog.FuncValue(info.Defs[fn.Name].(*types.Func)), owner)
				}
			}
		}
		add(ssaPkg.Func("init"), false)
		return bufferedBodySSAProducers(functions, pkg.Scope().Lookup("bufferedCommit").Type(), []types.Type{pkg.Scope().Lookup("WALBufferedCommitData").Type(), pkg.Scope().Lookup("walBufferedCommitPayload").Type()}, pkg.Scope().Lookup("OpBufferedCommit").(*types.Const), prog.FuncValue(writerObject), owners)
	}
	t.Run("production", func(t *testing.T) { require.Empty(t, check(t, "")) })
	mutations := map[string]string{
		"empty":                `func bypass(d *WALBufferedCommitData) bufferedCommit { c := bufferedCommit{}; c.writes = d.Writes; return c }`,
		"zero":                 `type guardAlias = bufferedCommit; func bypass(d *WALBufferedCommitData) bufferedCommit { var c guardAlias; c.writes = d.Writes; return c }`,
		"pointer":              `func bypass(c *bufferedCommit, d *WALBufferedCommitData) { c.writes = d.Writes }`,
		"map_alias":            `func bypass(c *bufferedCommit, d *WALBufferedCommitData) { writes := c.writes; for k,v := range d.Writes { writes[k] = v } }`,
		"var_alias":            `func bypass(c *bufferedCommit, d *WALBufferedCommitData) { var writes = c.writes; for k,v := range d.Writes { writes[k] = v } }`,
		"chained_alias":        `func bypass(c *bufferedCommit, d *WALBufferedCommitData) { first := c.writes; second := first; for k,v := range d.Writes { second[k] = v } }`,
		"phi_alias":            `func bypass(c *bufferedCommit, d *WALBufferedCommitData, choose bool) { w := map[string][]byte{}; if choose { w = c.writes }; for k,v := range d.Writes { w[k] = v } }`,
		"helper_alias":         `func mutate(w map[string][]byte) { w["x"] = nil }; func bypass(c *bufferedCommit) { mutate(c.writes) }`,
		"closure_alias":        `func bypass(c *bufferedCommit) { w := c.writes; func(){ w["x"] = nil }() }`,
		"transport_field":      `func bypass(d *WALBufferedCommitData, writes map[string][]byte) { d.Writes = writes }`,
		"wire_var_alias":       `func bypass(d *walBufferedCommitPayload) { var writes = d.Writes; writes["x"] = nil }`,
		"transport_var_alias":  `func bypass(d *WALBufferedCommitData) { var writes = d.Writes; writes["x"] = nil }`,
		"raw_transport_apply":  `import "github.com/dgraph-io/badger/v4"; func bypass(b *BadgerEngine, d *WALBufferedCommitData) error { return b.db.Update(func(tx *badger.Txn) error { return tx.Set([]byte("x"), d.Writes["x"]) }) }`,
		"alternate_wal_writer": `func bypass(w *WAL, d *WALBufferedCommitData) error { return w.Append(OpBufferedCommit, d) }`,
		"aggregate_escape":     `type holder struct { m map[string][]byte }; func bypass(c *bufferedCommit) holder { return holder{m:c.writes} }`,
		"range_body":           `func bypass(c *bufferedCommit) { for _, body := range c.writes { body[0] = 1 } }`,
		"returned_alias":       `func alias(w map[string][]byte) map[string][]byte { return w }; func bypass(c *bufferedCommit) { alias(c.writes)["x"] = nil }`,
		"opaque_alias":         `func bypass(c *bufferedCommit, f func(map[string][]byte)) { f(c.writes) }`,
		"body_alias":           `func bypass(c *bufferedCommit) { body := c.writes["x"]; body[0] = 1 }`,
		"new":                  `type guardAlias = bufferedCommit; func bypass() *bufferedCommit { return new(guardAlias) }`,
		"promoted":             `type guardEmbedded struct { bufferedCommit }; func bypass(c *guardEmbedded, d *WALBufferedCommitData) { c.writes = d.Writes }`,
		"conversion":           `type guardCopy bufferedCommit; func bypass(c guardCopy) bufferedCommit { return bufferedCommit(c) }`,
		"clear":                `func bypass(c *bufferedCommit) { clear(c.writes) }`,
	}
	for name, source := range mutations {
		t.Run(name, func(t *testing.T) {
			require.NotEmpty(t, check(t, source), "SSA must reject the physical producer/mutation")
		})
	}
	for name, source := range map[string]string{
		"captured_reader": `func allowed(c *bufferedCommit) int { w := c.writes; return func() int { return len(w) }() }`,
		"delegate":        `func delegate(b *BadgerEngine, d *WALBufferedCommitData) (bufferedCommit,error) { return b.importBufferedWALRecord(d, walForeignImport) }`,
		"unrelated":       `type unrelated struct { writes map[string][]byte }; func allowed(c *unrelated) { c.writes = nil }`,
	} {
		t.Run(name, func(t *testing.T) {
			require.Empty(t, check(t, source), "the owner call and unrelated field remain allowed")
		})
	}
}
