package storage

// Shared compiled-population loader author: Claude (storage-format-owners lane), 30 October 2026.
// Extracted unchanged from TestBufferedWALBindingOwner so census guards share one loader.

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

// storageSSAPopulation is the parsed production storage package and the
// compiler export data of its dependencies, loaded once per guard.
type storageSSAPopulation struct {
	fset    *token.FileSet
	files   []*ast.File
	imp     types.Importer
	pkgPath string
}

func loadStorageSSAPopulation(t *testing.T) *storageSSAPopulation {
	t.Helper()
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
	return &storageSSAPopulation{fset: fset, files: files, imp: imp, pkgPath: pkgPath}
}

// storageSSABuild is one type-checked and SSA-built storage package population.
type storageSSABuild struct {
	pkg   *types.Package
	info  *types.Info
	prog  *ssa.Program
	ssa   *ssa.Package
	files []*ast.File
}

// build type-checks the production files plus an optional mutation file named
// name (source without its package clause) and builds the package's SSA.
func (s *storageSSAPopulation) build(t *testing.T, name, source string) storageSSABuild {
	t.Helper()
	population := append([]*ast.File(nil), s.files...)
	if source != "" {
		mutation, err := parser.ParseFile(s.fset, name, "package storage\n"+source, parser.SkipObjectResolution)
		require.NoError(t, err)
		population = append(population, mutation)
	}
	info := &types.Info{
		Types: map[ast.Expr]types.TypeAndValue{}, Defs: map[*ast.Ident]types.Object{}, Uses: map[*ast.Ident]types.Object{},
		Implicits: map[ast.Node]types.Object{}, Selections: map[*ast.SelectorExpr]*types.Selection{}, Scopes: map[ast.Node]*types.Scope{},
		Instances: map[*ast.Ident]types.Instance{}, FileVersions: map[*ast.File]string{},
	}
	conf := types.Config{Importer: s.imp}
	pkg, err := conf.Check(s.pkgPath, s.fset, population, info)
	require.NoError(t, err, "production and this mutation must independently type-check")
	prog := ssa.NewProgram(s.fset, ssa.SanityCheckFunctions)
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
	return storageSSABuild{pkg: pkg, info: info, prog: prog, ssa: ssaPkg, files: population}
}
