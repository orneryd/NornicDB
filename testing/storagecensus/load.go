// Package storagecensus builds the census of every path that creates,
// replaces, hides, deletes or counts stored entities, or reads integrity and
// quota totals, in pkg/storage, pkg/multidb and pkg/cypher, and the owner each
// path goes through. The census is generated from the type-checked source
// (docs/generated/stored-entity-census.json and .md), and its tests fail when
// the generated artifact is stale or when an effect bypasses its owner.
package storagecensus

import (
	"fmt"
	"go/ast"
	"go/build/constraint"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

// Module is the import path prefix of the analysed module.
const Module = "github.com/orneryd/nornicdb/"

// Packages are the analysed packages, in dependency order: each imports only
// those before it among them.
var Packages = []string{"pkg/storage", "pkg/multidb", "pkg/cypher"}

// loaded is one type-checked package.
type loaded struct {
	rel   string // "pkg/storage"
	short string // "storage"
	pkg   *types.Package
	files []*ast.File
	info  *types.Info
}

// program is the analysed packages over one file set.
type program struct {
	fset *token.FileSet
	pkgs []*loaded
}

// sharedImporter resolves the analysed packages to the ones already checked,
// so a call from one into another names the same objects; third-party and
// standard-library packages are checked from source; other packages of the
// module are empty (a call into them is not a path this census follows).
type sharedImporter struct {
	own    map[string]*types.Package
	source types.Importer
	empty  map[string]*types.Package
}

func (s *sharedImporter) Import(path string) (*types.Package, error) {
	if p := s.own[path]; p != nil {
		return p, nil
	}
	if strings.HasPrefix(path, Module) {
		if p := s.empty[path]; p != nil {
			return p, nil
		}
		p := types.NewPackage(path, filepath.Base(path))
		p.MarkComplete()
		s.empty[path] = p
		return p, nil
	}
	return s.source.Import(path)
}

var (
	sourceOnce sync.Once
	sourceImp  types.Importer
)

// sourceImporter checks third-party and standard-library packages from
// source, once per process: every census load (the mutation tests make
// several) shares them.
func sourceImporter() types.Importer {
	sourceOnce.Do(func() { sourceImp = importer.ForCompiler(token.NewFileSet(), "source", nil) })
	return sourceImp
}

// load parses and type-checks the analysed packages under root, the module
// root. Test files are excluded, and files the linux/amd64 build excludes.
func load(root string) (*program, error) {
	fset := token.NewFileSet()
	imp := &sharedImporter{own: map[string]*types.Package{}, source: sourceImporter(), empty: map[string]*types.Package{}}
	prog := &program{fset: fset}
	for _, rel := range Packages {
		paths, err := filepath.Glob(filepath.Join(root, filepath.FromSlash(rel), "*.go"))
		if err != nil {
			return nil, err
		}
		sort.Strings(paths)
		var files []*ast.File
		for _, path := range paths {
			if strings.HasSuffix(path, "_test.go") {
				continue
			}
			src, err := os.ReadFile(path)
			if err != nil {
				return nil, err
			}
			// The position names the file relative to the root, so nothing
			// in the census depends on where the tree is checked out.
			name := filepath.ToSlash(filepath.Join(rel, filepath.Base(path)))
			file, err := parser.ParseFile(fset, name, src, parser.ParseComments)
			if err != nil {
				return nil, err
			}
			if buildsOnLinux(file, name) {
				files = append(files, file)
			}
		}
		info := &types.Info{Uses: map[*ast.Ident]types.Object{}, Defs: map[*ast.Ident]types.Object{},
			Selections: map[*ast.SelectorExpr]*types.Selection{}, Types: map[ast.Expr]types.TypeAndValue{}}
		conf := types.Config{Importer: imp, Error: func(error) {}, FakeImportC: true}
		pkg, _ := conf.Check(Module+rel, fset, files, info)
		if pkg == nil {
			return nil, fmt.Errorf("type-checking %s produced no package", rel)
		}
		imp.own[Module+rel] = pkg
		prog.pkgs = append(prog.pkgs, &loaded{rel: rel, short: filepath.Base(rel), pkg: pkg, files: files, info: info})
	}
	return prog, nil
}

// buildsOnLinux reports whether a linux/amd64 build with the default tags
// (noui, nolocalllm) includes file.
func buildsOnLinux(file *ast.File, name string) bool {
	for _, group := range file.Comments {
		if group.Pos() > file.Package {
			break
		}
		for _, c := range group.List {
			if !constraint.IsGoBuild(c.Text) {
				continue
			}
			expr, err := constraint.Parse(c.Text)
			if err != nil {
				continue
			}
			return expr.Eval(func(tag string) bool {
				switch tag {
				case "linux", "amd64", "noui", "nolocalllm", "cgo", "unix":
					return true
				}
				return strings.HasPrefix(tag, "go1.")
			})
		}
	}
	base := strings.TrimSuffix(filepath.Base(name), ".go")
	for _, other := range []string{"_windows", "_darwin", "_arm64", "_wasm", "_js", "_freebsd", "_openbsd", "_netbsd", "_plan9", "_solaris"} {
		if strings.HasSuffix(base, other) || strings.Contains(base, other+"_") {
			return false
		}
	}
	return true
}
