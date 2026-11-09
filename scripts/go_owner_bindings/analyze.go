package main

import (
	"errors"
	"go/ast"
	"go/build"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
)

// analyzer holds shared state for one request.
type analyzer struct {
	root string
	fset *token.FileSet
	// imp is the single shared importer for every package in the request.
	imp types.Importer
	ctx build.Context
	res *output
}

// sourceFile is one parsed candidate file.
type sourceFile struct {
	rel  string // path relative to root, slash separated
	full string
	file *ast.File
	tok  *token.File
	test bool
}

// analyze performs the whole request.
func analyze(req request) *output {
	root := req.Root
	if root == "" {
		root = "."
	}
	abs, err := filepath.Abs(root)
	if err != nil {
		return &output{Root: root, OK: false, Packages: []*packageInfo{}, Errors: []diagnostic{{
			Kind: "bad_root", Message: err.Error(),
		}}}
	}
	a := &analyzer{
		root: abs,
		fset: token.NewFileSet(),
		res:  &output{Root: abs, Packages: []*packageInfo{}, Errors: []diagnostic{}},
		ctx:  build.Default,
	}
	// One shared source importer for the standard library and for packages of
	// the module under analysis, so unresolved third-party modules never
	// prevent local object binding.
	a.imp = &sharedImporter{a: a}

	dirs, diags := a.collectDirs(req.Files)
	a.res.Errors = append(a.res.Errors, diags...)
	for _, dir := range dirs {
		a.res.Packages = append(a.res.Packages, a.analyzeDir(dir)...)
	}

	sort.SliceStable(a.res.Packages, func(i, j int) bool {
		pi, pj := a.res.Packages[i], a.res.Packages[j]
		if pi.Dir != pj.Dir {
			return pi.Dir < pj.Dir
		}
		return pi.Name < pj.Name
	})
	sort.SliceStable(a.res.Errors, func(i, j int) bool {
		return lessDiagnostic(a.res.Errors[i], a.res.Errors[j])
	})
	ok := len(a.res.Errors) == 0
	for _, p := range a.res.Packages {
		if p.TypeCheckFailed {
			ok = false
		}
	}
	a.res.OK = ok
	return a.res
}

// lessDiagnostic orders diagnostics deterministically.
func lessDiagnostic(ei, ej diagnostic) bool {
	switch {
	case ei.Path != ej.Path:
		return ei.Path < ej.Path
	case ei.Line != ej.Line:
		return ei.Line < ej.Line
	case ei.Column != ej.Column:
		return ei.Column < ej.Column
	case ei.Kind != ej.Kind:
		return ei.Kind < ej.Kind
	}
	return ei.Message < ej.Message
}

// collectDirs resolves request files to unique package directories.
func (a *analyzer) collectDirs(files []string) ([]string, []diagnostic) {
	diags := []diagnostic{}
	seen := map[string]bool{}
	var dirs []string
	for _, f := range files {
		if strings.TrimSpace(f) == "" {
			continue
		}
		p := f
		if !filepath.IsAbs(p) {
			p = filepath.Join(a.root, f)
		}
		st, err := os.Stat(p)
		if err != nil {
			diags = append(diags, diagnostic{Path: a.rel(p), Kind: "missing_file", Message: err.Error()})
			continue
		}
		dir := p
		if !st.IsDir() {
			dir = filepath.Dir(p)
		}
		dir = filepath.Clean(dir)
		if seen[dir] {
			continue
		}
		seen[dir] = true
		dirs = append(dirs, dir)
	}
	sort.Strings(dirs)
	return dirs, diags
}

// rel converts an absolute path to a slash-separated path relative to root.
func (a *analyzer) rel(p string) string {
	if p == "" {
		return ""
	}
	r, err := filepath.Rel(a.root, p)
	if err != nil {
		return filepath.ToSlash(p)
	}
	return filepath.ToSlash(r)
}

// analyzeDir inspects one directory and returns its packages. go/build decides
// which files belong to the build, so build constraints and _test.go handling
// follow the toolchain's own rules.
func (a *analyzer) analyzeDir(dir string) []*packageInfo {
	pkg, err := a.ctx.ImportDir(dir, 0)
	if err != nil && pkg == nil {
		a.res.Errors = append(a.res.Errors, a.buildDiagnostics(dir, err)...)
		return nil
	}
	a.res.Errors = append(a.res.Errors, a.buildDiagnostics(dir, err)...)

	names := append(append([]string{}, pkg.GoFiles...), pkg.CgoFiles...)
	names = append(names, pkg.TestGoFiles...)
	names = append(names, pkg.XTestGoFiles...)
	sort.Strings(names)

	var files []sourceFile
	for _, name := range names {
		full := filepath.Join(dir, name)
		isTest := strings.HasSuffix(name, "_test.go")
		src, rerr := os.ReadFile(full)
		if rerr != nil {
			a.res.Errors = append(a.res.Errors, diagnostic{
				Path: a.rel(full), Kind: "read_file", Message: rerr.Error(),
			})
			continue
		}
		af, perr := parser.ParseFile(a.fset, full, src, parser.SkipObjectResolution)
		if perr != nil {
			a.res.Errors = append(a.res.Errors, a.parseDiagnostics(a.rel(full), perr)...)
			continue
		}
		files = append(files, sourceFile{
			rel:  a.rel(full),
			full: full,
			file: af,
			tok:  a.fset.File(af.Pos()),
			test: isTest,
		})
	}

	basePkg := pkg.Name
	groups := map[string][]sourceFile{}
	for _, f := range files {
		pkgName := f.file.Name.Name
		if f.test && basePkg != "" && pkgName != basePkg {
			// External _test package: proven separately from the package it
			// tests, so its own declarations and bindings are reported.
			groups[pkgName] = append(groups[pkgName], f)
			continue
		}
		// Non-test files and same-package test files share one group.
		groups[pkgName] = append(groups[pkgName], f)
	}

	importPath := a.importPathFor(dir)
	pkgNames := make([]string, 0, len(groups))
	for n := range groups {
		pkgNames = append(pkgNames, n)
	}
	sort.Strings(pkgNames)
	out := []*packageInfo{}
	for _, n := range pkgNames {
		out = append(out, a.checkPackage(dir, importPath, n, groups[n]))
	}
	return out
}

// buildDiagnostics converts build errors into structured diagnostics.
func (a *analyzer) buildDiagnostics(dir string, err error) []diagnostic {
	if err == nil {
		return []diagnostic{}
	}
	var badGo *build.MultiplePackageError
	if errors.As(err, &badGo) {
		return []diagnostic{{
			Path:    a.rel(dir),
			Kind:    "build",
			Message: "found packages " + strings.Join(badGo.Packages, ", ") + " in " + a.rel(dir),
		}}
	}
	return []diagnostic{{Path: a.rel(dir), Kind: "build", Message: err.Error()}}
}

// parseDiagnostics renders parser errors as structured diagnostics.
func (a *analyzer) parseDiagnostics(fallback string, err error) []diagnostic {
	var list scannerErrors
	if errors.As(err, &list) {
		out := make([]diagnostic, 0, len(list))
		for _, e := range list {
			out = append(out, diagnostic{
				Path:    a.rel(e.Pos.Filename),
				Line:    e.Pos.Line,
				Column:  e.Pos.Column,
				Kind:    "parse",
				Message: e.Msg,
			})
		}
		return out
	}
	return []diagnostic{{Path: fallback, Kind: "parse", Message: err.Error()}}
}

// importPathFor derives the module import path for dir from go.mod.
func (a *analyzer) importPathFor(dir string) string {
	modRoot, modPath := findModule(a.root)
	if modRoot != "" && modPath != "" {
		if r, err := filepath.Rel(modRoot, dir); err == nil && withinRoot(r) {
			if r == "." {
				return modPath
			}
			return path.Join(modPath, filepath.ToSlash(r))
		}
	}
	return a.rel(dir)
}

// withinRoot reports whether a relative path stays inside its base.
func withinRoot(rel string) bool {
	return rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

// findModule walks up from start looking for a go.mod file.
func findModule(start string) (root, modulePath string) {
	dir := start
	for {
		data, err := os.ReadFile(filepath.Join(dir, "go.mod"))
		if err == nil {
			for _, line := range strings.Split(string(data), "\n") {
				line = strings.TrimSpace(line)
				if strings.HasPrefix(line, "module ") {
					return dir, strings.TrimSpace(strings.TrimPrefix(line, "module"))
				}
			}
			return dir, ""
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", ""
		}
		dir = parent
	}
}
