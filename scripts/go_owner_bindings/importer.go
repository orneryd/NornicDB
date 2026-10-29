package main

import (
	"errors"
	"go/ast"
	"go/build"
	"go/importer"
	"go/parser"
	"go/types"
	"os"
	"path/filepath"
	"strings"
)

// sharedImporter type-checks standard-library and same-module packages from
// source, sharing loaded packages across every inspected package. Unresolved
// third-party modules return an error, which go/types reports as an ordinary
// type error instead of a fatal condition.
type sharedImporter struct {
	a       *analyzer
	src     types.Importer
	modRoot string
	modPath string
	cache   map[string]*types.Package
}

// Import implements types.Importer.
func (s *sharedImporter) Import(importPath string) (*types.Package, error) {
	if s.cache == nil {
		s.cache = map[string]*types.Package{}
		s.src = importer.ForCompiler(s.a.fset, "source", nil)
		s.modRoot, s.modPath = findModule(s.a.root)
	}
	if pkg, ok := s.cache[importPath]; ok {
		return pkg, nil
	}
	if s.inModule(importPath) {
		if pkg, err := s.importModulePackage(importPath); err == nil {
			s.cache[importPath] = pkg
			return pkg, nil
		}
	}
	pkg, err := s.src.Import(importPath)
	if err != nil {
		return nil, err
	}
	s.cache[importPath] = pkg
	return pkg, nil
}

// inModule reports whether importPath belongs to the module under analysis.
func (s *sharedImporter) inModule(importPath string) bool {
	if s.modRoot == "" || s.modPath == "" {
		return false
	}
	return importPath == s.modPath || strings.HasPrefix(importPath, s.modPath+"/")
}

// localDir reports whether a path resolves to a directory inside the module
// root, so a shadowing name is never mistaken for one of the module packages.
func (s *sharedImporter) localDir(dir string) bool {
	abs, err := filepath.Abs(dir)
	if err != nil || s.modRoot == "" {
		return false
	}
	rel, err := filepath.Rel(s.modRoot, abs)
	if err != nil {
		return false
	}
	return withinRoot(rel)
}

// importModulePackage type-checks a package inside the module under analysis.
// The caller supplies the immutable staged source tree; no Git state is read.
func (s *sharedImporter) importModulePackage(importPath string) (*types.Package, error) {
	rel, err := filepath.Rel(s.modPath, importPath)
	if err != nil {
		return nil, err
	}
	dir := filepath.Join(s.modRoot, filepath.FromSlash(rel))
	if !s.localDir(dir) {
		return nil, errors.New("package " + importPath + " is not inside the module root")
	}
	pkg, err := s.a.ctx.ImportDir(dir, 0)
	if err != nil && pkg == nil {
		return nil, err
	}
	fset := s.a.fset
	var files []*ast.File
	names := append(append([]string{}, pkg.GoFiles...), pkg.CgoFiles...)
	for _, name := range names {
		full := filepath.Join(dir, name)
		src, rerr := os.ReadFile(full)
		if rerr != nil {
			return nil, rerr
		}
		af, perr := parser.ParseFile(fset, full, src, parser.SkipObjectResolution)
		if perr != nil {
			return nil, perr
		}
		files = append(files, af)
	}
	if len(files) == 0 {
		return nil, &build.NoGoError{Dir: dir}
	}
	conf := types.Config{
		Importer:  s,
		GoVersion: supportedGoVersion,
		// Package-level errors are collected here; the packages under
		// analysis report their own diagnostics.
		Error: func(error) {},
	}
	checked, _ := conf.Check(importPath, fset, files, nil)
	if checked == nil {
		return nil, errors.New("package " + importPath + " could not be checked")
	}
	return checked, nil
}
