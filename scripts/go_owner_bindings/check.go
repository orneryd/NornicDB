package main

import (
	"errors"
	"fmt"
	"go/ast"
	"go/token"
	"go/types"
	"path/filepath"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"
)

// checkPackage type-checks one package group and builds its metadata.
func (a *analyzer) checkPackage(dir, importPath, name string, group []sourceFile) *packageInfo {
	pi := &packageInfo{
		Name:        name,
		Dir:         a.rel(dir),
		ImportPath:  importPath,
		IsTest:      strings.HasSuffix(name, "_test"),
		Files:       []fileInfo{},
		Diagnostics: []diagnostic{},
	}

	astFiles := make([]*ast.File, 0, len(group))
	for _, f := range group {
		astFiles = append(astFiles, f.file)
	}
	info := &types.Info{
		Types:      map[ast.Expr]types.TypeAndValue{},
		Defs:       map[*ast.Ident]types.Object{},
		Uses:       map[*ast.Ident]types.Object{},
		Instances:  map[*ast.Ident]types.Instance{},
		Selections: map[*ast.SelectorExpr]*types.Selection{},
		Scopes:     map[ast.Node]*types.Scope{},
		Implicits:  map[ast.Node]types.Object{},
	}

	diags := []diagnostic{}
	conf := types.Config{
		Importer:  a.imp,
		GoVersion: supportedGoVersion,
		Error: func(err error) {
			var terr types.Error
			if !errors.As(err, &terr) {
				diags = append(diags, diagnostic{Kind: "typecheck", Message: err.Error()})
				return
			}
			pos := a.fset.Position(terr.Pos)
			diags = append(diags, diagnostic{
				Path:    a.rel(pos.Filename),
				Line:    pos.Line,
				Column:  pos.Column,
				Kind:    "typecheck",
				Message: terr.Msg,
			})
		},
	}
	pkgPath := importPath
	if pi.IsTest {
		pkgPath = importPath + "_test"
	}
	// Type errors are collected, never fatal: valid local object identities
	// survive unrelated unresolved module imports.
	pkg, _ := conf.Check(pkgPath, a.fset, astFiles, info)

	seenDecl := map[string]string{}
	for _, f := range group {
		fi := fileInfo{
			Path:     f.rel,
			Package:  f.file.Name.Name,
			TestFile: f.test,
			Funcs:    []funcInfo{},
		}
		for _, d := range f.file.Decls {
			fd, ok := d.(*ast.FuncDecl)
			if !ok {
				continue
			}
			fn := a.funcInfo(pkg, info, f, fd)
			if fd.Body == nil {
				diags = append(diags, diagnostic{
					Path:    f.rel,
					Line:    fn.Decl.Line,
					Kind:    "missing_body",
					Message: fmt.Sprintf("function %s has no body", fn.Name),
				})
			}
			key := fn.Name
			if fn.IsMethod {
				key = fn.Receiver + "." + fn.Name
			}
			if prev, dup := seenDecl[key]; dup && (fn.IsMethod || fn.Name != "init") && fn.Name != "_" {
				diags = append(diags, diagnostic{
					Path:    f.rel,
					Line:    fn.Decl.Line,
					Kind:    "duplicate_declaration",
					Message: fmt.Sprintf("%s already declared in %s", key, prev),
				})
			} else {
				seenDecl[key] = f.rel
			}
			fi.Funcs = append(fi.Funcs, fn)
		}
		sort.SliceStable(fi.Funcs, func(i, j int) bool {
			if fi.Funcs[i].Decl.Start != fi.Funcs[j].Decl.Start {
				return fi.Funcs[i].Decl.Start < fi.Funcs[j].Decl.Start
			}
			return fi.Funcs[i].Name < fi.Funcs[j].Name
		})
		pi.Files = append(pi.Files, fi)
	}
	sort.SliceStable(pi.Files, func(i, j int) bool { return pi.Files[i].Path < pi.Files[j].Path })
	sort.SliceStable(diags, func(i, j int) bool { return lessDiagnostic(diags[i], diags[j]) })
	pi.Diagnostics = diags
	pi.TypeCheckFailed = hasStructuralError(diags)
	return pi
}

// hasStructuralError reports compiler diagnostics. This is not a claim that
// every local object binding failed: callers may use an actual go/types object,
// but must never replace an unresolved target with a guessed name.
func hasStructuralError(diags []diagnostic) bool { return len(diags) != 0 }

// funcInfo builds metadata for one function declaration.
func (a *analyzer) funcInfo(pkg *types.Package, info *types.Info, f sourceFile, fd *ast.FuncDecl) funcInfo {
	fn := funcInfo{
		Name:     fd.Name.Name,
		IsMethod: fd.Recv != nil,
		Params:   []varInfo{},
		Results:  []varInfo{},
		Calls:    []callInfo{},
	}
	fn.Decl = a.posInfo(f.tok, fd.Pos(), fd.End())
	if fd.Body != nil {
		b := a.posInfo(f.tok, fd.Body.Pos(), fd.Body.End())
		fn.Body = &b
	}

	if o := info.Defs[fd.Name]; o != nil {
		if fo, ok := o.(*types.Func); ok {
			if sig, ok := fo.Type().(*types.Signature); ok {
				q := relQualifier(pkg)
				fn.TypeText = types.TypeString(sig, q)
				fn.Params = paramInfos(sig, q)
				fn.TestKind, fn.TestSigOK = testSignature(fd, sig)
				fn.Results = resultInfos(sig, q)
			}
		}
	}
	if fd.Recv != nil && len(fd.Recv.List) > 0 {
		fn.Receiver = recvName(fd.Recv.List[0])
	}

	if fd.Body != nil {
		fn.Calls = a.calls(pkg, info, f, fd.Body)
		fn.Writes = a.writes(info, fd.Body)
		fn.StateWrites, fn.UnresolvedWrites = a.stateWrites(info, fd.Body)
	}
	return fn
}

// relQualifier qualifies types by package name only, so output never depends
// on importer-internal paths.
func relQualifier(from *types.Package) types.Qualifier {
	return func(other *types.Package) string {
		if from != nil && other == from {
			return ""
		}
		return other.Name()
	}
}

// recvName renders a receiver type name.
func recvName(field *ast.Field) string {
	switch t := field.Type.(type) {
	case *ast.Ident:
		return t.Name
	case *ast.StarExpr:
		if id, ok := t.X.(*ast.Ident); ok {
			return "*" + id.Name
		}
	case *ast.IndexExpr:
		if id, ok := t.X.(*ast.Ident); ok {
			return id.Name
		}
	case *ast.IndexListExpr:
		if id, ok := t.X.(*ast.Ident); ok {
			return id.Name
		}
	}
	return ""
}

// paramInfos renders the parameters of sig.
func paramInfos(sig *types.Signature, q types.Qualifier) []varInfo {
	out := []varInfo{}
	params := sig.Params()
	for i := 0; i < params.Len(); i++ {
		v := params.At(i)
		vi := varInfo{Name: v.Name(), Type: types.TypeString(v.Type(), q)}
		if sig.Variadic() && i == params.Len()-1 {
			vi.Variadic = true
			if sl, ok := v.Type().(*types.Slice); ok {
				vi.Type = "..." + types.TypeString(sl.Elem(), q)
			}
		}
		out = append(out, vi)
	}
	return out
}

// resultInfos renders the results of sig.
func resultInfos(sig *types.Signature, q types.Qualifier) []varInfo {
	out := []varInfo{}
	res := sig.Results()
	for i := 0; i < res.Len(); i++ {
		v := res.At(i)
		out = append(out, varInfo{Name: v.Name(), Type: types.TypeString(v.Type(), q)})
	}
	return out
}

// testSignature uses the real testing object and go test's entry-point rule.
func testSignature(fd *ast.FuncDecl, sig *types.Signature) (string, bool) {
	if fd.Recv != nil || fd.Body == nil || sig == nil || sig.Params().Len() != 1 || sig.Results().Len() != 0 || fd.Type.TypeParams != nil {
		return "", false
	}
	pointer, ok := types.Unalias(sig.Params().At(0).Type()).(*types.Pointer)
	if !ok {
		return "", false
	}
	named, ok := types.Unalias(pointer.Elem()).(*types.Named)
	if !ok || named.Obj().Pkg() == nil || named.Obj().Pkg().Path() != "testing" {
		return "", false
	}
	kind, prefix, expected := "test", "Test", "T"
	if strings.HasPrefix(fd.Name.Name, "Benchmark") {
		kind, prefix, expected = "benchmark", "Benchmark", "B"
	}
	if !strings.HasPrefix(fd.Name.Name, prefix) || named.Obj().Name() != expected {
		return "", false
	}
	if suffix := strings.TrimPrefix(fd.Name.Name, prefix); suffix != "" {
		r, _ := utf8.DecodeRuneInString(suffix)
		if unicode.IsLower(r) {
			return "", false
		}
	}
	return kind, true
}

// writes records variable assignments by object identity, including captured
// writes; field mutations do not replace the resource variable itself.
func (a *analyzer) writes(info *types.Info, body *ast.BlockStmt) []objID {
	result := []objID{}
	add := func(id *ast.Ident) {
		if id == nil || id.Name == "_" {
			return
		}
		obj := info.Defs[id]
		if obj == nil {
			obj = info.Uses[id]
		}
		if _, ok := obj.(*types.Var); ok {
			result = append(result, a.objectID(obj))
		}
	}
	ast.Inspect(body, func(node ast.Node) bool {
		switch n := node.(type) {
		case *ast.AssignStmt:
			for _, lhs := range n.Lhs {
				if id, ok := lhs.(*ast.Ident); ok {
					add(id)
				}
			}
		case *ast.ValueSpec:
			for _, id := range n.Names {
				add(id)
			}
		case *ast.RangeStmt:
			if id, ok := n.Key.(*ast.Ident); ok {
				add(id)
			}
			if id, ok := n.Value.(*ast.Ident); ok {
				add(id)
			}
		case *ast.IncDecStmt:
			if id, ok := n.X.(*ast.Ident); ok {
				add(id)
			}
		}
		return true
	})
	return result
}

// posInfo converts a token range to byte offsets and lines.
func (a *analyzer) posInfo(tok *token.File, start, end token.Pos) posInfo {
	sp := a.fset.Position(start)
	ep := a.fset.Position(end)
	p := posInfo{Line: sp.Line, EndLn: ep.Line}
	if tok != nil {
		p.Start = tok.Offset(start)
		p.End = tok.Offset(end)
	}
	return p
}

// objectID records a resolved object identity, or the zero value when the
// object has no usable position.
func (a *analyzer) objectID(o types.Object) objID {
	if o == nil || o.Pos() == token.NoPos {
		return objID{}
	}
	pos := a.fset.Position(o.Pos())
	if !pos.IsValid() {
		return objID{}
	}
	return objID{File: a.rel(pos.Filename), Pos: pos.Offset, Type: types.TypeString(o.Type(), relQualifier(nil))}
}

// receiverName returns the receiver declared by a method itself, never an
// embedded field it was promoted through.
func receiverName(fn *types.Func) (string, bool) {
	sig, ok := fn.Type().(*types.Signature)
	if !ok || sig.Recv() == nil {
		return "", false
	}
	return typeName(sig.Recv().Type())
}

// typeName renders the named type behind t.
func typeName(t types.Type) (string, bool) {
	if t == nil {
		return "", false
	}
	if p, ok := t.(*types.Pointer); ok {
		t = p.Elem()
	}
	switch tt := t.(type) {
	case *types.Named:
		return tt.Obj().Name(), true
	case *types.Alias:
		return tt.Obj().Name(), true
	}
	return types.TypeString(t, relQualifier(nil)), true
}

// targetOf builds the target description for a bound function object. Identity
// comes from the object's own position, never from string guesses. For method
// calls, override is the call-site receiver type when it differs from the
// declaring type (promoted methods).
func (a *analyzer) targetOf(fn *types.Func, method bool, override string) *target {
	t := &target{Name: fn.Name()}
	if p := fn.Pkg(); p != nil {
		t.Package = p.Path()
	} else {
		t.Package = "builtin"
	}
	if method {
		t.Receiver = override
		if t.Receiver == "" {
			if r, ok := receiverName(fn); ok {
				t.Receiver = r
			}
		}
	}
	pos := a.fset.Position(fn.Pos())
	if pos.IsValid() {
		t.Line = pos.Line
		if pos.Filename != "" {
			if rel := a.rel(pos.Filename); !strings.HasPrefix(rel, "../") {
				t.File = rel
			} else {
				t.File = filepath.ToSlash(pos.Filename)
			}
		}
	}
	return t
}

// stateWrites identifies state mutations outside shared execution owners. An
// unresolved/indexed/indirect destination cannot support a shared-owner proof.
func (a *analyzer) stateWrites(info *types.Info, body *ast.BlockStmt) ([]objID, int) {
	writes := []objID{}
	unknown := 0
	add := func(lhs ast.Expr) {
		if lhs == nil {
			return
		}
		if _, ok := lhs.(*ast.Ident); ok {
			return
		}
		if selected, ok := lhs.(*ast.SelectorExpr); ok {
			var obj types.Object
			if selection := info.Selections[selected]; selection != nil && selection.Kind() == types.FieldVal {
				obj = selection.Obj()
			} else {
				obj = info.Uses[selected.Sel]
			}
			if field, ok := obj.(*types.Var); ok {
				id := a.objectID(field)
				if id.File != "" {
					writes = append(writes, id)
					return
				}
			}
		}
		unknown++
	}
	ast.Inspect(body, func(node ast.Node) bool {
		switch n := node.(type) {
		case *ast.AssignStmt:
			for _, lhs := range n.Lhs {
				add(lhs)
			}
		case *ast.RangeStmt:
			add(n.Key)
			add(n.Value)
		case *ast.IncDecStmt:
			add(n.X)
		}
		return true
	})
	return writes, unknown
}
