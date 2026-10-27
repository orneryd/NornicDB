package workcensus

// Context-owner convergence author: Claude (storage-format-owners lane), #1693, 30 October 2026.

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"go/ast"
	"go/constant"
	"go/format"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"golang.org/x/tools/go/ssa"
)

// SourceBoundary reports compiled, generated and inactive build files. Inactive
// syntax is selectable with IncludeInactive; its type information is unavailable.
type SourceBoundary struct{ File, Package, State, Digest string }

// InvisibleBody reports a reached dependency or synthetic function whose body
// was unavailable to source selection. This is a declared approximation boundary.
type InvisibleBody struct{ Package, Name, Symbol, Signature, Synthetic string }

var approximationLimits = []string{
	"CHA/VTA conservatively approximates interface, function-value and registry calls; source closures passed through opaque bodies are retained.",
	"Reflection, plugins and bodies in InvisibleBodies need client semantic review; membership is not a runtime or cancellation-dominance proof.",
	"Generic instances share source syntax and source identity; synthetic wrappers without source are reported separately.",
	"Build flags and Tests select one typed composition. Inactive source is syntax-only; generated/inactive files are reported in Sources.",
}

// Extract enumerates source using only Config data. SSA reachability and source
// declaration traversal are independent: all-source selectors include unused
// methods, aliases, global initializers and literals without familiar names.
// No classification manifest participates in discovery.
func Extract(c Config) (Inventory, error) {
	out := Inventory{Limitations: append([]string(nil), approximationLimits...)}
	if len(c.Selectors) == 0 {
		return out, fmt.Errorf("no member selectors")
	}
	if c.SourceOnly {
		if len(c.Roots) > 0 || c.IncludeProgram {
			return out, fmt.Errorf("source-only requires no Roots or IncludeProgram")
		}
		for _, selector := range c.Selectors {
			if selector.Reachability != "all" {
				return out, fmt.Errorf("source-only requires all-source selectors")
			}
		}
		out.Limitations = append(out.Limitations, "Source-only mode omits SSA/CHA/VTA and reachability/context-access diagnostics. Typed declarations, def/use and source-assignment aliases remain; unresolved indirect/interface targets are retained as conservative call candidates, never fabricated callees.")
	}
	for _, s := range c.Selectors {
		if err := s.validate(); err != nil {
			return out, err
		}
	}
	root, err := filepath.Abs(c.Dir)
	if err != nil {
		return out, err
	}
	if err := discoverDocuments(&c, root, &out); err != nil {
		return out, err
	}
	if len(c.Patterns) == 0 && len(c.Documents) > 0 {
		if c.IncludeProgram {
			return out, fmt.Errorf("Go program view requires package patterns")
		}
		if len(c.Roots) > 0 {
			return out, fmt.Errorf("Go roots require package patterns")
		}
		if err := extractDocuments(c, root, &out); err != nil {
			return out, err
		}
		sort.Slice(out.Members, func(i, j int) bool { return out.Members[i].ID < out.Members[j].ID })
		return out, nil
	}
	var loaded *Program
	if c.SourceOnly {
		pkgs, fset, loadErr := loadPackages(c, root)
		err = loadErr
		loaded = &Program{Root: root, Packages: pkgs, FileSet: fset}
	} else {
		loaded, err = LoadProgram(c)
	}
	if err != nil {
		return out, err
	}
	pkgs, graph := loaded.Packages, loaded.CallGraph
	all := map[*ssa.Function]bool{}
	for _, fn := range loaded.Functions {
		all[fn] = true
	}
	if c.IncludeProgram {
		out.Program = loaded
	}
	population, err := loaded.ReachableFrom(c.Roots)
	if err != nil {
		return out, err
	}
	reached := population.Functions
	out.Roots = population.Roots
	if len(out.Roots) == 0 {
		allSources := false
		for _, selector := range c.Selectors {
			allSources = allSources || selector.Reachability == "all"
		}
		if len(c.Roots) > 0 || !allSources {
			return out, fmt.Errorf("no matching source roots")
		}
	}
	bySyntax := map[token.Pos]*ssa.Function{}
	sourceReachable := map[token.Pos]bool{}
	invisible := map[string]InvisibleBody{}
	for fn := range all {
		source := sourceFunction(fn)
		if source.Syntax() != nil {
			pos := source.Syntax().Pos()
			if prior := bySyntax[pos]; prior == nil || source.String() < prior.String() {
				bySyntax[pos] = source
			}
			if reached[fn] {
				sourceReachable[pos] = true
			}
		} else if reached[fn] {
			pkg := ""
			if fn.Pkg != nil {
				pkg = fn.Pkg.Pkg.Path()
			}
			invisible[fn.String()] = InvisibleBody{Package: pkg, Name: fn.Name(), Symbol: fn.String(), Signature: typeString(fn.Signature), Synthetic: fn.Synthetic}
		}
	}
	for _, body := range invisible {
		out.InvisibleBodies = append(out.InvisibleBodies, body)
	}
	sort.Slice(out.InvisibleBodies, func(i, j int) bool { return out.InvisibleBodies[i].Symbol < out.InvisibleBodies[j].Symbol })
	lengths := fixedLengths(pkgs)
	e := sourceExtractor{config: c, out: &out, fset: loaded.FileSet, root: root, bySyntax: bySyntax, reachable: sourceReachable, lengths: lengths, seenFiles: map[string]bool{}, aliases: aliases(pkgs, graph)}
	e.packages = map[*types.Info]*types.Package{}
	for _, p := range pkgs {
		if p.Types != nil && p.TypesInfo != nil {
			e.packages[p.TypesInfo] = p.Types
		}
	}
	// Package variants can contain the same file. Prefer its ordinary package
	// and visit each physical source file exactly once, including in test mode.
	sort.SliceStable(pkgs, func(i, j int) bool { return !strings.Contains(pkgs[i].ID, "[") && strings.Contains(pkgs[j].ID, "[") })
	for _, p := range pkgs {
		if !packageSelected(p.Types.Path(), c.Packages) {
			continue
		}
		for _, f := range p.Syntax {
			if err := e.file(p.Types.Path(), f, p.TypesInfo, "compiled"); err != nil {
				return out, err
			}
		}
		for _, filename := range p.IgnoredFiles {
			if !strings.HasSuffix(filename, ".go") || (!c.Tests && strings.HasSuffix(filename, "_test.go")) {
				continue
			}
			raw, err := readSource(c, filename)
			if err != nil {
				return out, err
			}
			f, err := parser.ParseFile(loaded.FileSet, filename, raw, parser.ParseComments)
			if err != nil {
				return out, err
			}
			if err := e.file(p.Types.Path(), f, nil, "inactive"); err != nil {
				return out, err
			}
		}
	}
	for _, body := range out.InvisibleBodies {
		site := sourceSite{pkg: body.Package, owner: body.Symbol, name: body.Name, declaration: "external", state: "external", reachable: true}
		for _, kind := range selectKinds(site, c.Selectors) {
			code := body.Symbol + "\n" + body.Signature + "\n" + body.Synthetic
			out.Members = append(out.Members, Member{ID: "external:" + body.Symbol + "/" + kind + "#1", Owner: body.Symbol, OwnerDigest: fingerprint([]byte(code)), Kind: kind, Code: code, Digest: fingerprint([]byte(code)), Signature: body.Signature, Declaration: "external", State: "external", Reachable: true, Typed: true})
		}
	}
	if err := extractDocuments(c, root, &out); err != nil {
		return out, err
	}
	sort.Slice(out.Sources, func(i, j int) bool { return out.Sources[i].File < out.Sources[j].File })
	sort.Slice(out.Definitions, func(i, j int) bool { return out.Definitions[i].Symbol < out.Definitions[j].Symbol })
	sort.Slice(out.Members, func(i, j int) bool { return out.Members[i].ID < out.Members[j].ID })
	return out, nil
}

func packageSelected(pkg string, scope []string) bool {
	if len(scope) == 0 {
		return true
	}
	for _, p := range scope {
		if pkg == p || strings.HasPrefix(pkg, p+"/") {
			return true
		}
	}
	return false
}

func readSource(c Config, path string) ([]byte, error) {
	if raw, ok := c.Overlay[path]; ok {
		return raw, nil
	}
	return os.ReadFile(path)
}
func fingerprint(raw []byte) string {
	raw = bytes.ReplaceAll(raw, []byte("\r\n"), []byte("\n"))
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

type sourceExtractor struct {
	packages  map[*types.Info]*types.Package
	config    Config
	out       *Inventory
	fset      *token.FileSet
	root      string
	bySyntax  map[token.Pos]*ssa.Function
	reachable map[token.Pos]bool
	lengths   map[types.Object]int64
	seenFiles map[string]bool
	aliases   *callAliases
}

func (e *sourceExtractor) text(n ast.Node) (string, error) {
	var b bytes.Buffer
	if field, ok := n.(*ast.Field); ok {
		// go/printer accepts a field inside a type, not as a standalone node.
		wrapped := &ast.StructType{Fields: &ast.FieldList{List: []*ast.Field{field}}}
		if err := format.Node(&b, e.fset, wrapped); err != nil {
			return "", err
		}
		text := b.String()
		start, end := strings.IndexByte(text, '{'), strings.LastIndexByte(text, '}')
		return strings.TrimSpace(text[start+1 : end]), nil
	}
	err := format.Node(&b, e.fset, n)
	return b.String(), err
}

func (e *sourceExtractor) file(pkg string, f *ast.File, info *types.Info, state string) error {
	// //line directives describe diagnostic origins, not the physical bytes
	// selected by a source control or emitted by a compiler-generated unit.
	filename := e.fset.PositionFor(f.Pos(), false).Filename
	if e.seenFiles[filename] {
		return nil
	}
	e.seenFiles[filename] = true
	raw, err := readSource(e.config, filename)
	if err != nil {
		return err
	}
	file, err := filepath.Rel(e.root, filename)
	if err != nil {
		return err
	}
	file = filepath.ToSlash(file)
	generated := ast.IsGenerated(f)
	if generated && (file == ".." || strings.HasPrefix(file, "../")) {
		// Compiler cache paths are incidental. Keep their generated units in
		// provider order within the package, independent of the cache root/hash.
		prefix := "@generated/" + pkg + "/unit-"
		units := 0
		for _, source := range e.out.Sources {
			if source.Package == pkg && strings.HasPrefix(source.File, prefix) {
				units++
			}
		}
		file = fmt.Sprintf("%s%d.go", prefix, units+1)
	}
	reportState := state
	if generated {
		reportState += "/generated"
	}
	e.out.Sources = append(e.out.Sources, SourceBoundary{file, pkg, reportState, fingerprint(raw)})
	if (state == "inactive" && !e.config.IncludeInactive) || (generated && e.config.ExcludeGenerated) {
		return nil
	}
	imports := map[string]string{}
	for _, im := range f.Imports {
		path, _ := strconv.Unquote(im.Path.Value)
		name := filepath.Base(path)
		if im.Name != nil {
			name = im.Name.Name
		}
		imports[name] = path
	}
	base := sourceSite{info: info, pkg: pkg, pkgType: e.packages[info], file: file, state: reportState, imports: imports, raw: raw}
	// File selectors fingerprint the entire file. Declaration selectors below
	// avoid line identity and provide one owner for every top-level declaration.
	fileSite := base
	fileSite.node = f
	fileSite.owner = pkg + ".file:" + file
	fileSite.declaration = "file"
	if err := e.member(fileSite, nil, "", nil); err != nil {
		return err
	}
	// Blank declarations and init functions are intentionally repeatable.
	// Position, not a reassigned ordinal, identifies repeatable owners. Source
	// controls preserve unaffected physical byte coordinates.
	owners := map[string]int{}
	uniqueOwner := func(name string, repeatable bool, pos token.Pos) string {
		owners[name]++
		if repeatable {
			return fmt.Sprintf("%s@source:%d", name, e.fset.PositionFor(pos, false).Offset)
		}
		if owners[name] > 1 {
			return fmt.Sprintf("%s@declaration#%d", name, owners[name])
		}
		return name
	}
	for _, decl := range f.Decls {
		switch d := decl.(type) {
		case *ast.FuncDecl:
			name := pkg + "." + d.Name.Name
			if d.Recv != nil && len(d.Recv.List) > 0 {
				receiver, err := e.text(d.Recv.List[0].Type)
				if err != nil {
					return err
				}
				name = "(" + pkg + "." + receiver + ")." + d.Name.Name
			}
			repeatable := d.Name.Name == "_" || (d.Recv == nil && d.Name.Name == "init")
			if fn := e.bySyntax[d.Pos()]; fn != nil && !repeatable {
				name = fn.String()
			} else if info != nil && !repeatable {
				if fn, ok := info.ObjectOf(d.Name).(*types.Func); ok {
					name = fn.FullName()
				}
			}
			kind := "function"
			if d.Recv != nil {
				kind = "method"
			}
			if err := e.owner(base, d, uniqueOwner(name, repeatable, d.Pos()), kind, ""); err != nil {
				return err
			}
		case *ast.GenDecl:
			for _, spec := range d.Specs {
				name, kind := "", ""
				repeatable := false
				switch sp := spec.(type) {
				case *ast.TypeSpec:
					name = sp.Name.Name
					repeatable = name == "_"
					kind = "type"
					if sp.Assign.IsValid() {
						kind = "alias"
					}
				case *ast.ValueSpec:
					var ns []string
					for _, id := range sp.Names {
						ns = append(ns, id.Name)
						repeatable = repeatable || id.Name == "_"
					}
					name = strings.Join(ns, ",")
					kind = strings.ToLower(d.Tok.String())
				case *ast.ImportSpec:
					name = sp.Path.Value
					kind = "import"
				}
				if err := e.owner(base, spec, uniqueOwner(pkg+"."+kind+":"+name, repeatable, spec.Pos()), kind, ""); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func declaredType(n ast.Node, info *types.Info) types.Type {
	if info == nil {
		return nil
	}
	switch n := n.(type) {
	case *ast.FuncDecl:
		if obj := info.ObjectOf(n.Name); obj != nil {
			return obj.Type()
		}
	case *ast.TypeSpec:
		if obj := info.ObjectOf(n.Name); obj != nil {
			return obj.Type()
		}
	case *ast.ValueSpec:
		if len(n.Names) > 0 {
			if obj := info.ObjectOf(n.Names[0]); obj != nil {
				return obj.Type()
			}
		}
	case *ast.FuncLit:
		return info.TypeOf(n)
	case *ast.Field:
		return info.TypeOf(n.Type)
	}
	if expr, ok := n.(ast.Expr); ok {
		return info.TypeOf(expr)
	}
	return nil
}

func (e *sourceExtractor) owner(base sourceSite, n ast.Node, name, kind, parentScope string) error {
	parentID := base.ownerID
	base.owner = name
	base.ownerID = base.file + ":" + name
	base.ownerSymbol = ""
	if decl, ok := n.(*ast.FuncDecl); ok && base.info != nil {
		if obj, ok := base.info.ObjectOf(decl.Name).(*types.Func); ok {
			base.ownerSymbol = obj.FullName()
			if decl.Recv == nil && decl.Name.Name == "init" {
				base.ownerSymbol = ""
				if fn := e.bySyntax[n.Pos()]; fn != nil {
					base.ownerSymbol = fn.String()
				}
			}
			if decl.Name.Name == "_" {
				base.ownerSymbol = ""
			}
		}
	}
	base.declaration = kind
	base.typ = declaredType(n, base.info)
	base.reachable = e.reachable[n.Pos()]
	scope := parentScope
	if fn := e.bySyntax[n.Pos()]; fn != nil {
		if own := ContextInScope(fn, e.config.ContextTypes, e.config.ContextFields); own != "" {
			scope = own
		}
	}
	ownerText, err := e.text(n)
	if err != nil {
		return err
	}
	digest := fingerprint([]byte(ownerText))
	bindings, values := ownerValues(base, n)
	base.bindings = bindings
	definition := Definition{ID: base.ownerID, ParentID: parentID, Kind: kind, Values: values, Symbol: base.ownerSymbol, Package: base.pkg, File: base.file, Digest: digest}
	definitionIndex := len(e.out.Definitions)
	if fn, ok := n.(*ast.FuncDecl); ok && fn.Body == nil {
		definitionIndex = -1
	} else {
		e.out.Definitions = append(e.out.Definitions, definition)
	}
	counts := map[string]int{}
	var walkErr error
	var ancestors []ast.Node
	ast.Inspect(n, func(node ast.Node) bool {
		if node == nil {
			ancestors = ancestors[:len(ancestors)-1]
			return false
		}
		if walkErr != nil {
			return false
		}
		if fn, ok := node.(*ast.FuncLit); ok && node != n {
			closure := fmt.Sprintf("%s$@source:%d", name, e.fset.PositionFor(fn.Pos(), false).Offset)
			// A later literal keeps its identity when another is removed. SSA
			// instance order and diagnostic line directives supply no identity.
			walkErr = e.owner(base, fn, closure, "closure", scope)
			return false
		}
		if id, ok := node.(*ast.Ident); ok && base.info != nil {
			if obj, ok := base.info.Defs[id].(*types.Var); ok && id.Name != "_" && !obj.IsField() {
				if base.bindings[obj] == "" {
					ref := fmt.Sprintf("%s/variable:%s@source:%d", base.ownerID, id.Name, e.fset.PositionFor(id.Pos(), false).Offset)
					base.bindings[obj] = ref
					definition.Values = append(definition.Values, ValueDefinition{Reference: ref, Name: id.Name, Type: typeString(obj.Type()), Role: "variable"})
				}
			}
		}
		site := base
		site.node = node
		site.ancestors = ancestors
		switch operation := node.(type) {
		case *ast.AssignStmt:
			for _, left := range operation.Lhs {
				if id, ok := left.(*ast.Ident); ok && site.info != nil && site.info.Defs[id] != nil {
					continue
				}
				if ref := valueReference(left, site); ref != "" {
					definition.Writes = append(definition.Writes, ref)
				}
			}
		case *ast.IncDecStmt:
			if ref := valueReference(operation.X, site); ref != "" {
				definition.Writes = append(definition.Writes, ref)
			}
		case *ast.RangeStmt:
			if operation.Tok == token.ASSIGN {
				for _, left := range []ast.Expr{operation.Key, operation.Value} {
					if ref := valueReference(left, site); ref != "" {
						definition.Writes = append(definition.Writes, ref)
					}
				}
			}
		case *ast.UnaryExpr:
			if operation.Op == token.AND {
				if ref := valueReference(operation.X, site); ref != "" {
					definition.Escapes = append(definition.Escapes, ref)
				}
			}
		}
		switch node.(type) {
		case *ast.FuncDecl, *ast.FuncLit, *ast.TypeSpec, *ast.ImportSpec:
			site.declaration = nodeKind(site)
		}
		// A declaration's own kind is independent of its containing function.
		// ValueSpec does not record var/const; its enclosing GenDecl does.
		if _, ok := node.(*ast.ValueSpec); ok {
			for i := len(ancestors) - 1; i >= 0; i-- {
				if declaration, ok := ancestors[i].(*ast.GenDecl); ok {
					site.declaration = strings.ToLower(declaration.Tok.String())
					break
				}
			}
		}
		ancestors = append(ancestors, node)
		site.typ = declaredType(node, base.info)
		walkErr = e.member(site, counts, digest, &ownerMetadata{scope: scope, body: n})
		return walkErr == nil
	})
	if definitionIndex >= 0 {
		e.out.Definitions[definitionIndex] = definition
	}
	return walkErr
}

type ownerMetadata struct {
	scope string
	body  ast.Node
}

func (e *sourceExtractor) member(site sourceSite, counts map[string]int, ownerDigest string, owner *ownerMetadata) error {
	if call, ok := site.node.(*ast.CallExpr); ok {
		site.targets, site.unresolvedTargets = e.aliases.targetFacts(call, site.info)
		site.unresolvedTargets = e.config.SourceOnly && site.unresolvedTargets
	}
	if field, ok := site.node.(*ast.Field); ok && site.info != nil {
		for _, name := range field.Names {
			if obj := site.info.ObjectOf(name); obj != nil {
				site.targets = append(site.targets, obj)
			}
		}
	}
	kinds := selectKinds(site, e.config.Selectors)
	if len(kinds) == 0 {
		return nil
	}
	if counts == nil {
		counts = map[string]int{}
	}
	code, err := e.text(site.node)
	if err != nil {
		return err
	}
	for _, kind := range kinds {
		counts[kind]++
		pos := e.fset.PositionFor(site.node.Pos(), false)
		m := Member{ID: fmt.Sprintf("%s:%s/%s#%d", site.file, site.owner, kind, counts[kind]), Owner: site.owner, Kind: kind, Reachable: site.reachable, Digest: fingerprint([]byte(code)), OwnerDigest: ownerDigest, Line: pos.Line, Code: code, Offset: pos.Offset, Declaration: site.declaration, State: site.state, Typed: site.info != nil, Signature: typeString(site.typ), UnderlyingType: underlyingString(site.typ)}
		m.Node = nodeKind(site)
		m.UnresolvedTargets = site.unresolvedTargets
		m.File, m.OwnerSymbol, m.OwnerID = site.file, site.ownerSymbol, site.ownerID
		if expression, ok := site.node.(ast.Expr); ok {
			m.ValueReference = valueReference(expression, site)
		}
		end := e.fset.PositionFor(site.node.End(), false)
		m.SiteID = sourceOccurrenceID(site.file, kind, m.Node, pos.Offset, end.Offset)
		m.Column, m.EndOffset, m.EndLine, m.EndColumn = pos.Column, end.Offset, end.Line, end.Column
		if e.config.IncludeSyntax {
			m.Syntax = &GoSyntax{Package: site.pkgType, Node: site.node, Ancestors: append([]ast.Node(nil), site.ancestors...), Info: site.info, FileSet: e.fset, Source: site.raw}
			if owner != nil {
				m.Syntax.Owner = owner.body
			}
		}
		if site.info != nil {
			m.OperandType = typeString(operandType(site))
		}
		if assertion, ok := site.node.(*ast.TypeAssertExpr); ok {
			if site.info != nil && assertion.Type != nil {
				m.AssertionType = typeString(site.info.TypeOf(assertion.Type))
			}
			if len(site.ancestors) > 0 {
				switch parent := site.ancestors[len(site.ancestors)-1].(type) {
				case *ast.AssignStmt:
					m.CommaOK = len(parent.Lhs) == 2 && len(parent.Rhs) == 1
				case *ast.ValueSpec:
					m.CommaOK = len(parent.Names) == 2 && len(parent.Values) == 1
				}
			}
		}
		if owner != nil {
			m.Scope = owner.scope
			switch n := owner.body.(type) {
			case *ast.FuncDecl:
				if n.Body != nil {
					m.FunctionBody = e.fset.PositionFor(n.Body.Lbrace, false).Offset + 1
				}
			case *ast.FuncLit:
				m.FunctionBody = e.fset.PositionFor(n.Body.Lbrace, false).Offset + 1
			}
		}
		if site.info != nil {
			m.Bound, m.BoundReason = smallBound(site.node, site.info, e.lengths)
			m.BoundKnown = m.BoundReason != "" && m.Bound >= 0
		}
		switch n := site.node.(type) {
		case *ast.ForStmt:
			m.Open = e.fset.PositionFor(n.Body.Lbrace, false).Offset + 1
		case *ast.RangeStmt:
			m.Open = e.fset.PositionFor(n.Body.Lbrace, false).Offset + 1
		case *ast.CallExpr:
			if selector, ok := n.Fun.(*ast.SelectorExpr); ok {
				m.ReceiverReference = valueReference(selector.X, site)
				m.Receiver, err = e.text(selector.X)
				if err != nil {
					return err
				}
				if site.info != nil {
					m.ReceiverType = typeString(site.info.TypeOf(selector.X))
				}
			}
			if site.info != nil {
				m.CallSignature = typeString(site.info.TypeOf(n.Fun))
				if len(n.Args) > 1 {
					if typ := site.info.TypeOf(n.Args[1]); typ != nil {
						if basic, ok := typ.Underlying().(*types.Basic); ok {
							m.StringInput = basic.Info()&types.IsString != 0
						}
					}
				}
			}
			for _, target := range site.targets {
				if fn, ok := target.(*types.Func); ok {
					m.Callees = append(m.Callees, fn.FullName())
				}
			}
			sort.Strings(m.Callees)
			m.Open = e.fset.PositionFor(n.Lparen, false).Offset + 1
			m.CalleeEnd = e.fset.PositionFor(n.Fun.End(), false).Offset
			for _, arg := range n.Args {
				m.ArgReferences = append(m.ArgReferences, valueReference(arg, site))
				argType := ""
				if site.info != nil {
					argType = typeString(site.info.TypeOf(arg))
				}
				m.ArgTypes = append(m.ArgTypes, argType)
				argText, err := e.text(arg)
				if err != nil {
					return err
				}
				m.Args = append(m.Args, argText)
				scalar := false
				if site.info != nil {
					if typ := site.info.TypeOf(arg); typ != nil {
						if basic, ok := typ.Underlying().(*types.Basic); ok {
							scalar = basic.Info()&(types.IsNumeric|types.IsBoolean) != 0
						}
					}
				}
				m.ScalarArgs = append(m.ScalarArgs, scalar)
			}
			if site.info != nil && len(n.Args) > 0 {
				if v := site.info.Types[n.Args[0]].Value; v != nil && v.Kind() == constant.String {
					m.Format = constant.StringVal(v)
				}
				if typ := site.info.TypeOf(n.Args[0]); typ != nil {
					if slice, ok := typ.Underlying().(*types.Slice); ok {
						m.SliceType = typeString(typ)
						m.ElementType = typeString(slice.Elem())
					}
				}
			}
		}
		if contains([]string{"function", "method", "closure", "type", "alias", "var", "const"}, nodeKind(site)) {
			m.Callees, m.References = sourceReferences(site.node, site.info)
		}
		e.out.Members = append(e.out.Members, m)
	}
	return nil
}

func sourceReferences(n ast.Node, info *types.Info) ([]string, []string) {
	if info == nil {
		return nil, nil
	}
	calls, refs := map[string]bool{}, map[string]bool{}
	ast.Inspect(n, func(node ast.Node) bool {
		if node == nil {
			return false
		}
		if call, ok := node.(*ast.CallExpr); ok {
			if fn, ok := callObject(call.Fun, info).(*types.Func); ok {
				calls[fn.FullName()] = true
			}
		}
		if id, ok := node.(*ast.Ident); ok {
			if obj := info.ObjectOf(id); obj != nil && obj.Pkg() != nil {
				refs[obj.Pkg().Path()+"."+obj.Name()] = true
			}
		}
		return true
	})
	var cs, rs []string
	for c := range calls {
		cs = append(cs, c)
	}
	for r := range refs {
		rs = append(rs, r)
	}
	sort.Strings(cs)
	sort.Strings(rs)
	return cs, rs
}
