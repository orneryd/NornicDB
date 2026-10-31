package workcensus

import (
	"fmt"
	"go/ast"
	"go/types"
	"regexp"
	"strings"

	"golang.org/x/tools/go/ssa"
)

// Selector is a declarative union of names/prefixes/contains, intersected with
// the other supplied filters. Empty filters accept any value. Node names are
// declaration, function, method, closure, type, alias, var, const, import, file,
// external, for, range, call, identifier, index, slice, map, field, binary,
// switch, type-switch, type-assert, go, assign, return, composite, func-type and
// lexical text (with Pattern). Reachability is reachable
// (the default) or all. The first matching rule in each Group wins per possible call target;
// possible alias targets union their kinds once per source node. An empty
// Group defaults to Kind, allowing independent selector populations.
// Package filters describe callees for calls and source packages otherwise.
type Selector struct {
	Pattern         string   `json:"pattern,omitempty"`
	Languages       []string `json:"languages,omitempty"`
	Kind            string   `json:"kind,omitempty"`
	Node            string   `json:"node,omitempty"`
	Group           string   `json:"group,omitempty"`
	Reachability    string   `json:"reachability,omitempty"`
	Packages        []string `json:"packages,omitempty"`
	PackagePrefixes []string `json:"package_prefixes,omitempty"`
	Names           []string `json:"names,omitempty"`
	NamePrefixes    []string `json:"name_prefixes,omitempty"`
	NameContains    []string `json:"name_contains,omitempty"`
	Files           []string `json:"files,omitempty"`
	OwnerContains   []string `json:"owner_contains,omitempty"`
	States          []string `json:"states,omitempty"`
	ResultTypes     []string `json:"result_types,omitempty"`
	ReceiverTypes   []string `json:"receiver_types,omitempty"`
	OperandTypes    []string `json:"operand_types,omitempty"`
	TypeShapes      []string `json:"type_shapes,omitempty"`
	TypeContains    []string `json:"type_contains,omitempty"`
	Operators       []string `json:"operators,omitempty"`
	Builtin         bool     `json:"builtin,omitempty"`
	UnwrapResults   bool     `json:"unwrap_results,omitempty"`
	FoldCase        bool     `json:"fold_case,omitempty"`
	KindFromSymbol  bool     `json:"kind_from_symbol,omitempty"`
}

// Policy supplies reviewed classes and optional evidence-reference obligations.
// ReviewOwner and Subjects are client data, not claims of semantic execution.
// Custom classes require ClassKinds. Layers pin actual source callee bodies.
// BoundedClasses adds classes with bounded semantics; it cannot disable checks.
type Policy struct {
	RequireDelegationWitnesses bool              `json:"require_delegation_witnesses,omitempty"`
	Controls                   []SourceControl   `json:"controls,omitempty"`
	RequireSubjectsFor         []string          `json:"require_subjects_for,omitempty"`
	ClassFact                  string            `json:"class_fact,omitempty"`
	RequireSources             bool              `json:"require_sources,omitempty"`
	FactKeys                   []string          `json:"fact_keys,omitempty"`
	ForbiddenClasses           []string          `json:"forbidden_classes,omitempty"`
	Assignments                []Assignment      `json:"assignments,omitempty"`
	ClassKinds                 map[string]string `json:"class_kinds,omitempty"`
	Layers                     []OwnerLayer      `json:"layers,omitempty"`
	Classes                    []string          `json:"classes,omitempty"`
	BoundedClasses             []string          `json:"bounded_classes,omitempty"`
	RequireReviewOwner         bool              `json:"require_review_owner,omitempty"`
	RequireSubjects            bool              `json:"require_subjects,omitempty"`
	RequireOwnerDigest         bool              `json:"require_owner_digest,omitempty"`
	Owners                     []string          `json:"owners,omitempty"`
	Subjects                   []string          `json:"subjects,omitempty"`
}

func contains(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

func rootMatches(fn *ssa.Function, root Root) bool {
	fn = sourceFunction(fn)
	if fn == nil || fn.Pkg == nil || fn.Pkg.Pkg.Path() != root.Package || fn.Name() != root.Name {
		return false
	}
	if root.Receiver == "" {
		return true
	}
	if recv := fn.Signature.Recv(); recv != nil {
		t := types.Unalias(recv.Type())
		if ptr, ok := t.(*types.Pointer); ok {
			t = types.Unalias(ptr.Elem())
		}
		if named, ok := t.(*types.Named); ok {
			return named.Obj().Name() == root.Receiver
		}
	}
	return false
}

func sourceFunction(fn *ssa.Function) *ssa.Function {
	if fn != nil {
		if origin := fn.Origin(); origin != nil {
			return origin
		}
	}
	return fn
}

func (s Selector) validate() error {
	if s.Node == "text" {
		if s.Pattern == "" {
			return fmt.Errorf("text selector requires a pattern")
		}
		if _, err := regexp.Compile(s.Pattern); err != nil {
			return err
		}
	} else if s.Pattern != "" {
		return fmt.Errorf("pattern requires a text selector")
	}
	if s.Kind == "" && !s.KindFromSymbol {
		return fmt.Errorf("selector %q needs a kind", s.Node)
	}
	if s.Reachability != "" && s.Reachability != "reachable" && s.Reachability != "all" {
		return fmt.Errorf("unknown reachability %q", s.Reachability)
	}
	switch s.Node {
	case "declaration", "function", "method", "closure", "type", "alias", "var", "const", "import", "file", "external", "for", "range", "call", "identifier", "index", "slice", "map", "field", "binary", "switch", "assign", "return", "composite", "func-type", "type-assert", "type-switch", "go", "text":
	default:
		return fmt.Errorf("unknown selector node %q", s.Node)
	}
	return nil
}

type sourceSite struct {
	unresolvedTargets                    bool
	ownerID                              string
	bindings                             map[types.Object]string
	pkgType                              *types.Package
	language, nativeKind                 string
	ownerSymbol                          string
	node                                 ast.Node
	ancestors                            []ast.Node
	raw                                  []byte
	info                                 *types.Info
	pkg, file, owner, declaration, state string
	reachable                            bool
	typ                                  types.Type
	imports                              map[string]string
	name                                 string
	target                               types.Object
	targets                              []types.Object
}

func nodeKind(site sourceSite) string {
	if site.nativeKind != "" {
		return site.nativeKind
	}
	if site.node == nil {
		return site.declaration
	}
	switch n := site.node.(type) {
	case *ast.FuncDecl:
		if n.Recv != nil {
			return "method"
		}
		return "function"
	case *ast.FuncLit:
		return "closure"
	case *ast.TypeSpec:
		if n.Assign.IsValid() {
			return "alias"
		}
		return "type"
	case *ast.ValueSpec:
		return site.declaration
	case *ast.ImportSpec:
		return "import"
	case *ast.File:
		return "file"
	case *ast.ForStmt:
		return "for"
	case *ast.RangeStmt:
		return "range"
	case *ast.CallExpr:
		return "call"
	case *ast.Ident:
		return "identifier"
	case *ast.IndexExpr, *ast.IndexListExpr:
		return "index"
	case *ast.SliceExpr:
		return "slice"
	case *ast.MapType:
		return "map"
	case *ast.Field:
		return "field"
	case *ast.BinaryExpr:
		return "binary"
	case *ast.SwitchStmt:
		return "switch"
	case *ast.TypeSwitchStmt:
		return "type-switch"
	case *ast.TypeAssertExpr:
		return "type-assert"
	case *ast.GoStmt:
		return "go"
	case *ast.AssignStmt:
		return "assign"
	case *ast.ReturnStmt:
		return "return"
	case *ast.CompositeLit:
		return "composite"
	case *ast.FuncType:
		return "func-type"
	}
	return ""
}

func callObject(expr ast.Expr, info *types.Info) types.Object {
	if info == nil {
		return nil
	}
	switch f := expr.(type) {
	case *ast.Ident:
		return info.ObjectOf(f)
	case *ast.SelectorExpr:
		return info.ObjectOf(f.Sel)
	case *ast.IndexExpr:
		return callObject(f.X, info)
	case *ast.IndexListExpr:
		return callObject(f.X, info)
	case *ast.ParenExpr:
		return callObject(f.X, info)
	}
	return nil
}

func typeString(t types.Type) string {
	if t == nil {
		return ""
	}
	return types.TypeString(t, func(p *types.Package) string { return p.Path() })
}

func underlyingString(t types.Type) string {
	if t == nil {
		return ""
	}
	t = types.Unalias(t).Underlying()
	switch t := t.(type) {
	case *types.Basic:
		return types.Typ[t.Kind()].Name()
	case *types.Slice:
		return "[]" + underlyingString(t.Elem())
	case *types.Array:
		return fmt.Sprintf("[%d]%s", t.Len(), underlyingString(t.Elem()))
	default:
		return typeString(t)
	}
}

func signatureOf(site sourceSite) *types.Signature {
	if call, ok := site.node.(*ast.CallExpr); ok && site.info != nil {
		if t := site.info.TypeOf(call.Fun); t != nil {
			sig, _ := t.Underlying().(*types.Signature)
			return sig
		}
	}
	if site.typ != nil {
		sig, _ := site.typ.Underlying().(*types.Signature)
		return sig
	}
	return nil
}

func symbolOf(site sourceSite) (pkg, name string, builtin bool) {
	var object types.Object
	switch n := site.node.(type) {
	case *ast.CallExpr:
		object = site.target
		if object == nil {
			object = callObject(n.Fun, site.info)
		}
	case *ast.Ident:
		if site.info != nil {
			object = site.info.ObjectOf(n)
		}
		name = n.Name
	case *ast.FuncDecl:
		if site.info != nil {
			object = site.info.ObjectOf(n.Name)
		}
		name = n.Name.Name
	case *ast.TypeSpec:
		if site.info != nil {
			object = site.info.ObjectOf(n.Name)
		}
		name = n.Name.Name
	case *ast.ValueSpec:
		if len(n.Names) > 0 {
			name = n.Names[0].Name
			if site.info != nil {
				object = site.info.ObjectOf(n.Names[0])
			}
		}
	case *ast.Field:
		object = site.target
		if object == nil && len(n.Names) > 0 {
			name = n.Names[0].Name
			if site.info != nil {
				object = site.info.ObjectOf(n.Names[0])
			}
		}

	}
	if object != nil {
		name = object.Name()
		if p := object.Pkg(); p != nil {
			pkg = p.Path()
		}
		_, builtin = object.(*types.Builtin)
	} else {
		pkg = site.pkg
		if name == "" {
			name = site.name
		}
		if call, ok := site.node.(*ast.CallExpr); ok {
			switch f := call.Fun.(type) {
			case *ast.SelectorExpr:
				name = f.Sel.Name
				if id, ok := f.X.(*ast.Ident); ok {
					if imported, ok := site.imports[id.Name]; ok {
						pkg = imported
					}
				}
			case *ast.Ident:
				name = f.Name
			}
		}
	}
	return
}

func operandType(site sourceSite) types.Type {
	if site.info == nil {
		return nil
	}
	var expr ast.Expr
	switch n := site.node.(type) {
	case *ast.RangeStmt:
		expr = n.X
	case *ast.IndexExpr:
		expr = n.X
	case *ast.IndexListExpr:
		expr = n.X
	case *ast.SliceExpr:
		expr = n.X
	case *ast.BinaryExpr:
		expr = n.X
	case ast.Expr:
		expr = n
	}
	if expr != nil {
		return site.info.TypeOf(expr)
	}
	return site.typ
}

func nameMatches(name string, s Selector) bool {
	if len(s.Names)+len(s.NamePrefixes)+len(s.NameContains) == 0 {
		return true
	}
	normal := func(v string) string {
		if s.FoldCase {
			return strings.ToLower(v)
		}
		return v
	}
	name = normal(name)
	for _, v := range s.Names {
		if name == normal(v) {
			return true
		}
	}
	for _, v := range s.NamePrefixes {
		if strings.HasPrefix(name, normal(v)) {
			return true
		}
	}
	for _, v := range s.NameContains {
		if strings.Contains(name, normal(v)) {
			return true
		}
	}
	return false
}

func shape(t types.Type) string {
	if t == nil {
		return ""
	}
	switch types.Unalias(t).Underlying().(type) {
	case *types.Map:
		return "map"
	case *types.Slice:
		return "slice"
	case *types.Array:
		return "array"
	case *types.Struct:
		return "struct"
	case *types.Signature:
		return "signature"
	case *types.Interface:
		return "interface"
	case *types.Pointer:
		return "pointer"
	case *types.Basic:
		return underlyingString(t)
	}
	return ""
}

func matches(s Selector, site sourceSite) bool {
	language := site.language
	if language == "" {
		language = "go"
	}
	if len(s.Languages) > 0 && !contains(s.Languages, language) {
		return false
	}
	if s.Reachability != "all" && !site.reachable {
		return false
	}
	kind := nodeKind(site)
	if s.Node != kind {
		if !(s.Node == "declaration" && contains([]string{"function", "method", "closure", "type", "alias", "var", "const", "import"}, kind)) && !(s.Node == "function" && (kind == "method" || kind == "closure")) {
			return false
		}
	}
	if len(s.States) > 0 && !contains(s.States, site.state) {
		return false
	}
	if len(s.Files) > 0 {
		found := false
		for _, p := range s.Files {
			if strings.HasPrefix(site.file, p) {
				found = true
			}
		}
		if !found {
			return false
		}
	}
	if len(s.OwnerContains) > 0 {
		found := false
		for _, p := range s.OwnerContains {
			if strings.Contains(site.owner, p) {
				found = true
			}
		}
		if !found {
			return false
		}
	}
	pkg, name, builtin := symbolOf(site)
	if !site.unresolvedTargets && len(s.Packages)+len(s.PackagePrefixes) > 0 {
		found := contains(s.Packages, pkg)
		for _, p := range s.PackagePrefixes {
			found = found || strings.HasPrefix(pkg, p)
		}
		if !found {
			return false
		}
	}
	if s.Builtin && !builtin {
		return false
	}
	if !site.unresolvedTargets && !nameMatches(name, s) {
		return false
	}
	if len(s.OperandTypes) > 0 && !contains(s.OperandTypes, underlyingString(operandType(site))) {
		return false
	}
	if len(s.TypeShapes) > 0 && !contains(s.TypeShapes, shape(operandType(site))) {
		return false
	}
	if len(s.TypeContains) > 0 {
		text := typeString(operandType(site)) + " " + underlyingString(operandType(site))
		found := false
		for _, v := range s.TypeContains {
			found = found || strings.Contains(text, v)
		}
		if !found {
			return false
		}
	}
	if len(s.Operators) > 0 {
		binary, ok := site.node.(*ast.BinaryExpr)
		if !ok || !contains(s.Operators, binary.Op.String()) {
			return false
		}
	}
	if len(s.ResultTypes) > 0 || (len(s.ReceiverTypes) > 0 && !site.unresolvedTargets) {
		sig := signatureOf(site)
		if sig == nil {
			return false
		}
		if len(s.ReceiverTypes) > 0 && !site.unresolvedTargets && (sig.Recv() == nil || !contains(s.ReceiverTypes, typeString(sig.Recv().Type()))) {
			return false
		}
		if len(s.ResultTypes) > 0 {
			found := false
			for i := 0; i < sig.Results().Len(); i++ {
				t := sig.Results().At(i).Type()
				if s.UnwrapResults {
					if slice, ok := t.Underlying().(*types.Slice); ok {
						t = slice.Elem()
					}
					if ptr, ok := t.(*types.Pointer); ok {
						t = ptr.Elem()
					}
				}
				found = found || contains(s.ResultTypes, typeString(types.Unalias(t)))
			}
			if !found {
				return false
			}
		}
	}
	return true
}

func selectKinds(site sourceSite, selectors []Selector) []string {
	var kinds []string
	seenKinds := map[string]bool{}
	variants := []sourceSite{site}
	if len(site.targets) > 0 {
		variants = nil
		for _, target := range site.targets {
			variant := site
			variant.target = target
			variant.unresolvedTargets = false
			variants = append(variants, variant)
		}
		if site.unresolvedTargets {
			variants = append(variants, site)
		}
	}
	for _, site := range variants {
		groups := map[string]bool{}
		for _, s := range selectors {
			group := s.Group
			if group == "" {
				group = s.Kind
			}
			if groups[group] || !matches(s, site) {
				continue
			}
			kind := s.Kind
			if s.KindFromSymbol {
				if site.unresolvedTargets {
					kind = "unresolved-call-target"
				} else {
					pkg, name, _ := symbolOf(site)
					kind = pkg + "." + name
				}
			}
			groups[group] = true
			if !seenKinds[kind] {
				kinds = append(kinds, kind)
				seenKinds[kind] = true
			}
		}
	}
	return kinds
}
