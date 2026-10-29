//go:build nameflowcensus

// Package nameflow interprets decoded-name def/use over the shared census facts.
// It does not load, parse, enumerate, hash or validate repository source itself.
package nameflow

import (
	"fmt"
	"go/ast"
	"go/token"
	"go/types"
	"sort"
	"strconv"
	"strings"

	"github.com/orneryd/nornicdb/internal/workcensus"
)

// Contracts identify semantic producers, not sink spellings. Field entries name
// declared graph-identity products; calls name decoded return slots. The catalog
// is reviewed separately from the graph of assignments/uses derived below.
type Contracts struct {
	Calls           []CallProducer  `json:"calls"`
	Fields          []FieldProducer `json:"fields"`
	Writers         []string        `json:"writers"`
	RecordingOwners []string        `json:"recording_owners"`
}
type CallProducer struct {
	Symbol string `json:"symbol"`
	Result int    `json:"result"`
	Role   string `json:"role"`
	Reason string `json:"reason"`
}
type FieldProducer struct {
	Type, Field, Channel, Role, Reason string
}
type Edge struct {
	From, To, Kind, Site string
}
type Producer struct {
	Site, Cell, Role, Contract string
}
type Result struct {
	Inventory  workcensus.Inventory `json:"inventory"`
	Producers  []Producer           `json:"producers"`
	Edges      []Edge               `json:"edges"`
	Witnesses  map[string][]Edge    `json:"witnesses"`
	Unresolved []string             `json:"unresolved"`
	Compiled   *CompiledQueries     `json:"compiled_queries,omitempty"`
}
type function struct {
	params   []string
	receiver string
	returns  []string
}
type analysis struct {
	input          workcensus.Inventory
	contracts      Contracts
	members        map[ast.Node]workcensus.Member
	functions      map[string]function
	ownerFunctions map[string]string
	edges          []Edge
	producers      []Producer
	inputs         map[string][]string
	kinds          map[string]string
	cells          map[ast.Expr]string
	channels       map[string]bool
	compiled       bool
	unknown        map[string]bool
	imports        map[string]map[string]string
	shadowed       map[string]bool
}

// Analyze is a conservative, context-insensitive candidate analysis. Return and
// field unions can overapproximate callers; each candidate therefore needs an
// actual producer/caller interpretation, never approval inferred from reach.
// Primitive decoders and source writers are representation barriers. Roots are
// producer facts; comparisons and registry operations have no special allowlist.
func Analyze(input workcensus.Inventory, contracts Contracts) Result {
	return analyze(input, contracts, false)
}

func analyze(input workcensus.Inventory, contracts Contracts, compiled bool) Result {
	a := &analysis{compiled: compiled, input: input, contracts: contracts, members: map[ast.Node]workcensus.Member{}, functions: map[string]function{}, ownerFunctions: map[string]string{}, inputs: map[string][]string{}, kinds: map[string]string{}, cells: map[ast.Expr]string{}, channels: map[string]bool{}, unknown: map[string]bool{}, imports: map[string]map[string]string{}, shadowed: map[string]bool{}}
	if compiled {
		a.ssaFlows()
		return a.finish()
	}
	// Source-only builtin interpretation must not mistake a local parameter or
	// variable named copy/len/make for the language builtin.
	for _, m := range input.Members {
		if m.Syntax == nil {
			continue
		}
		remember := func(id *ast.Ident) { a.shadowed[sourceOwner(m)+":"+id.Name] = true }
		switch n := m.Syntax.Node.(type) {
		case *ast.AssignStmt:
			if n.Tok == token.DEFINE {
				for _, left := range n.Lhs {
					if id, ok := left.(*ast.Ident); ok {
						remember(id)
					}
				}
			}
		case *ast.ValueSpec:
			for _, id := range n.Names {
				remember(id)
			}
		case *ast.FuncDecl:
			for _, list := range []*ast.FieldList{n.Recv, n.Type.Params, n.Type.Results} {
				if list != nil {
					for _, field := range list.List {
						for _, id := range field.Names {
							remember(id)
						}
					}
				}
			}
		case *ast.FuncLit:
			for _, list := range []*ast.FieldList{n.Type.Params, n.Type.Results} {
				if list != nil {
					for _, field := range list.List {
						for _, id := range field.Names {
							remember(id)
						}
					}
				}
			}
		}
	}
	for _, m := range input.Members {
		if m.Syntax != nil {
			if spec, ok := m.Syntax.Node.(*ast.ImportSpec); ok {
				path, err := strconv.Unquote(spec.Path.Value)
				if err != nil {
					continue
				}
				name := path[strings.LastIndex(path, "/")+1:]
				if spec.Name != nil {
					name = spec.Name.Name
				}
				file, _, _ := strings.Cut(m.ID, ":")
				if a.imports[file] == nil {
					a.imports[file] = map[string]string{}
				}
				a.imports[file][name] = path
			}
		}
	}
	for _, m := range input.Members {
		if m.Syntax != nil {
			a.members[m.Syntax.Node] = m
			if d, ok := m.Syntax.Node.(*ast.FuncDecl); ok {
				name := a.functionName(m, d)
				f := function{}
				if d.Recv != nil {
					for _, field := range d.Recv.List {
						for _, id := range field.Names {
							f.receiver = a.object(m, id)
						}
					}
				}
				if d.Type.Params != nil {
					for _, field := range d.Type.Params.List {
						for _, id := range field.Names {
							f.params = append(f.params, a.object(m, id))
						}
					}
				}
				if d.Type.Results != nil {
					for _, field := range d.Type.Results.List {
						count := len(field.Names)
						if count == 0 {
							count = 1
						}
						for n := 0; n < count; n++ {
							f.returns = append(f.returns, fmt.Sprintf("return:%s:%d", name, len(f.returns)))
						}
					}
				}
				if d.Type.Results != nil {
					slot := 0
					for _, field := range d.Type.Results.List {
						if len(field.Names) == 0 {
							slot++
							continue
						}
						for _, id := range field.Names {
							a.copy(a.object(m, id), f.returns[slot], "named return", m.ID)
							slot++
						}
					}
				}
				a.functions[name] = f
				a.ownerFunctions[sourceOwner(m)] = name
			}
		}
	}
	for _, m := range input.Members {
		if m.Syntax == nil {
			continue
		}
		switch n := m.Syntax.Node.(type) {
		case ast.Expr:
			a.value(m, n)
		}
	}
	for _, m := range input.Members {
		if m.Syntax == nil {
			continue
		}
		switch n := m.Syntax.Node.(type) {
		case *ast.AssignStmt:
			for i, left := range n.Lhs {
				if id, ok := left.(*ast.Ident); ok && id.Name == "_" {
					continue
				}
				var from string
				if len(n.Rhs) == len(n.Lhs) {
					from = a.value(m, n.Rhs[i])
				} else if len(n.Rhs) == 1 {
					from = a.value(m, n.Rhs[0]) + fmt.Sprintf("#%d", i)
				}
				if from != "" {
					a.copy(from, a.lvalue(m, left), "assignment", m.ID)
				}
				if _, ok := left.(*ast.IndexExpr); ok {
					a.sink(m, "key write", left)
				}
			}
		case *ast.ValueSpec:
			for i, id := range n.Names {
				if len(n.Values) == len(n.Names) {
					a.copy(a.value(m, n.Values[i]), a.object(m, id), "declaration", m.ID)
				}
			}
		case *ast.RangeStmt:
			from := a.value(m, n.X)
			mapRange := false
			if m.Syntax.Info != nil {
				if typ := m.Syntax.Info.TypeOf(n.X); typ != nil {
					_, mapRange = typ.Underlying().(*types.Map)
				}
			}
			if n.Key != nil && mapRange {
				a.copy(from+"/key", a.lvalue(m, n.Key), "range map key", m.ID)
			}
			if n.Value != nil {
				a.copy(from+"/element", a.lvalue(m, n.Value), "range value", m.ID)
				a.copy(from, a.lvalue(m, n.Value), "conservative container value", m.ID)
			}
		case *ast.ReturnStmt:
			if name := a.ownerFunctions[sourceOwner(m)]; name != "" {
				f := a.functions[name]
				for i, e := range n.Results {
					if i < len(f.returns) {
						a.copy(a.value(m, e), f.returns[i], "return", m.ID)
					}
				}
			}
		}
	}
	a.ssaFlows()
	return a.finish()
}

func (a *analysis) functionName(m workcensus.Member, d *ast.FuncDecl) string {
	if m.Syntax.Info != nil {
		if f, ok := m.Syntax.Info.ObjectOf(d.Name).(*types.Func); ok {
			return f.FullName()
		}
	}
	file, _, _ := strings.Cut(m.ID, ":")
	directory := ""
	if i := strings.LastIndex(file, "/"); i >= 0 {
		directory = "/" + file[:i]
	}
	pkg := "github.com/orneryd/nornicdb" + directory
	if d.Recv != nil && len(d.Recv.List) > 0 {
		typ := d.Recv.List[0].Type
		pointer := ""
		if p, ok := typ.(*ast.StarExpr); ok {
			pointer = "*"
			typ = p.X
		}
		if id, ok := typ.(*ast.Ident); ok {
			return "(" + pointer + pkg + "." + id.Name + ")." + d.Name.Name
		}
	}
	return pkg + "." + d.Name.Name
}
func sourceOwner(m workcensus.Member) string {
	file, _, _ := strings.Cut(m.ID, ":")
	return file + ":" + m.Owner
}
func (a *analysis) object(m workcensus.Member, id *ast.Ident) string {
	if m.Syntax.Info != nil {
		if obj := m.Syntax.Info.ObjectOf(id); obj != nil {
			p := m.Syntax.FileSet.Position(obj.Pos())
			return fmt.Sprintf("object:%s:%d:%s", p.Filename, p.Offset, obj.Name())
		}
	}
	if id.Obj != nil {
		file, _, _ := strings.Cut(m.ID, ":")
		return fmt.Sprintf("syntax-object:%s:%d:%s", file, id.Obj.Pos(), id.Name)
	}
	return "untyped-object:" + sourceOwner(m) + ":" + id.Name
}
func (a *analysis) add(from, to, kind, site string) {
	if from != "" && to != "" && from != to {
		a.edges = append(a.edges, Edge{from, to, kind, site})
	}
}
func (a *analysis) copy(from, to, kind, site string) {
	a.add(from, to, kind, site)
	a.add(from+"/key", to+"/key", kind+" map keys", site)
	a.add(from+"/element", to+"/element", kind+" elements", site)
}
func (a *analysis) source(cell, role, contract, site string) {
	a.producers = append(a.producers, Producer{site, cell, role, contract})
}
func (a *analysis) lvalue(m workcensus.Member, e ast.Expr) string {
	switch e := e.(type) {
	case *ast.Ident:
		return a.object(m, e)
	case *ast.SelectorExpr:
		return a.field(m, e)
	case *ast.IndexExpr:
		base := a.value(m, e.X)
		if literal, ok := e.Index.(*ast.BasicLit); ok && literal.Kind == token.STRING {
			name, _ := strconv.Unquote(literal.Value)
			return base + "/field:" + name
		}
		return base + "/element"
	case *ast.StarExpr:
		return a.lvalue(m, e.X)
	case *ast.ParenExpr:
		return a.lvalue(m, e.X)
	}
	return a.value(m, e)
}
func (a *analysis) field(m workcensus.Member, e *ast.SelectorExpr) string {
	if m.Syntax.Info != nil {
		if sel := m.Syntax.Info.Selections[e]; sel != nil && sel.Kind() == types.FieldVal {
			typ := sel.Recv()
			for {
				p, ok := typ.(*types.Pointer)
				if !ok {
					break
				}
				typ = p.Elem()
			}
			if named, ok := types.Unalias(typ).(*types.Named); ok {
				obj := named.Obj()
				if obj.Pkg() != nil {
					return "field:" + obj.Pkg().Path() + "." + obj.Name() + "." + e.Sel.Name
				}
			}
		}
	}
	return a.value(m, e.X) + "/field:" + e.Sel.Name
}
func (a *analysis) value(m workcensus.Member, e ast.Expr) string {
	if e == nil {
		return ""
	}
	if cell, ok := a.cells[e]; ok {
		return cell
	}
	actual, ok := a.members[e]
	if !ok {
		actual = m
	}
	cell := fmt.Sprintf("value:%s:%d", actual.ID, m.Syntax.FileSet.Position(e.Pos()).Offset)
	a.cells[e] = cell
	member := actual.ID
	switch n := e.(type) {
	case *ast.Ident:
		a.copy(a.object(m, n), cell, "identifier use", member)
	case *ast.ParenExpr:
		a.copy(a.value(m, n.X), cell, "parenthesis", member)
	case *ast.StarExpr:
		a.copy(a.value(m, n.X), cell, "dereference", member)
	case *ast.UnaryExpr:
		if n.Op == token.AND {
			a.copy(a.value(m, n.X), cell, "address", member)
		}
	case *ast.SelectorExpr:
		field := a.field(m, n)
		a.copy(field, cell, "field read", member)
		if m.Syntax.Info == nil {
			// Inactive/source-only declarations have no compiler field object.
			// Retain a conservative field-name union across their struct
			// construction, parameters and returns, rather than losing the
			// product at an untyped aggregate boundary.
			a.copy("untyped-field:"+n.Sel.Name, cell, "source-only field union read", member)
			a.copy(a.value(m, n.X), cell, "source-only aggregate field candidate", member)
		}
		for _, producer := range a.contracts.Fields {
			if field == "field:"+producer.Type+"."+producer.Field {
				a.source(cell+producer.Channel, producer.Role, producer.Type+"."+producer.Field, member)
			}
		}
	case *ast.IndexExpr:
		base := a.value(m, n.X)
		a.copy(base+"/element", cell, "element read", member)
		if literal, ok := n.Index.(*ast.BasicLit); ok && literal.Kind == token.STRING {
			name, _ := strconv.Unquote(literal.Value)
			a.copy(base+"/field:"+name, cell, "named map value", member)
		}
		a.add(base, cell, "conservative container read", member)
		a.inputs[member] = append(a.inputs[member], a.value(m, n.Index))
		a.kinds[member] = "key lookup"
	case *ast.SliceExpr:
		a.copy(a.value(m, n.X), cell, "slice", member)
		a.sink(actual, "slice", n.X)
	case *ast.TypeAssertExpr:
		a.copy(a.value(m, n.X), cell, "type assertion", member)
		a.copy(cell, cell+"#0", "asserted value", member)
	case *ast.BinaryExpr:
		a.sink(actual, "comparison or concatenation", n.X, n.Y)
		if n.Op == token.ADD {
			a.add(a.value(m, n.X), cell, "concatenation", member)
			a.add(a.value(m, n.Y), cell, "concatenation", member)
		}
	case *ast.CallExpr:
		a.call(actual, n, cell)
	case *ast.CompositeLit:
		mapOrUnknown := m.Syntax.Info == nil
		if m.Syntax.Info != nil {
			if typ := m.Syntax.Info.TypeOf(n); typ != nil {
				_, mapOrUnknown = typ.Underlying().(*types.Map)
			}
		}
		for _, entry := range n.Elts {
			if pair, ok := entry.(*ast.KeyValueExpr); ok {
				v, yes := pair.Value.(ast.Expr)
				if !yes {
					continue
				}
				from := a.value(m, v)
				a.aggregateContent(from, cell, member)
				if id, yes := pair.Key.(*ast.Ident); yes {
					a.copy(from, cell+"/field:"+id.Name, "struct construction", member)
					if m.Syntax.Info == nil {
						a.copy(from, "untyped-field:"+id.Name, "source-only field union store", member)
					}
					if m.Syntax.Info != nil {
						typ := m.Syntax.Info.TypeOf(n)
						if p, yes := typ.(*types.Pointer); yes {
							typ = p.Elem()
						}
						if typ != nil {
							if named, yes := types.Unalias(typ).(*types.Named); yes && named.Obj().Pkg() != nil {
								a.copy(from, "field:"+named.Obj().Pkg().Path()+"."+named.Obj().Name()+"."+id.Name, "declared field store", member)
							}
						}
					}
				} else if key, yes := pair.Key.(*ast.BasicLit); yes && key.Kind == token.STRING {
					name, _ := strconv.Unquote(key.Value)
					a.copy(from, cell+"/field:"+name, "map construction value", member)
				}
				if key, yes := pair.Key.(ast.Expr); yes {
					a.add(a.value(m, key), cell+"/key", "map key construction", member)
					if mapOrUnknown {
						a.sink(actual, "key construction", key)
					}
				}
			} else if v, yes := entry.(ast.Expr); yes {
				from := a.value(m, v)
				a.copy(from, cell+"/element", "slice construction", member)
				a.aggregateContent(from, cell, member)
			}
		}
	}
	// The shared typed field product also carries its names when a struct
	// value is passed through a callback or collection as a whole. SSA can
	// represent its field stores separately from that aggregate value.
	if m.Syntax.Info != nil {
		if typ := m.Syntax.Info.TypeOf(e); typ != nil {
			if named, ok := types.Unalias(typ).(*types.Named); ok && named.Obj().Pkg() != nil {
				if structure, ok := named.Underlying().(*types.Struct); ok {
					for index := 0; index < structure.NumFields(); index++ {
						field := "field:" + named.Obj().Pkg().Path() + "." + named.Obj().Name() + "." + structure.Field(index).Name()
						a.aggregateContent(field, cell, member)
					}
				}
			}
		}
	}
	return cell
}

// A comparable struct/array can itself be a registry key. Its contained names
// therefore remain identity candidates when the aggregate is passed, returned
// or used as a whole, in addition to its more precise field/element edges.
func (a *analysis) aggregateContent(from, to, site string) {
	for _, channel := range []string{"", "/key", "/element"} {
		a.add(from+channel, to, "aggregate contains name", site)
	}
}

func (a *analysis) sink(m workcensus.Member, kind string, args ...ast.Expr) {
	for _, arg := range args {
		a.inputs[m.ID] = append(a.inputs[m.ID], a.value(m, arg))
	}
	a.kinds[m.ID] = kind
}
func callLeaf(e ast.Expr) string {
	switch n := e.(type) {
	case *ast.Ident:
		return n.Name
	case *ast.SelectorExpr:
		return n.Sel.Name
	case *ast.IndexExpr:
		return callLeaf(n.X)
	case *ast.IndexListExpr:
		return callLeaf(n.X)
	}
	return "<function value>"
}
func (a *analysis) call(m workcensus.Member, n *ast.CallExpr, cell string) {
	var args []string
	for _, arg := range n.Args {
		args = append(args, a.value(m, arg))
	}
	a.inputs[m.ID] = append(a.inputs[m.ID], args...)
	a.kinds[m.ID] = "call input/transform/emit"
	leaf := callLeaf(n.Fun)
	if selector, ok := n.Fun.(*ast.SelectorExpr); ok {
		a.inputs[m.ID] = append(a.inputs[m.ID], a.value(m, selector.X))
	}
	targets := m.Callees
	if len(targets) == 0 && !a.compiled {
		file, _, _ := strings.Cut(m.ID, ":")
		directory := ""
		if i := strings.LastIndex(file, "/"); i >= 0 {
			directory = "/" + file[:i]
		}
		target := "github.com/orneryd/nornicdb" + directory + "." + leaf
		if selector, ok := n.Fun.(*ast.SelectorExpr); ok {
			if qualifier, ok := selector.X.(*ast.Ident); ok {
				if path := a.imports[file][qualifier.Name]; path != "" {
					target = path + "." + selector.Sel.Name
				}
			}
		}
		targets = []string{target}
	}
	barrier := false
	for _, p := range a.contracts.Calls {
		for _, target := range targets {
			if target == p.Symbol {
				out := cell
				if p.Result > 0 {
					out += fmt.Sprintf("#%d", p.Result)
				}
				a.source(out, p.Role, p.Symbol, m.ID)
				a.copy(out, cell+fmt.Sprintf("#%d", p.Result), "decoded result slot", m.ID)
				barrier = true
			}
		}
	}
	for _, writer := range a.contracts.Writers {
		for _, target := range targets {
			barrier = barrier || target == writer
		}
	}
	resolved := false
	for _, target := range targets {
		if f, ok := a.functions[target]; ok {
			resolved = true
			for i, arg := range args {
				if i < len(f.params) {
					a.copy(arg, f.params[i], "actual to parameter", m.ID)
				}
			}
			if f.receiver != "" {
				if selector, ok := n.Fun.(*ast.SelectorExpr); ok {
					a.copy(a.value(m, selector.X), f.receiver, "receiver", m.ID)
				}
			}
			if !barrier {
				for i, result := range f.returns {
					out := cell + fmt.Sprintf("#%d", i)
					a.copy(result, out, "result to caller", m.ID)
					if i == 0 {
						a.copy(out, cell, "first result", m.ID)
					}
				}
			}
		}
	}
	if !resolved && !barrier {
		stringResult := true
		if a.builtin(m, n) {
			switch leaf {
			case "len", "cap", "copy", "make", "new", "delete", "clear", "close", "panic", "print", "println", "real", "imag", "complex", "recover":
				stringResult = false
			}
		}
		if m.Syntax.Info != nil {
			if typ := m.Syntax.Info.TypeOf(n); typ != nil {
				if basic, ok := typ.Underlying().(*types.Basic); ok {
					stringResult = basic.Info()&types.IsString != 0
				}
			}
		}
		if stringResult {
			for _, arg := range args {
				a.copy(arg, cell, "opaque/string-call result candidate", m.ID)
			}
		}
		if selector, ok := n.Fun.(*ast.SelectorExpr); ok {
			receiver := a.value(m, selector.X)
			if stringResult {
				a.copy(receiver, cell, "opaque receiver result candidate", m.ID)
			}
			a.inputs[m.ID] = append(a.inputs[m.ID], receiver)
		}
	}
	if leaf == "append" && a.builtin(m, n) {
		for i, arg := range args {
			if i == 0 || n.Ellipsis.IsValid() {
				a.copy(arg, cell, "append base or spread", m.ID)
			} else {
				a.copy(arg, cell+"/element", "append element", m.ID)
			}
		}
	}
	if leaf == "copy" && len(args) == 2 && a.builtin(m, n) {
		a.copy(args[1], a.lvalue(m, n.Args[0]), "copy destination storage", m.ID)
	}
	if selector, ok := n.Fun.(*ast.SelectorExpr); ok {
		for _, target := range targets {
			switch target {
			case "(*strings.Builder).WriteString", "(*strings.Builder).WriteByte", "(*strings.Builder).WriteRune", "(*bytes.Buffer).WriteString", "(*bytes.Buffer).Write", "(*bytes.Buffer).WriteByte", "(*bytes.Buffer).WriteRune":
				if len(args) > 0 {
					a.copy(args[0], a.lvalue(m, selector.X)+"/content", "builder content store", m.ID)
				}
			case "(*strings.Builder).String", "(*bytes.Buffer).String", "(*bytes.Buffer).Bytes":
				a.copy(a.lvalue(m, selector.X)+"/content", cell, "builder content result", m.ID)
			}
		}
	}
	// Dynamic/function-valued calls with unavailable target bodies remain visible.
	if len(m.Callees) == 0 && !resolved && !barrier && !a.builtin(m, n) {
		a.unknown[m.ID] = true
	}
}

func (a *analysis) builtin(m workcensus.Member, n *ast.CallExpr) bool {
	id, ok := n.Fun.(*ast.Ident)
	if !ok {
		return false
	}
	if m.Syntax.Info != nil {
		_, ok := m.Syntax.Info.ObjectOf(id).(*types.Builtin)
		return ok
	}
	if id.Obj != nil || a.shadowed[sourceOwner(m)+":"+id.Name] {
		return false
	}
	file, _, _ := strings.Cut(m.ID, ":")
	directory := ""
	if i := strings.LastIndex(file, "/"); i >= 0 {
		directory = "/" + file[:i]
	}
	_, local := a.functions["github.com/orneryd/nornicdb"+directory+"."+id.Name]
	_, declared := types.Universe.Lookup(id.Name).(*types.Builtin)
	return !local && declared
}

func (a *analysis) finish() Result {
	forward := map[string][]Edge{}
	for _, edge := range a.edges {
		forward[edge.From] = append(forward[edge.From], edge)
	}
	seen := map[string]bool{}
	parent := map[string]Edge{}
	var queue []string
	for _, p := range a.producers {
		if !seen[p.Cell] {
			seen[p.Cell] = true
			queue = append(queue, p.Cell)
		}
	}
	for i := 0; i < len(queue); i++ {
		for _, edge := range forward[queue[i]] {
			if !seen[edge.To] {
				seen[edge.To] = true
				parent[edge.To] = edge
				queue = append(queue, edge.To)
			}
		}
	}
	out := Result{Inventory: a.input, Producers: a.producers}
	out.Inventory.Limitations = append(append([]string(nil), a.input.Limitations...),
		"nameflow candidates use context-insensitive return/declared-field unions and a field-name union for source-only structs; each candidate requires producer/caller semantic review")
	for symbol, f := range a.functions {
		for i, cell := range f.returns {
			if seen[cell] {
				out.Producers = append(out.Producers, Producer{Cell: cell, Role: "derived decoded-identity return candidate", Contract: fmt.Sprintf("%s result %d", symbol, i)})
			}
		}
	}
	out.Inventory.Members = nil
	retainedEdges := map[Edge]bool{}
	for _, m := range a.input.Members {
		var inputs []string
		for _, cell := range a.inputs[m.ID] {
			for _, channel := range []string{"", "/key", "/element"} {
				if seen[cell+channel] {
					inputs = append(inputs, cell+channel)
				}
			}
		}
		if len(inputs) == 0 {
			continue
		}
		m.Facts = map[string][]string{"flow sink": {a.kinds[m.ID]}, "decoded inputs": inputs}
		if m.Syntax != nil {
			switch n := m.Syntax.Node.(type) {
			case *ast.BinaryExpr:
				m.Facts["operator"] = []string{n.Op.String()}
				if m.Syntax.Info != nil {
					for _, operand := range []ast.Expr{n.X, n.Y} {
						if typ := m.Syntax.Info.TypeOf(operand); typ != nil {
							m.Facts["operand types"] = append(m.Facts["operand types"], typ.String())
						}
					}
				}
			case *ast.IndexExpr:
				if m.Syntax.Info != nil {
					if typ := m.Syntax.Info.TypeOf(n.X); typ != nil {
						m.Facts["indexed type"] = []string{typ.String()}
					}
				}
			}
		}
		m.Class = ""
		m.Reason = ""
		out.Inventory.Members = append(out.Inventory.Members, m)
		for _, cell := range inputs {
			for {
				edge, ok := parent[cell]
				if !ok || retainedEdges[edge] {
					break
				}
				retainedEdges[edge] = true
				cell = edge.From
			}
		}
		if a.unknown[m.ID] {
			out.Unresolved = append(out.Unresolved, m.ID)
		}
	}
	for edge := range retainedEdges {
		out.Edges = append(out.Edges, edge)
	}
	sort.Slice(out.Edges, func(i, j int) bool { return fmt.Sprint(out.Edges[i]) < fmt.Sprint(out.Edges[j]) })
	sort.Slice(out.Producers, func(i, j int) bool { return fmt.Sprint(out.Producers[i]) < fmt.Sprint(out.Producers[j]) })
	sort.Strings(out.Unresolved)
	return out
}

// Config selects source facts independently of the prior manifest and callers.
// Untyped/inactive and peer boundaries remain engine-reported source facts.
func Config(dir string) workcensus.Config {
	c := workcensus.Config{Dir: dir, Patterns: []string{"./..."}, BuildFlags: []string{"-tags=noui,nolocalllm"}, IncludeSyntax: true, IncludeProgram: true, IncludeInactive: true, ExcludeGenerated: true, Packages: []string{"github.com/orneryd/nornicdb"}}
	exclusions := []string{".git", "vendor", "node_modules", "dist", "build", "testdata", "testutil", ".tn-operator", ".tn-review"}
	c.SourceRoots = []workcensus.SourceRoot{{Path: ".", Languages: map[string]string{".go": "go"}, ExcludeDirs: exclusions, ExcludeFiles: []string{"*_test.go"}}}
	for _, kind := range []string{"declaration", "import", "assign", "var", "range", "call", "identifier", "index", "slice", "field", "binary", "return", "composite", "type-assert"} {
		c.Selectors = append(c.Selectors, workcensus.Selector{Kind: kind, Node: kind, Reachability: "all"})
	}
	return c
}
