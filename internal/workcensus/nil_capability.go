package workcensus

import (
	"go/token"
	"go/types"

	"golang.org/x/tools/go/ssa"
)

// initializerCapabilities is a finite source-region invariant, not a heap
// interpreter. Imported code is opaque under the approved Go import-DAG model:
// without an exposed owner value/callable/type capability a dependency cannot
// forge entry to its importer. Native symbol/address synthesis is outside that
// model. No package, method, constructor or builtin name is a trust allowlist.
type initializerCapabilities struct {
	flow         *nilFlow
	fact         *globalInitializer
	reader       string
	instructions []ssa.Instruction
	uses         map[ssa.Value][]ssa.Instruction
	calls        map[*ssa.Call]*ssa.Function
	incoming     map[*ssa.Function][]*ssa.Call
	returns      map[*ssa.Function][]*ssa.Return
	slots        map[*ssa.Global][]*ssa.Store
	globals      map[string]*ssa.Global
	nodes        map[ssa.Value]*initializerRegion
	contents     map[ssa.Value][]ssa.Value
	protected    map[ssa.Value]bool
	escapes      map[ssa.Value][]ssa.Instruction
	data         map[types.Type]bool
	owned        map[types.Type]bool
	dependencies map[string]bool
}

type initializerRegion struct {
	flags uint8
	roots map[ssa.Value]bool
	edges []ssa.Value
}

const (
	initScalar uint8 = 1 << iota
	initNil
	initForeign
	initBorrowed
	initUnknown
)

func initializerFunctionPackage(fn *ssa.Function) *types.Package {
	if fn.Pkg != nil {
		return fn.Pkg.Pkg
	}
	if origin := fn.Origin(); origin != nil && origin.Pkg != nil {
		return origin.Pkg.Pkg
	}
	if obj := fn.Object(); obj != nil {
		return obj.Pkg()
	}
	return nil
}

func (c *initializerCapabilities) global(g *ssa.Global) *ssa.Global {
	key := g.String()
	if old := c.globals[key]; old != nil {
		return old
	}
	c.globals[key] = g
	return g
}

// dataOnly inspects named identities AND both method sets before descending
// through every field/element. Recursive or unknown shapes fail closed.
func (c *initializerCapabilities) dataOnly(t types.Type) bool {
	if answer, ok := c.data[t]; ok {
		return answer
	}
	c.data[t] = false
	switch x := t.(type) {
	case *types.Alias:
		c.data[t] = c.dataOnly(types.Unalias(x))
	case *types.Named:
		if types.NewMethodSet(x).Len() != 0 || types.NewMethodSet(types.NewPointer(x)).Len() != 0 {
			return false
		}
		for i := 0; i < x.TypeArgs().Len(); i++ {
			if !c.dataOnly(x.TypeArgs().At(i)) {
				return false
			}
		}
		c.data[t] = c.dataOnly(x.Underlying())
	case *types.Basic:
		c.data[t] = x.Info()&(types.IsBoolean|types.IsInteger|types.IsFloat|types.IsComplex|types.IsString) != 0
	case *types.Array:
		c.data[t] = c.dataOnly(x.Elem())
	case *types.Slice:
		c.data[t] = c.dataOnly(x.Elem())
	case *types.Struct:
		for i := 0; i < x.NumFields(); i++ {
			if !c.dataOnly(x.Field(i).Type()) {
				return false
			}
		}
		c.data[t] = true
	}
	return c.data[t]
}

// ownedType retains owner identities even for primitive-shaped records, typed
// nils, interface boxing, reflect type arguments and nested generic arguments.
func (c *initializerCapabilities) ownedType(t types.Type) bool {
	if answer, ok := c.owned[t]; ok {
		return answer
	}
	c.owned[t] = false
	var result bool
	switch x := t.(type) {
	case *types.Alias:
		result = c.ownedType(types.Unalias(x))
	case *types.Named:
		result = x.Obj().Pkg() != nil && x.Obj().Pkg().Path() == c.fact.owner.Pkg.Path()
		for i := 0; i < x.TypeArgs().Len(); i++ {
			result = result || c.ownedType(x.TypeArgs().At(i))
		}
		// A dependency's named definition cannot reference its importer.
		// Instantiation arguments can, so those are always inspected above.
		// Its implementation layout is NOT owner provenance (opaque reflect/
		// runtime values may contain foreign unsafe internals). Locally supplied
		// contents are checked independently by region stores and safeExternal.
	case *types.Basic:
		result = x.Kind() == types.UnsafePointer
	case *types.Pointer:
		result = c.ownedType(x.Elem())
	case *types.Array:
		result = c.ownedType(x.Elem())
	case *types.Slice:
		result = c.ownedType(x.Elem())
	case *types.Map:
		result = c.ownedType(x.Key()) || c.ownedType(x.Elem())
	case *types.Chan:
		result = c.ownedType(x.Elem())
	case *types.Struct:
		for i := 0; i < x.NumFields(); i++ {
			result = result || c.ownedType(x.Field(i).Type())
		}
	case *types.Signature:
		if x.Recv() != nil {
			result = c.ownedType(x.Recv().Type())
		}
		result = result || c.ownedType(x.Params()) || c.ownedType(x.Results())
	case *types.Interface:
		// Anonymous interfaces can expose owner types through method signatures
		// without carrying a concrete receiver (including through typed nils).
		// Their locally declared methods also retain owner interface identity.
		for i := 0; i < x.NumMethods(); i++ {
			method := x.Method(i)
			result = result || (method.Pkg() != nil && method.Pkg().Path() == c.fact.owner.Pkg.Path()) || c.ownedType(method.Type())
		}
		for i := 0; i < x.NumEmbeddeds(); i++ {
			result = result || c.ownedType(x.EmbeddedType(i))
		}
	case *types.Tuple:
		for i := 0; i < x.Len(); i++ {
			result = result || c.ownedType(x.At(i).Type())
		}
	case *types.TypeParam, *types.Union:
		result = true
	}
	c.owned[t] = result
	return result
}

func initializerCallableType(t types.Type, seen map[types.Type]bool) bool {
	if seen[t] {
		return false
	}
	seen[t] = true
	switch x := t.Underlying().(type) {
	case *types.Signature:
		return true
	case *types.Pointer:
		return initializerCallableType(x.Elem(), seen)
	case *types.Array:
		return initializerCallableType(x.Elem(), seen)
	case *types.Slice:
		return initializerCallableType(x.Elem(), seen)
	case *types.Map:
		return initializerCallableType(x.Key(), seen) || initializerCallableType(x.Elem(), seen)
	case *types.Struct:
		for i := 0; i < x.NumFields(); i++ {
			if initializerCallableType(x.Field(i).Type(), seen) {
				return true
			}
		}
	}
	return false
}

func (f *nilFlow) proveInitializerCapabilities(fact *globalInitializer, reader *ssa.Function) bool {
	c := newInitializerCapabilities(f, fact, reader)
	if !c.collect() {
		return false
	}
	c.solve()
	return c.validate()
}

func newInitializerCapabilities(f *nilFlow, fact *globalInitializer, reader *ssa.Function) *initializerCapabilities {
	c := &initializerCapabilities{flow: f, fact: fact, reader: reader.String(),
		uses: map[ssa.Value][]ssa.Instruction{}, calls: map[*ssa.Call]*ssa.Function{},
		incoming: map[*ssa.Function][]*ssa.Call{}, returns: map[*ssa.Function][]*ssa.Return{},
		slots: map[*ssa.Global][]*ssa.Store{}, globals: map[string]*ssa.Global{},
		nodes: map[ssa.Value]*initializerRegion{}, contents: map[ssa.Value][]ssa.Value{},
		protected: map[ssa.Value]bool{}, escapes: map[ssa.Value][]ssa.Instruction{}, data: map[types.Type]bool{}, owned: map[types.Type]bool{}, dependencies: map[string]bool{}}
	var visit func(*types.Package)
	visit = func(pkg *types.Package) {
		if c.dependencies[pkg.Path()] {
			return
		}
		c.dependencies[pkg.Path()] = true
		for _, dep := range pkg.Imports() {
			visit(dep)
		}
	}
	visit(fact.owner.Pkg)
	delete(c.dependencies, fact.owner.Pkg.Path())
	return c
}

func (c *initializerCapabilities) foreign(fn *ssa.Function) bool {
	pkg := initializerFunctionPackage(fn)
	return pkg != nil && c.dependencies[pkg.Path()]
}

func (c *initializerCapabilities) collect() bool {
	active, done := map[*ssa.Function]bool{}, map[*ssa.Function]bool{}
	var visit func(*ssa.Function, bool) bool
	visit = func(fn *ssa.Function, prefix bool) bool {
		if fn == nil || fn.String() == c.reader || active[fn] {
			return false
		}
		if done[fn] {
			return true
		}
		if len(fn.Blocks) == 0 {
			return false
		} // missing owned source is not opaque imported code
		if fn.Recover != nil {
			return false
		}
		active[fn] = true
		defer delete(active, fn)
		seen := map[*ssa.BasicBlock]bool{}
		var block func(*ssa.BasicBlock) bool
		block = func(b *ssa.BasicBlock) bool {
			if seen[b] {
				return true
			}
			seen[b] = true
			for _, ins := range b.Instrs {
				if prefix && ins == c.fact.store {
					return true
				}
				switch ins.(type) {
				case *ssa.DebugRef, *ssa.Alloc, *ssa.Store, *ssa.MapUpdate, *ssa.UnOp, *ssa.FieldAddr, *ssa.IndexAddr,
					*ssa.Slice, *ssa.Index, *ssa.Field, *ssa.Lookup, *ssa.Extract, *ssa.Range, *ssa.Next,
					*ssa.MakeMap, *ssa.MakeSlice, *ssa.MakeInterface, *ssa.ChangeInterface, *ssa.ChangeType,
					*ssa.Convert, *ssa.BinOp, *ssa.Phi, *ssa.If, *ssa.Jump, *ssa.Return, *ssa.Call:
				default:
					return false
				}
				c.instructions = append(c.instructions, ins)
				for _, op := range ins.Operands(nil) {
					if op != nil && *op != nil {
						v := *op
						if g, ok := v.(*ssa.Global); ok {
							v = c.global(g)
						}
						c.uses[v] = append(c.uses[v], ins)
					}
				}
				switch x := ins.(type) {
				case *ssa.Store:
					if g, ok := x.Addr.(*ssa.Global); ok {
						g = c.global(g)
						c.slots[g] = append(c.slots[g], x)
					}
				case *ssa.Return:
					c.returns[fn] = append(c.returns[fn], x)
				case *ssa.Call:
					if _, ok := x.Common().Value.(*ssa.Builtin); ok {
						continue
					}
					target := x.Common().StaticCallee()
					if target == nil {
						if !x.Common().IsInvoke() {
							return false
						}
						continue
					}
					if c.foreign(target) {
						continue
					}
					if len(target.Blocks) == 0 {
						target = c.flow.externalBody(target)
					}
					if target == nil {
						return false
					}
					c.calls[x] = target
					c.incoming[target] = append(c.incoming[target], x)
					if !visit(target, false) {
						return false
					}
				}
			}
			for _, succ := range b.Succs {
				if !block(succ) {
					return false
				}
			}
			return true
		}
		if !block(fn.Blocks[0]) {
			return false
		}
		done[fn] = true
		return true
	}
	return visit(c.fact.owner.Func("init"), true)
}

func (c *initializerCapabilities) node(v ssa.Value) *initializerRegion {
	if g, ok := v.(*ssa.Global); ok {
		v = c.global(g)
	}
	if n := c.nodes[v]; n != nil {
		return n
	}
	n := &initializerRegion{}
	c.nodes[v] = n
	add := func(v ssa.Value) { n.edges = append(n.edges, v); c.node(v) }
	foreign := func() { n.flags = initForeign }
	result := func(call *ssa.Call, index int) {
		if target := c.calls[call]; target != nil {
			for _, ret := range c.returns[target] {
				if index < len(ret.Results) {
					add(ret.Results[index])
				}
			}
		} else {
			foreign()
		}
	}
	switch x := v.(type) {
	case *ssa.Const:
		if x.IsNil() {
			n.flags = initNil
		} else {
			n.flags = initScalar
		}
	case *ssa.Function, *ssa.Builtin:
		n.flags = initScalar
	case *ssa.Global, *ssa.Alloc, *ssa.MakeMap, *ssa.MakeSlice:
		n.roots = map[ssa.Value]bool{v: true}
	case *ssa.Parameter:
		for _, call := range c.incoming[x.Parent()] {
			i := sourceParameterIndex(x) + len(call.Common().Args) - len(x.Parent().Params)
			if i >= 0 && i < len(call.Common().Args) {
				add(call.Common().Args[i])
			} else {
				n.flags |= initUnknown
			}
		}
	case *ssa.Phi:
		for _, edge := range x.Edges {
			add(edge)
		}
	case *ssa.ChangeType:
		add(x.X)
	case *ssa.ChangeInterface:
		add(x.X)
	case *ssa.MakeInterface:
		add(x.X)
	case *ssa.FieldAddr:
		add(x.X)
	case *ssa.IndexAddr:
		add(x.X)
	case *ssa.Slice:
		add(x.X)
	case *ssa.UnOp:
		if x.Op != token.MUL {
			n.flags = initUnknown
			break
		}
		if g, ok := x.X.(*ssa.Global); ok {
			g = c.global(g)
			if g.Pkg != nil && g.Pkg.Pkg.Path() != c.fact.owner.Pkg.Path() {
				foreign()
				break
			}
			for _, store := range c.slots[g] {
				add(store.Val)
			}
			switch x.Type().Underlying().(type) {
			case *types.Slice, *types.Map, *types.Pointer:
				n.flags |= initBorrowed
			}
		} else if c.dataOnly(x.Type()) {
			if _, slice := x.Type().Underlying().(*types.Slice); slice {
				n.flags = initBorrowed
			} else {
				n.flags = initScalar
			}
		} else {
			n.flags = initUnknown
			// A whole staging struct copy preserves its protected region.
			if _, ok := x.X.(*ssa.Alloc); ok {
				n.flags = 0
				add(x.X)
			}
		}
	case *ssa.Field:
		if c.dataOnly(x.Type()) {
			if _, ok := x.Type().Underlying().(*types.Slice); ok {
				n.flags = initBorrowed
			} else {
				n.flags = initScalar
			}
		} else {
			n.flags = initUnknown
		}
	case *ssa.Index:
		if c.dataOnly(x.Type()) {
			n.flags = initBorrowed
		} else {
			n.flags = initUnknown
		}
	case *ssa.Lookup:
		if c.dataOnly(x.Type()) {
			n.flags = initScalar
		} else {
			n.flags = initUnknown
		}
	case *ssa.Extract:
		if call, ok := x.Tuple.(*ssa.Call); ok {
			result(call, x.Index)
		} else if c.dataOnly(x.Type()) {
			n.flags = initScalar
		} else {
			n.flags = initBorrowed
		}
	case *ssa.Call:
		if builtin, ok := x.Common().Value.(*ssa.Builtin); ok {
			if builtin.Name() == "append" {
				n.roots = map[ssa.Value]bool{x: true}
				add(x.Common().Args[0])
			} else {
				n.flags = initScalar
			}
		} else {
			result(x, 0)
		}
	case *ssa.BinOp, *ssa.Convert:
		n.flags = initScalar
	case *ssa.Range, *ssa.Next:
		n.flags = initBorrowed
	default:
		n.flags = initUnknown
	}
	return n
}

func (c *initializerCapabilities) solve() {
	for _, ins := range c.instructions {
		if v, ok := ins.(ssa.Value); ok {
			c.node(v)
		}
		for _, op := range ins.Operands(nil) {
			if op != nil && *op != nil {
				c.node(*op)
			}
		}
	}
	// Each bit/root can only be added once. This computes seeded loop phi
	// unions without treating an unseeded provenance cycle as a source.
	for changed := true; changed; {
		changed = false
		for _, n := range c.nodes {
			for _, edge := range n.edges {
				e := c.node(edge)
				flags := n.flags | e.flags
				if flags != n.flags {
					changed = true
					n.flags = flags
				}
				for root := range e.roots {
					if !n.roots[root] {
						if n.roots == nil {
							n.roots = map[ssa.Value]bool{}
						}
						n.roots[root] = true
						changed = true
					}
				}
			}
		}
	}
	for _, ins := range c.instructions {
		var addr, val ssa.Value
		switch x := ins.(type) {
		case *ssa.Store:
			addr, val = x.Addr, x.Val
		case *ssa.MapUpdate:
			addr, val = x.Map, x.Value
		}
		if addr != nil {
			for root := range c.node(addr).roots {
				c.contents[root] = append(c.contents[root], val)
				if initializerCallableType(val.Type(), map[types.Type]bool{}) {
					c.protected[root] = true
				}
			}
		}
	}
	for _, node := range c.nodes {
		for root := range node.roots {
			if initializerCallableType(root.Type(), map[types.Type]bool{}) {
				c.protected[root] = true
			}
		}
	}
}

func (c *initializerCapabilities) local(v ssa.Value) bool {
	n := c.node(v)
	if n.flags&^(initNil) != 0 {
		return false
	}
	if len(n.roots) == 0 && n.flags != initNil {
		return false
	}
	if !c.seeded(v, map[ssa.Value]bool{}) {
		return false
	}
	for root := range n.roots {
		switch r := root.(type) {
		case *ssa.Alloc, *ssa.MakeMap, *ssa.MakeSlice, *ssa.Call:
		case *ssa.Global:
			if r.Pkg == nil || r.Pkg.Pkg.Path() != c.fact.owner.Pkg.Path() {
				return false
			}
		default:
			return false
		}
	}
	return true
}

// An append's possible fresh backing is not a provenance seed for its own
// destination. In particular a circular append/phi cannot justify itself.
func (c *initializerCapabilities) seeded(v ssa.Value, seen map[ssa.Value]bool) bool {
	if seen[v] {
		return false
	}
	seen[v] = true
	switch x := v.(type) {
	case *ssa.Alloc, *ssa.MakeMap, *ssa.MakeSlice, *ssa.Global:
		return true
	case *ssa.Const:
		return x.IsNil()
	}
	for _, edge := range c.node(v).edges {
		if c.seeded(edge, seen) {
			return true
		}
	}
	return false
}

func (c *initializerCapabilities) indexEscapes() {
	add := func(v ssa.Value, use ssa.Instruction) {
		for root := range c.node(v).roots {
			c.escapes[root] = append(c.escapes[root], use)
		}
	}
	for _, ins := range c.instructions {
		switch x := ins.(type) {
		case *ssa.Call:
			if _, builtin := x.Common().Value.(*ssa.Builtin); builtin || c.calls[x] != nil {
				continue
			}
			for _, arg := range x.Common().Args {
				add(arg, x)
			}
			if x.Common().IsInvoke() {
				add(x.Common().Value, x)
			}
		case *ssa.Store:
			if g, ok := x.Addr.(*ssa.Global); ok && !c.private(g) {
				add(x.Val, x)
			}
		}
	}
}

func (c *initializerCapabilities) writable(v ssa.Value, store ssa.Instruction) bool {
	if !c.local(v) {
		return false
	}
	for root := range c.node(v).roots {
		for _, escape := range c.escapes[root] {
			// Only finite one-way staging writes may precede an outward use.
			// Loops or incomparable helper contexts are not a temporal heap proof.
			if store.Parent() != escape.Parent() || !store.Block().Dominates(escape.Block()) {
				return false
			}
			if store.Block() == escape.Block() && instructionIndex(store) >= instructionIndex(escape) {
				return false
			}
			seen := map[*ssa.BasicBlock]bool{}
			var reaches func(*ssa.BasicBlock) bool
			reaches = func(b *ssa.BasicBlock) bool {
				if b == store.Block() {
					return true
				}
				if seen[b] {
					return false
				}
				seen[b] = true
				for _, succ := range b.Succs {
					if reaches(succ) {
						return true
					}
				}
				return false
			}
			for _, succ := range escape.Block().Succs {
				if reaches(succ) {
					return false
				}
			}
		}
	}
	return true
}

func (c *initializerCapabilities) safeExternal(v ssa.Value, active map[ssa.Value]bool) bool {
	if c.ownedType(v.Type()) {
		return false
	}
	if _, ok := v.Type().Underlying().(*types.Signature); ok {
		return false
	}
	n := c.node(v)
	if n.flags&initUnknown != 0 || n.flags == 0 && len(n.roots) == 0 {
		return false
	}
	if active[v] {
		return true
	}
	active[v] = true
	defer delete(active, v)
	switch x := v.(type) {
	case *ssa.MakeInterface:
		return c.safeExternal(x.X, active)
	case *ssa.ChangeInterface:
		return c.safeExternal(x.X, active)
	case *ssa.ChangeType:
		return c.safeExternal(x.X, active)
	case *ssa.Phi:
		for _, edge := range x.Edges {
			if !c.safeExternal(edge, active) {
				return false
			}
		}
	}
	for root := range n.roots {
		if g, global := root.(*ssa.Global); global && g.Pkg != nil && g.Pkg.Pkg.Path() == c.fact.owner.Pkg.Path() {
			return false
		}
		if c.protected[root] {
			return false
		}
		for _, val := range c.contents[root] {
			if !c.safeExternal(val, active) {
				return false
			}
		}
	}
	return true
}

func (c *initializerCapabilities) private(g *ssa.Global) bool {
	return g.Pkg != nil && g.Pkg.Pkg.Path() == c.fact.owner.Pkg.Path() && g.Object() != nil && !g.Object().Exported()
}

func (c *initializerCapabilities) exactCallable(v ssa.Value, seen map[ssa.Value]bool) bool {
	if seen[v] {
		return true
	}
	seen[v] = true
	if _, ok := v.(*ssa.Function); ok {
		return true
	}
	n := c.node(v)
	if n.flags != initScalar || len(n.roots) != 0 || len(n.edges) == 0 {
		return false
	}
	for _, edge := range n.edges {
		if !c.exactCallable(edge, seen) {
			return false
		}
	}
	return true
}

// before requires a root initializer store to precede every possible prefix
// call ancestry of a read. No global phase is inferred for an owned function.
func (c *initializerCapabilities) before(store *ssa.Store, use ssa.Instruction) bool {
	if use.Parent() == store.Parent() {
		return store.Block().Dominates(use.Block()) && (store.Block() != use.Block() || instructionIndex(store) < instructionIndex(use))
	}
	calls := c.incoming[use.Parent()]
	if len(calls) == 0 {
		return false
	}
	for _, call := range calls {
		if !c.before(store, call) {
			return false
		}
	}
	return true
}

func (c *initializerCapabilities) boxToSentinel(v ssa.Value, active map[ssa.Value]bool) bool {
	if active[v] {
		return false
	}
	active[v] = true
	defer delete(active, v)
	uses := c.uses[v]
	if uses == nil && v != c.fact.store.Val {
		return false
	}
	for _, use := range uses {
		switch x := use.(type) {
		case *ssa.DebugRef:
		case *ssa.ChangeInterface:
			if !c.boxToSentinel(x, active) {
				return false
			}
		case *ssa.Return:
			if len(x.Results) != 1 || x.Results[0] != v {
				return false
			}
			for _, call := range c.incoming[x.Parent()] {
				// The sentinel store is deliberately outside the prefix index.
				if call == c.fact.store.Val {
					continue
				}
				if !c.boxToSentinel(call, active) {
					return false
				}
			}
		default:
			return false
		}
	}
	return true
}

func (c *initializerCapabilities) validate() bool {
	c.indexEscapes()
	for _, ins := range c.instructions {
		switch x := ins.(type) {
		case *ssa.Field, *ssa.Index, *ssa.Lookup, *ssa.Range, *ssa.BinOp:
			for _, op := range ins.Operands(nil) {
				if op != nil && *op != nil {
					for root := range c.node(*op).roots {
						if c.protected[root] {
							return false
						}
					}
				}
			}
		case *ssa.Convert:
			for _, typ := range []types.Type{x.Type(), x.X.Type()} {
				if b, ok := typ.Underlying().(*types.Basic); ok && b.Kind() == types.UnsafePointer {
					return false
				}
			}
		case *ssa.MakeInterface:
			for root := range c.node(x.X).roots {
				if c.protected[root] {
					return false
				}
			}
			if _, callable := x.X.Type().Underlying().(*types.Signature); callable {
				return false
			}
			if c.ownedType(x.X.Type()) && !c.boxToSentinel(x, map[ssa.Value]bool{}) {
				return false
			}
		case *ssa.Store:
			if _, callable := x.Val.Type().Underlying().(*types.Signature); callable && !c.exactCallable(x.Val, map[ssa.Value]bool{}) {
				return false
			}
			if _, callable := x.Val.Type().Underlying().(*types.Signature); !callable && initializerCallableType(x.Val.Type(), map[types.Type]bool{}) && !c.local(x.Val) {
				return false
			}
			if !c.writable(x.Addr, x) {
				return false
			}
			for root := range c.node(x.Val).roots {
				if _, global := root.(*ssa.Global); global {
					return false
				}
			}
			if g, ok := x.Addr.(*ssa.Global); ok {
				if !c.private(g) && !c.safeExternal(x.Val, map[ssa.Value]bool{}) {
					return false
				}
			}
		case *ssa.MapUpdate:
			for _, value := range []ssa.Value{x.Key, x.Value} {
				if _, callable := value.Type().Underlying().(*types.Signature); callable && !c.exactCallable(value, map[ssa.Value]bool{}) {
					return false
				}
			}
			if _, callable := x.Value.Type().Underlying().(*types.Signature); !callable && initializerCallableType(x.Value.Type(), map[types.Type]bool{}) && !c.local(x.Value) {
				return false
			}
			if !c.writable(x.Map, x) {
				return false
			}
		case *ssa.UnOp:
			if x.Op != token.MUL {
				return false
			}
			if g, ok := x.X.(*ssa.Global); ok {
				g = c.global(g)
				if g.Pkg != nil && g.Pkg.Pkg.Path() != c.fact.owner.Pkg.Path() {
					continue
				}
				stores := c.slots[g]
				// Compiler-synthesized boolean initialization control has no
				// object, nilable value, owner identity or container capability.
				// No source global or package/symbol name is exempted.
				if g.Object() == nil && x.Type() == types.Typ[types.Bool] {
					continue
				}
				if len(stores) != 1 || !c.before(stores[0], x) {
					return false
				}
				readable := c.dataOnly(x.Type())
				if m, ok := x.Type().Underlying().(*types.Map); ok {
					readable = c.dataOnly(m.Key()) && c.dataOnly(m.Elem())
				}
				if _, ok := x.Type().Underlying().(*types.Interface); ok {
					readable = c.safeExternal(x, map[ssa.Value]bool{})
				}
				if !readable {
					return false
				} // sealed globals may not be read
			} else {
				for root := range c.node(x.X).roots {
					if !c.protected[root] {
						continue
					}
					semantic := 0
					for _, use := range c.uses[x] {
						if _, debug := use.(*ssa.DebugRef); !debug {
							semantic++
						}
					}
					if semantic == 0 {
						if _, fresh := root.(*ssa.Alloc); !fresh || !c.local(x.X) {
							return false
						}
						switch x.Type().Underlying().(type) {
						case *types.Array, *types.Struct:
						default:
							return false
						}
						continue
					}
					alloc, whole := x.X.(*ssa.Alloc)
					if !whole || root != alloc {
						return false
					}
					for _, use := range c.uses[x] {
						store, ok := use.(*ssa.Store)
						if !ok {
							return false
						}
						g, ok := store.Addr.(*ssa.Global)
						if !ok || !c.private(g) || store.Val != x {
							return false
						}
					}
				}
			}
		case *ssa.Call:
			if b, ok := x.Common().Value.(*ssa.Builtin); ok {
				switch b.Name() {
				case "append":
					if len(x.Common().Args) != 2 || !c.local(x.Common().Args[0]) || !c.dataOnly(x.Type()) {
						return false
					}
					for root := range c.node(x.Common().Args[0]).roots {
						if _, global := root.(*ssa.Global); global {
							return false
						}
						if len(c.escapes[root]) != 0 {
							return false
						}
					}
					source := c.node(x.Common().Args[1])
					if source.flags&initUnknown != 0 || source.flags == 0 && len(source.roots) == 0 {
						return false
					}
					for root := range source.roots {
						if _, global := root.(*ssa.Global); global {
							return false
						}
					}
				case "len", "cap", "delete", "max", "min":
					for _, arg := range x.Common().Args {
						for root := range c.node(arg).roots {
							if c.protected[root] {
								return false
							}
						}
					}
				default:
					return false // includes copy, unsafe and unmodelled builtins
				}
				continue
			}
			if target := c.calls[x]; target != nil {
				for _, arg := range x.Common().Args {
					for root := range c.node(arg).roots {
						if _, global := root.(*ssa.Global); global {
							return false
						}
					}
				}
				continue
			}
			if x.Common().IsInvoke() {
				method := x.Common().Method
				if method == nil || method.Pkg() == nil || method.Pkg().Path() == c.fact.owner.Pkg.Path() {
					return false
				}
				receiver := c.node(x.Common().Value)
				if receiver.flags != initForeign || len(receiver.roots) != 0 || !c.safeExternal(x.Common().Value, map[ssa.Value]bool{}) {
					return false
				}
			} else {
				target := x.Common().StaticCallee()
				if target == nil || !c.foreign(target) {
					return false
				}
				for _, typ := range target.TypeArgs() {
					if c.ownedType(typ) {
						return false
					}
				}
			}
			for _, arg := range x.Common().Args {
				if !c.safeExternal(arg, map[ssa.Value]bool{}) {
					return false
				}
			}
		}
	}
	return true
}
