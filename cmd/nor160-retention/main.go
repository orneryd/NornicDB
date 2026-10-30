// NOR-160 lane (Claude), 30 October 2026. Analysis-only client, not product code.
// Retention-point population over the maintained workcensus typed/SSA loader.
// A retention point is a holder (receiver struct or package global) whose field keeps
// a reference-bearing value. The query follows compiled flow from such a field to a
// value an exported function returns or delivers to a caller's callback, through
// value-preserving steps, closures, shallow containers and interprocedural summaries.
// A second pass reports every copier applied to a value that is already caller-owned.
// No loader, SSA builder or call graph is copied; no product code executes.
package main

import (
	"encoding/json"
	"fmt"
	"go/token"
	"go/types"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"

	"github.com/orneryd/nornicdb/internal/workcensus"
	"golang.org/x/tools/go/ssa"
)

const module = "github.com/orneryd/nornicdb"

// Retention kind controls, compiled as a virtual file of pkg/storage. Each must be
// reported as a retention exit (or, for the last, as a double copy).
const controlFile = `package storage

import "github.com/orneryd/nornicdb/pkg/callerowned"

type NOR160ControlCache struct{ m map[string]*Node }

func (c *NOR160ControlCache) Get(k string) *Node { return c.m[k] }

func (c *NOR160ControlCache) Visit(fn func(*Node)) {
	for _, n := range c.m {
		fn(n)
	}
}

type NOR160ControlReplay struct{ rows [][]any }

func (r *NOR160ControlReplay) Page(i, j int) [][]any { return r.rows[i:j] }

type NOR160ControlSecondary struct{ byLabel map[string][]*Node }

func (s *NOR160ControlSecondary) Label(l string) []*Node {
	out := make([]*Node, 0)
	for _, n := range s.byLabel[l] {
		out = append(out, n)
	}
	return out
}

type NOR160ControlHit struct {
	props  map[string]any
	labels []string
}

func (h NOR160ControlHit) Materialize() *Node { return &Node{Labels: h.labels, Properties: h.props} }

type NOR160ControlOverlay struct{ pending map[NodeID]*Node }

func (o *NOR160ControlOverlay) Read(id NodeID, fallback func(NodeID) *Node) *Node {
	if n, ok := o.pending[id]; ok {
		return n
	}
	return fallback(id)
}

type NOR160ControlPager interface{ Page(i, j int) [][]any }

type NOR160ControlRegistry struct{ s map[string]NOR160ControlPager }

func NewNOR160ControlRegistry(p *NOR160ControlReplay) *NOR160ControlRegistry {
	return &NOR160ControlRegistry{s: map[string]NOR160ControlPager{"a": p}}
}

func (r *NOR160ControlRegistry) Pull(id string) [][]any { return r.s[id].Page(0, 1) }

// v5 paired controls. Shell: copies a record and replaces every exported
// reference field through the owner (clean) / leaves one shared (flagged).
type NOR160ControlRecord struct {
	Node *Node
	Tags []string
}

type NOR160ControlRecords struct{ m map[string]*NOR160ControlRecord }

func nor160ShellOK(r *NOR160ControlRecord) *NOR160ControlRecord {
	out := *r
	out.Node = copyNode(r.Node)
	out.Tags = callerowned.Strings(r.Tags)
	return &out
}

func nor160ShellLeaky(r *NOR160ControlRecord) *NOR160ControlRecord {
	out := *r
	out.Node = copyNode(r.Node)
	return &out
}

func (c *NOR160ControlRecords) GetOK(k string) *NOR160ControlRecord { return nor160ShellOK(c.m[k]) }

func (c *NOR160ControlRecords) GetLeaky(k string) *NOR160ControlRecord { return nor160ShellLeaky(c.m[k]) }

// Strong update: a captured local overwritten by a copy before it is
// returned (clean) / re-stored with the retained image after the copy (flagged).
func (c *NOR160ControlCache) GetReassigned(k string) *Node {
	var n *Node
	read := func() { n = c.m[k] }
	read()
	n = copyNode(n)
	return n
}

func (c *NOR160ControlCache) GetReassignedLeaky(k string) *Node {
	var n *Node
	n = copyNode(c.m[k])
	read := func() { n = c.m[k] }
	read()
	return n
}

func NOR160ControlDoubleCopy(n *Node) *Node {
	owned := copyNode(n)
	return copyNode(owned)
}
`

var controls = []string{"NOR160ControlCache).Get", "NOR160ControlCache).Visit", "NOR160ControlReplay).Page", "NOR160ControlSecondary).Label", "NOR160ControlHit).Materialize", "NOR160ControlOverlay).Read", "NOR160ControlRegistry).Pull", "NOR160ControlRecords).GetLeaky", "NOR160ControlCache).GetReassignedLeaky"}

// Controls that must NOT be reported: a correct shell and a strong update.
var cleanControls = []string{"NOR160ControlRecords).GetOK", "NOR160ControlCache).GetReassigned"}

const doubleControl = "NOR160ControlDoubleCopy"

type finding struct {
	Exit    string   `json:"exit"`
	File    string   `json:"file"`
	Line    int      `json:"line"`
	Kind    string   `json:"kind"`
	Origins []string `json:"origins"`
}

type recopy struct {
	Func   string `json:"func"`
	File   string `json:"file"`
	Line   int    `json:"line"`
	Copier string `json:"copier"`
	Source string `json:"owned_source"`
}

type report struct {
	Author        string              `json:"author"`
	Target        string              `json:"target"`
	Loader        string              `json:"loader"`
	Functions     int                 `json:"functions_loaded"`
	Copiers       []string            `json:"copiers"`
	Exits         []finding           `json:"retention_exits"`
	Recopies      []recopy            `json:"copies_of_owned_values"`
	Controls      map[string]bool     `json:"controls_detected"`
	Debug         map[string][]string `json:"control_trace"`
	AllControlsOK bool                `json:"all_controls_detected"`
}

var tree string

func inModule(fn *ssa.Function) bool {
	for fn != nil && fn.Pkg == nil {
		fn = fn.Parent()
	}
	if fn == nil || fn.Pkg == nil {
		return false
	}
	path := fn.Pkg.Pkg.Path()
	return path == module || strings.HasPrefix(path, module+"/")
}

func pos(fn *ssa.Function, p token.Pos) (string, int) {
	if !p.IsValid() {
		p = fn.Pos()
	}
	x := fn.Prog.Fset.Position(p)
	rel, _ := filepath.Rel(tree, x.Filename)
	return filepath.ToSlash(rel), x.Line
}

func isTestFile(fn *ssa.Function) bool {
	file, _ := pos(fn, fn.Pos())
	return strings.HasSuffix(file, "_test.go")
}

// refBearing: the value can reach a mutable map, slice, pointer, channel or interface.
func refBearing(t types.Type, seen map[types.Type]bool) bool {
	if t == nil || seen[t] {
		return false
	}
	seen[t] = true
	switch x := types.Unalias(t).(type) {
	case *types.Named:
		if p := x.Obj().Pkg(); p != nil && (p.Path() == "time" || p.Path() == "context" || strings.HasPrefix(p.Path(), "sync")) {
			return false
		}
		return refBearing(x.Underlying(), seen)
	case *types.Map, *types.Slice, *types.Pointer, *types.Interface, *types.Chan:
		return true
	case *types.Tuple:
		for i := 0; i < x.Len(); i++ {
			if refBearing(x.At(i).Type(), seen) {
				return true
			}
		}
	case *types.Struct:
		for i := 0; i < x.NumFields(); i++ {
			if refBearing(x.Field(i).Type(), seen) {
				return true
			}
		}
	case *types.Array:
		return refBearing(x.Elem(), seen)
	}
	return false
}

// Entity-like: what a public read hands out — graph entities, property maps,
// rows/pages/results and anything whose fields carry them.
func entityLike(t types.Type, seen map[types.Type]bool) bool {
	if t == nil || seen[t] {
		return false
	}
	seen[t] = true
	switch x := types.Unalias(t).(type) {
	case *types.Named:
		if x.Obj().Pkg() != nil {
			path := x.Obj().Pkg().Path()
			if path == "time" || path == "context" || strings.HasPrefix(path, "sync") || !strings.HasPrefix(path, module) {
				return false
			}
		}
		return entityLike(x.Underlying(), seen)
	case *types.Map:
		if b, ok := x.Elem().Underlying().(*types.Interface); ok && b.Empty() {
			return true // map[string]any
		}
		return entityLike(x.Elem(), seen)
	case *types.Slice:
		if b, ok := x.Elem().Underlying().(*types.Interface); ok && b.Empty() {
			return true // []any, rows
		}
		return entityLike(x.Elem(), seen)
	case *types.Pointer:
		return entityLike(x.Elem(), seen)
	case *types.Array:
		return entityLike(x.Elem(), seen)
	case *types.Struct:
		for i := 0; i < x.NumFields(); i++ {
			if entityLike(x.Field(i).Type(), seen) {
				return true
			}
		}
	}
	return false
}

func handedOut(t types.Type) bool {
	return refBearing(t, map[types.Type]bool{}) && entityLike(t, map[types.Type]bool{})
}

func origin(fn *ssa.Function) *ssa.Function {
	if fn != nil && fn.Origin() != nil {
		return fn.Origin()
	}
	return fn
}

func staticCallee(c *ssa.CallCommon) *ssa.Function {
	if c.IsInvoke() {
		return nil
	}
	switch v := c.Value.(type) {
	case *ssa.Function:
		return v
	case *ssa.MakeClosure:
		f, _ := v.Fn.(*ssa.Function)
		return f
	}
	return nil
}

func fieldName(t types.Type, index int) string {
	for {
		if p, ok := types.Unalias(t).(*types.Pointer); ok {
			t = p.Elem()
			continue
		}
		break
	}
	name := t.String()
	if n, ok := types.Unalias(t).(*types.Named); ok {
		name = n.Obj().Name()
		if n.Obj().Pkg() != nil {
			name = strings.TrimPrefix(n.Obj().Pkg().Path(), module+"/") + "." + name
		}
	}
	st, ok := t.Underlying().(*types.Struct)
	if !ok || index >= st.NumFields() {
		return name
	}
	return name + "." + st.Field(index).Name()
}

// --- flow state -----------------------------------------------------------------

// tags: set of origin labels for a value ("field T.f", "param i").
type tags map[string]bool

func (t tags) add(o tags) bool {
	if t == nil {
		return false // values that cannot reach a reference carry no retention
	}
	changed := false
	for k := range o {
		if !t[k] {
			t[k] = true
			changed = true
		}
	}
	return changed
}

type summary struct {
	returns  tags // retained origins the function's results carry (fields + "param:i")
	callback tags // retained origins delivered to a func-typed parameter
}

type analysis struct {
	prog      *workcensus.Program
	summaries map[*ssa.Function]*summary
	copiers   map[*ssa.Function]bool
	owned     map[*ssa.Function]bool
	values    map[*ssa.Function]map[ssa.Value]tags
}

// ownedTag marks a value a copier or detaching owner produced. It flows like a
// retained origin but is never reported as one; a copier applied to a value
// carrying it copies a caller-owned value again.
const ownedTag = "owned:"

func (a *analysis) sum(fn *ssa.Function) *summary {
	s := a.summaries[fn]
	if s == nil {
		s = &summary{returns: tags{}, callback: tags{}}
		a.summaries[fn] = s
	}
	return s
}

// holderRoot: a field/global read is a retention root when its base is the method
// receiver, a package global, or a closure free variable bound to either.
func isReceiver(fn *ssa.Function, v ssa.Value) bool {
	top := fn
	for top.Parent() != nil {
		top = top.Parent()
	}
	if top.Signature.Recv() == nil || len(top.Params) == 0 {
		return false
	}
	recv := top.Params[0]
	seen := map[ssa.Value]bool{}
	var walk func(ssa.Value) bool
	walk = func(x ssa.Value) bool {
		if x == nil || seen[x] {
			return false
		}
		seen[x] = true
		switch y := x.(type) {
		case *ssa.Parameter:
			return y == recv
		case *ssa.UnOp:
			if y.Op == token.MUL {
				// load of the receiver's spilled copy
				if alloc, ok := y.X.(*ssa.Alloc); ok {
					for _, r := range *alloc.Referrers() {
						if st, ok := r.(*ssa.Store); ok && st.Addr == alloc && walk(st.Val) {
							return true
						}
					}
				}
				return walk(y.X)
			}
		case *ssa.Alloc:
			for _, r := range *y.Referrers() {
				if st, ok := r.(*ssa.Store); ok && st.Addr == y && walk(st.Val) {
					return true
				}
			}
		case *ssa.FreeVar:
			return freeVarBinding(y, walk)
		case *ssa.ChangeType:
			return walk(y.X)
		}
		return false
	}
	return walk(v)
}

func freeVarBinding(fv *ssa.FreeVar, walk func(ssa.Value) bool) bool {
	parent := fv.Parent()
	index := -1
	for i, f := range parent.FreeVars {
		if f == fv {
			index = i
		}
	}
	if index < 0 || parent.Parent() == nil {
		return false
	}
	for _, b := range parent.Parent().Blocks {
		for _, ins := range b.Instrs {
			if mc, ok := ins.(*ssa.MakeClosure); ok && mc.Fn == parent && index < len(mc.Bindings) && walk(mc.Bindings[index]) {
				return true
			}
		}
	}
	return false
}

// flowFunction computes per-value retained tags in fn and updates its summary.
// localKey is one tracked location of a local allocation: a field of a struct
// variable, or the whole variable (field -1) for other types.
type localKey struct {
	alloc *ssa.Alloc
	field int
}

// rdState maps each tracked local location to the retained tags of the
// definitions reaching a program point.
type rdState map[localKey]tags

func (s rdState) clone() rdState {
	out := make(rdState, len(s))
	for k, v := range s {
		c := tags{}
		c.add(v)
		out[k] = c
	}
	return out
}

func (s rdState) join(o rdState) bool {
	changed := false
	for k, v := range o {
		if s[k] == nil {
			s[k] = tags{}
		}
		changed = s[k].add(v) || changed
	}
	return changed
}

func allocStruct(al *ssa.Alloc) *types.Struct {
	st, _ := types.Unalias(al.Type()).(*types.Pointer).Elem().Underlying().(*types.Struct)
	return st
}

// keysOf lists the tracked locations of a local; for a struct only exported
// reference-bearing fields can be handed to a caller, so only they carry tags.
func keysOf(al *ssa.Alloc) []localKey {
	st := allocStruct(al)
	if st == nil {
		return []localKey{{al, -1}}
	}
	var out []localKey
	for i := 0; i < st.NumFields(); i++ {
		f := st.Field(i)
		if f.Exported() && refBearing(f.Type(), map[types.Type]bool{}) {
			out = append(out, localKey{al, i})
		}
	}
	return out
}

func storedKeys(addr ssa.Value) (keys []localKey, strong bool) {
	switch x := addr.(type) {
	case *ssa.Alloc:
		return keysOf(x), true
	case *ssa.FieldAddr:
		if al, ok := x.X.(*ssa.Alloc); ok && allocStruct(al) != nil {
			for _, k := range keysOf(al) {
				if k.field == x.Field {
					return []localKey{k}, true
				}
			}
			return nil, true // a scalar or unexported field: nothing handed out
		}
	}
	return nil, false
}

// capturedAllocs lists locals bound into closures; a call may run those
// closures, so their stores reach every later point (without killing).
func capturedAllocs(fn *ssa.Function) map[*ssa.Alloc]bool {
	out := map[*ssa.Alloc]bool{}
	for _, b := range fn.Blocks {
		for _, ins := range b.Instrs {
			if mc, ok := ins.(*ssa.MakeClosure); ok {
				for _, bound := range mc.Bindings {
					if al, ok := bound.(*ssa.Alloc); ok {
						out[al] = true
					}
				}
			}
		}
	}
	return out
}

func (a *analysis) flowFunction(fn *ssa.Function) bool {
	val := map[ssa.Value]tags{}
	get := func(v ssa.Value) tags {
		// A value that cannot reach a mutable reference carries no retention.
		if v == nil || !refBearing(v.Type(), map[types.Type]bool{}) {
			return nil
		}
		if t := val[v]; t != nil {
			return t
		}
		t := tags{}
		val[v] = t
		return t
	}
	mem := map[ssa.Value]tags{} // conservative memory for non-local addresses
	memGet := func(v ssa.Value) tags {
		if t := mem[v]; t != nil {
			return t
		}
		t := tags{}
		mem[v] = t
		return t
	}
	captured := capturedAllocs(fn)
	for i, p := range fn.Params {
		if fn.Signature.Recv() != nil && i == 0 {
			continue
		}
		if t := get(p); t != nil {
			t[fmt.Sprintf("param:%d", i)] = true
		}
	}
	// at records the reaching state before each instruction of the last pass.
	at := map[ssa.Instruction]rdState{}
	transfer := func(ins ssa.Instruction, state rdState) {
		switch x := ins.(type) {
		case *ssa.Store:
			if keys, strong := storedKeys(x.Addr); strong {
				t := get(x.Val)
				for _, k := range keys {
					c := tags{}
					c.add(t)
					if _, whole := x.Addr.(*ssa.Alloc); whole && k.field >= 0 {
						// a whole struct store defines each field from the value
						c.add(memGet(x.Val))
					}
					state[k] = c
				}
			}
		case *ssa.Call, *ssa.Go, *ssa.Defer:
			for al := range captured {
				if t := allocStores[al]; len(t) > 0 {
					for _, k := range keysOf(al) {
						if state[k] == nil {
							state[k] = tags{}
						}
						state[k].add(t)
					}
				}
			}
		}
	}
	runRD := func() {
		in := map[*ssa.BasicBlock]rdState{}
		out := map[*ssa.BasicBlock]rdState{}
		for iter, changed := 0, true; changed && iter < 50; iter++ {
			changed = false
			for _, b := range fn.Blocks {
				state := rdState{}
				for _, p := range b.Preds {
					if o := out[p]; o != nil {
						state.join(o)
					}
				}
				if in[b] == nil || !equalState(in[b], state) {
					in[b] = state
				}
				cur := state.clone()
				for _, ins := range b.Instrs {
					at[ins] = cur.clone()
					transfer(ins, cur)
				}
				if out[b] == nil || !equalState(out[b], cur) {
					out[b] = cur
					changed = true
				}
			}
		}
	}
	loadLocal := func(ins ssa.Instruction, addr ssa.Value) (tags, bool) {
		keys, strong := storedKeys(addr)
		if !strong {
			return nil, false
		}
		out := tags{}
		for _, k := range keys {
			out.add(at[ins][k])
		}
		return out, true
	}
	changed := true
	for round := 0; changed && round < 12; round++ {
		changed = false
		runRD()
		for _, b := range fn.Blocks {
			for _, ins := range b.Instrs {
				switch x := ins.(type) {
				case *ssa.FieldAddr:
					if isReceiver(fn, x.X) {
						if handedOut(x.Type().Underlying().(*types.Pointer).Elem()) {
							changed = memGet(x).add(tags{"field " + fieldName(x.X.Type(), x.Field): true}) || changed
						}
					}
					changed = memGet(x).add(get(x.X)) || changed
				case *ssa.Field:
					if isReceiver(fn, x.X) && handedOut(x.Type()) {
						changed = get(x).add(tags{"field " + fieldName(x.X.Type(), x.Field): true}) || changed
					}
					changed = get(x).add(get(x.X)) || changed
				case *ssa.UnOp:
					if x.Op == token.MUL {
						if local, ok := loadLocal(ins, x.X); ok {
							changed = get(x).add(local) || changed
							break
						}
						if g, ok := x.X.(*ssa.Global); ok && inModule(fn) && handedOut(x.Type()) {
							changed = get(x).add(tags{"global " + g.Name(): true}) || changed
						}
						changed = get(x).add(memGet(x.X)) || changed
						changed = get(x).add(get(x.X)) || changed
						if fv, ok := x.X.(*ssa.FreeVar); ok {
							changed = get(x).add(a.freeVarTags(fv)) || changed
						}
					} else {
						changed = get(x).add(get(x.X)) || changed
					}
				case *ssa.IndexAddr:
					changed = memGet(x).add(get(x.X)) || changed
					changed = memGet(x).add(memGet(x.X)) || changed
				case *ssa.Index:
					changed = get(x).add(get(x.X)) || changed
				case *ssa.Lookup:
					changed = get(x).add(get(x.X)) || changed
				case *ssa.Slice:
					changed = get(x).add(get(x.X)) || changed
					changed = get(x).add(memGet(x.X)) || changed
					if local, ok := loadLocal(ins, x.X); ok {
						changed = get(x).add(local) || changed
					}
				case *ssa.Range:
					changed = get(x).add(get(x.X)) || changed
				case *ssa.Next:
					changed = get(x).add(get(x.Iter)) || changed
				case *ssa.Extract:
					changed = get(x).add(get(x.Tuple)) || changed
				case *ssa.Phi:
					for _, e := range x.Edges {
						changed = get(x).add(get(e)) || changed
					}
				case *ssa.ChangeType:
					changed = get(x).add(get(x.X)) || changed
				case *ssa.Convert:
					changed = get(x).add(get(x.X)) || changed
				case *ssa.MakeInterface:
					changed = get(x).add(get(x.X)) || changed
				case *ssa.TypeAssert:
					changed = get(x).add(get(x.X)) || changed
				case *ssa.ChangeInterface:
					changed = get(x).add(get(x.X)) || changed
				case *ssa.Store:
					if _, strong := storedKeys(x.Addr); strong {
						break // tracked by reaching definitions
					}
					t := get(x.Val)
					changed = memGet(x.Addr).add(t) || changed
					for addr := x.Addr; ; {
						if fa, ok := addr.(*ssa.FieldAddr); ok {
							changed = memGet(fa.X).add(t) || changed
							addr = fa.X
							continue
						}
						if ia, ok := addr.(*ssa.IndexAddr); ok {
							changed = memGet(ia.X).add(t) || changed
							addr = ia.X
							continue
						}
						break
					}
					if fv, ok := x.Addr.(*ssa.FreeVar); ok && len(t) > 0 {
						a.recordFreeVarStore(fv, t)
					}
				case *ssa.MapUpdate:
					changed = get(x.Map).add(get(x.Value)) || changed
				case *ssa.Call:
					changed = a.flowCall(fn, x.Common(), get, val, x) || changed
				}
			}
		}
	}
	// Results and callback deliveries.
	s := a.sum(fn)
	before := len(s.returns) + len(s.callback)
	resultTags := func(ins ssa.Instruction, v ssa.Value) tags {
		out := tags{}
		if al, ok := v.(*ssa.Alloc); ok {
			// returning &local hands out its reaching field definitions
			for _, k := range keysOf(al) {
				out.add(at[ins][k])
			}
			return out
		}
		out.add(get(v))
		out.add(memGet(v))
		return out
	}
	for _, b := range fn.Blocks {
		for _, ins := range b.Instrs {
			switch x := ins.(type) {
			case *ssa.Return:
				for _, r := range x.Results {
					if handedOut(r.Type()) {
						s.returns.add(resultTags(ins, r))
					}
				}
			case *ssa.Call:
				if callbackParam(fn, x.Common().Value) {
					target := s
					if owner := callbackOwner(x.Common().Value); owner != nil && owner != fn {
						target = a.sum(owner)
					}
					for _, arg := range x.Common().Args {
						if handedOut(arg.Type()) {
							target.callback.add(fieldOnly(resultTags(ins, arg)))
						}
					}
				}
			}
		}
	}
	a.values[fn] = val
	top := fn
	for top.Parent() != nil {
		top = top.Parent()
	}
	if strings.Contains(top.String(), "NOR160Control") {
		var lines []string
		for _, b := range fn.Blocks {
			for _, ins := range b.Instrs {
				line := fmt.Sprintf("b%d %s", b.Index, ins.String())
				if v, ok := ins.(ssa.Value); ok {
					line = fmt.Sprintf("b%d %s = %s tags[%s]", b.Index, v.Name(), ins.String(), keysText(get(v)))
				}
				var reach []string
				for k, t := range at[ins] {
					if len(t) > 0 {
						reach = append(reach, fmt.Sprintf("%s.%d:%s", k.alloc.Name(), k.field, keysText(t)))
					}
				}
				sort.Strings(reach)
				if len(reach) > 0 {
					line += " reach{" + strings.Join(reach, " ") + "}"
				}
				lines = append(lines, line)
			}
		}
		lines = append(lines, "returns["+keysText(s.returns)+"] callback["+keysText(s.callback)+"]")
		controlTrace[fn.String()] = lines
	}
	return len(s.returns)+len(s.callback) != before
}

func equalState(a, b rdState) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		w := b[k]
		if len(v) != len(w) {
			return false
		}
		for t := range v {
			if !w[t] {
				return false
			}
		}
	}
	return true
}

func fieldOnly(t tags) tags {
	out := tags{}
	for k := range t {
		if !strings.HasPrefix(k, "param:") && !strings.HasPrefix(k, ownedTag) {
			out[k] = true
		}
	}
	return out
}

// inPlaceReshape reports a shapeNode call whose owned argument is the constant
// true: it reshapes a caller-owned node and copies nothing.
func inPlaceReshape(callee *ssa.Function, c *ssa.CallCommon) bool {
	if callee.Name() != "shapeNode" || len(c.Args) != 4 {
		return false
	}
	k, ok := c.Args[3].(*ssa.Const)
	return ok && k.Value != nil && k.Value.String() == "true"
}

// callbackOwner returns the function whose parameter a callback value is.
func callbackOwner(callee ssa.Value) *ssa.Function {
	switch c := callee.(type) {
	case *ssa.Parameter:
		return c.Parent()
	case *ssa.FreeVar:
		var owner *ssa.Function
		freeVarBinding(c, func(v ssa.Value) bool {
			if p, ok := v.(*ssa.Parameter); ok {
				owner = p.Parent()
				return true
			}
			if fv, ok := v.(*ssa.FreeVar); ok {
				owner = callbackOwner(fv)
				return owner != nil
			}
			return false
		})
		return owner
	}
	return nil
}

func callbackParam(fn *ssa.Function, callee ssa.Value) bool {
	switch c := callee.(type) {
	case *ssa.Parameter:
		_, ok := c.Type().Underlying().(*types.Signature)
		return ok
	case *ssa.FreeVar:
		// a captured callback parameter of the enclosing function
		owner := callbackOwner(c)
		if owner == nil {
			return false
		}
		_, ok := c.Type().Underlying().(*types.Signature)
		return ok
	}
	return false
}

var freeVarStores = map[*ssa.FreeVar]tags{}

func (a *analysis) recordFreeVarStore(fv *ssa.FreeVar, t tags) {
	// Stores to a captured variable in a closure are visible to the parent's
	// Alloc: record against the binding so the parent's loads see them.
	parent := fv.Parent()
	index := -1
	for i, f := range parent.FreeVars {
		if f == fv {
			index = i
		}
	}
	if index < 0 || parent.Parent() == nil {
		return
	}
	for _, b := range parent.Parent().Blocks {
		for _, ins := range b.Instrs {
			if mc, ok := ins.(*ssa.MakeClosure); ok && mc.Fn == parent && index < len(mc.Bindings) {
				if bound, ok := mc.Bindings[index].(*ssa.Alloc); ok {
					if allocStores[bound] == nil {
						allocStores[bound] = tags{}
					}
					if allocStores[bound].add(t) {
						storesChanged = true
					}
				}
			}
		}
	}
}

var allocStores = map[*ssa.Alloc]tags{}

// controlTrace records, for each planted control, every instruction with its
// value tags and the reaching local definitions, so a missed control explains
// itself in the report.
var controlTrace = map[string][]string{}

func keysText(t tags) string {
	var out []string
	for k := range t {
		out = append(out, k)
	}
	sort.Strings(out)
	return strings.Join(out, ",")
}

// storesChanged makes the outer fixpoint repeat when a closure adds a store
// its parent must see.
var storesChanged bool

func (a *analysis) freeVarTags(fv *ssa.FreeVar) tags {
	out := tags{}
	freeVarBinding(fv, func(v ssa.Value) bool {
		if al, ok := v.(*ssa.Alloc); ok {
			out.add(allocStores[al])
		}
		return false
	})
	return out
}

func (a *analysis) flowCall(fn *ssa.Function, c *ssa.CallCommon, get func(ssa.Value) tags, val map[ssa.Value]tags, call *ssa.Call) bool {
	changed := false
	if b, ok := c.Value.(*ssa.Builtin); ok {
		switch b.Name() {
		case "append":
			for _, arg := range c.Args {
				changed = get(call).add(get(arg)) || changed
			}
		case "copy":
			if len(c.Args) == 2 {
				changed = get(c.Args[0]).add(get(c.Args[1])) || changed
			}
		}
		return changed
	}
	callee := staticCallee(c)
	if callee == nil {
		for _, target := range a.dynamicCallees(fn, call) {
			changed = a.flowCallee(fn, target, c, get, call) || changed
		}
		return changed
	}
	return a.flowCallee(fn, callee, c, get, call)
}

// dynamicCallees: the loader's VTA/CHA call graph targets of an interface or
// function-value call site.
func (a *analysis) dynamicCallees(fn *ssa.Function, call *ssa.Call) []*ssa.Function {
	node := a.prog.CallGraph.Nodes[fn]
	if node == nil {
		return nil
	}
	var out []*ssa.Function
	for _, e := range node.Out {
		if e.Site == ssa.CallInstruction(call) && e.Callee != nil && e.Callee.Func != nil && inModule(e.Callee.Func) {
			out = append(out, e.Callee.Func)
		}
	}
	return out
}

func (a *analysis) flowCallee(fn, callee *ssa.Function, c *ssa.CallCommon, get func(ssa.Value) tags, call *ssa.Call) bool {
	changed := false
	o := origin(callee)
	if a.copiers[o] {
		// The result is a fresh caller-owned value. An in-place reshape
		// (shapeNode with owned=true) returns its argument, not a new copy.
		if t := get(call); t != nil && !inPlaceReshape(o, c) {
			return t.add(tags{ownedTag + o.Name(): true})
		}
		return false
	}
	if a.owned[o] {
		if t := get(call); t != nil {
			changed = t.add(tags{ownedTag + o.Name(): true}) || changed
		}
	}
	s := a.summaries[callee]
	if s == nil {
		s = a.summaries[o]
	}
	if s == nil {
		return false
	}
	args := c.Args
	if c.IsInvoke() {
		args = append([]ssa.Value{c.Value}, c.Args...)
	}
	for k := range s.returns {
		if strings.HasPrefix(k, "param:") {
			var i int
			fmt.Sscanf(k, "param:%d", &i)
			if i < len(args) {
				changed = get(call).add(get(args[i])) || changed
			}
			continue
		}
		changed = get(call).add(tags{k: true}) || changed
	}
	// Retained values a callee delivers to the callback we pass it: if our
	// argument is our own callback parameter, the delivery is ours too.
	if len(s.callback) > 0 {
		for _, arg := range args {
			if callbackParam(fn, arg) || isClosureForwardingCallback(fn, arg) {
				sum := a.sum(fn)
				for k := range s.callback {
					if !strings.HasPrefix(k, "param:") {
						sum.callback[k] = true
					}
				}
			}
		}
	}
	return changed
}

// isClosureForwardingCallback: a closure argument that calls one of fn's callback parameters.
func isClosureForwardingCallback(fn *ssa.Function, v ssa.Value) bool {
	mc, ok := v.(*ssa.MakeClosure)
	if !ok {
		return false
	}
	inner, ok := mc.Fn.(*ssa.Function)
	if !ok {
		return false
	}
	for _, b := range inner.Blocks {
		for _, ins := range b.Instrs {
			if call, ok := ins.(*ssa.Call); ok && callbackParam(inner, call.Common().Value) {
				return true
			}
		}
	}
	return false
}

// Copiers: the canonical owners and any function whose result is a fresh copy built
// by calling a copier on one of its parameters.
var baseCopiers = map[string]bool{
	module + "/pkg/storage.copyNode": true, module + "/pkg/storage.copyNodeWithoutEmbeddings": true,
	module + "/pkg/storage.copyEdge": true, module + "/pkg/storage.copyNodeForCaller": true,
	module + "/pkg/storage.CopyNode": true, module + "/pkg/storage.CopyEdge": true,
	module + "/pkg/storage.copyPropertyValue": true, module + "/pkg/storage.copyImagesForCaller": true,
	module + "/pkg/storage.projectCachedNodeForRead": true, module + "/pkg/storage.edgeMatchCandidate": true,
	module + "/pkg/storage.shapeNode": true,
	// The single ownership implementation (NOR-160 converged head).
	module + "/pkg/callerowned.Value": true, module + "/pkg/callerowned.Map": true,
	module + "/pkg/callerowned.List": true, module + "/pkg/callerowned.Rows": true,
	module + "/pkg/callerowned.RowsObservingMaps": true, module + "/pkg/callerowned.Strings": true,
	module + "/pkg/callerowned.Of": true,
}

func fullName(fn *ssa.Function) string {
	fn = origin(fn)
	if fn.Pkg == nil {
		return fn.String()
	}
	if fn.Signature.Recv() != nil {
		return fn.String()
	}
	return fn.Pkg.Pkg.Path() + "." + fn.Name()
}

func (a *analysis) findCopiers() {
	for _, fn := range a.prog.Functions {
		if baseCopiers[fullName(fn)] {
			a.copiers[origin(fn)] = true
		}
	}
	for round := 0; round < 6; round++ {
		for _, fn := range a.prog.Functions {
			if !inModule(fn) || a.copiers[origin(fn)] || len(fn.Params) == 0 || fn.Blocks == nil {
				continue
			}
			allReturnsCopied, any := true, false
			for _, b := range fn.Blocks {
				for _, ins := range b.Instrs {
					ret, ok := ins.(*ssa.Return)
					if !ok {
						continue
					}
					for _, r := range ret.Results {
						if !handedOut(r.Type()) {
							continue
						}
						any = true
						call, ok := r.(*ssa.Call)
						if !ok {
							if _, isConst := r.(*ssa.Const); isConst {
								continue
							}
							allReturnsCopied = false
							continue
						}
						callee := staticCallee(call.Common())
						if callee == nil || !a.copiers[origin(callee)] {
							allReturnsCopied = false
						}
					}
				}
			}
			if any && allReturnsCopied {
				a.copiers[origin(fn)] = true
			}
		}
	}
}

// findOwners: non-copier functions whose handed-out results are produced by a copier
// they call (e.g. an overlay reader that detaches its images). Their results are
// caller-owned, so a later copier applied to them is a second copy.
func (a *analysis) findOwners() {
	a.owned = map[*ssa.Function]bool{}
	for _, fn := range a.prog.Functions {
		if !inModule(fn) || fn.Blocks == nil || a.copiers[origin(fn)] {
			continue
		}
		calls := false
		for _, b := range fn.Blocks {
			for _, ins := range b.Instrs {
				if call, ok := ins.(ssa.CallInstruction); ok {
					if c := staticCallee(call.Common()); c != nil && a.copiers[origin(c)] {
						calls = true
					}
				}
			}
		}
		if !calls {
			continue
		}
		name := origin(fn).Name()
		if name == "readBufferedEntities" || name == "readBufferedEntitiesWithContext" {
			a.owned[origin(fn)] = true
		}
	}
}

// --- double copies ------------------------------------------------------------------

func (a *analysis) recopies(fn *ssa.Function, out *[]recopy) {
	values := a.values[fn]
	for _, b := range fn.Blocks {
		for _, ins := range b.Instrs {
			call, ok := ins.(ssa.CallInstruction)
			if !ok {
				continue
			}
			c := staticCallee(call.Common())
			if c == nil || !a.copiers[origin(c)] || inPlaceReshape(origin(c), call.Common()) {
				continue
			}
			for _, arg := range call.Common().Args {
				if !handedOut(arg.Type()) {
					continue
				}
				var sources []string
				for k := range values[arg] {
					if strings.HasPrefix(k, ownedTag) {
						sources = append(sources, "result of "+strings.TrimPrefix(k, ownedTag))
					}
				}
				if len(sources) > 0 {
					sort.Strings(sources)
					file, line := pos(fn, call.Pos())
					*out = append(*out, recopy{Func: fn.String(), File: file, Line: line, Copier: origin(c).Name(), Source: strings.Join(sources, "; ")})
				}
			}
		}
	}
}

// --- driver -------------------------------------------------------------------------

func exported(fn *ssa.Function) bool {
	if fn.Parent() != nil || !token.IsExported(fn.Name()) {
		return false
	}
	if recv := fn.Signature.Recv(); recv != nil {
		t := recv.Type()
		if p, ok := t.(*types.Pointer); ok {
			t = p.Elem()
		}
		n, ok := types.Unalias(t).(*types.Named)
		return ok && token.IsExported(n.Obj().Name())
	}
	return true
}

func fieldOrigins(t tags) []string {
	var out []string
	for k := range t {
		if !strings.HasPrefix(k, "param:") && !strings.HasPrefix(k, ownedTag) {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}

func targetTree(spec string) (string, func(), error) {
	if !strings.HasPrefix(spec, "git:") {
		return spec, func() {}, nil
	}
	sha := strings.TrimPrefix(spec, "git:")
	if len(sha) != 40 {
		return "", nil, fmt.Errorf("target must use a full commit SHA")
	}
	for _, c := range sha {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return "", nil, fmt.Errorf("invalid target SHA")
		}
	}
	dir, err := os.MkdirTemp("", "nor160-retention-"+sha[:8]+"-")
	if err != nil {
		return "", nil, err
	}
	cleanup := func() { _ = os.RemoveAll(dir) } // exactly the directory MkdirTemp created
	archive := filepath.Join(dir, "source.tar")
	for _, argv := range [][]string{{"git", "archive", "--format=tar", "--output", archive, sha}, {"tar", "-xf", archive, "-C", dir}} {
		command := exec.Command(argv[0], argv[1:]...)
		command.Stderr = os.Stderr
		if err := command.Run(); err != nil {
			cleanup()
			return "", nil, err
		}
	}
	if err := os.Remove(archive); err != nil {
		cleanup()
		return "", nil, err
	}
	return dir, cleanup, nil
}

func main() {
	if len(os.Args) != 3 {
		panic("usage: nor160-retention <tree|git:fullSHA> <output|->")
	}
	spec, output := os.Args[1], os.Args[2]
	var cleanup func()
	var err error
	tree, cleanup, err = targetTree(spec)
	if err != nil {
		panic(err)
	}
	defer cleanup()
	config := workcensus.Config{Dir: tree, Patterns: []string{"./..."}, BuildFlags: []string{"-tags=noui,nolocalllm"}, Env: append(os.Environ(), "CGO_ENABLED=0", "GOWORK=off")}
	config.Overlay = map[string][]byte{filepath.Join(tree, "pkg/storage/nor160_retention_control.go"): []byte(controlFile)}
	p, err := workcensus.LoadProgram(config)
	if err != nil {
		panic(err)
	}
	a := &analysis{prog: p, summaries: map[*ssa.Function]*summary{}, copiers: map[*ssa.Function]bool{}, values: map[*ssa.Function]map[ssa.Value]tags{}}
	a.findCopiers()
	a.findOwners()
	var fns []*ssa.Function
	for _, fn := range p.Functions {
		if inModule(fn) && fn.Blocks != nil && !isTestFile(fn) {
			fns = append(fns, fn)
		}
	}
	for round := 0; round < 10; round++ {
		changed := false
		storesChanged = false
		for _, fn := range fns {
			if a.copiers[origin(fn)] {
				continue
			}
			if a.flowFunction(fn) {
				changed = true
			}
		}
		if !changed && !storesChanged {
			break
		}
	}
	r := report{Author: "NOR-160 lane (Claude), 30 October 2026", Target: spec, Loader: "maintained internal/workcensus.LoadProgram (fixture base b013bdfd4)", Functions: len(p.Functions), Controls: map[string]bool{}}
	for fn := range a.copiers {
		r.Copiers = append(r.Copiers, fn.String())
	}
	sort.Strings(r.Copiers)
	for _, fn := range fns {
		if !exported(fn) || a.copiers[origin(fn)] {
			continue
		}
		s := a.summaries[fn]
		if s == nil {
			continue
		}
		file, line := pos(fn, fn.Pos())
		if o := fieldOrigins(s.returns); len(o) > 0 {
			r.Exits = append(r.Exits, finding{Exit: fn.String(), File: file, Line: line, Kind: "returns retained", Origins: o})
		}
		if o := fieldOrigins(s.callback); len(o) > 0 {
			r.Exits = append(r.Exits, finding{Exit: fn.String(), File: file, Line: line, Kind: "delivers retained to callback", Origins: o})
		}
	}
	for _, fn := range fns {
		a.recopies(fn, &r.Recopies)
	}
	sort.Slice(r.Exits, func(i, j int) bool {
		if r.Exits[i].File != r.Exits[j].File {
			return r.Exits[i].File < r.Exits[j].File
		}
		return r.Exits[i].Line < r.Exits[j].Line
	})
	sort.Slice(r.Recopies, func(i, j int) bool {
		if r.Recopies[i].File != r.Recopies[j].File {
			return r.Recopies[i].File < r.Recopies[j].File
		}
		return r.Recopies[i].Line < r.Recopies[j].Line
	})
	r.Debug = controlTrace
	r.AllControlsOK = true
	for _, c := range controls {
		found := false
		for _, e := range r.Exits {
			if strings.HasSuffix(e.Exit, c) {
				found = true
			}
		}
		r.Controls[c] = found
		r.AllControlsOK = r.AllControlsOK && found
	}
	for _, c := range cleanControls {
		reported := false
		for _, e := range r.Exits {
			if strings.HasSuffix(e.Exit, c) {
				reported = true
			}
		}
		// A clean control passes when it is NOT reported.
		r.Controls["clean:"+c] = !reported
		r.AllControlsOK = r.AllControlsOK && !reported
	}
	foundDouble := false
	for _, x := range r.Recopies {
		if strings.Contains(x.Func, doubleControl) {
			foundDouble = true
		}
	}
	r.Controls[doubleControl] = foundDouble
	r.AllControlsOK = r.AllControlsOK && foundDouble
	out := os.Stdout
	if output != "-" {
		if out, err = os.Create(output); err != nil {
			panic(err)
		}
		defer out.Close()
	}
	encoder := json.NewEncoder(out)
	encoder.SetIndent("", " ")
	if err := encoder.Encode(r); err != nil {
		panic(err)
	}
}
