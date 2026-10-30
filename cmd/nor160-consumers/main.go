// NOR-160 lane (Claude), 30 October 2026. Analysis-only client, not product code.
//
// Family census by value flow, over the maintained workcensus typed/SSA loader.
// Membership comes from the code: every value of a family type (storage Node and
// Edge, any module type that contains one, any module type that declares
// CallerCopy) carries an origin computed through calls, interface and closure
// callees (VTA), function-valued arguments, locals, slices and maps:
//
//	retained  read out of a container held by an object (a cache, buffer,
//	          pending map, snapshot population, global);
//	copy      a fresh value filled from another value of its own type;
//	decoded   a fresh value not filled from another family value;
//	param i   parameter i passed through;
//	funcres i the result of calling function-valued parameter i.
//
// Classes (no allowlist: owners are reported like everything else):
//
//	K0 retained escape     an exported function returns or visits a retained value.
//	K1 copy before select  a copy (or an element of a copied population) reaches a
//	                       function exit or its own next iteration undelivered.
//	                       Reading or measuring is not delivery; a bool callback is
//	                       a selection, not a delivery.
//	K1e                    the same, only on a path that returns a non-nil error.
//	K2 admission abandon   a copy stored into a retention point on one path and
//	                       dropped on another.
//	K3 partial consumption a copied population iterated by a loop that a call's
//	                       result can leave early (callback stop, error).
//	K4 ownership dispatch  one value merges an uncopied value and a copy of it.
//	K4b Copier dispatch    Copier assertions and CallerCopy calls; a second
//	                       CallerCopy reachable after a first one in a function.
//	K5 population copy     a loop stores copies of the elements it iterates.
//	                       Mappers (loops storing a function parameter's result)
//	                       are listed separately: the one slice owner is a mapper.
//	K6 copy of private     an unconditional copier applied to a value made for
//	                       this read (decoded or already copied).
//	K7 partial copier      a copy of a family type that keeps a reference field of
//	                       its source.
//
// Planted controls (virtual file in pkg/storage, compiles on every revision)
// must each be reported in their class; the clean controls must not be.
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
const storagePkg = module + "/pkg/storage"

const controlFile = `package storage

// NOR160ControlCache is a planted retention point for the census controls.
type NOR160ControlCache struct {
	nodes map[NodeID]*Node
	max   int
}

// K0: a retained node handed out without a copy.
func (c *NOR160ControlCache) GetShared(id NodeID) *Node { return c.nodes[id] }

// K1: copied, then rejected by a selection decision.
func (c *NOR160ControlCache) GetSelected(id NodeID, hidden func(*Node) bool) *Node {
	n := copyNode(c.nodes[id])
	if hidden(n) {
		return nil
	}
	return n
}

// K1: a producer that copies on a hit and decodes on a miss; its caller rejects.
func (c *NOR160ControlCache) nor160Load(id NodeID) *Node {
	if n, ok := c.nodes[id]; ok {
		return copyNode(n)
	}
	return &Node{ID: id}
}

func (c *NOR160ControlCache) GetLoaded(id NodeID, hidden func(*Node) bool) *Node {
	n := c.nor160Load(id)
	if hidden(n) {
		return nil
	}
	return n
}

// K5: a second population-copy loop. K1: a selection over its copied population.
func (c *NOR160ControlCache) nor160CopyAll() []*Node {
	out := make([]*Node, 0, len(c.nodes))
	for _, n := range c.nodes {
		out = append(out, copyNode(n))
	}
	return out
}

func (c *NOR160ControlCache) GetUnskipped(skip map[NodeID]bool) []*Node {
	var out []*Node
	for _, n := range c.nor160CopyAll() {
		if skip[n.ID] {
			continue
		}
		out = append(out, n)
	}
	return out
}

func nor160ControlSize(n *Node) int { return len(n.Properties) }

// K2: a copy made for cache admission, then abandoned by its budget check.
func (c *NOR160ControlCache) nor160Admit(n *Node) {
	cached := copyNode(n)
	if nor160ControlSize(cached) > c.max {
		return
	}
	c.nodes[n.ID] = cached
}

func (c *NOR160ControlCache) GetAdmitted(id NodeID) *Node {
	n := &Node{ID: id, Properties: map[string]interface{}{}}
	c.nor160Admit(n)
	return n
}

// K3: a pre-read copied population that a stopping callback leaves unvisited.
func (c *NOR160ControlCache) StreamCopied(ids []NodeID, visit func(*Node) error) error {
	buf := make([]*Node, 0, len(ids))
	for _, id := range ids {
		buf = append(buf, copyNode(c.nodes[id]))
	}
	for _, n := range buf {
		if err := visit(n); err != nil {
			return err
		}
	}
	return nil
}

// K4: a second ownership dispatcher.
func nor160ControlDeliver(n *Node, owned bool) *Node {
	if owned {
		return n
	}
	return copyNode(n)
}

func (c *NOR160ControlCache) GetDispatched(id NodeID, owned bool) *Node {
	return nor160ControlDeliver(c.nodes[id], owned)
}

// K6: a copy of a node decoded for this read, and a copy of a copy.
func (c *NOR160ControlCache) GetDecodedCopy(id NodeID) *Node {
	return copyNode(&Node{ID: id, Properties: map[string]interface{}{"k": []interface{}{int64(1)}}})
}

func (c *NOR160ControlCache) GetCopyOfCopy(id NodeID) *Node { return copyNode(copyNode(c.nodes[id])) }

// Clean: select on the retained image, then copy once. Must not be reported.
func (c *NOR160ControlCache) GetClean(id NodeID, hidden func(*Node) bool) *Node {
	n := c.nodes[id]
	if n == nil || hidden(n) {
		return nil
	}
	return copyNode(n)
}

// K7: a partial copier of a family type; and a complete one (must not be reported).
type NOR160ControlHit struct {
	Node *Node
	Tags []string
}

// CallerCopy makes the planted hit a copyable entity.
func (h *NOR160ControlHit) CallerCopy() any { return nor160ControlCloneHitComplete(h) }

type NOR160ControlHits struct{ hits map[string]*NOR160ControlHit }

func nor160ControlCloneHit(h *NOR160ControlHit) *NOR160ControlHit {
	c := *h
	c.Tags = append([]string(nil), h.Tags...)
	return &c
}

func nor160ControlCloneHitComplete(h *NOR160ControlHit) *NOR160ControlHit {
	c := *h
	c.Tags = append([]string(nil), h.Tags...)
	c.Node = copyNode(h.Node)
	return &c
}

func (s *NOR160ControlHits) GetHit(k string) *NOR160ControlHit { return nor160ControlCloneHit(s.hits[k]) }

func (s *NOR160ControlHits) GetHitComplete(k string) *NOR160ControlHit {
	return nor160ControlCloneHitComplete(s.hits[k])
}

// Exclusion "written after construction": K7 ignores a shared pointer only to
// a struct type no function writes outside its own allocation. NOR160ControlMeta
// is written after construction (NOR160Bump), so sharing it must be reported;
// NOR160ControlFrozen is never written, so sharing it must not.
type NOR160ControlMeta struct{ Count int }

func (m *NOR160ControlMeta) NOR160Bump() { m.Count++ }

type NOR160ControlFrozen struct{ Name string }

type NOR160ControlRecHit struct {
	Node   *Node
	Meta   *NOR160ControlMeta
	Frozen *NOR160ControlFrozen
}

// CallerCopy makes the planted record hit a copyable entity.
func (h *NOR160ControlRecHit) CallerCopy() any {
	c := *h
	c.Node = copyNode(h.Node)
	c.Meta = &NOR160ControlMeta{Count: h.Meta.Count}
	return &c
}

type NOR160ControlRecHits struct{ hits map[string]*NOR160ControlRecHit }

// K7: the copy keeps the written record shared with the retained hit.
func (s *NOR160ControlRecHits) GetSharedMeta(k string) *NOR160ControlRecHit {
	h := s.hits[k]
	c := *h
	c.Node = copyNode(h.Node)
	return &c
}

// Clean: the copy shares only the never-written record.
func (s *NOR160ControlRecHits) GetFrozenShared(k string) *NOR160ControlRecHit {
	h := s.hits[k]
	c := *h
	c.Node = copyNode(h.Node)
	c.Meta = &NOR160ControlMeta{Count: h.Meta.Count}
	return &c
}

// Exclusion "received from another package's public read": a caller that drops
// a value an exported read of another package delivered is not a member. The
// same read called inside its own package is not a delivery, so a selection
// over its copy must still be reported (K1). NOR160ControlReceiver in
// pkg/cypher is the cross-package clean case.
func (c *NOR160ControlCache) GetCopied(id NodeID) *Node { return copyNode(c.nodes[id]) }

func (c *NOR160ControlCache) GetCopiedSelected(id NodeID, hidden func(*Node) bool) *Node {
	n := c.GetCopied(id)
	if hidden(n) {
		return nil
	}
	return n
}

// Captured-variable flow: a read whose results a closure sets from a bound
// method value, and a population built by a closure that appends to a
// captured variable. Selection after the copy must be reported (K1), through
// an array of populations and after a same-package read; building the whole
// population must not.
type nor160ControlReader struct{ c *NOR160ControlCache }

func (r *nor160ControlReader) deliver(first bool) []*Node { return r.c.nor160CopyAll() }

func nor160ControlReadBoth(c *NOR160ControlCache) (first, second []*Node) {
	r := nor160ControlReader{c: c}
	consume := func(read func(bool) []*Node) {
		first = read(true)
		second = read(false)
	}
	consume(r.deliver)
	return first, second
}

func (c *NOR160ControlCache) GetCapturedSelected(skip map[NodeID]bool) []*Node {
	var out []*Node
	include := func(n *Node) { out = append(out, n) }
	first, second := nor160ControlReadBoth(c)
	for _, part := range [...][]*Node{first, second} {
		for _, n := range part {
			if !skip[n.ID] {
				include(n)
			}
		}
	}
	return out
}

func (c *NOR160ControlCache) GetCapturedAll() []*Node {
	var out []*Node
	include := func(n *Node) { out = append(out, n) }
	first, second := nor160ControlReadBoth(c)
	for _, part := range [...][]*Node{first, second} {
		for _, n := range part {
			include(n)
		}
	}
	return out
}

func (c *NOR160ControlCache) GetCapturedLabelled(label string) []*Node {
	var out []*Node
	for _, n := range c.GetCapturedAll() {
		if len(n.Labels) == 0 || n.Labels[0] != label {
			continue
		}
		out = append(out, n)
	}
	return out
}

// Selection inside a callee: a copied population handed to a function that
// drops some of its elements must be reported at the caller (K1); handed to
// one that returns all of them, not.
func nor160ControlKeep(all []*Node, skip map[NodeID]bool) []*Node {
	var out []*Node
	for _, n := range all {
		if skip[n.ID] {
			continue
		}
		out = append(out, n)
	}
	return out
}

func nor160ControlKeepAll(all []*Node) []*Node {
	var out []*Node
	for _, n := range all {
		out = append(out, n)
	}
	return out
}

func (c *NOR160ControlCache) GetSelectedInCallee(skip map[NodeID]bool) []*Node {
	return nor160ControlKeep(c.nor160CopyAll(), skip)
}

func (c *NOR160ControlCache) GetKeptInCallee() []*Node {
	return nor160ControlKeepAll(c.nor160CopyAll())
}

// A copy accumulated in a local population that one path does not return
// must be reported (K1); one returned on every path, not.
func (c *NOR160ControlCache) GetFirstUnused(ids []NodeID, small bool) []*Node {
	var first []*Node
	for _, id := range ids {
		first = append(first, copyNode(c.nodes[id]))
	}
	if small {
		return c.nor160CopyAll()
	}
	return first
}

func (c *NOR160ControlCache) GetFirstUsed(ids []NodeID) []*Node {
	var first []*Node
	for _, id := range ids {
		first = append(first, copyNode(c.nodes[id]))
	}
	return first
}

// Copied fields discarded after the copy: a copy whose vectors a callee clears,
// directly or as the callback of a stream that hands it copies, must be
// reported (K1); clearing a node decoded for this read, or a callback that
// keeps the copy whole, must not.
func nor160ControlStrip(n *Node) *Node {
	n.ChunkEmbeddings, n.NamedEmbeddings = nil, nil
	return n
}

func (c *NOR160ControlCache) nor160StreamCopies(visit func(*Node) error) error {
	for _, n := range c.nodes {
		if err := visit(copyNode(n)); err != nil {
			return err
		}
	}
	return nil
}

func (c *NOR160ControlCache) GetStripped(id NodeID) *Node {
	return nor160ControlStrip(copyNode(c.nodes[id]))
}

func (c *NOR160ControlCache) GetDecodedStripped(id NodeID) *Node {
	return nor160ControlStrip(&Node{ID: id, ChunkEmbeddings: [][]float32{{1}}})
}

func (c *NOR160ControlCache) StreamStripped(visit func(*Node) error) error {
	return c.nor160StreamCopies(func(n *Node) error { return visit(nor160ControlStrip(n)) })
}

func (c *NOR160ControlCache) StreamWhole(visit func(*Node) error) error {
	return c.nor160StreamCopies(func(n *Node) error { return visit(n) })
}
`

const cypherControlFile = `package cypher

import "github.com/orneryd/nornicdb/pkg/storage"

// NOR160ControlReceiver drops a node that storage's public read delivered to
// it. The census must prove the receipt and not report it.
func NOR160ControlReceiver(c *storage.NOR160ControlCache, id storage.NodeID, hidden func(*storage.Node) bool) *storage.Node {
	n := c.GetCopied(id)
	if hidden(n) {
		return nil
	}
	return n
}
`

// control: a planted function and the class that must report it.
type control struct {
	class, fn string
	clean     bool
}

var controls = []control{
	{"K0", "(*" + storagePkg + ".NOR160ControlCache).GetShared", false},
	{"K1", "(*" + storagePkg + ".NOR160ControlCache).GetSelected", false},
	{"K1", "(*" + storagePkg + ".NOR160ControlCache).GetLoaded", false},
	{"K1", "(*" + storagePkg + ".NOR160ControlCache).GetUnskipped", false},
	{"K2", "(*" + storagePkg + ".NOR160ControlCache).nor160Admit", false},
	{"K3", "(*" + storagePkg + ".NOR160ControlCache).StreamCopied", false},
	{"K4", storagePkg + ".nor160ControlDeliver", false},
	{"K5", "(*" + storagePkg + ".NOR160ControlCache).nor160CopyAll", false},
	{"K6", "(*" + storagePkg + ".NOR160ControlCache).GetDecodedCopy", false},
	{"K6", "(*" + storagePkg + ".NOR160ControlCache).GetCopyOfCopy", false},
	{"K7", "(*" + storagePkg + ".NOR160ControlHits).GetHit", false},
	{"K0", "(*" + storagePkg + ".NOR160ControlCache).GetClean", true},
	{"K1", "(*" + storagePkg + ".NOR160ControlCache).GetClean", true},
	{"K6", "(*" + storagePkg + ".NOR160ControlCache).GetClean", true},
	{"K7", "(*" + storagePkg + ".NOR160ControlHits).GetHitComplete", true},
	{"K7", "(*" + storagePkg + ".NOR160ControlRecHits).GetSharedMeta", false},
	{"K7", "(*" + storagePkg + ".NOR160ControlRecHits).GetFrozenShared", true},
	{"K1", "(*" + storagePkg + ".NOR160ControlCache).GetCopiedSelected", false},
	{"K1", module + "/pkg/cypher.NOR160ControlReceiver", true},
	{"K1", "(*" + storagePkg + ".NOR160ControlCache).GetCapturedSelected", false},
	{"K1", "(*" + storagePkg + ".NOR160ControlCache).GetCapturedLabelled", false},
	{"K1", "(*" + storagePkg + ".NOR160ControlCache).GetCapturedAll", true},
	{"K1", "(*" + storagePkg + ".NOR160ControlCache).GetSelectedInCallee", false},
	{"K1", "(*" + storagePkg + ".NOR160ControlCache).GetKeptInCallee", true},
	{"K1", "(*" + storagePkg + ".NOR160ControlCache).GetFirstUnused", false},
	{"K1", "(*" + storagePkg + ".NOR160ControlCache).GetFirstUsed", true},
	{"K1", "(*" + storagePkg + ".NOR160ControlCache).GetStripped", false},
	{"K1", "(*" + storagePkg + ".NOR160ControlCache).GetDecodedStripped", true},
	{"K1", "(*" + storagePkg + ".NOR160ControlCache).StreamStripped", false},
	{"K1", "(*" + storagePkg + ".NOR160ControlCache).StreamWhole", true},
}

var tree string

// ---------------------------------------------------------------- origins

type org struct {
	Copy, Retained, Decoded, Unknown bool
	Params, FuncRes                  uint64
}

func (o org) or(p org) org {
	return org{o.Copy || p.Copy, o.Retained || p.Retained, o.Decoded || p.Decoded, o.Unknown || p.Unknown, o.Params | p.Params, o.FuncRes | p.FuncRes}
}

func (o org) empty() bool { return o == (org{}) }

// private: only values made for this read (decoded or copied here).
func (o org) private() bool {
	return !o.empty() && !o.Retained && !o.Unknown && o.Params == 0 && o.FuncRes == 0
}

func (o org) overlaps(p org) bool {
	return o.Params&p.Params != 0 || (o.Retained && p.Retained) || (o.Decoded && p.Decoded) || (o.Unknown && p.Unknown) || o.FuncRes&p.FuncRes != 0
}

func bit(i int) uint64 {
	if i < 0 || i >= 64 {
		return 0
	}
	return 1 << uint(i)
}

type summary struct {
	Ret     []org
	Escapes []bool
	Copies  []bool
	Partial map[int][]string // parameter -> fields a copy of it keeps shared
	// FreeStores: what a closure stores into each captured variable, in terms
	// of its own parameters; the enclosing function's memory receives it at
	// every call of the closure.
	FreeStores []org
}

type memKey struct {
	base  ssa.Value
	field int
}

type fnState struct {
	o     map[ssa.Value]org
	mem   map[memKey]org
	src   map[ssa.Value]org  // copies of a known source: the origin of what each copies
	intro map[ssa.Value]bool // calls and allocations that introduce a copy here
	res   map[*ssa.Call][]org
	free  map[*ssa.FreeVar]org // captured variables: what the enclosing function holds there
}

func (s *fnState) val(v ssa.Value) org {
	switch x := v.(type) {
	case *ssa.Const, *ssa.Function, *ssa.Builtin, nil:
		return org{}
	case *ssa.Global:
		if family(x.Type()) {
			return org{Retained: true}
		}
		return org{}
	}
	return s.o[v]
}

type analysis struct {
	p       *workcensus.Program
	fns     []*ssa.Function
	sums    map[*ssa.Function]*summary
	states  map[*ssa.Function]*fnState
	callees map[ssa.CallInstruction][]*ssa.Function
	callers map[*ssa.Function][]*ssa.Function
}

// ---------------------------------------------------------------- types

var famMemo = map[types.Type]bool{}

func family(t types.Type) bool {
	t = types.Unalias(t)
	if v, ok := famMemo[t]; ok {
		return v
	}
	v := familyRec(t, map[types.Type]bool{})
	famMemo[t] = v
	return v
}

func familyRec(t types.Type, seen map[types.Type]bool) bool {
	t = types.Unalias(t)
	if seen[t] {
		return false
	}
	seen[t] = true
	switch x := t.(type) {
	case *types.Pointer:
		return familyRec(x.Elem(), seen)
	case *types.Slice:
		return familyRec(x.Elem(), seen)
	case *types.Array:
		return familyRec(x.Elem(), seen)
	case *types.Map:
		return familyRec(x.Elem(), seen)
	case *types.Chan:
		return familyRec(x.Elem(), seen)
	case *types.Tuple:
		for i := 0; i < x.Len(); i++ {
			if familyRec(x.At(i).Type(), seen) {
				return true
			}
		}
		return false
	case *types.Named:
		obj := x.Obj()
		if obj.Pkg() == nil || !inModulePath(obj.Pkg().Path()) {
			return false
		}
		if obj.Pkg().Path() == storagePkg && (obj.Name() == "Node" || obj.Name() == "Edge") {
			return true
		}
		if declaresCallerCopy(x) {
			return true
		}
		return familyRec(x.Underlying(), seen)
	case *types.Struct:
		for i := 0; i < x.NumFields(); i++ {
			if familyRec(x.Field(i).Type(), seen) {
				return true
			}
		}
	}
	return false
}

func declaresCallerCopy(n *types.Named) bool {
	for _, t := range []types.Type{n, types.NewPointer(n)} {
		ms := types.NewMethodSet(t)
		for i := 0; i < ms.Len(); i++ {
			if ms.At(i).Obj().Name() == "CallerCopy" {
				return true
			}
		}
	}
	return false
}

func inModulePath(path string) bool { return path == module || strings.HasPrefix(path, module+"/") }

// container: a map, slice or array (of family values).
func container(t types.Type) bool {
	switch types.Unalias(t).Underlying().(type) {
	case *types.Map, *types.Slice, *types.Array:
		return true
	}
	return false
}

func reference(t types.Type) bool {
	switch types.Unalias(t).Underlying().(type) {
	case *types.Pointer, *types.Map, *types.Slice, *types.Interface, *types.Chan:
		return true
	}
	return false
}

func structOf(t types.Type) (*types.Named, *types.Struct) {
	n, ok := types.Unalias(t).(*types.Named)
	if !ok {
		return nil, nil
	}
	s, ok := n.Underlying().(*types.Struct)
	if !ok {
		return nil, nil
	}
	return n, s
}

func pointee(t types.Type) types.Type {
	if p, ok := types.Unalias(t).Underlying().(*types.Pointer); ok {
		return p.Elem()
	}
	return nil
}

func resultTypes(sig *types.Signature) []types.Type {
	var out []types.Type
	for i := 0; i < sig.Results().Len(); i++ {
		out = append(out, sig.Results().At(i).Type())
	}
	return out
}

func isNilConst(v ssa.Value) bool {
	c, ok := v.(*ssa.Const)
	return ok && c.Value == nil
}

func isBoolResult(sig *types.Signature) bool {
	if sig.Results().Len() != 1 {
		return false
	}
	b, ok := sig.Results().At(0).Type().Underlying().(*types.Basic)
	return ok && b.Kind() == types.Bool
}

func isErrorType(t types.Type) bool {
	return types.Identical(t, types.Universe.Lookup("error").Type())
}

// ---------------------------------------------------------------- helpers

func inModule(fn *ssa.Function) bool {
	for fn != nil && fn.Pkg == nil {
		fn = fn.Parent()
	}
	if fn == nil || fn.Pkg == nil {
		return false
	}
	return inModulePath(fn.Pkg.Pkg.Path())
}

func pos(fn *ssa.Function, p token.Pos) (string, int) {
	if !p.IsValid() {
		p = fn.Pos()
	}
	x := fn.Prog.Fset.Position(p)
	rel, err := filepath.Rel(tree, x.Filename)
	if err != nil {
		rel = x.Filename
	}
	return filepath.ToSlash(rel), x.Line
}

func isTestFile(fn *ssa.Function) bool {
	file, _ := pos(fn, fn.Pos())
	return strings.HasSuffix(file, "_test.go")
}

func originFn(fn *ssa.Function) *ssa.Function {
	if fn != nil && fn.Origin() != nil {
		return fn.Origin()
	}
	return fn
}

func fullName(fn *ssa.Function) string {
	fn = originFn(fn)
	if fn.Pkg == nil || fn.Signature.Recv() != nil || fn.Parent() != nil {
		return fn.String()
	}
	return fn.Pkg.Pkg.Path() + "." + fn.Name()
}

func top(fn *ssa.Function) *ssa.Function {
	for fn.Parent() != nil {
		fn = fn.Parent()
	}
	return fn
}

func exported(fn *ssa.Function) bool {
	fn = originFn(fn)
	if fn.Parent() != nil || fn.Pkg == nil || !token.IsExported(fn.Name()) || !inModule(fn) {
		return false
	}
	if recv := fn.Signature.Recv(); recv != nil {
		t := recv.Type()
		if p := pointee(t); p != nil {
			t = p
		}
		if n, ok := types.Unalias(t).(*types.Named); ok && !n.Obj().Exported() {
			return false
		}
	}
	return true
}

func paramIndex(fn *ssa.Function, v ssa.Value) int {
	for i, p := range fn.Params {
		if p == v {
			return i
		}
	}
	return -1
}

// local: memory this function allocated itself.
func local(v ssa.Value) bool {
	switch x := v.(type) {
	case *ssa.Alloc, *ssa.MakeSlice, *ssa.MakeMap:
		return true
	case *ssa.Slice:
		return local(x.X)
	}
	return false
}

func localBase(v ssa.Value) ssa.Value {
	switch x := v.(type) {
	case *ssa.Alloc, *ssa.MakeSlice, *ssa.MakeMap:
		return v
	case *ssa.Slice:
		return localBase(x.X)
	}
	return nil
}

func memKeyOf(addr ssa.Value) (memKey, bool) {
	switch a := addr.(type) {
	case *ssa.Alloc:
		return memKey{a, -1}, true
	case *ssa.FieldAddr:
		if b := localBase(a.X); b != nil {
			return memKey{b, a.Field}, true
		}
	case *ssa.IndexAddr:
		if b := localBase(a.X); b != nil {
			return memKey{b, -1}, true
		}
	}
	return memKey{}, false
}

func callArgs(c *ssa.CallCommon) []ssa.Value {
	if c.IsInvoke() {
		return append([]ssa.Value{c.Value}, c.Args...)
	}
	return c.Args
}

// receivedAcross: the call is a public read of another package; what it
// returns has been delivered to this function, its caller.
func (a *analysis) receivedAcross(fn *ssa.Function, call ssa.CallInstruction) bool {
	here := top(originFn(fn)).Pkg
	c := call.Common()
	if c.IsInvoke() {
		return c.Method.Exported() && (here == nil || c.Method.Pkg() == nil || c.Method.Pkg() != here.Pkg)
	}
	callees := a.calleesOf(call)
	if len(callees) == 0 {
		return false
	}
	for _, g := range callees {
		g = originFn(g)
		if !exported(g) || g.Pkg == here {
			return false
		}
	}
	return true
}

func (a *analysis) calleesOf(call ssa.CallInstruction) []*ssa.Function {
	c := call.Common()
	if f := c.StaticCallee(); f != nil {
		return []*ssa.Function{f}
	}
	return a.callees[call]
}

// freeEnv: what the enclosing function holds in captured variable i of the
// closure fn, over every place it makes the closure. Before the enclosing
// function has a state the environment is empty; summaries grow to the
// fixpoint from there. A function with no enclosing function (a synthetic
// wrapper) has no known environment.
func (a *analysis) freeEnv(fn *ssa.Function, i int) (org, bool) {
	parent := fn.Parent()
	if parent == nil {
		return org{}, false
	}
	ps := a.states[parent]
	if ps == nil {
		return org{}, true
	}
	var env org
	for _, b := range parent.Blocks {
		for _, ins := range b.Instrs {
			mc, ok := ins.(*ssa.MakeClosure)
			if !ok || mc.Fn != fn || i >= len(mc.Bindings) {
				continue
			}
			if al, ok := mc.Bindings[i].(*ssa.Alloc); ok {
				env = env.or(a.loadOrigin(ps, al))
			} else {
				env = env.or(ps.val(mc.Bindings[i]))
			}
		}
	}
	return env, true
}

// closureWrites: the captured variables of fn that a call of a closure made
// in fn writes, and what it writes there.
func (a *analysis) closureWrites(fn *ssa.Function, s *fnState, call ssa.CallInstruction) map[memKey]org {
	c := call.Common()
	mc, ok := c.Value.(*ssa.MakeClosure)
	if !ok {
		return nil
	}
	g, _ := mc.Fn.(*ssa.Function)
	sg := a.sums[g]
	if sg == nil {
		return nil
	}
	var out map[memKey]org
	for i, b := range mc.Bindings {
		al, ok := b.(*ssa.Alloc)
		if !ok || i >= len(sg.FreeStores) || sg.FreeStores[i].empty() {
			continue
		}
		if out == nil {
			out = map[memKey]org{}
		}
		k := memKey{al, -1}
		out[k] = out[k].or(a.subst(fn, s, sg.FreeStores[i], c.Args))
	}
	return out
}

// unwrapped: a synthetic wrapper (a bound method value, an interface thunk)
// stands for the method it calls.
func (a *analysis) unwrapped(g *ssa.Function) *ssa.Function {
	if a.sums[g] != nil || g.Synthetic == "" {
		return g
	}
	for _, b := range g.Blocks {
		for _, ins := range b.Instrs {
			if call, ok := ins.(*ssa.Call); ok {
				if f := call.Common().StaticCallee(); f != nil && a.sums[f] != nil {
					return f
				}
			}
		}
	}
	return g
}

// ---------------------------------------------------------------- per-function origins

func (a *analysis) analyzeFn(fn *ssa.Function) *fnState {
	s := &fnState{o: map[ssa.Value]org{}, mem: map[memKey]org{}, src: map[ssa.Value]org{}, intro: map[ssa.Value]bool{}, res: map[*ssa.Call][]org{}, free: map[*ssa.FreeVar]org{}}
	for i, p := range fn.Params {
		if _, isFunc := p.Type().Underlying().(*types.Signature); isFunc || family(p.Type()) {
			s.o[p] = org{Params: bit(i)}
		}
	}
	for i, fv := range fn.FreeVars {
		if family(fv.Type()) || pointee(fv.Type()) != nil && family(pointee(fv.Type())) {
			env, known := a.freeEnv(fn, i)
			if !known {
				env = org{Unknown: true}
			}
			s.o[fv] = env
			if pointee(fv.Type()) != nil {
				s.free[fv] = env
			}
		}
	}
	for iter := 0; iter < 16; iter++ {
		changed := false
		for _, b := range fn.Blocks {
			for _, ins := range b.Instrs {
				switch x := ins.(type) {
				case *ssa.Store:
					if k, ok := memKeyOf(x.Addr); ok {
						n := s.mem[k].or(s.val(x.Val))
						if n != s.mem[k] {
							s.mem[k], changed = n, true
						}
					}
				case *ssa.MapUpdate:
					if base := localBase(x.Map); base != nil {
						k := memKey{base, -1}
						n := s.mem[k].or(s.val(x.Value))
						if n != s.mem[k] {
							s.mem[k], changed = n, true
						}
					}
				case ssa.CallInstruction:
					// A closure called here writes the variables it captured.
					for k, n := range a.closureWrites(fn, s, x) {
						if m := s.mem[k].or(n); m != s.mem[k] {
							s.mem[k], changed = m, true
						}
					}
				}
				switch x := ins.(type) {
				case *ssa.Call:
					// copy(dst, src) moves elements into dst.
					if bi, ok := x.Call.Value.(*ssa.Builtin); ok && bi.Name() == "copy" && len(x.Call.Args) == 2 {
						if base := localBase(x.Call.Args[0]); base != nil {
							k := memKey{base, -1}
							n := s.mem[k].or(s.val(x.Call.Args[1]))
							if n != s.mem[k] {
								s.mem[k], changed = n, true
							}
						}
					}
				}
				if v, ok := ins.(ssa.Value); ok {
					n := a.origin(fn, s, v)
					if n != s.o[v] {
						s.o[v], changed = n, true
					}
				}
			}
		}
		if !changed {
			break
		}
	}
	return s
}

func (a *analysis) origin(fn *ssa.Function, s *fnState, v ssa.Value) org {
	var o org
	switch x := v.(type) {
	case *ssa.Alloc:
		elem := pointee(x.Type())
		if src, ok := copySource(s, x); ok {
			s.src[x] = src
			s.intro[x] = true
			return org{Copy: true}
		}
		all := s.mem[memKey{x, -1}]
		for k, m := range s.mem {
			if k.base == x && k.field >= 0 {
				all = all.or(m)
			}
		}
		if all.empty() && elem != nil && family(elem) {
			if _, st := structOf(elem); st != nil {
				return org{Decoded: true}
			}
		}
		return all
	case *ssa.UnOp:
		switch x.Op {
		case token.MUL:
			o = a.loadOrigin(s, x.X)
		case token.ARROW:
			o = s.val(x.X)
		}
	case *ssa.Field:
		o = s.val(x.X)
	case *ssa.FieldAddr:
		o = s.val(x.X)
	case *ssa.IndexAddr:
		o = s.val(x.X)
		if b := localBase(x.X); b != nil {
			o = o.or(s.mem[memKey{b, -1}])
		}
	case *ssa.Index:
		o = s.val(x.X)
	case *ssa.Lookup:
		o = s.val(x.X)
		if b := localBase(x.X); b != nil {
			o = o.or(s.mem[memKey{b, -1}])
		}
	case *ssa.Range:
		o = s.val(x.X)
		if b := localBase(x.X); b != nil {
			o = o.or(s.mem[memKey{b, -1}])
		}
	case *ssa.Next:
		o = s.val(x.Iter)
	case *ssa.Extract:
		switch t := x.Tuple.(type) {
		case *ssa.Call:
			if rs := s.res[t]; x.Index < len(rs) {
				o = rs[x.Index]
			}
		case *ssa.Next:
			if x.Index > 0 {
				o = s.val(t)
			}
		default:
			if x.Index == 0 {
				o = s.val(x.Tuple)
			}
		}
	case *ssa.Phi:
		for _, e := range x.Edges {
			o = o.or(s.val(e))
		}
	case *ssa.Slice:
		o = s.val(x.X)
		if b := localBase(x.X); b != nil {
			o = o.or(s.mem[memKey{b, -1}])
		}
	case *ssa.MakeSlice, *ssa.MakeMap:
		o = s.mem[memKey{v, -1}]
	case *ssa.MakeInterface:
		o = s.val(x.X)
	case *ssa.ChangeType:
		o = s.val(x.X)
	case *ssa.Convert:
		o = s.val(x.X)
	case *ssa.ChangeInterface:
		o = s.val(x.X)
	case *ssa.TypeAssert:
		o = s.val(x.X)
	case *ssa.SliceToArrayPointer:
		o = s.val(x.X)
	case *ssa.MultiConvert:
		o = s.val(x.X)
	case *ssa.Call:
		rs := a.callResults(fn, s, x)
		s.res[x] = rs
		for _, r := range rs {
			o = o.or(r)
		}
		return o
	default:
		return org{}
	}
	if !family(v.Type()) {
		return org{}
	}
	return o
}

// loadOrigin: the origin of what addr points to.
func (a *analysis) loadOrigin(s *fnState, addr ssa.Value) org {
	switch ad := addr.(type) {
	case *ssa.Alloc:
		all := s.mem[memKey{ad, -1}]
		for k, m := range s.mem {
			if k.base == ad && k.field >= 0 {
				all = all.or(m)
			}
		}
		return all
	case *ssa.FieldAddr:
		if b := localBase(ad.X); b != nil {
			return s.mem[memKey{b, ad.Field}].or(s.mem[memKey{b, -1}])
		}
		ft := pointee(ad.Type())
		if ft == nil || !family(ft) {
			return org{}
		}
		if container(ft) {
			return org{Retained: true} // a container held by an object
		}
		return s.val(ad.X)
	case *ssa.IndexAddr:
		o := s.val(ad.X)
		if b := localBase(ad.X); b != nil {
			o = o.or(s.mem[memKey{b, -1}])
		}
		return o
	case *ssa.Global:
		if family(pointee(ad.Type())) {
			return org{Retained: true}
		}
		return org{}
	case *ssa.FreeVar:
		if env, ok := s.free[ad]; ok {
			return env
		}
		if t := pointee(ad.Type()); t != nil && family(t) {
			return org{Unknown: true}
		}
		return org{}
	}
	return s.val(addr)
}

// copySource: an allocation of a family struct filled from another value of
// the same type (whole, or field by field) is a copy of it.
func copySource(s *fnState, alloc *ssa.Alloc) (org, bool) {
	elem := pointee(alloc.Type())
	if elem == nil || !family(elem) {
		return org{}, false
	}
	n, st := structOf(elem)
	if n == nil || !entityType(n) {
		return org{}, false // a wrapper passed by value is not a structural copy
	}
	refs := alloc.Referrers()
	if refs == nil {
		return org{}, false
	}
	var src org
	found := false
	sameSource := func(v ssa.Value) (ssa.Value, bool) {
		if !types.Identical(v.Type(), elem) {
			return nil, false
		}
		switch x := v.(type) {
		case *ssa.UnOp:
			if x.Op == token.MUL {
				if _, isLocal := x.X.(*ssa.Alloc); !isLocal {
					return x.X, true
				}
			}
		case *ssa.Parameter, *ssa.Field, *ssa.Index:
			return v, true
		case *ssa.Extract:
			if _, isCall := x.Tuple.(*ssa.Call); !isCall {
				return v, true
			}
		}
		return nil, false
	}
	fresh := false
	for _, r := range *refs {
		switch x := r.(type) {
		case *ssa.Store:
			if x.Addr == alloc {
				if from, ok := sameSource(x.Val); ok {
					src, found = src.or(s.val(from)), true
				}
			}
		case *ssa.FieldAddr:
			if fr := x.Referrers(); fr != nil {
				for _, fref := range *fr {
					sto, ok := fref.(*ssa.Store)
					if !ok || sto.Addr != x {
						continue
					}
					direct := false
					if ld, ok := sto.Val.(*ssa.UnOp); ok && ld.Op == token.MUL {
						if fa, ok := ld.X.(*ssa.FieldAddr); ok && fa.Field == x.Field && types.Identical(pointee(fa.X.Type()), elem) {
							src, found, direct = src.or(s.val(fa.X)), true, true
						}
					}
					if !direct && x.Field < st.NumFields() && reference(st.Field(x.Field).Type()) && !isNilConst(sto.Val) {
						fresh = true
					}
				}
			}
		}
	}
	return src, found && fresh
}

// entityType: a storage Node or Edge, or a module type that declares CallerCopy.
func entityType(n *types.Named) bool {
	obj := n.Obj()
	if obj.Pkg() == nil || !inModulePath(obj.Pkg().Path()) {
		return false
	}
	if obj.Pkg().Path() == storagePkg && (obj.Name() == "Node" || obj.Name() == "Edge") {
		return true
	}
	return declaresCallerCopy(n)
}

func (a *analysis) callResults(fn *ssa.Function, s *fnState, call *ssa.Call) []org {
	c := call.Common()
	rts := resultTypes(c.Signature())
	out := make([]org, len(rts))
	if bi, ok := c.Value.(*ssa.Builtin); ok {
		if bi.Name() == "append" && len(out) == 1 && len(c.Args) == 2 {
			out[0] = s.val(c.Args[0]).or(s.val(c.Args[1]))
			for _, arg := range c.Args {
				if b := localBase(arg); b != nil {
					out[0] = out[0].or(s.mem[memKey{b, -1}])
				}
			}
		}
		return out
	}
	args := callArgs(c)
	callees := a.calleesOf(call)
	if len(callees) == 0 {
		idx := -1
		if p, ok := c.Value.(*ssa.Parameter); ok {
			idx = paramIndex(fn, p)
		}
		for i, t := range rts {
			if family(t) {
				if idx >= 0 {
					out[i] = org{FuncRes: bit(idx)}
				} else {
					out[i] = org{Unknown: true}
				}
			}
		}
		return out
	}
	var src org
	introduces := false
	for _, g := range callees {
		sg := a.sums[g]
		if sg == nil {
			for i, t := range rts {
				if family(t) {
					out[i].Unknown = true
				}
			}
			continue
		}
		for i := 0; i < len(out) && i < len(sg.Ret); i++ {
			out[i] = out[i].or(a.subst(fn, s, sg.Ret[i], args))
			if sg.Ret[i].Copy {
				s.intro[call] = true
			}
		}
		for j, cp := range sg.Copies {
			if cp && j < len(args) {
				src, introduces = src.or(s.val(args[j])), true
			}
		}
	}
	if introduces && len(out) > 0 && out[0].Copy {
		s.src[call] = src
	}
	return out
}

func (a *analysis) subst(fn *ssa.Function, s *fnState, o org, args []ssa.Value) org {
	r := org{Copy: o.Copy, Retained: o.Retained, Decoded: o.Decoded, Unknown: o.Unknown}
	for j := 0; j < 64; j++ {
		if o.Params&bit(j) != 0 {
			if j < len(args) {
				r = r.or(s.val(args[j]))
			} else {
				r.Unknown = true
			}
		}
		if o.FuncRes&bit(j) != 0 {
			if j < len(args) {
				r = r.or(a.funcValueResult(fn, s, args[j], args))
			} else {
				r.Unknown = true
			}
		}
	}
	return r
}

// funcValueResult: what calling the function value v returns, its own
// parameters standing for the elements the callee passes it.
func (a *analysis) funcValueResult(fn *ssa.Function, s *fnState, v ssa.Value, args []ssa.Value) org {
	var g *ssa.Function
	switch x := v.(type) {
	case *ssa.Function:
		g = x
	case *ssa.MakeClosure:
		g, _ = x.Fn.(*ssa.Function)
	case *ssa.Parameter:
		if i := paramIndex(fn, x); i >= 0 {
			return org{FuncRes: bit(i)}
		}
	}
	if g == nil {
		return org{Unknown: true}
	}
	g = a.unwrapped(g)
	sg := a.sums[g]
	if sg == nil || len(sg.Ret) == 0 {
		return org{Unknown: true}
	}
	o := sg.Ret[0]
	r := org{Copy: o.Copy, Retained: o.Retained, Decoded: o.Decoded, Unknown: o.Unknown || o.FuncRes != 0}
	if o.Params != 0 {
		for _, arg := range args {
			if _, isFunc := arg.Type().Underlying().(*types.Signature); !isFunc {
				r = r.or(s.val(arg))
			}
		}
	}
	return r
}

// ---------------------------------------------------------------- summaries

func (a *analysis) summarize(fn *ssa.Function, s *fnState) *summary {
	sum := &summary{Ret: make([]org, fn.Signature.Results().Len()), Escapes: make([]bool, len(fn.Params)), Copies: make([]bool, len(fn.Params)), FreeStores: make([]org, len(fn.FreeVars))}
	for _, b := range fn.Blocks {
		for _, ins := range b.Instrs {
			if st, ok := ins.(*ssa.Store); ok {
				for i, fv := range fn.FreeVars {
					if st.Addr == fv {
						sum.FreeStores[i] = sum.FreeStores[i].or(s.val(st.Val))
					}
				}
			}
		}
	}
	for _, b := range fn.Blocks {
		for _, ins := range b.Instrs {
			if ret, ok := ins.(*ssa.Return); ok {
				for i, r := range ret.Results {
					if i < len(sum.Ret) {
						sum.Ret[i] = sum.Ret[i].or(s.val(r))
					}
				}
			}
		}
	}
	for v, src := range s.src {
		for j := range sum.Copies {
			if src.Params&bit(j) != 0 {
				sum.Copies[j] = true
			}
		}
		if al, ok := v.(*ssa.Alloc); ok {
			if _, st := structOf(pointee(al.Type())); st != nil {
				if fields := sharedFields(al, st); len(fields) > 0 {
					for j := range sum.Copies {
						if src.Params&bit(j) != 0 {
							if sum.Partial == nil {
								sum.Partial = map[int][]string{}
							}
							sum.Partial[j] = fields
						}
					}
				}
			}
		}
	}
	for j := range sum.Escapes {
		for i := range sum.Ret {
			if sum.Ret[i].Params&bit(j) != 0 {
				sum.Escapes[j] = true
			}
		}
	}
	for _, b := range fn.Blocks {
		for _, ins := range b.Instrs {
			for j := range sum.Escapes {
				if sum.Escapes[j] {
					continue
				}
				has := func(v ssa.Value) bool { return s.val(v).Params&bit(j) != 0 }
				if a.deliversWith(fn, ins, has) {
					sum.Escapes[j] = true
				}
			}
		}
	}
	return sum
}

func (a *analysis) equalSummary(x, y *summary) bool {
	if x == nil || y == nil || len(x.Ret) != len(y.Ret) || len(x.Escapes) != len(y.Escapes) {
		return false
	}
	for i := range x.Ret {
		if x.Ret[i] != y.Ret[i] {
			return false
		}
	}
	for i := range x.Escapes {
		if x.Escapes[i] != y.Escapes[i] || x.Copies[i] != y.Copies[i] {
			return false
		}
	}
	if len(x.FreeStores) != len(y.FreeStores) {
		return false
	}
	for i := range x.FreeStores {
		if x.FreeStores[i] != y.FreeStores[i] {
			return false
		}
	}
	return true
}

// deliversWith: ins hands a value accepted by has to someone outside the
// function: a return, a store into memory it does not own, a send, a non-bool
// callback, or a callee that keeps or passes on that parameter.
func (a *analysis) deliversWith(fn *ssa.Function, ins ssa.Instruction, has func(ssa.Value) bool) bool {
	switch x := ins.(type) {
	case *ssa.Return:
		for _, r := range x.Results {
			if has(r) {
				return true
			}
		}
	case *ssa.Store:
		return has(x.Val) && !local(storeBase(x.Addr))
	case *ssa.MapUpdate:
		return has(x.Value) && localBase(x.Map) == nil
	case *ssa.Send:
		return has(x.X)
	case ssa.CallInstruction:
		c := x.Common()
		if _, ok := c.Value.(*ssa.Builtin); ok {
			return false
		}
		args := callArgs(c)
		callees := a.calleesOf(x)
		if len(callees) == 0 {
			if c.IsInvoke() {
				for _, arg := range args {
					if has(arg) {
						return true
					}
				}
				return false
			}
			if isBoolResult(c.Signature()) {
				return false // a predicate reads its argument
			}
			for _, arg := range c.Args {
				if has(arg) {
					return true
				}
			}
			return false
		}
		for _, g := range callees {
			sg := a.sums[g]
			for j, arg := range args {
				if !has(arg) {
					continue
				}
				if sg == nil || j >= len(sg.Escapes) || sg.Escapes[j] {
					return true
				}
			}
		}
	}
	return false
}

func storeBase(addr ssa.Value) ssa.Value {
	switch a := addr.(type) {
	case *ssa.FieldAddr:
		if b := localBase(a.X); b != nil {
			return b
		}
		return a.X
	case *ssa.IndexAddr:
		if b := localBase(a.X); b != nil {
			return b
		}
		return a.X
	}
	return addr
}

// ---------------------------------------------------------------- derived sets

// derived: the values that are, or hold, the root.
// With elements false it does not follow element reads: the population as a whole.
func (a *analysis) derived(fn *ssa.Function, s *fnState, roots []ssa.Value, elements bool) map[ssa.Value]bool {
	d := map[ssa.Value]bool{}
	for _, r := range roots {
		d[r] = true
	}
	in := func(v ssa.Value) bool { return d[v] }
	for iter := 0; iter < 12; iter++ {
		changed := false
		add := func(v ssa.Value) {
			if v != nil && !d[v] {
				d[v], changed = true, true
			}
		}
		for _, b := range fn.Blocks {
			for _, ins := range b.Instrs {
				switch x := ins.(type) {
				case *ssa.Phi:
					for _, e := range x.Edges {
						if in(e) {
							add(x)
						}
					}
				case *ssa.ChangeType:
					if in(x.X) {
						add(x)
					}
				case *ssa.MakeInterface:
					if in(x.X) {
						add(x)
					}
				case *ssa.Convert:
					if in(x.X) {
						add(x)
					}
				case *ssa.ChangeInterface:
					if in(x.X) {
						add(x)
					}
				case *ssa.TypeAssert:
					if in(x.X) {
						add(x)
					}
				case *ssa.Slice:
					if in(x.X) {
						add(x)
					}
				case *ssa.Extract:
					if in(x.Tuple) && family(x.Type()) {
						if _, isNext := x.Tuple.(*ssa.Next); !isNext || elements {
							add(x)
						}
					}
				case *ssa.Field:
					if in(x.X) && family(x.Type()) {
						add(x)
					}
				case *ssa.FieldAddr:
					if in(x.X) && family(x.Type()) {
						add(x)
					}
				case *ssa.IndexAddr:
					if in(x.X) && elements {
						add(x)
					}
				case *ssa.Index:
					if in(x.X) && family(x.Type()) && elements {
						add(x)
					}
				case *ssa.Lookup:
					if in(x.X) && family(x.Type()) && elements {
						add(x)
					}
				case *ssa.Range:
					if in(x.X) && elements {
						add(x)
					}
				case *ssa.Next:
					if in(x.Iter) && elements {
						add(x)
					}
				case *ssa.UnOp:
					if x.Op == token.MUL {
						if in(x.X) && family(x.Type()) {
							add(x)
						}
						if b := localBase(x.X); b != nil && in(b) {
							add(x)
						}
						if fa, ok := x.X.(*ssa.FieldAddr); ok {
							if b := localBase(fa.X); b != nil && in(b) {
								add(x)
							}
						}
					}
				case *ssa.Store:
					if in(x.Val) {
						if b := localBase(storeBase(x.Addr)); b != nil {
							add(b)
						} else if al, ok := x.Addr.(*ssa.Alloc); ok {
							add(al)
						}
					}
				case *ssa.MapUpdate:
					if in(x.Value) {
						if b := localBase(x.Map); b != nil {
							add(b)
						}
					}
				case *ssa.Call:
					c := x.Common()
					if bi, ok := c.Value.(*ssa.Builtin); ok {
						switch bi.Name() {
						case "append":
							for _, arg := range c.Args {
								if in(arg) {
									add(x)
								}
							}
						case "copy":
							if len(c.Args) == 2 && in(c.Args[1]) {
								if b := localBase(c.Args[0]); b != nil {
									add(b)
								}
							}
						}
						continue
					}
					// A closure that stores its argument into a captured
					// variable puts the root into that variable.
					if mc, ok := c.Value.(*ssa.MakeClosure); ok {
						if g, _ := mc.Fn.(*ssa.Function); g != nil && a.sums[g] != nil {
							for i, b := range mc.Bindings {
								al, ok := b.(*ssa.Alloc)
								if !ok || i >= len(a.sums[g].FreeStores) {
									continue
								}
								for j, arg := range c.Args {
									if in(arg) && a.sums[g].FreeStores[i].Params&bit(j) != 0 {
										add(al)
									}
								}
							}
						}
					}
					args := callArgs(c)
					for _, g := range a.calleesOf(x) {
						sg := a.sums[g]
						if sg == nil {
							continue
						}
						for j, arg := range args {
							if !in(arg) {
								continue
							}
							for _, r := range sg.Ret {
								if r.Params&bit(j) != 0 {
									add(x)
								}
							}
						}
					}
				}
			}
		}
		if !changed {
			break
		}
	}
	return d
}

// ---------------------------------------------------------------- drops

type dropKind int

const (
	noDrop dropKind = iota
	dropPlain
	dropError
)

// presentBranch: if cond is decided by the root (nil checks of the root or of a
// sibling result of its call, or its ok), the successor index on which the root
// exists; -1 otherwise.
func presentBranch(cond ssa.Value, d map[ssa.Value]bool, rootCall *ssa.Call) int {
	sibling := func(v ssa.Value) (bool, bool) { // (from root call, is the root value itself)
		if d[v] {
			return true, true
		}
		if ex, ok := v.(*ssa.Extract); ok && rootCall != nil && ex.Tuple == rootCall {
			return true, false
		}
		return false, false
	}
	if ok, self := sibling(cond); ok && !self {
		return 0 // the ok result: present when true
	}
	bin, ok := cond.(*ssa.BinOp)
	if !ok || (bin.Op != token.EQL && bin.Op != token.NEQ) {
		return -1
	}
	for _, pair := range [][2]ssa.Value{{bin.X, bin.Y}, {bin.Y, bin.X}} {
		if !isNilConst(pair[1]) {
			continue
		}
		from, self := sibling(pair[0])
		if !from {
			continue
		}
		// root != nil: present on true; err != nil: present on false.
		presentWhenNonNil := self
		if bin.Op == token.NEQ {
			if presentWhenNonNil {
				return 0
			}
			return 1
		}
		if presentWhenNonNil {
			return 1
		}
		return 0
	}
	return -1
}

func errorReturn(ret *ssa.Return) bool {
	if len(ret.Results) == 0 {
		return false
	}
	last := ret.Results[len(ret.Results)-1]
	return isErrorType(last.Type()) && !isNilConst(last)
}

// mayDrop: a path from the root's definition reaches a function exit, or the
// root's own block again, without delivering anything derived from it.
func (a *analysis) mayDrop(fn *ssa.Function, s *fnState, root ssa.Instruction, d map[ssa.Value]bool) (dropKind, bool) {
	rootCall, _ := root.(*ssa.Call)
	has := func(v ssa.Value) bool { return d[v] }
	var start *ssa.BasicBlock
	idx := -1
	if root == nil {
		// A parameter: its obligation starts at the function's entry.
		if len(fn.Blocks) == 0 {
			return noDrop, false
		}
		start = fn.Blocks[0]
	} else {
		start = root.Block()
		for i, ins := range start.Instrs {
			if ins == root {
				idx = i
			}
		}
		if idx < 0 {
			return noDrop, false
		}
	}
	storedRetention := false
	for _, b := range fn.Blocks {
		for _, ins := range b.Instrs {
			switch x := ins.(type) {
			case *ssa.Store:
				if has(x.Val) && !local(storeBase(x.Addr)) {
					storedRetention = true
				}
			case *ssa.MapUpdate:
				if has(x.Value) && localBase(x.Map) == nil {
					storedRetention = true
				}
			}
		}
	}
	// moves: the root goes into a local container. The obligation moves with
	// it: the container must then be delivered on every path, and coming
	// round to the root again after a move is not a drop.
	moves := func(ins ssa.Instruction) bool {
		switch x := ins.(type) {
		case *ssa.Call:
			if bi, ok := x.Call.Value.(*ssa.Builtin); ok && bi.Name() == "append" {
				for _, arg := range x.Call.Args {
					if has(arg) {
						return true
					}
				}
			}
			if len(a.closureWritesTo(x, has)) > 0 {
				return true
			}
		case *ssa.Store:
			return has(x.Val) && local(storeBase(x.Addr))
		case *ssa.MapUpdate:
			return has(x.Value) && localBase(x.Map) != nil
		}
		return false
	}
	// scan: whether the instructions deliver, and whether the root has moved.
	scan := func(instrs []ssa.Instruction, moved bool) (bool, bool) {
		for _, ins := range instrs {
			if a.deliversWith(fn, ins, has) {
				return true, moved
			}
			if moves(ins) {
				moved = true
			}
		}
		return false, moved
	}
	type state struct {
		b     *ssa.BasicBlock
		moved bool
	}
	var first []ssa.Instruction
	if idx+1 < len(start.Instrs) {
		first = start.Instrs[idx+1:]
	}
	done, moved0 := scan(first, false)
	if done {
		return noDrop, storedRetention
	}
	if ret, ok := start.Instrs[len(start.Instrs)-1].(*ssa.Return); ok {
		if errorReturn(ret) {
			return dropError, storedRetention
		}
		return dropPlain, storedRetention
	}
	seen := map[state]bool{}
	var stack []state
	push := func(b *ssa.BasicBlock, moved bool) {
		if iff, ok := b.Instrs[len(b.Instrs)-1].(*ssa.If); ok && !moved {
			if br := presentBranch(iff.Cond, d, rootCall); br >= 0 {
				stack = append(stack, state{b.Succs[br], moved})
				return
			}
		}
		for _, succ := range b.Succs {
			stack = append(stack, state{succ, moved})
		}
	}
	push(start, moved0)
	result := noDrop
	for len(stack) > 0 {
		st := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		b := st.b
		if b == start && root != nil {
			// The next iteration redefines the root: unless it moved into a
			// container, this one is dropped.
			if !st.moved && result == noDrop {
				result = dropPlain
			}
			continue
		}
		if seen[st] {
			continue
		}
		seen[st] = true
		done, moved := scan(b.Instrs, st.moved)
		if done {
			continue
		}
		last := b.Instrs[len(b.Instrs)-1]
		if ret, ok := last.(*ssa.Return); ok {
			if errorReturn(ret) {
				if result == noDrop {
					result = dropError
				}
			} else {
				result = dropPlain
			}
			continue
		}
		if _, ok := last.(*ssa.Panic); ok {
			continue
		}
		push(b, moved)
	}
	return result, storedRetention
}

// closureWritesTo: captured variables a call of a closure made here writes
// with a value accepted by has.
func (a *analysis) closureWritesTo(call *ssa.Call, has func(ssa.Value) bool) []*ssa.Alloc {
	mc, ok := call.Call.Value.(*ssa.MakeClosure)
	if !ok {
		return nil
	}
	g, _ := mc.Fn.(*ssa.Function)
	sg := a.sums[g]
	if sg == nil {
		return nil
	}
	var out []*ssa.Alloc
	for i, b := range mc.Bindings {
		al, ok := b.(*ssa.Alloc)
		if !ok || i >= len(sg.FreeStores) {
			continue
		}
		for j, arg := range call.Call.Args {
			if has(arg) && sg.FreeStores[i].Params&bit(j) != 0 {
				out = append(out, al)
			}
		}
	}
	return out
}

// paramDrops: for each function, the parameters whose value (an entity) or
// some element (a population) it can leave undelivered after handing some of
// it on: a selection made inside the callee. A function that never hands the
// value on only reads it, and its caller still holds it. Computed after the
// summaries converge, callees first, so a parameter passed on to a callee that
// drops it is dropped here too.
func (a *analysis) paramDrops() map[*ssa.Function][]bool {
	out := map[*ssa.Function][]bool{}
	for _, fn := range a.postOrder() {
		s := a.states[fn]
		if s == nil {
			continue
		}
		drops := make([]bool, len(fn.Params))
		for j, p := range fn.Params {
			if !family(p.Type()) {
				continue
			}
			if a.passesToDropper(fn, s, p, out) {
				drops[j] = true
				continue
			}
			if container(p.Type()) {
				d := a.derived(fn, s, []ssa.Value{p}, false)
				if a.receivesAny(fn, d) {
					continue // the population is handed on whole
				}
				for _, e := range a.leafElements(fn, s, d) {
					ei, _ := e.(ssa.Instruction)
					if ei == nil {
						continue
					}
					ed := a.derived(fn, s, []ssa.Value{e}, true)
					if !a.receivesAny(fn, ed) {
						continue
					}
					if k, _ := a.mayDrop(fn, s, ei, ed); k == dropPlain {
						drops[j] = true
					}
				}
				continue
			}
			d := a.derived(fn, s, []ssa.Value{p}, true)
			if !a.receivesAny(fn, d) {
				continue
			}
			if k, _ := a.mayDrop(fn, s, nil, d); k == dropPlain {
				drops[j] = true
			}
		}
		out[fn] = drops
	}
	return out
}

// receivesAny: some instruction hands a value of d on.
func (a *analysis) receivesAny(fn *ssa.Function, d map[ssa.Value]bool) bool {
	for _, b := range fn.Blocks {
		for _, ins := range b.Instrs {
			if a.deliversWith(fn, ins, func(x ssa.Value) bool { return d[x] }) {
				return true
			}
		}
	}
	return false
}

// passesToDropper: fn hands parameter p to a callee that drops it.
func (a *analysis) passesToDropper(fn *ssa.Function, s *fnState, p ssa.Value, drops map[*ssa.Function][]bool) bool {
	d := a.derived(fn, s, []ssa.Value{p}, false)
	_, _, ok := a.droppingCallIn(fn, d, drops)
	return ok
}

// droppingCallIn: some call in fn hands a value of d to a callee that drops it.
func (a *analysis) droppingCallIn(fn *ssa.Function, d map[ssa.Value]bool, drops map[*ssa.Function][]bool) (*ssa.Function, int, bool) {
	for _, b := range fn.Blocks {
		for _, ins := range b.Instrs {
			call, ok := ins.(ssa.CallInstruction)
			if !ok {
				continue
			}
			args := callArgs(call.Common())
			for _, g := range a.calleesOf(call) {
				dg := drops[g]
				for j, arg := range args {
					if j < len(dg) && dg[j] && d[arg] {
						return g, j, true
					}
				}
			}
		}
	}
	return nil, 0, false
}

// ---------------------------------------------------------------- loops

func reach(from *ssa.BasicBlock) map[*ssa.BasicBlock]bool {
	seen := map[*ssa.BasicBlock]bool{}
	stack := append([]*ssa.BasicBlock(nil), from.Succs...)
	for len(stack) > 0 {
		b := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if seen[b] {
			continue
		}
		seen[b] = true
		stack = append(stack, b.Succs...)
	}
	return seen
}

// loopOf: the blocks on a cycle through b (empty when b is on no loop).
func loopOf(fn *ssa.Function, b *ssa.BasicBlock) map[*ssa.BasicBlock]bool {
	fwd := reach(b)
	if !fwd[b] {
		return nil
	}
	out := map[*ssa.BasicBlock]bool{b: true}
	for _, x := range fn.Blocks {
		if fwd[x] && reach(x)[b] {
			out[x] = true
		}
	}
	return out
}

// callDecided: cond depends on the result of a non-builtin call.
func callDecided(v ssa.Value, depth int) bool {
	if depth > 6 || v == nil {
		return false
	}
	switch x := v.(type) {
	case *ssa.Call:
		_, builtin := x.Call.Value.(*ssa.Builtin)
		return !builtin
	case *ssa.Extract:
		if _, ok := x.Tuple.(*ssa.Next); ok {
			return false
		}
		return callDecided(x.Tuple, depth+1)
	case *ssa.BinOp:
		return callDecided(x.X, depth+1) || callDecided(x.Y, depth+1)
	case *ssa.UnOp:
		if x.Op == token.NOT {
			return callDecided(x.X, depth+1)
		}
	case *ssa.Phi:
		for _, e := range x.Edges {
			if callDecided(e, depth+1) {
				return true
			}
		}
	}
	return false
}

// elementsOf: element reads of any value in d (index, lookup, range).
func elementsOf(fn *ssa.Function, d map[ssa.Value]bool) []ssa.Value {
	var out []ssa.Value
	for _, b := range fn.Blocks {
		for _, ins := range b.Instrs {
			switch x := ins.(type) {
			case *ssa.UnOp:
				if ia, ok := x.X.(*ssa.IndexAddr); ok && x.Op == token.MUL && d[ia.X] && family(x.Type()) {
					out = append(out, x)
				}
			case *ssa.Index:
				if d[x.X] && family(x.Type()) {
					out = append(out, x)
				}
			case *ssa.Lookup:
				if d[x.X] && family(x.Type()) && !x.CommaOk {
					out = append(out, x)
				}
			case *ssa.Extract:
				if nx, ok := x.Tuple.(*ssa.Next); ok && x.Index == 2 {
					if rg, ok := nx.Iter.(*ssa.Range); ok && d[rg.X] && family(x.Type()) {
						out = append(out, x)
					}
				}
			}
		}
	}
	return out
}

// leafElements: the entity elements of a population, through populations of
// populations (an array of slices ranged in turn).
func (a *analysis) leafElements(fn *ssa.Function, s *fnState, d map[ssa.Value]bool) []ssa.Value {
	var out []ssa.Value
	seen := map[ssa.Value]bool{}
	var walk func(d map[ssa.Value]bool, depth int)
	walk = func(d map[ssa.Value]bool, depth int) {
		for _, e := range elementsOf(fn, d) {
			if seen[e] {
				continue
			}
			seen[e] = true
			if container(e.Type()) && depth < 4 {
				walk(a.derived(fn, s, []ssa.Value{e}, false), depth+1)
				continue
			}
			out = append(out, e)
		}
	}
	walk(d, 0)
	return out
}

// overwrites: for each function, the entity parameters whose reference fields
// (labels, properties, vectors, metadata) it overwrites, itself or through a
// callee. Overwriting a field of a copy discards what the copy copied there.
func (a *analysis) overwrites() map[*ssa.Function][]bool {
	out := map[*ssa.Function][]bool{}
	for _, fn := range a.postOrder() {
		s := a.states[fn]
		if s == nil {
			continue
		}
		w := make([]bool, len(fn.Params))
		for j, p := range fn.Params {
			n, _ := structOf(pointee(p.Type()))
			if n == nil || !entityType(n) {
				continue
			}
			d := a.derived(fn, s, []ssa.Value{p}, false)
			for _, b := range fn.Blocks {
				for _, ins := range b.Instrs {
					switch x := ins.(type) {
					case *ssa.Store:
						fa, ok := x.Addr.(*ssa.FieldAddr)
						if !ok || !d[fa.X] {
							continue
						}
						_, st := structOf(pointee(fa.X.Type()))
						// Replacing a pointer to a record no function writes
						// after construction (the shared runtime record)
						// discards nothing the copy copied.
						if st == nil {
							continue
						}
						if ft := st.Field(fa.Field).Type(); !reference(ft) || immutableReferent(ft) {
							continue
						}
						w[j] = true
					case ssa.CallInstruction:
						args := callArgs(x.Common())
						for _, g := range a.calleesOf(x) {
							for m, arg := range args {
								if m < len(out[g]) && out[g][m] && d[arg] {
									w[j] = true
								}
							}
						}
					}
				}
			}
		}
		out[fn] = w
	}
	return out
}

// copiesToCallback: for each function, the function-valued parameters it
// calls with a copy, itself or by handing the parameter to a callee that does.
func (a *analysis) copiesToCallback() map[*ssa.Function][]bool {
	out := map[*ssa.Function][]bool{}
	for _, fn := range a.postOrder() {
		s := a.states[fn]
		if s == nil {
			continue
		}
		cb := make([]bool, len(fn.Params))
		for _, b := range fn.Blocks {
			for _, ins := range b.Instrs {
				call, ok := ins.(ssa.CallInstruction)
				if !ok {
					continue
				}
				c := call.Common()
				if pv, ok := c.Value.(*ssa.Parameter); ok {
					if k := paramIndex(fn, pv); k >= 0 {
						for _, arg := range c.Args {
							if family(arg.Type()) && s.val(arg).Copy {
								cb[k] = true
							}
						}
					}
				}
				args := callArgs(c)
				for _, g := range a.calleesOf(call) {
					for m, arg := range args {
						if m >= len(out[g]) || !out[g][m] {
							continue
						}
						if k := paramIndex(fn, arg); k >= 0 {
							cb[k] = true
						}
					}
				}
			}
		}
		out[fn] = cb
	}
	return out
}

// funcOf: the function a function value denotes, when it is known here.
func (a *analysis) funcOf(v ssa.Value) *ssa.Function {
	switch x := v.(type) {
	case *ssa.Function:
		return a.unwrapped(x)
	case *ssa.MakeClosure:
		if g, ok := x.Fn.(*ssa.Function); ok {
			return a.unwrapped(g)
		}
	case *ssa.ChangeType:
		return a.funcOf(x.X)
	}
	return nil
}

func anyTrue(xs []bool) bool {
	for _, x := range xs {
		if x {
			return true
		}
	}
	return false
}

// ---------------------------------------------------------------- report

type hit struct {
	Class    string   `json:"class"`
	Function string   `json:"function"`
	File     string   `json:"file"`
	Line     int      `json:"line"`
	Detail   string   `json:"detail"`
	Entries  int      `json:"public_entries_reaching"`
	Examples []string `json:"entry_examples,omitempty"`
}

type report struct {
	Author        string            `json:"author"`
	Target        string            `json:"target"`
	Loader        string            `json:"loader"`
	Functions     int               `json:"functions_loaded"`
	Analyzed      int               `json:"functions_analyzed"`
	SummaryRounds int               `json:"summary_rounds"`
	Converged     bool              `json:"summaries_converged"`
	Counts        map[string]int    `json:"counts"`
	Hits          []hit             `json:"hits"`
	Mappers       []string          `json:"population_mappers"`
	Controls      map[string]string `json:"controls"`
	AllControlsOK bool              `json:"all_controls_detected"`
}

func (a *analysis) postOrder() []*ssa.Function {
	inSet := map[*ssa.Function]bool{}
	for _, fn := range a.fns {
		inSet[fn] = true
	}
	seen := map[*ssa.Function]bool{}
	var out []*ssa.Function
	var visit func(fn *ssa.Function)
	visit = func(fn *ssa.Function) {
		if seen[fn] {
			return
		}
		seen[fn] = true
		for _, b := range fn.Blocks {
			for _, ins := range b.Instrs {
				if call, ok := ins.(ssa.CallInstruction); ok {
					for _, g := range a.calleesOf(call) {
						if inSet[g] {
							visit(g)
						}
					}
				}
			}
		}
		out = append(out, fn)
	}
	for _, fn := range a.fns {
		visit(fn)
	}
	return out
}

func (a *analysis) entriesReaching(fn *ssa.Function) (int, []string) {
	start := top(originFn(fn))
	seen := map[*ssa.Function]bool{start: true}
	queue := []*ssa.Function{start}
	var found []string
	count := 0
	for len(queue) > 0 && len(seen) < 4000 {
		f := queue[0]
		queue = queue[1:]
		if exported(f) {
			count++
			if len(found) < 3 {
				found = append(found, fullName(f))
			}
		}
		for _, c := range a.callers[f] {
			c = top(originFn(c))
			if !seen[c] {
				seen[c] = true
				queue = append(queue, c)
			}
		}
	}
	return count, found
}

func allFunctions(p *workcensus.Program) []*ssa.Function {
	seen := map[*ssa.Function]bool{}
	var out []*ssa.Function
	var add func(fn *ssa.Function)
	add = func(fn *ssa.Function) {
		if fn == nil || seen[fn] {
			return
		}
		seen[fn] = true
		if inModule(fn) && fn.Blocks != nil && !isTestFile(fn) {
			out = append(out, fn)
		}
		for _, anon := range fn.AnonFuncs {
			add(anon)
		}
	}
	for _, fn := range p.Functions {
		add(fn)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].String() < out[j].String() })
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
	dir, err := os.MkdirTemp("", "nor160-consumers-"+sha[:8]+"-")
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
		panic("usage: nor160-consumers <tree|git:fullSHA> <output|->")
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
	config.Overlay = map[string][]byte{
		filepath.Join(tree, "pkg/storage/nor160_census_control.go"): []byte(controlFile),
		filepath.Join(tree, "pkg/cypher/nor160_census_control.go"):  []byte(cypherControlFile),
	}
	p, err := workcensus.LoadProgram(config)
	if err != nil {
		panic(err)
	}
	a := &analysis{p: p, fns: allFunctions(p), sums: map[*ssa.Function]*summary{}, states: map[*ssa.Function]*fnState{}, callees: map[ssa.CallInstruction][]*ssa.Function{}, callers: map[*ssa.Function][]*ssa.Function{}}
	for fn, node := range p.CallGraph.Nodes {
		if fn == nil || node == nil {
			continue
		}
		for _, e := range node.Out {
			if e.Callee == nil || e.Callee.Func == nil {
				continue
			}
			g := e.Callee.Func
			if e.Site != nil && inModule(g) && g.Blocks != nil {
				a.callees[e.Site] = append(a.callees[e.Site], g)
			}
			a.callers[g] = append(a.callers[g], fn)
		}
	}
	r := report{Author: "NOR-160 lane (Claude), 30 October 2026", Target: spec, Loader: "maintained internal/workcensus.LoadProgram", Functions: len(p.Functions), Analyzed: len(a.fns), Counts: map[string]int{}, Controls: map[string]string{}}

	// Summaries to a fixpoint, callees before callers.
	collectMutatedTypes(a.fns)
	order := a.postOrder()
	for round := 1; round <= 40; round++ {
		r.SummaryRounds = round
		changed := false
		for _, fn := range order {
			s := a.analyzeFn(fn)
			a.states[fn] = s
			sum := a.summarize(fn, s)
			if !a.equalSummary(a.sums[fn], sum) {
				a.sums[fn] = sum
				changed = true
			}
		}
		if !changed {
			r.Converged = true
			break
		}
	}

	drops := a.paramDrops()
	overwrite := a.overwrites()
	callbackCopy := a.copiesToCallback()
	seenHit := map[string]bool{}
	add := func(class string, fn *ssa.Function, p token.Pos, detail string) {
		file, line := pos(fn, p)
		key := class + "|" + fullName(fn) + "|" + file + fmt.Sprint(line) + "|" + detail
		if seenHit[key] {
			return
		}
		seenHit[key] = true
		n, ex := a.entriesReaching(fn)
		r.Hits = append(r.Hits, hit{Class: class, Function: fullName(fn), File: file, Line: line, Detail: detail, Entries: n, Examples: ex})
	}
	mappers := map[string]*ssa.Function{}

	for _, fn := range a.fns {
		s := a.states[fn]

		// K0: exported functions returning or visiting retained values.
		if exported(fn) {
			for _, b := range fn.Blocks {
				for _, ins := range b.Instrs {
					switch x := ins.(type) {
					case *ssa.Return:
						for _, v := range x.Results {
							if family(v.Type()) && s.val(v).Retained {
								add("K0", fn, x.Pos(), "returns a retained "+v.Type().String())
							}
						}
					case *ssa.Call:
						c := x.Common()
						if pv, ok := c.Value.(*ssa.Parameter); ok && paramIndex(fn, pv) >= 0 && !isBoolResult(c.Signature()) {
							for _, arg := range c.Args {
								if family(arg.Type()) && s.val(arg).Retained {
									add("K0", fn, x.Pos(), "visits a retained "+arg.Type().String())
								}
							}
						}
					}
				}
			}
		}

		// K1: a callback that overwrites fields, given to a function that hands it copies.
		for _, b := range fn.Blocks {
			for _, ins := range b.Instrs {
				call, ok := ins.(ssa.CallInstruction)
				if !ok {
					continue
				}
				args := callArgs(call.Common())
				for _, t := range a.calleesOf(call) {
					for k, arg := range args {
						if k >= len(callbackCopy[t]) || !callbackCopy[t][k] {
							continue
						}
						if g := a.funcOf(arg); g != nil && anyTrue(overwrite[g]) {
							add("K1", fn, call.Pos(), strings.TrimPrefix(fullName(t), module+"/")+" hands copies to "+strings.TrimPrefix(fullName(g), module+"/")+", which overwrites their fields")
						}
					}
				}
			}
		}

		for _, b := range fn.Blocks {
			for _, ins := range b.Instrs {
				v, isValue := ins.(ssa.Value)
				if !isValue || !family(v.Type()) {
					// tuples: the call value carries the tuple
					if call, ok := ins.(*ssa.Call); ok {
						v, isValue = call, true
					} else {
						continue
					}
				}
				if !s.intro[v] {
					continue
				}
				if call, ok := v.(*ssa.Call); ok && a.receivedAcross(fn, call) {
					continue // the caller of a public read received it
				}
				d := a.derived(fn, s, []ssa.Value{v}, true)

				// K1 on a copy handed to a callee that selects it out.
				if g, j, ok := a.droppingCallIn(fn, d, drops); ok {
					add("K1", fn, ins.Pos(), "copy "+copyLabel(v)+" is selected out inside "+strings.TrimPrefix(fullName(g), module+"/")+" (parameter "+fmt.Sprint(j)+")")
				}

				// K1 on a copy whose fields a callee overwrites.
				for _, bb := range fn.Blocks {
					for _, ii := range bb.Instrs {
						call, ok := ii.(ssa.CallInstruction)
						if !ok {
							continue
						}
						args := callArgs(call.Common())
						for _, g := range a.calleesOf(call) {
							for m, arg := range args {
								if m < len(overwrite[g]) && overwrite[g][m] && d[arg] {
									add("K1", fn, ins.Pos(), "fields of copy "+copyLabel(v)+" are overwritten inside "+strings.TrimPrefix(fullName(g), module+"/"))
								}
							}
						}
					}
				}

				// K1 / K1e / K2 on the copy itself.
				kind, stored := a.mayDrop(fn, s, ins, d)
				if kind != noDrop {
					class := "K1"
					if kind == dropError {
						class = "K1e"
					}
					if stored && kind == dropPlain {
						class = "K2"
					}
					add(class, fn, ins.Pos(), "copy "+copyLabel(v)+" can be left undelivered")
				}

				// Elements of a copied population.
				if t := v.Type(); container(t) || isTupleWithContainer(t) {
					d := a.derived(fn, s, []ssa.Value{v}, false)
					delivered := false
					for _, bb := range fn.Blocks {
						for _, ii := range bb.Instrs {
							if a.deliversWith(fn, ii, func(x ssa.Value) bool { return d[x] }) {
								delivered = true
							}
						}
					}
					for _, e := range a.leafElements(fn, s, d) {
						ei, _ := e.(ssa.Instruction)
						if ei == nil {
							continue
						}
						if !delivered {
							ed := a.derived(fn, s, []ssa.Value{e}, true)
							if k, _ := a.mayDrop(fn, s, ei, ed); k == dropPlain {
								add("K1", fn, ei.Pos(), "element of copied population "+copyLabel(v)+" is selected out after the copy")
							}
						}
						// K3: the loop over the population can be left early by a call's result.
						if loop := loopOf(fn, ei.Block()); loop != nil {
							for lb := range loop {
								iff, ok := lb.Instrs[len(lb.Instrs)-1].(*ssa.If)
								if !ok {
									continue
								}
								for _, succ := range lb.Succs {
									if !loop[succ] && callDecided(iff.Cond, 0) {
										add("K3", fn, iff.Pos(), "loop over copied population "+copyLabel(v)+" can stop early")
									}
								}
							}
						}
					}
				}
			}
		}

		// K3 for local buffers filled with copies (pre-read populations).
		for _, b := range fn.Blocks {
			for _, ins := range b.Instrs {
				ms, ok := ins.(ssa.Value)
				if !ok || !container(ms.Type()) || !family(ms.Type()) || !s.val(ms).Copy {
					continue
				}
				switch x := ms.(type) {
				case *ssa.Phi, *ssa.MakeSlice:
				case *ssa.Call:
					if bi, isB := x.Call.Value.(*ssa.Builtin); !isB || bi.Name() != "append" {
						continue
					}
				default:
					continue
				}
				d := a.derived(fn, s, []ssa.Value{ms}, false)
				for _, e := range elementsOf(fn, d) {
					ei, _ := e.(ssa.Instruction)
					if ei == nil {
						continue
					}
					if loop := loopOf(fn, ei.Block()); loop != nil {
						for lb := range loop {
							iff, ok := lb.Instrs[len(lb.Instrs)-1].(*ssa.If)
							if !ok {
								continue
							}
							for _, succ := range lb.Succs {
								if !loop[succ] && callDecided(iff.Cond, 0) {
									add("K3", fn, iff.Pos(), "loop over a buffer of copies can stop early")
								}
							}
						}
					}
				}
			}
		}

		// K4: one value merging an uncopied value and a copy of it.
		checkMerge := func(p token.Pos, inputs []ssa.Value) {
			for _, x := range inputs {
				ox := s.val(x)
				if ox.empty() || ox.Copy || !family(x.Type()) {
					continue
				}
				for _, y := range inputs {
					src, ok := s.src[y]
					if !ok {
						continue
					}
					if src.overlaps(ox) {
						add("K4", fn, p, "chooses between a value and a copy of it")
						return
					}
				}
			}
		}
		var rets [][]ssa.Value
		var retPos token.Pos
		for _, b := range fn.Blocks {
			for _, ins := range b.Instrs {
				switch x := ins.(type) {
				case *ssa.Phi:
					checkMerge(x.Pos(), x.Edges)
				case *ssa.Return:
					for i, res := range x.Results {
						for len(rets) <= i {
							rets = append(rets, nil)
						}
						rets[i] = append(rets[i], res)
					}
					if !retPos.IsValid() {
						retPos = x.Pos()
					}
				}
			}
		}
		for _, in := range rets {
			checkMerge(retPos, in)
		}

		// K4b: Copier dispatch sites, and a second CallerCopy after a first.
		var callerCopies []*ssa.Call
		for _, b := range fn.Blocks {
			for _, ins := range b.Instrs {
				switch x := ins.(type) {
				case *ssa.TypeAssert:
					if it, ok := x.AssertedType.Underlying().(*types.Interface); ok {
						for i := 0; i < it.NumMethods(); i++ {
							if it.Method(i).Name() == "CallerCopy" {
								add("K4b", fn, x.Pos(), "asserts a Copier")
							}
						}
					}
				case *ssa.Call:
					c := x.Common()
					if c.IsInvoke() && c.Method.Name() == "CallerCopy" {
						callerCopies = append(callerCopies, x)
					} else if f := c.StaticCallee(); f != nil && originFn(f).Name() == "CallerCopy" {
						callerCopies = append(callerCopies, x)
					}
				}
			}
		}
		for _, first := range callerCopies {
			for _, second := range callerCopies {
				if first == second {
					continue
				}
				later := first.Block() == second.Block() && blockIndex(first) < blockIndex(second) || reach(first.Block())[second.Block()]
				if later {
					add("K4b", fn, second.Pos(), "a second CallerCopy can follow a first")
				}
			}
		}

		// K5: loops storing copies of the elements they iterate; mappers.
		for _, b := range fn.Blocks {
			if loopOf(fn, b) == nil {
				continue
			}
			for _, ins := range b.Instrs {
				call, ok := ins.(*ssa.Call)
				if !ok {
					continue
				}
				if _, isParam := call.Common().Value.(*ssa.Parameter); isParam && storedInSlice(call) && anyElementArg(call.Common().Args) {
					mappers[fullName(fn)] = originFn(fn)
					continue
				}
				if _, intro := s.src[call]; intro && storedInSlice(call) && anyElementArg(callArgs(call.Common())) {
					add("K5", fn, call.Pos(), "loop stores copies of the elements it iterates ("+copyLabel(call)+")")
				}
			}
		}

		// K6: an unconditional copier applied to a value made for this read.
		for _, b := range fn.Blocks {
			for _, ins := range b.Instrs {
				call, ok := ins.(*ssa.Call)
				if !ok {
					continue
				}
				args := callArgs(call.Common())
				for _, g := range a.calleesOf(call) {
					sg := a.sums[g]
					if sg == nil {
						continue
					}
					for j, cp := range sg.Copies {
						if !cp || j >= len(args) {
							continue
						}
						passes, returnsCopy := false, false
						for _, rr := range sg.Ret {
							if rr.Params&bit(j) != 0 {
								passes = true
							}
							returnsCopy = returnsCopy || rr.Copy
						}
						if passes || !returnsCopy {
							continue // a dispatcher decides, or the copy is kept by a retention point
						}
						if s.val(args[j]).private() {
							add("K6", fn, call.Pos(), "copies a value made for this read ("+copyLabel(call)+")")
						}
					}
				}
			}
		}

		// K7: a copy of a family struct keeping a reference field of its source.
		for _, b := range fn.Blocks {
			for _, ins := range b.Instrs {
				al, ok := ins.(*ssa.Alloc)
				if !ok {
					continue
				}
				if _, intro := s.src[al]; !intro {
					continue
				}
				n, st := structOf(pointee(al.Type()))
				if st == nil {
					continue
				}
				if !s.src[al].Retained && !s.src[al].Unknown {
					continue // shared with a value the caller owns; callers passing retained values are judged below
				}
				for _, f := range sharedFields(al, st) {
					add("K7", fn, al.Pos(), n.Obj().Name()+"."+f+" is shared with a retained source")
				}
			}
		}
		for _, b := range fn.Blocks {
			for _, ins := range b.Instrs {
				call, ok := ins.(*ssa.Call)
				if !ok {
					continue
				}
				args := callArgs(call.Common())
				for _, g := range a.calleesOf(call) {
					sg := a.sums[g]
					if sg == nil {
						continue
					}
					for j, fields := range sg.Partial {
						if j < len(args) && s.val(args[j]).Retained {
							for _, f := range fields {
								add("K7", fn, call.Pos(), strings.TrimPrefix(fullName(g), module+"/")+" keeps "+f+" of a retained value shared")
							}
						}
					}
				}
			}
		}
	}

	// K5m: every population mapper (a loop storing a function parameter's
	// results) and every call to one, with the function it applies. Counted
	// with the other classes; the one slice owner must be among them.
	for m, mfn := range mappers {
		r.Mappers = append(r.Mappers, m)
		add("K5m", mfn, mfn.Pos(), "population mapper: a loop stores a function parameter's results")
	}
	sort.Strings(r.Mappers)
	for _, fn := range a.fns {
		for _, b := range fn.Blocks {
			for _, ins := range b.Instrs {
				call, ok := ins.(*ssa.Call)
				if !ok {
					continue
				}
				f := call.Common().StaticCallee()
				if f == nil {
					continue
				}
				name := fullName(f)
				if _, isMapper := mappers[name]; !isMapper {
					continue
				}
				applied := "a function value"
				for _, arg := range call.Common().Args {
					switch x := arg.(type) {
					case *ssa.Function:
						applied = strings.TrimPrefix(fullName(x), module+"/")
					case *ssa.MakeClosure:
						if g, ok := x.Fn.(*ssa.Function); ok {
							applied = "closure " + strings.TrimPrefix(fullName(g), module+"/")
						}
					}
				}
				add("K5m", fn, call.Pos(), "calls population mapper "+strings.TrimPrefix(name, module+"/")+" with "+applied)
			}
		}
	}
	sort.Slice(r.Hits, func(i, j int) bool {
		if r.Hits[i].Class != r.Hits[j].Class {
			return r.Hits[i].Class < r.Hits[j].Class
		}
		if r.Hits[i].File != r.Hits[j].File {
			return r.Hits[i].File < r.Hits[j].File
		}
		return r.Hits[i].Line < r.Hits[j].Line
	})
	for _, h := range r.Hits {
		r.Counts[h.Class]++
	}
	r.AllControlsOK = true
	for _, c := range controls {
		found := false
		for _, h := range r.Hits {
			if h.Class == c.class && h.Function == c.fn {
				found = true
			}
		}
		state := "detected"
		ok := found
		if c.clean {
			ok = !found
			state = "clean"
			if found {
				state = "FALSELY REPORTED"
			}
		} else if !found {
			state = "MISSED"
		}
		if !ok {
			r.AllControlsOK = false
		}
		r.Controls[c.class+" "+c.fn] = state
	}
	data, err := json.MarshalIndent(r, "", " ")
	if err != nil {
		panic(err)
	}
	if output == "-" {
		fmt.Println(string(data))
		return
	}
	if err := os.WriteFile(output, data, 0o644); err != nil {
		panic(err)
	}
}

// mutatedTypes: struct types whose fields some function writes in a value it
// did not allocate itself. A pointer to any other struct type is an immutable
// record: sharing it shares nothing a caller can change.
var mutatedTypes = map[*types.Named]bool{}

func immutableReferent(t types.Type) bool {
	p := pointee(t)
	if p == nil {
		return false
	}
	n, st := structOf(p)
	if st == nil {
		return false
	}
	return !mutatedTypes[n]
}

func collectMutatedTypes(fns []*ssa.Function) {
	for _, fn := range fns {
		for _, b := range fn.Blocks {
			for _, ins := range b.Instrs {
				st, ok := ins.(*ssa.Store)
				if !ok {
					continue
				}
				fa, ok := st.Addr.(*ssa.FieldAddr)
				if !ok {
					continue
				}
				if _, own := fa.X.(*ssa.Alloc); own {
					continue
				}
				if n, _ := structOf(pointee(fa.X.Type())); n != nil {
					mutatedTypes[n] = true
				}
			}
		}
	}
}

func isTupleWithContainer(t types.Type) bool {
	tu, ok := t.(*types.Tuple)
	if !ok {
		return false
	}
	for i := 0; i < tu.Len(); i++ {
		if container(tu.At(i).Type()) && family(tu.At(i).Type()) {
			return true
		}
	}
	return false
}

func blockIndex(ins ssa.Instruction) int {
	for i, x := range ins.Block().Instrs {
		if x == ins {
			return i
		}
	}
	return -1
}

func copyLabel(v ssa.Value) string {
	switch x := v.(type) {
	case *ssa.Call:
		c := x.Common()
		if c.IsInvoke() {
			return c.Value.Type().String() + "." + c.Method.Name()
		}
		if f := c.StaticCallee(); f != nil {
			return strings.TrimPrefix(fullName(f), module+"/")
		}
		return "dynamic call"
	case *ssa.Alloc:
		return "new " + strings.TrimPrefix(pointee(x.Type()).String(), module+"/")
	}
	return v.Name()
}

// storedInSlice: the call's result is appended to a slice or stored at an index.
func storedInSlice(call *ssa.Call) bool {
	refs := call.Referrers()
	if refs == nil {
		return false
	}
	for _, r := range *refs {
		switch x := r.(type) {
		case *ssa.Store:
			if _, ok := x.Addr.(*ssa.IndexAddr); ok && x.Val == call {
				return true
			}
		case *ssa.Call:
			if bi, ok := x.Call.Value.(*ssa.Builtin); ok && bi.Name() == "append" {
				return true
			}
		case *ssa.Extract:
			if x.Index == 0 && x.Referrers() != nil {
				for _, rr := range *x.Referrers() {
					if st, ok := rr.(*ssa.Store); ok {
						if _, ok := st.Addr.(*ssa.IndexAddr); ok {
							return true
						}
					}
					if c, ok := rr.(*ssa.Call); ok {
						if bi, ok := c.Call.Value.(*ssa.Builtin); ok && bi.Name() == "append" {
							return true
						}
					}
				}
			}
		}
	}
	return false
}

// anyElementArg: an argument read out of a slice, array or map being iterated.
func anyElementArg(args []ssa.Value) bool {
	for _, arg := range args {
		switch x := arg.(type) {
		case *ssa.UnOp:
			if _, ok := x.X.(*ssa.IndexAddr); ok && x.Op == token.MUL {
				return true
			}
		case *ssa.Index, *ssa.Lookup:
			return true
		case *ssa.Extract:
			if _, ok := x.Tuple.(*ssa.Next); ok {
				return true
			}
		}
	}
	return false
}

// sharedFields: reference fields of a copied struct that keep the source's
// value: a whole copy never overwrites them, or they are filled straight
// from the same field of a value of the same type.
func sharedFields(al *ssa.Alloc, st *types.Struct) []string {
	elem := pointee(al.Type())
	refs := al.Referrers()
	if refs == nil {
		return nil
	}
	whole := false
	overwritten := map[int]bool{}
	direct := map[int]bool{}
	for _, r := range *refs {
		switch x := r.(type) {
		case *ssa.Store:
			if x.Addr == al && types.Identical(x.Val.Type(), elem) {
				if _, isCall := x.Val.(*ssa.Call); !isCall {
					whole = true
				}
			}
		case *ssa.FieldAddr:
			if fr := x.Referrers(); fr != nil {
				for _, fref := range *fr {
					sto, ok := fref.(*ssa.Store)
					if !ok || sto.Addr != x {
						continue
					}
					overwritten[x.Field] = true
					if ld, ok := sto.Val.(*ssa.UnOp); ok && ld.Op == token.MUL {
						if fa, ok := ld.X.(*ssa.FieldAddr); ok && fa.Field == x.Field && types.Identical(pointee(fa.X.Type()), elem) {
							direct[x.Field] = true
						}
					}
				}
			}
		}
	}
	var out []string
	for i := 0; i < st.NumFields(); i++ {
		f := st.Field(i)
		if !reference(f.Type()) || immutableReferent(f.Type()) {
			continue
		}
		if (whole && !overwritten[i]) || direct[i] {
			out = append(out, f.Name())
		}
	}
	return out
}
