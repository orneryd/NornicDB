// NOR-160 lane (Claude), 30 October 2026. Analysis-only client, not product code.
// Consumer-closed population over the maintained workcensus typed/SSA loader:
//
//  1. every consumer of a node or edge delivery: each function value passed to any
//     call as a callback that receives nodes or edges (stream visitors, transaction
//     stream emitters, wrapper forwarders; storage_reader marks a pkg/storage
//     function or storage interface method as the deliverer), with every
//     copier it applies to its delivery, directly or through the helpers and
//     closures it hands the delivery to;
//  2. every overlay view: each caller of the write-behind overlay owner and of the
//     exported WriteBehindBuffer readers, with every copier applied to the result;
//  3. every caller-copy implementation: each type implementing callerowned.Copier,
//     each CallerCopy/callerOwned function, and the exported reference fields of the
//     copied type that the implementation (with its module helpers) never assigns.
//
// Planted controls (virtual file of pkg/storage) must be reported: a consumer that
// copies an owned delivery again, and a Copier omitting a graph-bearing field; a
// complete Copier must not be. No product code executes.
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

// NOR160ControlSecondCopy consumes an owned label-stream delivery and copies it again.
func NOR160ControlSecondCopy(b *BadgerEngine) []*Node {
	var out []*Node
	_ = b.StreamNodesByLabelProjectedInScope("", "L", nil, func(n *Node) error {
		out = append(out, copyNode(n))
		return nil
	})
	return out
}

// nor160ControlCloneMap clones a property map with no helper of the tree under
// analysis, so the controls type-check on every revision (release included).
func nor160ControlCloneMap(m map[string]any) map[string]any {
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// NOR160ControlCapturedEmitter copies an owned delivery again inside a captured
// emitter closure, the shape of a transaction stream emitter.
func NOR160ControlCapturedEmitter(b *BadgerEngine) []*Node {
	var out []*Node
	emit := func(n *Node) {
		out = append(out, copyNode(n))
	}
	_ = b.StreamNodesByLabelProjectedInScope("", "L", nil, func(n *Node) error {
		emit(n)
		return nil
	})
	return out
}

// NOR160ControlQueueCopier is a second entity copier: it rebuilds a node from
// its argument and clones its reference fields without the copy owner.
func NOR160ControlQueueCopier(node *Node) *Node {
	clone := *node
	clone.Labels = append([]string(nil), node.Labels...)
	clone.Properties = nor160ControlCloneMap(node.Properties)
	return &clone
}

// NOR160ControlSliceCopier is a hand-written slice copier beside the slice owner.
func NOR160ControlSliceCopier(nodes []*Node) []*Node {
	out := make([]*Node, len(nodes))
	for i, n := range nodes {
		out[i] = copyNode(n)
	}
	return out
}

// NOR160ControlDispatch is a second Copier dispatch rule beside the owner's.
func NOR160ControlDispatch(v any) any {
	if c, ok := v.(interface{ CallerCopy() any }); ok {
		return c.CallerCopy()
	}
	return v
}

// NOR160ControlPartialHit omits its relationship from its caller copy.
type NOR160ControlPartialHit struct {
	Props map[string]any
	Rel   *Edge
}

func (h NOR160ControlPartialHit) CallerCopy() any {
	h.Props = nor160ControlCloneMap(h.Props)
	return h
}

// NOR160ControlCompleteHit copies every reference field.
type NOR160ControlCompleteHit struct {
	Props map[string]any
	Rel   *Edge
}

func (h NOR160ControlCompleteHit) CallerCopy() any {
	h.Props = nor160ControlCloneMap(h.Props)
	h.Rel = copyEdge(h.Rel)
	return h
}
`

// Structural copiers of a delivered entity or value. shapeNode copies only when its
// owned argument is not the constant true.
var copierNames = map[string]bool{
	storagePkg + ".copyNode": true, storagePkg + ".copyNodeWithoutEmbeddings": true, storagePkg + ".copyEdge": true,
	storagePkg + ".CopyNode": true, storagePkg + ".CopyEdge": true, storagePkg + ".projectCachedNodeForRead": true,
	storagePkg + ".shapeNode": true, storagePkg + ".copyImagesForCaller": true, storagePkg + ".copyPropertyValue": true,
	"(*" + storagePkg + ".Node).CallerCopy": true, "(*" + storagePkg + ".Edge).CallerCopy": true,
	module + "/pkg/callerowned.Value": true, module + "/pkg/callerowned.Map": true, module + "/pkg/callerowned.List": true,
	module + "/pkg/callerowned.Rows": true, module + "/pkg/callerowned.RowsObservingMaps": true, module + "/pkg/callerowned.Of": true,
}

var tree string

type consumer struct {
	Consumer    string   `json:"consumer"`
	File        string   `json:"file"`
	Line        int      `json:"line"`
	Callee      string   `json:"delivered_by"`
	FromStorage bool     `json:"storage_reader"`
	Site        string   `json:"call_site"`
	Copies      []string `json:"copies_of_delivery,omitempty"`
	Retains     bool     `json:"retains_delivery"`
}

type view struct {
	Function string   `json:"function"`
	File     string   `json:"file"`
	Line     int      `json:"line"`
	Reader   string   `json:"reads"`
	Copies   []string `json:"copies_of_result,omitempty"`
}

type copierImpl struct {
	Implementation string   `json:"implementation"`
	File           string   `json:"file"`
	Line           int      `json:"line"`
	Type           string   `json:"copied_type"`
	Fields         []string `json:"reference_fields"`
	Unassigned     []string `json:"unassigned_reference_fields,omitempty"`
}

type report struct {
	Author        string          `json:"author"`
	Target        string          `json:"target"`
	Loader        string          `json:"loader"`
	Functions     int             `json:"functions_loaded"`
	Consumers     []consumer      `json:"delivery_consumers"`
	Views         []view          `json:"overlay_views"`
	CopierTypes   []string        `json:"copier_types"`
	Copiers       []copierImpl    `json:"caller_copy_implementations"`
	CopySites     []string        `json:"copier_call_sites"`
	EntityCopiers []string        `json:"independent_entity_copiers"`
	SliceCopiers  []string        `json:"slice_copiers"`
	Dispatchers   []string        `json:"copier_dispatch_rules"`
	Controls      map[string]bool `json:"controls_detected"`
	AllControlsOK bool            `json:"all_controls_detected"`
}

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

func origin(fn *ssa.Function) *ssa.Function {
	if fn != nil && fn.Origin() != nil {
		return fn.Origin()
	}
	return fn
}

func fullName(fn *ssa.Function) string {
	fn = origin(fn)
	if fn.Pkg == nil || fn.Signature.Recv() != nil {
		return fn.String()
	}
	return fn.Pkg.Pkg.Path() + "." + fn.Name()
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

func calleeName(c *ssa.CallCommon) string {
	if c.IsInvoke() {
		return c.Value.Type().String() + "." + c.Method.Name()
	}
	if f := staticCallee(c); f != nil {
		return fullName(f)
	}
	return ""
}

// storageEntity: *storage.Node, *storage.Edge, or a slice of either.
func storageEntity(t types.Type) bool {
	if s, ok := types.Unalias(t).(*types.Slice); ok {
		t = s.Elem()
	}
	p, ok := types.Unalias(t).(*types.Pointer)
	if !ok {
		return false
	}
	n, ok := types.Unalias(p.Elem()).(*types.Named)
	return ok && n.Obj().Pkg() != nil && n.Obj().Pkg().Path() == storagePkg && (n.Obj().Name() == "Node" || n.Obj().Name() == "Edge")
}

// deliveryParams: indexes of a callback signature's storage-entity parameters.
func deliveryParams(t types.Type) []int {
	sig, ok := t.Underlying().(*types.Signature)
	if !ok {
		return nil
	}
	var out []int
	for i := 0; i < sig.Params().Len(); i++ {
		if storageEntity(sig.Params().At(i).Type()) {
			out = append(out, i)
		}
	}
	return out
}

// deliversFromStorage: the call hands the callback to a storage reader.
func deliversFromStorage(c *ssa.CallCommon) bool {
	if c.IsInvoke() {
		n, ok := types.Unalias(c.Value.Type()).(*types.Named)
		return ok && n.Obj().Pkg() != nil && n.Obj().Pkg().Path() == storagePkg
	}
	f := staticCallee(c)
	for f != nil && f.Pkg == nil && f.Parent() != nil {
		f = f.Parent()
	}
	return f != nil && f.Pkg != nil && f.Pkg.Pkg.Path() == storagePkg
}

func callbackFunction(v ssa.Value) *ssa.Function {
	switch x := v.(type) {
	case *ssa.Function:
		return x
	case *ssa.MakeClosure:
		f, _ := x.Fn.(*ssa.Function)
		return f
	case *ssa.ChangeType:
		return callbackFunction(x.X)
	}
	return nil
}

// derives reports whether v is (a value-preserving view of) one of roots.
func derives(v ssa.Value, roots map[ssa.Value]bool, seen map[ssa.Value]bool) bool {
	if v == nil || seen[v] {
		return false
	}
	seen[v] = true
	if roots[v] {
		return true
	}
	switch x := v.(type) {
	case *ssa.Phi:
		for _, e := range x.Edges {
			if derives(e, roots, seen) {
				return true
			}
		}
	case *ssa.ChangeType:
		return derives(x.X, roots, seen)
	case *ssa.MakeInterface:
		return derives(x.X, roots, seen)
	case *ssa.TypeAssert:
		return derives(x.X, roots, seen)
	case *ssa.Extract:
		return derives(x.Tuple, roots, seen)
	case *ssa.Field:
		return derives(x.X, roots, seen)
	case *ssa.FieldAddr:
		return derives(x.X, roots, seen)
	case *ssa.Index:
		return derives(x.X, roots, seen)
	case *ssa.IndexAddr:
		return derives(x.X, roots, seen)
	case *ssa.Lookup:
		return derives(x.X, roots, seen)
	case *ssa.Slice:
		return derives(x.X, roots, seen)
	case *ssa.Next:
		return derives(x.Iter, roots, seen)
	case *ssa.Range:
		return derives(x.X, roots, seen)
	case *ssa.UnOp:
		if x.Op == token.MUL {
			return derives(x.X, roots, seen)
		}
	}
	return false
}

func constTrue(v ssa.Value) bool {
	k, ok := v.(*ssa.Const)
	return ok && k.Value != nil && k.Value.String() == "true"
}

// copiesOf lists every copier fn applies to a value derived from roots, following
// the module functions and closures it hands such a value to (depth-bounded).
func copiesOf(fn *ssa.Function, roots map[ssa.Value]bool, depth int, visited map[string]bool) []string {
	if fn == nil || fn.Blocks == nil || depth > 4 {
		return nil
	}
	var out []string
	for _, b := range fn.Blocks {
		for _, ins := range b.Instrs {
			call, ok := ins.(ssa.CallInstruction)
			if !ok {
				continue
			}
			c := call.Common()
			name := calleeName(c)
			for i, arg := range c.Args {
				if !derives(arg, roots, map[ssa.Value]bool{}) {
					continue
				}
				file, line := pos(fn, call.Pos())
				at := fmt.Sprintf("%s:%d", file, line)
				if copierNames[name] {
					if strings.HasSuffix(name, ".shapeNode") && len(c.Args) == 4 {
						if constTrue(c.Args[3]) {
							continue // reshape in place
						}
						if _, isConst := c.Args[3].(*ssa.Const); !isConst {
							out = append(out, fmt.Sprintf("%s shapeNode(owned=%s) at %s (copies when not owned)", fn.Name(), c.Args[3].Name(), at))
							continue
						}
					}
					out = append(out, fmt.Sprintf("%s %s at %s", fn.Name(), name[strings.LastIndex(name, "/")+1:], at))
					continue
				}
				callee := staticCallee(c)
				if callee == nil {
					callee = capturedCallee(fn, c)
				}
				if callee == nil || !inModule(callee) || callee.Blocks == nil || i >= len(callee.Params) {
					continue
				}
				key := fmt.Sprintf("%s#%d", callee.String(), i)
				if visited[key] {
					continue
				}
				visited[key] = true
				out = append(out, copiesOf(callee, map[ssa.Value]bool{callee.Params[i]: true}, depth+1, visited)...)
			}
		}
	}
	return out
}

// capturedCallee resolves a call through a closure the function captured from
// its parent (a transaction stream emitter): a free variable bound to the
// closure, or to a variable the parent stores the closure in.
func capturedCallee(fn *ssa.Function, c *ssa.CallCommon) *ssa.Function {
	if c.IsInvoke() {
		return nil
	}
	v := c.Value
	byRef := false
	if load, ok := v.(*ssa.UnOp); ok && load.Op == token.MUL {
		v, byRef = load.X, true
	}
	fv, ok := v.(*ssa.FreeVar)
	if !ok || fn.Parent() == nil {
		return nil
	}
	index := -1
	for i, free := range fn.FreeVars {
		if free == fv {
			index = i
		}
	}
	if index < 0 {
		return nil
	}
	for _, b := range fn.Parent().Blocks {
		for _, ins := range b.Instrs {
			mc, ok := ins.(*ssa.MakeClosure)
			if !ok || mc.Fn != fn || index >= len(mc.Bindings) {
				continue
			}
			binding := mc.Bindings[index]
			if !byRef {
				if f := callbackFunction(binding); f != nil {
					return f
				}
				continue
			}
			// The parent stores the closure into the captured variable.
			for _, pb := range fn.Parent().Blocks {
				for _, pins := range pb.Instrs {
					if st, ok := pins.(*ssa.Store); ok && st.Addr == binding {
						if f := callbackFunction(st.Val); f != nil {
							return f
						}
					}
				}
			}
		}
	}
	return nil
}

// rebuiltEntityFields: exported reference fields fn assigns on a storage Node or
// Edge it initialised by copying another entity (clone := *node).
func rebuiltEntityFields(fn *ssa.Function) []string {
	copies := map[ssa.Value]*types.Struct{}
	for _, b := range fn.Blocks {
		for _, ins := range b.Instrs {
			st, ok := ins.(*ssa.Store)
			if !ok {
				continue
			}
			load, ok := st.Val.(*ssa.UnOp)
			if !ok || load.Op != token.MUL {
				continue
			}
			if _, isAlloc := st.Addr.(*ssa.Alloc); !isAlloc || !storageEntity(st.Addr.Type()) {
				continue
			}
			p := types.Unalias(st.Addr.Type()).(*types.Pointer)
			if s, ok := p.Elem().Underlying().(*types.Struct); ok {
				copies[st.Addr] = s
			}
		}
	}
	seen := map[string]bool{}
	var out []string
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
			s, ok := copies[fa.X]
			if !ok {
				continue
			}
			f := s.Field(fa.Field)
			if f.Exported() && refBearing(f.Type(), map[types.Type]bool{}) && !seen[f.Name()] {
				seen[f.Name()] = true
				out = append(out, f.Name())
			}
		}
	}
	sort.Strings(out)
	return out
}

// declaresCallerCopy: an interface type that declares CallerCopy.
func declaresCallerCopy(t types.Type) bool {
	iface, ok := t.Underlying().(*types.Interface)
	if !ok {
		return false
	}
	for i := 0; i < iface.NumMethods(); i++ {
		if iface.Method(i).Name() == "CallerCopy" {
			return true
		}
	}
	return false
}

// elementCopier: a structural copier, a type's own copier, or the owner's
// typed walk.
func elementCopier(c *ssa.CallCommon) bool {
	name := calleeName(c)
	if copierNames[name] && !strings.HasSuffix(name, ".shapeNode") {
		return true
	}
	if strings.HasSuffix(name, ".shapeNode") && len(c.Args) == 4 && !constTrue(c.Args[3]) {
		return true
	}
	if f := staticCallee(c); f != nil {
		n := origin(f).Name()
		return n == "CallerCopy" || n == "callerOwned" || strings.HasSuffix(fullName(f), "/pkg/callerowned.Of")
	}
	return false
}

// inCycle: the block lies on a loop of its function.
func inCycle(start *ssa.BasicBlock) bool {
	seen := map[*ssa.BasicBlock]bool{}
	stack := append([]*ssa.BasicBlock(nil), start.Succs...)
	for len(stack) > 0 {
		b := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if b == start {
			return true
		}
		if seen[b] {
			continue
		}
		seen[b] = true
		stack = append(stack, b.Succs...)
	}
	return false
}

// storedAsElement: the call's result is stored into a slice element or
// appended to a slice.
func storedAsElement(call *ssa.Call) bool {
	refs := call.Referrers()
	if refs == nil {
		return false
	}
	for _, ref := range *refs {
		switch x := ref.(type) {
		case *ssa.Store:
			if _, ok := x.Addr.(*ssa.IndexAddr); ok && x.Val == call {
				return true
			}
		case *ssa.MakeInterface, *ssa.ChangeType:
			if v, ok := ref.(ssa.Value); ok && v.Referrers() != nil {
				for _, inner := range *v.Referrers() {
					if st, ok := inner.(*ssa.Store); ok {
						if _, ok := st.Addr.(*ssa.IndexAddr); ok {
							return true
						}
					}
				}
			}
		case *ssa.Call:
			if b, ok := x.Call.Value.(*ssa.Builtin); ok && b.Name() == "append" {
				return true
			}
		}
	}
	return false
}

// retains: fn stores a delivery-derived value into memory that outlives the call
// (a field, global, captured variable or append into one).
func retains(fn *ssa.Function, roots map[ssa.Value]bool) bool {
	for _, b := range fn.Blocks {
		for _, ins := range b.Instrs {
			switch x := ins.(type) {
			case *ssa.Store:
				if derives(x.Val, roots, map[ssa.Value]bool{}) {
					return true
				}
			case *ssa.Call:
				if b, ok := x.Call.Value.(*ssa.Builtin); ok && b.Name() == "append" {
					for _, arg := range x.Call.Args[1:] {
						if derives(arg, roots, map[ssa.Value]bool{}) {
							return true
						}
					}
				}
			}
		}
	}
	return false
}

// overlay readers: the write-behind overlay owner and exported buffer readers.
func overlayReader(name string) bool {
	if name == "(*"+storagePkg+".BadgerEngine).readBufferedEntities" {
		return true
	}
	prefix := "(*" + storagePkg + ".WriteBehindBuffer)."
	if !strings.HasPrefix(name, prefix) {
		return false
	}
	method := strings.TrimPrefix(name, prefix)
	return token.IsExported(method) || strings.HasSuffix(method, "OverlayLocked")
}

// referenceFields: exported fields of a struct that can reach a mutable reference.
func referenceFields(t types.Type) (string, []string, *types.Struct) {
	if p, ok := types.Unalias(t).(*types.Pointer); ok {
		t = p.Elem()
	}
	n, ok := types.Unalias(t).(*types.Named)
	if !ok {
		return t.String(), nil, nil
	}
	st, ok := n.Underlying().(*types.Struct)
	if !ok {
		return n.String(), nil, nil
	}
	var fields []string
	for i := 0; i < st.NumFields(); i++ {
		f := st.Field(i)
		if f.Exported() && refBearing(f.Type(), map[types.Type]bool{}) {
			fields = append(fields, f.Name())
		}
	}
	return strings.TrimPrefix(n.String(), module+"/"), fields, st
}

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
	case *types.Map, *types.Slice, *types.Pointer, *types.Interface, *types.Chan, *types.Signature:
		return true
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

// assignedFields: fields of st assigned (stored through a field address of the
// copied type) by fn or by the module functions it calls (depth-bounded).
func assignedFields(fn *ssa.Function, st *types.Struct, out map[string]bool, depth int, visited map[*ssa.Function]bool) {
	if fn == nil || fn.Blocks == nil || depth > 3 || visited[fn] {
		return
	}
	visited[fn] = true
	for _, b := range fn.Blocks {
		for _, ins := range b.Instrs {
			switch x := ins.(type) {
			case *ssa.Store:
				fa, ok := x.Addr.(*ssa.FieldAddr)
				if !ok {
					continue
				}
				p, ok := types.Unalias(fa.X.Type()).(*types.Pointer)
				if !ok || !types.Identical(p.Elem().Underlying(), st) {
					continue
				}
				out[st.Field(fa.Field).Name()] = true
			case ssa.CallInstruction:
				if callee := staticCallee(x.Common()); callee != nil && inModule(callee) {
					assignedFields(callee, st, out, depth+1, visited)
				}
			}
		}
	}
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
	config.Overlay = map[string][]byte{filepath.Join(tree, "pkg/storage/nor160_consumer_control.go"): []byte(controlFile)}
	p, err := workcensus.LoadProgram(config)
	if err != nil {
		panic(err)
	}
	fns := allFunctions(p)
	r := report{Author: "NOR-160 lane (Claude), 30 October 2026", Target: spec, Loader: "maintained internal/workcensus.LoadProgram", Functions: len(fns), Controls: map[string]bool{}}

	// 1 and 2: delivery consumers and overlay views.
	for _, fn := range fns {
		for _, b := range fn.Blocks {
			for _, ins := range b.Instrs {
				call, ok := ins.(ssa.CallInstruction)
				if !ok {
					continue
				}
				c := call.Common()
				name := calleeName(c)
				if overlayReader(name) {
					roots := map[ssa.Value]bool{}
					if v := call.Value(); v != nil {
						roots[v] = true
					}
					file, line := pos(fn, call.Pos())
					r.Views = append(r.Views, view{Function: fn.String(), File: file, Line: line, Reader: name, Copies: copiesOf(fn, roots, 0, map[string]bool{})})
				}
				fromStorage := deliversFromStorage(c)
				for _, arg := range c.Args {
					params := deliveryParams(arg.Type())
					cb := callbackFunction(arg)
					if len(params) == 0 || cb == nil || cb.Blocks == nil {
						continue
					}
					roots := map[ssa.Value]bool{}
					for _, i := range params {
						if i < len(cb.Params) {
							roots[cb.Params[i]] = true
						}
					}
					file, line := pos(cb, cb.Pos())
					sf, sl := pos(fn, call.Pos())
					r.Consumers = append(r.Consumers, consumer{Consumer: cb.String(), File: file, Line: line, Callee: name, FromStorage: fromStorage,
						Site: fmt.Sprintf("%s:%d", sf, sl), Copies: copiesOf(cb, roots, 0, map[string]bool{}), Retains: retains(cb, roots)})
				}
			}
		}
	}

	// 3: caller-copy implementations, by interface and by name.
	var copierIface *types.Interface
	for _, pkg := range p.Packages {
		if pkg.Types != nil && pkg.Types.Path() == module+"/pkg/callerowned" {
			if obj := pkg.Types.Scope().Lookup("Copier"); obj != nil {
				copierIface, _ = obj.Type().Underlying().(*types.Interface)
			}
		}
	}
	if copierIface == nil {
		// A revision before pkg/callerowned (the release): the same method set.
		sig := types.NewSignatureType(nil, nil, nil, nil, types.NewTuple(types.NewVar(token.NoPos, nil, "", types.NewInterfaceType(nil, nil))), false)
		copierIface = types.NewInterfaceType([]*types.Func{types.NewFunc(token.NoPos, nil, "CallerCopy", sig)}, nil).Complete()
	}
	implementsCopier := map[string]bool{}
	if copierIface != nil {
		for _, pkg := range p.SSA.AllPackages() {
			path := pkg.Pkg.Path()
			if path != module && !strings.HasPrefix(path, module+"/") {
				continue
			}
			for _, member := range pkg.Members {
				tn, ok := member.(*ssa.Type)
				if !ok {
					continue
				}
				t := tn.Type()
				if types.Implements(t, copierIface) || types.Implements(types.NewPointer(t), copierIface) {
					implementsCopier[strings.TrimPrefix(t.String(), module+"/")] = true
				}
			}
		}
	}
	for t := range implementsCopier {
		r.CopierTypes = append(r.CopierTypes, t)
	}
	sort.Strings(r.CopierTypes)
	for _, fn := range fns {
		if fn.Parent() != nil {
			continue
		}
		lower := strings.ToLower(fn.Name())
		if fn.Name() != "CallerCopy" && !strings.Contains(lower, "callerowned") && !copierNames[fullName(fn)] {
			continue
		}
		var copied types.Type
		switch {
		case fn.Signature.Recv() != nil:
			copied = fn.Signature.Recv().Type()
		case fn.Signature.Params().Len() > 0:
			copied = fn.Signature.Params().At(0).Type()
		default:
			continue
		}
		typeName, fields, st := referenceFields(copied)
		impl := copierImpl{Implementation: fn.String(), Type: typeName, Fields: fields}
		impl.File, impl.Line = pos(fn, fn.Pos())
		if st != nil {
			assigned := map[string]bool{}
			assignedFields(fn, st, assigned, 0, map[*ssa.Function]bool{})
			for _, f := range fields {
				if !assigned[f] {
					impl.Unassigned = append(impl.Unassigned, f)
				}
			}
		}
		r.Copiers = append(r.Copiers, impl)
	}

	// 4: every copier call site in the module, for a written disposition each.
	for _, fn := range fns {
		if copierNames[fullName(fn)] {
			continue
		}
		for _, b := range fn.Blocks {
			for _, ins := range b.Instrs {
				call, ok := ins.(ssa.CallInstruction)
				if !ok {
					continue
				}
				c := call.Common()
				name := calleeName(c)
				if !copierNames[name] || strings.Contains(name, "/pkg/callerowned.") {
					continue
				}
				note := ""
				if strings.HasSuffix(name, ".shapeNode") && len(c.Args) == 4 {
					if constTrue(c.Args[3]) {
						note = " (owned: reshape in place)"
					} else if _, isConst := c.Args[3].(*ssa.Const); !isConst {
						note = " (owned=" + c.Args[3].Name() + ")"
					}
				}
				file, line := pos(fn, call.Pos())
				r.CopySites = append(r.CopySites, fmt.Sprintf("%s:%d %s -> %s%s", file, line, strings.TrimPrefix(fn.String(), module+"/"), name[strings.LastIndex(name, "/")+1:], note))
			}
		}
	}
	sort.Strings(r.CopySites)

	// 5: every function outside the copy owner that rebuilds a storage node or
	// edge from its argument and replaces a reference field: a second entity
	// copier (or a projection, dispositioned in writing).
	for _, fn := range fns {
		if copierNames[fullName(fn)] || fn.Name() == "CallerCopy" || strings.Contains(strings.ToLower(fn.Name()), "callerowned") {
			continue
		}
		if fields := rebuiltEntityFields(fn); len(fields) > 0 {
			file, line := pos(fn, fn.Pos())
			r.EntityCopiers = append(r.EntityCopiers, fmt.Sprintf("%s:%d %s replaces %s", file, line, strings.TrimPrefix(fn.String(), module+"/"), strings.Join(fields, ",")))
		}
	}
	sort.Strings(r.EntityCopiers)

	// 6 and 7: every hand-written slice copier (a copier call inside a loop
	// whose result is stored into a slice element or appended) and every Copier
	// dispatch rule (a CallerCopy call through an interface, or an assertion to
	// an interface declaring it) outside the owner package.
	for _, fn := range fns {
		owner := fn
		for owner.Pkg == nil && owner.Parent() != nil {
			owner = owner.Parent()
		}
		if owner.Pkg == nil || strings.HasPrefix(owner.Pkg.Pkg.Path(), module+"/pkg/callerowned") {
			continue
		}
		for _, b := range fn.Blocks {
			for _, ins := range b.Instrs {
				switch x := ins.(type) {
				case *ssa.TypeAssert:
					if declaresCallerCopy(x.AssertedType) {
						file, line := pos(fn, x.Pos())
						r.Dispatchers = append(r.Dispatchers, fmt.Sprintf("%s:%d %s asserts %s", file, line, strings.TrimPrefix(fn.String(), module+"/"), x.AssertedType))
					}
				case ssa.CallInstruction:
					c := x.Common()
					if c.IsInvoke() && c.Method.Name() == "CallerCopy" {
						file, line := pos(fn, x.Pos())
						r.Dispatchers = append(r.Dispatchers, fmt.Sprintf("%s:%d %s calls CallerCopy through %s", file, line, strings.TrimPrefix(fn.String(), module+"/"), c.Value.Type()))
					}
					call, ok := ins.(*ssa.Call)
					if !ok || !elementCopier(c) || !inCycle(b) || !storedAsElement(call) {
						continue
					}
					file, line := pos(fn, x.Pos())
					r.SliceCopiers = append(r.SliceCopiers, fmt.Sprintf("%s:%d %s -> %s", file, line, strings.TrimPrefix(fn.String(), module+"/"), calleeName(c)))
				}
			}
		}
	}
	sort.Strings(r.SliceCopiers)
	sort.Strings(r.Dispatchers)

	// Controls.
	r.Controls["second copy of an owned delivery"] = false
	for _, c := range r.Consumers {
		if strings.Contains(c.Consumer, "NOR160ControlSecondCopy") && len(c.Copies) > 0 {
			r.Controls["second copy of an owned delivery"] = true
		}
	}
	r.Controls["second copy inside a captured emitter"] = false
	for _, c := range r.Consumers {
		if strings.Contains(c.Consumer, "NOR160ControlCapturedEmitter") && len(c.Copies) > 0 {
			r.Controls["second copy inside a captured emitter"] = true
		}
	}
	r.Controls["second entity copier"] = false
	for _, c := range r.EntityCopiers {
		if strings.Contains(c, "NOR160ControlQueueCopier") && strings.Contains(c, "Labels,Properties") {
			r.Controls["second entity copier"] = true
		}
	}
	r.Controls["hand-written slice copier"] = false
	for _, c := range r.SliceCopiers {
		if strings.Contains(c, "NOR160ControlSliceCopier") {
			r.Controls["hand-written slice copier"] = true
		}
	}
	r.Controls["second Copier dispatch rule"] = false
	for _, c := range r.Dispatchers {
		if strings.Contains(c, "NOR160ControlDispatch") {
			r.Controls["second Copier dispatch rule"] = true
		}
	}
	partial, complete := false, true
	for _, c := range r.Copiers {
		if strings.Contains(c.Implementation, "NOR160ControlPartialHit") {
			partial = len(c.Unassigned) == 1 && c.Unassigned[0] == "Rel"
		}
		if strings.Contains(c.Implementation, "NOR160ControlCompleteHit") {
			complete = len(c.Unassigned) == 0
		}
	}
	r.Controls["partial copier omitting a graph field"] = partial
	r.Controls["clean: complete copier not reported"] = complete
	r.Controls["copier types include both controls"] = implementsCopier["pkg/storage.NOR160ControlPartialHit"] && implementsCopier["pkg/storage.NOR160ControlCompleteHit"]
	r.AllControlsOK = true
	for _, ok := range r.Controls {
		r.AllControlsOK = r.AllControlsOK && ok
	}

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
