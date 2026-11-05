package storagecensus

import (
	"fmt"
	"go/constant"
	"go/token"
	"go/types"
	"sort"
	"strings"

	"golang.org/x/tools/go/ssa"
)

// A key space is a range of Badger keys that holds one kind of state, named
// by the storage constant its keys start with (a second constant, or a
// string, after prefixMVCCMeta). Classes are what the census checks: the
// classes in orderedClasses hold stored entity state, and every write to
// them must run under write order (see enforce.go).
type keySpace struct {
	Name  string `json:"name"`
	Class string `json:"class"`
}

// spaceClasses classifies every key space by its constant (or string)
// name. A write whose key starts with a constant that is not listed here, or
// whose key the census cannot trace to a constant, is reported: the table
// is complete by construction.
var spaceClasses = map[string]string{
	"prefixNode":             "entity-body",
	"prefixEdge":             "entity-body",
	"prefixLabelIndex":       "entity-index",
	"prefixOutgoingIndex":    "entity-index",
	"prefixIncomingIndex":    "entity-index",
	"prefixEdgeTypeIndex":    "entity-index",
	"prefixEdgeBetweenIndex": "entity-index",
	"prefixEdgeBetweenHead":  "entity-index",
	"prefixTemporalIndex":    "entity-index",
	"prefixTemporalHead":     "entity-index",
	// The pending-embedding index is a work queue, not entity state: its
	// reader revalidates every entry against the node (dropping stale and
	// no-longer-needed ones), and RefreshPendingEmbeddingsIndex rebuilds it.
	"prefixPendingEmbed":      "embedding-queue",
	"prefixEmbedding":         "entity-sidecar",
	"prefixMVCCNode":          "entity-version",
	"prefixMVCCEdge":          "entity-version",
	"prefixMVCCNodeHead":      "entity-head",
	"prefixMVCCEdgeHead":      "entity-head",
	"prefixMVCCOutgoingAdj":   "entity-adjacency",
	"prefixMVCCIncomingAdj":   "entity-adjacency",
	"prefixMVCCPruneFloor":    "version-retention",
	"prefixSchema":            "schema",
	"prefixBufferedFlush":     "write-behind-marker",
	"prefixIDDictNodeForward": "id-dictionary",
	"prefixIDDictEdgeForward": "id-dictionary",
	"prefixIDDictCounter":     "id-dictionary",
	"prefixIDDictNodeReverse": "id-dictionary",
	"prefixIDDictEdgeReverse": "id-dictionary",
	"prefixIDFreelist":        "id-dictionary",
	"prefixPropKeyForward":    "property-key-dictionary",
	"prefixPropKeyReverse":    "property-key-dictionary",
	"prefixPropKeyCounter":    "property-key-dictionary",
	"prefixChangeEvent":       "change-log",

	"prefixMVCCMetaSchemaVersion":           "store-metadata",
	"prefixMVCCMetaEdgeBetweenIndexReady":   "store-metadata",
	"prefixMVCCMetaNamespaceSeq":            "mvcc-sequence",
	"prefixMVCCMetaLabelIndexReady":         "store-metadata",
	"prefixMVCCMetaLabelCountReady":         "store-metadata",
	"prefixMVCCMetaLabelCount":              "derived-count",
	"prefixMVCCMetaCleanShutdown":           "store-metadata",
	"prefixMVCCMetaEdgeTypeCountReady":      "store-metadata",
	"prefixMVCCMetaEdgeTypeCount":           "derived-count",
	"prefixMVCCMetaLabelCountDelta":         "derived-count",
	"prefixMVCCMetaEdgeTypeCountDelta":      "derived-count",
	"prefixMVCCMetaEdgeTypeStartLabelCount": "derived-count",
	"prefixMVCCMetaEdgeTypeEndLabelCount":   "derived-count",
	"prefixMVCCMetaLargeCommitIntent":       "store-metadata",
	// Durable namespace cleanup intent, written atomically by the commit owner.
	"prefixMVCCMetaNamespaceDrop":       "store-metadata",
	"prefixMVCCMetaDeleteCleanupMarker": "store-metadata",
	"prefixMVCCMetaTemporalIndexFormat": "store-metadata",
	"prefixMVCCMetaManualSuppression":   "suppression",
	// A write of every metadata key space at once (a namespace's key
	// families dropped together): it includes the ordered ones.
	"prefixMVCCMeta":               "metadata-family",
	`prefixMVCCMeta+"accessmeta:"`: "access-metadata",
	// WAL property provenance is serialized by walPropertySource; identity
	// changes precede imports and clean-close proof follows native durability.
	`prefixMVCCMeta+"wal-property-identity"`: "store-metadata",
	`prefixMVCCMeta+"wal-property-foreign"`:  "store-metadata",
	`prefixMVCCMeta+"wal-property-clean"`:    "store-metadata",
	// This startup-only marker records an admitted V4 WAL replay; it is
	// deleted atomically with the V4 catalog and schema transaction.
	`prefixMVCCMeta+"wal-v4-upgrade-pending"`: "store-metadata",
	// This separate startup-only marker tracks a V4 WAL format conversion
	// when the catalog is already V4; it is removed after recovery and sync.
	`prefixMVCCMeta+"wal-v4-msgpack-upgrade-pending"`: "store-metadata",
	// Retired first bytes (retiredPrefixes).
	"retired 0x17": "retired",
}

// retiredPrefixes are first bytes no storage constant names any more: key
// spaces of older stores that only the on-start migrations read and delete
// (migrateLegacyPolicyMetadata: the index tombstones before the V3 layout).
var retiredPrefixes = map[byte]string{0x17: "retired 0x17"}

// orderedClasses are the classes that hold stored entity state.
var orderedClasses = map[string]bool{
	"entity-body": true, "entity-index": true, "entity-sidecar": true, "entity-version": true,
	"entity-head": true, "entity-adjacency": true, "derived-count": true, "suppression": true, "metadata-family": true,
}

// byteSrc is where one byte of a key comes from: a constant, a parameter of
// the function that builds the key, or a string constant it starts.
type byteSrc struct {
	known bool
	val   byte
	param int // >= 0: the byte is this parameter
	str   string
	// vals are the constants the byte takes, when it ranges over a
	// constant array: the shape stands for one key per value (expand).
	vals []byte
	// why says why an unknown byte is unknown.
	why string
	// free > 0: the byte is captured variable free-1 of the closure (its
	// cell when deref), resolved where the closure is made.
	free  int
	deref bool
}

// expand splits a shape whose bytes range over constants into one shape per
// constant.
func expand(s keyShape) []keyShape {
	for i, b := range s.bytes {
		if len(b.vals) == 0 {
			continue
		}
		var out []keyShape
		for _, v := range b.vals {
			c := s
			c.bytes = append([]byteSrc(nil), s.bytes...)
			c.bytes[i] = byteSrc{known: true, val: v, param: -1}
			out = append(out, expand(c)...)
		}
		return out
	}
	return []keyShape{s}
}

// keyShape is what the census knows of a key: its first two bytes, or that
// it is a parameter (or captured variable) of the function, or why it is
// unknown.
type keyShape struct {
	bytes   []byteSrc // at most two
	param   int       // >= 0: the key is this parameter
	free    int       // >= 0: the key is this captured variable
	unknown string
	// whole names a write of any key at all: one whose key an unbounded
	// scan of the store returned.
	whole string
	// nilKey marks the nil key itself: never written.
	nilKey bool
	// mode refines param and free: "call" means the key is what calling
	// the parameter (a key builder passed as a value) returns, "deref" the
	// value the captured variable's cell holds.
	mode      string
	selectors []selectorFact // predicate selecting a key-builder Phi arm
}

func paramShape(i int) keyShape { return keyShape{param: i, free: -1} }

// capturedByte reports whether a byte of s is a closure's captured
// variable, still to be resolved where the closure is made.
func (s keyShape) capturedByte() bool {
	for _, b := range s.bytes {
		if b.free > 0 {
			return true
		}
	}
	return false
}
func freeShape(i int) keyShape { return keyShape{param: -1, free: i} }
func unknownShape(why string) keyShape {
	return keyShape{param: -1, free: -1, unknown: why}
}
func bytesShape(b []byteSrc) keyShape {
	if len(b) > 2 {
		b = b[:2]
	}
	return keyShape{bytes: b, param: -1, free: -1}
}

// keys traces key values to their spaces.
type keys struct {
	p               *program
	first           map[byte]string // constant value -> name, for a key's first byte
	meta            map[byte]string // the same for the byte after prefixMVCCMeta
	metaByte        byte
	returns         map[*ssa.Function][]keyShape
	active          map[*ssa.Function]bool
	fieldStores     map[string][]ssa.Value
	activeFields    map[string]bool
	activeCells     map[*ssa.Alloc]bool
	activePhis      map[*ssa.Phi]bool
	activeElemCells map[*ssa.Alloc]bool
	mapUpdates      map[string][]ssa.Value
}

func newKeys(p *program) (*keys, error) {
	k := &keys{p: p, first: map[byte]string{}, meta: map[byte]string{}, returns: map[*ssa.Function][]keyShape{}, active: map[*ssa.Function]bool{}}
	storage := p.ssa.ImportedPackage(Module + "pkg/storage")
	if storage == nil {
		return nil, fmt.Errorf("pkg/storage is not loaded")
	}
	scope := storage.Pkg.Scope()
	meta, ok := scope.Lookup("prefixMVCCMeta").(*types.Const)
	if !ok {
		return nil, fmt.Errorf("pkg/storage has no constant prefixMVCCMeta")
	}
	mv, _ := constant.Uint64Val(meta.Val())
	k.metaByte = byte(mv)
	for _, name := range scope.Names() {
		c, ok := scope.Lookup(name).(*types.Const)
		if !ok || !strings.HasPrefix(name, "prefix") || name == "prefixMVCCMeta" {
			continue
		}
		if b, ok := c.Type().Underlying().(*types.Basic); !ok || b.Kind() != types.Byte && b.Kind() != types.Uint8 {
			continue
		}
		v, _ := constant.Uint64Val(c.Val())
		table := k.first
		if strings.HasPrefix(name, "prefixMVCCMeta") {
			table = k.meta
		}
		if prev, dup := table[byte(v)]; dup {
			return nil, fmt.Errorf("key prefix constants %s and %s have the same value %#x", prev, name, v)
		}
		table[byte(v)] = name
	}
	return k, nil
}

// space names the key space a shape's bytes name, or "" when a byte is not
// known; param reports the parameter index a byte comes from, or -1.
func (k *keys) space(s keyShape) (name string, param int, why string) {
	if s.unknown != "" {
		return "", -1, s.unknown
	}
	if len(s.bytes) == 0 {
		return "", -1, "no constant prefix"
	}
	b0 := s.bytes[0]
	if !b0.known {
		if b0.param >= 0 {
			return "", b0.param, ""
		}
		if b0.free > 0 {
			return "", -1, ""
		}
		return "", -1, "first byte is not a constant" + because(b0.why)
	}
	if b0.val != k.metaByte {
		if n, ok := k.first[b0.val]; ok {
			return n, -1, ""
		}
		if n, ok := retiredPrefixes[b0.val]; ok {
			return n, -1, ""
		}
		return "", -1, fmt.Sprintf("first byte %#x is no storage prefix constant", b0.val)
	}
	if len(s.bytes) < 2 {
		return "prefixMVCCMeta", -1, "" // every metadata key space
	}
	b1 := s.bytes[1]
	switch {
	case b1.str != "":
		// Literal metadata keys use their exact classified identity. Prefix
		// families below retain their separately declared matching contract.
		literal := fmt.Sprintf("prefixMVCCMeta+%q", b1.str)
		if _, classified := spaceClasses[literal]; classified {
			return literal, -1, ""
		}
		for _, sub := range []string{"accessmeta:"} {
			if strings.HasPrefix(b1.str, sub) {
				return `prefixMVCCMeta+"` + sub + `"`, -1, ""
			}
		}
		return "", -1, fmt.Sprintf("prefixMVCCMeta followed by string %q", b1.str)
	case b1.known:
		if n, ok := k.meta[b1.val]; ok {
			return n, -1, ""
		}
		return "", -1, fmt.Sprintf("prefixMVCCMeta followed by %#x, no constant", b1.val)
	case b1.param >= 0:
		return "", b1.param, ""
	case b1.free > 0:
		return "", -1, ""
	}
	return "", -1, "prefixMVCCMeta followed by a byte that is not a constant" + because(b1.why)
}

func because(why string) string {
	if why == "" {
		return ""
	}
	return " (" + why + ")"
}

// shapes traces the []byte (or string) value v in its function.
func (k *keys) shapes(v ssa.Value, depth int, resultIndex ...int) []keyShape {
	if depth > 24 {
		return []keyShape{unknownShape("trace too deep")}
	}
	switch v := v.(type) {
	case *ssa.Function:
		if len(resultIndex) != 1 {
			break // a function value alone is not a returned key
		}
		index := resultIndex[0]
		if index == 0 {
			if s, ok := k.returns[v]; ok {
				return s
			}
		}
		if k.active[v] {
			return nil
		}
		k.active[v] = true
		defer delete(k.active, v)
		var out []keyShape
		for _, b := range v.Blocks {
			if ret, ok := b.Instrs[len(b.Instrs)-1].(*ssa.Return); ok && index < len(ret.Results) {
				// A nil key (returned with an error) is never written.
				if c, ok := ret.Results[index].(*ssa.Const); ok && c.IsNil() {
					continue
				}
				out = append(out, k.shapes(ret.Results[index], depth+1)...)
			}
		}
		out = dedupShapes(out)
		if index == 0 {
			k.returns[v] = out
		}
		return out
	case *ssa.Parameter:
		for i, p := range v.Parent().Params {
			if p == v {
				return []keyShape{paramShape(i)}
			}
		}
	case *ssa.FreeVar:
		for i, f := range v.Parent().FreeVars {
			if f == v {
				return []keyShape{freeShape(i)}
			}
		}
	case *ssa.Phi:
		// A loop variable reaches itself (buf = append(buf[:0], key...)):
		// the cycle adds nothing.
		if k.activePhis[v] {
			return nil
		}
		if k.activePhis == nil {
			k.activePhis = map[*ssa.Phi]bool{}
		}
		k.activePhis[v] = true
		defer delete(k.activePhis, v)
		var out []keyShape
		for _, e := range v.Edges {
			out = append(out, k.shapes(e, depth+1)...)
		}
		return out
	case *ssa.Slice:
		return k.shapes(v.X, depth+1)
	case *ssa.Alloc:
		return []keyShape{bytesShape(k.storedBytes(v))}
	case *ssa.MakeSlice:
		// make([]byte, n) filled through indexes: key[0] = prefix.
		return []keyShape{bytesShape(k.storedBytes(v))}
	case *ssa.Const:
		if v.Value != nil && v.Value.Kind() == constant.String {
			return []keyShape{bytesShape([]byteSrc{{str: constant.StringVal(v.Value), param: -1}})}
		}
		if v.IsNil() {
			// A nil key is no key (Badger rejects an empty key); as the
			// base of an append it contributes nothing.
			s := bytesShape(nil)
			s.nilKey = true
			return []keyShape{s}
		}
	case *ssa.Convert:
		return k.shapes(v.X, depth+1)
	case *ssa.ChangeType:
		return k.shapes(v.X, depth+1)
	case *ssa.BinOp:
		// A string concatenation starts with its left operand.
		return k.shapes(v.X, depth+1)
	case *ssa.Extract:
		if call, ok := v.Tuple.(*ssa.Call); ok {
			return k.callShapes(call, v.Index, depth)
		}
		// The key of a range over a map of keys: every key put in it.
		if next, ok := v.Tuple.(*ssa.Next); ok && v.Index == 1 && !next.IsString {
			if rng, ok := next.Iter.(*ssa.Range); ok {
				return k.mapKeyShapes(rng.X, depth)
			}
		}
	case *ssa.Call:
		return k.callShapes(v, 0, depth)
	case *ssa.UnOp:
		if v.Op == token.MUL {
			return k.loadShapes(v.X, depth)
		}
	}
	return []keyShape{unknownShape(fmt.Sprintf("%T at %s", v, k.p.pos(v.Pos())))}
}

// loadShapes traces a key loaded from memory: a local cell, a captured
// variable's cell, or a struct field (every value the module stores in that
// field, in any function).
func (k *keys) loadShapes(addr ssa.Value, depth int) []keyShape {
	switch a := addr.(type) {
	case *ssa.Alloc:
		return k.cellShapes(a, depth)
	case *ssa.FreeVar:
		for i, f := range a.Parent().FreeVars {
			if f == a {
				s := freeShape(i)
				s.mode = "deref"
				return []keyShape{s}
			}
		}
	case *ssa.Global:
		var out []keyShape
		for _, fn := range k.p.funcs {
			for _, b := range fn.Blocks {
				for _, ins := range b.Instrs {
					if st, ok := ins.(*ssa.Store); ok && st.Addr == a {
						for _, s := range k.shapes(st.Val, depth+1) {
							if s.param >= 0 || s.free >= 0 {
								s = unknownShape("package variable " + a.Name() + " holds a key its writer receives")
							}
							out = append(out, s)
						}
					}
				}
			}
		}
		if len(out) == 0 {
			return []keyShape{unknownShape("package variable " + a.Name() + " is never stored")}
		}
		return out
	case *ssa.FieldAddr:
		name, _ := k.p.field(a.X.Type().Underlying().(*types.Pointer).Elem(), a.Field)
		if name == "" {
			return []keyShape{unknownShape("load of a field outside the state packages at " + k.p.pos(a.Pos()))}
		}
		return k.fieldShapes(name, depth)
	case *ssa.IndexAddr:
		// An element of a slice of keys: every key put in it.
		return k.elementShapes(a.X, depth, map[ssa.Value]bool{})
	}
	return []keyShape{unknownShape(fmt.Sprintf("load of %T at %s", addr, k.p.pos(addr.Pos())))}
}

// elementShapes are the shapes of the keys a slice (or array) of keys holds:
// the elements of its literal, everything appended to it, what the cell or
// field holding it is set to; a slice a function receives (or a closure
// captures) is resolved by its callers ("elem").
func (k *keys) elementShapes(v ssa.Value, depth int, seen map[ssa.Value]bool) []keyShape {
	if depth > 24 {
		return []keyShape{unknownShape("trace too deep")}
	}
	if seen[v] {
		return nil
	}
	seen[v] = true
	elem := func(s keyShape) []keyShape {
		s.mode = "elem"
		return []keyShape{s}
	}
	switch v := v.(type) {
	case *ssa.Slice:
		return k.elementShapes(v.X, depth+1, seen)
	case *ssa.Phi:
		var out []keyShape
		for _, e := range v.Edges {
			out = append(out, k.elementShapes(e, depth+1, seen)...)
		}
		return out
	case *ssa.Const:
		if v.IsNil() {
			return nil
		}
	case *ssa.Parameter:
		for i, p := range v.Parent().Params {
			if p == v {
				return elem(paramShape(i))
			}
		}
	case *ssa.FreeVar:
		for i, f := range v.Parent().FreeVars {
			if f == v {
				return elem(freeShape(i))
			}
		}
	case *ssa.Alloc, *ssa.MakeSlice:
		if a, ok := v.(*ssa.Alloc); ok {
			if _, cell := a.Type().(*types.Pointer).Elem().Underlying().(*types.Slice); cell {
				// keys = append(keys, key...): the cell reaches itself (in
				// its function or through a closure); the cycle adds nothing.
				if k.activeElemCells[a] {
					return nil
				}
				if k.activeElemCells == nil {
					k.activeElemCells = map[*ssa.Alloc]bool{}
				}
				k.activeElemCells[a] = true
				defer delete(k.activeElemCells, a)
				var out []keyShape
				for _, st := range k.cellStores(a) {
					for _, s := range k.elementShapes(st, depth+1, seen) {
						out = append(out, k.contextFree(s, parentOf(st), a.Parent(), depth+1)...)
					}
				}
				return out
			}
		}
		// A literal (or make) filled through its indexes.
		var out []keyShape
		for _, ref := range *v.Referrers() {
			ia, ok := ref.(*ssa.IndexAddr)
			if !ok {
				continue
			}
			for _, r := range *ia.Referrers() {
				if st, ok := r.(*ssa.Store); ok && st.Addr == ia {
					out = append(out, k.shapes(st.Val, depth+1)...)
				}
			}
		}
		return out
	case *ssa.UnOp:
		if v.Op != token.MUL {
			break
		}
		switch x := v.X.(type) {
		case *ssa.Alloc:
			return k.elementShapes(x, depth+1, seen)
		case *ssa.FreeVar:
			return k.elementShapes(x, depth+1, seen)
		case *ssa.FieldAddr:
			if name, _ := k.p.field(x.X.Type(), x.Field); name != "" {
				k.indexFieldStores()
				var out []keyShape
				for _, st := range k.fieldStores[name] {
					for _, s := range k.elementShapes(st, depth+1, seen) {
						if s.param >= 0 || s.free >= 0 {
							s = unknownShape("field " + name + " holds keys its writer receives")
						}
						out = append(out, s)
					}
				}
				return out
			}
		}
	case *ssa.Call:
		c := v.Common()
		if b, ok := c.Value.(*ssa.Builtin); ok && b.Name() == "append" && len(c.Args) == 2 {
			return append(k.elementShapes(c.Args[0], depth+1, seen), k.elementShapes(c.Args[1], depth+1, seen)...)
		}
		if callee := c.StaticCallee(); callee != nil && inModule(callee) && callee.Blocks != nil {
			var out []keyShape
			for _, b := range callee.Blocks {
				if ret, ok := b.Instrs[len(b.Instrs)-1].(*ssa.Return); ok && len(ret.Results) > 0 {
					for _, s := range k.elementShapes(ret.Results[0], depth+1, seen) {
						out = append(out, k.substitute(s, c.Args, v, depth)...)
					}
				}
			}
			return out
		}
	}
	return []keyShape{unknownShape(fmt.Sprintf("slice of keys %T at %s", v, k.p.pos(v.Pos())))}
}

// cellShapes are the values stored into a local cell.
func (k *keys) cellShapes(a *ssa.Alloc, depth int) []keyShape {
	if k.activeCells == nil {
		k.activeCells = map[*ssa.Alloc]bool{}
	}
	if k.activeCells[a] {
		return nil // a store of a value derived from the cell itself
	}
	k.activeCells[a] = true
	defer delete(k.activeCells, a)
	var out []keyShape
	for _, v := range k.cellStores(a) {
		for _, s := range k.shapes(v, depth+1) {
			// A value a closure stores is resolved up to the cell's function.
			out = append(out, k.contextFree(s, parentOf(v), a.Parent(), depth+1)...)
		}
	}
	if len(out) == 0 {
		return []keyShape{unknownShape("cell with no store at " + k.p.pos(a.Pos()))}
	}
	return out
}

// fieldShapes are the shapes of every value stored in a field; a parameter
// of the storing function is not known here.
func (k *keys) fieldShapes(name string, depth int) []keyShape {
	k.indexFieldStores()
	if k.activeFields == nil {
		k.activeFields = map[string]bool{}
	}
	if k.activeFields[name] {
		return nil
	}
	k.activeFields[name] = true
	defer delete(k.activeFields, name)
	var out []keyShape
	for _, v := range k.fieldStores[name] {
		for _, s := range k.shapes(v, depth+1) {
			out = append(out, k.contextFree(s, parentOf(v), nil, depth+1)...)
		}
	}
	if len(out) == 0 {
		return []keyShape{unknownShape("field " + name + " is never stored")}
	}
	return out
}

// parentOf is the function a value belongs to, or nil for a value of none
// (a constant, a function, a package variable).
func parentOf(v ssa.Value) *ssa.Function {
	switch v := v.(type) {
	case *ssa.Parameter:
		return v.Parent()
	case *ssa.FreeVar:
		return v.Parent()
	case ssa.Instruction:
		return v.Parent()
	}
	return nil
}

// contextFree resolves a shape traced in fn for a reader in target: a cell's
// function reading what a closure inside it stored (resolved through the
// closures' bindings up to target, whose own parameters stay its own), or,
// with target nil, a field or map read anywhere (resolved over every call
// of fn and every closure of it).
func (k *keys) contextFree(s keyShape, fn, target *ssa.Function, depth int) []keyShape {
	if fn == target {
		return []keyShape{s}
	}
	if fn == nil || depth > 24 {
		if s.param >= 0 || s.free >= 0 {
			return []keyShape{unknownShape("key of no function's context")}
		}
		return []keyShape{s}
	}
	switch {
	case s.param >= 0:
		if s.param >= len(fn.Params) {
			return []keyShape{unknownShape("variadic parameter")}
		}
		args, why := k.paramArgs(fn.Params[s.param])
		if why != "" {
			return []keyShape{unknownShape(why)}
		}
		var out []keyShape
		for _, arg := range args {
			for _, r := range k.argShapes(s, arg, depth+1) {
				out = append(out, k.contextFree(r, parentOf(arg), target, depth+1)...)
			}
		}
		return out
	case s.free >= 0:
		if s.free >= len(fn.FreeVars) {
			return []keyShape{unknownShape("captured variable out of range")}
		}
		var out []keyShape
		bound := k.bindings(fn.FreeVars[s.free])
		for _, b := range bound {
			for _, r := range k.argShapes(s, b, depth+1) {
				out = append(out, k.contextFree(r, fn.Parent(), target, depth+1)...)
			}
		}
		if len(bound) == 0 {
			return []keyShape{unknownShape("captured variable of a closure never made")}
		}
		return out
	}
	if s.capturedByte() && fn.Parent() != nil {
		// Captured bytes resolve in the maker, then up to target.
		return k.contextFree(k.capturedBytes(s, fn), fn.Parent(), target, depth+1)
	}
	return []keyShape{k.closureBytes(s, fn, nil)}
}

// capturedBytes resolves only the captured bytes of a shape traced in
// closure, in its maker.
func (k *keys) capturedBytes(s keyShape, closure *ssa.Function) keyShape {
	out := s
	out.bytes = append([]byteSrc(nil), s.bytes...)
	for i, b := range out.bytes {
		if b.free > 0 {
			out.bytes[i] = k.boundByte(b, closure, nil)
		}
	}
	return out
}

// mapKeyShapes are the shapes of the keys a map of keys holds: every key the
// module puts in the field the map is loaded from.
func (k *keys) mapKeyShapes(m ssa.Value, depth int) []keyShape {
	load, ok := m.(*ssa.UnOp)
	var name string
	if ok && load.Op == token.MUL {
		if fa, ok := load.X.(*ssa.FieldAddr); ok {
			name, _ = k.p.field(fa.X.Type(), fa.Field)
		}
	}
	if name == "" {
		return []keyShape{unknownShape(fmt.Sprintf("range over a map of keys %T at %s", m, k.p.pos(m.Pos())))}
	}
	if k.mapUpdates == nil {
		k.mapUpdates = map[string][]ssa.Value{}
		for _, fn := range k.p.funcs {
			for _, b := range fn.Blocks {
				for _, ins := range b.Instrs {
					up, ok := ins.(*ssa.MapUpdate)
					if !ok {
						continue
					}
					if l, ok := up.Map.(*ssa.UnOp); ok && l.Op == token.MUL {
						if fa, ok := l.X.(*ssa.FieldAddr); ok {
							if n, _ := k.p.field(fa.X.Type(), fa.Field); n != "" {
								k.mapUpdates[n] = append(k.mapUpdates[n], up.Key)
							}
						}
					}
				}
			}
		}
	}
	var out []keyShape
	for _, key := range k.mapUpdates[name] {
		for _, s := range k.shapes(key, depth+1) {
			out = append(out, k.contextFree(s, parentOf(key), nil, depth+1)...)
		}
	}
	if len(out) == 0 {
		return []keyShape{unknownShape("map field " + name + " never gets a key")}
	}
	return out
}

// indexFieldStores records, per state field, every value the module stores
// in it (a composite literal stores its fields the same way).
// fieldKey names a struct field for indexFieldStores: a state field by its
// type's name (program.field); a field of an unnamed struct type (a table
// such as badgerKeyFamilies) by the struct's type and the field's position.
func (k *keys) fieldKey(t types.Type, index int) string {
	if n, _ := k.p.field(t, index); n != "" {
		return n
	}
	if ptr, ok := t.Underlying().(*types.Pointer); ok {
		t = ptr.Elem()
	}
	if st, ok := t.(*types.Struct); ok && index < st.NumFields() {
		return fmt.Sprintf("%s#%d", types.TypeString(st, nil), index)
	}
	return ""
}

func (k *keys) indexFieldStores() {
	if k.fieldStores == nil {
		k.fieldStores = map[string][]ssa.Value{}
		for _, fn := range k.p.funcs {
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
					if n := k.fieldKey(fa.X.Type(), fa.Field); n != "" {
						k.fieldStores[n] = append(k.fieldStores[n], st.Val)
					}
				}
			}
		}
	}
}

// storedBytes is the constant (or parameter) bytes stored at indexes 0 and 1
// of an array allocation: a composite literal such as []byte{prefixNode}.
func (k *keys) storedBytes(a ssa.Value) []byteSrc {
	var out [2]*byteSrc
	// make([]byte, n) with a constant n is an array sliced whole: its
	// bytes are stored through the slice.
	refs := append([]ssa.Instruction(nil), *a.Referrers()...)
	for _, ref := range *a.Referrers() {
		if sl, ok := ref.(*ssa.Slice); ok && sl.Low == nil {
			refs = append(refs, *sl.Referrers()...)
		}
	}
	for _, ref := range refs {
		ia, ok := ref.(*ssa.IndexAddr)
		if !ok {
			continue
		}
		idx, ok := ia.Index.(*ssa.Const)
		if !ok {
			continue
		}
		i := idx.Int64()
		if i > 1 {
			continue
		}
		for _, r := range *ia.Referrers() {
			if st, ok := r.(*ssa.Store); ok && st.Addr == ia {
				src := k.byteOf(st.Val)
				out[i] = &src
			}
		}
	}
	var b []byteSrc
	for _, s := range out {
		if s == nil {
			break
		}
		b = append(b, *s)
	}
	return b
}

// byteOf is where a byte value comes from.
func (k *keys) byteOf(v ssa.Value) byteSrc {
	switch v := v.(type) {
	case *ssa.Const:
		if v.Value != nil {
			if u, ok := constant.Uint64Val(v.Value); ok {
				return byteSrc{known: true, val: byte(u), param: -1}
			}
		}
	case *ssa.Parameter:
		for i, p := range v.Parent().Params {
			if p == v {
				return byteSrc{param: i}
			}
		}
	case *ssa.Convert:
		return k.byteOf(v.X)
	case *ssa.FreeVar:
		for i, f := range v.Parent().FreeVars {
			if f == v {
				return byteSrc{param: -1, free: i + 1}
			}
		}
	case *ssa.UnOp:
		if f, ok := v.X.(*ssa.FreeVar); ok && v.Op == token.MUL {
			for i, q := range f.Parent().FreeVars {
				if q == f {
					return byteSrc{param: -1, free: i + 1, deref: true}
				}
			}
		}
		// An element of a constant array: for _, b := range [2]byte{...}
		// (or of the slice literal []byte{...}).
		if ia, ok := v.X.(*ssa.IndexAddr); ok {
			x := ia.X
			if sl, ok := x.(*ssa.Slice); ok {
				x = sl.X
			}
			if arr, ok := x.(*ssa.Alloc); ok {
				if vals := k.arrayConsts(arr); len(vals) > 0 {
					return byteSrc{param: -1, vals: vals}
				}
			}
		}
	case *ssa.Index:
		if load, ok := v.X.(*ssa.UnOp); ok {
			if arr, ok := load.X.(*ssa.Alloc); ok {
				if vals := k.arrayConsts(arr); len(vals) > 0 {
					return byteSrc{param: -1, vals: vals}
				}
			}
		}
	}
	vals, why := k.byteVals(v, map[ssa.Value]bool{})
	if why == "" && len(vals) > 0 {
		return byteSrc{param: -1, vals: vals}
	}
	return byteSrc{param: -1, why: why}
}

// byteVals are the constants a byte value may take, over every way a value
// reaches it: the arguments of every call of a parameter's function (the
// VTA call graph), the bindings of every closure a captured variable
// belongs to, and every value the module stores in a struct field. why says
// which value is no constant, or is "" when every one is.
func (k *keys) byteVals(v ssa.Value, seen map[ssa.Value]bool) (vals []byte, why string) {
	if seen[v] {
		return nil, ""
	}
	seen[v] = true
	union := func(vs []ssa.Value) ([]byte, string) {
		var out []byte
		for _, x := range vs {
			got, why := k.byteVals(x, seen)
			if why != "" {
				return nil, why
			}
			out = append(out, got...)
		}
		return dedupBytes(out), ""
	}
	switch v := v.(type) {
	case *ssa.Const:
		if v.Value != nil {
			if u, ok := constant.Uint64Val(v.Value); ok {
				return []byte{byte(u)}, ""
			}
		}
	case *ssa.Convert:
		return k.byteVals(v.X, seen)
	case *ssa.Phi:
		return union(v.Edges)
	case *ssa.Parameter:
		args, why := k.paramArgs(v)
		if why != "" {
			return nil, why
		}
		return union(args)
	case *ssa.FreeVar:
		return union(k.bindings(v))
	case *ssa.Field:
		if name := k.fieldKey(v.X.Type(), v.Field); name != "" {
			k.indexFieldStores()
			return union(k.fieldStores[name])
		}
	case *ssa.UnOp:
		if v.Op != token.MUL {
			break
		}
		switch x := v.X.(type) {
		case *ssa.FieldAddr:
			if name := k.fieldKey(x.X.Type(), x.Field); name != "" {
				k.indexFieldStores()
				return union(k.fieldStores[name])
			}
		case *ssa.IndexAddr:
			// An element of a literal: for _, p := range []byte{...}.
			arr := x.X
			if sl, ok := arr.(*ssa.Slice); ok {
				arr = sl.X
			}
			if a, ok := arr.(*ssa.Alloc); ok {
				if vals := k.arrayConsts(a); len(vals) > 0 {
					return dedupBytes(vals), ""
				}
			}
		case *ssa.Alloc:
			return union(k.cellStores(x))
		case *ssa.FreeVar:
			// A variable captured by reference: what its cells hold.
			var stored []ssa.Value
			for _, b := range k.bindings(x) {
				a, ok := b.(*ssa.Alloc)
				if !ok {
					return nil, fmt.Sprintf("captured cell bound to %T at %s", b, k.p.pos(b.Pos()))
				}
				stored = append(stored, k.cellStores(a)...)
			}
			return union(stored)
		}
	}
	if u, ok := v.(*ssa.UnOp); ok {
		return nil, fmt.Sprintf("load of %T at %s", u.X, k.p.pos(v.Pos()))
	}
	return nil, fmt.Sprintf("%T at %s", v, k.p.pos(v.Pos()))
}

// paramArgs are the values every call of p's function passes for p. A
// function nothing calls passes none when nothing outside the module can call
// it either (a promoted method's wrapper, an unexported function).
func (k *keys) paramArgs(p *ssa.Parameter) ([]ssa.Value, string) {
	fn := p.Parent()
	i := -1
	for j, q := range fn.Params {
		if q == p {
			i = j
		}
	}
	node := k.p.cg.Nodes[fn]
	if i < 0 || node == nil {
		return nil, "parameter of " + k.p.funcName(fn) + ", which the call graph lacks"
	}
	if len(node.In) == 0 && fn.Synthetic == "" && fn.Object() != nil && fn.Object().Exported() {
		return nil, "parameter of " + k.p.funcName(fn) + ", which only callers outside the module call"
	}
	var out []ssa.Value
	for _, e := range node.In {
		if e.Site == nil {
			return nil, "parameter of " + k.p.funcName(fn) + ", called from outside the call graph"
		}
		c := e.Site.Common()
		args := c.Args
		if c.IsInvoke() {
			args = append([]ssa.Value{c.Value}, c.Args...)
		}
		if i >= len(args) {
			return nil, "parameter of " + k.p.funcName(fn) + " passed variadically"
		}
		out = append(out, args[i])
	}
	return out, ""
}

// bindings are the values every closure of f's function captures for f.
func (k *keys) bindings(f *ssa.FreeVar) []ssa.Value {
	fn := f.Parent()
	i := -1
	for j, q := range fn.FreeVars {
		if q == f {
			i = j
		}
	}
	if i < 0 || fn.Parent() == nil {
		return nil
	}
	var out []ssa.Value
	for _, b := range fn.Parent().Blocks {
		for _, ins := range b.Instrs {
			if mc, ok := ins.(*ssa.MakeClosure); ok && mc.Fn == fn && i < len(mc.Bindings) {
				out = append(out, mc.Bindings[i])
			}
		}
	}
	return out
}

// cellStores are the values stored into a local cell: in its function, and
// through the captured variable of every closure that captures it.
func (k *keys) cellStores(a *ssa.Alloc) []ssa.Value {
	var out []ssa.Value
	var visit func(addr ssa.Value)
	visit = func(addr ssa.Value) {
		refs := addr.Referrers()
		if refs == nil {
			return
		}
		for _, ref := range *refs {
			switch r := ref.(type) {
			case *ssa.Store:
				if r.Addr == addr {
					out = append(out, r.Val)
				}
			case *ssa.MakeClosure:
				for i, b := range r.Bindings {
					if b == addr {
						visit(r.Fn.(*ssa.Function).FreeVars[i])
					}
				}
			}
		}
	}
	visit(a)
	return out
}

func dedupBytes(in []byte) []byte {
	seen := map[byte]bool{}
	var out []byte
	for _, b := range in {
		if !seen[b] {
			seen[b] = true
			out = append(out, b)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// closureBytes replaces, in a shape traced in a closure, the bytes that come
// from the closure's own parameters by the constants its calls pass: a
// closure is accounted where it is made, not where it is called.
func (k *keys) closureBytes(s keyShape, closure *ssa.Function, bindings []ssa.Value) keyShape {
	out := s
	out.bytes = append([]byteSrc(nil), s.bytes...)
	for i, b := range out.bytes {
		if b.free > 0 {
			out.bytes[i] = k.boundByte(b, closure, bindings)
			continue
		}
		if b.known || b.param < 0 || len(b.vals) > 0 {
			continue
		}
		out.bytes[i] = byteSrc{param: -1, why: "variadic closure parameter"}
		if b.param < len(closure.Params) {
			vals, why := k.byteVals(closure.Params[b.param], map[ssa.Value]bool{})
			if why == "" && len(vals) > 0 {
				out.bytes[i] = byteSrc{param: -1, vals: vals}
			} else {
				out.bytes[i].why = why
			}
		}
	}
	return out
}

// boundByte resolves a captured byte b of closure in its maker: against the
// value the MakeClosure binds (bindings, when the maker is known), else over
// every closure of it.
func (k *keys) boundByte(b byteSrc, closure *ssa.Function, bindings []ssa.Value) byteSrc {
	i := b.free - 1
	if i >= len(closure.FreeVars) {
		return byteSrc{param: -1, why: "captured byte out of range"}
	}
	var bound []ssa.Value
	if bindings != nil {
		if i < len(bindings) {
			bound = []ssa.Value{bindings[i]}
		}
	} else {
		bound = k.bindings(closure.FreeVars[i])
	}
	if len(bound) == 1 {
		v := bound[0]
		if b.deref {
			if f, ok := v.(*ssa.FreeVar); ok {
				// The cell is itself captured by the maker: resolved where
				// the maker is made.
				for j, q := range f.Parent().FreeVars {
					if q == f {
						return byteSrc{param: -1, free: j + 1, deref: true}
					}
				}
			}
			a, ok := v.(*ssa.Alloc)
			if !ok {
				return byteSrc{param: -1, why: fmt.Sprintf("captured cell bound to %T", v)}
			}
			if stores := k.cellStores(a); len(stores) == 1 && parentOf(stores[0]) == a.Parent() {
				// One value in the maker's context: a parameter of the maker
				// stays a parameter, resolved by its callers.
				return k.byteOf(stores[0])
			}
			vals, why := k.byteVals(&ssa.UnOp{Op: token.MUL, X: a}, map[ssa.Value]bool{})
			if why == "" && len(vals) > 0 {
				return byteSrc{param: -1, vals: vals}
			}
			return byteSrc{param: -1, why: why}
		}
		return k.byteOf(v)
	}
	var vals []byte
	for _, v := range bound {
		x := v
		if b.deref {
			x = &ssa.UnOp{Op: token.MUL, X: v}
		}
		got, why := k.byteVals(x, map[ssa.Value]bool{})
		if why != "" {
			return byteSrc{param: -1, why: why}
		}
		vals = append(vals, got...)
	}
	if len(vals) == 0 {
		return byteSrc{param: -1, why: "captured byte of a closure never made"}
	}
	return byteSrc{param: -1, vals: dedupBytes(vals)}
}

// arrayConsts are the constants stored into an array allocation, when every
// element stored is a constant.
func (k *keys) arrayConsts(arr *ssa.Alloc) []byte {
	var vals []byte
	for _, ref := range *arr.Referrers() {
		ia, ok := ref.(*ssa.IndexAddr)
		if !ok {
			continue
		}
		for _, r := range *ia.Referrers() {
			st, ok := r.(*ssa.Store)
			if !ok || st.Addr != ia {
				continue
			}
			c, ok := st.Val.(*ssa.Const)
			if !ok || c.Value == nil {
				return nil
			}
			u, ok := constant.Uint64Val(c.Value)
			if !ok {
				return nil
			}
			vals = append(vals, byte(u))
		}
	}
	return vals
}

// callShapes traces a call's result: append and the Append functions of
// encoding/binary keep their first argument's prefix; a module function's
// result is traced through its returns, its parameters replaced by the
// call's arguments.
func (k *keys) callShapes(call *ssa.Call, index int, depth int) []keyShape {
	c := call.Common()
	if b, ok := c.Value.(*ssa.Builtin); ok && b.Name() == "append" && len(c.Args) == 2 {
		bases := k.shapes(c.Args[0], depth+1)
		if len(bases) == 0 {
			// The buffer is the cell being traced (buf = append(buf[:0],
			// key...)): what it holds is what is appended.
			bases = []keyShape{bytesShape(nil)}
		}
		var out []keyShape
		for _, base := range bases {
			if base.unknown != "" || base.param >= 0 || base.free >= 0 || len(base.bytes) >= 2 {
				out = append(out, base)
				continue
			}
			for _, tail := range k.shapes(c.Args[1], depth+1) {
				if tail.unknown != "" || tail.param >= 0 || tail.free >= 0 || tail.whole != "" {
					if len(base.bytes) == 0 {
						out = append(out, tail)
					} else {
						out = append(out, base)
					}
					continue
				}
				out = append(out, bytesShape(append(append([]byteSrc(nil), base.bytes...), tail.bytes...)))
			}
		}
		return out
	}
	callee := c.StaticCallee()
	if callee == nil {
		// A key builder passed in as a parameter (or captured by a closure):
		// its callers (or the closure's maker) say which.
		if p, ok := c.Value.(*ssa.Parameter); ok && index == 0 {
			for i, q := range p.Parent().Params {
				if q == p {
					s := paramShape(i)
					s.mode = "call"
					return []keyShape{s}
				}
			}
		}
		f, ok := c.Value.(*ssa.FreeVar)
		if load, isLoad := c.Value.(*ssa.UnOp); isLoad && load.Op == token.MUL {
			// Captured by reference: the closure's maker binds the cell.
			f, ok = load.X.(*ssa.FreeVar)
		}
		if ok && index == 0 {
			for i, q := range f.Parent().FreeVars {
				if q == f {
					s := freeShape(i)
					s.mode = "call"
					return []keyShape{s}
				}
			}
		}
		return []keyShape{unknownShape("dynamic call at " + k.p.pos(call.Pos()))}
	}
	if !inModule(callee) {
		// A badger.Entry stands for its key: NewEntry's first argument,
		// kept by the With* methods (their receiver).
		if callee.Pkg != nil && strings.HasPrefix(callee.Pkg.Pkg.Path(), "github.com/dgraph-io/badger/") && len(c.Args) > 0 &&
			(callee.Name() == "NewEntry" || callee.Signature.Recv() != nil && strings.HasPrefix(callee.Name(), "With") &&
				strings.HasSuffix(types.TypeString(callee.Signature.Recv().Type(), nil), "badger/v4.Entry")) {
			return k.shapes(c.Args[0], depth+1)
		}
		// A key a scan returns: in the range the scan is bounded to.
		if (callee.Name() == "Key" || callee.Name() == "KeyCopy") && callee.Signature.Recv() != nil &&
			strings.HasSuffix(types.TypeString(callee.Signature.Recv().Type(), nil), "badger/v4.Item") && len(c.Args) > 0 {
			return k.scanShapes(c.Args[0], depth)
		}
		// encoding/binary's Append functions (a byte order's methods take
		// the order as their first argument) extend their buffer.
		if strings.HasPrefix(callee.Name(), "Append") {
			base := 0
			if callee.Signature.Recv() != nil {
				base = 1
			}
			if base < len(c.Args) {
				return k.shapes(c.Args[base], depth+1)
			}
		}
		return []keyShape{unknownShape("result of " + callee.String())}
	}
	var out []keyShape
	for _, s := range k.shapes(callee, depth, index) {
		out = append(out, k.substitute(s, c.Args, call, depth)...)
	}
	return out
}

// substitute replaces, in a shape traced in a callee, its parameters by the
// call's arguments.
func (k *keys) substitute(s keyShape, args []ssa.Value, call *ssa.Call, depth int) []keyShape {
	if s.param >= 0 {
		if s.param < len(args) {
			return k.argShapes(s, args[s.param], depth)
		}
		return []keyShape{unknownShape("variadic parameter")}
	}
	if s.free >= 0 {
		return []keyShape{unknownShape("captured variable of a called closure")}
	}
	out := s
	out.bytes = append([]byteSrc(nil), s.bytes...)
	for i, b := range out.bytes {
		if !b.known && b.param >= 0 && b.param < len(args) {
			out.bytes[i] = k.byteOf(args[b.param])
		}
	}
	return []keyShape{out}
}

// argShapes resolves a parameter (or captured variable) shape s against the
// value v the caller (or closure maker) supplies: the value itself, the key
// a supplied key builder returns ("call"), or what a supplied cell holds
// ("deref").
func (k *keys) argShapes(s keyShape, v ssa.Value, depth int) []keyShape {
	if depth > 32 {
		return []keyShape{unknownShape("recursive key builder argument")}
	}
	switch s.mode {
	case "call":
		var fn *ssa.Function
		switch f := v.(type) {
		case *ssa.Function:
			fn = f
		case *ssa.MakeClosure:
			fn = f.Fn.(*ssa.Function)
		case *ssa.Alloc:
			// A key builder captured by reference: every builder its
			// cell is set to.
			var out []keyShape
			for _, st := range k.cellStores(f) {
				out = append(out, k.argShapes(s, st, depth+1)...)
			}
			if len(out) == 0 {
				return []keyShape{unknownShape("key builder cell with no store at " + k.p.pos(f.Pos()))}
			}
			return out
		case *ssa.Phi:
			var out []keyShape
			for i, edge := range f.Edges {
				for _, shape := range k.argShapes(s, edge, depth+1) {
					shape.selectors = append(append([]selectorFact(nil), shape.selectors...), blockSelectors(f.Parent(), f.Block().Preds[i], f.Block())...)
					out = append(out, shape)
				}
			}
			return out
		case *ssa.Parameter:
			for i, q := range f.Parent().Params {
				if q == f {
					out := paramShape(i)
					out.mode = "call"
					return []keyShape{out}
				}
			}
		}
		if fn == nil || fn.Blocks == nil {
			return []keyShape{unknownShape(fmt.Sprintf("key builder passed as %T", v))}
		}
		var out []keyShape
		for _, r := range k.shapes(fn, depth+1, 0) {
			if r.param >= 0 || r.free >= 0 {
				r = unknownShape("key builder " + k.p.funcName(fn) + " returns a key its caller supplies")
			}
			for i, b := range r.bytes {
				if !b.known && b.param >= 0 && r.bytes[i].str == "" {
					r.bytes = append([]byteSrc(nil), r.bytes...)
					r.bytes[i] = byteSrc{param: -1}
				}
			}
			out = append(out, r)
		}
		return out
	case "deref":
		if a, ok := v.(*ssa.Alloc); ok {
			return k.cellShapes(a, depth)
		}
		return k.loadShapes(v, depth)
	case "elem":
		return k.elementShapes(v, depth+1, map[ssa.Value]bool{})
	}
	return k.shapes(v, depth+1)
}

// id keeps actual SSA identity in conditional shapes. SSA's String method
// names values for display only: two functions can both have a parameter kind.
func (s keyShape) id(identity bool) string {
	selectors := s.selectors
	s.selectors = nil
	key := fmt.Sprintf("%v", s)
	for _, fact := range selectors {
		key += fmt.Sprintf("|%s:%s:%d:%d:%s", fact.value.Parent(), fact.value.Name(), fact.value.Pos(), fact.number, fact.op)
		if identity {
			key += fmt.Sprintf(":%p", fact.value)
		}
	}
	return key
}

func dedupShapes(in []keyShape) []keyShape {
	seen := map[string]bool{}
	var out []keyShape
	for _, s := range in {
		key := s.id(true)
		if !seen[key] {
			seen[key] = true
			out = append(out, s)
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].id(false) < out[j].id(false) })
	return out
}

// scanShapes traces the key of a scanned item to the prefix its iterator is
// bounded to: the argument of ValidForPrefix (or Seek) on that iterator.
func (k *keys) scanShapes(item ssa.Value, depth int) []keyShape {
	call, ok := item.(*ssa.Call)
	if !ok || len(call.Common().Args) == 0 {
		return []keyShape{unknownShape("scanned item of unknown origin at " + k.p.pos(item.Pos()))}
	}
	it := call.Common().Args[0]
	var out []keyShape
	for _, ref := range *it.Referrers() {
		rc, ok := ref.(*ssa.Call)
		if !ok {
			continue
		}
		callee := rc.Common().StaticCallee()
		// Seek only positions the iterator; ValidForPrefix bounds it.
		if callee == nil || callee.Name() != "ValidForPrefix" || len(rc.Common().Args) < 2 {
			continue
		}
		out = append(out, k.shapes(rc.Common().Args[1], depth+1)...)
	}
	// An iterator opened with IteratorOptions.Prefix set is bounded to it.
	if open, ok := it.(*ssa.Call); ok {
		if callee := open.Common().StaticCallee(); callee != nil && callee.Name() == "NewIterator" && len(open.Common().Args) == 2 {
			out = append(out, k.optionPrefix(open.Common().Args[1], depth+1)...)
		}
	}
	if len(out) == 0 {
		return []keyShape{{param: -1, free: -1, whole: "badger.unbounded-scan"}}
	}
	// Every bound holds for every item: one the census traces suffices
	// (ValidForPrefix(opts.Prefix) reads the bound back from the options).
	var known []keyShape
	for _, s := range out {
		if s.unknown == "" {
			known = append(known, s)
		}
	}
	if len(known) > 0 {
		return known
	}
	return out
}

// optionPrefix traces the Prefix field of a badger.IteratorOptions value:
// set in a local copy, or in the options a module function returns.
func (k *keys) optionPrefix(opts ssa.Value, depth int) []keyShape {
	if depth > 24 {
		return nil
	}
	switch v := opts.(type) {
	case *ssa.UnOp:
		a, ok := v.X.(*ssa.Alloc)
		if !ok || v.Op != token.MUL {
			return nil
		}
		var out []keyShape
		for _, ref := range *a.Referrers() {
			switch r := ref.(type) {
			case *ssa.FieldAddr:
				st, ok := r.X.Type().Underlying().(*types.Pointer).Elem().Underlying().(*types.Struct)
				if !ok || st.Field(r.Field).Name() != "Prefix" {
					continue
				}
				for _, fr := range *r.Referrers() {
					if s, ok := fr.(*ssa.Store); ok && s.Addr == r {
						out = append(out, k.shapes(s.Val, depth+1)...)
					}
				}
			case *ssa.Store:
				if r.Addr == a {
					out = append(out, k.optionPrefix(r.Val, depth+1)...)
				}
			}
		}
		return out
	case *ssa.Call:
		callee := v.Common().StaticCallee()
		if callee == nil || !inModule(callee) {
			return nil
		}
		var out []keyShape
		for _, b := range callee.Blocks {
			if ret, ok := b.Instrs[len(b.Instrs)-1].(*ssa.Return); ok && len(ret.Results) > 0 {
				for _, s := range k.optionPrefix(ret.Results[0], depth+1) {
					out = append(out, k.substitute(s, v.Common().Args, v, depth)...)
				}
			}
		}
		return out
	}
	return nil
}
