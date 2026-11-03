package workcensus

import (
	"go/token"
	"go/types"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/tools/go/ssa"
)

// localObjectEscapeFixture holds the lease plants for the local-object escape
// owner. Every plant allocates one *lease whose token field is already stored,
// and the question each plant asks is whether a reference derived from that
// object's address leaves the frame's own memory: an address handed to a call,
// stored as a value, captured by a closure, or passed to go/defer. Nothing here
// names a production function, field, package or type.
const localObjectEscapeFixture = `package fixture

type token struct{ n int }

type lease struct {
	token *token
	bound bool
	self  *lease
}

func probe() int { return 1 }

func sink(t *token) {}

func fillNil(l *lease) { l.token = nil }

func takeToken(p **token) { *p = nil }

var keepLease *lease

func conforming() {
	l := &lease{token: &token{}}
	l.bound = true
	_ = probe()
	sink(l.token)
}

func returnConforming() lease {
	l := &lease{token: &token{}}
	l.bound = true
	_ = probe()
	return *l
}

func returnEscaped() lease {
	l := &lease{token: &token{}}
	keepLease = l
	return *l
}

func overwritten() {
	l := &lease{token: &token{}}
	l.token = nil
	_ = probe()
	sink(l.token)
}

func omitted() {
	l := &lease{}
	l.bound = true
	_ = probe()
	sink(l.token)
}

func wholeZero() {
	l := &lease{token: &token{}}
	*l = lease{}
	sink(l.token)
}

func sibling(other *lease) {
	l := &lease{token: &token{}}
	other.token = nil
	sink(l.token)
}

func aliased() {
	l := &lease{token: &token{}}
	p := l
	p.token = nil
	sink(l.token)
}

func cellAliased() {
	var p *lease
	l := &lease{token: &token{}}
	hold := func() { p = l }
	hold()
	p.token = nil
	sink(l.token)
}

func build(t *token) {
	l := &lease{token: t}
	_ = probe()
	sink(l.token)
}

func callerNil() { build(nil) }

func staticWriter() {
	l := &lease{token: &token{}}
	fillNil(l)
	sink(l.token)
}

func dynamicWriter(opaque func(*lease)) {
	l := &lease{token: &token{}}
	opaque(l)
	sink(l.token)
}

func callDynamicWriter() { dynamicWriter(fillNil) }

func closureWriter() {
	l := &lease{token: &token{}}
	f := func() { l.token = nil }
	f()
	sink(l.token)
}

func goWriter() {
	l := &lease{token: &token{}}
	go func(x *lease) { x.token = nil }(l)
	sink(l.token)
}

func deferWriter() {
	l := &lease{token: &token{}}
	defer func(x *lease) { x.token = nil }(l)
	sink(l.token)
}

func escapeGlobal() {
	l := &lease{token: &token{}}
	keepLease = l
	sink(l.token)
}

func backedge() {
	l := &lease{token: &token{}}
	l.self = l
	sink(l.token)
}

func fieldAddressEscape() {
	l := &lease{token: &token{}}
	takeToken(&l.token)
	sink(l.token)
}

func beforeAfter() {
	l := &lease{token: &token{}}
	l.token = &token{}
	_ = probe()
	l.token = nil
	sink(l.token)
}

func repeatedStore() {
	l := &lease{token: &token{}}
	l.token = nil
	_ = probe()
	l.token = nil
	sink(l.token)
}

func nilSource() { fillNil(&lease{}) }

var _ = []any{conforming, returnConforming, returnEscaped, overwritten, omitted,
	wholeZero, sibling, aliased, cellAliased, build, callerNil, staticWriter,
	dynamicWriter, callDynamicWriter, closureWriter, goWriter, deferWriter,
	escapeGlobal, backedge, fieldAddressEscape, beforeAfter, repeatedStore,
	nilSource, fillNil, takeToken, sink, probe}
`

// localObjectClosureFixture holds the value-struct plants: the object is a frame
// value whose pointer field a closure captures and writes. These plants are
// loaded in their own program so a field's possible values come from these
// plants alone, which keeps the answer attributable.
const localObjectClosureFixture = `package fixture

type marker struct{ n int }

type frame struct{ item *marker }

func probe() int { return 1 }

func observe(m *marker) {}

func unrelated() {
	v := frame{item: &marker{}}
	_ = probe()
	observe(v.item)
}

func closureWrite() {
	v := frame{item: &marker{}}
	f := func() { v.item = nil }
	f()
	observe(v.item)
}

func fieldPointerClosure() {
	v := frame{item: &marker{}}
	p := &v.item
	f := func() { *p = nil }
	f()
	observe(v.item)
}

var _ = []any{unrelated, closureWrite, fieldPointerClosure, observe, probe}
`

// localObjectPlant names one fixture function and what the escape owner must
// decide about the object its read names.
type localObjectPlant struct {
	name   string
	callee string // the fixture callee whose one call the plant's field read reaches
	// inline is localInlineObject at that read: true only when no reference
	// derived from the object's address leaves the frame's own memory, so no
	// call the frame makes can write the field.
	inline bool
	// reaching is the kind of the one definition that reaches the field at the
	// read, or "" where the read is not rooted at a local object.
	reaching string
	// nilChecked marks a plant whose product answer is asserted; nilAtRead is
	// the expected presence of a nil witness there.
	nilChecked bool
	nilAtRead  bool
}

// localObjectPlants is the lease obligation matrix. The conforming plant is the
// one whose unrelated call must stop being a possible writer of the field; every
// plant that lets a reference derived from the object's address out of the frame
// keeps that call a possible writer, and every plant with a real write, a zero
// field, another object's store, a caller nil or an escaped address keeps
// reporting a nil.
var localObjectPlants = []localObjectPlant{
	// The unrelated call takes no reference to the object, and the only
	// reference uses are loads and stores of its inline fields.
	{name: "conforming", callee: "sink", inline: true, reaching: "store", nilChecked: true},

	// Real writes: a later store, an omitted field, a whole-value store,
	// another object's field store, an alias mutation, and a caller's nil.
	{name: "overwritten", callee: "sink", inline: true, reaching: "store", nilChecked: true, nilAtRead: true},
	{name: "omitted", callee: "sink", inline: true, reaching: "start", nilChecked: true, nilAtRead: true},
	{name: "wholeZero", callee: "sink", inline: true, reaching: "whole", nilChecked: true, nilAtRead: true},
	{name: "sibling", callee: "sink", inline: true, reaching: "unknown", nilChecked: true, nilAtRead: true},
	// An SSA alias is the same value, so its field store is the object's own.
	{name: "aliased", callee: "sink", inline: true, reaching: "store", nilChecked: true, nilAtRead: true},
	// An alias held in a cell stores the address as a value, and the read then
	// goes through that cell rather than through the object's own address.
	{name: "cellAliased", callee: "sink", inline: false, nilChecked: true, nilAtRead: true},
	{name: "build", callee: "sink", inline: true, reaching: "store", nilChecked: true, nilAtRead: true},

	// The address stored as a value: into a package variable, or into the
	// object itself. Both facts refuse a store whose value is the address; the
	// field walk additionally treats the self store as an unknown write.
	{name: "escapeGlobal", callee: "sink", inline: false, reaching: "unknown", nilChecked: true, nilAtRead: true},
	{name: "backedge", callee: "sink", inline: false, reaching: "unknown", nilChecked: true, nilAtRead: true},

	// The address or a reference derived from it handed to a call: static,
	// dynamic, and through the field's own address.
	{name: "staticWriter", callee: "sink", inline: false, reaching: "unknown", nilChecked: true, nilAtRead: true},
	{name: "dynamicWriter", callee: "sink", inline: false, reaching: "unknown", nilChecked: true, nilAtRead: true},
	{name: "fieldAddressEscape", callee: "sink", inline: false, reaching: "unknown", nilChecked: true, nilAtRead: true},

	// The variable captured by a closure, or passed to go. A closure value can
	// run at any time, so its stores are not the frame's visible stores.
	{name: "closureWriter", callee: "sink", inline: false, nilChecked: true, nilAtRead: true},
	{name: "goWriter", callee: "sink", inline: false, reaching: "unknown", nilChecked: true, nilAtRead: true},

	// A deferred call runs after the read, so the address escapes and the read
	// still sees the stored token.
	{name: "deferWriter", callee: "sink", inline: false, reaching: "store", nilChecked: true},

	// Stores before and after the unrelated call: both are real writes, and the
	// read follows the store that reaches it.
	{name: "beforeAfter", callee: "sink", inline: true, reaching: "store", nilChecked: true, nilAtRead: true},
	{name: "repeatedStore", callee: "sink", inline: true, reaching: "store", nilChecked: true, nilAtRead: true},
}

// localClosurePlants is the value-struct matrix: the same two facts asked of an
// object a closure captures, and the product answer at the read after the call.
var localClosurePlants = []localObjectPlant{
	// The unrelated call and the stores before the read leave the token stored.
	{name: "unrelated", callee: "observe", inline: true, reaching: "whole", nilChecked: true},

	// The exact counterexample: the closure writes the pointer field of the
	// captured value struct, so the read after the call must keep reporting a
	// nil. A model that proves the field non-nil here has ignored that write.
	{name: "closureWrite", callee: "observe", inline: false, reaching: "unknown", nilChecked: true, nilAtRead: true},

	// A field address held in a cell and captured by a closure is a derived
	// reference that leaves the frame, and the write through it reaches the
	// read. The store is not an inline field store the field owner attributes,
	// so the nil witness is not required to name it.
	{name: "fieldPointerClosure", callee: "observe", inline: false, reaching: "unknown"},
}

func loadLocalObjectEscapeProgram(t *testing.T) *Program {
	t.Helper()
	return loadEscapeProgram(t, "local_object_escape.go", localObjectEscapeFixture)
}

func loadLocalObjectClosureProgram(t *testing.T) *Program {
	t.Helper()
	return loadEscapeProgram(t, "local_object_closure.go", localObjectClosureFixture)
}

func loadEscapeProgram(t *testing.T, name, fixture string) *Program {
	t.Helper()
	c := genericConfig(t)
	c.IncludeProgram = true
	c.Overlay = map[string][]byte{filepath.Join(c.Dir, name): []byte(fixture)}
	return mustExtract(t, c).Program
}

// fixtureFunction returns the fixture package function with that name.
func fixtureFunction(t *testing.T, p *Program, name string) *ssa.Function {
	t.Helper()
	for _, fn := range p.Functions {
		if fn.Pkg != nil && fn.Pkg.Pkg.Path() == "census.fixture" && fn.Name() == name {
			return fn
		}
	}
	t.Fatalf("fixture function %s not loaded", name)
	return nil
}

// fieldRead returns the plant's single call to callee, with the inline field
// address it reads, that address's load, and the field itself.
func fieldRead(t *testing.T, fn *ssa.Function, callee string) (ssa.CallInstruction, ssa.Value, *ssa.UnOp, *types.Var) {
	t.Helper()
	var calls []ssa.CallInstruction
	for _, block := range fn.Blocks {
		for _, ins := range block.Instrs {
			call, ok := ins.(ssa.CallInstruction)
			if !ok || call.Common().StaticCallee() == nil || call.Common().StaticCallee().Name() != callee {
				continue
			}
			calls = append(calls, call)
		}
	}
	if len(calls) != 1 {
		t.Fatalf("%s: %d calls to %s, want one", fn.Name(), len(calls), callee)
	}
	load, ok := calls[0].Common().Args[0].(*ssa.UnOp)
	if !ok || load.Op != token.MUL {
		t.Fatalf("%s: %s argument is not a load", fn.Name(), callee)
	}
	addr, ok := load.X.(*ssa.FieldAddr)
	if !ok {
		t.Fatalf("%s: %s reads through %T, not an inline field address", fn.Name(), callee, load.X)
	}
	field := structField(addr.X.Type(), addr.Field)
	if field == nil {
		t.Fatalf("%s: the read names no inline field", fn.Name())
	}
	return calls[0], addr, load, field
}

// TestLocalObjectEscapeOwnership pins the escape owner's answer for every plant:
// only an object whose address-derived references stay in its frame is inline,
// and only for such an object may a non-builtin call stop being a writer of the
// field. Every plant's read addresses an inline field, so the whole-cell fact
// must refuse the same object the strict inline fact follows.
func TestLocalObjectEscapeOwnership(t *testing.T) {
	checkOwnership(t, loadLocalObjectEscapeProgram(t), localObjectPlants)
}

// TestLocalClosureEscapeOwnership pins the same two facts for the value-struct
// plants.
func TestLocalClosureEscapeOwnership(t *testing.T) {
	checkOwnership(t, loadLocalObjectClosureProgram(t), localClosurePlants)
}

func checkOwnership(t *testing.T, p *Program, plants []localObjectPlant) {
	t.Helper()
	for _, plant := range plants {
		t.Run(plant.name, func(t *testing.T) {
			fn := fixtureFunction(t, p, plant.name)
			_, addr, load, field := fieldRead(t, fn, plant.callee)
			if got := localInlineObject(addr); got != plant.inline {
				t.Errorf("%s: localInlineObject=%v want %v", plant.name, got, plant.inline)
			}
			if object, ok := addressRoot(addr).(*ssa.Alloc); ok && localCell(object) {
				t.Errorf("%s: the whole-cell fact accepted an object whose inline field the read addresses", plant.name)
			}
			if plant.reaching == "" {
				return
			}
			definitions := reachingDefinitions(load, addr, field)
			if len(definitions) != 1 {
				t.Fatalf("%s: definitions=%v want exactly one", plant.name, definitions)
			}
			if got := definitionKind(definitions[0]); got != plant.reaching {
				t.Errorf("%s: reaching definition=%s want %s", plant.name, got, plant.reaching)
			}
		})
	}
}

// definitionKind labels the definition the walk stopped at.
func definitionKind(d definition) string {
	switch {
	case d.store != nil:
		return "store"
	case d.whole != nil:
		return "whole"
	case d.unknown:
		return "unknown"
	case d.nilBranch:
		return "nil-branch"
	case d.nonNilBranch:
		return "non-nil-branch"
	}
	return "start"
}

// TestLocalObjectEscapePreservesNilFlow keeps the product answer for every
// plant: the conforming object's field keeps the token stored before the
// unrelated call, and every plant with a real overwrite, an omitted or zero
// field, another object's store, an alias mutation, a caller's nil or an
// escaped address still reports a nil.
func TestLocalObjectEscapePreservesNilFlow(t *testing.T) {
	checkNilFlow(t, loadLocalObjectEscapeProgram(t), localObjectPlants)
}

// TestLocalClosureEscapePreservesNilFlow keeps the product answer for the
// value-struct plants, the exact counterexample included.
func TestLocalClosureEscapePreservesNilFlow(t *testing.T) {
	checkNilFlow(t, loadLocalObjectClosureProgram(t), localClosurePlants)
}

func checkNilFlow(t *testing.T, p *Program, plants []localObjectPlant) {
	t.Helper()
	for _, plant := range plants {
		if !plant.nilChecked {
			continue
		}
		t.Run(plant.name, func(t *testing.T) {
			fn := fixtureFunction(t, p, plant.name)
			call, _, load, _ := fieldRead(t, fn, plant.callee)
			witness := p.MayBeNil(load, call)
			if plant.nilAtRead && len(witness) == 0 {
				t.Errorf("%s: nil not reported", plant.name)
			}
			if !plant.nilAtRead && len(witness) != 0 {
				t.Errorf("%s: non-nil field reported as nil: %s", plant.name, strings.Join(witness, " "))
			}
		})
	}
}

// TestValueStructClosureRetainsFieldWrite isolates the exact counterexample. The
// pointer field is initialized, the closure sets it to nil, and the read after
// the call must report that nil: the closure's store is a real write the
// frame's own instruction stream does not show, so no proof may skip it.
func TestValueStructClosureRetainsFieldWrite(t *testing.T) {
	p := loadLocalObjectClosureProgram(t)
	fn := fixtureFunction(t, p, "closureWrite")
	call, addr, load, field := fieldRead(t, fn, "observe")
	object, ok := addressRoot(addr).(*ssa.Alloc)
	if !ok {
		t.Fatalf("closureWrite: the read is not rooted at a local allocation: %T", addressRoot(addr))
	}
	if localInlineObject(addr) {
		t.Errorf("closureWrite: the captured object is still reported inline, so the closure call stops being a writer")
	}
	if localCell(object) {
		t.Errorf("closureWrite: the whole-cell fact followed an object whose inline field is written")
	}
	witness := p.MayBeNil(load, call)
	if len(witness) == 0 {
		t.Fatalf("closureWrite: the closure's nil store is missing and the field is still proven non-nil")
	}
	if !strings.Contains(strings.Join(witness, " "), "stored in field "+field.Name()) {
		t.Errorf("closureWrite: the nil witness does not name the field store: %s", strings.Join(witness, " "))
	}
}

// TestLocalCellWholeCellFact pins that the exported whole-cell fact keeps its own
// meaning: it follows a capturing closure of the cell itself, and it refuses the
// object whose inline field is addressed. The strict inline fact is a different
// answer, asked only through localInlineObject.
func TestLocalCellWholeCellFact(t *testing.T) {
	p := loadLocalObjectEscapeProgram(t)
	t.Run("captured cell", func(t *testing.T) {
		fn := fixtureFunction(t, p, "closureWriter")
		var cell ssa.Value
		for _, block := range fn.Blocks {
			for _, ins := range block.Instrs {
				closure, ok := ins.(*ssa.MakeClosure)
				if ok && len(closure.Bindings) > 0 {
					cell = closure.Bindings[0]
				}
			}
		}
		alloc, ok := cell.(*ssa.Alloc)
		if !ok {
			t.Fatalf("closureWriter: no captured cell, got %T", cell)
		}
		if !localCell(alloc) || !LocalCell(alloc) {
			t.Errorf("closureWriter: the captured cell lost the whole-cell fact: localCell=%v LocalCell=%v", localCell(alloc), LocalCell(alloc))
		}
	})
	t.Run("field addressed", func(t *testing.T) {
		fn := fixtureFunction(t, p, "conforming")
		_, addr, _, _ := fieldRead(t, fn, "sink")
		alloc, ok := addressRoot(addr).(*ssa.Alloc)
		if !ok {
			t.Fatalf("conforming: the read is not rooted at a local allocation: %T", addressRoot(addr))
		}
		if localCell(alloc) || LocalCell(alloc) {
			t.Errorf("conforming: the whole-cell fact accepted an object whose inline field is addressed")
		}
	})
}

// TestLocalObjectEscapeStopsProjection pins the two projected-field outcomes at
// a successful return: the conforming object's field is proven, and the object
// whose address reached a package variable is not (the diagnostic
// BadAdmissionEscape shape, where a global pointer store slipped through the
// inline field's ownership test).
func TestLocalObjectEscapeStopsProjection(t *testing.T) {
	p := loadLocalObjectEscapeProgram(t)
	for _, want := range []struct {
		name    string
		escaped bool
	}{
		{name: "returnConforming", escaped: false},
		{name: "returnEscaped", escaped: true},
	} {
		t.Run(want.name, func(t *testing.T) {
			fn := fixtureFunction(t, p, want.name)
			var ret *ssa.Return
			for _, block := range fn.Blocks {
				if last, ok := block.Instrs[len(block.Instrs)-1].(*ssa.Return); ok {
					ret = last
				}
			}
			if ret == nil {
				t.Fatalf("%s: no return", want.name)
			}
			value := ret.Results[0]
			field := structField(value.Type(), 0)
			if field == nil {
				t.Fatalf("%s: return does not hold the inline struct", want.name)
			}
			projection := p.ProjectFieldAt(value, []int{fieldIndex(value.Type(), field)}, ret)
			if projection.Field != field {
				t.Fatalf("%s: projection field=%v want %v", want.name, projection.Field, field)
			}
			if want.escaped {
				if len(projection.Witness) == 0 || !projection.Unknown {
					t.Errorf("%s: escaped object's field still proven stable: witness=%v unknown=%v", want.name, projection.Witness, projection.Unknown)
				}
				return
			}
			if len(projection.Witness) != 0 || projection.Unknown {
				t.Errorf("%s: conforming object's field not proven: witness=%v unknown=%v why=%v", want.name, projection.Witness, projection.Unknown, projection.Why)
			}
		})
	}
}
