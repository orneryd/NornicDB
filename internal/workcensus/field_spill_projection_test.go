package workcensus

import (
	"sort"
	"strings"
	"testing"

	"golang.org/x/tools/go/ssa"
)

// The spill projection fixture reproduces, generically, the shape of a
// lifecycle lease and its callers, and the one projection the inline field
// reader used to assume:
//
//   - mintLease is a checked tuple constructor (beginLifecycleWrite): every
//     refused admission returns the zero lease with the error that refused it,
//     and only the admitted path returns the built lease with a nil error.
//   - finish is a value method that never reads the lease's context field
//     (lifecycleWrite.finish), so the compiler spills its receiver parameter
//     into the callee's own local as one whole-value store.
//   - takeLease is a pointer helper that reads the context field of a lease it
//     received through a pointer, as the deletion and restore paths read
//     lifecycle.ctx from *lifecycleWrite.
//   - the plants keep the lease in a local, defer its value-method release, copy
//     it through a whole-value load (the restore shape) and hand its address to
//     the pointer helper; one plant parks the copy's address in a package
//     variable, the escaped/pointer chain this projection cannot prove.
//
// The question at every use is the same: the lease's context field is the
// constructor's checked success return, not an automatic zero because the
// field of some local copy was never addressed. A whole-value load of a local
// object whose field was never addressed holds whatever whole-value stores put
// there; answering "zero" without reading them contaminates the field's
// provenance for every other use.
//
// Nothing here names a production function, package, field or type.

const spillProjectionBase = `package fixture

import "context"

type spillRefusal struct{}

func (*spillRefusal) Error() string { return "refused" }

type spillGate struct{ depth int }

type spillSeal struct{ n int }

type spillLease struct {
	flow    context.Context
	owner   *spillGate
	held    *spillSeal
	dropped bool
}

// finish is the lease's release: a value method, so deferring it copies the
// lease and it never reads flow, exactly as lifecycleWrite.finish does.
func (l spillLease) finish() { l.owner.leave() }

func (g *spillGate) leave() { g.depth-- }

func (g *spillGate) hold() *spillSeal { return &spillSeal{} }

func (g *spillGate) enter(ctx context.Context) error {
	if ctx == nil {
		return &spillRefusal{}
	}
	g.depth++
	return nil
}

func (g *spillGate) drain(ctx context.Context) error { return ctx.Err() }

// mintLease is the checked tuple constructor: every refusal returns the zero
// lease with its error; only the admitted path returns the built lease with a
// nil error.
func mintLease(g *spillGate, ctx context.Context) (spillLease, error) {
	if err := g.enter(ctx); err != nil {
		return spillLease{}, err
	}
	if err := g.drain(ctx); err != nil {
		g.leave()
		return spillLease{}, err
	}
	return spillLease{flow: ctx, owner: g, held: g.hold()}, nil
}

func fillSpill(ctx context.Context) error { return ctx.Err() }

// takeLease reads the context field of a lease it received through a pointer:
// it has no definition of that field in its own body, so the field's possible
// values anywhere are what it sees.
func takeLease(l *spillLease) error { return fillSpill(l.flow) }

var parkedLease *spillLease

func parkLease(l *spillLease) { parkedLease = l }

var leaseFactory func() (spillLease, error)

var leaseErrorFactory func() error
`

// spillProjectionControl is the positive control every plant group carries: the
// admitted chain read directly. A group cannot pass its plants by answering
// zero everywhere without failing this control.
const spillProjectionControl = `
func admittedDirect(g *spillGate) error {
	life, err := mintLease(g, context.Background())
	if err != nil {
		return err
	}
	defer life.finish()
	return fillSpill(life.flow)
}
`

// spillProjectionPlants are the shapes the projection must read: the
// value-method receiver spill, the copy the restore path makes, and the pointer
// helper that reads the lease's field. Every lease in this group comes from the
// constructor's checked success return, which is non-nil.
const spillProjectionPlants = spillProjectionControl + `
func deferredCopy(g *spillGate) error {
	life, err := mintLease(g, context.Background())
	if err != nil {
		return err
	}
	defer life.finish()
	return takeLease(&life)
}

func copiedLease(g *spillGate) error {
	life, err := mintLease(g, context.Background())
	if err != nil {
		return err
	}
	copied := life
	defer copied.finish()
	return takeLease(&copied)
}
`

// spillZeroPlants keep a real zero source on the supplying path: a constructor
// whose success return is the zero lease or omits the field, an ignored error,
// an error the model cannot prove non-nil, an erased error, and an unresolved
// factory.
const spillZeroPlants = spillProjectionControl + `
func zeroLease(g *spillGate) (spillLease, error) { return spillLease{}, nil }

func zeroSuccess(g *spillGate) error {
	life, err := zeroLease(g)
	if err != nil {
		return err
	}
	defer life.finish()
	return fillSpill(life.flow)
}

func omittedLease(g *spillGate) (spillLease, error) { return spillLease{owner: g}, nil }

func omittedField(g *spillGate) error {
	life, err := omittedLease(g)
	if err != nil {
		return err
	}
	defer life.finish()
	return fillSpill(life.flow)
}

func ignoredError(g *spillGate) error {
	life, _ := mintLease(g, context.Background())
	defer life.finish()
	return fillSpill(life.flow)
}

func opaqueLease(g *spillGate) (spillLease, error) { return spillLease{}, leaseErrorFactory() }

func unknownError(g *spillGate) error {
	life, err := opaqueLease(g)
	if err != nil {
		return err
	}
	defer life.finish()
	return fillSpill(life.flow)
}

func erasedLease(g *spillGate) (l spillLease, err error) {
	defer func() { err = nil }()
	return spillLease{}, &spillRefusal{}
}

func erasedError(g *spillGate) error {
	life, err := erasedLease(g)
	if err != nil {
		return err
	}
	defer life.finish()
	return fillSpill(life.flow)
}

func unknownFactory(g *spillGate) error {
	life, err := leaseFactory()
	if err != nil {
		return err
	}
	defer life.finish()
	return fillSpill(life.flow)
}
`

// spillGuardPlants keep the zero return because the success edge of the
// constructor's own error test does not reach the read: a different call's
// error is tested, or the error is overwritten before it.
const spillGuardPlants = spillProjectionControl + `
func mismatchedGuard(g *spillGate) error {
	life, _ := mintLease(g, context.Background())
	if err := g.drain(context.Background()); err != nil {
		return err
	}
	defer life.finish()
	return fillSpill(life.flow)
}

func overwrittenError(g *spillGate) error {
	life, err := mintLease(g, context.Background())
	err = nil
	if err != nil {
		return err
	}
	defer life.finish()
	return fillSpill(life.flow)
}
`

// spillWritePlants keep the zero return because a write reaches the read after
// the admission: through an alias, a capturing closure, a pointer handed to a
// helper, a whole-value overwrite, a cyclic overwrite, or an escaped parking
// slot that a later call clears.
const spillWritePlants = spillProjectionControl + `
func clearLease(l *spillLease) { l.flow = nil }

func clearParked() { parkedLease.flow = nil }

func aliasedWrite(g *spillGate) error {
	life, err := mintLease(g, context.Background())
	if err != nil {
		return err
	}
	defer life.finish()
	alias := &life
	alias.flow = nil
	return fillSpill(life.flow)
}

func closureWrite(g *spillGate) error {
	life, err := mintLease(g, context.Background())
	if err != nil {
		return err
	}
	defer life.finish()
	hold := func() { life.flow = nil }
	hold()
	return fillSpill(life.flow)
}

func escapedWrite(g *spillGate) error {
	life, err := mintLease(g, context.Background())
	if err != nil {
		return err
	}
	defer life.finish()
	clearLease(&life)
	return fillSpill(life.flow)
}

func overwrittenLease(g *spillGate) error {
	life, err := mintLease(g, context.Background())
	if err != nil {
		return err
	}
	defer life.finish()
	life = spillLease{}
	return fillSpill(life.flow)
}

func cyclicLease(g *spillGate, attempts int) error {
	life, err := mintLease(g, context.Background())
	if err != nil {
		return err
	}
	defer life.finish()
	for i := 0; i < attempts; i++ {
		if i == 1 {
			life = spillLease{}
		}
	}
	return fillSpill(life.flow)
}

func parkedWrite(g *spillGate) error {
	life, err := mintLease(g, context.Background())
	if err != nil {
		return err
	}
	parkLease(&life)
	defer life.finish()
	clearParked()
	return takeLease(&life)
}
`

// spillHeapPlants keep the pointer chain unresolved: a zero lease is parked in
// the heap the helper reads through, so nothing in the source proves the field
// of the object the helper receives.
const spillHeapPlants = spillProjectionControl + `
func parkedZero(g *spillGate) error {
	parkLease(&spillLease{})
	return takeLease(parkedLease)
}
`

// spillEscapePlants keep the copy's release unresolved: the copy's address is
// parked in a package variable, so a write through that reference may reach the
// field and no inline projection can prove the value the release receives.
const spillEscapePlants = spillProjectionControl + `
func parkedCopy(g *spillGate) error {
	life, err := mintLease(g, context.Background())
	if err != nil {
		return err
	}
	copied := life
	parkLease(&copied)
	defer copied.finish()
	return takeLease(&copied)
}
`

func loadSpillProjection(t *testing.T, name, plants string) *Program {
	t.Helper()
	return loadEscapeProgram(t, name, spillProjectionBase+plants)
}

// spillUse is one plant's read of its lease: the deferred value-method release
// that receives the whole lease, and the downstream call that receives the
// lease's inline context field as a plain value.
type spillUse struct {
	lease  ssa.Value // the lease value passed to finish
	finish ssa.Instruction
	ctx    ssa.Value // the context value passed to the downstream call
	call   ssa.CallInstruction
}

func spillUses(t *testing.T, p *Program) map[string]spillUse {
	t.Helper()
	uses := map[string]spillUse{}
	for _, fn := range p.Functions {
		if fn.Pkg == nil || fn.Pkg.Pkg.Path() != "census.fixture" {
			continue
		}
		owner := OutermostFunction(fn).Name()
		for _, block := range fn.Blocks {
			for _, ins := range block.Instrs {
				switch ins := ins.(type) {
				case *ssa.Defer:
					callee := ins.Common().StaticCallee()
					if callee == nil || callee.Name() != "finish" || len(ins.Common().Args) != 1 {
						continue
					}
					use := uses[owner]
					use.lease, use.finish = ins.Common().Args[0], ins
					uses[owner] = use
				case *ssa.Call:
					callee := ins.Common().StaticCallee()
					if callee == nil || (callee.Name() != "fillSpill" && callee.Name() != "takeLease") {
						continue
					}
					for _, argument := range ins.Common().Args {
						if argument.Type().String() != "context.Context" {
							continue
						}
						use := uses[owner]
						use.ctx, use.call = argument, ins
						uses[owner] = use
					}
				}
			}
		}
	}
	return uses
}

// spillFlowPath is the inline index path of the lease's context field.
func spillFlowPath(t *testing.T, p *Program) []int {
	t.Helper()
	st := structOf(namedType(t, p, "spillLease"))
	for i := 0; i < st.NumFields(); i++ {
		if st.Field(i).Name() == "flow" {
			return []int{i}
		}
	}
	t.Fatal("spillLease has no flow field")
	return nil
}

// spillFieldWitness is the field answer at the helper's own read: the site a
// census attributes the flow to, not to its own call.
func spillFieldWitness(t *testing.T, p *Program) (witness, why []string) {
	t.Helper()
	for _, fn := range p.Functions {
		if fn.Pkg == nil || fn.Pkg.Pkg.Path() != "census.fixture" || fn.Name() != "takeLease" {
			continue
		}
		for _, block := range fn.Blocks {
			for _, ins := range block.Instrs {
				call, ok := ins.(ssa.CallInstruction)
				if !ok || call.Common().StaticCallee() == nil || call.Common().StaticCallee().Name() != "fillSpill" {
					continue
				}
				for _, argument := range call.Common().Args {
					if argument.Type().String() == "context.Context" {
						return p.MayBeNilWhy(argument, call)
					}
				}
			}
		}
	}
	t.Fatal("takeLease's downstream read not located")
	return nil, nil
}

// spillPlantNames is the deterministic order a census walks its uses in.
func spillPlantNames(uses map[string]spillUse) []string {
	names := make([]string, 0, len(uses))
	for name := range uses {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// TestFieldSpillProjectionFollowsCheckedConstructor is the product outcome: with
// every lease supplied by the constructor's checked success return, the pointer
// helper's read of the lease's context field is non-nil, because the release's
// receiver spill and the restore copy are whole-value stores this call's error
// check covers. The inline projection must follow those stores instead of
// answering zero merely because the copied object's field was never addressed.
func TestFieldSpillProjectionFollowsCheckedConstructor(t *testing.T) {
	p := loadSpillProjection(t, "spill_projection.go", spillProjectionPlants)
	path := spillFlowPath(t, p)
	uses := spillUses(t, p)
	for _, name := range []string{"admittedDirect", "deferredCopy", "copiedLease"} {
		use, found := uses[name]
		if !found || use.lease == nil {
			t.Fatalf("%s: deferred release not located: found=%v", name, found)
		}
		projection := p.ProjectFieldAt(use.lease, path, use.finish)
		t.Logf("%s release witness=%v why=%v", name, projection.Witness, projection.Why)
		if projection.Witness != nil {
			t.Errorf("%s: the deferred release keeps a zero lease: %v", name, projection.Witness)
		}
		if use.ctx == nil {
			continue
		}
		witness, why := p.MayBeNilWhy(use.ctx, use.call)
		t.Logf("%s own read witness=%v why=%v", name, witness, why)
		if witness != nil {
			t.Errorf("%s: the lease's context field may be nil where it is passed on: %v", name, witness)
		}
	}
	witness, why := spillFieldWitness(t, p)
	t.Logf("pointer helper witness=%v why=%v", witness, why)
	if witness != nil {
		t.Errorf("the receiver spill replaced the constructor's checked success with a zero field: %v", witness)
	}
	if !strings.Contains(strings.Join(why, " "), "source-proven non-nil error excluded by same-call success check") {
		t.Errorf("the pointer helper's field was not judged by the constructor's success returns: %v", why)
	}
}

// TestFieldSpillProjectionNegatives keeps every real zero source visible: each
// group carries the same positive control, so a group cannot pass by answering
// zero everywhere, and every plant must keep a possible-nil answer. A plant
// whose copy escapes keeps its release unresolved: no inline projection proves
// the value a write through that reference may reach.
func TestFieldSpillProjectionNegatives(t *testing.T) {
	groups := []struct {
		name   string
		plants string
		// escaped plants park the released object's address, so the release
		// itself must stay possibly zero.
		escaped string
	}{
		{name: "zero_sources", plants: spillZeroPlants},
		{name: "unproven_guards", plants: spillGuardPlants},
		{name: "later_writes", plants: spillWritePlants},
		{name: "unresolved_heap", plants: spillHeapPlants},
		{name: "restore_escape", plants: spillEscapePlants, escaped: "parkedCopy"},
	}
	for _, group := range groups {
		t.Run(group.name, func(t *testing.T) {
			p := loadSpillProjection(t, "spill_"+group.name+".go", group.plants)
			path := spillFlowPath(t, p)
			uses := spillUses(t, p)
			control, found := uses["admittedDirect"]
			if !found || control.ctx == nil {
				t.Fatal("positive control not located")
			}
			if witness, _ := p.MayBeNilWhy(control.ctx, control.call); witness != nil {
				t.Fatalf("positive control answered nil: %v", witness)
			}
			if group.escaped != "" {
				escaped, found := uses[group.escaped]
				if !found || escaped.lease == nil {
					t.Fatalf("%s: deferred release not located", group.escaped)
				}
				projection := p.ProjectFieldAt(escaped.lease, path, escaped.finish)
				t.Logf("%s release witness=%v why=%v", group.escaped, projection.Witness, projection.Why)
				if projection.Witness == nil && !projection.Unknown {
					t.Errorf("%s: an escaped copy's release was proven non-nil", group.escaped)
				}
			}
			for _, name := range spillPlantNames(uses) {
				use := uses[name]
				if name == "admittedDirect" || use.ctx == nil {
					continue
				}
				witness, why := p.MayBeNilWhy(use.ctx, use.call)
				t.Logf("%s read witness=%v why=%v", name, witness, why)
				if witness == nil {
					t.Errorf("%s: the lease's context field was proven non-nil", name)
				}
			}
			if witness, why := spillFieldWitness(t, p); witness == nil {
				t.Errorf("the pointer helper was proven non-nil with a real zero source in the program: %v", why)
			}
		})
	}
}
