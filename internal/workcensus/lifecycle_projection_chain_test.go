package workcensus

import (
	"go/types"
	"sort"
	"strings"
	"testing"

	"golang.org/x/tools/go/ssa"
)

// The lifecycle projection chain fixture reproduces, generically, the shape of
// pkg/storage's maintenance admission and the callers it serves:
//
//   - mintLease is beginLifecycleWrite. Every refused admission returns the zero
//     lease with the error that refused it (an admission call's error, or a
//     literal refusal); only the admitted path returns the built lease with a
//     nil error.
//   - mintScopedLease is beginMaintenanceWrite. It admits the inner lifecycle
//     write with "l, err := mintLease(...); if err != nil { return l, err }",
//     keeps that lease in its own local fields, refuses a conflicting scope with
//     the same zero-lease/literal-error guard, and otherwise returns the
//     admitted lease with a nil error.
//   - markLeases is the caller (MarkNewTemporalConstraintsForEngine): inside a
//     closure per unit of work it checks the constructor's error, defers the
//     lease's value-method release, makes an unrelated call, and passes the
//     lease's inline context field to the backfill it admits.
//
// Every use below asks the same question: does the model know the lease's
// context field non-nil at this use, because the only returns that supply the
// lease are the constructor's success returns. A model that answers zero only
// because some other function leaves the field zero, or because the field's
// stores anywhere include a zero composite literal, has not read this call.
//
// Nothing here names a production function, package, field or type.

const lifecycleConstructors = `package fixture

import "context"

type leaseRefusal struct{ code int }

func (r *leaseRefusal) Error() string { return "refused" }

type leaseGate struct{ depth int }

type leaseSeal struct{ n int }

type renewalLease struct {
	flow    context.Context
	owner   *leaseGate
	held    *leaseSeal
	dropped bool
	sealed  bool
}

// finish is the lease's release: a value method, as lifecycleWrite.finish is,
// so deferring it copies the lease and can write nothing through it.
func (l renewalLease) finish() { l.owner.leave() }

func (g *leaseGate) leave() { g.depth-- }

func (g *leaseGate) enter(ctx context.Context) error {
	if ctx == nil {
		return &leaseRefusal{code: 1}
	}
	g.depth++
	return nil
}

func (g *leaseGate) drain(ctx context.Context) error { return ctx.Err() }

func (g *leaseGate) admit(scope string) error {
	if scope == "refuse" {
		return &leaseRefusal{code: 2}
	}
	return nil
}

func (g *leaseGate) hold() *leaseSeal { return &leaseSeal{} }

func (g *leaseGate) drop(prefix string) {}

func (g *leaseGate) scope() []string { return []string{"one"} }

func leaseEmpty(g *leaseGate, label string) (bool, error) { return label == "empty", nil }

func (g *leaseGate) backfillLease(ctx context.Context, label string) error { return fillMore(ctx) }

func fillMore(ctx context.Context) error { return fillIndex(ctx) }

func fillIndex(ctx context.Context) error { return ctx.Err() }

// mintLease is beginLifecycleWrite: every refused admission returns the zero
// lease with the error that refused it; only the admitted path returns the
// built lease, with a nil error.
func mintLease(g *leaseGate, ctx context.Context, dropped string) (renewalLease, error) {
	if err := g.enter(ctx); err != nil {
		return renewalLease{}, err
	}
	if err := g.drain(ctx); err != nil {
		g.leave()
		return renewalLease{}, err
	}
	seal := g.hold()
	if seal == nil {
		g.leave()
		return renewalLease{}, &leaseRefusal{code: 3}
	}
	l := renewalLease{flow: ctx, owner: g, held: seal}
	if dropped != "" {
		g.drop(dropped)
		l.dropped = true
	}
	return l, nil
}

// mintScopedLease is beginMaintenanceWrite: it admits the inner lifecycle write,
// then refuses a conflicting scope, again returning the zero lease and the
// refusal, and otherwise rewrites the admitted lease's own fields.
func mintScopedLease(g *leaseGate, ctx context.Context, scope []string) (renewalLease, error) {
	l, err := mintLease(g, ctx, "")
	if err != nil {
		return l, err
	}
	if len(scope) == 0 {
		if err := g.admit(""); err != nil {
			l.finish()
			return renewalLease{}, err
		}
	} else {
		for _, namespace := range scope {
			if err := g.admit(namespace); err != nil {
				l.finish()
				return renewalLease{}, err
			}
		}
	}
	prefix := ""
	if len(scope) == 1 {
		prefix = scope[0] + ":"
	}
	g.drop(prefix)
	l.dropped = true
	return l, nil
}
`

// lifecycleControl is the positive control every plant group carries: the same
// admitted chain read the same way, so a group cannot pass its plants by
// answering zero everywhere.
const lifecycleControl = `
func outerDirect(g *leaseGate) error {
	life, err := mintScopedLease(g, context.Background(), nil)
	if err != nil {
		return err
	}
	defer life.finish()
	return fillMore(life.flow)
}
`

// lifecycleChainPlants are the shapes the census must resolve: the inner tuple
// alone, the outer constructor alone, and the closure frame the production
// caller uses, with a deferred value-method release and an unrelated call
// between the checked admission and the read.
const lifecycleChainPlants = lifecycleControl + `
func innerOnly(g *leaseGate) error {
	life, err := mintLease(g, context.Background(), "")
	if err != nil {
		return err
	}
	defer life.finish()
	return fillMore(life.flow)
}

func markLeases(g *leaseGate, count *int, labels []string) error {
	for _, label := range labels {
		if err := func() error {
			life, err := mintScopedLease(g, context.Background(), g.scope())
			if err != nil {
				return err
			}
			defer life.finish()
			empty, err := leaseEmpty(g, label)
			if err != nil {
				return err
			}
			if !empty {
				if err := g.backfillLease(life.flow, label); err != nil {
					return err
				}
			}
			*count = *count + 1
			return nil
		}(); err != nil {
			return err
		}
	}
	return nil
}
`

// lifecycleZeroPlants keep a real zero source on the supplying path: a factory
// whose success return is the zero lease or omits the field, an ignored or
// overwritten error, an error the model cannot prove non-nil, and a deferred
// writer that erases it.
const lifecycleZeroPlants = lifecycleControl + `
func zeroScoped(g *leaseGate) (renewalLease, error) { return renewalLease{}, nil }

func omittedScoped(g *leaseGate) (renewalLease, error) { return renewalLease{owner: g}, nil }

func zeroSuccess(g *leaseGate) error {
	life, err := zeroScoped(g)
	if err != nil {
		return err
	}
	defer life.finish()
	return fillMore(life.flow)
}

func omittedField(g *leaseGate) error {
	life, err := omittedScoped(g)
	if err != nil {
		return err
	}
	defer life.finish()
	return fillMore(life.flow)
}

func ignoredError(g *leaseGate) error {
	life, _ := mintScopedLease(g, context.Background(), nil)
	defer life.finish()
	return fillMore(life.flow)
}

var leaseFactory func() (renewalLease, error)

func unknownFactory(g *leaseGate) error {
	life, err := leaseFactory()
	if err != nil {
		return err
	}
	defer life.finish()
	return fillMore(life.flow)
}

var leaseErrorFactory func() error

func opaqueLease(g *leaseGate) (renewalLease, error) { return renewalLease{}, leaseErrorFactory() }

func unknownError(g *leaseGate) error {
	life, err := opaqueLease(g)
	if err != nil {
		return err
	}
	defer life.finish()
	return fillMore(life.flow)
}

var leaseGlobalError error

func globalLease(g *leaseGate) (renewalLease, error) { return renewalLease{}, leaseGlobalError }

func globalError(g *leaseGate) error {
	life, err := globalLease(g)
	if err != nil {
		return err
	}
	defer life.finish()
	return fillMore(life.flow)
}

func erasedLease(g *leaseGate) (l renewalLease, err error) {
	defer func() { err = nil }()
	return renewalLease{}, &leaseRefusal{code: 4}
}

func erasedError(g *leaseGate) error {
	life, err := erasedLease(g)
	if err != nil {
		return err
	}
	defer life.finish()
	return fillMore(life.flow)
}
`

// lifecycleGuardPlants keep the zero return because the success edge of the
// constructor's own error test does not reach the read: the test is bypassed, a
// different call's error is tested, or the error is overwritten before it.
const lifecycleGuardPlants = lifecycleControl + `
func bypassedGuard(g *leaseGate) error {
	life, err := mintScopedLease(g, context.Background(), nil)
	if g.depth > 0 {
		goto release
	}
	if err != nil {
		return err
	}
release:
	defer life.finish()
	return fillMore(life.flow)
}

func mismatchedError(g *leaseGate) error {
	life, _ := mintScopedLease(g, context.Background(), nil)
	if err := g.drain(context.Background()); err != nil {
		return err
	}
	defer life.finish()
	return fillMore(life.flow)
}

func overwrittenError(g *leaseGate) error {
	life, err := mintScopedLease(g, context.Background(), nil)
	err = nil
	if err != nil {
		return err
	}
	defer life.finish()
	return fillMore(life.flow)
}
`

// lifecycleWritePlants keep the zero return because a write reaches the read
// after the admission: through an alias, a capturing closure, the address
// handed to a call, a whole-value overwrite, or a cyclic overwrite.
const lifecycleWritePlants = lifecycleControl + `
func takeLease(g *leaseGate, l *renewalLease) { l.flow = nil }

func aliasedWrite(g *leaseGate) error {
	life, err := mintScopedLease(g, context.Background(), nil)
	if err != nil {
		return err
	}
	defer life.finish()
	alias := &life
	alias.flow = nil
	return fillMore(life.flow)
}

func closureWrite(g *leaseGate) error {
	life, err := mintScopedLease(g, context.Background(), nil)
	if err != nil {
		return err
	}
	defer life.finish()
	hold := func() { life.flow = nil }
	hold()
	return fillMore(life.flow)
}

func escapedWrite(g *leaseGate) error {
	life, err := mintScopedLease(g, context.Background(), nil)
	if err != nil {
		return err
	}
	defer life.finish()
	takeLease(g, &life)
	return fillMore(life.flow)
}

func overwrittenLease(g *leaseGate) error {
	life, err := mintScopedLease(g, context.Background(), nil)
	if err != nil {
		return err
	}
	defer life.finish()
	life = renewalLease{}
	return fillMore(life.flow)
}

func cyclicLease(g *leaseGate, attempts int) error {
	life, err := mintScopedLease(g, context.Background(), nil)
	if err != nil {
		return err
	}
	defer life.finish()
	for i := 0; i < attempts; i++ {
		if i == 1 {
			life = renewalLease{}
		}
	}
	return fillMore(life.flow)
}
`

func loadLifecycleChain(t *testing.T, name, plants string) *Program {
	t.Helper()
	return loadEscapeProgram(t, name, lifecycleConstructors+plants)
}

// lifecycleUse is one plant's read of its lease: the deferred value-method
// release that receives the whole lease, and the unrelated call that receives
// its inline context field as a plain value.
type lifecycleUse struct {
	lease  ssa.Value // the lease value passed to finish
	finish ssa.Instruction
	ctx    ssa.Value // the context value passed to the downstream call
	call   ssa.CallInstruction
}

func lifecycleUses(t *testing.T, p *Program) map[string]lifecycleUse {
	t.Helper()
	uses := map[string]lifecycleUse{}
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
					if callee == nil || (callee.Name() != "fillMore" && callee.Name() != "backfillLease") {
						continue
					}
					// A method call's receiver is its first SSA operand: take the
					// argument the plant passes the lease's context field as.
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

// lifecycleFlowPath is the inline index path of the lease's context field.
func lifecycleFlowPath(t *testing.T, p *Program) []int {
	t.Helper()
	st := structOf(namedType(t, p, "renewalLease"))
	for i := 0; i < st.NumFields(); i++ {
		if st.Field(i).Name() == "flow" {
			return []int{i}
		}
	}
	t.Fatal("renewalLease has no flow field")
	return nil
}

// namedType returns a named type declared in the fixture package.
func namedType(t *testing.T, p *Program, name string) types.Type {
	t.Helper()
	for _, fn := range p.Functions {
		if fn.Pkg == nil || fn.Pkg.Pkg.Path() != "census.fixture" {
			continue
		}
		if object := fn.Pkg.Pkg.Scope().Lookup(name); object != nil {
			return object.Type()
		}
	}
	t.Fatalf("fixture type %s not loaded", name)
	return nil
}

// downstreamWitness is the nil verdict at the deepest use of a context the
// plants passed down through fillMore: the site a census attributes to the
// root read, not to its own call.
func downstreamWitness(p *Program) (witness, why []string) {
	for _, fn := range p.Functions {
		if fn.Pkg == nil || fn.Pkg.Pkg.Path() != "census.fixture" || fn.Name() != "fillIndex" {
			continue
		}
		for _, block := range fn.Blocks {
			for _, ins := range block.Instrs {
				call, ok := ins.(ssa.CallInstruction)
				if !ok || call.Common().Method == nil || call.Common().Method.Name() != "Err" {
					continue
				}
				// An interface method call's receiver is its value operand.
				return p.MayBeNilWhy(call.Common().Value, call)
			}
		}
	}
	return []string{"no downstream use found"}, nil
}

// plantNames is the deterministic order a census walks its uses in.
func plantNames(uses map[string]lifecycleUse) []string {
	names := make([]string, 0, len(uses))
	for name := range uses {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// TestLifecycleProjectionChain asks the positive question at every use: the
// lease's context field is non-nil where the caller uses it, because the only
// returns that supply the lease are the checked constructors' success returns.
func TestLifecycleProjectionChain(t *testing.T) {
	p := loadLifecycleChain(t, "lifecycle_chain.go", lifecycleChainPlants)
	path := lifecycleFlowPath(t, p)
	uses := lifecycleUses(t, p)
	for _, name := range []string{"innerOnly", "outerDirect", "markLeases"} {
		use, found := uses[name]
		if !found || use.lease == nil || use.ctx == nil {
			t.Fatalf("%s: use not located: found=%v", name, found)
		}
		projection := p.ProjectFieldAt(use.lease, path, use.finish)
		if projection.Witness != nil {
			t.Errorf("%s: deferred release keeps a zero lease: %v", name, projection.Witness)
		}
		witness, why := p.MayBeNilWhy(use.ctx, use.call)
		t.Logf("%s field witness=%v why=%v", name, witness, why)
		if witness != nil {
			t.Errorf("%s: the lease's context field may be nil where it is passed on: %v", name, witness)
			continue
		}
		// The verdict must come from this call's checked constructor returns,
		// not from a convention that would answer for any lease.
		if !strings.Contains(strings.Join(why, " "), "source-proven non-nil error excluded by same-call success check") {
			t.Errorf("%s: the field was not judged by the constructor's success returns: %v", name, why)
		}
	}
	witness, why := downstreamWitness(p)
	t.Logf("downstream witness=%v why=%v", witness, why)
	if witness != nil {
		t.Errorf("a nil context reaches the deepest use of a checked lease: %v", witness)
	} else if !strings.Contains(strings.Join(why, " "), "source-proven non-nil error excluded by same-call success check") {
		t.Errorf("the deepest use was not judged by the root read's source: %v", why)
	}
}

// TestLifecycleProjectionChainNegatives keeps every real zero source visible:
// each group carries the same positive control, so a group cannot pass by
// answering zero everywhere.
func TestLifecycleProjectionChainNegatives(t *testing.T) {
	groups := []struct {
		name   string
		plants string
		// fieldOnly plants have no visible callee at all: the field answer must
		// stay unknown, while the value flow may use the external-result
		// convention.
		fieldOnly map[string]bool
		// zeroAtRelease plants take their zero lease from the constructor's
		// refused returns, so the deferred release receives it too. A plant
		// whose write only reaches the read after registration releases the
		// admitted lease: the defer argument is evaluated where it stands.
		zeroAtRelease bool
	}{
		{name: "zero_sources", plants: lifecycleZeroPlants, fieldOnly: map[string]bool{"unknownFactory": true}, zeroAtRelease: true},
		{name: "unproven_guards", plants: lifecycleGuardPlants, zeroAtRelease: true},
		{name: "later_writes", plants: lifecycleWritePlants},
	}
	for _, group := range groups {
		t.Run(group.name, func(t *testing.T) {
			p := loadLifecycleChain(t, "lifecycle_"+group.name+".go", group.plants)
			path := lifecycleFlowPath(t, p)
			uses := lifecycleUses(t, p)
			control, found := uses["outerDirect"]
			if !found {
				t.Fatal("positive control not located")
			}
			if witness := p.ProjectFieldAt(control.lease, path, control.finish).Witness; witness != nil {
				t.Fatalf("positive control answered zero: %v", witness)
			}
			if witness, _ := p.MayBeNilWhy(control.ctx, control.call); witness != nil {
				t.Fatalf("positive control's context field answered nil: %v", witness)
			}
			for _, name := range plantNames(uses) {
				use := uses[name]
				if name == "outerDirect" || use.lease == nil {
					continue
				}
				projection := p.ProjectFieldAt(use.lease, path, use.finish)
				if group.zeroAtRelease {
					if projection.Witness == nil {
						t.Errorf("%s: lost the zero lease on the deferred release", name)
						continue
					}
				} else {
					t.Logf("%s release witness=%v why=%v", name, projection.Witness, projection.Why)
					if projection.Witness != nil {
						t.Errorf("%s: a write after registration changed the released lease: %v", name, projection.Witness)
					}
				}
				if group.fieldOnly[name] {
					if !projection.Unknown {
						t.Errorf("%s: unresolved factory answered a decided field: %v", name, projection.Why)
					}
					continue
				}
				if use.ctx == nil {
					t.Errorf("%s: no downstream use located", name)
					continue
				}
				witness, why := p.MayBeNilWhy(use.ctx, use.call)
				t.Logf("%s field witness=%v why=%v", name, witness, why)
				if witness == nil {
					t.Errorf("%s: the lease's context field was proven non-nil", name)
				}
			}
		})
	}
}
