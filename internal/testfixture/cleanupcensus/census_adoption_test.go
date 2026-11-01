// Author: L4 implementation lane (Codex), 1 November 2026.
// Canonical adoption graph facts, shared by identity and source reconciliation.
package cleanupcensus_test

import (
	"fmt"
	"go/ast"
	"strings"

	"golang.org/x/tools/go/ssa"
)

func resolvedCallees(site ssa.CallInstruction, graph map[ssa.CallInstruction][]*ssa.Function) []*ssa.Function {
	if targets := graph[site]; len(targets) > 0 {
		return targets
	}
	if target := site.Common().StaticCallee(); target != nil {
		return []*ssa.Function{target}
	}
	return nil
}

func ownershipArguments(site ssa.CallInstruction) []ssa.Value {
	args := site.Common().Args
	if site.Common().IsInvoke() {
		return append([]ssa.Value{site.Common().Value}, args...)
	}
	return args
}

// adoptionEffects reports a conservative function-level effect, not lifetime
// identity. It prevents a local constructor-only source proof from accepting a
// function whose helpers or materialized callbacks can reach the owner. Only
// module edges are traversed: traversing testing.T.Run would merge every test.
// Unresolved dynamic calls produce unknown, never evidence of no adoption.
func (a *analysis) adoptionEffects(c *census, functions []*ssa.Function, graph map[ssa.CallInstruction][]*ssa.Function) {
	parents := map[*ssa.Function]map[*ssa.Function]bool{}
	adopts, unknown := map[*ssa.Function]bool{}, map[*ssa.Function]bool{}
	module := func(fn *ssa.Function) bool {
		return fn != nil && strings.HasPrefix(functionPackage(fn), modulePath)
	}
	for _, fn := range functions {
		if !module(fn) {
			continue
		}
		if functionPackage(fn) == fixturePath && baseName(fn) == "ownedClose" {
			adopts[fn] = true
		}
		link := func(target *ssa.Function) {
			if module(target) {
				if parents[target] == nil {
					parents[target] = map[*ssa.Function]bool{}
				}
				parents[target][fn] = true
			}
		}
		for _, child := range fn.AnonFuncs {
			link(child)
		}
		for _, block := range fn.Blocks {
			for _, instr := range block.Instrs {
				if site, ok := instr.(ssa.CallInstruction); ok {
					targets := resolvedCallees(site, graph)
					if _, builtin := site.Common().Value.(*ssa.Builtin); !builtin && len(targets) == 0 {
						unknown[fn] = true
					}
					for _, target := range targets {
						link(target)
					}
				}
				// A callback passed to an external scheduler, or returned without
				// being called here, still belongs to this source function.
				for _, operand := range instr.Operands(nil) {
					if operand != nil {
						if target, ok := (*operand).(*ssa.Function); ok {
							link(target)
						}
					}
				}
			}
		}
	}
	propagate := func(effects map[*ssa.Function]bool) {
		var pending []*ssa.Function
		for fn := range effects {
			pending = append(pending, fn)
		}
		for len(pending) > 0 {
			fn := pending[len(pending)-1]
			pending = pending[:len(pending)-1]
			for parent := range parents[fn] {
				if !effects[parent] {
					effects[parent] = true
					pending = append(pending, parent)
				}
			}
		}
	}
	propagate(adopts)
	propagate(unknown)
	// Multiple build variants share a source declaration. OR their effects;
	// an explicit false requires all available variants to be resolved.
	type effect struct{ adopts, unknown bool }
	bySource := map[string]effect{}
	for _, fn := range functions {
		if !module(fn) {
			continue
		}
		decl, ok := fn.Syntax().(*ast.FuncDecl)
		if !ok {
			continue
		}
		pos := a.fset.Position(decl.Pos())
		key := fmt.Sprintf("%s:%d", relative(a.base, pos.Filename), pos.Line)
		prior := bySource[key]
		bySource[key] = effect{prior.adopts || adopts[fn], prior.unknown || unknown[fn]}
	}
	for key, effect := range bySource {
		value := "false"
		if effect.adopts {
			value = "true"
		} else if effect.unknown {
			value = "unknown"
		}
		c.adoptionEffectFacts = append(c.adoptionEffectFacts, key+" | may adopt "+value)
	}
	c.adoptionEffectFacts = unique(c.adoptionEffectFacts)
}

// The same concrete type is used on adopted and Track-only paths so a
// context-insensitive adoption of track's parameter cannot pass this control.
const plantedIntrinsicAdoption = `package replication

import (
    "errors"
    "testing"
    fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
)

type zzIntrinsicResource struct{}
func (*zzIntrinsicResource) Close() error { return nil }
func zzIntrinsicConstruct() (*zzIntrinsicResource, error) { return &zzIntrinsicResource{}, nil }
func zzIntrinsicConstruct3() (*zzIntrinsicResource, int, error) { return &zzIntrinsicResource{}, 1, nil }
func zzIntrinsicResults(t *testing.T) {
    result, _ := fixtureowner.OwnResult[*zzIntrinsicResource](t, errors.New("closed"))(zzIntrinsicConstruct())
    _ = result.Close() // intrinsic-result
    result3, _, _ := fixtureowner.OwnResult3[*zzIntrinsicResource, int](t, errors.New("closed"))(zzIntrinsicConstruct3())
    _ = result3.Close() // intrinsic-result3
}
func zzIntrinsicAdopt(t *testing.T, resource *zzIntrinsicResource) {
    fixtureowner.OwnResult[*zzIntrinsicResource](t, errors.New("closed"))(resource, nil)
}
func zzIntrinsicFactory(t *testing.T) func(*zzIntrinsicResource) {
    return func(resource *zzIntrinsicResource) { zzIntrinsicAdopt(t, resource) }
}
func zzIntrinsicIndirect(t *testing.T) {
    curried := &zzIntrinsicResource{}
    adopt := zzIntrinsicFactory(t)
    adopt(curried)
    _ = curried.Close() // intrinsic-curried
}
func zzIntrinsicFunction(t *testing.T) {
    release := fixtureowner.CloseFunc(func() error { return nil })
    fixtureowner.Own(t, release)
    _ = release.Close() // intrinsic-function
}
func zzIntrinsicBefore(t *testing.T, last, first *zzIntrinsicResource) {
    fixtureowner.Own(t, last, first)
}
func zzIntrinsicDependencies(t *testing.T) {
    last, first := &zzIntrinsicResource{}, &zzIntrinsicResource{}
    zzIntrinsicBefore(t, last, first)
    _ = first.Close() // intrinsic-before
}
func zzIntrinsicRelease(resource *zzIntrinsicResource) { _ = resource.Close() }
func zzIntrinsicForwardOnly(t *testing.T) {
    owned := fixtureowner.Own(t, &zzIntrinsicResource{})
    zzIntrinsicRelease(owned)
    other := &zzIntrinsicResource{}
    zzIntrinsicRelease(other)
    _ = other.Close() // intrinsic-unowned-negative
}
type zzIntrinsicAggregate struct { resource *zzIntrinsicResource }
func (a *zzIntrinsicAggregate) Close() error { return a.resource.Close() }
func zzIntrinsicReturnAggregate(t *testing.T) *zzIntrinsicAggregate {
    inside := fixtureowner.Own(t, &zzIntrinsicResource{})
    return &zzIntrinsicAggregate{resource: inside}
}
func zzIntrinsicReturnedAggregate(t *testing.T) {
    aggregate := zzIntrinsicReturnAggregate(t)
    _ = aggregate.Close() // intrinsic-returned-aggregate
    _ = aggregate.resource.Close() // intrinsic-returned-field
}
func zzIntrinsicTrackOnly(t *testing.T) {
    tracked, _ := fixtureowner.TrackResult[*zzIntrinsicResource](t, errors.New("closed"))(zzIntrinsicConstruct())
    _ = tracked.Close() // intrinsic-track-negative
}
`
