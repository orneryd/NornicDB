package workcensus

import (
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/tools/go/ssa"
)

func TestReviewedNonNilResultContractBoundaries(t *testing.T) {
	c := genericConfig(t)
	c.Overlay = map[string][]byte{filepath.Join(c.Dir, "provider.go"): []byte(`package fixture
import("context";"sync")
type Provider struct{once sync.Once;ctx context.Context}
func NewProvider()*Provider{return &Provider{}}
func(p *Provider) init(){p.once.Do(func(){p.ctx=context.Background()})}
func(p *Provider) Context()context.Context{p.init();return p.ctx}
func sink(ctx context.Context)error{return ctx.Err()}
func Direct(p *Provider)error{return sink(p.Context())}
func RawField(p *Provider)error{return sink(p.ctx)}
func Nullable(p *Provider,accepted bool)context.Context{if !accepted{return nil};return p.Context()}
func Optional(p *Provider,accepted bool)error{return sink(Nullable(p,accepted))}
func Pair(p *Provider)(context.Context,context.Context){return p.Context(),nil}
func WrongSlot(p *Provider)error{_,second:=Pair(p);return sink(second)}
type Source interface{Context()context.Context}
type Broken struct{}
func(Broken)Context()context.Context{return nil}
func Mixed(source Source)error{return sink(source.Context())}
func Use(p *Provider){_=Mixed(p);_=Mixed(Broken{})}
`)}
	program, err := LoadProgram(c)
	if err != nil {
		t.Fatal(err)
	}
	var provider, pair *ssa.Function
	type use struct {
		value ssa.Value
		call  ssa.CallInstruction
	}
	uses := map[string]use{}
	for _, fn := range program.Functions {
		if fn.String() == "(*census.fixture.Provider).Context" {
			provider = fn
		}
		if fn.Name() == "Pair" {
			pair = fn
		}
		for _, block := range fn.Blocks {
			for _, instr := range block.Instrs {
				call, ok := instr.(ssa.CallInstruction)
				if !ok {
					continue
				}
				target := call.Common().StaticCallee()
				if target != nil && target.Name() == "sink" {
					uses[fn.Name()] = use{call.Common().Args[0], call}
				}
			}
		}
	}
	if provider == nil || pair == nil {
		t.Fatal("fixture provider identities absent")
	}
	if witness := program.MayBeNil(uses["Direct"].value, uses["Direct"].call); witness == nil {
		t.Fatal("control must reproduce the lazy-provider field overapproximation before registering a review")
	}
	contract := NonNilResultContract{Function: provider, Index: 0, Evidence: "reviewed lazy init; fixture source held fixed by this test"}
	if err := program.SetNonNilResultContracts([]NonNilResultContract{contract, {Function: pair, Index: 0, Evidence: "reviewed first slot only"}}); err != nil {
		t.Fatal(err)
	}
	witness, why := program.MayBeNilWhy(uses["Direct"].value, uses["Direct"].call)
	if witness != nil || !strings.Contains(strings.Join(why, "\n"), "reviewed-result-contract:") {
		t.Fatalf("exact provider should carry its reviewed reason, witness=%v why=%v", witness, why)
	}
	for _, name := range []string{"RawField", "Optional", "WrongSlot", "Mixed"} {
		u, ok := uses[name]
		if !ok {
			t.Fatalf("missing control %s", name)
		}
		if witness := program.MayBeNil(u.value, u.call); witness == nil {
			t.Errorf("%s was incorrectly cleared by another callee/result contract", name)
		}
	}
	for _, bad := range []NonNilResultContract{{Function: provider, Index: 1, Evidence: "wrong slot"}, {Function: provider, Index: 0}, {Function: &ssa.Function{}, Index: 0, Evidence: "foreign identity"}} {
		if err := program.SetNonNilResultContracts([]NonNilResultContract{bad}); err == nil {
			t.Error("invalid contract was accepted")
		}
	}
	if witness := program.MayBeNil(uses["Direct"].value, uses["Direct"].call); witness != nil {
		t.Fatal("invalid replacement must preserve the last fully validated contract set")
	}
	if err := program.SetNonNilResultContracts(nil); err != nil {
		t.Fatal(err)
	}
	if witness := program.MayBeNil(uses["Direct"].value, uses["Direct"].call); witness == nil {
		t.Fatal("removing the review must reset memoized verdicts")
	}
}
