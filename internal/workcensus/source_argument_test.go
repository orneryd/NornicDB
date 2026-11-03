package workcensus

import (
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/tools/go/ssa"
)

func TestSourceParameterArgumentBindings(t *testing.T) {
	c := genericConfig(t)
	c.IncludeProgram = true
	c.Overlay = map[string][]byte{filepath.Join(c.Dir, "bindings.go"): []byte(`package fixture
import "context"
type host struct{}
func scalar(c context.Context){}
func(h *host) method(c context.Context){}
type api interface{method(context.Context)}
type outer struct{*host}
func plain(c context.Context){scalar(c)}
func static(h *host,c context.Context){h.method(c)}
func interfaced(h api,c context.Context){h.method(c)}
func bound(h *host,c context.Context){f:=h.method;f(c)}
func promoted(o *outer,c context.Context){f:=(*outer).method;f(o,c)}
func generic[T any](value T)T{return value}
func instantiated(c context.Context){_=generic[context.Context](c)}
func Use(){h:=new(host);c:=context.Background();plain(c);static(h,c);interfaced(h,c);bound(h,c);promoted(&outer{h},c);instantiated(c)}
`)}
	inv := mustExtract(t, c)
	p := inv.Program
	found := map[string]bool{}
	incompatibleReceiver := false
	for _, fn := range p.Functions {
		owner := fn
		if fn.Origin() != nil {
			owner = fn.Origin()
		}
		if owner.Pkg == nil || owner.Pkg.Pkg.Path() != "census.fixture" {
			continue
		}
		var param *ssa.Parameter
		switch {
		case fn.Name() == "scalar":
			param = fn.Params[0]
		case fn.Name() == "method" && fn.Signature.Recv() != nil && strings.Contains(fn.Signature.Recv().Type().String(), "*census.fixture.host"):
			param = fn.Params[1]
		case fn.Origin() != nil && fn.Origin().Name() == "generic" && len(fn.TypeArgs()) == 1 && fn.TypeArgs()[0].String() == "context.Context":
			param = fn.Params[0]
		default:
			continue
		}
		index := sourceParameterIndex(param)
		if index < 0 || fn.Params[index] != param {
			t.Fatal("wrong parameter ordinal", fn, index)
		}
		for _, site := range p.SourceCallSites(fn) {
			caller := site.Parent().Name()
			arg, ok := sourceArgumentAt(param, index, site)
			t.Logf("target=%s parameter=%s caller=%s call=%s binding=%v compatible=%v", fn, param, caller, site.Common(), arg, ok)
			if !ok {
				t.Errorf("lost compatible context binding %s <- %s", fn, caller)
				continue
			}
			actual, ok := arg.(*ssa.Parameter)
			if !ok || actual.Name() != "c" || actual.Parent() != site.Parent() {
				t.Errorf("wrong actual context binding %v at %s", arg, site.Common())
			}
			found[caller] = true
			if _, ok := sourceArgumentAt(param, -1, site); ok {
				t.Fatal("negative cached index accepted")
			}
			if _, ok := sourceArgumentAt(param, len(fn.Params), site); ok {
				t.Fatal("out of bounds cached index accepted")
			}
			if len(fn.Params) > 1 && index != 0 {
				if _, ok := sourceArgumentAt(param, 0, site); ok {
					t.Fatal("cached index belonging to another parameter accepted")
				}
			}
			if caller == "bound" || caller == "interfaced" {
				receiver := fn.Params[0]
				if _, ok := sourceArgumentAt(receiver, sourceParameterIndex(receiver), site); ok {
					t.Fatal("omitted source receiver fabricated")
				}
			}
			if caller == "static" {
				receiver := fn.Params[0]
				actual, ok := sourceArgumentAt(receiver, sourceParameterIndex(receiver), site)
				if !ok || actual != site.Common().Args[0] {
					t.Fatal("explicit static receiver lost")
				}
			}
			if caller == "promoted" {
				// This is a real source attribution through the promotion wrapper:
				// its receiver is *outer, not *host. Context still aligns, receiver
				// must be rejected by the existing assignability precondition.
				receiver := fn.Params[0]
				if _, ok := sourceArgumentAt(receiver, sourceParameterIndex(receiver), site); ok {
					t.Fatal("incompatible promoted receiver accepted")
				}
				incompatibleReceiver = true
			}
		}
	}
	for _, name := range []string{"plain", "static", "interfaced", "bound", "promoted", "instantiated"} {
		if !found[name] {
			t.Errorf("missing actual source binding control %s", name)
		}
	}
	if !incompatibleReceiver {
		t.Fatal("missing real incompatible-wrapper binding control")
	}
}
