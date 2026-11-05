package workcensus

import (
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/tools/go/ssa"
)

func TestInlineFieldSuccessfulReturnTuples(t *testing.T) {
	c := genericConfig(t)
	c.IncludeProgram = true
	c.Overlay = map[string][]byte{filepath.Join(c.Dir, "tuple.go"): []byte(`package fixture
import "context"
type lease struct{Flow context.Context}
type failure struct{}
func(*failure)Error()string{return "failure"}
func makeLease(fail bool)(lease,error){if fail{return lease{},new(failure)};return lease{context.Background()},nil}
func zeroSuccess()(lease,error){return lease{},nil}
func omitted(ok bool)(lease,error){if ok{return lease{context.Background()},nil};return lease{},nil}
func callerLease(ctx context.Context)(lease,error){return lease{ctx},nil}
var errorFactory func()error
func unknownError()(lease,error){return lease{},errorFactory()}
var globalError error
func globalFailure()(lease,error){return lease{},globalError}
func erasedFailure()(s lease,err error){defer func(){err=nil}();return lease{},new(failure)}
func consume(s lease)error{return s.Flow.Err()}
func Good()error{s,err:=makeLease(false);if err!=nil{return err};return consume(s)}
func GoodCopied()error{s,err:=makeLease(false);if err!=nil{return err};q:=s;s=lease{};return consume(q)}
func BeforeAfter()error{s,err:=makeLease(false);_=consume(s);if err!=nil{return err};return consume(s)}
func BadBypass(skip bool)error{s,err:=makeLease(false);if skip{goto use};if err!=nil{return err};use:return consume(s)}
var leaseFactory func()(lease,error)
func BadUnknownFactory()error{s,err:=leaseFactory();if err!=nil{return err};return consume(s)}
func BadZero()error{s,err:=zeroSuccess();if err!=nil{return err};return consume(s)}
func BadOmitted(ok bool)error{s,err:=omitted(ok);if err!=nil{return err};return consume(s)}
func BadIgnored()error{s,_:=makeLease(false);return consume(s)}
func BadMismatched()error{s,_:=makeLease(false);_,other:=makeLease(false);if other!=nil{return other};return consume(s)}
func BadOverwrite()error{s,err:=makeLease(false);if err!=nil{return err};s=lease{};return consume(s)}
func BadAlias()error{s,err:=makeLease(false);if err!=nil{return err};alias:=&s;alias.Flow=nil;return consume(s)}
func BadCallerNil()error{s,err:=callerLease(nil);if err!=nil{return err};return consume(s)}
func BadUnknownError()error{s,err:=unknownError();if err!=nil{return err};return consume(s)}
func BadErasedError()error{s,err:=erasedFailure();if err!=nil{return err};return consume(s)}
func BadGlobalError()error{s,err:=globalFailure();if err!=nil{return err};return consume(s)}
func BadErrorOverwrite()error{s,err:=makeLease(false);err=nil;if err!=nil{return err};return consume(s)}
func Use(){_=Good();_=GoodCopied();_=BeforeAfter();_=BadBypass(true);_=BadUnknownFactory();_=BadZero();_=BadOmitted(false);_=BadIgnored();_=BadMismatched();_=BadOverwrite();_=BadAlias();_=BadCallerNil();_=BadUnknownError();_=BadErasedError();_=BadGlobalError();_=BadErrorOverwrite()}
`)}
	inv := mustExtract(t, c)
	got := map[string]FieldProjection{}
	var mixed []*ssa.Call
	for _, fn := range inv.Program.Functions {
		if fn.Pkg == nil || fn.Pkg.Pkg.Path() != "census.fixture" {
			continue
		}
		for _, b := range fn.Blocks {
			for _, ins := range b.Instrs {
				call, ok := ins.(*ssa.Call)
				if !ok || call.Common().StaticCallee() == nil || call.Common().StaticCallee().Name() != "consume" {
					continue
				}
				if fn.Name() == "BeforeAfter" {
					mixed = append(mixed, call)
					continue
				}
				q := inv.Program.ProjectFieldAt(call.Common().Args[0], []int{0}, call)
				got[fn.Name()] = q
				t.Logf("%s witness=%v why=%v", fn.Name(), q.Witness, q.Why)
			}
		}
	}
	for _, name := range []string{"Good", "GoodCopied"} {
		q, found := got[name]
		if !found {
			t.Fatal("missing positive", name)
		}
		if q.Witness != nil {
			t.Errorf("%s retained zero-on-error return: %v", name, q.Witness)
		}
		if !strings.Contains(strings.Join(q.Why, " "), "source-proven non-nil error") {
			t.Errorf("%s lacks source return exclusion reason: %v", name, q.Why)
		}
	}
	for _, name := range []string{"BadZero", "BadOmitted", "BadIgnored", "BadMismatched", "BadOverwrite", "BadAlias", "BadCallerNil", "BadUnknownError", "BadErasedError", "BadGlobalError", "BadErrorOverwrite", "BadBypass", "BadUnknownFactory"} {
		q, found := got[name]
		if !found || q.Witness == nil {
			t.Errorf("lost %s negative: found=%v projection=%+v", name, found, q)
		}
	}
	if len(mixed) != 2 {
		t.Fatalf("expected two uses of same result, got%d", len(mixed))
	}
	if mixed[0].Pos() > mixed[1].Pos() {
		mixed[0], mixed[1] = mixed[1], mixed[0]
	}
	for _, order := range [][]int{{0, 1}, {1, 0}} {
		for _, index := range order {
			call := mixed[index]
			q := inv.Program.ProjectFieldAt(call.Common().Args[0], []int{0}, call)
			if (q.Witness == nil) != (index == 1) {
				t.Errorf("use%d order%v conditional projection leaked: %+v", index, order, q)
			}
		}
	}

}
