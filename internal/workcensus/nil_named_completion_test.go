package workcensus

import (
	"golang.org/x/tools/go/ssa"
	"path/filepath"
	"strings"
	"testing"
)

const namedCompletionFixture = `package fixture
import ("context"; "errors")
func sink(ctx context.Context) error { return ctx.Err() }
func phase(ctx context.Context, fail bool) (context.Context,error) { if fail {return nil,errors.New("canceled")}; return ctx,nil }
func owner(ctx context.Context,fail bool) (completion context.Context, err error) {
 completion=ctx
 defer func(){ cause:=recover(); if cause!=nil {panic(cause)} }()
 completion,err=phase(ctx,fail)
 if err!=nil {return completion,err}
 return completion,nil
}
func nestedOwner(ctx context.Context,fail bool) (completion context.Context, err error) {
 completion=ctx
 defer func(){ cause:=recover();func(){defer func(){cleanup:=recover();if cleanup!=nil && cause==nil{cause=cleanup}}()}();if cause!=nil{panic(cause)}}()
 completion,err=phase(ctx,fail);if err!=nil{return completion,err};return completion,nil
}
func nestedSafe() error {ctx,err:=nestedOwner(context.Background(),false);if err!=nil{return err};return sink(ctx)}
func safe() error { ctx,err:=owner(context.Background(),false); if err!=nil{return err}; return sink(ctx) }
func nilOwner() (completion context.Context,err error) { defer func(){_ = recover()}(); return }
func realNil() error {ctx,err:=nilOwner();if err!=nil{return err};return sink(ctx)}
func mutationOwner() (completion context.Context,err error) {completion=context.Background();defer func(){completion=nil}();return}
func deferMutation() error {ctx,err:=mutationOwner();if err!=nil{return err};return sink(ctx)}
func ignoredError() error {ctx,_:=owner(context.Background(),true);return sink(ctx)}
func branchOwner(ok bool) (completion context.Context,err error) {defer func(){_ = recover()}();if ok {completion=context.Background()};return}
func mixedBranch() error {ctx,err:=branchOwner(false);if err!=nil{return err};return sink(ctx)}
func recoveredOwner() (completion context.Context,err error) { defer func(){_ = recover()}();panic("before assignment") }
func recoveredNil() error {ctx,err:=recoveredOwner();if err!=nil{return err};return sink(ctx)}
func overwrittenCauseOwner() (completion context.Context,err error) {defer func(){cause:=recover();cause=nil;if cause!=nil{panic(cause)}}();panic("before assignment")}
func overwrittenCause() error {ctx,err:=overwrittenCauseOwner();if err!=nil{return err};return sink(ctx)}
var mutateCause func(*any)
func escapedCauseOwner() (completion context.Context,err error) {defer func(){cause:=recover();mutateCause(&cause);if cause!=nil{panic(cause)}}();panic("before assignment")}
func escapedCause() error {ctx,err:=escapedCauseOwner();if err!=nil{return err};return sink(ctx)}
func Use(){_ = overwrittenCause();_ = escapedCause();_ = nestedSafe();_ = safe();_ = realNil();_ = deferMutation();_ = ignoredError();_ = mixedBranch();_ = recoveredNil()}
`

func TestNamedCompletionReachingDefinitions(t *testing.T) {
	c := genericConfig(t)
	c.IncludeProgram = true
	c.Overlay = map[string][]byte{filepath.Join(c.Dir, "named_completion.go"): []byte(namedCompletionFixture)}
	inv := mustExtract(t, c)
	got := map[string]string{}
	why := map[string][]string{}
	for _, fn := range inv.Program.Functions {
		if fn.Pkg == nil || fn.Pkg.Pkg.Path() != "census.fixture" {
			continue
		}
		if fn.Name() == "owner" {
			var b strings.Builder
			fn.WriteTo(&b)
			t.Logf("owner SSA\n%s", b.String())
		}
		for _, block := range fn.Blocks {
			for _, ins := range block.Instrs {
				call, ok := ins.(ssa.CallInstruction)
				if !ok || call.Common().StaticCallee() == nil || call.Common().StaticCallee().Name() != "sink" {
					continue
				}
				w, reasons := inv.Program.MayBeNilWhy(call.Common().Args[0], call)
				t.Logf("%s witness=%v proof=%v", fn.Name(), w, reasons)
				why[fn.Name()] = reasons
				if w != nil {
					got[fn.Name()] = strings.Join(w, " ")
				}
			}
		}
	}
	for _, name := range []string{"safe", "nestedSafe"} {
		if !strings.Contains(strings.Join(why[name], " "), "proof: unreachable Recover block") {
			t.Errorf("%s lacks exact recovery proof: %v", name, why[name])
		}
		if w, ok := got[name]; ok {
			t.Errorf("%s dominating named completion incorrectly nil: %s", name, w)
		}
	}
	for _, name := range []string{"realNil", "deferMutation", "ignoredError", "mixedBranch", "recoveredNil", "overwrittenCause", "escapedCause"} {
		if _, ok := got[name]; !ok {
			t.Errorf("lost %s negative control", name)
		}
	}
}
