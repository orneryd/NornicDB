package storagecensus

import (
	"github.com/stretchr/testify/require"
	"golang.org/x/tools/go/ssa"
	"strings"
	"testing"
)

func TestErrorPropagationRequiresExactNonescapingReturn(t *testing.T) {
	_, pkg := coverageProgram(t, `package storage
 func source()error{return nil};func release(){};var state int;func body(){state++}
 func direct()error{err:=source();if err!=nil{return err};body();return nil}
 func directStore(p *int)error{err:=source();if err!=nil{*p=1;return err};return nil}
 func directWrites()error{err:=source();if err!=nil{body();return err};body();return nil}
 func deferred()error{defer release();err:=source();if err!=nil{return err};body();return nil}
 func rewritten()(result error){defer func(){result=nil}();err:=source();if err!=nil{return err};body();return nil}
 func swallowed()error{defer release();err:=source();if err!=nil{return nil};body();return nil}
 func writes()error{defer release();err:=source();if err!=nil{body();return err};body();return nil}
 func node()*int{return nil}
 func pointer()*int{value:=node();if value!=nil{return value};return nil}
 var sentinel error
 func filteredDirect()error{err:=source();if err!=nil{if err!=sentinel{return err}};body();return nil}
 func filteredDirectWrites()error{err:=source();if err!=nil{if err!=sentinel{body();return err}};body();return nil}
 func filtered()error{defer release();err:=source();if err!=nil{if err!=sentinel{return err}};body();return nil}
 func filteredNil()error{defer release();err:=source();if err==nil{return nil};if err!=sentinel{return err};body();return nil}
 func unguarded()error{defer release();err:=source();if err!=sentinel{return err};body();return nil}
 func filteredWrites()error{defer release();err:=source();if err!=nil{if err!=sentinel{body();return err}};body();return nil}
 func filteredRewritten()(result error){defer func(){result=nil}();err:=source();if err!=nil{if err!=sentinel{return err}};body();return nil}
 func filteredOtherError()error{defer release();other:=source();err:=source();if other!=nil{if err!=sentinel{return err}};body();return nil}
 func filteredJoin(choice bool)error{defer release();err:=source();if choice{if err==nil{return nil}};if err!=sentinel{return err};body();return nil}
 func filteredSwallowed()error{defer release();err:=source();if err!=nil{if err!=sentinel{return nil}};body();return nil}
 `)
	for _, tc := range []struct {
		name string
		want bool
	}{{"direct", true}, {"directWrites", false}, {"directStore", false}, {"filteredDirect", true}, {"filteredDirectWrites", false}, {"deferred", true}, {"rewritten", false}, {"swallowed", false}, {"writes", false}, {"pointer", false}, {"filtered", true}, {"filteredNil", true}, {"unguarded", false}, {"filteredWrites", false}, {"filteredRewritten", false}, {"filteredSwallowed", false}, {"filteredOtherError", false}, {"filteredJoin", false}} {
		t.Run(tc.name, func(t *testing.T) {
			fn := pkg.Func(tc.name)
			found := false
			for _, block := range fn.Blocks {
				if branch, ok := block.Instrs[len(block.Instrs)-1].(*ssa.If); ok {
					if strings.HasPrefix(tc.name, "filtered") {
						cmp, ok := branch.Cond.(*ssa.BinOp)
						if !ok {
							continue
						}
						if c, ok := cmp.X.(*ssa.Const); ok && c.IsNil() {
							continue
						}
						if c, ok := cmp.Y.(*ssa.Const); ok && c.IsNil() {
							continue
						}
					}
					found = true
					require.Equal(t, tc.want, (&decider{}).propagatesError(branch))
				}
			}
			require.True(t, found)
		})
	}
}
