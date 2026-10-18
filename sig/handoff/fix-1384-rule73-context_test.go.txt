package cypher

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRule73PatternContextStateAttack(t *testing.T){
	for _,withEdge:=range []bool{true,false}{
		e,ctx:=newUnitExecutor(t)
		create:="CREATE (:Q {id:1}),(:Q {id:2})"
		if withEdge {create="CREATE (:Q {id:1})-[:R]->(:Q {id:2})"}
		_,err:=e.Execute(ctx,create,nil);require.NoError(t,err)
		for _,viewport:=range []bool{false,true}{
			runCtx:=ctx
			if viewport {runCtx=WithTemporalViewport(ctx,AsOfTemporalViewport(time.Now().Add(time.Hour)))}
			for _,expression:=range []string{"exists((a)-[:R]->(b))","EXISTS { (a)-[:R]->(b) }","COUNT { (a)-[:R]->(b) } > 0","size(COLLECT { MATCH (a)-[:R]->(b) RETURN 1 }) > 0","size([(a)-[:R]->(b) | 1]) > 0"}{
				r,err:=e.Execute(runCtx,"MATCH (a:Q {id:1}),(b:Q {id:2}) RETURN "+expression+" AS v",nil)
				var rows interface{};if r!=nil{rows=r.Rows}
				t.Logf("R73_CONTEXT edge=%t viewport=%t expr=%q rows=%v err=%v",withEdge,viewport,expression,rows,err)
				if err!=nil || r==nil || fmt.Sprint(r.Rows)!=fmt.Sprintf("[[%t]]",withEdge){t.Errorf("same live snapshot gave the wrong existence result")}
			}
		}
		cancelled,cancel:=context.WithCancel(ctx);cancel()
		_,err=e.Execute(cancelled,"MATCH (a:Q {id:1}) RETURN EXISTS { (a)-[:R]->() }",nil)
		t.Logf("R73_CONTEXT cancelled error=%v",err)
		if err==nil{t.Error("cancelled statement succeeded")}
	}
}
