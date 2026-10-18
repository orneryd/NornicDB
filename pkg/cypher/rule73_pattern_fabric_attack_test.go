package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// Native routing control: inner expressions use the same Q fixture and
// Neo4j-established results as the differential matrix. This is not a claim
// that Neo4j's composite database protocol was exercised by that matrix.
func TestRule73PatternFabricStateAttack(t *testing.T){
	e:=newCompositeFixture(t)
	ctx:=context.Background()
	_,err:=e.Execute(ctx,"USE pcomp.other CREATE (:Q {id:1})-[:R {w:1}]->(:Q {id:2})-[:R {w:2}]->(:Q {id:3})",nil)
	require.NoError(t,err)
	for _,horizon:=range []string{"","WITH *","WITH 1 AS local"}{
		for _,expr:=range []struct{value,want string}{
			{"exists((a)-[:R]->(b))","[[1 true] [2 true] [3 false]]"},
			{"EXISTS { "+horizon+" MATCH (b:Q) WHERE (a)-[:R]->(b) }","[[1 true] [2 true] [3 false]]"},
			{"COUNT { "+horizon+" MATCH (b:Q) WHERE (a)-[:R]->(b) }","[[1 1] [2 1] [3 0]]"},
			{"size(COLLECT { "+horizon+" MATCH (b:Q) WHERE (a)-[:R]->(b) RETURN b.id })","[[1 1] [2 1] [3 0]]"},
			{"size([(a)-[:R]->(b) | 1])","[[1 1] [2 1] [3 0]]"},
		}{
			for _,route:=range []string{"use","call-use"}{
				body:="MATCH (a:Q) OPTIONAL MATCH (a)-[:R]->(b:Q) WITH a,b,a.id AS k RETURN k AS id,"+expr.value+" AS v ORDER BY id"
				query:="USE pcomp.other "+body
				if route=="call-use" {query="CALL { USE pcomp.other "+body+" } RETURN id,v ORDER BY id"}
				r,err:=e.Execute(ctx,query,nil);var rows interface{};if r!=nil {rows=r.Rows}
				t.Logf("R73_FABRIC route=%s horizon=%q expression=%q rows=%v err=%v want=%s",route,horizon,expr.value,rows,err,expr.want)
				if err!=nil || fmt.Sprint(rows)!=expr.want {t.Errorf("routed inner graph-expression result differs")}
			}
		}
	}
}
