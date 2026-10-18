package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type rule73AdjacencyProbe struct {
	*storage.MemoryEngine
	maxRead int
}

func(p *rule73AdjacencyProbe) GetOutgoingEdges(id storage.NodeID)([]*storage.Edge,error){
	edges,err:=p.MemoryEngine.GetOutgoingEdges(id);p.maxRead=max(p.maxRead,len(edges));return edges,err
}
func(p *rule73AdjacencyProbe) GetIncomingEdges(id storage.NodeID)([]*storage.Edge,error){
	edges,err:=p.MemoryEngine.GetIncomingEdges(id);p.maxRead=max(p.maxRead,len(edges));return edges,err
}
func(p *rule73AdjacencyProbe) OutgoingEdgeHeaders(id storage.NodeID)([]*storage.Edge,bool,error){
	edges,ok,err:=p.MemoryEngine.OutgoingEdgeHeaders(id);p.maxRead=max(p.maxRead,len(edges));return edges,ok,err
}
func(p *rule73AdjacencyProbe) IncomingEdgeHeaders(id storage.NodeID)([]*storage.Edge,bool,error){
	edges,ok,err:=p.MemoryEngine.IncomingEdgeHeaders(id);p.maxRead=max(p.maxRead,len(edges));return edges,ok,err
}
func(p *rule73AdjacencyProbe) GetAdjacentEdges(id storage.NodeID)([]*storage.Edge,[]*storage.Edge,error){
	out,in,err:=p.MemoryEngine.GetAdjacentEdges(id);p.maxRead=max(p.maxRead,len(out),len(in));return out,in,err
}

func rule73ResourceFixture(t *testing.T)(*StorageExecutor,*rule73AdjacencyProbe){
	store:=&rule73AdjacencyProbe{MemoryEngine:newTestMemoryEngine(t)}
	e:=NewStorageExecutor(storage.NewNamespacedEngine(store,"test"))
	e.cache=nil
	for _,q:=range []string{
		"CREATE (:R73Hub),(:R73Target),(:R73Leaf)",
		"MATCH (a:R73Hub),(x:R73Leaf) UNWIND range(1,20000) AS i CREATE (a)-[:R]->(x)",
		"MATCH (a:R73Hub),(b:R73Target) CREATE (a)-[:R]->(b)",
	}{_,err:=e.Execute(context.Background(),q,nil);require.NoError(t,err)}
	return e,store
}

func rule73ResourceQueries()[]string{
	var queries []string
	for _,p:=range []string{"(a)-[:R]->(b)","(b)<-[:R]-(a)","(a)-[:R]-(b)","(a)-[:R*1..1]->(b)"}{
		for _,expr:=range []string{"exists("+p+")","EXISTS { "+p+" }","COUNT { "+p+" }","size(COLLECT { MATCH "+p+" RETURN 1 AS v })","size(["+p+" | 1])"}{
			queries=append(queries,"MATCH (a:R73Hub),(b:R73Target) RETURN "+expr+" AS found")
		}
	}
	return queries
}

func TestRule73PatternResourceAttack(t *testing.T){
	e,store:=rule73ResourceFixture(t)
	for index,q:=range rule73ResourceQueries(){t.Run(fmt.Sprint(index),func(t *testing.T){
		store.maxRead=0
		r,err:=e.Execute(context.Background(),q,nil)
		var rows interface{}
		if r!=nil{rows=r.Rows}
		t.Logf("R73_RESOURCE query=%q max-adjacency=%d rows=%v error=%v",q,store.maxRead,rows,err)
		if err!=nil{t.Errorf("query failed: %v",err)}
		if store.maxRead>64{t.Errorf("bound result materialized dense adjacency: %d",store.maxRead)}
	})}
}
