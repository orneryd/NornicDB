package storage

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCompositeSourceAdmissionPrecedesEveryMutation(t *testing.T) {
	denied:=errors.New("physical writer admission refused")
	for name, mutate:=range map[string]func(*CompositeEngine,*Node,*Edge)error{
		"create node":func(view *CompositeEngine,_ *Node,_ *Edge)error{_,err:=view.CreateNode(&Node{ID:"fresh",Database:"two"});return err},
		"update node":func(view *CompositeEngine,node *Node,_ *Edge)error{node.Properties["changed"]=true;return view.UpdateNode(node)},
		"delete node":func(view *CompositeEngine,_ *Node,_ *Edge)error{return view.DeleteNode("4:two:same")},
		"create relationship":func(view *CompositeEngine,_ *Node,_ *Edge)error{return view.CreateEdge(&Edge{ID:"fresh",Database:"two",StartNode:"same",EndNode:"target",Type:"R"})},
		"update relationship":func(view *CompositeEngine,_ *Node,edge *Edge)error{edge.Properties["changed"]=true;return view.UpdateEdge(edge)},
		"delete relationship":func(view *CompositeEngine,_ *Node,_ *Edge)error{return view.DeleteEdge("5:two:rel")},
		"bulk create nodes":func(view *CompositeEngine,_ *Node,_ *Edge)error{return view.BulkCreateNodes([]*Node{{ID:"fresh",Database:"two"}})},
		"bulk create relationships":func(view *CompositeEngine,_ *Node,_ *Edge)error{return view.BulkCreateEdges([]*Edge{{ID:"fresh",Database:"two",StartNode:"same",EndNode:"target",Type:"R"}})},
		"bulk delete nodes":func(view *CompositeEngine,_ *Node,_ *Edge)error{return view.BulkDeleteNodes([]NodeID{"4:two:same"})},
		"bulk delete relationships":func(view *CompositeEngine,_ *Node,_ *Edge)error{return view.BulkDeleteEdges([]EdgeID{"5:two:rel"})},
	}{
		t.Run(name,func(t *testing.T){
			sources:=map[string]Engine{}
			for _,name:=range []string{"one","two"}{
				base:=NewMemoryEngine()
				t.Cleanup(func(){require.NoError(t,base.Close())})
				store:=NewNamespacedEngine(base,name)
				for _,id:=range []NodeID{"same","target"}{_,err:=store.CreateNode(&Node{ID:id,Labels:[]string{"N"},Properties:map[string]interface{}{}});require.NoError(t,err)}
				require.NoError(t,store.CreateEdge(&Edge{ID:"rel",StartNode:"same",EndNode:"target",Type:"R",Properties:map[string]interface{}{}}))
				sources[name]=store
			}
			composite:=NewCompositeEngine(sources,map[string]string{"one":"one","two":"two"},map[string]string{"one":"read_write","two":"read_write"})
			var reads,writes []string
			view:=composite.WithConstituentScope(func(alias string,source Engine,write bool)(Engine,error){
				if write {writes=append(writes,alias);return nil,denied}
				reads=append(reads,alias);return source,nil
			})
			node,err:=view.GetNode("4:two:same")
			require.NoError(t,err)
			edge,err:=view.GetEdge("5:two:rel")
			require.NoError(t,err)
			require.Equal(t,[]string{"two","two"},reads,"native source selection must not open other participants")
			require.ErrorIs(t,mutate(view,node,edge),denied)
			require.Equal(t,[]string{"two"},writes)
			for _,store:=range sources{
				nodes,err:=store.NodeCount();require.NoError(t,err);require.EqualValues(t,2,nodes)
				edges,err:=store.EdgeCount();require.NoError(t,err);require.EqualValues(t,1,edges)
				node,err:=store.GetNode("same");require.NoError(t,err);require.NotContains(t,node.Properties,"changed")
				edge,err:=store.GetEdge("rel");require.NoError(t,err);require.NotContains(t,edge.Properties,"changed")
			}
			require.NoError(t,view.Close(),"a borrowed view does not close its sources")
			_,err=sources["two"].GetNode("same");require.NoError(t,err)
		})
	}
}
