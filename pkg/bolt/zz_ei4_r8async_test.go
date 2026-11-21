package bolt
import("context";"fmt";"strings";"testing";"github.com/orneryd/nornicdb/pkg/storage";"github.com/stretchr/testify/require")
func TestEI4R8AsyncLargeVectors(t *testing.T){for _,d:=range []bool{false,true}{t.Run(fmt.Sprintf("drained=%v",d),func(t *testing.T){ei4r8AsyncVectors(t,1800,d)})}}
func TestEI4R8AsyncSmallVectorsControl(t *testing.T){for _,d:=range []bool{false,true}{t.Run(fmt.Sprintf("drained=%v",d),func(t *testing.T){ei4r8AsyncVectors(t,8,d)})}}
func ei4r8AsyncVectors(t *testing.T,chunks int,drain bool){
 for _,mode:=range []struct{strict,batch bool}{{false,false},{true,false},{false,true}}{
  for _,explicit:=range journalForms(){
   t.Run(fmt.Sprintf("strict=%v/batch=%v/explicit=%v",mode.strict,mode.batch,explicit),func(t *testing.T){
    c:=journalConfig();c.Database.AsyncWritesEnabled=true;c.Database.StrictDurability=mode.strict;if mode.batch{c.Database.WALSyncMode="batch"}
    l:=journalOpen(t,t.TempDir(),c)
    l.mustRun("CREATE (:Large {id:'large', text:$text, remove_me:1})",map[string]any{"text":strings.Repeat("x",1300000)})
    if drain{require.NoError(t,l.db.GetBaseStorageForManager().(*storage.WALEngine).GetInnerEngine().(*storage.BadgerEngine).FlushWriteBehind(context.Background()))};l.embed("Large",chunks)
    for _,q:=range []string{"MATCH (n:Large) SET n.hidden=true","MATCH (n:Large) SET n:Archived","MATCH (n:Large) REMOVE n.remove_me"}{
     _,err:=l.run(q,nil,explicit);require.NoError(t,err,q)
     ns:=l.nodesByLabel("Large");require.Len(t,ns,1)
     require.Equal(t,journalVectorPrint(journalVectors(chunks,0)),journalVectorPrint(ns[0].ChunkEmbeddings),q)
     reader:=l.db.GetBaseStorageForManager().(*storage.WALEngine).GetInnerEngine().(interface{GetNodeEmbeddingMeta(context.Context,storage.NodeID)(map[string]any,error)})
     meta,err:=reader.GetNodeEmbeddingMeta(context.Background(),storage.NodeID("nornic:"+string(ns[0].ID)));require.NoError(t,err)
     t.Logf("%s: chunks=%d vectors preserved, live has_embedding=%v",q,chunks,meta["has_embedding"])
    }
    l=l.reopen(c);l.requireVectors("Large",1,chunks)
    require.Equal(t,int64(1),l.count("MATCH (n:Large:Archived) WHERE n.hidden AND n.remove_me IS NULL RETURN count(n)",nil))
   })
  }
 }
}
