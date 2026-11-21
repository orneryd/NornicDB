package storage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

func TestEI4Ind1790RejectOutOfRangeRetention(t *testing.T) {
	payload, err := EncodeEntityTransaction(EntityTransactionRequest{Namespace:"test",LogicalID:"malformed",Operations:[]Operation{{Type:OpUpdateNode,Node:&Node{ID:"test:n"}}}})
	require.NoError(t,err)
	for _, mark := range []int64{4,8,256,257,-255,4294967297} {
		t.Run(fmt.Sprint(mark),func(t *testing.T){
			var wire map[string]msgpack.RawMessage
			require.NoError(t,msgpack.Unmarshal(payload,&wire))
			var changes []map[string]msgpack.RawMessage
			require.NoError(t,msgpack.Unmarshal(wire["Changes"],&changes))
			var node map[string]msgpack.RawMessage
			require.NoError(t,msgpack.Unmarshal(changes[0]["Node"],&node))
			node["RetainedVectors"],err=msgpack.Marshal(mark)
			require.NoError(t,err)
			changes[0]["Node"],err=msgpack.Marshal(node)
			require.NoError(t,err)
			wire["Changes"],err=msgpack.Marshal(changes)
			require.NoError(t,err)
			wire["Version"],err=msgpack.Marshal(uint16(4))
			require.NoError(t,err)
			malformed,err:=msgpack.Marshal(wire)
			require.NoError(t,err)
			_,ops,err:=DecodeEntityTransaction(malformed)
			t.Logf("mark=%d decoded=%+v error=%v",mark,ops,err)
			if err==nil && mark==256 {
				b:=ei4ind1790Engine(t,false)
				_,createErr:=b.CreateNode(t.Context(),&Node{ID:"test:n",ChunkEmbeddings:[][]float32{{99,2,3}},EmbedMeta:map[string]any{"has_embedding":true}})
				require.NoError(t,createErr)
				applyErr:=ApplyAcceptedEntityTransaction(b,malformed)
				require.NoError(t,applyErr)
				got,readErr:=b.GetNode(t.Context(),"test:n")
				require.NoError(t,readErr)
				t.Logf("MALFORMED-APPLY mark=256 applyError=%v oldVectors=[[99 2 3]] newVectors=%v newMeta=%v",applyErr,got.ChunkEmbeddings,got.EmbedMeta)
			}
			require.Error(t,err,"out-of-range retention mark must be refused")
		})
	}
}
