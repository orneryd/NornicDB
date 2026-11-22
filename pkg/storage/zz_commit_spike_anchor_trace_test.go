package storage

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/vmihailenco/msgpack/v5"
)

// TestCommitSpikeAnchorTrace isolates the journal decision made for the same
// unchanged-vector update shape as TestCommitSpike: one update per node versus
// three updates to each node in the same explicit transaction.
func TestCommitSpikeAnchorTrace(t *testing.T) {
	ctx := context.Background()
	base, err := NewBadgerEngineInMemory()
	if err != nil { t.Fatal(err) }
	t.Cleanup(func(){ _ = base.Close() })
	undoWAL := config.WithWALEnabled()
	t.Cleanup(undoWAL)
	wal, err := NewWAL(filepath.Join(t.TempDir(), "trace-wal"), DefaultWALConfig())
	if err != nil { t.Fatal(err) }
	t.Cleanup(func(){ _ = wal.Close() })
	for i:=0; i<2; i++ {
		id := NodeID(fmt.Sprintf("nornic:node-%02d", i))
		_, err = base.CreateNode(ctx, &Node{ID:id, Labels:[]string{"Source"}, Properties:map[string]any{"revision":int64(0)}, ChunkEmbeddings:[][]float32{make([]float32,2048)}})
		if err != nil { t.Fatal(err) }
	}

	for _, scenario := range []struct{name string; repeats int}{{"one_update_per_node",1},{"three_updates_per_node",3}} {
		t.Run(scenario.name, func(t *testing.T) {
			tx, err := base.BeginTransaction(ctx, OrdinaryBegin)
			if err != nil { t.Fatal(err) }
			t.Cleanup(func(){ if tx.Status == TxStatusActive { _ = tx.Rollback() } })
			for nodeIndex:=0; nodeIndex<2; nodeIndex++ {
				id:=NodeID(fmt.Sprintf("nornic:node-%02d", nodeIndex))
				for update:=0; update<scenario.repeats; update++ {
					before, err:=tx.GetNode(ctx,id)
					if err != nil { t.Fatal(err) }
					after:=copyNode(before)
					after.Properties["revision"]=int64(update+1)
					if err:=tx.UpdateGraphNode(ctx,NodeUpdateRequest{After:after,Policy:StoredNodeImages}); err!=nil { t.Fatal(err) }
				}
			}
			ops:=append([]Operation(nil),tx.operations...)
			references,err:=wal.managedReferences(ctx,ops,func()error{return nil})
			if err!=nil { t.Fatal(err) }
			refIndex:=map[uint32]bool{}
			for _,r:=range references.entries { refIndex[r.Change]=true }
			for i,op:=range ops {
				journaled:="full_image"
				if refIndex[uint32(i)] { journaled="reference" }
				t.Logf("CSANCHOR scenario=%s op=%d node=%s anchor_eligible=%v journaled=%s",scenario.name,i,op.Node.ID,op.nodeAnchorEligible,journaled)
			}
			payload,err:=EncodeEntityTransaction(EntityTransactionRequest{Namespace:"nornic",LogicalID:"commit-spike-trace-"+scenario.name,Operations:ops,Mode:CommitModeDirect,VectorReferences:references})
			if err!=nil { t.Fatal(err) }
			record,err:=msgpack.Marshal(&walEntityCommitData{Payload:payload})
			if err!=nil { t.Fatal(err) }
			t.Logf("CSANCHOR_RECORD scenario=%s operations=%d references=%d encoded_record_bytes=%d payload_bytes=%d",scenario.name,len(ops),len(references.entries),len(record),len(payload))
		})
	}
}
