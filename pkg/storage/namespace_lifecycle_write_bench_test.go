package storage

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
)

func BenchmarkNamespaceLifecycleWrites(b *testing.B) {
	for _, mode := range []string{"native","wal","transaction","mvcc-append"} {
		b.Run(mode,func(b *testing.B){
			b.Cleanup(config.WithWALEnabled())
			base := NewMemoryEngine()
			var target Engine = base
			if mode == "wal" {
				wal,err := NewWAL(filepath.Join(b.TempDir(),"wal"),&WALConfig{SyncMode:"none"})
				if err != nil { b.Fatal(err) }
				target = NewWALEngine(base,wal)
			}
			defer target.Close()
			node := &Node{ID:"bench_lifetime:node",Labels:[]string{"Item"},Properties:map[string]any{"value":int64(0)}}
			if _,err := target.CreateNode(node);err != nil { b.Fatal(err) }
			// Keep another namespace present without making it part of this write.
			if _,err := target.CreateNode(&Node{ID:"bench_peer:node"});err != nil { b.Fatal(err) }
			b.ReportAllocs()
			b.ResetTimer()
			for i:=0;i<b.N;i++ {
				node.Properties["value"] = int64(i)
				switch mode {
				case "transaction":
					tx,err := base.BeginTransaction()
					if err != nil { b.Fatal(err) }
					if err := tx.SetNamespace("bench_lifetime");err != nil { tx.Rollback();b.Fatal(err) }
					if err := tx.UpdateNode(node);err != nil { tx.Rollback();b.Fatal(err) }
					if err := tx.Commit();err != nil { b.Fatal(err) }
				case "mvcc-append":
					if err := base.AppendNodeVersion(node,MVCCVersion{CommitTimestamp:time.Unix(0,int64(i+1)),CommitSequence:uint64(i+1)});err != nil { b.Fatal(err) }
				default:
					if err := target.UpdateNode(node);err != nil { b.Fatal(err) }
				}
			}
			b.StopTimer()
		})
	}
}
