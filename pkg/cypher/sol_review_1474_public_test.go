package cypher_test

import (
    "bytes"
    "context"
    "encoding/gob"
    "path/filepath"
    "testing"

    "github.com/dgraph-io/badger/v4"
    "github.com/orneryd/nornicdb/pkg/cypher"
    "github.com/orneryd/nornicdb/pkg/replication"
    "github.com/orneryd/nornicdb/pkg/storage"
    "github.com/stretchr/testify/require"
)

func sol1474PublicNode(id storage.NodeID) *storage.Node {
    n := &storage.Node{ID:id, Labels:[]string{"Doc"}, Properties:map[string]any{"text":"same", "k":int64(1)}, NamedEmbeddings:map[string][]float32{"title":{3, 5, 7}}}
    for i:=0; i<16; i++ {
        v:=make([]float32, 1024)
        v[i]=float32(i+1)
        n.ChunkEmbeddings=append(n.ChunkEmbeddings, v)
    }
    return n
}

func sol1474PublicCount(t *testing.T, dir string, id storage.NodeID) int {
    t.Helper()
    db, err := badger.Open(badger.DefaultOptions(dir).WithLogger(nil))
    require.NoError(t, err)
    defer db.Close()
    count:=0
    p:=append(append([]byte{0x08}, []byte(id)...), 0)
    require.NoError(t, db.View(func(txn *badger.Txn) error {
        opts:=badger.DefaultIteratorOptions
        opts.PrefetchValues=false
        it:=txn.NewIterator(opts)
        defer it.Close()
        for it.Seek(p); it.ValidForPrefix(p); it.Next() { count++ }
        return nil
    }))
    return count
}

func TestSol1474PublicPaths(t *testing.T) {
    for _, mode:=range []string{"SET", "REMOVE", "MERGE", "DELETE", "bulk-delete", "replication-delete", "replication-shrink", "restore-delete", "overlapping-delete"} {
        t.Run(mode, func(t *testing.T) {
            dir:=filepath.Join(t.TempDir(), "data")
            e, err:=storage.NewBadgerEngine(dir)
            require.NoError(t, err)
            defer e.Close()
            ns:=storage.NewNamespacedEngine(e, "test")
            exec:=cypher.NewStorageExecutor(ns)
            if mode=="bulk-delete" {
                require.NoError(t, e.BulkCreateNodes([]*storage.Node{sol1474PublicNode("test:doc")}))
            } else if mode=="overlapping-delete" {
                _, err=e.CreateNode(&storage.Node{ID:"test:doc", Labels:[]string{"Doc"}, Properties:map[string]any{"text":"same", "k":int64(1)}})
                require.NoError(t, err)
            } else {
                _, err=e.CreateNode(sol1474PublicNode("test:doc"))
                require.NoError(t, err)
            }
            run:=func(q string) { _, err:=exec.Execute(context.Background(), q, nil); require.NoError(t, err) }
            switch mode {
            case "SET": run("MATCH (n:Doc) SET n.text = 'changed'")
            case "REMOVE": run("MATCH (n:Doc) REMOVE n.text")
            case "MERGE": run("MERGE (n:Doc {k:1}) ON MATCH SET n.text = 'changed'")
            case "DELETE": run("MATCH (n:Doc) DETACH DELETE n")
            case "bulk-delete": require.NoError(t, e.BulkDeleteNodes([]storage.NodeID{"test:doc"}))
            case "replication-delete", "replication-shrink":
                adapter, err:=replication.NewStorageAdapterWithWAL(e, filepath.Join(t.TempDir(), "wal"))
                require.NoError(t, err)
                defer adapter.Close()
                cmd:=&replication.Command{Type:replication.CmdDeleteNode, Data:[]byte("test:doc")}
                if mode=="replication-shrink" {
                    var data bytes.Buffer
                    require.NoError(t, gob.NewEncoder(&data).Encode(&replication.NodePayload{ID:"test:doc", Labels:[]string{"Doc"}, Properties:map[string]any{"text":"same"}, ChunkEmbeddings:[][]float32{{2, 3}}}))
                    cmd=&replication.Command{Type:replication.CmdUpdateNode, Data:data.Bytes()}
                }
                require.NoError(t, adapter.ApplyCommand(cmd))
            case "restore-delete":
                backup:=filepath.Join(t.TempDir(), "backup")
                require.NoError(t, e.Backup(backup))
                require.NoError(t, e.Restore(backup))
                require.NoError(t, e.DeleteNode("test:doc"))
            case "overlapping-delete":
                source, err:=e.GetNode("test:doc")
                require.NoError(t, err)
                run("BEGIN")
                run("MATCH (n:Doc) DETACH DELETE n")
                source.ChunkEmbeddings=sol1474PublicNode("test:doc").ChunkEmbeddings
                source.EmbedMeta=map[string]any{"chunk_count":16}
                require.NoError(t, e.UpdateNodeEmbeddingSidecar(source))
                run("COMMIT")
            }
            if mode=="SET" || mode=="REMOVE" || mode=="MERGE" {
                node, err:=e.GetNode("test:doc")
                require.NoError(t, err)
                require.Equal(t, map[string][]float32{"title":{3, 5, 7}}, node.NamedEmbeddings)
                require.Empty(t, node.ChunkEmbeddings)
            }
            require.NoError(t, e.Close())
            require.Zero(t, sol1474PublicCount(t, dir, "test:doc"), "obsolete embedding keys survive the public path")
        })
    }
}
