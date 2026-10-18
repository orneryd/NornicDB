package storage

import (
    "fmt"
    "testing"

    "github.com/dgraph-io/badger/v4"
    "github.com/stretchr/testify/require"
)

func sol1474Large(id NodeID) *Node {
    n := &Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]any{"text": "same"}}
    for i := 0; i < 16; i++ {
        v := make([]float32, 1024)
        v[i] = float32(i+1)
        n.ChunkEmbeddings = append(n.ChunkEmbeddings, v)
    }
    return n
}

func sol1474Keys(t *testing.T, e *BadgerEngine, id NodeID) int {
    t.Helper()
    count := 0
    require.NoError(t, e.withView(func(txn *badger.Txn) error {
        p := embeddingPrefix(id)
        opts := badger.DefaultIteratorOptions
        opts.PrefetchValues = false
        it := txn.NewIterator(opts)
        defer it.Close()
        for it.Seek(p); it.ValidForPrefix(p); it.Next() { count++ }
        return nil
    }))
    return count
}

func TestSol1474TransactionCleanup(t *testing.T) {
    for _, mode := range []string{"committed-shrink", "create-delete", "create-shrink", "delete-recreate", "rollback-create", "rollback-delete", "committed-delete"} {
        t.Run(mode, func(t *testing.T) {
            e := newTestEngine(t)
            id := NodeID("test:sol1474")
            if mode == "committed-shrink" || mode == "delete-recreate" || mode == "rollback-delete" || mode == "committed-delete" {
                _, err := e.CreateNode(sol1474Large(id))
                require.NoError(t, err)
                require.Equal(t, 16, sol1474Keys(t, e, id))
            }
            tx, err := e.BeginTransaction()
            require.NoError(t, err)
            defer tx.Rollback()
            switch mode {
            case "create-delete", "create-shrink", "rollback-create":
                _, err = tx.CreateNode(sol1474Large(id))
                require.NoError(t, err)
            }
            switch mode {
            case "committed-shrink", "create-shrink":
                n := sol1474Large(id)
                n.ChunkEmbeddings = [][]float32{{2, 3, 5}}
                require.NoError(t, tx.UpdateNode(n))
            case "create-delete", "delete-recreate", "rollback-delete", "committed-delete":
                require.NoError(t, tx.DeleteNode(id))
                if mode == "delete-recreate" {
                    _, err = tx.CreateNode(&Node{ID:id, Labels: []string{"Doc"}, Properties: map[string]any{"text":"replacement"}})
                    require.NoError(t, err)
                }
            }
            if mode == "rollback-create" || mode == "rollback-delete" {
                require.NoError(t, tx.Rollback())
                want := 0
                if mode == "rollback-delete" { want = 16 }
                require.Equal(t, want, sol1474Keys(t, e, id))
                return
            }
            require.NoError(t, tx.Commit())
            require.Zero(t, sol1474Keys(t, e, id), "successful transaction must remove all obsolete physical embedding keys")
        })
    }
}

func TestSol1474DeleteVersusEmbed(t *testing.T) {
    for _, operation := range []string{"delete", "shrink"} {
        t.Run(operation, func(t *testing.T) {
            e := newTestEngine(t)
            id := NodeID("test:sol1474-concurrent")
            _, err := e.CreateNode(&Node{ID:id, Labels:[]string{"Doc"}, Properties:map[string]any{"text":"same"}})
            require.NoError(t, err)
            source, err := e.GetNode(id)
            require.NoError(t, err)
            tx, err := e.BeginTransaction()
            require.NoError(t, err)
            defer tx.Rollback()
            if operation == "delete" {
                require.NoError(t, tx.DeleteNode(id))
            } else {
                require.NoError(t, tx.UpdateNode(&Node{ID:id, Labels:source.Labels, Properties:source.Properties, ChunkEmbeddings:[][]float32{{7, 11}}}))
            }
            // The background embedder finishes while the user's transaction
            // remains open. These operations overlap without scheduler tricks.
            source.ChunkEmbeddings = sol1474Large(id).ChunkEmbeddings
            source.EmbedMeta = map[string]any{"chunk_count":16}
            require.NoError(t, e.UpdateNodeEmbeddingSidecar(source))
            require.Equal(t, 17, sol1474Keys(t, e, id))
            err = tx.Commit()
            if err != nil {
                t.Logf("conflict safely rejected user transaction: %v", err)
                return
            }
            require.Zero(t, sol1474Keys(t, e, id), fmt.Sprintf("%s committed after embed: no separately stored keys may survive", operation))
        })
    }
}
