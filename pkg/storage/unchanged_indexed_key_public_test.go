package storage_test

import (
	"context"
	"fmt"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
	"time"
)

// Author: PR #1670 implementation lane. No maintenance hooks or product changes.
// Concurrent public MATCH readers must never lose a node whose key stays fixed.
func TestUnchangedIndexedKeyConcurrentPublic(t *testing.T) {
	for _, kind := range indexKinds[:2] {
		t.Run(kind.name, func(t *testing.T) {
			env := openIndexEnv(t)
			admin := cypher.NewStorageExecutor(env.store)
			mustRunIndex(t, admin, kind.ddl, nil)
			mustRunIndex(t, admin, "CREATE (:Doc {id:'stable', c:0})", nil)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer cancel()
			start := make(chan struct{})
			failures := make(chan string, 9)
			var wg sync.WaitGroup
			for w := 0; w < 9; w++ {
				wg.Add(1)
				go func(w int) {
					defer wg.Done()
					<-start
					exec := cypher.NewStorageExecutor(env.store)
					for i := 0; i < 5000; i++ {
						query := "MATCH (n:Doc {id:'stable'}) RETURN n.id"
						if w == 0 {
							query = "MATCH (n:Doc {id:'stable'}) SET n.c = n.c + 1 RETURN n.id"
						}
						out := runIndexExecute(ctx, exec, false, query, nil)
						if out.err != nil || len(out.rows) != 1 {
							failures <- fmt.Sprintf("worker %d iteration %d rows=%v error=%v", w, i, out.rows, out.err)
							return
						}
					}
				}(w)
			}
			close(start)
			wg.Wait()
			close(failures)
			for failure := range failures {
				t.Error(failure)
			}
			require.Equal(t, [][]any{{int64(1)}}, mustRunIndex(t, admin, "MATCH (n:Doc {id:'stable'}) RETURN count(n)", nil))
		})
	}
}
