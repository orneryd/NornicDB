package multidb

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestL9QueryLimitCounterOwner(t *testing.T) {
	for _, limit := range []int{0, 1} {
		name := "limited"
		if limit == 0 {
			name = "unlimited"
		}
		t.Run(name, func(t *testing.T) {
			manager, database := setupTestManagerWithLimits(t, &Limits{Query: QueryLimits{MaxConcurrentQueries: limit}})
			manager.mu.Lock()
			_, checker, err := manager.databaseViewLocked(database, false)
			manager.mu.Unlock()
			require.NoError(t, err)
			start := make(chan struct{})
			var workers sync.WaitGroup
			for range 8 {
				workers.Add(1)
				go func() {
					defer workers.Done()
					<-start
					for range 1000 {
						_, release, err := checker.CheckQueryLimits(t.Context())
						if err != nil {
							if limit == 0 {
								t.Error(err)
							}
							continue
						}
						release()
					}
				}()
			}
			close(start)
			workers.Wait()
			_, release, err := checker.CheckQueryLimits(t.Context())
			require.NoError(t, err)
			release()
		})
	}
}
