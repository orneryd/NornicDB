package testfixture

import (
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
)

func TestSnapshotRetainsEmptyAndDetachedSliceContract(t *testing.T) {
	var owner sync.Mutex
	var values []int
	require.Nil(t, Snapshot(&owner, &values))
	values = make([]int, 0)
	require.Nil(t, Snapshot(&owner, &values))
	values = []int{1, 2}
	got := Snapshot(&owner, &values)
	require.Equal(t, values, got)
	got[0] = 99
	require.Equal(t, 1, values[0])
	values[1] = 88
	require.Equal(t, 2, got[1])
}

func TestSnapshotSharesTheProducerLock(t *testing.T) {
	var owner sync.Mutex
	var values []int
	var writers sync.WaitGroup
	writers.Add(1)
	go func() {
		defer writers.Done()
		for i := 0; i < 1000; i++ {
			owner.Lock()
			values = append(values, i)
			owner.Unlock()
		}
	}()
	for i := 0; i < 1000; i++ {
		got := Snapshot(&owner, &values)
		for index, value := range got {
			require.Equal(t, index, value)
		}
	}
	writers.Wait()
	require.Len(t, Snapshot(&owner, &values), 1000)
}
