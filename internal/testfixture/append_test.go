package testfixture

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAppendRecordsOneValuePerCallInLockOrder(t *testing.T) {
	var owner sync.Mutex
	var values []int
	Append(&owner, &values, 1)
	require.Equal(t, []int{1}, Snapshot(&owner, &values))
	Append(&owner, &values, 2)
	Append(&owner, &values, 3)
	require.Equal(t, []int{1, 2, 3}, Snapshot(&owner, &values))

	// Appending to a non-nil empty producer keeps the producer's own storage
	// policy; the recorded values remain exactly the appended ones.
	empty := make([]int, 0)
	Append(&owner, &empty, 7)
	require.Equal(t, []int{7}, Snapshot(&owner, &empty))
}

func TestAppendTakesTheProducerLockSnapshotReadsWith(t *testing.T) {
	var owner sync.Mutex
	var values []int
	var writers sync.WaitGroup
	writers.Add(1)
	go func() {
		defer writers.Done()
		for i := 0; i < 1000; i++ {
			Append(&owner, &values, i)
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

func TestAppendKeepsEveryValueFromConcurrentWriters(t *testing.T) {
	var owner sync.Mutex
	var values []int
	var writers sync.WaitGroup
	for writer := 0; writer < 4; writer++ {
		writers.Add(1)
		go func() {
			defer writers.Done()
			for i := 0; i < 250; i++ {
				Append(&owner, &values, i)
			}
		}()
	}
	for i := 0; i < 250; i++ {
		Snapshot(&owner, &values)
	}
	writers.Wait()
	require.Len(t, Snapshot(&owner, &values), 1000)
}
