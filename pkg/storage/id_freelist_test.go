package storage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestFreelist_RecyclesNumIDAfterPruneAndTTL exercises the full loop:
// create → delete → prune pushes to freelist → wait past TTL → allocate
// reuses the numID.
func TestFreelist_RecyclesNumIDAfterPruneAndTTL(t *testing.T) {
	engine, err := NewBadgerEngineWithOptions(BadgerOptions{
		InMemory: true,
		EngineOptions: EngineOptions{
			// Short TTL so the test completes quickly.
			IDFreelistTTL: 50 * time.Millisecond,
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })

	// Seed one node and capture its numID.
	_, err = engine.CreateNode(&Node{ID: "test:node-1", Labels: []string{"X"}})
	require.NoError(t, err)
	firstNum, ok := engine.idDict.lookupNodeNumID("test:node-1")
	require.True(t, ok)
	require.Equal(t, uint64(1), firstNum)

	// Delete + prune to push numID onto the freelist.
	require.NoError(t, engine.DeleteNode("test:node-1"))
	_, err = engine.PruneMVCCVersions(context.Background(), MVCCPruneOptions{})
	require.NoError(t, err)

	// Before TTL elapses, allocating a new entity must NOT reclaim it.
	_, err = engine.CreateNode(&Node{ID: "test:node-2", Labels: []string{"X"}})
	require.NoError(t, err)
	secondNum, ok := engine.idDict.lookupNodeNumID("test:node-2")
	require.True(t, ok)
	require.NotEqual(t, firstNum, secondNum, "numID reused before TTL expired — debounce broken")

	// After TTL elapses, the next allocation should reuse the parked numID.
	time.Sleep(60 * time.Millisecond)
	_, err = engine.CreateNode(&Node{ID: "test:node-3", Labels: []string{"X"}})
	require.NoError(t, err)
	thirdNum, ok := engine.idDict.lookupNodeNumID("test:node-3")
	require.True(t, ok)
	require.Equal(t, firstNum, thirdNum, "TTL-aged numID should have been reclaimed")
}

// TestFreelist_HeadIsFifo ensures the first numID pushed is the first
// reclaimed — prevents scans from having to pick "oldest" by value.
func TestFreelist_HeadIsFifo(t *testing.T) {
	engine, err := NewBadgerEngineWithOptions(BadgerOptions{
		InMemory: true,
		EngineOptions: EngineOptions{
			IDFreelistTTL: 20 * time.Millisecond,
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })

	for i := 0; i < 3; i++ {
		_, err := engine.CreateNode(&Node{ID: NodeID(letter(i)), Labels: []string{"X"}})
		require.NoError(t, err)
	}
	num1, _ := engine.idDict.lookupNodeNumID(NodeID(letter(0)))
	num2, _ := engine.idDict.lookupNodeNumID(NodeID(letter(1)))
	num3, _ := engine.idDict.lookupNodeNumID(NodeID(letter(2)))

	// Delete in order 0, 1, 2 (each push is strictly newer than the last).
	require.NoError(t, engine.DeleteNode(NodeID(letter(0))))
	require.NoError(t, engine.DeleteNode(NodeID(letter(1))))
	require.NoError(t, engine.DeleteNode(NodeID(letter(2))))
	_, err = engine.PruneMVCCVersions(context.Background(), MVCCPruneOptions{})
	require.NoError(t, err)

	time.Sleep(30 * time.Millisecond)

	// First allocation should reclaim num1 (oldest), second num2, third num3.
	_, err = engine.CreateNode(&Node{ID: "test:new1", Labels: []string{"X"}})
	require.NoError(t, err)
	got, _ := engine.idDict.lookupNodeNumID("test:new1")
	require.Equal(t, num1, got, "first reclaim should be the oldest numID")

	_, err = engine.CreateNode(&Node{ID: "test:new2", Labels: []string{"X"}})
	require.NoError(t, err)
	got, _ = engine.idDict.lookupNodeNumID("test:new2")
	require.Equal(t, num2, got)

	_, err = engine.CreateNode(&Node{ID: "test:new3", Labels: []string{"X"}})
	require.NoError(t, err)
	got, _ = engine.idDict.lookupNodeNumID("test:new3")
	require.Equal(t, num3, got)
}

func letter(i int) string {
	return "test:n" + string(rune('a'+i))
}
