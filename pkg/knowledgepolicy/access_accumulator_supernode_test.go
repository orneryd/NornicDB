package knowledgepolicy

import (
	"fmt"
	"sync"
	"testing"
)

func TestAccumulator_SuperNode_128Goroutines(t *testing.T) {
	a := NewAccessAccumulator(true, 0)
	const goroutines = 128
	const increments = 10000

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < increments; i++ {
				a.IncrementAccess("super-node")
			}
		}()
	}
	wg.Wait()

	total := a.ReadThrough("super-node", "accessCount", 0)
	expected := int64(goroutines * increments)
	if total != expected {
		t.Errorf("super-node: expected %d, got %d", expected, total)
	}
}

func TestAccumulator_SuperNode_DrainUnderContention(t *testing.T) {
	a := NewAccessAccumulator(true, 0)
	const goroutines = 64
	const increments = 5000
	const drains = 10

	var wg sync.WaitGroup
	wg.Add(goroutines)

	totalDrained := int64(0)
	var drainMu sync.Mutex

	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < increments; i++ {
				a.IncrementAccess("hot-node")
			}
		}()
	}

	for d := 0; d < drains; d++ {
		merged := a.DrainAll()
		if m, ok := merged["hot-node"]; ok {
			drainMu.Lock()
			totalDrained += m.accessCount
			drainMu.Unlock()
		}
	}

	wg.Wait()

	finalMerged := a.DrainAll()
	if m, ok := finalMerged["hot-node"]; ok {
		totalDrained += m.accessCount
	}

	expected := int64(goroutines * increments)
	if totalDrained != expected {
		t.Errorf("total drained: expected %d, got %d", expected, totalDrained)
	}
}

func TestAccumulator_SuperNode_MixedEntities(t *testing.T) {
	a := NewAccessAccumulator(true, 0)
	const goroutines = 128
	const increments = 1000

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(g int) {
			defer wg.Done()
			for i := 0; i < increments; i++ {
				a.IncrementAccess("super-node")
				a.IncrementAccess(fmt.Sprintf("cold-node-%d", g))
			}
		}(g)
	}
	wg.Wait()

	superTotal := a.ReadThrough("super-node", "accessCount", 0)
	if superTotal != int64(goroutines*increments) {
		t.Errorf("super-node: expected %d, got %d", goroutines*increments, superTotal)
	}

	for g := 0; g < goroutines; g++ {
		coldTotal := a.ReadThrough(fmt.Sprintf("cold-node-%d", g), "accessCount", 0)
		if coldTotal != int64(increments) {
			t.Errorf("cold-node-%d: expected %d, got %d", g, increments, coldTotal)
		}
	}
}
