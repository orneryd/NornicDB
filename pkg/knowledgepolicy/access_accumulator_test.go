package knowledgepolicy

import (
	"fmt"
	"sync"
	"testing"
)

func TestAccumulator_Disabled(t *testing.T) {
	a := NewAccessAccumulator(false)
	a.IncrementAccess("n1")
	a.IncrementTraversal("n1")
	a.IncrementCustom("n1", "hits", 5)

	val := a.ReadThrough("n1", "accessCount", 0)
	if val != 0 {
		t.Errorf("expected 0 when disabled, got %d", val)
	}
}

func TestAccumulator_SingleGoroutine(t *testing.T) {
	a := NewAccessAccumulator(true)

	for i := 0; i < 100; i++ {
		a.IncrementAccess("n1")
	}
	for i := 0; i < 50; i++ {
		a.IncrementTraversal("n1")
	}

	accessCount := a.ReadThrough("n1", "accessCount", 0)
	if accessCount != 100 {
		t.Errorf("accessCount: expected 100, got %d", accessCount)
	}

	traversalCount := a.ReadThrough("n1", "traversalCount", 0)
	if traversalCount != 50 {
		t.Errorf("traversalCount: expected 50, got %d", traversalCount)
	}
}

func TestAccumulator_ConcurrentAccess(t *testing.T) {
	a := NewAccessAccumulator(true)
	const goroutines = 64
	const increments = 1000

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < increments; i++ {
				a.IncrementAccess("n1")
			}
		}()
	}
	wg.Wait()

	total := a.ReadThrough("n1", "accessCount", 0)
	expected := int64(goroutines * increments)
	if total != expected {
		t.Errorf("expected %d, got %d", expected, total)
	}
}

func TestAccumulator_ConcurrentMultiEntity(t *testing.T) {
	a := NewAccessAccumulator(true)
	const goroutines = 32
	const entities = 16
	const increments = 500

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(g int) {
			defer wg.Done()
			entityID := fmt.Sprintf("n%d", g%entities)
			for i := 0; i < increments; i++ {
				a.IncrementAccess(entityID)
			}
		}(g)
	}
	wg.Wait()

	for e := 0; e < entities; e++ {
		entityID := fmt.Sprintf("n%d", e)
		count := a.ReadThrough(entityID, "accessCount", 0)
		expected := int64((goroutines / entities) * increments)
		if count != expected {
			t.Errorf("%s: expected %d, got %d", entityID, expected, count)
		}
	}
}

func TestAccumulator_CustomCounters(t *testing.T) {
	a := NewAccessAccumulator(true)
	a.IncrementCustom("n1", "views", 3)
	a.IncrementCustom("n1", "views", 7)

	val := a.ReadThrough("n1", "views", 10)
	if val != 20 {
		t.Errorf("expected 20 (10 persisted + 10 buffered), got %d", val)
	}
}

func TestAccumulator_ReadThroughWithPersisted(t *testing.T) {
	a := NewAccessAccumulator(true)
	a.IncrementAccess("n1")
	a.IncrementAccess("n1")
	a.IncrementAccess("n1")

	val := a.ReadThrough("n1", "accessCount", 100)
	if val != 103 {
		t.Errorf("expected 103 (100 persisted + 3 buffered), got %d", val)
	}
}

func TestAccumulator_DrainAll(t *testing.T) {
	a := NewAccessAccumulator(true)

	for i := 0; i < 50; i++ {
		a.IncrementAccess("n1")
	}
	for i := 0; i < 30; i++ {
		a.IncrementTraversal("n2")
	}

	merged := a.DrainAll()

	if d, ok := merged["n1"]; !ok || d.accessCount != 50 {
		t.Errorf("n1 accessCount: expected 50, got %v", merged["n1"])
	}
	if d, ok := merged["n2"]; !ok || d.traversalCount != 30 {
		t.Errorf("n2 traversalCount: expected 30, got %v", merged["n2"])
	}

	afterDrain := a.ReadThrough("n1", "accessCount", 0)
	if afterDrain != 0 {
		t.Errorf("expected 0 after drain, got %d", afterDrain)
	}
}

func TestAccumulator_ClearEntity(t *testing.T) {
	a := NewAccessAccumulator(true)
	a.IncrementAccess("n1")
	a.IncrementAccess("n2")

	a.ClearEntity("n1")

	n1 := a.ReadThrough("n1", "accessCount", 0)
	n2 := a.ReadThrough("n2", "accessCount", 0)
	if n1 != 0 {
		t.Errorf("expected 0 after clear, got %d", n1)
	}
	if n2 != 1 {
		t.Errorf("expected 1 for uncleared, got %d", n2)
	}
}

func TestAccumulator_Timestamps_MaxWins(t *testing.T) {
	a := NewAccessAccumulator(true)
	a.SetTimestamp("n1", "lastAccessedAt", 100)
	a.SetTimestamp("n1", "lastAccessedAt", 50)
	a.SetTimestamp("n1", "lastAccessedAt", 200)

	merged := a.DrainAll()
	if merged["n1"].overflow["lastAccessedAt"] != 200 {
		t.Errorf("expected max timestamp 200, got %d", merged["n1"].overflow["lastAccessedAt"])
	}
}
