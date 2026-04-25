package knowledgepolicy

import (
	"context"
	"sync"
	"testing"
	"time"
)

type mockStore struct {
	mu      sync.Mutex
	entries map[string]*AccessMetaEntry
}

func newMockStore() *mockStore {
	return &mockStore{entries: make(map[string]*AccessMetaEntry)}
}

func (m *mockStore) GetAccessMeta(entityID string) (*AccessMetaEntry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	e := m.entries[entityID]
	if e == nil {
		return nil, nil
	}
	copy := *e
	return &copy, nil
}

func (m *mockStore) PutAccessMeta(entityID string, entry *AccessMetaEntry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	copy := *entry
	m.entries[entityID] = &copy
	return nil
}

func (m *mockStore) get(entityID string) *AccessMetaEntry {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.entries[entityID]
}

func TestFlusher_BasicFlush(t *testing.T) {
	store := newMockStore()
	acc := NewAccessAccumulator(true, 0)
	f := NewAccessFlusher(acc, store, time.Hour)

	acc.IncrementAccess("n1")
	acc.IncrementAccess("n1")
	acc.IncrementAccess("n1")
	acc.IncrementTraversal("n2")

	f.Flush()

	e1 := store.get("n1")
	if e1 == nil {
		t.Fatal("expected entry for n1")
	}
	if e1.Fixed.AccessCount != 3 {
		t.Errorf("n1 accessCount: expected 3, got %d", e1.Fixed.AccessCount)
	}
	if e1.MutationCount != 1 {
		t.Errorf("n1 mutationCount: expected 1, got %d", e1.MutationCount)
	}

	e2 := store.get("n2")
	if e2 == nil {
		t.Fatal("expected entry for n2")
	}
	if e2.Fixed.TraversalCount != 1 {
		t.Errorf("n2 traversalCount: expected 1, got %d", e2.Fixed.TraversalCount)
	}
}

func TestFlusher_AccumulatesAcrossFlushes(t *testing.T) {
	store := newMockStore()
	acc := NewAccessAccumulator(true, 0)
	f := NewAccessFlusher(acc, store, time.Hour)

	acc.IncrementAccess("n1")
	f.Flush()

	acc.IncrementAccess("n1")
	acc.IncrementAccess("n1")
	f.Flush()

	e := store.get("n1")
	if e.Fixed.AccessCount != 3 {
		t.Errorf("expected accumulated 3, got %d", e.Fixed.AccessCount)
	}
	if e.MutationCount != 2 {
		t.Errorf("expected 2 mutations, got %d", e.MutationCount)
	}
}

func TestFlusher_MergesWithExisting(t *testing.T) {
	store := newMockStore()
	store.entries["n1"] = &AccessMetaEntry{
		TargetID: "n1",
		Fixed:    AccessMetaFixedFields{AccessCount: 100},
	}
	acc := NewAccessAccumulator(true, 0)
	f := NewAccessFlusher(acc, store, time.Hour)

	acc.IncrementAccess("n1")
	f.Flush()

	e := store.get("n1")
	if e.Fixed.AccessCount != 101 {
		t.Errorf("expected 101 (100+1), got %d", e.Fixed.AccessCount)
	}
}

func TestFlusher_EmptyDrain(t *testing.T) {
	store := newMockStore()
	acc := NewAccessAccumulator(true, 0)
	f := NewAccessFlusher(acc, store, time.Hour)

	f.Flush()

	if len(store.entries) != 0 {
		t.Error("expected no entries for empty drain")
	}
}

func TestFlusher_DisabledAccumulator(t *testing.T) {
	store := newMockStore()
	acc := NewAccessAccumulator(false, 0)
	f := NewAccessFlusher(acc, store, time.Hour)

	f.Start(context.Background())
	f.Stop()

	if len(store.entries) != 0 {
		t.Error("expected no entries when disabled")
	}
}

func TestFlusher_StartStop(t *testing.T) {
	store := newMockStore()
	acc := NewAccessAccumulator(true, 0)
	f := NewAccessFlusher(acc, store, 50*time.Millisecond)

	acc.IncrementAccess("n1")

	ctx := context.Background()
	f.Start(ctx)
	time.Sleep(150 * time.Millisecond)
	f.Stop()

	e := store.get("n1")
	if e == nil {
		t.Fatal("expected entry after timed flush")
	}
	if e.Fixed.AccessCount < 1 {
		t.Error("expected at least 1 access count")
	}
}

func TestFlusher_StopFlushesRemaining(t *testing.T) {
	store := newMockStore()
	acc := NewAccessAccumulator(true, 0)
	f := NewAccessFlusher(acc, store, time.Hour)

	f.Start(context.Background())
	acc.IncrementAccess("n1")
	f.Stop()

	e := store.get("n1")
	if e == nil {
		t.Fatal("expected final flush on Stop")
	}
	if e.Fixed.AccessCount != 1 {
		t.Errorf("expected 1, got %d", e.Fixed.AccessCount)
	}
}

func TestFlusher_CustomOverflow(t *testing.T) {
	store := newMockStore()
	acc := NewAccessAccumulator(true, 0)
	f := NewAccessFlusher(acc, store, time.Hour)

	acc.IncrementCustom("n1", "views", 5)
	acc.IncrementCustom("n1", "views", 3)
	f.Flush()

	e := store.get("n1")
	if e == nil {
		t.Fatal("expected entry")
	}
	val, ok := e.Overflow["views"]
	if !ok {
		t.Fatal("expected overflow[views]")
	}
	if v, ok := val.(int64); !ok || v != 8 {
		t.Errorf("expected 8, got %v", val)
	}
}

func TestFlusher_ConcurrentFlush(t *testing.T) {
	store := newMockStore()
	acc := NewAccessAccumulator(true, 0)
	f := NewAccessFlusher(acc, store, time.Hour)

	var wg sync.WaitGroup
	wg.Add(32)
	for g := 0; g < 32; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				acc.IncrementAccess("n1")
			}
		}()
	}
	wg.Wait()
	f.Flush()

	e := store.get("n1")
	if e.Fixed.AccessCount != 3200 {
		t.Errorf("expected 3200, got %d", e.Fixed.AccessCount)
	}
}
