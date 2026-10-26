package knowledgepolicy

import (
	"errors"
	"sort"
	"testing"
	"time"
)

// scriptedAccessMetaStore records the flusher's read requests and lets a test
// inject a read result or error for the whole request boundary.
type scriptedAccessMetaStore struct {
	reads  []AccessMetaReadRequest
	entry  *AccessMetaEntry
	err    error
	writes map[string]*AccessMetaEntry
}

func (s *scriptedAccessMetaStore) ReadAccessMeta(request AccessMetaReadRequest) (AccessMetaReadResult, error) {
	s.reads = append(s.reads, request)
	if s.err != nil {
		return AccessMetaReadResult{}, s.err
	}
	return AccessMetaReadResult{Entry: s.entry}, nil
}

func (s *scriptedAccessMetaStore) PutAccessMeta(entityID string, entry *AccessMetaEntry) error {
	if s.writes == nil {
		s.writes = make(map[string]*AccessMetaEntry)
	}
	s.writes[entityID] = entry
	return nil
}

func TestMockStoreReadAccessMetaPointCopiesEntry(t *testing.T) {
	store := newMockStore()
	stored := &AccessMetaEntry{TargetID: "n1", Fixed: AccessMetaFixedFields{AccessCount: 4}}
	if err := store.PutAccessMeta("n1", stored); err != nil {
		t.Fatal(err)
	}
	stored.Fixed.AccessCount = 9

	result, err := store.ReadAccessMeta(AccessMetaReadRequest{Mode: AccessMetaReadPoint, EntityID: "n1"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Entry == nil || result.Entry.Fixed.AccessCount != 4 {
		t.Fatalf("expected shallow copy with count 4, got %+v", result.Entry)
	}
	if result.Entries != nil {
		t.Fatalf("point read must not populate Entries, got %d", len(result.Entries))
	}
	result.Entry.Fixed.AccessCount = 1
	again, err := store.ReadAccessMeta(AccessMetaReadRequest{Mode: AccessMetaReadPoint, EntityID: "n1"})
	if err != nil {
		t.Fatal(err)
	}
	if again.Entry.Fixed.AccessCount != 4 {
		t.Fatalf("point read must not expose stored entry, got %d", again.Entry.Fixed.AccessCount)
	}
	missing, err := store.ReadAccessMeta(AccessMetaReadRequest{Mode: AccessMetaReadPoint, EntityID: "absent"})
	if err != nil {
		t.Fatal(err)
	}
	if missing.Entry != nil {
		t.Fatal("expected nil entry for a missing entity")
	}
}

func TestMockStoreReadAccessMetaAllIsDeterministic(t *testing.T) {
	store := newMockStore()
	for _, id := range []string{"c", "a", "b"} {
		if err := store.PutAccessMeta(id, &AccessMetaEntry{TargetID: id}); err != nil {
			t.Fatal(err)
		}
	}
	var want []string
	for i := 0; i < 8; i++ {
		result, err := store.ReadAccessMeta(AccessMetaReadRequest{Mode: AccessMetaReadAll})
		if err != nil {
			t.Fatal(err)
		}
		if result.Entry != nil {
			t.Fatal("all read must not populate Entry")
		}
		var ids []string
		for _, entry := range result.Entries {
			ids = append(ids, entry.TargetID)
		}
		if !sort.StringsAreSorted(ids) {
			t.Fatalf("all read is not ordered: %v", ids)
		}
		if want == nil {
			want = ids
			continue
		}
		if len(ids) != len(want) {
			t.Fatalf("all read length changed: %v vs %v", ids, want)
		}
		for j := range want {
			if ids[j] != want[j] {
				t.Fatalf("all read order changed: %v vs %v", ids, want)
			}
		}
	}
	if len(want) != 3 {
		t.Fatalf("expected 3 entries, got %v", want)
	}
	empty, err := newMockStore().ReadAccessMeta(AccessMetaReadRequest{Mode: AccessMetaReadAll})
	if err != nil {
		t.Fatal(err)
	}
	if empty.Entries == nil || len(empty.Entries) != 0 {
		t.Fatalf("empty store must return an empty, non-nil entry slice, got %#v", empty.Entries)
	}
}

func TestMockStoreReadAccessMetaRejectsUnknownMode(t *testing.T) {
	store := newMockStore()
	if _, err := store.ReadAccessMeta(AccessMetaReadRequest{Mode: AccessMetaReadMode(7)}); err == nil {
		t.Fatal("expected an error for an unsupported read mode")
	}
}

func TestFlusherReadsPointEntryPerEntity(t *testing.T) {
	store := &scriptedAccessMetaStore{entry: &AccessMetaEntry{
		TargetID: "n1", Fixed: AccessMetaFixedFields{AccessCount: 10},
	}}
	acc := NewAccessAccumulator(true, 0)
	f := NewAccessFlusher(acc, store, time.Hour)

	acc.IncrementAccess("n1")
	acc.IncrementAccess("n1")
	f.Flush()

	if len(store.reads) != 1 {
		t.Fatalf("expected one read, got %d", len(store.reads))
	}
	if got := store.reads[0]; got.Mode != AccessMetaReadPoint || got.EntityID != "n1" {
		t.Fatalf("expected a point read of n1, got %+v", got)
	}
	written := store.writes["n1"]
	if written == nil || written.Fixed.AccessCount != 12 {
		t.Fatalf("expected the existing entry plus 2 accesses, got %+v", written)
	}
}

func TestFlusherSkipsEntityOnReadError(t *testing.T) {
	store := &scriptedAccessMetaStore{err: errors.New("scripted access metadata read failure")}
	acc := NewAccessAccumulator(true, 0)
	f := NewAccessFlusher(acc, store, time.Hour)

	acc.IncrementAccess("n1")
	f.Flush()

	if len(store.writes) != 0 {
		t.Fatalf("a failed read must skip the entity, wrote %v", store.writes)
	}
}

func TestFlusherCreatesEntryOnEmptyRead(t *testing.T) {
	store := &scriptedAccessMetaStore{}
	acc := NewAccessAccumulator(true, 0)
	f := NewAccessFlusher(acc, store, time.Hour)

	acc.IncrementAccess("n1")
	f.Flush()

	written := store.writes["n1"]
	if written == nil || written.TargetID != "n1" || written.TargetScope != ScopeNode || written.Fixed.AccessCount != 1 {
		t.Fatalf("expected a new node-scope entry with one access, got %+v", written)
	}
}
