package antlr

import (
	"fmt"
	"testing"
	"unsafe"
)

type cancellationTestStop struct{}
type integerCollisionComparator struct{}

func (integerCollisionComparator) Hash1(int) int                { return 0 }
func (integerCollisionComparator) Equals2(left, right int) bool { return left == right }
func expectWorkStop(t *testing.T, run func()) {
	t.Helper()
	defer func() {
		if _, ok := recover().(cancellationTestStop); !ok {
			t.Fatal("expected private cancellation sentinel")
		}
	}()
	run()
}

func TestCancellationScalarDirectory(t *testing.T) {
	directory := scalarDirectory[int]{}
	values := make([]int, 10000)
	for i := range values {
		values[i] = i
		directory.put(i*7919, &values[i], nil)
	}
	for i := range values {
		if result := directory.get(i*7919, nil); result == nil || *result != i {
			t.Fatal(i, result)
		}
	}
	for _, key := range []int{-1, 1 << 62, 1 << 61, -1 << 62} {
		value := key
		directory.put(key, &value, nil)
		if *directory.get(key, nil) != key {
			t.Fatal(key)
		}
	}
	count := 0
	directory.each(nil, func(*int) bool { count++; return true })
	if count != len(values)+4 {
		t.Fatal(count)
	}
	count = 0
	directory.each(nil, func(*int) bool { count++; return false })
	if count != 1 {
		t.Fatal(count)
	}
	for stop := 1; stop < 130; stop++ {
		calls := 0
		check := func() {
			calls++
			if calls == stop {
				panic(cancellationTestStop{})
			}
		}
		value := 123
		func() {
			defer func() {
				if e := recover(); e != nil {
					if _, ok := e.(cancellationTestStop); !ok {
						panic(e)
					}
				}
			}()
			directory.put(991, &value, check)
		}()
		if calls >= stop && directory.get(991, nil) != nil {
			t.Fatalf("published canceled insertion at %d", stop)
		}
		if calls < stop {
			break
		}
	}
	if unsafe.Sizeof(scalarDirectoryNode[int]{}) > 64 {
		t.Fatal("directory node unexpectedly large")
	}
	if unsafe.Sizeof(sequencePage[*ATNConfig]{}) > 4096 || unsafe.Sizeof(collisionPage[*ATNConfig]{}) > 4096 {
		t.Fatal("copy page exceeds bound")
	}
}

func TestCancellationCollectionsPublishComplete(t *testing.T) {
	comparator := integerCollisionComparator{}
	store := NewJStore[int, Comparator[int]](comparator, UnknownCollection, "test")
	mapping := NewJMap[int, string, Comparator[int]](comparator, UnknownCollection, "test")
	for i := 0; i < 200; i++ {
		store.Put(i)
		mapping.Put(i, fmt.Sprint(i))
	}
	if value, ok := store.Get(199); !ok || value != 199 {
		t.Fatal(value, ok)
	}
	if value, ok := mapping.Get(199); !ok || value != "199" {
		t.Fatal(value, ok)
	}
	if value, exists := mapping.Put(199, "replacement"); !exists || value != "199" {
		t.Fatal("Put must retain existing value")
	}
	for _, kind := range []string{"store", "map", "delete"} {
		for stop := 1; ; stop++ {
			calls := 0
			check := func() {
				calls++
				if calls == stop {
					panic(cancellationTestStop{})
				}
			}
			operation := &CollectionOperation[int]{Check: check}
			stopped := false
			func() {
				defer func() {
					if value := recover(); value != nil {
						if _, ok := value.(cancellationTestStop); !ok {
							panic(value)
						}
						stopped = true
					}
				}()
				switch kind {
				case "store":
					store.PutWithOperation(201, operation)
				case "map":
					mapping.PutWithOperation(201, "201", operation)
				case "delete":
					mapping.DeleteWithOperation(199, operation)
				}
			}()
			if !stopped {
				break
			}
			if store.Len() != 200 || mapping.Len() != 200 {
				t.Fatalf("partial commit in %s at %d", kind, stop)
			}
		}
		if kind == "store" {
			if store.Len() != 201 {
				t.Fatal(store.Len())
			}
			store = NewJStore[int, Comparator[int]](comparator, UnknownCollection, "test")
			for i := 0; i < 200; i++ {
				store.Put(i)
			}
		}
		if kind == "map" {
			mapping.Delete(201)
		}
	}
	if mapping.Len() != 199 {
		t.Fatal(mapping.Len())
	}
	if _, found := mapping.Get(199); found {
		t.Fatal("delete did not remove")
	}
	mapping.Clear()
	// v4.13.1 Clear drops entries without resetting its historical Len counter.
	// Preserve that compatibility detail rather than changing unrelated API behavior.
	if mapping.Len() != 199 {
		t.Fatal("legacy Clear count changed", mapping.Len())
	}
	if _, found := mapping.Get(198); found {
		t.Fatal("Clear retained an entry")
	}
	count := 0
	store.Each(func(int) bool { count++; return false })
	if count != 200 {
		t.Fatal("legacy Each contract changed")
	}
	if len(store.Values()) != 200 || len(store.SortedSlice(func(a, b int) bool { return a < b })) != 200 {
		t.Fatal("materialization changed")
	}
}

func TestCancellationSequenceGrowth(t *testing.T) {
	sequence := workSequence[int]{}
	empty := scalarDirectory[int]{}
	if !empty.each(func() {}, func(*int) bool { t.Fatal("empty directory visited a value"); return false }) {
		t.Fatal("empty directory traversal changed")
	}
	for i := 0; i < 10000; i++ {
		sequence.Append(i, nil)
	}
	sequence.Set(0, 123, func() {})
	if sequence.At(0, nil) != 123 {
		t.Fatal("first-page update failed")
	}
	sequence.Set(0, 0, nil)
	for i := 0; i < 10000; i++ {
		if sequence.At(i, nil) != i {
			t.Fatal(i)
		}
	}
	for i := 9999; i >= 0; i-- {
		if sequence.Pop(nil) != i {
			t.Fatal(i)
		}
	}
	for i := 0; i < 10000; i++ {
		sequence.Append(i, nil)
	}
	for stop := 1; stop < 30; stop++ {
		copy := sequence
		calls := 0
		check := func() {
			calls++
			if calls == stop {
				panic(cancellationTestStop{})
			}
		}
		func() {
			defer func() {
				if e := recover(); e != nil {
					if _, ok := e.(cancellationTestStop); !ok {
						panic(e)
					}
				}
			}()
			copy.Append(10000, check)
		}()
		if calls >= stop && copy.Len() != sequence.Len() {
			t.Fatal("partial sequence append")
		}
	}
}
