package cypher

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"
)

func TestPipelineStringMapBoundedDirectory(t *testing.T) {
	for _, checked := range []bool{false, true} {
		index := pipelineStringMap[int]{}
		if checked {
			index.probe = &pipelineCancellation{ctx: context.Background()}
		}
		for item := 0; item < 10000; item++ {
			require.True(t, index.Set(fmt.Sprint(item), item))
		}
		for item := 0; item < 10000; item++ {
			value, found := index.Get(fmt.Sprint(item))
			require.True(t, found)
			require.Equal(t, item, value)
			require.True(t, index.Set(fmt.Sprint(item), item+1))
		}
		var walk func(*pipelineStringMapNode[int], int, int)
		walk = func(node *pipelineStringMapNode[int], last, depth int) {
			require.LessOrEqual(t, depth, 64)
			if node.bit < 0 {
				return
			}
			require.Greater(t, node.bit, last)
			walk(node.children[0], node.bit, depth+1)
			walk(node.children[1], node.bit, depth+1)
		}
		walk(index.root, -1, 0)
		_, found := index.Get("absent")
		require.False(t, found)
	}
	require.LessOrEqual(t, unsafe.Sizeof(pipelineStringMapPage[int]{}), uintptr(4096))
	require.LessOrEqual(t, unsafe.Sizeof(pipelineStringMapPage[interface{}]{}), uintptr(4096))
}

func pipelineMapCollisionFixture(key string, entries int) *pipelineStringMap[int] {
	bucket := &pipelineStringMapBucket[int]{first: pipelineStringMapEntry[int]{key: key + "0", value: 0}}
	for item := 1; item < entries; item++ {
		if bucket.tail == nil || bucket.tail.used == 64 {
			page := &pipelineStringMapPage[int]{}
			if bucket.tail == nil {
				bucket.head = page
			} else {
				bucket.tail.next = page
			}
			bucket.tail = page
		}
		bucket.tail.values[bucket.tail.used] = pipelineStringMapEntry[int]{key: key + fmt.Sprint(item), value: item}
		bucket.tail.used++
	}
	return &pipelineStringMap[int]{size: entries, root: &pipelineStringMapNode[int]{hash: hashPipelineString(key, pipelineStringMapSeed, nil), bit: -1, bucket: bucket}}
}

func TestPipelineStringMapCollisionCancellation(t *testing.T) {
	key := strings.Repeat("x", 1<<20)
	for _, write := range []bool{false, true} {
		run := func(index *pipelineStringMap[int], probe *pipelineCancellation) {
			index.probe = probe
			if write {
				index.Set(key, 1000)
			} else {
				index.Get(key)
			}
		}
		counter := &pipelineCountingContext{Context: context.Background()}
		run(pipelineMapCollisionFixture(key, 140), &pipelineCancellation{ctx: counter})
		require.Greater(t, counter.calls, 2)
		for stop := 1; stop <= counter.calls; stop++ {
			index := pipelineMapCollisionFixture(key, 140)
			ctx := &pipelineCountingContext{Context: context.Background(), stop: stop}
			probe := &pipelineCancellation{ctx: ctx}
			run(index, probe)
			require.ErrorIs(t, probe.err, context.Canceled)
			index.probe = nil
			_, found := index.Get(key)
			require.False(t, found, "cancelled insertion must not publish")
		}
	}
	index := pipelineMapCollisionFixture(key, 140)
	require.True(t, index.Set(key, 1000))
	value, found := index.Get(key)
	require.True(t, found)
	require.Equal(t, 1000, value)
	require.True(t, index.Set(key, 1001))
	value, _ = index.Get(key)
	require.Equal(t, 1001, value)
}

func TestPipelineStringSetInsertionPositions(t *testing.T) {
	set := pipelineStringSet{}
	position, exists := set.position("a")
	require.Zero(t, position)
	require.False(t, exists)
	position, exists = set.position("b")
	require.Equal(t, 1, position)
	require.False(t, exists)
	position, exists = set.position("a")
	require.Zero(t, position)
	require.True(t, exists)
	require.True(t, set.contains("b"))
	require.False(t, set.contains("c"))
	require.True(t, set.add("b"))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	set.probe = &pipelineCancellation{ctx: ctx}
	require.False(t, set.add("c"))
	require.False(t, set.contains("a"))
	require.Equal(t, 2, set.next)
}

func TestPipelineStringMapCancelEveryPreparationPoint(t *testing.T) {
	for _, shape := range []string{"empty", "directory", "collision_empty", "collision_full", "collision_partial"} {
		t.Run(shape, func(t *testing.T) {
			for stop := 1; stop <= 2; stop++ {
				for phase := 1; phase <= 128; phase++ {
					index := &pipelineStringMap[int]{}
					switch shape {
					case "directory":
						for i := 0; i < 256; i++ {
							require.True(t, index.Set(fmt.Sprint(i), i))
						}
					case "collision_empty":
						index = pipelineMapCollisionFixture("target", 1)
					case "collision_full":
						index = pipelineMapCollisionFixture("target", 65)
					case "collision_partial":
						index = pipelineMapCollisionFixture("target", 3)
					}
					ctx := &pipelineCountingContext{Context: context.Background(), stop: stop}
					probe := &pipelineCancellation{ctx: ctx, remaining: phase}
					index.probe = probe
					inserted := index.Set("target", 999)
					index.probe = nil
					value, found := index.Get("target")
					if probe.err != nil {
						require.False(t, inserted)
						require.False(t, found, "partial insertion at phase %d", phase)
					} else {
						require.True(t, inserted)
						require.True(t, found)
						require.Equal(t, 999, value)
					}
				}
			}
		})
	}
}

func TestPipelineStringMapSharedOperationsAndDeletion(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	probe := &pipelineCancellation{ctx: ctx}
	index := pipelineStringMap[int]{}
	_, found := index.FirstKeyWithCancellation(probe)
	require.False(t, found)
	require.False(t, index.DeleteWithCancellation("missing", probe))
	for i := 0; i < 1000; i++ {
		require.True(t, index.SetWithCancellation(fmt.Sprint(i), i, probe))
	}
	require.Nil(t, index.probe, "persistent cache must not retain query work")
	require.Equal(t, 1000, index.Len())
	first, found := index.FirstKeyWithCancellation(probe)
	require.True(t, found)
	_, found = index.Get(first)
	require.True(t, found)
	require.False(t, index.DeleteWithCancellation("missing", probe))
	for i := 0; i < 1000; i++ {
		value, ok := index.GetWithCancellation(fmt.Sprint(i), probe)
		require.True(t, ok)
		require.Equal(t, i, value)
		require.True(t, index.DeleteWithCancellation(fmt.Sprint(i), probe))
	}
	require.Zero(t, index.Len())
	require.Nil(t, index.root)
	require.True(t, index.SetWithCancellation("same", 1, probe))
	require.True(t, index.SetWithCancellation("same", 2, probe))
	require.Equal(t, 1, index.Len())
	cancel()
	probe.remaining = 1
	require.False(t, index.SetWithCancellation("cancelled", 3, probe))
	_, found = index.GetWithCancellation("same", probe)
	require.False(t, found)
	_, found = index.FirstKeyWithCancellation(probe)
	require.False(t, found)
	require.False(t, index.DeleteWithCancellation("same", probe))
	require.Equal(t, 1, index.Len())
	index.Clear()
	require.Zero(t, index.Len())
	require.Nil(t, index.root)
	longKey := strings.Repeat("x", 65536)
	require.True(t, index.Set(longKey, 1))
	stopped := &pipelineCountingContext{Context: context.Background(), stop: 1}
	require.False(t, index.DeleteWithCancellation(longKey, &pipelineCancellation{ctx: stopped, remaining: 2}))
	require.Equal(t, 1, index.Len())
}

func TestPipelineStringMapCollisionDeletionIsAtomic(t *testing.T) {
	key := strings.Repeat("x", 128)
	for _, length := range []int{1, 2, 65, 66, 140} {
		for _, position := range []int{0, length / 2, length - 1} {
			target := key + fmt.Sprint(position)
			fixture := func() *pipelineStringMap[int] {
				index := pipelineMapCollisionFixture(key, length)
				index.root.hash = hashPipelineString(target, pipelineStringMapSeed, nil)
				return index
			}
			index := fixture()
			require.True(t, index.DeleteWithCancellation(target, nil))
			require.Equal(t, length-1, index.Len())
			_, found := index.Get(target)
			require.False(t, found)
			for i := 0; i < length; i++ {
				if i == position {
					continue
				}
				entry := index.find(index.root.bucket, key+fmt.Sprint(i))
				require.NotNil(t, entry)
				require.Equal(t, i, entry.value)
			}
			for phase := 1; phase <= 128; phase++ {
				index = fixture()
				ctx := &pipelineCountingContext{Context: context.Background(), stop: 1}
				probe := &pipelineCancellation{ctx: ctx, remaining: phase}
				deleted := index.DeleteWithCancellation(target, probe)
				if probe.err != nil {
					require.False(t, deleted)
					require.Equal(t, length, index.Len())
					require.NotNil(t, index.find(index.root.bucket, target))
				}
			}
		}
	}
}
