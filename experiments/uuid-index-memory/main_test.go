package main

import (
	"fmt"
	"strings"
	"sync"
	"testing"
)

func TestExactCompatibility(t *testing.T) {
	ids := []string{"nornic:550e8400-e29b-41d4-a716-446655440000", "nornic:550E8400-E29B-41D4-A716-446655440000", "550e8400e29b41d4a716446655440000", "not-a-uuid", "tenant:日本語/\x00", strings.Repeat("x", 8192)}
	for _, mode := range []string{"baseline", "paged", "packed", "packed-hash"} {
		t.Run(mode, func(t *testing.T) {
			var d dictionary
			switch mode {
			case "baseline":
				d = newMaps(false)
			case "paged":
				d = newMaps(true)
			case "packed":
				d = newPacked()
			case "packed-hash":
				d = newPackedHash()
			}
			for i, id := range ids {
				num := uint64(i)*65537 + 1
				d.Put(id, num)
				d.Put(id, num+1)
				if n, ok := d.Get(id); !ok || n != num {
					t.Fatal("exact forward", i, n, ok)
				}
				if s, ok := d.Reverse(num); !ok || s != id {
					t.Fatal("exact reverse", i)
				}
			}
			for i, id := range ids {
				d.Delete(id)
				if _, ok := d.Get(id); ok {
					t.Fatal("deleted forward")
				}
				if _, ok := d.Reverse(uint64(i)*65537 + 1); ok {
					t.Fatal("deleted reverse")
				}
			}
		})
	}
}
func TestPackedCollisionsAndChurn(t *testing.T) {
	d := newPacked()
	ids := []string{}
	for i := 0; len(ids) < 200; i++ {
		s := fmt.Sprint("collision/", i)
		if shard(s) == 0 && hash(s)&15 == 0 {
			ids = append(ids, s)
		}
	}
	for round := 0; round < 3; round++ {
		for i, id := range ids {
			d.Put(id, uint64(i)+1)
		}
		for i, id := range ids {
			if n, ok := d.Get(id); !ok || n != uint64(i)+1 {
				t.Fatal("collision lost")
			}
		}
		for _, id := range ids {
			d.Delete(id)
		}
	}
}
func TestDisjointConcurrentMutations(t *testing.T) {
	for _, mode := range []string{"baseline", "paged", "packed", "packed-hash"} {
		t.Run(mode, func(t *testing.T) {
			var d dictionary
			switch mode {
			case "baseline":
				d = newMaps(false)
			case "paged":
				d = newMaps(true)
			case "packed":
				d = newPacked()
			case "packed-hash":
				d = newPackedHash()
			}
			var wg sync.WaitGroup
			for worker := 0; worker < 8; worker++ {
				wg.Add(1)
				go func(w int) {
					defer wg.Done()
					for i := 0; i < 500; i++ {
						num := uint64(w*500+i) + 1
						id := fmt.Sprint("worker/", num)
						d.Put(id, num)
						if n, ok := d.Get(id); !ok || n != num {
							t.Error("forward")
						}
						if s, ok := d.Reverse(num); !ok || s != id {
							t.Error("reverse")
						}
						d.Delete(id)
					}
				}(worker)
			}
			wg.Wait()
		})
	}
}
