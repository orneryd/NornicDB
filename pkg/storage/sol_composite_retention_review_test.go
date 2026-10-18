package storage

import (
	"runtime"
	"strings"
	"testing"
	"unsafe"
	"weak"
)

func TestSolCompositeRetiredIDCollectible(t *testing.T) {
	for _, owner := range []string{"values", "fullIndex", "prefixIndex"} {
		t.Run(owner, func(t *testing.T) {
			ref, bucket := func() (weak.Pointer[byte], []NodeID) {
				idx := &CompositeIndex{Properties: []string{"a"}, fullIndex: make(map[string][]NodeID), prefixIndex: make(map[string][]NodeID)}
				if owner != "values" {
					idx.Properties = append(idx.Properties, "b")
				}
				props := map[string]any{"a": 1, "b": 2}
				ref := func() weak.Pointer[byte] {
					id := NodeID(strings.Repeat("retired-id", 8192))
					if err := idx.IndexNode("test:kept", props); err != nil {
						t.Fatal(err)
					}
					if err := idx.IndexNode(id, props); err != nil {
						t.Fatal(err)
					}
					ref := weak.Make(unsafe.StringData(string(id)))
					idx.RemoveNode(id, props)
					return ref
				}()
				values := []any{1}
				if owner != "values" {
					values = append(values, 2)
				}
				got := idx.LookupFull(values...)
				if len(got) != 1 || got[0] != "test:kept" {
					t.Fatalf("live index changed: %v", got)
				}
				prefix := idx.LookupPrefix(1)
				if len(prefix) != 1 || prefix[0] != "test:kept" {
					t.Fatalf("live prefix changed: %v", prefix)
				}
				var bucket []NodeID
				switch owner {
				case "values":
					for _, ids := range idx.values {
						bucket = ids
					}
				case "fullIndex":
					for _, ids := range idx.fullIndex {
						bucket = ids
					}
				case "prefixIndex":
					for _, ids := range idx.prefixIndex {
						bucket = ids
						break
					}
				}
				if len(bucket) != 1 || bucket[0] != "test:kept" {
					t.Fatalf("surviving bucket changed: %v", bucket)
				}
				// Keep only this owner alive, so each bucket has its own GC proof.
				return ref, bucket
			}()
			runtime.GC()
			if ref.Value() != nil {
				t.Error("removed ID retained in index slice backing array")
			}
			runtime.KeepAlive(bucket)
		})
	}
}
