package storage

import (
	"os"
	"strings"
	"testing"
)

// Writers declare data; the durable issuer alone interprets that declaration
// and changes cache eligibility. Keep new writer families on that same path.
func nodeCacheScopeOwnerViolations(name string, source []byte) []string {
	return durableWriterViolations(name, source)
}

func TestNodeCacheScopeSingleOwner(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		source, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		for _, problem := range nodeCacheScopeOwnerViolations(name, source) {
			t.Error(problem)
		}
	}
}

func TestNodeCacheScopeSingleOwnerMutation(t *testing.T) {
	for name, body := range map[string]string{
		"copied fence":       "b.nodeCacheInvalidationTs = ts",
		"copied issuance":    "b.nodeCacheCommitTs.Store(ts)",
		"fence bypass":       "b.fenceUnknownNodeCacheWritesLocked(ts)",
		"issuer bypass":      "b.cacheCommitNodeWrites(ts, noNodeCacheWrites())",
		"scope check bypass": "b.invalidateNodeCacheScopeLocked(noNodeCacheWrites(), ts)",
	} {
		t.Run(name, func(t *testing.T) {
			source := "package storage; func (b *BadgerEngine) writer(ts uint64) { " + body + " }"
			if len(nodeCacheScopeOwnerViolations("new_writer.go", []byte(source))) == 0 {
				t.Fatal("duplicate cache eligibility owner passed the static guard")
			}
		})
	}
}
