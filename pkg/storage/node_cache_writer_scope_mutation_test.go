package storage

import (
	"os"
	"strings"
	"testing"
)

// Check references as well as calls: assigning a method value must not hide
// an unscoped durable entrance from the same contract.
func writerScopeCallViolations(name string, source []byte) []string {
	return durableWriterViolations(name, source)
}

func TestNodeCacheWriterScopeCalls(t *testing.T) {
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
		for _, problem := range writerScopeCallViolations(name, source) {
			t.Error(problem)
		}
	}
}

func TestNodeCacheWriterScopeGuardMutation(t *testing.T) {
	source, err := os.ReadFile("badger_edges.go")
	if err != nil {
		t.Fatal(err)
	}
	start := strings.Index(string(source), "func (b *BadgerEngine) DeleteEdge(")
	if start < 0 {
		t.Fatal("edge delete entrance missing")
	}
	prefix, writer := string(source[:start]), string(source[start:])
	if !strings.Contains(writer, "relationshipUpdateCacheWrites(nil, &deletedEdge), ") {
		t.Fatal("edge delete declaration missing")
	}
	mutated := prefix + strings.Replace(writer, "relationshipUpdateCacheWrites(nil, &deletedEdge), ", "", 1)
	if len(writerScopeCallViolations("badger_edges.go", []byte(mutated))) == 0 {
		t.Fatal("omitting a known writer scope passed the static guard")
	}
	if len(writerScopeCallViolations("mutation.go", []byte("package storage; func writer() { var scope nodeCacheWriteScope; _ = scope }"))) == 0 {
		t.Fatal("undeclared local scope passed the static guard")
	}
}
