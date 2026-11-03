//go:build !cgo

package workcensus

import (
	"strings"
	"testing"
)

func TestCensusLanguageRequiresCGO(t *testing.T) {
	_, err := Extract(Config{Dir: t.TempDir(), Documents: []Document{{Path: "peer.ts", Language: "typescript"}}, Selectors: []Selector{{Kind: "peer", Node: "function", Reachability: "all"}}})
	if err == nil || !strings.Contains(err.Error(), "CGO-enabled census") {
		t.Fatalf("language parser did not explicitly refuse unavailable grammar: %v", err)
	}
}
