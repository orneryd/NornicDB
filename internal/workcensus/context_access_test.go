package workcensus

// Author: #1667 implementation lane (Codex).

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestContextAccessIgnoresDiscardedBindings(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "go.mod"), []byte("module context.fixture\n\ngo 1.26\n"), 0600); err != nil {
		t.Fatal(err)
	}
	raw := `package sample
import "context"
type holder struct { ctx context.Context }
func discard(_ context.Context) { for range []int{1} {} }
func unnamed(context.Context) { for range []int{1} {} }
func (_ *holder) DiscardReceiver() { for range []int{1} {} }
func (*holder) UnnamedReceiver() { for range []int{1} {} }
func Execute(ctx context.Context) {
 discard(ctx); unnamed(ctx)
 h:=&holder{ctx:ctx}; h.DiscardReceiver(); h.UnnamedReceiver()
 callback:=func(_ context.Context) { for range []int{1} {} }
 callback(ctx)
}
`
	if err := os.WriteFile(filepath.Join(dir, "source.go"), []byte(raw), 0600); err != nil {
		t.Fatal(err)
	}
	inv := mustExtract(t, Config{Dir: dir, Patterns: []string{"./..."}, Roots: []Root{{Package: "context.fixture", Name: "Execute", Required: true}}, ContextFields: []string{"ctx"}, Selectors: []Selector{{Kind: "range", Node: "range"}}})
	if len(inv.Members) != 5 {
		t.Fatalf("want five reachable contexts, got %d", len(inv.Members))
	}
	for _, m := range inv.Members {
		want := ""
		if strings.Contains(m.Owner, ".Execute$") {
			want = "ctx"
		}
		if m.Scope != want {
			t.Errorf("%s: accessible context %q, want %q", m.Owner, m.Scope, want)
		}
	}
}
