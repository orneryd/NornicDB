//go:build cgo

package workcensus

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestGenericCensusLanguagePeers(t *testing.T) {
	dir := t.TempDir()
	js := `const unicode = "😀";
const literal = "function fake() {}";
// function alsoFake() {}
export function first(q) { return q.split(","); }
const assigned = q => q.indexOf(":");
const obj = { method(q) { return /a+/.test(q); } };
class Reader { arbitrary(q) { return q.length; } }
`
	ts := `type Header = { columns: string[] };
const parse = (q: string): string[] => q.split(";");
export function fresh(q: string): Header { return {columns: parse(q)}; }
`
	tsx := `export const View = ({q}: {q: string}) => <span>{q}</span>;`
	for name, raw := range map[string]string{"peer.mjs": js, "peer.ts": ts, "peer.tsx": tsx} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(raw), 0600); err != nil {
			t.Fatal(err)
		}
	}
	c := Config{Dir: dir, Documents: []Document{{Path: "peer.mjs", Language: "javascript"}, {Path: "peer.ts", Language: "typescript"}, {Path: "peer.tsx", Language: "tsx"}}, Selectors: []Selector{{Kind: "peer", Node: "function", Reachability: "all"}, {Kind: "split", Node: "call", Names: []string{"split"}, Reachability: "all", Languages: []string{"javascript", "typescript"}}}}
	positive := mustExtract(t, c)
	approved := reviewed(positive)
	if failures := Validate(positive, approved, fixturePolicy); len(failures) > 0 {
		t.Fatal(failures)
	}
	functions := map[string]int{}
	utf16 := false
	splits := 0
	for _, m := range positive.Members {
		if m.Kind == "peer" {
			functions[m.Language]++
			if strings.Contains(m.Owner, "fake") || strings.Contains(m.Owner, "Fake") {
				t.Fatal("string/comment impersonated a function")
			}
			if m.Language == "javascript" && m.Offset > m.UTF16Offset {
				utf16 = true
			}
		}
		if m.Kind == "split" {
			splits++
		}
		if m.Typed || m.Reachable {
			t.Fatal("language peer fabricated Go type or SSA reachability")
		}
	}
	if functions["javascript"] != 4 || functions["typescript"] != 2 || functions["tsx"] != 1 || !utf16 || splits != 2 {
		t.Fatalf("peer population %v, UTF16 %v, split calls %d", functions, utf16, splits)
	}
	path := filepath.Join(dir, "peer.mjs")
	c.Overlay = map[string][]byte{path: []byte(js + `function censusPeerProbe(q) { for(let i=0;i<q.length;i++) if(q[i] === ':') return i; return -1; }`)}
	actual := mustExtract(t, c)
	failures := Validate(actual, approved, fixturePolicy)
	found := false
	for _, failure := range failures {
		found = found || (strings.HasPrefix(failure, "UNLISTED ") && strings.Contains(failure, "censusPeerProbe"))
	}
	if !found {
		t.Fatalf("new peer escaped shared extraction/validation: %v", failures)
	}
	c.Overlay = map[string][]byte{path: []byte("\n\n" + js)}
	// Named owners retain their membership names; anonymous owners have
	// positional identities and require fresh coordinates after movement.
	named := func(inv Inventory) Inventory {
		var members []Member
		for _, m := range inv.Members {
			if !strings.Contains(m.Owner, "@source:") {
				members = append(members, m)
			}
		}
		inv.Members = members
		return inv
	}
	if failures := Validate(named(mustExtract(t, c)), named(approved), fixturePolicy); len(failures) > 0 {
		t.Fatal("named peer identity used lines", failures)
	}
	c.Overlay = map[string][]byte{path: []byte("function invalid( {")}
	if _, err := Extract(c); err == nil {
		t.Fatal("invalid peer syntax silently passed")
	}
}
