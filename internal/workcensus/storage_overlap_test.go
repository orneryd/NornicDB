package workcensus

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// Author: #1667 implementation lane (Codex). This discovery client supplies git
// snapshots as data to the same syntax-only engine. It neither parses/walks Go
// nor classifies a function from its name. The output is review evidence.
func TestStorageRetainedOwnerDiscovery(t *testing.T) {
	if flag.Lookup("test.run").Value.String() != "^TestStorageRetainedOwnerDiscovery$" {
		t.Skip("explicit owner-discovery selector required")
	}
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	git := func(args ...string) []byte {
		t.Helper()
		command := exec.Command("git", args...)
		command.Dir = root
		out, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v: %v: %s", args, err, out)
		}
		return out
	}
	changed := func(base, head string) map[string]bool {
		out := map[string]bool{}
		for _, name := range strings.Fields(string(git("diff", "--name-only", base, head, "--", "*.go", ":!*_test.go"))) {
			out[name] = true
		}
		return out
	}
	lane := changed("HEAD^", "HEAD")
	for _, layer := range []struct{ name, base, head string }{
		{"#1406", "fc48d85d1^", "fc48d85d1"},
		{"#1503", "fc48d85d1", "d663ce8f1"},
		{"#1636", "d663ce8f1", "HEAD^"},
	} {
		discoverRetainedLayer(t, git, changed(layer.base, layer.head), lane, layer.name, layer.base, layer.head)
	}
	t.Fatal("owner intersections are review evidence, never semantic approval")
}

func discoverRetainedLayer(t *testing.T, git func(...string) []byte, maintainer, lane map[string]bool, layer, beforeRevision, afterRevision string) {
	t.Helper()
	var files []string
	for name := range maintainer {
		if lane[name] {
			files = append(files, name)
		}
	}
	sort.Strings(files)
	// Existence is source data too: a deletion must not be reconstructed from
	// a similarly named function. git ls-tree distinguishes it from read errors.
	snapshot := func(revision string) map[string]Member {
		t.Helper()
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "go.mod"), []byte("module "+strings.TrimSuffix(module, "/")+"\n\ngo 1.26\n"), 0600); err != nil {
			t.Fatal(err)
		}
		for _, file := range files {
			if len(git("ls-tree", "--name-only", revision, "--", file)) == 0 {
				continue
			}
			path := filepath.Join(dir, filepath.FromSlash(file))
			if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, git("show", revision+":"+file), 0600); err != nil {
				t.Fatal(err)
			}
		}
		inventory, err := Extract(Config{Dir: dir, SourceRoots: []SourceRoot{{Path: ".", Languages: map[string]string{".go": "go"}}}, Selectors: []Selector{{Kind: "owner", Node: "declaration", Reachability: "all"}}})
		if err != nil {
			t.Fatal(err)
		}
		out := map[string]Member{}
		for _, member := range inventory.Members {
			if member.Declaration == "function" || member.Declaration == "method" {
				out[member.ID] = member
			}
		}
		return out
	}
	before := snapshot(beforeRevision)
	after := snapshot(afterRevision)
	base := snapshot("d663ce8f1")
	parent := snapshot("HEAD^")
	current := snapshot("HEAD")
	identities := map[string]bool{}
	for id := range before {
		identities[id] = true
	}
	for id := range after {
		identities[id] = true
	}
	var ids []string
	for id := range identities {
		if before[id].Digest != after[id].Digest {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	for _, id := range ids {
		row := struct {
			Author, Layer, ID                       string
			Before, After, Release, Parent, Current Member
			LaneChanged                             bool
		}{"#1667 implementation lane (Codex): exact engine facts, no semantic disposition", layer, id, before[id], after[id], base[id], parent[id], current[id], parent[id].Digest != current[id].Digest}
		encoded, err := json.Marshal(row)
		if err != nil {
			t.Fatal(err)
		}
		fmt.Println("OWNER_INTERSECTION " + string(encoded))
	}
	fmt.Printf("OWNER_INTERSECTION_COUNT layer=%s files=%d owner-functions=%d\n", layer, len(files), len(ids))
}
