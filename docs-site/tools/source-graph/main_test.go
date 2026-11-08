package main

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestCallsRespectLexicalShadowing(t *testing.T) {
	root := t.TempDir()
	for file, source := range map[string]string{
		"pkg/a/a.go": "package a\nfunc Target() {}\nfunc Direct() { Target() }\nfunc Shadow() { Target := func() {}; Target() }\n",
		"pkg/b/b.go": "package b\nimport target \"github.com/orneryd/nornicdb/pkg/a\"\nfunc Direct() { target.Target() }\nfunc Shadow() { target := struct{Target func()}{Target:func(){}}; target.Target() }\n",
	} {
		destination := filepath.Join(root, file)
		if err := os.MkdirAll(filepath.Dir(destination), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(destination, []byte(source), 0644); err != nil {
			t.Fatal(err)
		}
	}
	output, err := exec.Command("go", "run", "main.go", root).CombinedOutput()
	if err != nil {
		t.Fatalf("extract: %v\n%s", err, output)
	}
	var graph Graph
	if err = json.Unmarshal(output, &graph); err != nil {
		t.Fatal(err)
	}
	calls := map[string]bool{}
	for _, edge := range graph.Links {
		calls[edge.Source+"->"+edge.Target] = true
	}
	for _, source := range []string{"pkg/a/a.go:Direct", "pkg/b/b.go:Direct"} {
		if !calls[source+"->pkg/a/a.go:Target"] {
			t.Errorf("missing package-resolved call from %s", source)
		}
	}
	for _, edge := range graph.Links {
		if edge.Source == "pkg/a/a.go:Shadow" || edge.Source == "pkg/b/b.go:Shadow" {
			t.Errorf("local binding was incorrectly linked to a package declaration: %+v", edge)
		}
	}
}

func TestRepeatedInitDeclarationsHaveUniqueIDs(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "pkg", "a")
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "a.go"), []byte("package a\nfunc Target() {}\nfunc init() { Target() }\nfunc init() { Target() }\n"), 0644); err != nil {
		t.Fatal(err)
	}
	output, err := exec.Command("go", "run", "main.go", root).CombinedOutput()
	if err != nil {
		t.Fatalf("extract: %v %s", err, output)
	}
	var graph Graph
	if err := json.Unmarshal(output, &graph); err != nil {
		t.Fatal(err)
	}
	ids := map[string]bool{}
	for _, node := range graph.Nodes {
		if ids[node.ID] {
			t.Errorf("duplicate symbol ID: %s", node.ID)
		}
		ids[node.ID] = true
	}
	if len(graph.Links) != 2 {
		t.Errorf("expected calls from both init declarations; got %d", len(graph.Links))
	}
}

func TestBuildVariantsAreNotArbitrarilySelected(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "pkg", "a")
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}
	for file, source := range map[string]string{"a_windows.go": "package a\nfunc Target() {}\nfunc FromWindows(){Target()}\n", "a_linux.go": "package a\nfunc Target() {}\nfunc FromLinux(){Target()}\n", "shared.go": "package a\nfunc Across(){Target()}\n"} {
		if err := os.WriteFile(filepath.Join(dir, file), []byte(source), 0644); err != nil {
			t.Fatal(err)
		}
	}
	output, err := exec.Command("go", "run", "main.go", root).CombinedOutput()
	if err != nil {
		t.Fatalf("extract: %v %s", err, output)
	}
	var graph Graph
	if err := json.Unmarshal(output, &graph); err != nil {
		t.Fatal(err)
	}
	for _, edge := range graph.Links {
		if edge.Source == "pkg/a/shared.go:Across" {
			t.Error("selected a build variant without build context")
		}
		if edge.Source == "pkg/a/a_linux.go:FromLinux" && edge.Target != "pkg/a/a_linux.go:Target" {
			t.Errorf("wrong file binding: %+v", edge)
		}
	}
}
