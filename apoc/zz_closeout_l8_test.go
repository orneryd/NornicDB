package apoc

// Close-out lane L8, round 3 (2026-11-22): the procedure part of hunt lead
// L-H9-018. docs/features/apoc-functions.md:6 says most apoc.* names come
// from the optional APOC plugin (apoc.so, built from this package), not from
// the server. Every documented name the server does not answer is looked up
// in this package's registry (Initialize with the default configuration,
// List) and in the plugin's export list. Also: the Qdrant migration script
// the skill page runs (docs/skills/qdrant-migration.skill.md:110).
// Test-only, not for merge.

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

func TestL8_H9_018_PluginRegistry(t *testing.T) {
	if err := Initialize(nil, nil); err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	registered := map[string]bool{}
	for _, name := range List() {
		registered[name] = true
	}
	exported := map[string]bool{}
	for _, name := range PluginFunctionNames() {
		exported[name] = true
	}
	file, err := os.Open(filepath.Join("..", "pkg", "server", "testdata", "l8", "h9-leads-raw-r1.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 1<<20), 1<<20)
	counts := map[string]int{}
	absent := []string{}
	for scanner.Scan() {
		cols := strings.Split(scanner.Text(), "\t")
		if len(cols) < 5 || cols[2] != "procedure" {
			continue
		}
		name := cols[3]
		parts := strings.Split(name, ".")
		if last := parts[len(parts)-1]; last != "" && last[0] >= 'A' && last[0] <= 'Z' {
			continue
		}
		class := "in neither the plugin registry nor the server"
		switch {
		case registered[name] && exported[name]:
			class = "plugin registry and plugin function export"
		case registered[name]:
			class = "plugin registry (procedure or built-in name)"
		case IsBuiltInFunction(name):
			class = "native built-in name"
		}
		if class == "in neither the plugin registry nor the server" {
			absent = append(absent, name)
		}
		counts[class]++
		data, _ := json.Marshal(map[string]any{"lead": "L-H9-018", "item": cols[1], "name": name, "documented": cols[4], "class": class})
		t.Logf("L8_PLUGIN %s", data)
	}
	sort.Strings(absent)
	data, _ := json.Marshal(map[string]any{"lead": "L-H9-018", "counts": counts, "absent": absent, "registry_size": len(registered)})
	t.Logf("L8_PLUGIN_SUMMARY %s", data)
	for _, script := range []string{"scripts/migration/qdrant/migrate.py", "scripts/migration/qdrant/migrate.go", "scripts/migration/neo4j/migrate.py"} {
		_, err := os.Stat(filepath.Join("..", script))
		data, _ := json.Marshal(map[string]any{"lead": "L-H9-018", "script": script, "exists": err == nil})
		t.Logf("L8_SCRIPT %s", data)
	}
}
