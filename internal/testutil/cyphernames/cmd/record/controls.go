package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
)

// The same test symbol passes in both executions. Only the production call is
// removed. Coverage is then generated against the retained original census.
func removalControl(dir string) string {
	fixture, err := os.MkdirTemp("internal", "emissionrecordercontrol")
	must(err)
	defer os.RemoveAll(fixture)
	production := "package recordercontrol\nimport \"github.com/orneryd/nornicdb/internal/cyphertext\"\nfunc Emit() string { return cyphertext.QuoteName(\"space \", \"key\") }\n"
	test := "package recordercontrol\nimport \"testing\"\nfunc TestRetainedSubject(t *testing.T) { if Emit()==\"\" { t.Fatal(\"empty\") } }\n"
	must(os.WriteFile(filepath.Join(fixture, "emitter.go"), []byte(production), 0600))
	must(os.WriteFile(filepath.Join(fixture, "emitter_test.go"), []byte(test), 0600))
	before, after := filepath.Join(dir, "control-before.jsonl"), filepath.Join(dir, "control-after.jsonl")
	args := []string{"test", "-tags=noui,nolocalllm,cypheremission", "./" + filepath.ToSlash(fixture), "-run", "^TestRetainedSubject$", "-count=1"}
	must(run(append(os.Environ(), "NORNIC_NAME_RECORDING="+before), "go", args...))
	mutant := "package recordercontrol\nfunc Emit() string { return \"retained test without production emission\" }\n"
	must(os.WriteFile(filepath.Join(fixture, "emitter.go"), []byte(mutant), 0600))
	must(os.WriteFile(after, nil, 0600))
	must(run(append(os.Environ(), "NORNIC_NAME_RECORDING="+after), "go", args...))
	baseline, err := os.ReadFile(before)
	must(err)
	removed, err := os.ReadFile(after)
	must(err)
	row := site{"go", filepath.ToSlash(filepath.Join(fixture, "emitter.go")), "Emit", "QuoteName", "cyphertext.QuoteName(\"space \", \"key\")", 3, 3}
	path := filepath.Join(dir, "control.json")
	data, err := json.Marshal(map[string]any{"site": row, "before": strings.Split(strings.TrimSpace(string(baseline)), "\n"), "after": strings.Split(strings.TrimSpace(string(removed)), "\n"), "test_symbol": "TestRetainedSubject"})
	must(err)
	must(os.WriteFile(path, data, 0600))
	return path
}

func peerRemovalControls(dir string) []string {
	fixture, err := os.MkdirTemp("scripts", "emissionrecordercontrol")
	must(err)
	defer os.RemoveAll(fixture)
	var paths []string
	for _, peer := range []struct{ runtime, extension, header, call, test string }{
		{"node", "mjs", "import {quoteName} from '../../internal/testutil/cyphernames/peer_recording.mjs';\n", "function Emit() { return quoteName('space ', 'key'); }\n", "function TestRetainedSubject() { if (!Emit()) throw Error('empty'); }\nTestRetainedSubject();\n"},
		{"python3", "py", "import sys,os\nsys.path.insert(0,os.getcwd())\nfrom internal.testutil.cyphernames.peer_recording import install\ninstall()\nfrom scripts.migration.cypher_names import quote_name\n", "def Emit(): return quote_name('space ', 'key')\n", "def TestRetainedSubject(): assert Emit()\nTestRetainedSubject()\n"},
	} {
		file := filepath.Join(fixture, "emitter."+peer.extension)
		before, after := filepath.Join(dir, peer.extension+"-before.jsonl"), filepath.Join(dir, peer.extension+"-after.jsonl")
		must(os.WriteFile(file, []byte(peer.header+peer.call+peer.test), 0600))
		must(run(append(os.Environ(), "NORNIC_NAME_RECORDING="+before), peer.runtime, file))
		mutant := "function Emit() { return 'retained'; }\n"
		runtime := "js"
		if peer.extension == "py" {
			mutant = "def Emit(): return 'retained'\n"
			runtime = "python"
		}
		must(os.WriteFile(file, []byte(peer.header+mutant+peer.test), 0600))
		must(os.WriteFile(after, nil, 0600))
		must(run(append(os.Environ(), "NORNIC_NAME_RECORDING="+after), peer.runtime, file))
		baseline, err := os.ReadFile(before)
		must(err)
		removed, err := os.ReadFile(after)
		must(err)
		line := strings.Count(peer.header, "\n") + 1
		row := site{runtime, filepath.ToSlash(file), "Emit", "quoteName", strings.TrimSpace(peer.call), line, line}
		path := filepath.Join(dir, peer.extension+"-control.json")
		data, err := json.Marshal(map[string]any{"site": row, "before": strings.Split(strings.TrimSpace(string(baseline)), "\n"), "after": strings.Split(strings.TrimSpace(string(removed)), "\n"), "test_symbol": "TestRetainedSubject"})
		must(err)
		must(os.WriteFile(path, data, 0600))
		paths = append(paths, path)
	}
	return paths
}
