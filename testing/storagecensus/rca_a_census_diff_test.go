package storagecensus

// Root-cause diagnostic (campaign v1.5.2, lane D, group G-test-1863-bookkeeping; run by lane A).
// Test-side only. Regenerates the census in memory and prints only the lines
// of docs/generated/stored-entity-census.json that differ from the committed
// file (removed '-' / added '+'), so the stale entries can be tied to the
// source change that made them stale. Writes nothing.

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRCACensusDiff(t *testing.T) {
	r := census(t)
	js, err := r.JSON()
	if err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(artifact+".json")))
	if err != nil {
		t.Fatal(err)
	}
	got = bytes.ReplaceAll(got, []byte("\r\n"), []byte("\n"))
	count := func(lines []string) map[string]int {
		m := map[string]int{}
		for _, l := range lines {
			m[strings.TrimSpace(l)]++
		}
		return m
	}
	committed, generated := count(strings.Split(string(got), "\n")), count(strings.Split(string(js), "\n"))
	shown := 0
	for l, n := range committed {
		if generated[l] < n && shown < 120 {
			t.Logf("RCA_CENSUS - %s", l)
			shown++
		}
	}
	for l, n := range generated {
		if committed[l] < n && shown < 240 {
			t.Logf("RCA_CENSUS + %s", l)
			shown++
		}
	}
	t.Logf("RCA_CENSUS committed_bytes=%d generated_bytes=%d differing_lines_shown=%d equal=%v", len(got), len(js), shown, bytes.Equal(got, js))
}
