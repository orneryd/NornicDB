// Package temporalguide runs the published temporal examples through test-owned
// query adapters. It contains no database implementation or production routing.
package temporalguide

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Example reads a named Cypher fence verbatim. The caller's package working
// directory must be two levels below the repository root (pkg/cypher, bolt or
// server). A removed, renamed or duplicated example fails its consuming test.
func Example(t testing.TB, guide, name string) string {
	t.Helper()
	path := filepath.Join("..", "..", "docs", "user-guides", guide+".md")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	marker := "<!-- temporal-example: " + name + " -->"
	text := string(data)
	if strings.Count(text, marker) != 1 {
		t.Fatalf("%s must contain exactly one %s", path, marker)
	}
	_, tail, _ := strings.Cut(text, marker)
	body, ok := strings.CutPrefix(strings.TrimSpace(tail), "```cypher\n")
	if !ok {
		t.Fatalf("%s must introduce a Cypher fence", marker)
	}
	query, _, ok := strings.Cut(body, "\n```")
	if !ok {
		t.Fatalf("%s has an unclosed Cypher fence", marker)
	}
	return query
}

// Runner executes one statement and returns all projected rows. Protocol tests
// keep their native client and error handling in the adapter.
type Runner func(string, map[string]interface{}) ([][]interface{}, error)
