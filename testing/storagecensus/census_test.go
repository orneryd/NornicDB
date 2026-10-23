package storagecensus

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// root is the module root, two directories above this package.
var root = filepath.Join("..", "..")

var (
	censusOnce   sync.Once
	censusReport *Report
	censusErr    error
)

func census(t *testing.T) *Report {
	t.Helper()
	censusOnce.Do(func() { censusReport, censusErr = Census(root, nil) })
	require.NoError(t, censusErr)
	return censusReport
}

const artifact = "docs/generated/stored-entity-census"

// TestStoredEntityCensusIsCurrent regenerates the census and compares it with
// the committed artifact. NORNICDB_CENSUS_UPDATE=1 writes it instead;
// NORNICDB_CENSUS_PRINT=1 also prints both forms.
func TestStoredEntityCensusIsCurrent(t *testing.T) {
	r := census(t)
	js, err := r.JSON()
	require.NoError(t, err)
	md := r.Markdown()
	if os.Getenv("NORNICDB_CENSUS_PRINT") != "" {
		fmt.Printf("CENSUS-MD-BEGIN\n%sCENSUS-MD-END\n", md)
		fmt.Printf("CENSUS-JSON-BEGIN\n%sCENSUS-JSON-END\n", js)
	}
	if os.Getenv("NORNICDB_CENSUS_CHAINS") != "" {
		// Per unordered write, the distinct paths below their entry points.
		for _, u := range r.Unordered {
			paths := map[string][]string{}
			for entry, chain := range u.Chains {
				below := strings.Join(chain[1:], " -> ")
				paths[below] = append(paths[below], entry)
			}
			fmt.Printf("CHAINS %s %s (%d paths)\n", u.Origin, u.Space, len(paths))
			for _, below := range sortedKeys(paths) {
				fmt.Printf("  %d via %s: %s\n", len(paths[below]), paths[below][0], below)
			}
		}
	}
	for ext, want := range map[string][]byte{".json": js, ".md": md} {
		path := filepath.Join(root, filepath.FromSlash(artifact+ext))
		if os.Getenv("NORNICDB_CENSUS_UPDATE") != "" {
			require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
			require.NoError(t, os.WriteFile(path, want, 0o644))
			continue
		}
		got, err := os.ReadFile(path)
		require.NoError(t, err, "the census artifact is missing: regenerate it")
		got = bytes.ReplaceAll(got, []byte("\r\n"), []byte("\n"))
		require.True(t, bytes.Equal(got, want), "%s is stale: regenerate it (NORNICDB_CENSUS_UPDATE=1)", artifact+ext)
	}
}

// TestStoredEntityWritesAreOrdered fails on a write of ordered state an
// entry point reaches with no acquisition, on a write whose key the census
// cannot classify, and on table rows that no longer match the code.
func TestStoredEntityWritesAreOrdered(t *testing.T) {
	r := census(t)
	require.Empty(t, r.Violations, "census violations:\n  %s", strings.Join(r.Violations, "\n  "))
}
