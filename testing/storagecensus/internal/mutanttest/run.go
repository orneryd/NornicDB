package mutanttest

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/testing/storagecensus"
	"github.com/stretchr/testify/require"
)

// partitionCount is the number of partitions the mutant matrix is split into:
// one test package under testing/storagecensus/mutants each.
const partitionCount = 4

// partitionOf returns the partition that runs the mutant at index.
func partitionOf(index int) int { return index % partitionCount }

// Run applies each mutant assigned to partition to the module under root as an
// overlay and requires the census to report the violation it introduces, for
// the intended reason. It censuses the unchanged module once and then runs one
// full census per mutant of its partition, sequentially.
//
// partition is zero-based in 0..partitionCount-1; anything else fails without
// running a census. Each partition runs exactly the cases whose index in the
// shared matrix has its remainder, so the four packages together apply the
// whole matrix once. A short test run is skipped: every mutant costs a full
// census.
func Run(t *testing.T, root string, partition int) {
	t.Helper()
	if partition < 0 || partition >= partitionCount {
		t.Fatalf("partition %d out of range 0..%d", partition, partitionCount-1)
	}
	if testing.Short() {
		t.Skip("one full census per mutant")
	}
	abs, err := filepath.Abs(root)
	require.NoError(t, err)
	base, err := storagecensus.Census(root, nil)
	require.NoError(t, err)
	require.Empty(t, base.Violations)
	for i, m := range mutants {
		if partitionOf(i) != partition {
			continue
		}
		t.Run(m.name, func(t *testing.T) {
			path := filepath.Join(abs, filepath.FromSlash(m.file))
			src := []byte(m.src)
			if m.src == "" {
				orig, err := os.ReadFile(path)
				require.NoError(t, err)
				text := strings.ReplaceAll(string(orig), "\r\n", "\n")
				for _, e := range m.edits {
					require.Equal(t, 1, strings.Count(text, e[0]), "mutation anchor in %s", m.file)
					text = strings.Replace(text, e[0], e[1], 1)
				}
				src = []byte(text)
			}
			r, err := storagecensus.Census(root, map[string][]byte{path: src})
			require.NoError(t, err)
			// The violation must be the mutant's: absent from the census of
			// the unchanged module.
			inBase := map[string]bool{}
			for _, v := range base.Violations {
				inBase[v] = true
			}
			baseRow := func(u storagecensus.UnorderedRow) bool {
				for _, b := range base.Unordered {
					if b.Origin == u.Origin && b.Space == u.Space && slices.Contains(b.Entries, m.entry) {
						return true
					}
				}
				return false
			}
			found := false
			if m.entry != "" {
				// An unordered write the entry point reaches, of the space wanted.
				for _, u := range r.Unordered {
					found = found || (slices.Contains(u.Entries, m.entry) && strings.Contains(u.Space, m.want[len(m.want)-1]) && !baseRow(u))
				}
			} else {
				for _, v := range r.Violations {
					all := !inBase[v]
					for _, w := range m.want {
						all = all && strings.Contains(v, w)
					}
					found = found || all
				}
			}
			require.True(t, found, "the census did not reject the mutant %q for its reason %q; violations:\n  %s", m.name, m.want, strings.Join(r.Violations, "\n  "))
		})
	}
}
