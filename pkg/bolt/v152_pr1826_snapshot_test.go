package bolt

// v1.5.2 verification (scratch, test-only): TestManyChunkNodeCommitsAndIsCompacted
// with the file it reads named. "any" selects the newest snapshot-* file, as the
// test did before #1945 (unfinished .json.tmp files included); "published"
// selects the newest snapshot-*.json, the population recovery reads. After a
// failed read the published snapshot, the live node and the reopened store are
// checked, so a failure says what became of the data.

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func p26NewestSnapshot(t *testing.T, root, pattern string) string {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(root, "snapshots", pattern))
	require.NoError(t, err)
	if len(files) == 0 {
		return ""
	}
	return files[len(files)-1]
}

func p26ReadSnapshot(t *testing.T, path string, want [][]float32) (found int, size int64, head string, err error) {
	file, err := os.Open(path)
	if err != nil {
		return 0, 0, "", err
	}
	defer file.Close()
	if info, statErr := file.Stat(); statErr == nil {
		size = info.Size()
	}
	var first [8]byte
	n, _ := file.ReadAt(first[:], 0)
	head = fmt.Sprintf("%q", first[:n])
	proof := manyChunkSnapshotProof{t: t, want: want}
	_, err = storage.ReadSnapshot(t.Context(), file, &proof)
	return proof.found, size, head, err
}

func TestV152P26ManyChunkSnapshot(t *testing.T) {
	const chunks = 6600
	for _, selection := range []string{"any", "published"} {
		t.Run(selection, func(t *testing.T) {
			pattern := map[string]string{"any": "snapshot-*", "published": "snapshot-*.json"}[selection]
			config := journalConfig()
			config.Database.WALAutoCompactionEnabled = true
			config.Database.WALSnapshotInterval = 2 * time.Second
			lab := journalOpen(t, t.TempDir(), config)
			lab.mustRun("CREATE (:Many {id: 'many', text: 'alpha', flag: false})", nil)
			lab.embed("Many", chunks)
			lab.requireVectors("Many", 1, chunks)
			newest := func() string { return p26NewestSnapshot(t, lab.root, pattern) }
			before := newest()
			_, err := lab.run("MATCH (n:Many {id: 'many'}) SET n.flag = true", nil, true)
			require.NoError(t, err)
			require.Eventually(t, func() bool { return newest() != before }, 30*time.Second, 250*time.Millisecond)
			require.Eventually(t, func() bool {
				_, largest := lab.journalSince(0)
				return largest == 0
			}, 30*time.Second, 250*time.Millisecond)
			want := journalVectors(chunks, 0)
			selected := newest()
			found, size, head, readErr := p26ReadSnapshot(t, selected, want)
			t.Logf("P26ROW SNAP selection=%s read %s: size at open %d bytes, first bytes %s, node found %d, error: %v", selection, filepath.Base(selected), size, head, found, readErr)
			if readErr != nil || found != 1 {
				published := p26NewestSnapshot(t, lab.root, "snapshot-*.json")
				pFound, pSize, pHead, pErr := p26ReadSnapshot(t, published, want)
				t.Logf("P26ROW SNAP after the failed read: newest published %s: size %d, first bytes %s, node with all %d vectors found %d, error: %v", filepath.Base(published), pSize, pHead, chunks, pFound, pErr)
				t.Errorf("P26 SNAP selection=%s: reading %s (unfinished file: %t) fails: %v", selection, filepath.Base(selected), strings.HasSuffix(selected, ".tmp"), readErr)
				if pErr != nil || pFound != 1 {
					t.Errorf("P26 SNAPLOSS the published snapshot %s does not hold the node: found %d, %v", filepath.Base(published), pFound, pErr)
				}
			}
			check := func(stage string) {
				require.Equal(t, [][]any{{true}}, lab.mustRun("MATCH (n:Many {id: 'many'}) RETURN n.flag", nil), stage)
				lab.requireVectors("Many", 1, chunks)
			}
			check("commit")
			lab = lab.reopen(config)
			check("restart")
			lab.requireNodeMeta("Many", chunks)
			t.Logf("P26ROW SNAP selection=%s: node, flag and all %d vectors read before and after the restart (read had failed: %t)", selection, chunks, readErr != nil)
		})
	}
}
