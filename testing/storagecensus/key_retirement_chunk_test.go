package storagecensus

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// A bounded retirement walk receives the chunk of keys it deletes from a
// helper that scans them: keys, err := chunkFrom(...). The keys the delete
// writes are the keys the helper's returns hold, so the census classifies the
// delete by the scan the chunk comes from. This is the retirement path's own
// keys, not an allowance: the companion test below keeps a chunk the helper
// does not produce itself unclassified.
func TestChunkRetirementKeysAreTracedThroughTheScanThatProducesThem(t *testing.T) {
	r := census(t)
	spaces := map[string]bool{}
	for _, w := range r.Writes {
		if !strings.HasPrefix(w.Func, "pkg/storage.BadgerEngine.discardCountDeltaKeys$1 ") {
			continue
		}
		require.NotEqual(t, "unclassified", w.Class, "the retirement walk's delete must be classified: %+v", w)
		spaces[w.Space] = true
	}
	require.Contains(t, spaces, "prefixMVCCMetaLabelCountDelta",
		"the label count-delta retirement must be classified as the delta keys it scans")
	require.Contains(t, spaces, "prefixMVCCMetaEdgeTypeCountDelta",
		"the edge-type count-delta retirement must be classified as the delta keys it scans")
	for space := range spaces {
		require.Equal(t, "derived-count", spaceClasses[space], "retired count deltas remain derived counts: %s", space)
	}
}

// The trace above follows the helper's returns. A chunk the helper receives
// from across the module boundary has no provenance the census can bound, so
// the retirement walk's delete must remain an unclassified key: the trace
// resolves actual returns and does not accept every returned key slice.
func TestChunkRetirementKeepsForeignChunkKeysUnclassified(t *testing.T) {
	abs, err := filepath.Abs(root)
	require.NoError(t, err)
	path := filepath.Join(abs, "pkg/storage/badger_count_deltas.go")
	source, err := os.ReadFile(path)
	require.NoError(t, err)
	text := strings.Replace(string(source), "\treturn keys, nil\n}\n", "\treturn censusForeignChunk, nil\n}\n", 1)
	require.NotEqual(t, string(source), text, "the chunk reader's own return must be the mutation site")
	text += `
// censusForeignChunk stands in for a key chunk a reader receives from outside
// the module: the census cannot bound its provenance.
var censusForeignChunk [][]byte
`
	report, err := Census(root, map[string][]byte{path: []byte(text)})
	require.NoError(t, err)
	var why string
	for _, w := range report.Writes {
		if !strings.HasPrefix(w.Func, "pkg/storage.BadgerEngine.discardCountDeltaKeys$1 ") {
			continue
		}
		require.Equal(t, "unclassified", w.Class,
			"a chunk the helper does not produce itself must not be classified: %+v", w)
		why = w.Why
	}
	require.NotEmpty(t, why, "the retirement walk's unclassifiable delete must be reported")
	require.Contains(t, why, "slice of keys", "the report must say the returned slice has no traced provenance")
}
