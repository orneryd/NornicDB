package capturecensus

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/internal/workcensus"
	"github.com/stretchr/testify/require"
)

func TestSourceDigestUsesCompiledOverlay(t *testing.T) {
	const original = "package fixture\nfunc owned() int { return 1 }\n"
	c, _ := fixture(t, original)
	const symbol = "capture.fixture.owned"
	load := func(c workcensus.Config) *workcensus.Program {
		t.Helper()
		p, err := workcensus.LoadProgram(c)
		require.NoError(t, err)
		return p
	}
	digest := func(body string) string {
		sum := sha256.Sum256([]byte(body))
		return hex.EncodeToString(sum[:])
	}
	loadedDisk := load(c)
	baseline := newSourceDigests(loadedDisk).of(symbol)
	require.Equal(t, digest("func owned() int { return 1 }"), baseline)
	t.Run("disk changed after compilation", func(t *testing.T) {
		path := filepath.Join(c.Dir, "owner.go")
		require.NoError(t, os.WriteFile(path, []byte(strings.Replace(original, "return 1", "return 2", 1)), 0600))
		defer func() { require.NoError(t, os.WriteFile(path, []byte(original), 0600)) }()
		require.Equal(t, baseline, newSourceDigests(loadedDisk).of(symbol))
	})
	for _, replacement := range []string{"2", "123456789"} {
		t.Run(replacement, func(t *testing.T) {
			changed := strings.Replace(original, "return 1", "return "+replacement, 1)
			overlay := []byte(changed)
			c.Overlay = map[string][]byte{filepath.Join(c.Dir, "owner.go"): overlay}
			p := load(c)
			// Loading owns a snapshot: caller edits cannot change what was compiled.
			copy(overlay, []byte(original))
			delete(c.Overlay, filepath.Join(c.Dir, "owner.go"))
			got := newSourceDigests(p).of(symbol)
			require.Equal(t, digest("func owned() int { return "+replacement+" }"), got)
			require.NotEqual(t, baseline, got, "changed compiled bodies must invalidate a disk-source review")
		})
	}
}
