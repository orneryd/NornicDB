// Read-4 run for M07 (v1.5.2 verification campaign, 23 Nov 2026). Test-only, not
// for merge. The same construction as TestVerifyH1_001_NativeBackupRestores,
// but the backup chosen is one whose very first byte is '[' (0x5b) instead of
// '{'. Every observation is logged as one "VERIFY_H1_BRACKET" line.
package server

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVerifyH1_001b_NativeBackupBracketRestores(t *testing.T) {
	base := t.TempDir()
	src := verifyH1Open(t, filepath.Join(base, "src"), false)
	ctx := context.Background()
	hit := ""
	firstBytes := map[byte]int{}
	for i := 0; i < 3000 && hit == ""; i++ {
		_, err := src.db.ExecuteCypher(ctx, "MERGE (p:Pad {k: 1}) SET p.s = $s", map[string]interface{}{"s": strings.Repeat("x", i)})
		require.NoError(t, err)
		path := filepath.Join(base, fmt.Sprintf("backup-%d.bak", i))
		require.NoError(t, src.db.Backup(ctx, path), "backup %d", i)
		data, err := os.ReadFile(path)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(data), 8)
		firstBytes[data[0]]++
		if data[0] == '[' {
			hit = path
			t.Logf("VERIFY_H1_BRACKET backup %d: size=%d first8=% x firstRecordLen(uint64 LE)=%d", i, len(data), data[:8], binary.LittleEndian.Uint64(data[:8]))
		} else {
			_ = os.Remove(path)
		}
	}
	t.Logf("VERIFY_H1_BRACKET distinct first bytes seen=%d '{'=%d '['=%d", len(firstBytes), firstBytes['{'], firstBytes['['])
	require.NotEmpty(t, hit, "no backup in 3000 starts with '['")
	want := src.mustRun("MATCH (p:Pad) RETURN size(p.s)").Rows

	native := verifyH1Open(t, filepath.Join(base, "native"), false)
	restorer, ok := native.db.GetBaseStorageForManager().(interface{ Restore(string) error })
	require.True(t, ok, "base storage has a native Restore")
	nativeErr := restorer.Restore(hit)
	t.Logf("VERIFY_H1_BRACKET engine native restore of the same file: err=%v", nativeErr)
	if nativeErr == nil {
		t.Logf("VERIFY_H1_BRACKET engine native restore: Pad rows %v (source %v)", native.mustRun("MATCH (p:Pad) RETURN size(p.s)").Rows, want)
	}

	target := verifyH1Open(t, filepath.Join(base, "target"), false)
	err := target.db.Restore(context.Background(), hit)
	t.Logf("VERIFY_H1_BRACKET DB.Restore of a native backup starting with '[': err=%v", err)
	require.NoError(t, nativeErr, "the file is a valid native backup")
	require.NoError(t, err, "DB.Restore of a native backup written without error")
	assert.Equal(t, want, target.mustRun("MATCH (p:Pad) RETURN size(p.s)").Rows)
}
