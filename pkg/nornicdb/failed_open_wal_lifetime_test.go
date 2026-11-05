package nornicdb

import (
	"bytes"
	"github.com/stretchr/testify/require"
	"os"
	"path/filepath"
	"runtime/pprof"
	"strings"
	"testing"
	"time"
)

func openWALSyncWorkers() int {
	var stacks bytes.Buffer
	if err := pprof.Lookup("goroutine").WriteTo(&stacks, 2); err != nil {
		panic(err)
	}
	return strings.Count(stacks.String(), "github.com/orneryd/nornicdb/pkg/storage.(*WAL).batchSyncLoop(")
}

func TestFailedOpenReleasesWALAndRetry(t *testing.T) {
	before := openWALSyncWorkers()
	dir := t.TempDir()
	manifest := filepath.Join(dir, "wal", "segments", "manifest.json")
	require.NoError(t, os.MkdirAll(filepath.Dir(manifest), 0755))
	require.NoError(t, os.WriteFile(manifest, []byte("{broken"), 0644))
	cfg := DefaultConfig()
	cfg.Database.AsyncWritesEnabled = true
	cfg.Database.WALAutoCompactionEnabled = false
	for range 2 {
		db, err := Open(dir, cfg)
		if db != nil {
			require.NoError(t, db.Close())
		}
		require.Error(t, err)
		require.Nil(t, db)
		require.Eventually(t, func() bool { return before == openWALSyncWorkers() }, time.Second, time.Millisecond, "failed Open retained a WAL sync worker")
	}
	require.NoError(t, os.Remove(manifest))
	db, err := Open(dir, cfg)
	require.NoError(t, err)
	require.NoError(t, db.Close())
	require.Eventually(t, func() bool { return before == openWALSyncWorkers() }, time.Second, time.Millisecond, "successful retry retained workers from earlier failures")
}
