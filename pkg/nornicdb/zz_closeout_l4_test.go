package nornicdb

// v1.5.2 close-out lane L4 (application register rows) on the database API:
//   - H-385 / H-400: a clean stop is quick and the next start reports "Clean
//     shutdown detected" without rebuilding MVCC heads; a crash-consistent
//     copy (taken while the database runs) rebuilds heads and keeps the data;
//     with the MVCC lifecycle on and off.
//   - H-423 (c): database.mvcc_lifecycle_interval "0s" from YAML gives a
//     manual-only lifecycle: no automatic loop, a manual prune still works.
// Test file only; not for merge.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	nornicConfig "github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

func l4DBReport(t *testing.T, item, check string, pass bool, detail map[string]any) {
	t.Helper()
	detail["item"] = item
	detail["check"] = check
	detail["pass"] = pass
	data, _ := json.Marshal(detail)
	fmt.Fprintf(os.Stdout, "L4_RESULT %s\n", data)
	if !pass {
		t.Errorf("%s %s: expected behaviour does not hold: %s", item, check, data)
	}
}

type l4LogBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *l4LogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *l4LogBuffer) take() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	s := b.buf.String()
	b.buf.Reset()
	return s
}

func l4Config(lifecycle bool) *Config {
	cfg := DefaultConfig()
	cfg.Memory.EmbeddingEnabled = false
	cfg.Memory.AutoLinksEnabled = false
	cfg.Memory.DecayEnabled = false
	cfg.Database.MVCCLifecycleEnabled = lifecycle
	return cfg
}

func l4CopyDir(t *testing.T, from, to string) {
	t.Helper()
	require.NoError(t, filepath.Walk(from, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(from, path)
		target := filepath.Join(to, rel)
		if info.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		if strings.HasSuffix(path, "LOCK") {
			return nil
		}
		src, err := os.Open(path)
		if err != nil {
			return err
		}
		defer src.Close()
		dst, err := os.Create(target)
		if err != nil {
			return err
		}
		defer dst.Close()
		_, err = io.Copy(dst, src)
		return err
	}))
}

func TestCloseoutL4H385H400CleanStopAndCrashStart(t *testing.T) {
	logs := &l4LogBuffer{}
	previous := log.Writer()
	log.SetOutput(io.MultiWriter(previous, logs))
	t.Cleanup(func() { log.SetOutput(previous) })
	for _, lifecycle := range []bool{false, true} {
		name := map[bool]string{false: "lifecycle-off", true: "lifecycle-on"}[lifecycle]
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			db, err := Open(dir, l4Config(lifecycle))
			require.NoError(t, err)
			_, err = db.ExecuteCypher(ctx, "UNWIND range(1, 2000) AS k CREATE (:L4Stop {k: k})", nil)
			require.NoError(t, err)
			time.Sleep(2 * time.Second) // idle, as before a stop
			logs.take()
			start := time.Now()
			require.NoError(t, db.Close())
			stop := time.Since(start)

			db, err = Open(dir, l4Config(lifecycle))
			require.NoError(t, err)
			cleanStart := logs.take()
			rows, err := db.ExecuteCypher(ctx, "MATCH (n:L4Stop) RETURN count(n)", nil)
			require.NoError(t, err)
			clean := strings.Contains(cleanStart, "Clean shutdown detected") && !strings.Contains(cleanStart, "Rebuilding MVCC heads")
			l4DBReport(t, "H-400", "clean-stop/"+name, stop < 10*time.Second && clean && fmt.Sprint(rows.Rows) == "[[2000]]",
				map[string]any{"stopSeconds": stop.Seconds(), "cleanShutdownDetected": strings.Contains(cleanStart, "Clean shutdown detected"),
					"rebuiltHeads": strings.Contains(cleanStart, "Rebuilding MVCC heads"), "rows": fmt.Sprint(rows.Rows)})

			// Crash-consistent copy: taken while the database runs, after the
			// startup consumed its clean marker.
			_, err = db.ExecuteCypher(ctx, "UNWIND range(1, 500) AS k CREATE (:L4Crash {k: k})", nil)
			require.NoError(t, err)
			copyDir := t.TempDir()
			l4CopyDir(t, dir, copyDir)
			require.NoError(t, db.Close())
			logs.take()
			crashed, err := Open(copyDir, l4Config(lifecycle))
			crashStart := logs.take()
			detail := map[string]any{"openError": fmt.Sprint(err), "rebuiltHeads": strings.Contains(crashStart, "Rebuilding MVCC heads"),
				"cleanShutdownDetected": strings.Contains(crashStart, "Clean shutdown detected")}
			ok := err == nil && strings.Contains(crashStart, "Rebuilding MVCC heads") && !strings.Contains(crashStart, "Clean shutdown detected")
			if err == nil {
				before, _ := crashed.ExecuteCypher(ctx, "MATCH (n:L4Stop) RETURN count(n)", nil)
				after, _ := crashed.ExecuteCypher(ctx, "MATCH (n:L4Crash) RETURN count(n)", nil)
				if before != nil && after != nil {
					detail["stopRows"] = fmt.Sprint(before.Rows)
					detail["crashRows"] = fmt.Sprint(after.Rows)
					ok = ok && fmt.Sprint(before.Rows) == "[[2000]]" && fmt.Sprint(after.Rows) == "[[500]]"
				} else {
					ok = false
				}
				require.NoError(t, crashed.Close())
			}
			l4DBReport(t, "H-385", "crash-start-rebuilds/"+name, ok, detail)
		})
	}
}

func TestCloseoutL4H423ManualOnlyLifecycle(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte("database:\n  mvcc_lifecycle_interval: \"0s\"\n"), 0o600))
	loaded, err := nornicConfig.LoadFromFile(path)
	require.NoError(t, err)
	cfg := l4Config(true)
	cfg.Database.MVCCLifecycleCycleInterval = loaded.Database.MVCCLifecycleCycleInterval
	db, err := Open(filepath.Join(dir, "data"), cfg)
	require.NoError(t, err)
	defer db.Close()
	ctx := context.Background()
	_, err = db.ExecuteCypher(ctx, "CREATE (n:L4Life {v: 1}) SET n.v = 2 SET n.v = 3", nil)
	require.NoError(t, err)
	automatic := db.lifecycleManager != nil && db.lifecycleManager.automaticEnabled()
	status, statusErr := db.LifecycleStatus()
	pruneErr := db.TriggerLifecyclePrune(ctx)
	l4DBReport(t, "H-423", "manual-only-lifecycle", loaded.Database.MVCCLifecycleCycleInterval == 0 && db.lifecycleManager != nil && !automatic && statusErr == nil && pruneErr == nil,
		map[string]any{"interval": loaded.Database.MVCCLifecycleCycleInterval.String(), "managerBuilt": db.lifecycleManager != nil, "automatic": automatic,
			"status": fmt.Sprint(status), "statusError": fmt.Sprint(statusErr), "manualPruneError": fmt.Sprint(pruneErr)})
}
