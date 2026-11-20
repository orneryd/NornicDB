package cypher

// Hunt ground H2 (v1.5.2 open hunt, "around the fixes"): shared output of the
// cypher-package variants. A variant re-applies one fix's own regression
// assertion to a sibling path. It never fails the test on a disagreement: it
// prints one "HUNT_LEAD <json>" line per disagreement and one
// "HUNT_COVERAGE <json>" line per harness, so every case of a batch runs.
// Test file only; not for merge.

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func h2Lead(lead map[string]any) {
	lead["ground"] = "H2"
	data, err := json.Marshal(lead)
	if err != nil {
		data = []byte(fmt.Sprintf("%q", fmt.Sprint(lead)))
	}
	fmt.Fprintf(os.Stdout, "HUNT_LEAD %s\n", data)
}

func h2Coverage(coverage map[string]any) {
	coverage["ground"] = "H2"
	data, _ := json.Marshal(coverage)
	fmt.Fprintf(os.Stdout, "HUNT_COVERAGE %s\n", data)
}

// h2Store is one storage stack a variant runs on.
type h2Store struct {
	name string
	open func(t *testing.T) storage.Engine
}

// h2Stores are the receivers a count, read or write may reach: the in-memory
// Badger engine behind a namespace (the regression tests' fixture) and the
// server's stack (disk Badger -> WAL -> namespace).
func h2Stores() []h2Store {
	return []h2Store{
		{name: "badger-mem+ns", open: func(t *testing.T) storage.Engine {
			base, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineInMemory())
			require.NoError(t, err)
			t.Cleanup(func() { _ = base.Close() })
			return storage.NewNamespacedEngine(base, "h2hunt")
		}},
		{name: "server-stack", open: func(t *testing.T) storage.Engine {
			dir := t.TempDir()
			base, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(dir))
			if base != nil {
				fixtureowner.Own(t, base)
			}
			require.NoError(t, err)
			wal, err := storage.NewWAL(filepath.Join(dir, "wal"), nil)
			require.NoError(t, err)
			engine := storage.NewWALEngine(base, wal)
			t.Cleanup(func() {
				_ = wal.Close()
				_ = base.Close()
			})
			return storage.NewNamespacedEngine(engine, "h2hunt")
		}},
	}
}

// h2Exec runs one statement and reports its rows or its error.
func h2Exec(exec *StorageExecutor, ctx context.Context, statement string, params map[string]interface{}) ([][]interface{}, error) {
	result, err := exec.Execute(ctx, statement, params)
	if err != nil {
		return nil, err
	}
	return result.Rows, nil
}
