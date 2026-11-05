package storage

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
)

// The store population is checked after the API returns its original error.
// Result ownership alone cannot see a store which that API discards internally.
func TestRecoveryErrorStorePopulation(t *testing.T) {
	for _, api := range []string{"plain", "logger", "with_result", "transactions"} {
		t.Run(api, func(t *testing.T) {
			for _, failure := range []string{"snapshot_directory", "malformed_json", "duplicate_nodes", "missing_edge_endpoints", "invalid_manifest", "truncated_stream", "wal_directory"} {
				t.Run(failure, func(t *testing.T) {
					requireRecoveryStoresClosed(t)
					dir := t.TempDir()
					wal := filepath.Join(dir, "wal")
					snapshot := filepath.Join(dir, "snapshot")
					var setup error
					switch failure {
					case "snapshot_directory":
						setup = os.Mkdir(snapshot, 0755)
					case "malformed_json":
						setup = os.WriteFile(snapshot, []byte("{bad-json"), 0600)
					case "duplicate_nodes":
						var data []byte
						data, setup = json.Marshal(&Snapshot{Sequence: 1, Version: "1.0", Nodes: []*Node{{ID: "duplicate"}, {ID: "duplicate"}}})
						if setup == nil {
							setup = os.WriteFile(snapshot, data, 0600)
						}
					case "missing_edge_endpoints":
						setup = SaveSnapshot(&Snapshot{Sequence: 1, Version: "1.0", Edges: []*Edge{{ID: "bad", StartNode: "missing_a", EndNode: "missing_b", Type: "R"}}}, snapshot)
					case "invalid_manifest":
						snapshot = ""
						if setup = os.Mkdir(wal, 0755); setup == nil {
							_, setup = accessWALManifest(wal, walPublishManifest, &WALManifest{Version: walManifestVersion, Segments: []WALSegment{{FirstSeq: 1, LastSeq: 1, Path: "../invalid.wal"}}})
						}
					case "truncated_stream":
						setup = os.WriteFile(snapshot, []byte(streamSnapshotMagic), 0600)
					case "wal_directory":
						snapshot = ""
						setup = os.MkdirAll(walActivePath(wal), 0755)
					}
					if setup != nil {
						t.Fatal(setup)
					}
					var engine *MemoryEngine
					var err error
					switch api {
					case "plain":
						engine, err = fixtureowner.OwnResult[*MemoryEngine](t, ErrStorageClosed)(RecoverFromWAL(wal, snapshot))
					case "logger":
						engine, err = fixtureowner.OwnResult[*MemoryEngine](t, ErrStorageClosed)(RecoverFromWALWithLogger(wal, snapshot, nil))
					case "with_result":
						engine, _, err = fixtureowner.OwnResult3[*MemoryEngine, ReplayResult](t, ErrStorageClosed)(RecoverFromWALWithResult(wal, snapshot))
					case "transactions":
						engine, _, err = fixtureowner.OwnResult3[*MemoryEngine, *TransactionRecoveryResult](t, ErrStorageClosed)(RecoverWithTransactions(wal, snapshot))
					}
					// A directory named wal.log is not an empty WAL. All recovery
					// APIs must reject it without returning a usable store.
					if err == nil || engine != nil {
						t.Fatalf("failed recovery must return its error and no engine: engine=%v err=%v", engine, err)
					}
				})
			}
		})
	}
}
