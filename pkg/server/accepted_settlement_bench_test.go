// Accepted-settlement cost harness for the public embedded Cypher write path.
package server

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"

	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// settlementSetStatement is the one measured statement. Both lifecycle shapes
// submit exactly this text, so an implicit autocommit statement and an explicit
// BEGIN/COMMIT envelope differ only in their transaction control commands.
const settlementSetStatement = "MATCH (n:Doc {id: 'n'}) SET n.payload = $payload"

// BenchmarkPublicCypherManagedEntitySettlement is a baseline/candidate cost
// harness for one public embedded Cypher row update per measured operation. It
// runs four sequential cases: an implicit autocommit statement and an explicit
// BEGIN/MATCH SET/COMMIT sequence, each with BadgerOptions.WriteBehind off and
// on.
//
// Every case opens its own isolated Badger database behind a real WAL (default
// WAL settings), a DatabaseManager-namespaced store and a managed quota that
// admits one node (MaxNodes=1, no MaxBytes limit), so the measured pipeline
// includes accepted entity settlement and managed quota admission for a
// single-row update driven entirely through the public Execute entry point.
//
// This workload is deliberately scoped: it is not the original inaccessible
// TrueNAS suite, and it is not the full application or search pipeline — no
// HTTP/Bolt transport, authentication, embedding or search work is included.
// The source is meant to compile and run unchanged on the baseline and the
// candidate commits; paired protocol and performance conclusions belong to the
// parent review.
func BenchmarkPublicCypherManagedEntitySettlement(b *testing.B) {
	for _, explicit := range []bool{false, true} {
		for _, writeBehind := range []bool{false, true} {
			envelope := "implicit"
			if explicit {
				envelope = "explicit-begin-commit"
			}
			b.Run(fmt.Sprintf("%s/BadgerOptions.WriteBehind=%t", envelope, writeBehind), func(b *testing.B) {
				benchmarkPublicCypherManagedEntitySettlement(b, explicit, writeBehind)
			})
		}
	}
}

func benchmarkPublicCypherManagedEntitySettlement(b *testing.B, explicit, writeBehind bool) {
	b.Helper()
	// The process-wide WAL flag returns to its previous value only after the
	// stores this case opened are closed, so its restore is the first cleanup
	// registered here (and therefore the last one to run).
	b.Cleanup(config.WithWALEnabled())

	dir := b.TempDir()
	base, err := runtimeFixtureOwner.TrackResult[*storage.BadgerEngine](b, storage.ErrStorageClosed)(
		storage.NewBadgerEngineWithOptions(storage.BadgerOptions{
			DataDir:     filepath.Join(dir, "badger"),
			WriteBehind: writeBehind,
		}))
	if err != nil {
		b.Fatalf("open isolated badger: %v", err)
	}
	walDir := filepath.Join(dir, "wal")
	walConfig := storage.DefaultWALConfig()
	walConfig.Dir = walDir
	wal, err := storage.NewWAL(walDir, walConfig)
	if err != nil {
		b.Fatalf("open wal: %v", err)
	}
	manager, err := multidb.NewDatabaseManager(b.Context(), storage.NewWALEngine(base, wal), nil)
	if err != nil {
		b.Fatalf("open database manager: %v", err)
	}
	// The manager close owns the WAL engine, the WAL and Badger, so it runs
	// before this case's temporary directory removal and WAL flag restore.
	manager = runtimeFixtureOwner.OwnChecked(b, manager)

	database := manager.DefaultDatabaseName()
	store, err := manager.GetStorage(database)
	if err != nil {
		b.Fatalf("resolve %q storage: %v", database, err)
	}
	exec := cypher.NewStorageExecutor(store)
	ctx := b.Context()
	control := cypher.WithTransactionControl(ctx)

	if _, err := exec.Execute(ctx, "CREATE (:Doc {id: 'n', payload: 'seed'})", nil); err != nil {
		b.Fatalf("seed Doc row: %v", err)
	}
	// One managed Doc row: the node quota admits the fixture and every measured
	// update, and a zero MaxBytes limit keeps byte accounting (and its scan) off.
	if err := manager.SetDatabaseLimits(ctx, database, &multidb.Limits{Storage: multidb.StorageLimits{MaxNodes: 1}}); err != nil {
		b.Fatalf("set managed node quota: %v", err)
	}

	// Alternate two preboxed string parameter values so every measured update
	// changes the stored payload instead of becoming a no-op.
	values := [2]string{"alpha", "beta"}
	params := [2]map[string]any{{"payload": values[0]}, {"payload": values[1]}}

	// Warm the statement/analysis cache, the one-time quota census and the
	// namespace's read views before the timer starts; the warm write itself is
	// not measured. It uses the opposite value from the first timed update.
	if explicit {
		if _, err := exec.Execute(control, "BEGIN", nil); err != nil {
			b.Fatalf("warm begin: %v", err)
		}
		if _, err := exec.Execute(ctx, settlementSetStatement, params[1]); err != nil {
			b.Fatalf("warm set: %v", err)
		}
		if _, err := exec.Execute(control, "COMMIT", nil); err != nil {
			b.Fatalf("warm commit: %v", err)
		}
	} else if _, err := exec.Execute(ctx, settlementSetStatement, params[1]); err != nil {
		b.Fatalf("warm set: %v", err)
	}
	// The fixture's own records and the warm write are already journaled; only
	// the records the timed loop appends are counted against b.N.
	baseline := len(settlementSemanticRecords(b, walDir))

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if explicit {
			if _, err := exec.Execute(control, "BEGIN", nil); err != nil {
				b.Fatalf("begin at %d: %v", i, err)
			}
			if _, err := exec.Execute(ctx, settlementSetStatement, params[i%len(params)]); err != nil {
				b.Fatalf("set at %d: %v", i, err)
			}
			if _, err := exec.Execute(control, "COMMIT", nil); err != nil {
				b.Fatalf("commit at %d: %v", i, err)
			}
			continue
		}
		if _, err := exec.Execute(ctx, settlementSetStatement, params[i%len(params)]); err != nil {
			b.Fatalf("set at %d: %v", i, err)
		}
	}
	b.StopTimer()

	// Untimed correctness. The last buffered write-behind generation is forced
	// into Badger with a Background flush so the durable read below observes the
	// physical image rather than a pending generation or an overlay.
	if err := base.FlushWriteBehind(context.Background()); err != nil {
		b.Fatalf("flush write-behind: %v", err)
	}
	durable, err := storage.GetStoredNodesByLabel(ctx, storage.NewNamespacedEngine(base, database), "Doc")
	if err != nil {
		b.Fatalf("read durable Doc rows: %v", err)
	}
	if len(durable) != 1 {
		b.Fatalf("durable image holds %d Doc rows, want 1", len(durable))
	}
	wantPayload := values[(b.N-1)%len(values)]
	if payload, _ := durable[0].Properties["payload"].(string); payload != wantPayload {
		b.Fatalf("durable payload = %q after %d measured writes, want %q", payload, b.N, wantPayload)
	}
	if nodes, err := store.NodeCount(ctx); err != nil {
		b.Fatalf("count managed nodes: %v", err)
	} else if nodes != 1 {
		b.Fatalf("managed store holds %d nodes, want 1", nodes)
	}
	wantMode := storage.CommitModeImplicit
	if explicit {
		wantMode = storage.CommitModeExplicit
	}
	records := settlementSemanticRecords(b, walDir)
	if len(records) < baseline {
		b.Fatalf("journal holds %d semantic records, fewer than the %d present before the timed loop", len(records), baseline)
	}
	if measured := len(records) - baseline; measured != b.N {
		b.Fatalf("journal appended %d semantic records for %d measured operations, want one per operation", measured, b.N)
	}
	for i, record := range records[baseline:] {
		if record.Mode != wantMode {
			b.Fatalf("semantic record %d of the measured operations carries envelope mode %d, want %d", i, record.Mode, wantMode)
		}
	}
}

// settlementSemanticRecords decodes every semantic entity-commit record in dir,
// which is the record the journal appends once per accepted settlement. The
// payload is read through the public db.txlog rendering and decoded with the
// same semantic decoder replay and replication use, so a record is counted only
// when its envelope — the originating mode included — is readable.
func settlementSemanticRecords(tb testing.TB, dir string) []storage.EntityTransactionEnvelope {
	tb.Helper()
	entries, err := storage.ReadWALEntriesFromDir(tb.Context(), dir)
	if err != nil {
		tb.Fatalf("read wal entries: %v", err)
	}
	envelopes := make([]storage.EntityTransactionEnvelope, 0, len(entries))
	for _, entry := range entries {
		if entry.Operation != storage.OpEntityCommit {
			continue
		}
		rendered, err := storage.EntryPayloadJSON(entry)
		if err != nil {
			tb.Fatalf("render entity commit %d: %v", entry.Sequence, err)
		}
		var record struct {
			Payload []byte `json:"payload"`
		}
		if err := json.Unmarshal([]byte(rendered), &record); err != nil {
			tb.Fatalf("decode entity commit %d payload bytes: %v", entry.Sequence, err)
		}
		envelope, _, err := storage.DecodeEntityTransaction(record.Payload)
		if err != nil {
			tb.Fatalf("decode entity commit %d envelope: %v", entry.Sequence, err)
		}
		envelopes = append(envelopes, envelope)
	}
	return envelopes
}
