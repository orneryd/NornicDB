package cypher

// Terminal-cost harness for the #1548 transaction-read regression report.
//
// This is a reconstruction of the reported "read inside an explicit
// transaction" harness from README-1548-read-regression.md: the original files
// were not attached. The same reconstructed workload was used on both historical
// revisions and the correction. It is retained here to measure the full
// transaction sequence rather than a single read in an already-open snapshot.
//
// Each iteration is a sequence of separate Execute calls on one StorageExecutor.
//
//	read_in_tx     BEGIN -> MATCH (n:NOR180Cost) RETURN count(n) AS c -> COMMIT
//	new_read_in_tx BEGIN -> MATCH ... WHERE n.k = <i> ... -> COMMIT
//	COMMIT         BEGIN -> CREATE (n:NOR180Cost) DELETE n -> COMMIT
//	ROLLBACK       BEGIN -> CREATE (n:NOR180Cost) DELETE n -> ROLLBACK
//	implicit       CREATE (n:NOR180Cost) DELETE n
//
// The read rows never create a node and every row starts on a fresh engine, so
// they read an empty label: any large per-iteration cost cannot be data.

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

const nor180Namespace = "nor180_cost"

// nor180Engine is the recipe's storage wrapper: the test memory engine, made to
// expose a real WAL to the executor, with transaction commits routed through
// CommitContext.
type nor180Engine struct {
	*storage.MemoryEngine
	wal *storage.WAL
}

func (e *nor180Engine) GetWAL() *storage.WAL { return e.wal }

func (e *nor180Engine) CommitTransaction(ctx context.Context, tx *storage.BadgerTransaction) error {
	return tx.CommitContext(ctx)
}

// newNOR180Harness builds a fresh engine per sub-benchmark, as the recipe does.
func newNOR180Harness(b *testing.B) (*StorageExecutor, context.Context) {
	b.Helper()
	engine := newTestMemoryEngine(b)
	wal, err := storage.NewWAL(b.TempDir(), nil)
	if err != nil {
		b.Fatalf("new wal: %v", err)
	}
	b.Cleanup(func() { _ = wal.Close() })
	wrapped := &nor180Engine{MemoryEngine: engine, wal: wal}
	return NewStorageExecutor(storage.NewNamespacedEngine(wrapped, nor180Namespace)), context.Background()
}

// nor180Run runs one statement. wantEmptyCount checks the read rows' documented
// result on the first iteration only, so the check does not enter the measured
// steady-state cost.
func nor180Run(b *testing.B, exec *StorageExecutor, ctx context.Context, statement string, wantEmptyCount bool) {
	b.Helper()
	result, err := exec.Execute(ctx, statement, nil)
	if err != nil {
		b.Fatalf("%s: %v", statement, err)
	}
	if result == nil {
		b.Fatalf("%s returned no result", statement)
	}
	if wantEmptyCount && (len(result.Rows) != 1 || len(result.Rows[0]) != 1 || result.Rows[0][0] != int64(0)) {
		b.Fatalf("%s returned %v, want a single zero count", statement, result.Rows)
	}
}

// BenchmarkNOR180TerminalCost measures the reported harness row by row.
func BenchmarkNOR180TerminalCost(b *testing.B) {
	for _, row := range []string{"read_in_tx", "new_read_in_tx", "COMMIT", "ROLLBACK", "implicit"} {
		row := row
		b.Run(row, func(b *testing.B) {
			exec, ctx := newNOR180Harness(b)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				switch row {
				case "read_in_tx":
					nor180Run(b, exec, ctx, "BEGIN", false)
					nor180Run(b, exec, ctx, "MATCH (n:NOR180Cost) RETURN count(n) AS c", i == 0)
					nor180Run(b, exec, ctx, "COMMIT", false)
				case "new_read_in_tx":
					nor180Run(b, exec, ctx, "BEGIN", false)
					nor180Run(b, exec, ctx, fmt.Sprintf("MATCH (n:NOR180Cost) WHERE n.k = %d RETURN count(n) AS c", i), i == 0)
					nor180Run(b, exec, ctx, "COMMIT", false)
				case "COMMIT":
					nor180Run(b, exec, ctx, "BEGIN", false)
					nor180Run(b, exec, ctx, "CREATE (n:NOR180Cost) DELETE n", false)
					nor180Run(b, exec, ctx, "COMMIT", false)
				case "ROLLBACK":
					nor180Run(b, exec, ctx, "BEGIN", false)
					nor180Run(b, exec, ctx, "CREATE (n:NOR180Cost) DELETE n", false)
					nor180Run(b, exec, ctx, "ROLLBACK", false)
				default:
					nor180Run(b, exec, ctx, "CREATE (n:NOR180Cost) DELETE n", false)
				}
			}
			b.StopTimer()
		})
	}
}
