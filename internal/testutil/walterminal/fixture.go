// Author: Codex, NOR-180 implementation lane, 2026-10-27.
// Package walterminal supplies an independent logical WAL fault for protocol tests.
package walterminal

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/cypher"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/memory"
	"github.com/orneryd/nornicdb/pkg/storage"
)

type Engine struct {
	*storage.MemoryEngine
	WAL    *storage.WAL
	Commit func(context.Context, *storage.BadgerTransaction) error
}

func New(t testing.TB) *Engine {
	t.Helper()
	wal, err := storage.NewWAL(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	fixtureowner.OwnChecked(t, wal)
	base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	return &Engine{MemoryEngine: base, WAL: wal}
}
func (e *Engine) GetWAL() *storage.WAL { return e.WAL }
func (e *Engine) CommitTransaction(ctx context.Context, tx *storage.BadgerTransaction) error {
	if e.Commit != nil {
		return e.Commit(ctx, tx)
	}
	return tx.CommitContext(ctx)
}

// AdmissionError models the existing storage admission status contract, including
// its independently localizable primary message. It injects a controlled primary
// at native commit; it does not claim to exercise memory-pressure sampling.
type AdmissionError struct{ error }

func (e *AdmissionError) Unwrap() error { return e.error }
func (e *AdmissionError) BoltErrorCode() string {
	return "Neo.TransientError.General.MemoryPoolOutOfMemoryError"
}
func (e *AdmissionError) StatusMessage() string {
	return localization.ApplicationMemoryPressure().Fallback
}
func Primary(kind string) error {
	if kind == "timeout" {
		return nornicerrors.MarkStatementTimeout(context.DeadlineExceeded, nornicerrors.ErrDatabaseQueryTimeLimit)
	}
	return &AdmissionError{localization.NewLocalizedError("Neo.TransientError.General.MemoryPoolOutOfMemoryError", localization.ApplicationMemoryPressure(), memory.ErrLimitExceeded)}
}
func RejectCommit(e *Engine, primary error, closed bool) {
	e.Commit = func(_ context.Context, tx *storage.BadgerTransaction) error {
		if closed {
			if err := e.WAL.Close(); err != nil {
				return err
			}
		}
		return nornicerrors.CombineTerminalOutcomes(primary, tx.Rollback())
	}
}

var procedureID atomic.Uint64

// FailingProcedure closes the logical WAL inside an admitted statement, after
// staging a real write. The HTTP route must consume the subsequent rollback error.
func FailingProcedure(t testing.TB, e *Engine, closed bool) string {
	t.Helper()
	name := fmt.Sprintf("nor180.protocol%d", procedureID.Add(1))
	err := cypher.RegisterUserProcedure(cypher.ProcedureSpec{Name: name, Signature: name + "() :: (value :: INTEGER)", Mode: cypher.ProcedureModeWrite, Returns: []cypher.ProcedureColumn{{Name: "value", Type: "INTEGER"}}}, func(ctx context.Context, exec *cypher.StorageExecutor, _ string, _ []interface{}) (*cypher.ExecuteResult, error) {
		if _, err := exec.Execute(ctx, "CREATE (:WalWire)", nil); err != nil {
			return nil, err
		}
		if closed {
			if err := e.WAL.Close(); err != nil {
				return nil, err
			}
		}
		return nil, errors.New("NOR-180 primary statement failure")
	})
	if err != nil {
		t.Fatal(err)
	}
	return "CALL " + name + "() YIELD value RETURN value"
}
func Count(t testing.TB, e *Engine) int64 {
	t.Helper()
	result, err := cypher.NewStorageExecutor(storage.NewNamespacedEngine(e.MemoryEngine, "nornic")).Execute(context.Background(), "MATCH (n:WalWire) RETURN count(n)", nil)
	if err != nil {
		t.Fatal(err)
	}
	return result.Rows[0][0].(int64)
}
