// Author: Codex, NOR-180 implementation lane, 2026-10-27; boundary moved to
// statementFailure by Claude, 2026-10-30.
package server

import (
	"context"
	"errors"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/localization"
)

type walWireBenchmarkDiagnostic struct {
	error
	message localization.Message
}

func (e *walWireBenchmarkDiagnostic) Unwrap() error                          { return e.error }
func (e *walWireBenchmarkDiagnostic) StatusDiagnostic() localization.Message { return e.message }

type walWireBenchmarkMemory struct{ error }

func (e *walWireBenchmarkMemory) Unwrap() error { return e.error }
func (e *walWireBenchmarkMemory) BoltErrorCode() string {
	return "Neo.TransientError.General.MemoryPoolOutOfMemoryError"
}
func (e *walWireBenchmarkMemory) StatusMessage() string {
	return localization.ApplicationMemoryPressure().Fallback
}

var walWireBenchmarkResult QueryError

// Both arms receive identical already-constructed errors. Only the public HTTP
// statement-failure boundary is timed (statementFailure, which has the same
// signature on both arms; no executor, so no result header lookup). Primary
// rows measure a failure without diagnostics; joined rows add the WAL-failure
// diagnostic text delivered to clients.
func BenchmarkWALTerminalWireMessages(b *testing.B) {
	timeout := nornicerrors.MarkStatementTimeout(context.DeadlineExceeded, nornicerrors.ErrDatabaseQueryTimeLimit)
	memory := &walWireBenchmarkMemory{localization.NewLocalizedError("Neo.TransientError.General.MemoryPoolOutOfMemoryError", localization.ApplicationMemoryPressure(), nil)}
	cause := errors.New("failed to write WAL tx abort for benchmark-tx: wal: closed")
	diagnostic := &walWireBenchmarkDiagnostic{cause, localization.ExternalDiagnostic(cause)}
	for _, primary := range []struct {
		name string
		err  error
	}{{"timeout", timeout}, {"memory", memory}} {
		for _, withWAL := range []bool{false, true} {
			name := primary.name + "/primary"
			err := primary.err
			if withWAL {
				name = primary.name + "/with_wal"
				err = errors.Join(err, diagnostic)
			}
			b.Run(name, func(b *testing.B) {
				b.ReportAllocs()
				localize := func(message localization.Message) string { return message.Fallback }
				var response TransactionResponse
				for i := 0; i < b.N; i++ {
					walWireBenchmarkResult = statementFailure(context.Background(), &response, nil, "", err, localize)
				}
			})
		}
	}
}
