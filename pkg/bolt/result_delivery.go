package bolt

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/resultstream"
)

// Each RUN owns a batch until its records are PULLed or DISCARDed. The
// connection tracker retains failure ownership of every outstanding batch.
func (s *Session) beginContinuationDelivery(ctx context.Context) context.Context {
	if !s.inTransaction {
		if s.lastDelivery != nil {
			s.lastDelivery.Fail()
		}
		if s.lastResult != nil {
			s.lastResult.memoryLease.Release()
		}
	}
	s.lastDelivery = s.continuationDelivery.Fork()
	return resultstream.WithDelivery(ctx, s.lastDelivery)
}
func (s *Session) abandonContinuationResults() {
	if s.lastResult != nil {
		s.lastResult.memoryLease.Release()
	}
	if s.lastDelivery != nil {
		s.lastDelivery.Fail()
		s.lastDelivery = nil
	}
	for _, stream := range s.resultStreams {
		if stream.result != nil {
			stream.result.memoryLease.Release()
		}
		if stream.delivery != nil {
			stream.delivery.Fail()
		}
	}
}
func (s *Session) flushResultDelivery(statementID int64, stream resultStream, complete bool) error {
	if err := s.flushIfPending(); err != nil {
		if stream.delivery != nil {
			stream.delivery.Fail()
		}
		return err
	}
	if stream.delivery != nil && stream.delivery.Pending() {
		stream.delivery.DeliveredWritten()
		if complete && stream.delivery.Pending() {
			stream.delivery.Fail()
		}
	}
	s.updateResultStream(statementID, stream, !complete)
	return nil
}

// DISCARD selects complete records without writing them. Use the same
// PackStream encoder as PULL so reference ownership is independent of whether
// a procedure page is returned as a map, JSON text or a nested Bolt value.
func (s *Session) discardResultDelivery(stream resultStream, rows [][]any) {
	if stream.delivery == nil || !stream.delivery.Pending() {
		return
	}
	resolver := s.recordEntityDatabaseResolver(stream.database)
	for _, row := range rows {
		buf := encodeRecordListInto(s.recordBuf[:0], row, s.useUTCDateTimeStructs(), s.boltV5(), stream.database, resolver)
		stream.delivery.DiscardEncoded(buf)
		s.recordBuf = buf[:0]
	}
}
