package bolt

import (
 "context"
 "github.com/orneryd/nornicdb/pkg/resultstream"
)

// Each RUN owns a batch until its records are PULLed or DISCARDed. The
// connection tracker retains failure ownership of every outstanding batch.
func (s *Session) beginContinuationDelivery(ctx context.Context) context.Context {
 if !s.inTransaction&&s.lastDelivery!=nil { s.lastDelivery.Fail() }
 s.lastDelivery=s.continuationDelivery.Fork()
 return resultstream.WithDelivery(ctx,s.lastDelivery)
}
func (s *Session) abandonContinuationResults() {
 if s.lastDelivery!=nil { s.lastDelivery.Fail();s.lastDelivery=nil }
 for _,stream:=range s.resultStreams {
  if stream.delivery!=nil { stream.delivery.Fail() }
 }
}
func (s *Session) flushResultDelivery(stream resultStream,rows [][]any,complete bool) error {
 if err:=s.flushIfPending();err!=nil {
  if stream.delivery!=nil { stream.delivery.Fail() }
  return err
 }
 if stream.delivery!=nil {
  if complete { stream.delivery.Delivered() } else { stream.delivery.DeliveredValues(rows) }
 }
 return nil
}

