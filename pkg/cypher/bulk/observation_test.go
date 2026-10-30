package bulk

import (
	"context"
	"errors"
	"testing"
	"time"
)

// The owner reports exactly what the statement's context reports: moving a
// check onto it changes no error value, cause or wait signal.
func TestObservationMatchesContext(t *testing.T) {
	live := context.Background()
	if err := ObserveContext(live); err != nil {
		t.Fatalf("live context observed as %v", err)
	}
	if Cause(live) != nil {
		t.Fatalf("live context has cause %v", Cause(live))
	}
	if Done(live) != live.Done() {
		t.Fatal("Done is not the context's own signal")
	}

	cause := errors.New("statement stopped")
	canceled, cancel := context.WithCancelCause(context.Background())
	cancel(cause)
	if err := ObserveContext(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled context observed as %v", err)
	}
	if got := Cause(canceled); got != cause {
		t.Fatalf("cause %v, want %v", got, cause)
	}
	select {
	case <-Done(canceled):
	default:
		t.Fatal("Done is not closed after cancellation")
	}

	expired, stop := context.WithTimeout(context.Background(), time.Nanosecond)
	defer stop()
	<-expired.Done()
	if err := ObserveContext(expired); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expired context observed as %v", err)
	}
	if got := Cause(expired); !errors.Is(got, context.DeadlineExceeded) {
		t.Fatalf("expired cause %v", got)
	}
}
