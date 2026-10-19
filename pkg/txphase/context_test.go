package txphase

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestCommitAdmissionOwnsOutcome(t *testing.T) {
	ctx, cancel := WithExecutionDeadline(context.Background(), time.Second)
	commit, err := CommitContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	if commit.Err() != nil {
		t.Fatal("admitted commit inherited request cancellation")
	}
	if _, ok := commit.Deadline(); ok {
		t.Fatal("admitted commit retained statement deadline")
	}
	again, err := CommitContext(ctx)
	if err != nil || again.Err() != nil {
		t.Fatal("admission must be idempotent after cancellation", err)
	}
}
func TestCommitAdmissionRefusesExpiredStatements(t *testing.T) {
	ctx, cancel := WithExecutionDeadline(context.Background(), time.Nanosecond)
	defer cancel()
	<-ctx.Done()
	if _, err := CommitContext(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
}
func TestIndependentStatementCommitDoesNotAdmitRequest(t *testing.T) {
	ctx, cancel := WithExecutionDeadline(context.Background(), time.Second)
	statement := Statements(ctx)
	commit, err := CommitContext(statement)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	if commit.Err() == nil {
		t.Fatal("independent statement lost cancellation")
	}
	if _, err := CommitContext(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal("inner commit admitted outer request", err)
	}
}
