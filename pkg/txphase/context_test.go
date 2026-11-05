package txphase

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestCommitAdmissionOwnsOutcome(t *testing.T) {
	ctx, cancel := WithExecutionDeadline(context.Background(), time.Second)
	if CommitAdmitted(ctx) {
		t.Fatal("request was admitted before terminal commit")
	}
	commit, err := CommitContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !CommitAdmitted(ctx) {
		t.Fatal("terminal commit admission was not recorded")
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
	if CommitAdmitted(statement) {
		t.Fatal("independent statement admitted the request")
	}
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

func TestResultContextUsesOnlyAdmittedTerminalLifetime(t *testing.T) {
	type identityKey struct{}
	for _, mode := range []string{"admitted", "unadmitted", "independent", "no_owner"} {
		t.Run(mode, func(t *testing.T) {
			parent := context.WithValue(t.Context(), identityKey{}, "request identity")
			ctx, cancel := WithExecutionDeadline(parent, time.Minute)
			defer cancel()
			switch mode {
			case "admitted":
				if _, err := CommitContext(ctx); err != nil {
					t.Fatal(err)
				}
			case "independent":
				ctx = Statements(ctx)
				if _, err := CommitContext(ctx); err != nil {
					t.Fatal(err)
				}
			case "no_owner":
				cancel()
				ctx, cancel = context.WithCancel(parent)
				defer cancel()
			}
			cancel()
			result := ResultContext(ctx)
			if got := result.Value(identityKey{}); got != "request identity" {
				t.Fatalf("lost request identity: %v", got)
			}
			if mode == "admitted" {
				if result.Err() != nil || result.Done() != nil {
					t.Fatal("terminal result inherited canceled request")
				}
				if _, ok := result.Deadline(); ok {
					t.Fatal("terminal result retained statement deadline")
				}
			} else if !errors.Is(result.Err(), context.Canceled) {
				t.Fatal("unadmitted result lost cancellation", result.Err())
			}
			if CommitAdmitted(ctx) != (mode == "admitted") {
				t.Fatal("result projection changed commit admission")
			}
		})
	}
}
