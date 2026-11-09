package cypher

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// Exercise Execute's deferred logger as well as its parser with a cold DFA.
func TestExecuteSlowQueryLoggingDeadline(t *testing.T) {
	exec, logged := captureSlowQueryLog(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := exec.Execute(ctx, "MATCH (n) WHERE n.x > 1 RETURN n", nil)
	elapsed := time.Since(start)
	if err != nil && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("execute: %v", err)
	}
	if elapsed > 100*time.Millisecond {
		t.Errorf("5 ms deadline including slow-query logging took %s", elapsed)
	}
	if len(logged()) != 1 {
		t.Fatal("expected a slow-query log record")
	}
}

func TestExecuteSlowQueryLoggingAlreadyCanceled(t *testing.T) {
	exec, logged := captureSlowQueryLog(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	start := time.Now()
	result, err := exec.Execute(ctx, "MATCH (n) WHERE n.secret = 'private-value' RETURN n", nil)
	if !errors.Is(err, context.Canceled) || result != nil {
		t.Fatalf("cancelled Execute returned result=%v err=%v", result, err)
	}
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Errorf("already-cancelled Execute including logging took %s", elapsed)
	}
	queries := logged()
	if len(queries) != 1 || queries[0] != RedactedPlaceholder {
		t.Fatalf("missing or unsafe slow-query redaction: %q", queries)
	}
}

func TestRedactForLogCancellationDuringValidation(t *testing.T) {
	exec, _ := captureSlowQueryLog(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	validate := redactGrammarValidate
	t.Cleanup(func() { redactGrammarValidate = validate })
	redactGrammarValidate = func(got context.Context, query string) error {
		if got != ctx {
			t.Fatal("grammar validation lost the statement context")
		}
		cancel()
		return validate(got, query)
	}
	if got := exec.redactForLog(ctx, "RETURN 'private-value'"); got != RedactedPlaceholder {
		t.Fatalf("cancelled validation logged %q", got)
	}
	if exec.hasCachedValidSyntax("RETURN 'private-value'", syntaxGrammarAccepted) {
		t.Fatal("cancelled validation published a successful grammar verdict")
	}
}

type cancelRedactionAtCheck struct {
	context.Context
	cancel         context.CancelFunc
	checks, target int
}

func (c *cancelRedactionAtCheck) Err() error {
	c.checks++
	if c.checks == c.target {
		c.cancel()
	}
	return c.Context.Err()
}

func TestSlowQueryLogCancellationDuringLongToken(t *testing.T) {
	for _, query := range []string{
		"/*" + strings.Repeat("secret", 1<<18) + "*/ RETURN 1",
		"RETURN '" + strings.Repeat("secret", 1<<18) + "'",
	} {
		exec, logged := captureSlowQueryLog(t)
		ctx, cancel := context.WithCancel(t.Context())
		during := &cancelRedactionAtCheck{Context: ctx, cancel: cancel, target: len(query)/1024 + 32}
		exec.emitSlowQueryLog(during, query, nil, time.Second)
		cancel()
		queries := logged()
		if len(queries) != 1 || queries[0] != RedactedPlaceholder {
			t.Fatalf("cancelled long-token scan logged %q", queries)
		}
	}
}
