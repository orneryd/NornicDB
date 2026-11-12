package fn

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/orneryd/nornicdb/internal/queryprofile"
)

// recordingObserver is a request-owned observer that keeps the opaque callback
// reports and hands out the access counters of the canonical reads it received.
type recordingObserver struct {
	mu         sync.Mutex
	unobserved []string
	counters   []*queryprofile.ReadCounter
}

func (o *recordingObserver) UnobservedRead(_ context.Context, category, name string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.unobserved = append(o.unobserved, category+": "+name)
}

func (o *recordingObserver) Access(_ context.Context, _, _ string) *queryprofile.ReadCounter {
	o.mu.Lock()
	defer o.mu.Unlock()
	counter := &queryprofile.ReadCounter{}
	o.counters = append(o.counters, counter)
	return counter
}

// report returns opaque callbacks and every observed acquisition, including a
// known zero. Counter values must not erase evidence that the reader ran.
func (o *recordingObserver) report() (unobserved []string, reads int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]string(nil), o.unobserved...), len(o.counters)
}

// restoreRegistration restores the registry entry of name when the test ends,
// so a replacement never leaks into another test's built-in.
func restoreRegistration(t *testing.T, name string) {
	t.Helper()
	key := cyphertext.NameLower(strings.TrimSpace(name))
	mu.Lock()
	previous, present := registry[key]
	mu.Unlock()
	t.Cleanup(func() {
		mu.Lock()
		defer mu.Unlock()
		if present {
			registry[key] = previous
			return
		}
		delete(registry, key)
	})
}

// evaluateWithObserver evaluates name under a fresh request-owned observer and
// returns the observer's reports.
func evaluateWithObserver(t *testing.T, name string, ctx Context) (value interface{}, unobserved []string, reads int) {
	t.Helper()
	observer := &recordingObserver{}
	value, found, err := EvaluateFunction(queryprofile.WithObserver(context.Background(), observer), name, nil, ctx)
	if err != nil || !found {
		t.Fatalf("evaluate %s: found=%v err=%v", name, found, err)
	}
	unobserved, reads = observer.report()
	return value, unobserved, reads
}

func TestRegisterReadObservationPolicyControlsOpaqueCallback(t *testing.T) {
	ctx := Context{Eval: func(expression string) (interface{}, error) { return expression, nil }}

	Register("policy_canonical_value", func(_ context.Context, _ Context, _ []string) (interface{}, error) {
		return "canonical", nil
	}, CanonicalReads)
	value, unobserved, reads := evaluateWithObserver(t, "policy_canonical_value", ctx)
	if value != "canonical" || len(unobserved) != 0 || reads != 0 {
		t.Fatalf("canonical registration: value=%v unobserved=%v reads=%d", value, unobserved, reads)
	}

	Register("policy_opaque_value", func(_ context.Context, _ Context, _ []string) (interface{}, error) {
		return "opaque", nil
	}, OpaqueReads)
	value, unobserved, reads = evaluateWithObserver(t, "policy_opaque_value", ctx)
	if value != "opaque" || reads != 0 {
		t.Fatalf("opaque registration: value=%v reads=%d", value, reads)
	}
	if len(unobserved) != 1 || unobserved[0] != "registered function callback: policy_opaque_value" {
		t.Fatalf("opaque registration reports its actual invocation once, got %v", unobserved)
	}

	var zero ReadObservation
	if zero != OpaqueReads {
		t.Fatalf("the zero read observation must be OpaqueReads, got %v", zero)
	}
	Register("policy_zero_value", func(_ context.Context, _ Context, _ []string) (interface{}, error) {
		return "zero", nil
	}, zero)
	if _, unobserved, _ = evaluateWithObserver(t, "policy_zero_value", ctx); len(unobserved) != 1 {
		t.Fatalf("a registration without a stated policy stays opaque, got %v", unobserved)
	}
}

// TestToupperBuiltinIsCanonical proves the public registration of the case
// function owner no longer reports a correct built-in as an opaque callback,
// while still returning the mapped text.
func TestToupperBuiltinIsCanonical(t *testing.T) {
	ctx := Context{Eval: func(expression string) (interface{}, error) {
		if expression == "'abc'" {
			return "abc", nil
		}
		return nil, nil
	}}
	observer := &recordingObserver{}
	value, found, err := EvaluateFunction(queryprofile.WithObserver(context.Background(), observer), "toupper", []string{"'abc'"}, ctx)
	if err != nil || !found {
		t.Fatalf("toUpper builtin: found=%v err=%v", found, err)
	}
	if value != "ABC" {
		t.Fatalf("toUpper('abc') = %v, want ABC", value)
	}
	if unobserved, reads := observer.report(); len(unobserved) != 0 || reads != 0 {
		t.Fatalf("a source-reviewed pure builtin reports no opaque callback and no read, got %v / %d", unobserved, reads)
	}
}

func TestRegisterReplacementSwapsImplementationAndPolicyTogether(t *testing.T) {
	restoreRegistration(t, "policy_replaced")
	ctx := Context{Eval: func(expression string) (interface{}, error) { return expression, nil }}

	Register("policy_replaced", func(_ context.Context, _ Context, _ []string) (interface{}, error) {
		return "first", nil
	}, CanonicalReads)
	if value, unobserved, _ := evaluateWithObserver(t, "policy_replaced", ctx); value != "first" || len(unobserved) != 0 {
		t.Fatalf("first registration: value=%v unobserved=%v", value, unobserved)
	}

	// Replacing the name replaces the policy with the implementation: the
	// custom callback is opaque even though the name was canonical before.
	Register("policy_replaced", func(_ context.Context, _ Context, _ []string) (interface{}, error) {
		return "second", nil
	}, OpaqueReads)
	if value, unobserved, _ := evaluateWithObserver(t, "policy_replaced", ctx); value != "second" || len(unobserved) != 1 {
		t.Fatalf("opaque replacement: value=%v unobserved=%v", value, unobserved)
	}

	Register("policy_replaced", func(_ context.Context, _ Context, _ []string) (interface{}, error) {
		return "third", nil
	}, CanonicalReads)
	if value, unobserved, _ := evaluateWithObserver(t, "policy_replaced", ctx); value != "third" || len(unobserved) != 0 {
		t.Fatalf("canonical replacement: value=%v unobserved=%v", value, unobserved)
	}
}

// TestReplacingBuiltinNameStaysUnknown replaces the toUpper built-in with a
// custom callback: the name carries no trust, so the custom implementation is
// reported as an opaque callback again.
func TestReplacingBuiltinNameStaysUnknown(t *testing.T) {
	restoreRegistration(t, "toupper")
	ctx := Context{Eval: func(expression string) (interface{}, error) {
		if expression == "'abc'" {
			return "abc", nil
		}
		return nil, nil
	}}
	Register("toupper", func(_ context.Context, _ Context, _ []string) (interface{}, error) {
		return "custom", nil
	}, OpaqueReads)
	observer := &recordingObserver{}
	value, found, err := EvaluateFunction(queryprofile.WithObserver(context.Background(), observer), "toupper", []string{"'abc'"}, ctx)
	if err != nil || !found || value != "custom" {
		t.Fatalf("built-in replacement: value=%v found=%v err=%v", value, found, err)
	}
	if unobserved, _ := observer.report(); len(unobserved) != 1 || unobserved[0] != "registered function callback: toupper" {
		t.Fatalf("a replacement of a built-in name must stay unknown, got %v", unobserved)
	}
}

func TestRegisterRejectsUnknownReadObservation(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatalf("expected panic for an unknown read observation")
		}
	}()
	Register("policy_unknown_observation", func(_ context.Context, _ Context, _ []string) (interface{}, error) {
		return nil, nil
	}, ReadObservation(2))
}

func TestEvaluateFunctionPreservesResultsAndErrorsUnderBothPolicies(t *testing.T) {
	ctx := Context{Eval: func(expression string) (interface{}, error) { return expression, nil }}
	registerErr := errors.New("handler failed")

	for _, tc := range []struct {
		name  string
		reads ReadObservation
	}{
		{name: "policy_result_error_opaque", reads: OpaqueReads},
		{name: "policy_result_error_canonical", reads: CanonicalReads},
	} {
		t.Run(tc.name, func(t *testing.T) {
			Register(tc.name+"_nil", func(_ context.Context, _ Context, _ []string) (interface{}, error) {
				return nil, nil
			}, tc.reads)
			if value, found, err := EvaluateFunction(context.Background(), tc.name+"_nil", nil, ctx); !found || err != nil || value != nil {
				t.Fatalf("nil result: value=%v found=%v err=%v", value, found, err)
			}

			Register(tc.name+"_boxed_nil", func(_ context.Context, _ Context, _ []string) (interface{}, error) {
				return map[string]interface{}(nil), nil
			}, tc.reads)
			if value, found, err := EvaluateFunction(context.Background(), tc.name+"_boxed_nil", nil, ctx); !found || err != nil || value != nil {
				t.Fatalf("boxed nil result is admitted as null: value=%v found=%v err=%v", value, found, err)
			}

			Register(tc.name+"_error", func(_ context.Context, _ Context, _ []string) (interface{}, error) {
				return nil, registerErr
			}, tc.reads)
			value, found, err := EvaluateFunction(context.Background(), tc.name+"_error", nil, ctx)
			if !found || !errors.Is(err, registerErr) || value != nil {
				t.Fatalf("error result: value=%v found=%v err=%v", value, found, err)
			}
		})
	}
}

// TestConcurrentRegistrationKeepsImplementationAndPolicyPaired registers one
// name with two (implementation, policy) pairs while evaluating it: a call
// must never see one registration's implementation with another's policy.
func TestConcurrentRegistrationKeepsImplementationAndPolicyPaired(t *testing.T) {
	const name = "policy_concurrent_pair"
	restoreRegistration(t, name)
	ctx := Context{Eval: func(expression string) (interface{}, error) { return expression, nil }}
	canonical := func(_ context.Context, _ Context, _ []string) (interface{}, error) { return int64(1), nil }
	opaque := func(_ context.Context, _ Context, _ []string) (interface{}, error) { return int64(2), nil }
	Register(name, canonical, CanonicalReads)

	var writers sync.WaitGroup
	stop := make(chan struct{})
	for writer := 0; writer < 4; writer++ {
		writers.Add(1)
		go func() {
			defer writers.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				Register(name, canonical, CanonicalReads)
				Register(name, opaque, OpaqueReads)
			}
		}()
	}

	var readers sync.WaitGroup
	for reader := 0; reader < 4; reader++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for evaluation := 0; evaluation < 500; evaluation++ {
				observer := &recordingObserver{}
				value, found, err := EvaluateFunction(queryprofile.WithObserver(context.Background(), observer), name, nil, ctx)
				if err != nil || !found {
					t.Errorf("evaluate %s: found=%v err=%v", name, found, err)
					return
				}
				unobserved, _ := observer.report()
				switch value {
				case int64(1):
					if len(unobserved) != 0 {
						t.Errorf("canonical implementation reported an opaque callback: %v", unobserved)
						return
					}
				case int64(2):
					if len(unobserved) != 1 {
						t.Errorf("opaque implementation did not report exactly one opaque callback: %v", unobserved)
						return
					}
				default:
					t.Errorf("unexpected result %v", value)
					return
				}
			}
		}()
	}

	readers.Wait()
	close(stop)
	writers.Wait()
}

// TestCanonicalPolicyKeepsNestedArgumentObservations proves a canonical
// registration does not hide a read its argument produced: the inner
// observation is reported where it happened, and the function adds no opaque
// callback of its own.
func TestCanonicalPolicyKeepsNestedArgumentObservations(t *testing.T) {
	Register("policy_nested_argument", func(_ context.Context, ctx Context, args []string) (interface{}, error) {
		return ctx.Eval(args[0])
	}, CanonicalReads)
	observer := &recordingObserver{}
	workCtx := queryprofile.WithObserver(context.Background(), observer)
	ctx := Context{Eval: func(string) (interface{}, error) {
		queryprofile.Access(workCtx, "node", "nested argument").Add(1, 1)
		return "inner", nil
	}}
	value, found, err := EvaluateFunction(workCtx, "policy_nested_argument", []string{"n"}, ctx)
	if err != nil || !found || value != "inner" {
		t.Fatalf("nested argument: value=%v found=%v err=%v", value, found, err)
	}
	unobserved, reads := observer.report()
	if len(unobserved) != 0 || reads != 1 {
		t.Fatalf("nested argument observation: unobserved=%v reads=%d", unobserved, reads)
	}
}

// TestEvaluateFunctionPreservesStatementCancellation proves both policies
// receive the caller's statement context and return its cancellation error.
func TestEvaluateFunctionPreservesStatementCancellation(t *testing.T) {
	for _, tc := range []struct {
		name  string
		reads ReadObservation
	}{
		{name: "policy_cancel_opaque", reads: OpaqueReads},
		{name: "policy_cancel_canonical", reads: CanonicalReads},
	} {
		t.Run(tc.name, func(t *testing.T) {
			Register(tc.name, func(workCtx context.Context, _ Context, _ []string) (interface{}, error) {
				if err := workCtx.Err(); err != nil {
					return nil, err
				}
				return "not canceled", nil
			}, tc.reads)
			workCtx, cancel := context.WithCancel(context.Background())
			cancel()
			value, found, err := EvaluateFunction(workCtx, tc.name, nil, Context{Eval: func(string) (interface{}, error) { return nil, nil }})
			if !found || !errors.Is(err, context.Canceled) || value != nil {
				t.Fatalf("canceled call: value=%v found=%v err=%v", value, found, err)
			}
		})
	}
}

func TestReadObserverKeepsKnownZeroAcquisitions(t *testing.T) {
	observer := &recordingObserver{}
	queryprofile.Access(queryprofile.WithObserver(t.Context(), observer), "KnownEmpty", "").Add(0, 0)
	unobserved, reads := observer.report()
	if len(unobserved) != 0 || reads != 1 {
		t.Fatalf("one known-zero acquisition must remain observable: callbacks=%v reads=%d", unobserved, reads)
	}
}

// This component ledger has no clause scope to freeze.
func (o *recordingObserver) WithLifetime(lifetime context.Context, _ context.Context) context.Context {
	return queryprofile.WithObserver(lifetime, o)
}
