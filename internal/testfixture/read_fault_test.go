package testfixture

import (
	"context"
	"errors"
	"testing"
)

type readFaultSource interface {
	Read(context.Context) ([]string, error)
}

func TestReadWithFailureInjectedFaultSkipsRead(t *testing.T) {
	injected := errors.New("injected read failure")
	var source readFaultSource // Binding/invoking this source before admission would panic.
	calls := 0
	got, err := ReadWithFailure(t.Context(), injected, func(ctx context.Context) ([]string, error) {
		calls++
		return source.Read(ctx)
	})
	if calls != 0 || got != nil || err != injected {
		t.Fatalf("fault preemption: calls=%d result=%v error=%v", calls, got, err)
	}
}

func TestReadWithFailureReturnsUnderlyingResultAndContext(t *testing.T) {
	failure := errors.New("ordinary underlying failure")
	for _, tc := range []struct {
		name  string
		value []string
		err   error
	}{
		{"healthy", []string{"stored"}, nil},
		{"partial error", []string{"partial"}, failure},
		{"nil error result", nil, failure},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			var passed context.Context
			calls := 0
			got, err := ReadWithFailure(ctx, nil, func(actual context.Context) ([]string, error) {
				passed = actual
				calls++
				return tc.value, tc.err
			})
			if calls != 1 || passed != ctx || err != tc.err {
				t.Fatalf("underlying pair/context changed: calls=%d context=%v error=%v", calls, passed == ctx, err)
			}
			if (got == nil) != (tc.value == nil) || len(got) != len(tc.value) {
				t.Fatalf("result shape changed: %v vs %v", got, tc.value)
			}
			if len(got) > 0 && &got[0] != &tc.value[0] {
				t.Fatal("returned result lost its original ownership")
			}
		})
	}
}
