package fn

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRegisteredCaseFunctionStopsAtArgumentBoundary(t *testing.T) {
	for _, name := range []string{"tolower", "toupper"} {
		t.Run(name, func(t *testing.T) {
			t.Run("already-cancelled", func(t *testing.T) {
				workCtx, cancel := context.WithCancel(context.Background())
				cancel()
				calls := 0
				scope := Context{Eval: func(string) (interface{}, error) { calls++; return "VALUE", nil }}
				value, found, err := EvaluateFunction(workCtx, name, []string{"$value"}, scope)
				require.True(t, found)
				require.ErrorIs(t, err, context.Canceled)
				require.Nil(t, value)
				require.Zero(t, calls)
			})
			t.Run("cancelled-by-argument", func(t *testing.T) {
				workCtx, cancel := context.WithCancel(context.Background())
				defer cancel()
				scope := Context{Eval: func(string) (interface{}, error) { cancel(); return "VALUE", nil }}
				value, found, err := EvaluateFunction(workCtx, name, []string{"$value"}, scope)
				require.True(t, found)
				require.ErrorIs(t, err, context.Canceled)
				require.Nil(t, value)
			})
			t.Run("deadline-during-argument", func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					workCtx, cancel := context.WithTimeout(context.Background(), time.Second)
					defer cancel()
					scope := Context{Eval: func(string) (interface{}, error) { <-workCtx.Done(); return "VALUE", nil }}
					value, found, err := EvaluateFunction(workCtx, name, []string{"$value"}, scope)
					require.True(t, found)
					require.ErrorIs(t, err, context.DeadlineExceeded)
					require.Nil(t, value)
				})
			})
			t.Run("ordinary-argument-error", func(t *testing.T) {
				want := errors.New("argument failed")
				scope := Context{Eval: func(string) (interface{}, error) { return nil, want }}
				value, found, err := EvaluateFunction(context.Background(), name, []string{"$value"}, scope)
				require.True(t, found)
				require.ErrorIs(t, err, want)
				require.Nil(t, value)
			})
		})
	}
}

func TestCaseFunctionDoesNotPublishCanceledConversion(t *testing.T) {
	workCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	scope := Context{Eval: func(string) (interface{}, error) { return "VALUE", nil }}
	value, err := CaseFunction("testCase", func(value string) string { cancel(); return value }, nil)(workCtx, scope, []string{"$value"})
	require.ErrorIs(t, workCtx.Err(), context.Canceled)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, value)
}
