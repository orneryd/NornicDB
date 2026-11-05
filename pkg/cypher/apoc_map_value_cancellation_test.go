package cypher

import (
	"context"
	"fmt"
	"testing"
	"time"

	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
	"github.com/stretchr/testify/require"
)

// The map value functions copy caller-owned containers. A canceled statement
// must end that copy with the caller's error and no value: the statement
// result is produced once, so a partially built map would be a value nobody
// asked for.

// apocCancellationMap is a map whose count+1 top-level entries are a "drop"
// key and count entries that each hold a nested map and a list holding a map,
// so a recursive removal copies nested containers as well as the top level.
// The top-level "drop" key keeps apoc.map.removeKey on its recursive path.
func apocCancellationMap(count int) map[string]interface{} {
	entries := make(map[string]interface{}, count+1)
	entries["drop"] = int64(-1)
	for index := 0; index < count; index++ {
		entries[fmt.Sprintf("k%04d", index)] = map[string]interface{}{
			"drop": int64(index),
			"keep": int64(index),
			"list": []interface{}{
				map[string]interface{}{"drop": int64(index), "keep": int64(index)},
				int64(index),
			},
		}
	}
	return entries
}

// apocCancellationExpected is apocCancellationMap(count) with every nested
// "drop" key removed and nothing dropped, built independently of the
// implementation.
func apocCancellationExpected(count int) map[string]interface{} {
	expected := make(map[string]interface{}, count)
	for index := 0; index < count; index++ {
		expected[fmt.Sprintf("k%04d", index)] = map[string]interface{}{
			"keep": int64(index),
			"list": []interface{}{
				map[string]interface{}{"keep": int64(index)},
				int64(index),
			},
		}
	}
	return expected
}

// apocRemovalScope evaluates the removal arguments from a table, as the
// statement evaluator does for a row.
func apocRemovalScope(entries map[string]interface{}) cypherfn.Context {
	return neo5FunctionContext(map[string]interface{}{
		"m":      entries,
		"keys":   []interface{}{"drop"},
		"key":    "drop",
		"config": map[string]interface{}{"recursive": true},
	})
}

// TestApocMapRemovalStopsAtCanceledStatement: apoc.map.removeKey and
// apoc.map.removeKeys end a large recursive removal with the caller's
// cancellation or deadline error and no value, and the same call under the
// statement's healthy context returns the exact copied map.
func TestApocMapRemovalStopsAtCanceledStatement(t *testing.T) {
	const count = 256
	entries := apocCancellationMap(count)
	want := apocCancellationExpected(count)
	for _, tc := range []struct {
		function string
		args     []string
	}{
		{"apoc.map.removekeys", []string{"m", "keys", "config"}},
		{"apoc.map.removekey", []string{"m", "key", "config"}},
	} {
		t.Run(tc.function, func(t *testing.T) {
			t.Run("healthy statement", func(t *testing.T) {
				value, found, err := cypherfn.EvaluateFunction(context.Background(), tc.function, tc.args, apocRemovalScope(entries))
				require.True(t, found)
				require.NoError(t, err)
				require.Equal(t, want, value)
			})

			t.Run("already canceled", func(t *testing.T) {
				workCtx, cancel := context.WithCancel(context.Background())
				cancel()
				value, found, err := cypherfn.EvaluateFunction(workCtx, tc.function, tc.args, apocRemovalScope(entries))
				require.True(t, found)
				require.ErrorIs(t, err, context.Canceled)
				require.Nil(t, value)
			})

			t.Run("canceled while evaluating an argument", func(t *testing.T) {
				workCtx, cancel := context.WithCancel(context.Background())
				defer cancel()
				values := map[string]interface{}{
					"m":      entries,
					"keys":   []interface{}{"drop"},
					"key":    "drop",
					"config": map[string]interface{}{"recursive": true},
				}
				evaluated := 0
				scope := cypherfn.Context{Eval: func(expression string) (interface{}, error) {
					evaluated++
					if evaluated == 1 {
						cancel()
					}
					return values[expression], nil
				}}
				value, found, err := cypherfn.EvaluateFunction(workCtx, tc.function, tc.args, scope)
				require.True(t, found)
				require.ErrorIs(t, err, context.Canceled)
				require.Nil(t, value)
			})

			t.Run("deadline already exceeded", func(t *testing.T) {
				workCtx, cancel := context.WithDeadline(context.Background(), time.Now())
				defer cancel()
				require.ErrorIs(t, workCtx.Err(), context.DeadlineExceeded)
				value, found, err := cypherfn.EvaluateFunction(workCtx, tc.function, tc.args, apocRemovalScope(entries))
				require.True(t, found)
				require.ErrorIs(t, err, context.DeadlineExceeded)
				require.Nil(t, value)
			})
		})
	}
}

// apocObservationCancelContext is the caller's statement context with a real
// cancel function that cancels once the copy has asked for its error state
// apocCancelObservation times. The removal's own check before the copy is the
// first observation, so a later one can only come from inside the copy.
// Counting observations instead of timing keeps the test deterministic.
type apocObservationCancelContext struct {
	context.Context
	cancel       context.CancelFunc
	at           int
	observations int
}

func (c *apocObservationCancelContext) Err() error {
	c.observations++
	if c.observations >= c.at {
		c.cancel()
	}
	return c.Context.Err()
}

// TestApocMapRemovalStopsInsideTheCopy: the recursive copy observes the
// caller's cancellation while it runs, not only before it starts.
func TestApocMapRemovalStopsInsideTheCopy(t *testing.T) {
	entries := apocCancellationMap(512)
	for _, tc := range []struct {
		function string
		args     []string
	}{
		{"apoc.map.removekeys", []string{"m", "keys", "config"}},
		{"apoc.map.removekey", []string{"m", "key", "config"}},
	} {
		t.Run(tc.function, func(t *testing.T) {
			parent, cancel := context.WithCancel(context.Background())
			defer cancel()
			workCtx := &apocObservationCancelContext{Context: parent, cancel: cancel, at: 2}
			value, found, err := cypherfn.EvaluateFunction(workCtx, tc.function, tc.args, apocRemovalScope(entries))
			require.True(t, found)
			require.ErrorIs(t, err, context.Canceled)
			require.Nil(t, value)
			require.GreaterOrEqual(t, workCtx.observations, 2, "the copy observed the caller's context in its loop")
		})
	}
}

// TestApocMapConstructorsStopAtCanceledStatement: the map constructors copy
// their arguments as well; a canceled statement ends the merge, clean and
// fromPairs copies with the caller's error and no value, and the healthy
// statement returns the exact map.
func TestApocMapConstructorsStopAtCanceledStatement(t *testing.T) {
	const count = 256
	entries := apocCancellationMap(count)
	nulled := make(map[string]interface{}, len(entries)+1)
	for key, value := range entries {
		nulled[key] = value
	}
	nulled["nulled"] = nil
	cleaned := make(map[string]interface{}, len(entries))
	for key, value := range entries {
		cleaned[key] = value
	}
	pairs := make([]interface{}, 0, count)
	for index := 0; index < count; index++ {
		pairs = append(pairs, []interface{}{fmt.Sprintf("p%04d", index), int64(index)})
	}
	paired := make(map[string]interface{}, count)
	for index := 0; index < count; index++ {
		paired[fmt.Sprintf("p%04d", index)] = int64(index)
	}
	merged := make(map[string]interface{}, len(entries)+1)
	for key, value := range entries {
		merged[key] = value
	}
	merged["extra"] = int64(1)

	for _, tc := range []struct {
		function string
		args     []string
		values   map[string]interface{}
		want     map[string]interface{}
	}{
		{
			function: "apoc.map.merge",
			args:     []string{"left", "right"},
			values:   map[string]interface{}{"left": entries, "right": map[string]interface{}{"extra": int64(1)}},
			want:     merged,
		},
		{
			function: "apoc.map.clean",
			args:     []string{"left", "keys", "items"},
			values:   map[string]interface{}{"left": nulled, "keys": []interface{}{}, "items": []interface{}{}},
			want:     cleaned,
		},
		{
			function: "apoc.map.frompairs",
			args:     []string{"pairs"},
			values:   map[string]interface{}{"pairs": pairs},
			want:     paired,
		},
	} {
		t.Run(tc.function, func(t *testing.T) {
			scope := func() cypherfn.Context { return neo5FunctionContext(tc.values) }
			value, found, err := cypherfn.EvaluateFunction(context.Background(), tc.function, tc.args, scope())
			require.True(t, found)
			require.NoError(t, err)
			require.Equal(t, tc.want, value)

			workCtx, cancel := context.WithCancel(context.Background())
			cancel()
			value, found, err = cypherfn.EvaluateFunction(workCtx, tc.function, tc.args, scope())
			require.True(t, found)
			require.ErrorIs(t, err, context.Canceled)
			require.Nil(t, value)
		})
	}
}
