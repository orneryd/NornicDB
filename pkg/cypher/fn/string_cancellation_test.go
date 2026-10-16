package fn

import (
	"context"
	"errors"
	"sort"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"golang.org/x/text/transform"
	"golang.org/x/text/unicode/norm"
)

type functionWork struct{ calls, stop int }

func (w *functionWork) Continue() bool { w.calls++; return w.stop == 0 || w.calls < w.stop }
func (w *functionWork) Err() error {
	if w.stop != 0 && w.calls >= w.stop {
		return context.Canceled
	}
	return nil
}
func (w *functionWork) SortStrings(values []string) bool {
	if !w.Continue() {
		return false
	}
	sort.Strings(values)
	return true
}

type failedTransform struct{ transform.NopResetter }

var transformFailure = errors.New("transform failed")

func (failedTransform) Transform([]byte, []byte, bool) (int, int, error) {
	return 0, 0, transformFailure
}

func TestStringReadersBorrowCancellation(t *testing.T) {
	text := strings.Repeat("ΟΣßİé", 2000) + "ΟΣ"
	for _, upper := range []bool{false, true} {
		want, err := MapStringCase(text, upper, nil)
		require.NoError(t, err)
		got, err := MapStringCase(text, upper, &functionWork{})
		require.NoError(t, err)
		require.Equal(t, want, got)
		_, err = MapStringCase(text, upper, &functionWork{stop: 2})
		require.ErrorIs(t, err, context.Canceled)
	}
	for _, input := range []string{
		strings.Repeat("A", 4095) + "Σ",
		strings.Repeat("A", 4095) + "ΣA",
		"A" + strings.Repeat("\u0301", 10000) + "Σ",
		strings.Repeat("A'", 4095) + "Σ",
		strings.Repeat("\xffé", 3000) + "ΟΣ",
	} {
		want := lowerCaseText(input)
		got, err := MapStringCase(input, false, &functionWork{})
		require.NoError(t, err)
		require.True(t, want == got, "case mapping changed across a chunk boundary (input bytes=%d)", len(input))
	}
	length, err := CharacterCount(text, &functionWork{})
	require.NoError(t, err)
	require.Equal(t, 10002, length)
	_, err = CharacterCount(text, &functionWork{stop: 2})
	require.ErrorIs(t, err, context.Canceled)
	want, _, err := transform.String(norm.NFC, text)
	require.NoError(t, err)
	got, err := TransformString(text, norm.NFC, nil)
	require.NoError(t, err)
	require.Equal(t, want, got)
	got, err = TransformString(text, norm.NFC, &functionWork{})
	require.NoError(t, err)
	require.Equal(t, want, got)
	_, err = TransformString(text, failedTransform{}, &functionWork{})
	require.ErrorIs(t, err, transformFailure)
	_, err = TransformString("", failedTransform{}, &functionWork{})
	require.ErrorIs(t, err, transformFailure)
	for _, upper := range []bool{true, false} {
		_, err = MapStringCase("ΟΣ", upper, &functionWork{})
		require.NoError(t, err)
	}
}

func TestPropertyKeysBorrowCancellation(t *testing.T) {
	fields := map[string]interface{}{"b": 2, "a": 1}
	for _, value := range []interface{}{fields, &storage.Node{Properties: fields}, &storage.Edge{Properties: fields}} {
		keys, ok := PropertyKeysWithCancellation(value, &functionWork{})
		require.True(t, ok)
		require.Equal(t, []interface{}{"a", "b"}, keys)
		for stop := 1; stop <= 6; stop++ {
			keys, ok = PropertyKeysWithCancellation(value, &functionWork{stop: stop})
			require.False(t, ok)
			require.Nil(t, keys)
		}
	}
	labels, err := labelValues([]string{"a", "b"}, &functionWork{})
	require.NoError(t, err)
	require.Equal(t, []interface{}{"a", "b"}, labels)
	_, err = labelValues([]string{"a", "b"}, &functionWork{stop: 2})
	require.ErrorIs(t, err, context.Canceled)
	labels, err = labelValues(nil, nil)
	require.NoError(t, err)
	require.Empty(t, labels)
	_, ok := PropertyKeys(fields)
	require.True(t, ok)
	for _, target := range []string{"node", "edge", "map"} {
		for _, stop := range []int{0, 1, 4} {
			work := &functionWork{stop: stop}
			value, found, err := EvaluateFunction("keys", []string{target}, Context{
				Cancellation: work, Nodes: map[string]*storage.Node{"node": {Properties: fields}}, Rels: map[string]*storage.Edge{"edge": {Properties: fields}},
				Eval: func(string) (interface{}, error) { return fields, nil },
			})
			require.True(t, found)
			if stop == 0 {
				require.NoError(t, err)
				require.Equal(t, []interface{}{"a", "b"}, value)
			} else {
				require.ErrorIs(t, err, context.Canceled)
			}
		}
	}
}

func TestGrowingFunctionOutputsCancelAtEveryWorkPosition(t *testing.T) {
	text := strings.Repeat("ΟΣßİé", 2000)
	fields := make(map[string]interface{}, 129)
	labels := make([]string, 129)
	for index := range labels {
		labels[index] = strings.Repeat("x", index+1)
		fields[labels[index]] = index
	}
	for _, run := range []func(*functionWork) bool{
		func(w *functionWork) bool {
			result, err := MapStringCase(text, false, w)
			if err != nil {
				require.Empty(t, result)
			}
			return err == nil
		},
		func(w *functionWork) bool {
			result, err := MapStringCase(text, true, w)
			if err != nil {
				require.Empty(t, result)
			}
			return err == nil
		},
		func(w *functionWork) bool {
			result, err := TransformString(text, norm.NFC, w)
			if err != nil {
				require.Empty(t, result)
			}
			return err == nil
		},
		func(w *functionWork) bool {
			result, ok := PropertyKeysWithCancellation(fields, w)
			if !ok {
				require.Nil(t, result)
			}
			return ok
		},
		func(w *functionWork) bool {
			result, err := labelValues(labels, w)
			if err != nil {
				require.Nil(t, result)
			}
			return err == nil
		},
	} {
		healthy := &functionWork{}
		require.True(t, run(healthy))
		for stop := 1; stop <= healthy.calls; stop++ {
			work := &functionWork{stop: stop}
			require.False(t, run(work), "stop %d/%d", stop, healthy.calls)
			require.ErrorIs(t, work.Err(), context.Canceled)
		}
	}
}
