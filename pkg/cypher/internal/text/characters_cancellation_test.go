package text

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

type textWork struct{ calls, stop int }

func TestCharacterGrowingOutputsCancelAtEveryWorkPosition(t *testing.T) {
	input := strings.Repeat("é😀", 129)
	run := func(work Work) (string, bool) { return SubstringWithWork(input, 0, 257, work) }
	healthy := &textWork{}
	want, ok := run(healthy)
	require.True(t, ok)
	require.Equal(t, Substring(input, 0, 257), want)
	for stop := 1; stop <= healthy.calls; stop++ {
		work := &textWork{stop: stop}
		result, ok := run(work)
		require.False(t, ok, "stop %d/%d", stop, healthy.calls)
		require.Empty(t, result)
	}
}

func (w *textWork) Continue() bool { w.calls++; return w.stop == 0 || w.calls < w.stop }

func TestCharacterReadersCancellationAndParity(t *testing.T) {
	long := strings.Repeat("é", 4097)
	count, ok := LengthWithWork(long, &textWork{})
	require.True(t, ok)
	require.Equal(t, 4097, count)
	_, ok = LengthWithWork(long, &textWork{stop: 2})
	require.False(t, ok)
	for _, input := range []string{"aé😀z", "a\xffz", ""} {
		for _, run := range []func(Work) (string, bool){
			func(w Work) (string, bool) { return SubstringWithWork(input, 1, 2, w) },
			func(w Work) (string, bool) { return FromWithWork(input, 1, w) },
			func(w Work) (string, bool) { return LeftWithWork(input, 2, w) },
			func(w Work) (string, bool) { return RightWithWork(input, 2, w) },
			func(w Work) (string, bool) { return AtWithWork(input, -1, w) },
		} {
			want, valid := run(nil)
			got, ok := run(&textWork{})
			require.Equal(t, valid, ok)
			require.Equal(t, want, got)
			for stop := 1; stop <= 8; stop++ {
				work := &textWork{stop: stop}
				got, ok = run(work)
				if work.calls >= stop {
					require.False(t, ok)
					require.Empty(t, got)
				} else {
					require.Equal(t, valid, ok)
					require.Equal(t, want, got)
				}
			}
		}
	}
	for _, run := range []func() (string, bool){
		func() (string, bool) { return SubstringWithWork("abc", -1, -1, &textWork{}) },
		func() (string, bool) { return SubstringWithWork("abc", -1, 99, &textWork{}) },
		func() (string, bool) { return FromWithWork("abc", -1, &textWork{}) },
		func() (string, bool) { return LeftWithWork("abc", -1, &textWork{}) },
		func() (string, bool) { return RightWithWork("abc", -1, &textWork{}) },
		func() (string, bool) { return LeftWithWork("abc", 99, &textWork{}) },
		func() (string, bool) { return RightWithWork("abc", 99, &textWork{}) },
		func() (string, bool) { return AtWithWork("abc", 99, &textWork{}) },
	} {
		_, _ = run()
	}
}
