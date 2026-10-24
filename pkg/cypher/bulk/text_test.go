package bulk

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// textInputs are small strings and strings of several strides whose
// multi-byte runes (and spaces) fall on the chunk boundaries.
func textInputs() []string {
	spaces := strings.Repeat("　 \t", CopyStride/2)
	word := strings.Repeat("AbßKKİx", CopyStride/3)
	return []string{
		"", " ", "abc", "  Hello World  ",
		spaces + word + spaces,
		word + strings.Repeat(" ", 3*CopyStride),
		strings.Repeat(" ", 3*CopyStride) + "z",
		strings.Repeat("é", 2*CopyStride) + "needle" + strings.Repeat("é", 10),
		spaces,
	}
}

func TestTextMatchesTheStandardLibrary(t *testing.T) {
	ctx := context.Background()
	for _, s := range textInputs() {
		require.Equal(t, strings.TrimSpace(s), TrimSpace(ctx, s))
		require.Equal(t, strings.Trim(s, " 　z"), Trim(ctx, s, " 　z"))
		require.Equal(t, strings.ToUpper(s), ToUpper(ctx, s))
		require.Equal(t, strings.ToLower(s), ToLower(ctx, s))
		require.Equal(t, asciiMap(s, 'a', 'z', 'A'-'a'), UpperASCII(ctx, s))
		require.Equal(t, asciiMap(s, 'A', 'Z', 'a'-'A'), LowerASCII(ctx, s))
		require.Equal(t, s, String(ctx, []byte(s)))
		require.Equal(t, []byte(s), Bytes(ctx, s))
		for _, sub := range []string{"", "needle", "én", "zz", "KK"} {
			require.Equal(t, strings.Index(s, sub), Index(ctx, s, sub), "Index %q", sub)
			require.Equal(t, strings.Contains(s, sub), Contains(ctx, s, sub))
		}
		require.Equal(t, strings.ContainsAny(s, "zİ"), ContainsAny(ctx, s, "zİ"))
		for _, other := range []string{strings.ToUpper(s), strings.ToLower(s), s + "x", strings.ToUpper(s) + "K"} {
			require.Equal(t, strings.EqualFold(s, other), EqualFold(ctx, s, other))
		}
	}
}

func TestTextStopsWhenCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	large := strings.Repeat(" ", 4*CopyStride) + "x"
	for name, call := range map[string]func(){
		"TrimSpace": func() { TrimSpace(ctx, large) },
		"ToUpper":   func() { ToUpper(ctx, large) },
		"Index":     func() { Index(ctx, large, "y") },
		"EqualFold": func() { EqualFold(ctx, large, large) },
		"String":    func() { String(ctx, []byte(large)) },
	} {
		var err error
		func() {
			defer RecoverStop(&err)
			call()
		}()
		require.ErrorIs(t, err, context.Canceled, name)
	}
	// A short string is the library call: nothing to observe.
	require.Equal(t, "x", TrimSpace(ctx, " x "))
}

// asciiMap is the byte-by-byte reference for UpperASCII and LowerASCII.
func asciiMap(s string, low, high byte, shift int) string {
	out := []byte(s)
	for i, b := range out {
		if b >= low && b <= high {
			out[i] = byte(int(b) + shift)
		}
	}
	return string(out)
}
