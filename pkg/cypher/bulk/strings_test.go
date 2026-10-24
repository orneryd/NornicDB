package bulk

import (
	"context"
	"strings"
	"testing"
	"unicode"

	"github.com/stretchr/testify/require"
)

func stringPassInputs() []string {
	large := strings.Repeat("ab,c ", CopyStride/2)
	return []string{
		"", "a", "abc", "a,b,,c", ",,", "héllo wörld", "\xff\xfe,x", "  lead and trail  ",
		large, large + "\xffend", strings.Repeat("é", Stride*3), strings.Repeat(",", CopyStride+7),
	}
}

func TestStringPassesMatchTheStandardLibrary(t *testing.T) {
	ctx := context.Background()
	for _, s := range stringPassInputs() {
		for _, sep := range []string{",", "", "ab", "é", "zz"} {
			for _, n := range []int{-1, 0, 1, 2, 5, 1 << 20} {
				got, err := SplitN(ctx, s, sep, n)
				require.NoError(t, err)
				require.Equal(t, strings.SplitN(s, sep, n), got, "SplitN(%d bytes, %q, %d)", len(s), sep, n)
				replaced, err := Replace(ctx, s, sep, "<>", n)
				require.NoError(t, err)
				require.Equal(t, strings.Replace(s, sep, "<>", n), replaced, "Replace(%d bytes, %q, %d)", len(s), sep, n)
			}
			got, err := Split(ctx, s, sep)
			require.NoError(t, err)
			require.Equal(t, strings.Split(s, sep), got)
			all, err := ReplaceAll(ctx, s, sep, "")
			require.NoError(t, err)
			require.Equal(t, strings.ReplaceAll(s, sep, ""), all)
		}
		fields, err := Fields(ctx, s)
		require.NoError(t, err)
		require.Equal(t, strings.Fields(s), fields)
		comma := func(r rune) bool { return r == ',' || unicode.IsSpace(r) }
		byComma, err := FieldsFunc(ctx, s, comma)
		require.NoError(t, err)
		require.Equal(t, strings.FieldsFunc(s, comma), byComma)
		parts := strings.Split(s, ",")
		joined, err := Join(ctx, parts, "--")
		require.NoError(t, err)
		require.Equal(t, strings.Join(parts, "--"), joined)
	}
	for _, s := range []string{"", "x", "abc", "héllo", strings.Repeat("y", 1000), strings.Repeat("z", CopyStride+3)} {
		for _, count := range []int{0, 1, 3, 1 << 10, 1 << 17} {
			repeated, err := Repeat(ctx, s, count)
			require.NoError(t, err)
			require.True(t, strings.Repeat(s, count) == repeated, "Repeat(%d bytes, %d)", len(s), count)
		}
	}
	require.Panics(t, func() { _, _ = Repeat(ctx, "x", -1) })
}

// A large pass on a canceled context returns its error and no result.
func TestStringPassesStopOnCancellation(t *testing.T) {
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	large := strings.Repeat("a,", CopyStride*4)
	parts, err := Split(canceled, large, ",")
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, parts)
	replaced, err := ReplaceAll(canceled, large, ",", ";")
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, replaced)
	joined, err := Join(canceled, strings.Split(large, ","), "|")
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, joined)
	repeated, err := Repeat(canceled, "abc", CopyStride)
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, repeated)
	fields, err := Fields(canceled, strings.Repeat("a ", CopyStride))
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, fields)
}
