package text

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

type bufferWork struct{ calls, stop int }

func TestBufferAppendAliasedItems(t *testing.T) {
	for _, offsets := range [][3]int{{200, 0, 900}, {100, 300, 900}, {0, 0, 900}, {900, 0, 900}} {
		initial, source, count := offsets[0], offsets[1], offsets[2]
		makeInput := func() []int {
			values := make([]int, 2048)
			for index := range values {
				values[index] = index
			}
			return values
		}
		wantInput, gotInput := makeInput(), makeInput()
		want := append(wantInput[:initial], wantInput[source:source+count]...)
		work := &bufferWork{}
		got, ok := AppendItems(gotInput[:initial], gotInput[source:source+count], work)
		require.True(t, ok)
		require.Equal(t, want, got)
		require.Equal(t, 1+(count+127)/128, work.calls)
	}
}

func (work *bufferWork) Continue() bool {
	work.calls++
	return work.stop == 0 || work.calls < work.stop
}

func TestBufferGrowthInsideOldStringPrefix(t *testing.T) {
	for _, appendValue := range []func(*strings.Builder, Work) bool{
		func(out *strings.Builder, w Work) bool { return AppendString(out, "!", w) },
		func(out *strings.Builder, w Work) bool { return AppendBytes(out, []byte("!"), w) },
		func(out *strings.Builder, w Work) bool { return AppendByte(out, '!', w) },
		func(out *strings.Builder, w Work) bool { return AppendRune(out, '世', w) },
	} {
		var out strings.Builder
		out.Grow(1 << 20)
		old := strings.Repeat("p", out.Cap())
		out.WriteString(old)
		work := &bufferWork{stop: 3}
		require.False(t, appendValue(&out, work))
		require.Equal(t, 3, work.calls)
		require.Equal(t, 4096, out.Len(), "cancel while copying the old prefix, before adding one new value")
		require.Equal(t, strings.Repeat("p", 4096), out.String())
		require.Equal(t, strings.Repeat("p", len(old)), old, "reset/growth must preserve aliased old strings")
	}
}

func TestBufferStringGrowthAndWrites(t *testing.T) {
	var empty strings.Builder
	require.True(t, ReserveString(&empty, 4096, nil))
	require.Zero(t, empty.Cap(), "nil callers retain native write-time growth")
	for _, work := range []Work{nil, &bufferWork{}} {
		var out strings.Builder
		require.True(t, AppendString(&out, strings.Repeat("x", 10000), work))
		require.True(t, AppendBytes(&out, []byte("bytes"), work))
		require.True(t, AppendByte(&out, '!', work))
		require.True(t, AppendRune(&out, '世', work))
		require.True(t, AppendRune(&out, -1, work))
		require.Equal(t, strings.Repeat("x", 10000)+"bytes!世�", out.String())
		require.True(t, AppendString(&out, "", work))
		require.True(t, AppendBytes(&out, nil, work))
	}
	for _, appendValue := range []func(*strings.Builder, Work) bool{func(o *strings.Builder, w Work) bool { return AppendString(o, "", w) }, func(o *strings.Builder, w Work) bool { return AppendBytes(o, nil, w) }} {
		var out strings.Builder
		require.False(t, appendValue(&out, &bufferWork{stop: 1}))
		require.Zero(t, out.Len())
	}
	var out strings.Builder
	out.Grow(1 << 20)
	out.WriteString(strings.Repeat("x", out.Cap()))
	length := out.Len()
	work := &bufferWork{}
	require.True(t, AppendByte(&out, '!', work))
	require.Equal(t, 1+(length+4095)/4096, work.calls, "old-prefix work must scale even for one appended byte")
	require.Equal(t, length+1, out.Len())
	require.Equal(t, 200, bufferCapacity(100, 150))
	require.Equal(t, 400, bufferCapacity(100, 400))
	require.Equal(t, 128, bufferCapacity(0, 1))
	require.Equal(t, int(^uint(0)>>1), bufferCapacity(int(^uint(0)>>1), int(^uint(0)>>1)))
}

func TestBufferSliceGrowthInsideOldPrefix(t *testing.T) {
	old := make([]int64, 1<<18)
	for index := range old {
		old[index] = int64(index)
	}
	work := &bufferWork{stop: 3}
	result, ok := AppendItem(old, int64(-1), work)
	require.False(t, ok)
	require.Nil(t, result)
	require.Equal(t, 3, work.calls)
	for index, value := range old {
		require.Equal(t, int64(index), value)
	}
	work = &bufferWork{}
	result, ok = AppendItem(old, int64(-1), work)
	require.True(t, ok)
	require.Equal(t, int64(-1), result[len(old)])
	require.Equal(t, 2+(len(old)+127)/128, work.calls, "old prefix and new values each consume checked copy units")
	for stop := 1; stop <= work.calls; stop += 127 {
		_, ok := AppendItems(old, []int64{-1}, &bufferWork{stop: stop})
		require.False(t, ok)
	}
}

func TestBufferSliceNativeAndReservedPaths(t *testing.T) {
	for _, work := range []Work{nil, &bufferWork{}} {
		values, ok := AppendItems([]int{1, 2}, []int{3, 4}, work)
		require.True(t, ok)
		require.Equal(t, []int{1, 2, 3, 4}, values)
		values = make([]int, 2, 8)
		values[0], values[1] = 1, 2
		values, ok = AppendItem(values, 3, work)
		require.True(t, ok)
		require.Equal(t, []int{1, 2, 3}, values)
		_, ok = AppendItems([]struct{}{{}}, []struct{}{{}}, work)
		require.True(t, ok)
		_, ok = AppendItems([][8192]byte{{}}, [][8192]byte{{}}, work)
		require.True(t, ok)
	}
	_, ok := AppendItems([]int{}, []int{1, 2}, &bufferWork{stop: 2})
	require.False(t, ok)
}
