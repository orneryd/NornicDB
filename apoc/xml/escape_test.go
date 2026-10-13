package xml

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Escape and Unescape replace in one pass: the same text gives the same
// result on every call, and a replacement's own text isn't replaced again
// (#1163).
func TestEscapeAndUnescapeReplaceInOnePass(t *testing.T) {
	for range 50 {
		require.Equal(t, "&lt;a&gt; &amp; &quot;b&quot; &apos;c&apos;", Escape(`<a> & "b" 'c'`))
		require.Equal(t, "&lt;&gt;", Escape("<>"))
		require.Equal(t, `<a> & "b" 'c'`, Unescape("&lt;a&gt; &amp; &quot;b&quot; &apos;c&apos;"))
		require.Equal(t, "&lt;", Unescape("&amp;lt;"))
	}
}
