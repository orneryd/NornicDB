package label_test

import (
	"testing"
	"unicode/utf8"

	"github.com/orneryd/nornicdb/apoc"
	"github.com/orneryd/nornicdb/apoc/label"
	apocstorage "github.com/orneryd/nornicdb/apoc/storage"
	"github.com/stretchr/testify/require"
)

func TestNormalizePreservesUnicodeInitialAndRemainder(t *testing.T) {
	require.NoError(t, apoc.Initialize(apocstorage.NewInMemoryStorage(), nil))
	for _, tc := range []struct{ input, want string }{
		{"élan", "Élan"}, {"ßeta", "ßeta"}, {"ﬃle", "ﬃle"}, {" person ", "Person"}, {"", ""}, {" \t\n", ""},
		{"e\u0301lan", "E\u0301lan"}, {"\u0301élan", "\u0301élan"},
		{"é漢字", "É漢字"}, {"漢字", "漢字"}, {"😀hello", "😀hello"},
		{"𐐨élan", "𐐀élan"}, {"ǳuro", "Ǳuro"}, {"\ufffdabc", "\ufffdabc"},
	} {
		t.Run(tc.input, func(t *testing.T) {
			got := label.Normalize(tc.input)
			require.True(t, utf8.ValidString(got))
			require.Equal(t, tc.want, got)
			registered, err := apoc.Call("apoc.label.normalize", []interface{}{tc.input})
			require.NoError(t, err)
			require.Equal(t, got, registered)
		})
	}
}

func TestNormalizePreservesAdmittedUTF16NameUnits(t *testing.T) {
	require.NoError(t, apoc.Initialize(apocstorage.NewInMemoryStorage(), nil))
	for _, tc := range []struct{ input, want string }{
		{"\xed\xa0\x80élan", "\xed\xa0\x80élan"},
		{"élan\xed\xbf\xbf", "Élan\xed\xbf\xbf"},
	} {
		require.Equal(t, tc.want, label.Normalize(tc.input))
		registered, err := apoc.Call("apoc.label.normalize", []interface{}{tc.input})
		require.NoError(t, err)
		require.Equal(t, tc.want, registered)
	}
}

// These cases retain the existing Go helper contract, rather than adding a
// new rejection API or treating admitted name units as malformed bytes.
func TestNormalizeRetainsMalformedByteHandling(t *testing.T) {
	for _, tc := range []struct{ input, want string }{
		{"\xffabc", "\ufffdabc"}, {"\xc3", "\ufffd"}, {"a\xff", "A\xff"},
		{"\xed\xa0", "\ufffd\xa0"},
	} {
		require.Equal(t, tc.want, label.Normalize(tc.input))
	}
}
