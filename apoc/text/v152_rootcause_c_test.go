package text_test

import (
	"testing"

	apoctext "github.com/orneryd/nornicdb/apoc/text"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestV152RCCAPOCTextOwners is deliberately smaller than the public NOR-282
// corpus. Each trigger calls one exported production owner directly and fails
// at that boundary; its nearby control proves the function itself was reached
// and is not generally unusable. The owner line is logged for the lab receipt.
func TestV152RCCAPOCTextOwners(t *testing.T) {
	tests := []struct {
		name, owner string
		trigger     func() any
		want        any
		control     func() any
		controlWant any
	}{
		{"split_regex", "apoc/text/text.go:37 literal strings.Split", func() any { return apoctext.Split("a1b2c", "[0-9]") }, []string{"a", "b", "c"}, func() any { return apoctext.Split("a,b", ",") }, []string{"a", "b"}},
		{"replace_regex", "apoc/text/text.go:47 literal strings.ReplaceAll", func() any { return apoctext.Replace("a1b2c", "[0-9]", "X") }, "aXbXc", func() any { return apoctext.Replace("aba", "a", "x") }, "xbx"},
		{"clean_deletes_whitespace", "apoc/text/text.go:237 strings.Fields collapse", func() any { return apoctext.Clean("  a  b  ") }, "ab", func() any { return apoctext.Clean("") }, ""},
		{"camel_punctuation", "apoc/text/text.go:188-200 splitWords/title", func() any { return apoctext.CamelCase("hello foo.bar") }, "helloFooBar", func() any { return apoctext.CamelCase("hello world") }, "helloWorld"},
		{"upper_camel_unicode", "apoc/text/text.go:222-228 strings.Title", func() any { return apoctext.UpperCamelCase("ßeta world") }, "SSetaWorld", func() any { return apoctext.UpperCamelCase("hello world") }, "HelloWorld"},
		{"urlencode_percent", "apoc/text/text.go:542 space-only encoding", func() any { return apoctext.Urlencode("a+b/é") }, "a%2Bb%2F%C3%A9", func() any { return apoctext.Urlencode("hello world") }, "hello+world"},
		{"urldecode_percent", "apoc/text/text.go:551 plus-only decoding", func() any { return apoctext.Urldecode("hello%20world") }, "hello world", func() any { return apoctext.Urldecode("hello+world") }, "hello world"},
		{"base64_encode_placeholder", "apoc/text/text.go:559-561 identity placeholder", func() any { return apoctext.Base64Encode("hello") }, "aGVsbG8=", func() any { return apoctext.Base64Encode("") }, ""},
		{"base64_decode_placeholder", "apoc/text/text.go:569-571 identity placeholder", func() any { return apoctext.Base64Decode("aGVsbG8=") }, "hello", func() any { return apoctext.Base64Decode("") }, ""},
		{"index_utf16", "apoc/text/text.go:580 strings.Index byte offset", func() any { return apoctext.IndexOf("éx", "x") }, 1, func() any { return apoctext.IndexOf("ax", "x") }, 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := test.trigger()
			t.Logf("RCC_APOC owner=%s trigger=%#v want=%#v", test.owner, got, test.want)
			assert.Equal(t, test.want, got, "direct owner differs from the APOC 5.26.30 contract")
			control := test.control()
			t.Logf("RCC_APOC_CONTROL owner=%s got=%#v want=%#v", test.owner, control, test.controlWant)
			require.Equal(t, test.controlWant, control)
		})
	}
}
