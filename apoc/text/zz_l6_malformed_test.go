package text_test

// Lane L6 (closeout): cause proof for the malformed-input members of G-apoc-text.
// Each owner is called directly with the malformed input of the NOR-282 corpus
// (split_invalid_pattern, replace_invalid_pattern, urldecode_malformed,
// Base64Decode "%%%"). The run logs (1) the owner's Go signature, showing that
// it has no error result, (2) the value it returns for the malformed input,
// and (3) the result of a test-side corrected step (the standard decoder or
// regex compiler) on the same input, which reports the error APOC reports.

import (
	"encoding/base64"
	"net/url"
	"reflect"
	"regexp"
	"testing"

	apoctext "github.com/orneryd/nornicdb/apoc/text"
)

func zzL6HasErrorResult(fn any) (string, bool) {
	ft := reflect.TypeOf(fn)
	errType := reflect.TypeOf((*error)(nil)).Elem()
	has := false
	for i := 0; i < ft.NumOut(); i++ {
		if ft.Out(i).Implements(errType) {
			has = true
		}
	}
	return ft.String(), has
}

func TestL6APOCTextMalformedInputOwners(t *testing.T) {
	type tc struct {
		name, owner string
		fn          any
		call        func() any
		corrected   func() error
		control     func() any
		controlWant any
		correctedOK func() error
	}
	cases := []tc{
		{"split_invalid_pattern", "apoc/text/text.go:33-38 Split", apoctext.Split,
			func() any { return apoctext.Split("a1b2c", "[") },
			func() error { _, err := regexp.Compile("["); return err },
			func() any { return apoctext.Split("a,b", ",") }, []string{"a", "b"},
			func() error { _, err := regexp.Compile(","); return err }},
		{"replace_invalid_pattern", "apoc/text/text.go:46-48 Replace", apoctext.Replace,
			func() any { return apoctext.Replace("abc", "[", "X") },
			func() error { _, err := regexp.Compile("["); return err },
			func() any { return apoctext.Replace("aba", "a", "x") }, "xbx",
			func() error { _, err := regexp.Compile("a"); return err }},
		{"urldecode_malformed_zz", "apoc/text/text.go:550-552 Urldecode", apoctext.Urldecode,
			func() any { return apoctext.Urldecode("%zz") },
			func() error { _, err := url.QueryUnescape("%zz"); return err },
			func() any { return apoctext.Urldecode("hello+world") }, "hello world",
			func() error { _, err := url.QueryUnescape("hello+world"); return err }},
		{"urldecode_malformed_badZZ", "apoc/text/text.go:550-552 Urldecode", apoctext.Urldecode,
			func() any { return apoctext.Urldecode("bad%ZZ") },
			func() error { _, err := url.QueryUnescape("bad%ZZ"); return err },
			func() any { return apoctext.Urldecode("a+b") }, "a b",
			func() error { _, err := url.QueryUnescape("a+b"); return err }},
		{"urldecode_malformed_trailing_percent", "apoc/text/text.go:550-552 Urldecode", apoctext.Urldecode,
			func() any { return apoctext.Urldecode("x%") },
			func() error { _, err := url.QueryUnescape("x%"); return err },
			func() any { return apoctext.Urldecode("x") }, "x",
			func() error { _, err := url.QueryUnescape("x"); return err }},
		{"base64decode_malformed", "apoc/text/text.go:569-572 Base64Decode", apoctext.Base64Decode,
			func() any { return apoctext.Base64Decode("%%%") },
			func() error { _, err := base64.StdEncoding.DecodeString("%%%"); return err },
			func() any { return apoctext.Base64Decode("") }, "",
			func() error { _, err := base64.StdEncoding.DecodeString("aGVsbG8="); return err }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			sig, hasErr := zzL6HasErrorResult(c.fn)
			got := c.call()
			corrErr := c.corrected()
			ctl := c.control()
			ctlCorrErr := c.correctedOK()
			t.Logf("L6_APOC_MALFORMED case=%s owner=%q signature=%q has_error_result=%v owner_value=%#v corrected_step_err=%v control_value=%#v control_corrected_err=%v",
				c.name, c.owner, sig, hasErr, got, corrErr, ctl, ctlCorrErr)
			if !reflect.DeepEqual(ctl, c.controlWant) {
				t.Fatalf("control: owner returned %#v, want %#v", ctl, c.controlWant)
			}
			if ctlCorrErr != nil {
				t.Fatalf("control: corrected step rejected well-formed input: %v", ctlCorrErr)
			}
			if corrErr == nil {
				t.Fatalf("corrected step accepted the malformed input; premise does not hold")
			}
			if !hasErr {
				t.Errorf("CAUSE: owner %s has no error result (signature %s) and returns %#v for malformed input; the corrected step reports %v", c.owner, sig, got, corrErr)
			}
		})
	}
}
