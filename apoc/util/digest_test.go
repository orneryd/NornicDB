package util

import (
	"math"
	"testing"
)

// DigestText is APOC's: the list's values' Java strings concatenated, a
// null adding nothing.
func TestDigestTextIsAPOCConcatenation(t *testing.T) {
	cases := []struct {
		value interface{}
		want  string
	}{
		{[]interface{}{"a", "b", nil, "c"}, "abc"},
		{[]interface{}{"ab", int64(1), true, 2.5, 1.0, nil}, "ab1true2.51.0"},
		{[]interface{}{[]interface{}{int64(1), "a", nil}}, "[1, a, null]"},
		{[]interface{}{map[string]interface{}{"b": int64(2), "a": "x"}}, "{a=x, b=2}"},
		{[]string{"x", "y"}, "xy"},
		{"hello", "hello"},
		{nil, ""},
	}
	for _, c := range cases {
		if got := DigestText(c.value); got != c.want {
			t.Errorf("DigestText(%#v) = %q, want %q", c.value, got, c.want)
		}
	}
	if got := SHA256([]interface{}{"abc"}); got != "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad" {
		t.Errorf("SHA256 = %s", got)
	}
	if got := MD5([]interface{}{"abc"}); got != "900150983cd24fb0d6963f7d28e17f72" {
		t.Errorf("MD5 = %s", got)
	}
	if got := SHA1([]interface{}{"abc"}); got != "a9993e364706816aba3e25717850c26c9cd0d89d" {
		t.Errorf("SHA1 = %s", got)
	}
}

// JavaDoubleString is Java's Double.toString.
func TestJavaDoubleString(t *testing.T) {
	for value, want := range map[float64]string{
		1.0: "1.0", 1e7: "1.0E7", 1.5e7: "1.5E7", 0.001: "0.001", 1e-4: "1.0E-4", 123.456: "123.456",
		9999999.0: "9999999.0", 1.2345e-10: "1.2345E-10", -2.5e300: "-2.5E300", 0: "0.0",
		math.Copysign(0, -1): "-0.0", math.Inf(1): "Infinity", math.Inf(-1): "-Infinity",
	} {
		if got := JavaDoubleString(value); got != want {
			t.Errorf("JavaDoubleString(%v) = %q, want %q", value, got, want)
		}
	}
	if got := JavaDoubleString(math.NaN()); got != "NaN" {
		t.Errorf("NaN = %q", got)
	}
}
