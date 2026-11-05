package util

import (
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"testing"
)

func TestDigestTypedCypherLists(t *testing.T) {
	type integers []int64
	for _, tc := range []struct {
		name  string
		value interface{}
		text  string
	}{
		{"integers", []int64{1, 2}, "12"},
		{"floats", []float64{1, 2.5}, "1.02.5"},
		{"booleans", []bool{true, false}, "truefalse"},
		{"named", integers{1, 2}, "12"},
		{"nil", []int64(nil), ""},
		{"nested", []interface{}{[]int64{1, 2}, []float64{1, 2.5}}, "[1, 2][1.0, 2.5]"},
		{"map", []interface{}{map[string]interface{}{"items": integers{1, 2}}}, "{items=[1, 2]}"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := DigestText(tc.value); got != tc.text {
				t.Errorf("text: got %q, want %q", got, tc.text)
			}
			bytes := []byte(tc.text)
			m, s1, s256 := md5.Sum(bytes), sha1.Sum(bytes), sha256.Sum256(bytes)
			for _, digest := range []struct {
				fn   func(interface{}) string
				want string
			}{
				{MD5, hex.EncodeToString(m[:])}, {SHA1, hex.EncodeToString(s1[:])}, {SHA256, hex.EncodeToString(s256[:])},
			} {
				if got := digest.fn(tc.value); got != digest.want {
					t.Errorf("digest: got %s, want %s", got, digest.want)
				}
			}
		})
	}
}
