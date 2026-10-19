package util

import (
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"testing"
)

func TestDigestByteArrays(t *testing.T) {
	data := []byte{0, 1, 127, 128, 255}
	for _, tc := range []struct {
		name  string
		value interface{}
		text  string
	}{
		{"binary", []interface{}{data}, "[0, 1, 127, -128, -1]"},
		{"empty", []interface{}{[]byte{}}, "[]"},
		{"nil", []interface{}{[]byte(nil)}, "[]"},
		{"nested-list", []interface{}{[]interface{}{data}}, "[[0, 1, 127, -128, -1]]"},
		{"nested-map", []interface{}{map[string]interface{}{"data": data}}, "{data=[0, 1, 127, -128, -1]}"},
		{"root-extension", data, "[0, 1, 127, -128, -1]"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := DigestText(tc.value); got != tc.text {
				t.Fatalf("text = %q, want %q", got, tc.text)
			}
			// Hash the source-specified Java text independently of DigestText.
			m, s1, s256 := md5.Sum([]byte(tc.text)), sha1.Sum([]byte(tc.text)), sha256.Sum256([]byte(tc.text))
			for _, algorithm := range []struct {
				name string
				fns  []func(interface{}) string
				want string
			}{
				{"md5", []func(interface{}) string{MD5, Md5Hex, Md5Base64}, hex.EncodeToString(m[:])},
				{"sha1", []func(interface{}) string{SHA1, Sha1Hex, Sha1Base64}, hex.EncodeToString(s1[:])},
				{"sha256", []func(interface{}) string{SHA256, Sha256Hex, Sha256Base64}, hex.EncodeToString(s256[:])},
			} {
				for _, digest := range algorithm.fns {
					if got := digest(tc.value); got != algorithm.want {
						t.Errorf("%s = %s, want %s", algorithm.name, got, algorithm.want)
					}
				}
			}
		})
	}
	if got := MD5([]interface{}{data}); got != "dc234d8d2c22e88c3a7e1f69ab43379c" {
		t.Fatalf("APOC byte-array vector = %s", got)
	}
}

func BenchmarkDigestByteArrays(b *testing.B) {
	for _, tc := range []struct {
		name  string
		value interface{}
	}{
		{"text", []interface{}{"abc", int64(1), true}},
		{"binary", []interface{}{[]byte{0, 1, 127, 128, 255}}},
		{"nested", []interface{}{map[string]interface{}{"data": []byte{0, 1, 127, 128, 255}}}},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = DigestText(tc.value)
			}
		})
	}
}
