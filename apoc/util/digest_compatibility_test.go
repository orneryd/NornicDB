package util

import "testing"

func TestDigestAPOCListHashes(t *testing.T) {
	values := []interface{}{"a", "b", nil, "c"}
	for _, tc := range []struct {
		name   string
		digest func(interface{}) string
		want   string
	}{
		{"md5", MD5, "900150983cd24fb0d6963f7d28e17f72"},
		{"sha1", SHA1, "a9993e364706816aba3e25717850c26c9cd0d89d"},
		{"sha256", SHA256, "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"},
	} {
		if got := tc.digest(values); got != tc.want {
			t.Errorf("%s: got %s, want %s", tc.name, got, tc.want)
		}
	}
}
