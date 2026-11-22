package text

import (
	"strconv"
	"strings"
	"testing"
)

var n68TextSink string
var n68IndexSink int

func BenchmarkN68NonRegex(b *testing.B) {
	for _, size := range []int{1, 64} {
		input := strings.Repeat("Hello É ä λ-42! ", size)
		for _, tc := range []struct {
			name string
			call func(string) string
		}{{"Clean", Clean}, {"Camel", CamelCase}, {"UpperCamel", UpperCamelCase}, {"URL", Urlencode}, {"Base64", Base64Encode}} {
			b.Run(tc.name+"/"+strconv.Itoa(size), func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					n68TextSink = tc.call(input)
				}
			})
		}
		b.Run("Index/"+strconv.Itoa(size), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				n68IndexSink = IndexOf(input, "42")
			}
		})
	}
}
