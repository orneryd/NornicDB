package jsontext

import (
	"fmt"
	"testing"
)

var ingestProjectionSink []byte

// BenchmarkJSONMarshalBufferedWrites exercises the mixed valid and invalid
// string bytes present in buffered write keys, plus byte-valued records.
func BenchmarkJSONMarshalBufferedWrites(b *testing.B) {
	writes := make(map[string][]byte, 2048)
	for i := 0; i < 2048; i++ {
		key := fmt.Sprintf("db:node:%04d:", i) + string([]byte{0x80 + byte(i%64), 0xfe})
		writes[key] = []byte("representative binary record payload")
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		data, err := Marshal(writes)
		if err != nil {
			b.Fatal(err)
		}
		ingestProjectionSink = data
	}
}
