package storage

import (
	"bytes"
	"testing"

	"github.com/vmihailenco/msgpack/v5"
)

func BenchmarkDecodeStrictIntegerValues(b *testing.B) {
	values := make([]int64, 1024)
	for i := range values {
		values[i] = int64(i - len(values)/2)
	}
	encoded, err := msgpack.Marshal(values)
	if err != nil {
		b.Fatal(err)
	}
	reader := bytes.NewReader(encoded)
	decoder := msgpack.NewDecoder(reader)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reader.Reset(encoded)
		decoder.Reset(reader)
		if _, err := decodeStrictTypedValue(decoder); err != nil {
			b.Fatal(err)
		}
	}
}
