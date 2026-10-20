package storage

import (
	"encoding/json"
	"testing"
)

var remoteHTTPNumericBenchmarkSink interface{}

func BenchmarkRemoteHTTPExactNumbers(b *testing.B) {
	for _, tc := range []struct{ name, raw string }{
		{"ordinary", `{"v":[42,1.5,0]}`},
		{"large", `{"v":[9007199254740993,9223372036854775807,-9007199254740993]}`},
	} {
		b.Run(tc.name, func(b *testing.B) {
			raw := json.RawMessage(tc.raw)
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				index := 0
				var err error
				remoteHTTPNumericBenchmarkSink, err = decodeRemoteHTTPValue(raw, nil, &index, nil)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
