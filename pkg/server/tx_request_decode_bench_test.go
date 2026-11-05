package server

import (
	"bytes"
	"testing"
)

// BenchmarkTransactionRequestDecode measures decoding a /tx/commit request
// body the way readJSON does: every HTTP Cypher request pays for it.
func BenchmarkTransactionRequestDecode(b *testing.B) {
	bodies := map[string][]byte{
		"no_params":     []byte(`{"statements":[{"statement":"MATCH (n:Person) RETURN n.name AS name LIMIT 10"}]}`),
		"scalar_params": []byte(`{"statements":[{"statement":"MATCH (n:Person {id: $id}) WHERE n.age > $age RETURN n SKIP $s LIMIT $l","parameters":{"id":"p-1","age":30,"s":0,"l":25}}]}`),
		"batch_params":  []byte(`{"statements":[{"statement":"UNWIND $rows AS r CREATE (:P {id: r.id, w: r.w})","parameters":{"rows":[{"id":1,"w":0.5},{"id":2,"w":1.5},{"id":3,"w":2.5},{"id":4,"w":3.5},{"id":5,"w":4.5},{"id":6,"w":5.5},{"id":7,"w":6.5},{"id":8,"w":7.5}]}}]}`),
	}
	for name, body := range bodies {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				var req TransactionRequest
				if err := decodeTransactionRequest(bytes.NewReader(body), &req, transactionRequestLimit{}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
