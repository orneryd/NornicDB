package cypher

import (
	"context"
	"testing"
)

// BenchmarkVectorQueryInputResolution measures a vector query procedure from
// its public entry point: argument binding, third-argument resolution and the
// procedure's own search. Both procedures and both input sizes are measured.
// The missing-index case isolates binding and resolution from the search: the
// statement resolves its third argument before it looks the index up.
func BenchmarkVectorQueryInputResolution(b *testing.B) {
	ctx := context.Background()
	for _, size := range []struct {
		name string
		dims int
	}{
		{name: "small", dims: 4},
		{name: "large", dims: 256},
	} {
		exec := newVectorInputExecutor(b, size.dims)
		exec.SetEmbedder(&mockQueryEmbedder{embedding: axisVector32(size.dims, 0)})
		literal := vectorLiteral(size.dims, 0)
		param := map[string]interface{}{"q": axisVector32(size.dims, 0)}
		for _, procedure := range vectorInputProcedures {
			cases := []struct {
				name    string
				value   string
				params  map[string]interface{}
				index   string
				wantErr bool
			}{
				{name: "literal", value: literal, index: procedure.index},
				{name: "param", value: "$q", params: param, index: procedure.index},
				{name: "search-text", value: "'benchmark query text'", index: procedure.index},
				{name: "missing-index", value: literal, index: "missing_" + procedure.index, wantErr: true},
			}
			for _, input := range cases {
				query := vectorInputQuery(procedure.procedure, input.index, "10", input.value)
				b.Run(size.name+"/"+procedure.name+"/"+input.name, func(b *testing.B) {
					b.ReportAllocs()
					for i := 0; i < b.N; i++ {
						result, err := exec.Execute(ctx, query, input.params)
						switch {
						case input.wantErr && err == nil:
							b.Fatal("a vector query on a missing index succeeded")
						case !input.wantErr && err != nil:
							b.Fatal(err)
						case !input.wantErr && result == nil:
							b.Fatal("a vector query returned no result")
						}
					}
				})
			}
		}
	}
}
