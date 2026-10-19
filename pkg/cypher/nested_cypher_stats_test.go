package cypher

import (
 "context"
 "testing"
 "github.com/orneryd/nornicdb/testing/cypher/nestedstats"
 "github.com/stretchr/testify/require"
)

func TestNestedCypherUpdateStats(t *testing.T) {
 for _, tc := range nestedstats.Cases() {
  t.Run(tc.Name, func(t *testing.T) {
   run := func(query string) *QueryStats {
    exec := NewStorageExecutor(newTestMemoryEngine(t))
    if tc.Setup != "" { _, err := exec.Execute(context.Background(),tc.Setup,nil); require.NoError(t,err) }
    result, err := exec.Execute(context.Background(),query,tc.Params)
    require.NoError(t,err)
    if result.Stats == nil { return &QueryStats{} }
    return result.Stats
   }
   require.Equal(t,run(tc.Direct),run(tc.Query))
  })
 }
}

func BenchmarkNestedCypherRun(b *testing.B) {
 store := newTestMemoryEngine(b)
 exec := NewStorageExecutor(store)
 ctx := context.Background()
 params := map[string]any{"body":"RETURN 1 AS value"}
 b.ReportAllocs()
 b.ResetTimer()
 for i:=0; i<b.N; i++ {
  if _,err:=exec.Execute(ctx,"CALL apoc.cypher.run($body,{}) YIELD value RETURN value",params);err!=nil { b.Fatal(err) }
 }
}
