package cypher

import (
 "context"
 "fmt"
 "os"
 "os/exec"
 "path/filepath"
 "runtime"
 "runtime/pprof"
 "testing"

 "github.com/orneryd/nornicdb/pkg/storage"
)

// Diagnostic only; remove before publishing.
func TestU4LexicalNumericProfile(t *testing.T) {
 if os.Getenv("NORNICDB_U4_LEXICAL_PROFILE") == "" { t.Skip("explicit remote diagnostic") }
 engine := NewStorageExecutorWithQueryCachePolicy(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"), 0, 0)
 ctx := context.Background()
 if _, err := engine.Execute(ctx, "UNWIND range(1, 1000) AS i CREATE (:FnRow {name: 'name ' + toString(i), n: i, d: date('2020-01-01') + duration({days: i % 300})})", nil); err != nil { t.Fatal(err) }
 const query = "MATCH (r:FnRow) RETURN abs(r.n - 500) AS a, round(r.n / 7.0) AS b, coalesce(r.missing, r.n) AS c, sqrt(r.n) AS d"
 run := func() {
  result, err := engine.Execute(ctx, query, nil)
  if err != nil { t.Fatal(err) }
  if len(result.Rows) != 1000 { t.Fatalf("rows=%d",len(result.Rows)) }
 }
 run()
 dir := t.TempDir()
 prior := runtime.MemProfileRate
 runtime.MemProfileRate = 512
 defer func() { runtime.MemProfileRate = prior }()
 write := func(name string) string {
  runtime.GC(); runtime.GC()
  name = filepath.Join(dir,name)
  file,err := os.Create(name); if err != nil { t.Fatal(err) }
  if err := pprof.Lookup("allocs").WriteTo(file,0); err != nil { t.Fatal(err) }
  if err := file.Close(); err != nil { t.Fatal(err) }
  return name
 }
 before := write("before.pprof")
 for i := 0; i < 100; i++ { run() }
 after := write("after.pprof")
 binary,err := os.Executable(); if err != nil { t.Fatal(err) }
 for _, args := range [][]string {
  {"tool","pprof","-top","-alloc_objects","-nodecount=80","-base="+before,binary,after},
  {"tool","pprof","-top","-alloc_space","-nodecount=80","-base="+before,binary,after},
  {"tool","pprof","-list=appendFunctionArguments|appendArguments|singleFunctionArgument|splitFunctionArgs|parseFunctionCallWS","-alloc_objects","-base="+before,binary,after},
 } {
  output,err := exec.Command("go",args...).CombinedOutput()
  fmt.Printf("U4_NUMERIC_PROFILE %v\n%s\n",args[1:],output)
  if err != nil { t.Fatal(err) }
 }
}
