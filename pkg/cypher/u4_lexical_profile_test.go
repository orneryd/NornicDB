package cypher

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strconv"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// Diagnostic only: committed to an immutable TrueNAS input, removed before PR.
func TestU4LexicalColdProfile(t *testing.T) {
	if os.Getenv("NORNICDB_U4_LEXICAL_PROFILE") == "" { t.Skip("explicit remote diagnostic") }
	dir := t.TempDir()
	prior := runtime.MemProfileRate
	runtime.MemProfileRate = 2048
	defer func() { runtime.MemProfileRate = prior }()
	engine := NewStorageExecutorWithQueryCachePolicy(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"), 0, 0)
	writeAllocs := func(name string) string {
		runtime.GC()
		runtime.GC()
		path := filepath.Join(dir, name)
		file, err := os.Create(path)
		if err != nil { t.Fatal(err) }
		if err := pprof.Lookup("allocs").WriteTo(file, 0); err != nil { t.Fatal(err) }
		if err := file.Close(); err != nil { t.Fatal(err) }
		return path
	}
	before := writeAllocs("before.pprof")
	cpuPath := filepath.Join(dir, "cpu.pprof")
	cpu, err := os.Create(cpuPath)
	if err != nil { t.Fatal(err) }
	if err := pprof.StartCPUProfile(cpu); err != nil { t.Fatal(err) }
	for i := 0; i < 2000; i++ {
		query := functionLookupStatements[0].query + ", " + strconv.Itoa(i) + " AS n"
		if _, err := engine.Execute(context.Background(), query, nil); err != nil { t.Fatal(err) }
	}
	pprof.StopCPUProfile()
	if err := cpu.Close(); err != nil { t.Fatal(err) }
	after := writeAllocs("after.pprof")
	binary, err := os.Executable()
	if err != nil { t.Fatal(err) }
	for _, args := range [][]string{
		{"tool", "pprof", "-top", "-nodecount=35", binary, cpuPath},
		{"tool", "pprof", "-top", "-alloc_space", "-nodecount=35", "-base="+before, binary, after},
		{"tool", "pprof", "-top", "-cum", "-alloc_space", "-nodecount=35", "-base="+before, binary, after},
		{"tool", "pprof", "-list=readFunctionSyntax", "-alloc_space", "-base="+before, binary, after},
		{"tool", "pprof", "-peek=readFunctionSyntax", "-alloc_space", "-base="+before, binary, after},
	} {
		output, err := exec.Command("go", args...).CombinedOutput()
		fmt.Printf("U4_PROFILE %v\n%s\n", args[1:], output)
		if err != nil { t.Fatal(err) }
	}
}
