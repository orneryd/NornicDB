package cypher

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime/pprof"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Opt-in diagnostic (never committed): CPU profile of healthy-path statements.
func TestZZProfile1391(t *testing.T) {
	if os.Getenv("ZZ_PROFILE_1391") == "" {
		t.Skip("opt-in")
	}
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "bench")
	for i := 0; i < 2000; i++ {
		_, err := store.CreateNode(&storage.Node{ID: storage.NodeID(fmt.Sprintf("person-%d", i)), Labels: []string{"Person"},
			Properties: map[string]interface{}{"id": i, "name": fmt.Sprintf("p%d", i), "age": i % 60}})
		require.NoError(t, err)
	}
	exec := NewStorageExecutor(store)
	for _, q := range []struct{ name, cypher string }{
		{"unwind", "UNWIND range(1, 5000) AS x WITH x WHERE x % 3 = 0 RETURN sum(x) AS total"},
		{"projection", "MATCH (n:Person) RETURN n.name AS name, toUpper(n.name) AS upper, n.age * 2 + 1 AS score, [x IN range(1, 3) | x * n.age] AS multiples"},
		{"literal", "RETURN 1"},
		{"ordermap", "UNWIND range(1, 2000) AS i WITH {k: i % 7, v: i} AS m RETURN m ORDER BY m"},
	} {
		ctx, cancel := context.WithCancel(context.Background())
		path := filepath.Join(t.TempDir(), q.name+".pprof")
		f, err := os.Create(path)
		require.NoError(t, err)
		require.NoError(t, pprof.StartCPUProfile(f))
		deadline := time.Now().Add(3 * time.Second)
		n := 0
		for time.Now().Before(deadline) {
			_, err := exec.Execute(ctx, q.cypher, map[string]interface{}{"nonce": n})
			require.NoError(t, err)
			n++
		}
		pprof.StopCPUProfile()
		f.Close()
		cancel()
		out, _ := exec_command("go", "tool", "pprof", "-top", "-nodecount=35", path)
		t.Logf("=== %s: %d statements in 3s\n%s", q.name, n, out)
		out, _ = exec_command("go", "tool", "pprof", "-top", "-cum", "-nodecount=45", path)
		t.Logf("=== %s cum\n%s", q.name, out)
		if peek := os.Getenv("ZZ_PEEK_1391"); peek != "" && q.name == os.Getenv("ZZ_PEEK_CASE") {
			out, _ = exec_command("go", "tool", "pprof", "-peek", peek, path)
			t.Logf("=== %s peek\n%s", q.name, out)
		}
	}
}

func exec_command(name string, args ...string) (string, error) {
	out, err := exec.Command(name, args...).CombinedOutput()
	return string(out), err
}
