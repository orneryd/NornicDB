package nornicdb

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Section F27. Reopen a store after the acknowledged create, delete and
// re-create sequence is interrupted before close. The subprocess is the test
// binary for this same build, so the probe needs no separate checkout.
func TestV152PortRecreatedRelationshipAfterCrash(t *testing.T) {
	const dirEnv = "NORNICDB_V152_PORT_CRASH_CHILD"
	const ready = "V152_PORT_STORE_READY"
	cfg := DefaultConfig()
	cfg.Database.AsyncWritesEnabled = false
	cfg.Memory.EmbeddingEnabled = false
	cfg.Memory.DecayEnabled = false
	cfg.Memory.AutoLinksEnabled = false

	if dir := os.Getenv(dirEnv); dir != "" {
		db, err := Open(dir, cfg)
		require.NoError(t, err)
		for _, query := range []string{
			"CREATE (:P {id:'a'}),(:P {id:'b'})",
			"MATCH (a:P {id:'a'}),(b:P {id:'b'}) MERGE (a)-[:LINK]->(b)",
			"MATCH ()-[r:LINK]->() DELETE r",
			"MATCH (a:P {id:'a'}),(b:P {id:'b'}) MERGE (a)-[:LINK]->(b)",
			"MATCH (a:P)-[r:LINK]->(b:P) SET r.checked=true RETURN a.id,b.id",
		} {
			_, err := db.ExecuteCypher(t.Context(), query, nil)
			require.NoError(t, err, query)
		}
		fmt.Println(ready)
		for {
			time.Sleep(time.Hour)
		}
	}

	dir := t.TempDir()
	bin, err := os.Executable()
	require.NoError(t, err)
	cmd := exec.CommandContext(t.Context(), bin, "-test.run=^TestV152PortRecreatedRelationshipAfterCrash$", "-test.v")
	cmd.Env = append(os.Environ(), dirEnv+"="+dir)
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	cmd.Stderr = cmd.Stdout
	require.NoError(t, cmd.Start())
	done := make(chan bool, 1)
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			line := scanner.Text()
			if line == ready {
				done <- true
				return
			}
			t.Log(line)
		}
		done <- false
	}()
	select {
	case childReady := <-done:
		if !childReady {
			t.Fatalf("seed process exited before acknowledging the writes: %v", cmd.Wait())
		}
	case <-time.After(20 * time.Second):
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		<-done
		t.Fatal("seed process did not acknowledge the writes")
	}
	require.NoError(t, cmd.Process.Kill())
	require.Error(t, cmd.Wait(), "the seed was killed before database close")

	db, err := Open(dir, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	for _, query := range []string{
		"MATCH (a:P)-[r:LINK]->(b:P) RETURN a.id,b.id,r.checked",
		"MATCH (a:P)-[r:LINK]->(b:P) SET r.checked=true RETURN a.id,b.id,r.checked",
	} {
		result, err := db.ExecuteCypher(t.Context(), query, nil)
		require.NoError(t, err)
		require.Equal(t, [][]any{{"a", "b", true}}, result.Rows)
	}
}
