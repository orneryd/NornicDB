package bolt

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/cypher/testutil"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// The client statement shapes over Bolt: a RESET a moment after RUN, and how
// long the server's statement then works on (PARSESCALE route=bolt).
func TestBoltStatementTextParseScaling(t *testing.T) {
	sizes := os.Getenv("NORNICDB_PARSE_SIZES")
	if sizes == "" {
		t.Skip("NORNICDB_PARSE_SIZES lists the statement sizes to measure")
	}
	store := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = store.Close() })
	tracked := &bulkInputExecutor{executor: cypher.NewStorageExecutor(storage.NewNamespacedEngine(store, "test"))}
	server := New(&Config{Port: 0, MaxConnections: 4, ReadBufferSize: 64 << 10, WriteBufferSize: 64 << 10}, tracked)
	port := serveBoltIntegrationServer(t, server)
	stopped := map[string]bool{}
	for _, field := range strings.Split(sizes, ",") {
		size, err := strconv.Atoi(strings.TrimSpace(field))
		require.NoError(t, err)
		for _, shape := range testutil.StatementShapes(size) {
			if stopped[shape.Name] {
				continue
			}
			conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
			require.NoError(t, err)
			require.NoError(t, PerformHandshakeWithTesting(t, conn))
			require.NoError(t, SendHello(t, conn, nil))
			require.NoError(t, ReadSuccess(t, conn))
			began := make(chan struct{})
			tracked.arm(func() { close(began) })
			require.NoError(t, SendMessage(conn, BuildRunMessage(shape.Text, nil, nil)))
			select {
			case <-began:
			case <-time.After(60 * time.Second):
				t.Fatalf("%s: the server never started the statement", shape.Name)
			}
			time.Sleep(5 * time.Millisecond)
			canceledAt := time.Now()
			require.NoError(t, SendMessage(conn, BuildResetMessage()))
			err = tracked.wait()
			late := time.Since(canceledAt)
			_ = conn.Close()
			t.Logf("PARSESCALE shape=%s route=bolt bytes=%d after_cancel=%v outcome=%v", shape.Name, len(shape.Text), late, err)
			if late > 20*time.Second {
				stopped[shape.Name] = true
			}
		}
	}
}
