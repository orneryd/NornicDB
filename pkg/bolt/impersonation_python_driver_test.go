package bolt

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type impersonationDriverFactory struct {
	QueryExecutor
	store  storage.Engine
	search *search.Service
}

func (f *impersonationDriverFactory) NewSessionExecutor() QueryExecutor {
	executor := cypher.NewStatementExecutor(f.store)
	executor.SetSearchService(f.search)
	return &transactionalBoltQueryExecutorAdapter{
		boltQueryExecutorAdapter: boltQueryExecutorAdapter{executor: executor},
	}
}

// Run with NORNICDB_NEO4J_PYTHON pointing to a Python environment containing
// testdata/impersonation_driver_requirements.txt. The explicit integration run
// fails if the selected interpreter or driver cannot execute the matrix.
func TestBoltImpersonationOfficialPythonDriver(t *testing.T) {
	python := os.Getenv("NORNICDB_NEO4J_PYTHON")
	if python == "" {
		t.Skip("set NORNICDB_NEO4J_PYTHON for the official Python driver integration")
	}
	f := newImpersonationFixture(t)
	for _, user := range []string{"alice936", "writer936", "bob936", "walled936"} {
		f.grant("svc", auth.ImpersonateUser(user), false)
	}
	base, err := storage.NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	store := storage.NewNamespacedEngine(base, "nornic")
	seed := cypher.NewStatementExecutor(store)
	_, err = seed.Execute(context.Background(), "CREATE (:Document {content: 'python driver retrieval alpha'})-[:TOPIC_GROUPS]->(:Topic {name: 'alpha'})", nil)
	require.NoError(t, err)
	index := search.NewService(store)
	t.Cleanup(func() { require.NoError(t, index.Close()) })
	require.NoError(t, index.BuildIndexes(context.Background()))
	config := *f.server.config
	config.Logger = f.server.log
	server := New(&config, &impersonationDriverFactory{store: store, search: index})
	server.SetResolvedAccessResolver(f.server.resolvedAccessResolver)
	server.SetDatabaseAccessModeResolver(f.server.databaseAccessModeResolver)
	server.SetImpersonationResolver(f.server.impersonationAllowed)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server.listener = listener
	served := make(chan error, 1)
	go func() { served <- server.serve() }()
	closed := false
	closeServer := func() {
		if !closed {
			require.NoError(t, server.Close())
			require.NoError(t, <-served)
			require.Eventually(t, func() bool { return server.activeConnections.Load() == 0 }, 5*time.Second, time.Millisecond)
			closed = true
		}
	}
	t.Cleanup(closeServer)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	command := exec.CommandContext(ctx, python, "testdata/impersonation_driver.py", listener.Addr().String())
	output, err := command.CombinedOutput()
	t.Logf("official Python driver matrix:\n%s", output)
	require.NoError(t, err, string(output))
	closeServer()

	// A pool of size one alone would not prove connection reuse. Correlate
	// the driver's phase markers with the actual TCP endpoint and identity.
	connections := map[string]map[string]bool{}
	for _, line := range strings.Split(f.logs.String(), "\n") {
		var event struct {
			User      string `json:"user"`
			Executing string `json:"executingUser"`
			Remote    string `json:"remote"`
			Params    struct {
				Phase    string `json:"phase"`
				Expected string `json:"expected"`
			} `json:"params"`
		}
		if json.Unmarshal([]byte(line), &event) != nil || !strings.HasPrefix(event.Params.Phase, "pool-") {
			continue
		}
		require.Equal(t, "svc936", event.User)
		executing := event.Executing
		if executing == "" {
			executing = event.User
		}
		require.Equal(t, event.Params.Expected, executing)
		if connections[event.Params.Phase] == nil {
			connections[event.Params.Phase] = map[string]bool{}
		}
		connections[event.Params.Phase][event.Remote] = true
	}
	for _, phase := range []string{"pool-bolt", "pool-neo4j"} {
		require.Len(t, connections[phase], 1, "%s must reuse one actual TCP connection across users", phase)
	}
}
