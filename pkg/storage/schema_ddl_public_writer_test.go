package storage_test

// B21 public transport regression: an autocommit CREATE INDEX and an unrelated
// data writer, both issued through a real Bolt or HTTP client.
//
// The native begin admits an autocommit schema statement exclusively before its
// snapshot (storage.AutocommitSchemaBegin), so the statement's window holds no
// entity commit. A data writer that arrives inside that window is admitted only
// after the statement publishes, and the index it publishes therefore never
// rejects the statement with "schema snapshot changed before commit". The
// in-package schedule test (schema_ddl_data_writer_test.go) pins that mechanism
// at the fill's entry point; this file proves both public routes reach it,
// including the write-behind configuration whose acknowledged commits the begin
// applies before it takes its exclusive hold.
//
// Ordering is exact: the DDL's fill hook (storage.SetSchemaIndexFillHookForTesting)
// signals the statement's entry into its window, and the writer is launched from
// there. The one bounded wait that is not a join guard observes that the writer
// does not complete while the window is open; a positive requirement is never
// satisfied by waiting.
//
// The Cypher parser is process-wide (config.init), so both parsers run these
// identical statements in separate NORNICDB_PARSER=nornic and
// NORNICDB_PARSER=antlr runs of this test.

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

const (
	// publicDDLJoinBudget bounds a wait for a launched public statement.
	publicDDLJoinBudget = 60 * time.Second
	// publicDDLWindowBudget bounds the observation that no data commit lands
	// inside the schema statement's window. It never satisfies a positive
	// requirement: the statements' own outcomes are asserted by their joins.
	publicDDLWindowBudget = time.Second
)

// publicDDLStatement is one statement launched through a public client.
type publicDDLStatement struct {
	done chan struct{}
	mu   sync.Mutex
	err  error
}

func launchPublicStatement(run func() ([][]interface{}, error)) *publicDDLStatement {
	statement := &publicDDLStatement{done: make(chan struct{})}
	go func() {
		_, err := run()
		statement.mu.Lock()
		statement.err = err
		statement.mu.Unlock()
		close(statement.done)
	}()
	return statement
}

// join waits for the launched statement's outcome.
func (s *publicDDLStatement) join(t *testing.T, what string) error {
	t.Helper()
	select {
	case <-s.done:
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.err
	case <-time.After(publicDDLJoinBudget):
		t.Fatalf("%s did not return within %s", what, publicDDLJoinBudget)
		return nil
	}
}

// TestPublicDDLCommitSurvivesConcurrentDataWriter runs the regression over both
// public transports, with write-behind off and on.
func TestPublicDDLCommitSurvivesConcurrentDataWriter(t *testing.T) {
	for _, writeBehind := range []bool{false, true} {
		for _, route := range []string{"Bolt", "HTTP"} {
			t.Run(fmt.Sprintf("writeBehind=%v/%s", writeBehind, route), func(t *testing.T) {
				fixture := openProtocolServerFixture(t, protocolServerOptions{writeBehind: writeBehind})
				fixture.seed(t, "CREATE (:W {v:'seed'})")
				ddl := pr1655OpenPublicClient(t, fixture.server, route)
				writer := pr1655OpenPublicClient(t, fixture.server, route)

				entered := make(chan struct{})
				release := make(chan struct{})
				var enterOnce, releaseOnce sync.Once
				unpark := func() { releaseOnce.Do(func() { close(release) }) }
				restore := storage.SetSchemaIndexFillHookForTesting(func(name, _ string) error {
					if name != "w_v" {
						return nil
					}
					enterOnce.Do(func() { close(entered) })
					<-release
					return nil
				})
				// A failed assertion leaves no statement parked and no hook set.
				t.Cleanup(func() {
					unpark()
					restore()
				})

				staged := launchPublicStatement(func() ([][]interface{}, error) {
					return ddl.run("CREATE INDEX w_v FOR (n:W) ON (n.v)")
				})
				select {
				case <-entered:
				case <-time.After(publicDDLJoinBudget):
					t.Fatalf("the DDL never entered its index fill within %s", publicDDLJoinBudget)
				}

				// The statement is inside its window; an unrelated autocommit
				// data write arrives there and must wait for publication.
				data := launchPublicStatement(func() ([][]interface{}, error) {
					return writer.run("CREATE (:W {v:'peer'})")
				})
				select {
				case <-data.done:
					t.Fatalf("a data commit completed inside the schema statement's window: %v", data.join(t, "the data writer"))
				case <-time.After(publicDDLWindowBudget):
				}

				unpark()
				require.NoError(t, staged.join(t, "the DDL statement"), "the DDL must commit while a data writer waits for it")
				require.NoError(t, data.join(t, "the data writer"), "the data write must commit after the DDL published")

				// A write-behind acknowledgement may complete the data write
				// before its replay applies it: drain through the existing public
				// flush, then assert the published index holds both nodes.
				require.NoError(t, fixture.engine.FlushWriteBehind(context.Background()))
				schema := fixture.engine.GetSchemaForNamespace("nornic")
				_, published := schema.GetCompositeIndex("w_v")
				require.True(t, published, "the DDL must publish its index")
				for _, value := range []string{"seed", "peer"} {
					ids, err := schema.PropertyIndexLookup(context.Background(), "W", "v", value)
					require.NoError(t, err)
					require.Lenf(t, ids, 1, "the published index must hold the %q node", value)
				}
			})
		}
	}
}
