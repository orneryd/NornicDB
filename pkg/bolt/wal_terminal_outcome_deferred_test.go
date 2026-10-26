// Author: Codex,NOR-180 compiled deferred-consumer classes,29October2026.
package bolt

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/internal/testutil/walterminal"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestBoltWALTerminalDeferredOutcomeValue(t *testing.T) {
	profiles := []walterminal.ContractProfile{
		{Name: "commit", Kinds: []string{"commit"}},
		{Name: "rollback", Kinds: []string{"rollback"}},
		walterminal.ContractProfiles[0], // distinct abort primaries
		walterminal.ContractProfiles[2], // committed child plus aborted child
	}
	for _, profile := range profiles {
		for _, mode := range []string{"pull_completion", "discard", "reset_abandons", "pull_then_reset", "next_run_abandons"} {
			for _, closed := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/closed=%v", profile.Name, mode, closed), func(t *testing.T) {
					readEngine := walterminal.New(t)
					executor := newWALConsumerExecutor(readEngine)
					var single *cypher.StorageExecutor
					var singleEngine *walterminal.Engine
					var aggregate *walterminal.OutcomeContract
					if len(profile.Kinds) == 1 {
						singleEngine = walterminal.New(t)
						single = cypher.NewStorageExecutor(storage.NewNamespacedEngine(singleEngine, "nornic"))
						_, err := single.Execute(context.Background(), "BEGIN", nil)
						require.NoError(t, err)
						_, err = single.Execute(context.Background(), "CREATE (:WalWire)", nil)
						require.NoError(t, err)
					} else {
						aggregate = walterminal.NewOutcomeContract(t, profile, closed, false)
					}
					observed := make(chan error, 1)
					produced := make(chan struct{})
					const nextQuery = "RETURN 42 AS next_value"
					executor.run = func(ctx context.Context, query string, params map[string]any) (*QueryResult, error) {
						if query == nextQuery {
							return executor.transactionalBoltQueryExecutorAdapter.Execute(ctx, query, params)
						}
						result, runErr := executor.transactionalBoltQueryExecutorAdapter.Execute(ctx, query, params)
						close(produced)
						if mode != "pull_completion" {
							<-ctx.Done()
						}
						var terminalErr error
						if single != nil {
							if closed {
								if err := singleEngine.WAL.Close(); err != nil {
									return nil, err
								}
							}
							command := "ROLLBACK"
							if profile.Name == "commit" {
								command = "COMMIT"
							}
							_, terminalErr = single.Execute(cypher.WithTransactionControl(context.Background()), command, nil)
						} else {
							outer := cypher.NewStorageExecutor(storage.NewNamespacedEngine(readEngine, "nornic"))
							_, terminalErr = outer.Execute(context.Background(), aggregate.Flow.Query, nil)
						}
						err := errors.Join(runErr, terminalErr)
						if mode != "pull_completion" {
							err = errors.Join(ctx.Err(), err)
						}
						observed <- err
						return result, err
					}
					conn := openBoltTestConn(t, startControlledTransactionServer(t, executor))
					require.NoError(t, SendRun(t, conn, "UNWIND range(1,1200) AS i RETURN i", nil, nil))
					require.NoError(t, ReadSuccess(t, conn), "actual read reached the lazy threshold")
					if mode == "pull_completion" || mode == "pull_then_reset" {
						require.NoError(t, SendPull(t, conn, map[string]any{"n": int64(-1)}))
						if mode == "pull_then_reset" {
							select {
							case <-produced:
							case <-time.After(5 * time.Second):
								t.Fatal("stream did not produce its rows")
							}
						}
					}
					switch mode {
					case "discard":
						sendDiscard(t, conn, -1)
					case "reset_abandons", "pull_then_reset":
						require.NoError(t, SendReset(t, conn))
					case "next_run_abandons":
						require.NoError(t, SendRun(t, conn, nextQuery, nil, nil))
					}
					var kind byte
					var metadata map[string]any
					for {
						var data []byte
						var err error
						kind, data, err = ReadMessage(conn)
						require.NoError(t, err)
						if kind == MsgRecord {
							continue
						}
						if kind != MsgIgnored {
							metadata, _, err = decodePackStreamMap(data, 0)
							require.NoError(t, err)
						}
						break
					}
					var actual error
					select {
					case actual = <-observed:
					case <-time.After(5 * time.Second):
						t.Fatal("provider terminal owner did not complete")
					}
					// Establish actual native truth before checking any classification.
					if single != nil {
						want := int64(0)
						if profile.Name == "commit" {
							want = 1
						}
						require.Equal(t, want, walterminal.Count(t, singleEngine))
						require.False(t, single.HasActiveTransaction())
					} else {
						aggregate.RequireNative(t)
					}
					if closed {
						require.ErrorIs(t, actual, storage.ErrWALClosed)
						require.Equal(t, byte(MsgFailure), kind)
						code, _ := metadata["code"].(string)
						message, _ := metadata["message"].(string)
						if single != nil {
							if profile.Name == "commit" {
								walterminal.RequireCommitMarkerFailure(t, message)
							} else {
								require.Contains(t, message, "failed to write WAL tx abort")
							}
							want, _ := walterminal.ContractStatus(profile.Name)
							require.Equal(t, want, code, "classification belongs to the completed native operation")
						} else {
							aggregate.RequireIdentity(t, actual)
							aggregate.RequireView(t, code, message)
						}
					} else if mode == "pull_then_reset" {
						require.Equal(t, byte(MsgIgnored), kind)
					} else {
						require.Equal(t, byte(MsgSuccess), kind)
					}
					if mode == "pull_then_reset" {
						require.NoError(t, ReadSuccess(t, conn))
					}
				})
			}
		}
	}
}
