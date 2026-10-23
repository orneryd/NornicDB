package bolt

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"strings"
	"sync"
	"testing"

	neo4jdriver "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Review probe (test-only, r73 35f7/5d36): integer and float values written
// through Cypher and through Go parameters read identically warm and cold on
// every public route; Bolt sees canonical INTEGER/FLOAT values.
func TestReviewClaude35f7IntegerPublicRoutes(t *testing.T) {
	dir := t.TempDir()
	cypherKeys := []string{"i8max", "i8min", "u8", "n129", "i16max", "u16", "n32769", "i32max", "u32", "i64max", "i64min", "neg", "zero", "f3", "fneg", "l", "lrev", "lf", "rng", "sz", "ti"}
	goKeys := []string{"gp", "gpl", "gpm", "gu", "gbig", "gf"}
	keys := append(append([]string{}, cypherKeys...), goKeys...)
	var ret, types []string
	for _, k := range keys {
		ret = append(ret, "n."+k)
		types = append(types, "valueType(n."+k+")")
	}
	valuesQuery := "MATCH (n:R73P {key:'p'}) RETURN " + strings.Join(ret, ", ")
	typesQuery := "MATCH (n:R73P {key:'p'}) RETURN " + strings.Join(types, ", ")
	snapshots := map[string]string{}
	record := func(t *testing.T, phase, route string, value any) {
		t.Helper()
		got := fmt.Sprintf("%#v", value)
		t.Logf("%s/%s: %s", phase, route, got)
		if phase == "warm" {
			snapshots[route] = got
			return
		}
		if snapshots[route] != got {
			t.Errorf("ROUTE %s COLD != WARM\n warm=%s\n cold=%s", route, snapshots[route], got)
		}
	}
	for _, phase := range []string{"warm", "cold"} {
		t.Run(phase, func(t *testing.T) {
			base, err := storage.NewBadgerEngine(dir)
			require.NoError(t, err)
			t.Cleanup(func() { _ = base.Close() })
			store := storage.NewNamespacedEngine(base, "nornic")
			exec := cypher.NewStorageExecutorWithQueryCachePolicy(store, 0, 0)
			ctx := context.Background()
			if phase == "warm" {
				_, err = exec.Execute(ctx, "CREATE (n:R73P {key:'p', i8max:127, i8min:-128, u8:128, n129:-129, i16max:32767, u16:32768, n32769:-32769, i32max:2147483647, u32:2147483648, i64max:9223372036854775807, i64min:-9223372036854775807-1, neg:-1, zero:0, f3:3.0, fneg:-2.5, l:[128,3,-129], lrev:[3,128], lf:[1.0,2.0], rng:range(126,129), sz:size([1,2,3]), ti:toInteger('300')})", nil)
				require.NoError(t, err)
				for key, value := range map[string]any{"gp": int(128), "gpl": []int{128, 3}, "gpm": []any{int(-129), int(128)}, "gu": uint64(5), "gbig": uint64(math.MaxUint64), "gf": float32(1.5)} {
					if _, err := exec.Execute(ctx, "MATCH (n:R73P {key:'p'}) SET n."+key+" = $v", map[string]any{"v": value}); err != nil {
						t.Logf("SET %s (%T) rejected: %v", key, value, err)
					}
				}
				_, err = exec.Execute(ctx, "CREATE CONSTRAINT r73u FOR (u:R73U) REQUIRE u.k IS UNIQUE", nil)
				require.NoError(t, err)
				_, err = store.CreateNode(&storage.Node{ID: "u1", Labels: []string{"R73U"}, Properties: map[string]any{"k": int(128)}})
				require.NoError(t, err)
			}
			result, err := exec.Execute(ctx, valuesQuery, nil)
			require.NoError(t, err)
			require.Len(t, result.Rows, 1)
			record(t, phase, "execute-values", result.Rows[0])
			if result, err := exec.Execute(ctx, typesQuery, nil); err == nil && len(result.Rows) == 1 {
				record(t, phase, "execute-valueType", result.Rows[0])
			} else {
				t.Logf("valueType query: %v", err)
			}
			nodes, err := store.GetNodesByLabel("R73P")
			require.NoError(t, err)
			require.Len(t, nodes, 1)
			props := map[string]string{}
			for _, k := range keys {
				props[k] = fmt.Sprintf("%#v", nodes[0].Properties[k])
			}
			record(t, phase, "storage-get", props)
			tx, err := base.BeginTransaction()
			require.NoError(t, err)
			txNode, err := tx.GetNode(storage.NodeID("nornic:" + string(nodes[0].ID)))
			require.NoError(t, err)
			txProps := map[string]string{}
			for _, k := range keys {
				txProps[k] = fmt.Sprintf("%#v", txNode.Properties[k])
			}
			_ = tx.Rollback()
			record(t, phase, "storage-tx", txProps)
			for _, stmt := range []string{"CREATE (:R73U {k:128})", "CREATE (:R73U {k:128.0})", "MERGE (u:R73U {k:128}) RETURN count(u)"} {
				res, err := exec.Execute(ctx, stmt, nil)
				t.Logf("%s unique %q: err=%v", phase, stmt, err)
				if strings.HasPrefix(stmt, "CREATE") && err == nil {
					t.Errorf("UNIQUE %s: %q accepted a duplicate of Go int(128)", phase, stmt)
				}
				if strings.HasPrefix(stmt, "MERGE") && err == nil {
					count, _ := store.GetNodesByLabel("R73U")
					if len(count) != 1 {
						t.Errorf("UNIQUE %s: MERGE created a second node (%d nodes, rows=%v)", phase, len(count), res.Rows)
					}
				}
			}
			mgr, err := multidb.NewDatabaseManager(base, nil)
			require.NoError(t, err)
			t.Cleanup(func() { _ = mgr.Close() })
			server := NewWithDatabaseManager(&Config{Port: 0, MaxConnections: 4, ReadBufferSize: 8192, WriteBufferSize: 8192}, &mockExecutor{}, mgr)
			port := startBoltTestServer(t, server)
			driver, err := neo4jdriver.NewDriverWithContext(fmt.Sprintf("bolt://127.0.0.1:%d", port), neo4jdriver.NoAuth())
			require.NoError(t, err)
			t.Cleanup(func() { _ = driver.Close(ctx) })
			for _, mode := range []string{"auto", "explicit"} {
				session := driver.NewSession(ctx, neo4jdriver.SessionConfig{DatabaseName: "nornic", AccessMode: neo4jdriver.AccessModeRead})
				var values []any
				if mode == "auto" {
					res, err := session.Run(ctx, valuesQuery, nil)
					require.NoError(t, err)
					rec, err := res.Single(ctx)
					require.NoError(t, err)
					values = rec.Values
				} else {
					got, err := session.ExecuteRead(ctx, func(tx neo4jdriver.ManagedTransaction) (any, error) {
						res, err := tx.Run(ctx, valuesQuery, nil)
						if err != nil {
							return nil, err
						}
						rec, err := res.Single(ctx)
						if err != nil {
							return nil, err
						}
						return rec.Values, nil
					})
					require.NoError(t, err)
					values = got.([]any)
				}
				_ = session.Close(ctx)
				record(t, phase, "bolt-"+mode, values)
				for i, k := range keys {
					switch v := values[i].(type) {
					case int64, float64, string, nil:
					case []any:
						for _, item := range v {
							switch item.(type) {
							case int64, float64, string:
							default:
								t.Errorf("BOLT %s %s: list item %#v is %T", phase, k, item, item)
							}
						}
					default:
						t.Errorf("BOLT %s %s: %#v is %T", phase, k, v, v)
					}
				}
				if !reflect.DeepEqual(values[keys2index(keys, "f3")], float64(3)) {
					t.Errorf("BOLT %s f3 lost FLOAT: %#v", phase, values[keys2index(keys, "f3")])
				}
			}
		})
	}
}

func keys2index(keys []string, key string) int {
	for i, k := range keys {
		if k == key {
			return i
		}
	}
	return -1
}

// Go embedding API: concurrent statements that share one parameter map.
func TestReviewClaude35f7SharedParamsConcurrentExecute(t *testing.T) {
	base, err := storage.NewBadgerEngine(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = base.Close() })
	store := storage.NewNamespacedEngine(base, "nornic")
	exec := cypher.NewStorageExecutorWithQueryCachePolicy(store, 0, 0)
	params := map[string]any{"props": map[string]any{"a": int64(1), "n": int(3), "s": "x"}}
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				if _, err := exec.Execute(context.Background(), "CREATE (n:SP $props)", params); err != nil {
					t.Error(err)
				}
			}
		}()
	}
	wg.Wait()
	t.Logf("shared params after: n=%T", params["props"].(map[string]any)["n"])
}
