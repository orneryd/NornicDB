package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// The r11 schedules enter through each public transport. Persistence is read
// from a separate session after the enclosing owner rolls back or fails.
func TestStorageMembershipPublicRoutes(t *testing.T) {
	ctx := context.Background()
	t.Cleanup(cypher.ClearUserProcedures)
	s, auth := setupTestServer(t)
	token := "Bearer " + getAuthToken(t, auth, "admin")
	manager := s.databaseManager()
	require.NoError(t, s.dbManager.CreateDatabase(ctx, multidb.DatabaseCreateRequest{Name: "smone"}))
	require.NoError(t, s.dbManager.CreateCompositeDatabase(ctx, "smcomp", []multidb.ConstituentRef{
		{Alias: "one", DatabaseName: "smone", Type: "local", AccessMode: "read_write"},
	}))
	for _, rebuild := range []bool{false, true} {
		name := "membership.direct"
		if rebuild {
			name = "membership.rebuilt"
		}
		rebuild := rebuild
		require.NoError(t, cypher.RegisterUserProcedure(cypher.ProcedureSpec{
			Name: name, Signature: name + "(body :: STRING) :: (v :: INTEGER)",
			Mode: cypher.ProcedureModeRead, ExecutesStatements: true,
			Params: []cypher.ProcedureParam{{Name: "body", Type: "STRING"}}, MinArgs: 1, MaxArgs: 1,
			Returns: []cypher.ProcedureColumn{{Name: "v", Type: "INTEGER"}},
		}, func(_ context.Context, e *cypher.StorageExecutor, _ string, args []interface{}) (*cypher.ExecuteResult, error) {
			if rebuild {
				view, err := e.FunctionStorage("")
				if err != nil {
					return nil, err
				}
				e = cypher.NewStorageExecutor(storage.NewTracedEngine(view))
				e.SetDatabaseManager(manager)
			}
			_, err := e.Execute(context.Background(), args[0].(string), nil)
			return &cypher.ExecuteResult{Columns: []string{"v"}, Rows: [][]interface{}{{int64(1)}}}, err
		}))
	}
	post := func(path, statement string, params map[string]any) (TransactionResponse, error) {
		rec := makeRequest(t, s, http.MethodPost, path, map[string]any{"statements": []map[string]any{{"statement": statement, "parameters": params}}}, token)
		var resp TransactionResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			return resp, err
		}
		if len(resp.Errors) > 0 {
			return resp, fmt.Errorf("%s", resp.Errors[0].Message)
		}
		return resp, nil
	}
	driver, err := neo4j.NewDriverWithContext(fabricLifetimeBolt(t, s), neo4j.NoAuth())
	require.NoError(t, err)
	t.Cleanup(func() { _ = driver.Close(ctx) })
	run := func(route, db, completion, statement string, params map[string]any) error {
		switch route {
		case "executor":
			store, err := s.dbManager.GetStorage(db)
			if err != nil {
				return err
			}
			e := cypher.NewStorageExecutor(store)
			e.SetDatabaseManager(manager)
			if completion == "rollback" {
				_, err = e.Execute(ctx, "BEGIN", nil)
				if err != nil {
					return err
				}
			}
			_, err = e.Execute(ctx, statement, params)
			if completion == "rollback" {
				_, endErr := e.Execute(ctx, "ROLLBACK", nil)
				if err == nil {
					err = endErr
				}
			}
			return err
		case "http":
			path := "/db/" + db + "/tx"
			if completion == "auto" {
				_, err := post(path+"/commit", statement, params)
				return err
			}
			opened, err := post(path, "RETURN 1", nil)
			if err != nil {
				return err
			}
			commit, err := url.Parse(opened.Commit)
			if err != nil {
				return err
			}
			path = strings.TrimSuffix(commit.Path, "/commit")
			_, err = post(path, statement, params)
			makeRequest(t, s, http.MethodDelete, path, nil, token)
			return err
		case "bolt":
			session := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: db})
			defer func() { _ = session.Close(ctx) }()
			if completion == "auto" {
				result, err := session.Run(ctx, statement, params)
				if err == nil {
					_, err = result.Consume(ctx)
				}
				return err
			}
			tx, err := session.BeginTransaction(ctx)
			if err != nil {
				return err
			}
			result, err := tx.Run(ctx, statement, params)
			if err == nil {
				_, err = result.Consume(ctx)
			}
			endErr := tx.Rollback(ctx)
			if err == nil {
				err = endErr
			}
			return err
		}
		return fmt.Errorf("unknown route %s", route)
	}
	for _, route := range []string{"executor", "http", "bolt"} {
		for _, schedule := range []string{"script", "case_script", "begin", "commit", "rollback", "wrapped_native"} {
			for _, completion := range []string{"rollback", "auto"} {
				if schedule == "wrapped_native" && completion == "auto" {
					continue
				}
				t.Run(route+"/"+schedule+"/"+completion, func(t *testing.T) {
					mark := strings.ReplaceAll(t.Name(), "/", "_")
					write := "CREATE (:MembershipMark {run:'" + mark + "'})"
					body, procedure, db := "BEGIN "+write+" COMMIT", "direct", "smcomp"
					switch schedule {
					case "case_script":
						body = "BEGIN CALL apoc.cypher.run('" + strings.ReplaceAll(write, "'", "\\'") + " RETURN 1 AS v', {}) YIELD value CASE WHEN false THEN ROLLBACK ELSE RETURN value COMMIT"
					case "begin", "commit", "rollback":
						body = strings.ToUpper(schedule)
					case "wrapped_native":
						body, procedure, db = "USE smcomp.one "+write, "rebuilt", "smone"
					}
					statement := "CALL membership." + procedure + "($body) YIELD v RETURN v"
					if db == "smcomp" {
						statement = "USE smcomp.one " + statement
					}
					if completion == "auto" {
						statement = strings.TrimSuffix(statement, "RETURN v") + "RETURN 1 / 0 AS fail"
					}
					err := run(route, db, completion, statement, map[string]any{"body": body})
					persisted, readErr := post("/db/smone/tx/commit", "MATCH (n:MembershipMark {run:$run}) RETURN count(n)", map[string]any{"run": mark})
					require.NoError(t, readErr)
					require.Equal(t, float64(0), persisted.Results[0].Data[0].Row[0], "nested work escaped the enclosing owner")
					require.Error(t, err, "nested transaction control must be refused")
					if schedule == "wrapped_native" {
						require.Contains(t, strings.ToLower(err.Error()), "transaction")
					} else {
						require.Contains(t, strings.ToLower(err.Error()), "owned")
					}
				})
			}
		}
	}
}
