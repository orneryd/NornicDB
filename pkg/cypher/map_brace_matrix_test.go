package cypher

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/testing/cypher/querytextmatrix"
)

func TestMapBraceSyntaxTriviaMatrix(t *testing.T) {
	ctx := context.Background()
	var reference neo4j.DriverWithContext
	if uri := os.Getenv("NORNICDB_NEO4J_REFERENCE_URI"); uri != "" {
		var err error
		reference, err = neo4j.NewDriverWithContext(uri, neo4j.NoAuth())
		if err != nil {
			t.Fatal(err)
		}
		defer reference.Close(ctx)
	}
	for _, mode := range []string{"autocommit", "explicit"} {
		t.Run(mode, func(t *testing.T) {
			executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "map_brace_matrix"))
			querytextmatrix.MapBraceCases(func(item querytextmatrix.MapBraceCase) {
				t.Run(item.Name, func(t *testing.T) {
					if mode == "explicit" {
						if _, err := executor.Execute(ctx, "BEGIN", nil); err != nil {
							t.Fatal(err)
						}
						defer func() {
							if _, err := executor.Execute(ctx, "ROLLBACK", nil); err != nil {
								t.Error(err)
							}
						}()
					} else if item.Write {
						if _, err := executor.Execute(ctx, "MATCH (n:MapBraceMatrix) DETACH DELETE n", nil); err != nil {
							t.Fatal(err)
						}
					}
					preflight := validateStaticMapKeys(item.Query)
					if (preflight != nil) != item.SyntaxError {
						t.Errorf("raw validation: %v; expected syntax failure=%v", preflight, item.SyntaxError)
					}
					result, err := executor.Execute(ctx, item.Query, nil)
					if reference != nil {
						assertMapBraceReference(t, ctx, reference, mode == "explicit", item, result, err)
					}
					if item.SyntaxError {
						if err == nil {
							t.Fatal("malformed literal accepted before execution")
						}
						code, _ := nornicerrors.Neo4jStatus(err)
						if code != "Neo.ClientError.Statement.SyntaxError" {
							t.Fatalf("status=%s: %v", code, err)
						}
						return
					}
					if err != nil {
						t.Fatalf("query=%s: %v", item.Query, err)
					}
					if item.Explain {
						return
					}
					if item.Empty {
						if len(result.Rows) != 0 {
							t.Fatalf("expected no rows, got %#v", result.Rows)
						}
						return
					}
					if !reflect.DeepEqual(result.Rows, [][]interface{}{{item.Want}}) {
						t.Fatalf("rows=%#v want=%#v query=%s", result.Rows, item.Want, item.Query)
					}
					if !reflect.DeepEqual(result.Columns, []string{item.Alias}) {
						t.Fatalf("columns=%v want=%s", result.Columns, item.Alias)
					}
				})
			})
		})
	}
}

func assertMapBraceReference(t *testing.T, ctx context.Context, driver neo4j.DriverWithContext, explicit bool, item querytextmatrix.MapBraceCase, actual *ExecuteResult, actualErr error) {
	t.Helper()
	session := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "neo4j"})
	defer session.Close(ctx)
	var cursor neo4j.ResultWithContext
	var err error
	if explicit {
		var tx neo4j.ExplicitTransaction
		tx, err = session.BeginTransaction(ctx)
		if err == nil {
			defer tx.Rollback(ctx)
			cursor, err = tx.Run(ctx, item.Query, item.Params)
		}
	} else {
		cursor, err = session.Run(ctx, item.Query, item.Params)
	}
	var expected [][]interface{}
	var columns []string
	if err == nil {
		columns, err = cursor.Keys()
		if err == nil {
			for cursor.Next(ctx) {
				expected = append(expected, cursor.Record().Values)
			}
			err = cursor.Err()
			if err == nil {
				_, err = cursor.Consume(ctx)
			}
		}
	}
	expectedCode, actualCode := "", ""
	if err != nil {
		var neoError *neo4j.Neo4jError
		if !errors.As(err, &neoError) {
			t.Fatalf("reference transport: %v", err)
		}
		expectedCode = neoError.Code
	}
	if actualErr != nil {
		actualCode, _ = nornicerrors.Neo4jStatus(actualErr)
	}
	if actualCode != expectedCode || (actualErr != nil) != (err != nil) {
		t.Fatalf("reference status=%s local=%s: %v query=%s", expectedCode, actualCode, actualErr, item.Query)
	}
	if err != nil || item.Explain {
		return
	}
	if !reflect.DeepEqual(actual.Columns, columns) {
		t.Fatalf("reference columns=%v local=%v", columns, actual.Columns)
	}
	if len(expected) == 0 && len(actual.Rows) == 0 {
		return
	}
	wantJSON, _ := json.Marshal(expected)
	gotJSON, _ := json.Marshal(actual.Rows)
	if string(wantJSON) != string(gotJSON) {
		t.Fatalf("reference rows=%s local=%s query=%s", wantJSON, gotJSON, item.Query)
	}
}
