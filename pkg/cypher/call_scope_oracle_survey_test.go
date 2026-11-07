package cypher

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// A live reference survey establishes visibility before changing the scope owner.
// This diagnostic is opt-in and records reference results rather than asserting
// NornicDB's previous behavior as an oracle.
func TestCallScopeOracleSurvey(t *testing.T) {
	uri := os.Getenv("NORNICDB_CALL_SCOPE_ORACLE_URI")
	if uri == "" {
		t.Skip("live Neo4j 5.26 scope survey")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	driver, err := neo4j.NewDriverWithContext(uri, neo4j.NoAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer driver.Close(context.Background())
	session := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "neo4j"})
	defer session.Close(context.Background())
	run := func(query string) map[string]any {
		record := map[string]any{"query": query}
		result, err := session.Run(ctx, query, nil)
		if err == nil {
			var keys []string
			keys, err = result.Keys()
			record["columns"] = keys
			if err == nil {
				var records []*neo4j.Record
				records, err = result.Collect(ctx)
				rows := [][]any{}
				for _, r := range records {
					rows = append(rows, r.Values)
				}
				record["rows"] = rows
			}
		}
		if err != nil {
			record["error"] = err.Error()
			if e, ok := err.(*neo4j.Neo4jError); ok {
				record["code"] = e.Code
			}
		}
		return record
	}
	for _, q := range []string{"CALL dbms.components() YIELD versions RETURN versions", "MERGE (:CallScopeOracleTarget {n:7})"} {
		raw, _ := json.Marshal(run(q))
		fmt.Println("CALL_SCOPE_SURVEY " + string(raw))
	}
	leaves := []string{"RETURN *", "WITH * RETURN *", "WITH a RETURN *", "WITH a AS z RETURN *", "WITH 9 AS z RETURN *", "RETURN a", "WITH a RETURN a", "MATCH (b:CallScopeOracleTarget) RETURN *", "MATCH (b:CallScopeOracleTarget) WITH * RETURN *", "MATCH (b:CallScopeOracleTarget) RETURN b.n AS value"}
	for _, call := range []string{"", "CALL (a) { ", "CALL (*) { ", "CALL { ", "CALL { WITH a ", "CALL { WITH * "} {
		for _, kind := range []string{"COLLECT", "COUNT", "EXISTS"} {
			for _, leaf := range leaves {
				body := "RETURN " + kind + " { " + leaf + " } AS v"
				query := "UNWIND [1,2] AS a " + body
				if call != "" {
					query = "UNWIND [1,2] AS a " + call + body + " } RETURN a,v ORDER BY a"
				}
				raw, _ := json.Marshal(run(query))
				fmt.Println("CALL_SCOPE_SURVEY " + string(raw))
			}
		}
		for _, leaf := range leaves {
			query := "UNWIND [1,2] AS a " + leaf
			if call != "" {
				query = "UNWIND [1,2] AS a " + call + leaf + " } RETURN *"
			}
			raw, _ := json.Marshal(run(query))
			fmt.Println("CALL_SCOPE_SURVEY " + string(raw))
		}
	}
}
