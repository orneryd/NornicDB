package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/observability"
)

// r2OrdinaryStatements are the ordinary statements the accepted Neo4j 5.26
// statistics contract records raw, with their numeric, string and property
// literals. The WHERE lookup is the exact statement the GH530 query runs.
var r2OrdinaryStatements = []string{
	"RETURN 530 AS value",
	"CREATE (n:PR1525Ref {name: 'ordinary-name-value', score: 4271, active: true, ratio: 3.5})",
	"MATCH (n:PR1525Ref) WHERE n.name = 'RETURN530 AS value' RETURN count(n) AS matched",
}

// TestR2PublicQueryStatistics proves the Neo4j-compatible statistics contract
// through both public row renderers and both parsers: ordinary statements are
// recorded with their literals raw, and an administration command carrying a
// synthetic credential is recorded in neither the user database's statistics
// nor the system database's, which reports no statistics at all.
func TestR2PublicQueryStatistics(t *testing.T) {
	binary := solBuild(t, "./cmd/nornicdb")
	for _, parser := range []string{"nornic", "antlr"} {
		t.Run(parser, func(t *testing.T) {
			var value [18]byte
			if _, err := rand.Read(value[:]); err != nil {
				t.Fatal("canary generation failed")
			}
			canary := hex.EncodeToString(value[:])
			username := "r2_user_" + canary[:8]
			hp, bp, dir := solPort(t), solPort(t), t.TempDir()
			solStart(t, binary, []string{
				"NORNICDB_PARSER=" + parser,
				"NORNICDB_BOLT_ENABLED=true",
				"NORNICDB_AUTH_JWT_SECRET=synthetic-r2-statistics-key-6198374",
				"NORNICDB_REMOTE_CREDENTIALS_KEY=synthetic-r2-statistics-key-6198374",
			}, "serve", "--no-auth", "--headless", "--http-port="+hp, "--bolt-port="+bp,
				"--data-dir="+filepath.Join(dir, "data"), "--embedding-enabled=false", "--stdio-log-max-kb=0")
			base := "http://127.0.0.1:" + hp
			_ = solGet(t, base+"/status")
			httpQuery := func(db, query string) []byte {
				t.Helper()
				raw, _ := json.Marshal(map[string]any{"statements": []map[string]string{{"statement": query}}})
				resp, err := (&http.Client{Timeout: 5 * time.Second}).Post(base+"/db/"+db+"/tx/commit", "application/json", bytes.NewReader(raw))
				if err != nil {
					t.Fatal("HTTP request failed; details withheld")
				}
				defer resp.Body.Close()
				data, err := io.ReadAll(resp.Body)
				if err != nil || bytes.Contains(data, []byte(`"errors":[{`)) {
					var failure struct {
						Errors []struct {
							Code    string
							Message string
						}
					}
					_ = json.Unmarshal(data, &failure)
					for _, item := range failure.Errors {
						t.Errorf("HTTP query error %s: %s", item.Code, observability.RedactText(item.Message, canary, "synthetic-r2-statistics-key-6198374"))
					}
					t.Fatal("HTTP query failed; capture withheld")
				}
				return data
			}
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			driver, err := neo4j.NewDriverWithContext("bolt://127.0.0.1:"+bp, neo4j.NoAuth())
			if err != nil {
				t.Fatal("Bolt driver init failed")
			}
			defer driver.Close(context.Background())
			// r2BoltRows runs one statement per session on db and returns the
			// statistics retrieval rows' data maps rendered as JSON.
			r2BoltRows := func(db string, statements ...string) []string {
				t.Helper()
				session := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: db})
				defer session.Close(context.Background())
				for _, statement := range statements {
					result, err := session.Run(ctx, statement, nil)
					if err != nil {
						t.Fatalf("Bolt statement on %s failed; details withheld", db)
					}
					if _, err := result.Consume(ctx); err != nil {
						t.Fatalf("Bolt statement on %s failed; details withheld", db)
					}
				}
				result, err := session.Run(ctx, "CALL db.stats.retrieve('QUERIES')", nil)
				if err != nil {
					t.Fatalf("Bolt statistics request on %s failed; details withheld", db)
				}
				rows := make([]string, 0)
				for result.Next(ctx) {
					data, ok := result.Record().Get("data")
					if !ok {
						t.Fatalf("Bolt statistics data absent on %s", db)
					}
					raw, err := json.Marshal(data)
					if err != nil {
						t.Fatal("Bolt statistics serialization failed")
					}
					rows = append(rows, string(raw))
				}
				if err := result.Err(); err != nil {
					t.Fatalf("Bolt statistics on %s failed; details withheld", db)
				}
				return rows
			}
			// Ordinary statements are recorded raw through both renderers.
			for _, statement := range r2OrdinaryStatements {
				httpQuery("nornic", statement)
			}
			httpData := httpQuery("nornic", "CALL db.stats.retrieve('QUERIES')")
			for _, statement := range r2OrdinaryStatements {
				registered, _ := json.Marshal(statement)
				if !bytes.Contains(httpData, registered) {
					t.Errorf("HTTP statistics omitted the raw ordinary statement %q", statement)
				}
			}
			boltRows := r2BoltRows("nornic", r2OrdinaryStatements...)
			boltData := strings.Join(boltRows, "\n")
			for _, statement := range r2OrdinaryStatements {
				registered, _ := json.Marshal(statement)
				if !strings.Contains(boltData, string(registered)) {
					t.Errorf("Bolt statistics omitted the raw ordinary statement %q", statement)
				}
			}
			const lookup = "CALL db.stats.retrieve('QUERIES') YIELD data WHERE data.query = 'RETURN 530 AS value' RETURN data.query AS query"
			var lookupBody struct {
				Results []struct {
					Data []struct {
						Row []string `json:"row"`
					} `json:"data"`
				} `json:"results"`
			}
			if err := json.Unmarshal(httpQuery("nornic", lookup), &lookupBody); err != nil || len(lookupBody.Results) != 1 || len(lookupBody.Results[0].Data) != 1 || len(lookupBody.Results[0].Data[0].Row) != 1 || lookupBody.Results[0].Data[0].Row[0] != "RETURN 530 AS value" {
				t.Fatal("HTTP executed-query lookup did not return the exact ordinary statement")
			}
			lookupSession := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "nornic"})
			defer lookupSession.Close(context.Background())
			lookupResult, err := lookupSession.Run(ctx, lookup, nil)
			if err != nil {
				t.Fatal("Bolt executed-query lookup failed; details withheld")
			}
			lookupRows, err := lookupResult.Collect(ctx)
			if err != nil || len(lookupRows) != 1 || len(lookupRows[0].Values) != 1 || lookupRows[0].Values[0] != "RETURN 530 AS value" {
				t.Fatal("Bolt executed-query lookup did not return the exact ordinary statement")
			}
			// Administration commands keep their credentials out of every
			// statistics surface.
			admin := func(suffix string) string {
				return "CREATE COMPOSITE DATABASE " + username + suffix + " ALIAS remote FOR DATABASE nornic AT 'http://127.0.0.1:1' USER 'probe' PASSWORD '" + canary + "' TYPE remote ACCESS read"
			}
			httpQuery("nornic", admin("_http_user"))
			httpQuery("system", admin("_http_system"))
			r2BoltRows("nornic", admin("_bolt_user"))
			r2BoltRows("system", admin("_bolt_system"))
			if data := httpQuery("nornic", "CALL db.stats.retrieve('QUERIES')"); bytes.Contains(data, []byte(canary)) {
				t.Error("user-database HTTP statistics disclosed an administration credential; capture withheld")
			}
			if data := httpQuery("nornic", "CALL db.stats.retrieve('QUERIES')"); bytes.Contains(data, []byte("CREATE COMPOSITE DATABASE")) {
				t.Error("user-database HTTP statistics recorded an administration command")
			}
			userRows := r2BoltRows("nornic")
			for _, row := range userRows {
				if strings.Contains(row, canary) {
					t.Error("user-database Bolt statistics disclosed an administration credential; capture withheld")
				}
				if strings.Contains(row, "CREATE COMPOSITE DATABASE") {
					t.Error("user-database Bolt statistics recorded an administration command")
				}
			}
			systemRows := r2BoltRows("system")
			if len(systemRows) != 0 {
				t.Errorf("system database reported %d statistics rows, want none", len(systemRows))
			}
			for _, section := range []string{"QUERIES", "ALL", "GRAPH COUNTS"} {
				raw, _ := json.Marshal(map[string]any{"statements": []map[string]string{{"statement": "CALL db.stats.retrieve('" + section + "')"}}})
				resp, err := (&http.Client{Timeout: 5 * time.Second}).Post(base+"/db/system/tx/commit", "application/json", bytes.NewReader(raw))
				if err != nil {
					t.Fatal("HTTP request failed; details withheld")
				}
				data, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				if bytes.Contains(data, []byte(`"errors":[{`)) {
					t.Fatalf("system %s retrieval failed; details withheld", section)
				}
				var body struct {
					Results []struct {
						Data []json.RawMessage `json:"data"`
					} `json:"results"`
				}
				if err := json.Unmarshal(data, &body); err != nil || len(body.Results) != 1 {
					t.Fatalf("system %s retrieval returned an unexpected body; capture withheld", section)
				}
				if len(body.Results[0].Data) != 0 {
					t.Errorf("system %s retrieval returned %d rows, want none", section, len(body.Results[0].Data))
				}
			}
		})
	}
}
