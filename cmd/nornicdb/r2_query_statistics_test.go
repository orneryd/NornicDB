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
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/observability"
)

// The reviewer's saved remote-PASSWORD repro, with both public row renderers.
// Strict ANTLR rejects remote-composite syntax, so its successful stored-query
// case uses a graph property literal through the same statistics collector.
func TestR2PublicQueryStatistics(t *testing.T) {
	binary := solBuild(t, "./cmd/nornicdb")
	for _, parser := range []string{"default", "antlr"} {
		t.Run(parser, func(t *testing.T) {
			var value [18]byte
			if _, err := rand.Read(value[:]); err != nil {
				t.Fatal("canary generation failed")
			}
			canary := hex.EncodeToString(value[:])
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
			post := func(db, query string) []byte {
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
			db := "system"
			query := "CREATE COMPOSITE DATABASE r2_remote ALIAS remote FOR DATABASE nornic AT 'http://127.0.0.1:1' USER 'probe' PASSWORD '" + canary + "' TYPE remote ACCESS read"
			if parser == "antlr" {
				db = "nornic"
				query = "CREATE (n:R2SavedCredential {password: '" + canary + "'})"
			}
			post(db, query)
			expectedQuery, _ := json.Marshal(cypher.RedactLiterals(query))
			data := post(db, "CALL db.stats.retrieve('QUERIES')")
			if !bytes.Contains(data, expectedQuery) || bytes.Contains(data, []byte(canary)) {
				t.Error("HTTP saved-query diagnostic absent or disclosed a literal; capture withheld")
			}
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			driver, err := neo4j.NewDriverWithContext("bolt://127.0.0.1:"+bp, neo4j.NoAuth())
			if err != nil {
				t.Fatal("Bolt driver init failed")
			}
			defer driver.Close(context.Background())
			session := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: db})
			defer session.Close(ctx)
			result, err := session.Run(ctx, "CALL db.stats.retrieve('QUERIES')", nil)
			if err != nil {
				t.Fatal("Bolt statistics request failed; details withheld")
			}
			found := false
			for result.Next(ctx) {
				data, ok := result.Record().Get("data")
				if !ok {
					t.Fatal("Bolt statistics data absent")
				}
				raw, err := json.Marshal(data)
				if err != nil || bytes.Contains(raw, []byte(canary)) {
					t.Error("Bolt saved-query diagnostic disclosed a literal; capture withheld")
				}
				found = found || bytes.Contains(raw, expectedQuery)
			}
			if result.Err() != nil || !found {
				t.Fatal("Bolt statistics failed or omitted the configured statement; details withheld")
			}
		})
	}
}
