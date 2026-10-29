package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/internal/testutil/compositeddl"
	"github.com/orneryd/nornicdb/pkg/observability"
)

type compositeDDLReply struct {
	Rows          []map[string]any
	Code, Message string
}

// Use separate real server processes, each with its own catalog and explicit
// parser setting. The outer Go test's parser label cannot choose a server's parser.
func TestCompositeDDLPublicParserParity(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	binary := filepath.Join(dir, "nornicdb")
	build := exec.Command("go", "build", "-tags=noui,nolocalllm", "-o", binary, "./cmd/nornicdb")
	build.Dir = root
	if data, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, data)
	}
	baseline := map[string]compositeDDLReply{}
	observed := map[string]compositeDDLReply{}
	for _, parser := range []string{"nornic", "antlr"} {
		for _, protocol := range []string{"http", "bolt", "bolt_tx"} {
			t.Run(parser+"/"+protocol, func(t *testing.T) {
				request := compositeDDLServer(t, binary, filepath.Join(dir, parser+"_"+protocol), parser, protocol)
				query := func(q string) compositeDDLReply { return request(q, nil) }
				for i, c := range compositeddl.Cases() {
					t.Run(c.Name, func(t *testing.T) {
						db := fmt.Sprintf("ddl_%03d", i) + c.DatabaseSuffix
						expand := func(q string) string { return strings.ReplaceAll(q, "$db", db) }
						for _, setup := range c.Setup {
							r := query(expand(setup))
							if r.Code != "" {
								t.Fatalf("setup: %s %s", r.Code, compositeDDLSafe(r.Message))
							}
						}
						r := request(expand(c.Statement), c.Parameters)
						observed[parser+"/"+protocol+"/"+c.Name+"/reply"] = r
						if (r.Code != "") != c.Reject {
							t.Errorf("rejected=%t, expected=%t: %s %s", r.Code != "", c.Reject, r.Code, compositeDDLSafe(r.Message))
						}
						if c.ReplyAlias != "" && (len(r.Rows) != 1 || r.Rows[0]["alias"] != expand(c.ReplyAlias) || r.Rows[0]["database"] != db) {
							t.Error("filtered listing did not return the expected alias and database")
						}
						key := protocol + "/" + c.Name
						if parser == "nornic" {
							baseline[key] = r
						} else {
							native, grammar := baseline[key], r
							if c.Reject {
								// Parser-specific wording and source positions are diagnostics,
								// while failure class and returned data are the shared contract.
								// Keep the original messages in observed for the complete
								// credential-output gate, and inspect catalog state below.
								if native.Message == "" || grammar.Message == "" {
									t.Error("rejected command lacks a diagnostic")
								}
								native.Message, grammar.Message = "", ""
							}
							if !reflect.DeepEqual(native, grammar) {
								t.Errorf("parser replies differ: native=%s antlr=%s", compositeDDLSafe(fmt.Sprint(baseline[key])), compositeDDLSafe(fmt.Sprint(r)))
							}
						}
						inspect := "SHOW COMPOSITE DATABASES"
						if c.Inspect == "constituents" || c.Inspect == "empty" {
							inspect = "SHOW CONSTITUENTS FOR COMPOSITE DATABASE " + db
						}
						if c.Inspect == "aliases" {
							inspect = "SHOW ALIASES"
						}
						state := query(inspect)
						observed[parser+"/"+protocol+"/"+c.Name+"/catalog"] = state
						if state.Code != "" {
							t.Errorf("catalog query: %s %s", state.Code, compositeDDLSafe(state.Message))
							return
						}
						// Database timestamps are incidental; retain the catalog identity/type.
						if c.Inspect == "databases" {
							for j, row := range state.Rows {
								state.Rows[j] = map[string]any{"name": row["name"], "type": row["type"]}
							}
						}
						compositeDDLSort(state.Rows)
						if parser == "nornic" {
							baseline[key+"/catalog"] = state
						} else if !reflect.DeepEqual(baseline[key+"/catalog"], state) {
							t.Error("parser catalog states differ")
						}
						if c.Inspect == "empty" && len(state.Rows) != 0 {
							t.Error("dropped constituent remains")
						}
						if c.Inspect == "aliases" && c.Alias != "" {
							found := false
							for _, row := range state.Rows {
								if row["alias"] == expand(c.Alias) && row["database"] == expand(c.Database) {
									found = true
								}
							}
							if !found {
								t.Error("expected standalone alias absent")
							}
						}
						if c.Inspect == "constituents" {
							found := false
							for _, row := range state.Rows {
								if row["alias"] != c.Alias {
									continue
								}
								found = true
								for field, expected := range map[string]string{"database": c.Database, "type": c.Type, "access_mode": c.Access, "uri": c.URI, "auth_mode": c.Auth, "user": c.User, "secret_ref": c.Secret} {
									if field == "user" && expected != "" && !strings.EqualFold(strings.TrimSpace(c.Auth), "user_password") {
										expected = observability.RedactedPlaceholder()
									}
									actual := row[field]
									if field == "uri" {
										if u, err := url.Parse(fmt.Sprint(actual)); err == nil {
											u.User = nil
											actual = u.String()
										}
									}
									if actual != expected {
										t.Errorf("catalog %s=%s, expected=%s", field, compositeDDLSafe(fmt.Sprint(actual)), expected)
									}
								}
							}
							if !found {
								t.Error("expected constituent absent")
							}
						}
					})
				}
				if parser == "antlr" {
					t.Run("syntax_diagnostics", func(t *testing.T) {
						dataQuery := compositeDDLServer(t, binary, filepath.Join(dir, "syntax_"+protocol), parser, protocol, "nornic")
						for _, q := range []string{"RETURN 1 AS value '" + compositeddl.Password + "'", "MATCH (n) WHERE n.password = '" + compositeddl.Password + "' RETURN n @"} {
							r := dataQuery(q, nil)
							if r.Code != "Neo.ClientError.Statement.SyntaxError" {
								t.Errorf("malformed query: %s %s", r.Code, compositeDDLSafe(r.Message))
							}
							compositeDDLNoSecrets(t, r)
						}
					})
				}
			})
		}
	}
	// Check every reply and catalog view from both parsers and all protocols.
	// The canonical display owner is integrated; incomplete observation fails.
	t.Run("redaction_depends_on_1506_1507", func(t *testing.T) {
		if len(observed) != len(compositeddl.Cases())*12 {
			t.Fatal("credential verification requires the complete parent test across both parsers and all protocols")
		}
		keys := make([]string, 0, len(observed))
		for key := range observed {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			t.Run(key, func(t *testing.T) { compositeDDLNoSecrets(t, observed[key]) })
		}
	})
}

func compositeDDLSafe(s string) string {
	return strings.ReplaceAll(strings.ReplaceAll(s, compositeddl.Password, "<password>"), compositeddl.BarePassword, "<password>")
}

func compositeDDLNoSecrets(t *testing.T, r compositeDDLReply) {
	t.Helper()
	data, _ := json.Marshal(r)
	if bytes.Contains(data, []byte(compositeddl.Password)) || bytes.Contains(data, []byte(compositeddl.BarePassword)) {
		t.Error("reply disclosed synthetic password")
	}
	for _, row := range r.Rows {
		if _, ok := row["password"]; ok {
			t.Error("catalog exposes password column")
		}
	}
}

func compositeDDLSort(rows []map[string]any) {
	sort.Slice(rows, func(i, j int) bool {
		a, _ := json.Marshal(rows[i])
		b, _ := json.Marshal(rows[j])
		return string(a) < string(b)
	})
}

func compositeDDLServer(t *testing.T, binary, dir, parser, protocol string, databases ...string) func(string, map[string]any) compositeDDLReply {
	t.Helper()
	database := "system"
	if len(databases) > 0 {
		database = databases[0]
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	port := func() string {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer l.Close()
		return fmt.Sprint(l.Addr().(*net.TCPAddr).Port)
	}
	hp, bp := port(), port()
	log, err := os.Create(filepath.Join(dir, "server.log"))
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(binary, "serve", "--address=127.0.0.1", "--http-port="+hp, "--bolt-port="+bp, "--data-dir="+filepath.Join(dir, "data"), "--embedding-enabled=false", "--stdio-log-max-kb=0", "--no-auth")
	cmd.Dir = dir
	cmd.Env = []string{"HOME=" + dir, "PATH=/usr/local/go/bin:/usr/local/bin:/usr/bin:/bin", "NORNICDB_LANGUAGE=en", "NORNICDB_BOLT_ENABLED=true", "NORNICDB_EMBEDDING_ENABLED=false", "NORNICDB_TELEMETRY_ENABLED=false", "NORNICDB_TELEMETRY_LISTEN=127.0.0.1:0", "NORNICDB_AUTH_JWT_SECRET=synthetic-composite-ddl-key-982741", "NORNICDB_PARSER=" + parser}
	cmd.Stdout, cmd.Stderr = log, log
	if err := cmd.Start(); err != nil {
		log.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait(); _ = log.Close() })
	client := &http.Client{Timeout: 10 * time.Second}
	httpQuery := func(q string, parameters map[string]any) compositeDDLReply {
		body, _ := json.Marshal(map[string]any{"statements": []map[string]any{{"statement": q, "parameters": parameters}}})
		resp, err := client.Post("http://127.0.0.1:"+hp+"/db/"+database+"/tx/commit", "application/json", bytes.NewReader(body))
		if err != nil {
			return compositeDDLReply{Code: "transport", Message: err.Error()}
		}
		defer resp.Body.Close()
		data, _ := io.ReadAll(resp.Body)
		var payload struct {
			Results []struct {
				Columns []string `json:"columns"`
				Data    []struct {
					Row []any `json:"row"`
				} `json:"data"`
			} `json:"results"`
			Errors []struct {
				Code    string `json:"code"`
				Message string `json:"message"`
			} `json:"errors"`
		}
		if err := json.Unmarshal(data, &payload); err != nil {
			return compositeDDLReply{Code: "decode", Message: err.Error()}
		}
		if len(payload.Errors) > 0 {
			return compositeDDLReply{Code: payload.Errors[0].Code, Message: payload.Errors[0].Message}
		}
		r := compositeDDLReply{Rows: []map[string]any{}}
		for _, result := range payload.Results {
			for _, record := range result.Data {
				row := map[string]any{}
				for i, key := range result.Columns {
					row[key] = record.Row[i]
				}
				r.Rows = append(r.Rows, row)
			}
		}
		return r
	}
	deadline := time.Now().Add(30 * time.Second)
	for httpQuery("SHOW DATABASES", nil).Code != "" {
		if time.Now().After(deadline) {
			data, _ := os.ReadFile(log.Name())
			t.Fatalf("server startup: %s", compositeDDLSafe(string(data)))
		}
		time.Sleep(100 * time.Millisecond)
	}
	if protocol == "http" {
		return httpQuery
	}
	driver, err := neo4j.NewDriverWithContext("bolt://127.0.0.1:"+bp, neo4j.NoAuth())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = driver.Close(context.Background()) })
	return func(q string, parameters map[string]any) compositeDDLReply {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		session := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: database})
		defer session.Close(ctx)
		var result neo4j.ResultWithContext
		var err error
		var tx neo4j.ExplicitTransaction
		if protocol == "bolt_tx" {
			tx, err = session.BeginTransaction(ctx)
			if err == nil {
				defer tx.Close(ctx)
				result, err = tx.Run(ctx, q, parameters)
			}
		} else {
			result, err = session.Run(ctx, q, parameters)
		}
		r := compositeDDLReply{Rows: []map[string]any{}}
		if err == nil {
			var records []*neo4j.Record
			records, err = result.Collect(ctx)
			for _, record := range records {
				r.Rows = append(r.Rows, record.AsMap())
			}
		}
		if err == nil && tx != nil {
			err = tx.Commit(ctx)
		}
		if err != nil {
			var ne *neo4j.Neo4jError
			if errors.As(err, &ne) {
				return compositeDDLReply{Code: ne.Code, Message: ne.Msg}
			}
			return compositeDDLReply{Code: "transport", Message: err.Error()}
		}
		return r
	}
}
