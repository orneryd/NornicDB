package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Permanent public HTTP regression from review r2. No reachable remote is
// needed: CREATE/ADD store configuration and the response must not echo it.
func TestReviewR2AlterWireResponse(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal("root setup failed")
	}
	dir := t.TempDir()
	binary := filepath.Join(dir, "nornicdb")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	build := exec.CommandContext(ctx, "go", "build", "-tags=noui,nolocalllm", "-o", binary, "./cmd/nornicdb")
	build.Dir = root
	if _, err = build.CombinedOutput(); err != nil {
		t.Fatal("build failed; details withheld")
	}
	hp, bp := solPort(t), solPort(t)
	serverDir := filepath.Join(dir, "server")
	stop := investigationStart(t, binary, serverDir, hp, bp, false, "nornic")
	base := "http://127.0.0.1:" + hp
	ready := func() {
		t.Helper()
		for attempt := 0; attempt < 3; attempt++ {
			status, _, err := investigationHTTP(base, "nornic", "RETURN 1", "")
			if err == nil && status == http.StatusOK {
				return
			}
			time.Sleep(30 * time.Second)
		}
		t.Fatal("server readiness failed")
	}
	ready()
	request := func(t *testing.T, label, query string, wantError bool) {
		t.Helper()
		status, data, err := investigationHTTP(base, "system", query, "")
		var body investigationResponse
		decodeErr := json.Unmarshal(data, &body)
		if err != nil || decodeErr != nil || status != http.StatusOK || (len(body.Errors) != 0) != wantError {
			t.Fatal(label + " had an unexpected outcome; details withheld")
		}
		if bytes.Contains(data, []byte(investigationPass)) {
			t.Error(label + " disclosed a synthetic password; value withheld")
		}
		if !wantError {
			return
		}
		// The same rejected statement through the public GraphQL resolver must
		// report the resolution error, executed by the cypher resolver rather
		// than rejected by schema/GraphQL validation, without echoing credentials.
		graphqlBody, marshalErr := json.Marshal(map[string]any{
			"query":     `query($statement: String!) { cypher(input: {statement: $statement, database: "system"}) { columns } }`,
			"variables": map[string]string{"statement": query},
		})
		if marshalErr != nil {
			t.Fatal(label + " GraphQL request could not be encoded; details withheld")
		}
		graphqlResponse, graphqlErr := (&http.Client{Timeout: 8 * time.Second}).Post(base+"/graphql", "application/json", bytes.NewReader(graphqlBody))
		if graphqlErr != nil {
			t.Error(label + " GraphQL transport failure; details withheld")
			return
		}
		graphqlData, readErr := io.ReadAll(graphqlResponse.Body)
		graphqlResponse.Body.Close()
		if readErr != nil {
			t.Error(label + " GraphQL response read failure; details withheld")
			return
		}
		if graphqlResponse.StatusCode != http.StatusOK {
			t.Error(label + " GraphQL response was not HTTP 200; status withheld")
			return
		}
		var graphqlResult struct {
			Errors []struct {
				Message string `json:"message"`
				Path    []any  `json:"path"`
			} `json:"errors"`
		}
		if decodeErr := json.Unmarshal(graphqlData, &graphqlResult); decodeErr != nil {
			t.Error(label + " GraphQL response could not be decoded; body withheld")
			return
		}
		if len(graphqlResult.Errors) == 0 {
			t.Error(label + " GraphQL returned no rejection errors; body withheld")
			return
		}
		for _, graphqlError := range graphqlResult.Errors {
			if strings.TrimSpace(graphqlError.Message) == "" {
				t.Error(label + " GraphQL rejection carried an empty message; body withheld")
			}
			if len(graphqlError.Path) != 1 || graphqlError.Path[0] != "cypher" {
				t.Error(label + " GraphQL rejection was not reported by the cypher resolver; path withheld")
			}
		}
		if bytes.Contains(graphqlData, []byte(investigationPass)) {
			t.Error(label + " GraphQL response disclosed a synthetic password; value withheld")
		}
	}
	checkDuplicatePassword := func() {
		t.Helper()
		for _, command := range []string{"CREATE COMPOSITE DATABASE malformed_password ALIAS r FOR DATABASE nornic", "ALTER COMPOSITE DATABASE malformed_password ADD ALIAS r FOR DATABASE nornic"} {
			request(t, "duplicate PASSWORD error", command+" AT 'http://127.0.0.1:1' USER 'admin' PASSWORD PASSWORD '"+investigationPass+"'", true)
		}
	}
	checkDuplicatePassword()
	for _, scheme := range []string{"http", "bolt"} {
		for _, form := range []string{"explicit", "uri", "both"} {
			t.Run("native/"+scheme+"/"+form, func(t *testing.T) {
				db := "wire_" + scheme + "_" + form
				request(t, "CREATE setup", "CREATE COMPOSITE DATABASE "+db+" ALIAS seed FOR DATABASE nornic TYPE local ACCESS read", false)
				endpoint := scheme + "://127.0.0.1:1"
				if form != "explicit" {
					endpoint = scheme + "://admin:" + investigationPass + "@127.0.0.1:1"
				}
				suffix := ""
				if form != "uri" {
					suffix = " USER admin PASSWORD '" + investigationPass + "'"
				}
				declaration := " FOR DATABASE nornic AT '" + endpoint + "'" + suffix + " TYPE remote ACCESS read"
				request(t, "ALTER ADD", "ALTER COMPOSITE DATABASE "+db+" ADD ALIAS r"+declaration, false)
				request(t, "CREATE remote", "CREATE COMPOSITE DATABASE "+db+"_create ALIAS r"+declaration, false)
				request(t, "SHOW constituents", "SHOW CONSTITUENTS FOR COMPOSITE DATABASE "+db, false)
				request(t, "EXPLAIN", "EXPLAIN CREATE COMPOSITE DATABASE "+db+"_plan ALIAS r"+declaration, false)
				request(t, "duplicate ADD error", "ALTER COMPOSITE DATABASE "+db+" ADD ALIAS r"+declaration, true)
				request(t, "unsupported SET error", "ALTER COMPOSITE DATABASE "+db+" SET ALIAS r AT '"+endpoint+"'", true)
				request(t, "CREATE unexpected URI error", "CREATE COMPOSITE DATABASE "+db+"_bad ALIAS r FOR DATABASE nornic '"+endpoint+"'", true)
				request(t, "ALTER unexpected URI error", "ALTER COMPOSITE DATABASE "+db+" ADD ALIAS bad FOR DATABASE nornic '"+endpoint+"'", true)
			})
		}
	}
	if strings.EqualFold(strings.TrimSpace(os.Getenv("NORNICDB_PARSER")), "antlr") {
		stop()
		stop = investigationStart(t, binary, serverDir, hp, bp, false, "antlr")
		ready()
		checkDuplicatePassword()
		// The existing remote-DDL grammar gap is not part of this correction.
		// Exercise real rejection output and catalog display after native setup.
		for _, command := range []string{"CREATE COMPOSITE DATABASE wire_rejected ALIAS r FOR DATABASE nornic", "ALTER COMPOSITE DATABASE wire_http_uri ADD ALIAS rejected FOR DATABASE nornic"} {
			request(t, "ANTLR AT rejection", command+" AT 'http://admin:"+investigationPass+"@127.0.0.1:1' TYPE remote ACCESS read", true)
			request(t, "ANTLR unexpected URI rejection", command+" 'http://admin:"+investigationPass+"@127.0.0.1:1'", true)
		}
	}
	request(t, "SHOW composite databases", "SHOW COMPOSITE DATABASES", false)
	response, err := (&http.Client{Timeout: 8 * time.Second}).Get(base + "/db/wire_http_uri")
	if err != nil {
		t.Fatal("HTTP catalog request failed")
	}
	data, err := io.ReadAll(response.Body)
	response.Body.Close()
	if err != nil || response.StatusCode != http.StatusOK || !bytes.Contains(data, []byte(`"uri"`)) {
		t.Fatal("HTTP catalog output missing; details withheld")
	}
	if bytes.Contains(data, []byte(investigationPass)) {
		t.Error("HTTP catalog disclosed a synthetic password; value withheld")
	}
	stop()
	files, err := filepath.Glob(filepath.Join(serverDir, "process-*.log"))
	if err != nil || len(files) == 0 {
		t.Fatal("process log capture missing")
	}
	for _, file := range files {
		data, err := os.ReadFile(file)
		if err != nil || bytes.Contains(data, []byte(investigationPass)) {
			t.Error("process log inspection failed or disclosed a password; details withheld")
		}
	}
}
