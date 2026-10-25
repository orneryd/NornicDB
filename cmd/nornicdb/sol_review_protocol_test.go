package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestSolReviewProtocolErrors(t *testing.T) {
	var requests atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, password, ok := r.BasicAuth()
		if !ok || user != "probe" || password != solSecret {
			t.Error("remote credential consumption changed")
		}
		requests.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(401)
		_ = json.NewEncoder(w).Encode(map[string]any{"errors": []map[string]string{{"code": "Neo.ClientError.Security.Unauthorized", "message": "synthetic rejection " + password}}})
	}))
	defer upstream.Close()
	binary := solBuild(t, "./cmd/nornicdb")
	hp, bp := solPort(t), solPort(t)
	dir := t.TempDir()
	solStart(t, binary, []string{"NORNICDB_BOLT_ENABLED=true", "NORNICDB_AUTH_JWT_SECRET=synthetic-sol-remote-encryption-4178263951"}, "serve", "--no-auth", "--http-port="+hp, "--bolt-port="+bp, "--data-dir="+filepath.Join(dir, "data"), "--embedding-enabled=false", "--stdio-log-max-kb=0")
	base := "http://127.0.0.1:" + hp
	_ = solGet(t, base+"/status")
	client := &http.Client{Timeout: 5 * time.Second}
	post := func(path string, body any) []byte {
		t.Helper()
		raw, _ := json.Marshal(body)
		response, err := client.Post(base+path, "application/json", bytes.NewReader(raw))
		if err != nil {
			t.Fatal("protocol HTTP request failed; details withheld")
		}
		defer response.Body.Close()
		data, _ := io.ReadAll(response.Body)
		solAbsent(t, path, data)
		return data
	}
	create := "CREATE COMPOSITE DATABASE sol_remote ALIAS remote FOR DATABASE nornic AT '" + strings.Replace(upstream.URL, "://", "://probe:"+solSecret+"@", 1) + "' TYPE remote ACCESS read"
	data := post("/db/system/tx/commit", map[string]any{"statements": []map[string]string{{"statement": create}}})
	if bytes.Contains(data, []byte(`"errors":[{`)) {
		t.Fatal("composite setup failed; output withheld")
	}
	query := "USE sol_remote.remote RETURN 1 AS n"
	before := requests.Load()
	data = post("/db/nornic/tx/commit", map[string]any{"statements": []map[string]string{{"statement": query}}})
	if requests.Load() == before || !bytes.Contains(data, []byte("synthetic rejection")) {
		t.Error("HTTP remote error not reached")
	}
	before = requests.Load()
	data = post("/graphql", map[string]string{"query": fmt.Sprintf(`query { cypher(input: {statement: %q}) {columns rows rowCount} }`, query)})
	if requests.Load() == before || !bytes.Contains(data, []byte("synthetic rejection")) {
		t.Error("GraphQL remote error not reached")
	}
	before = requests.Load()
	driver, err := neo4j.NewDriverWithContext("bolt://127.0.0.1:"+bp, neo4j.NoAuth())
	if err != nil {
		t.Fatal("Bolt driver init failed")
	}
	defer driver.Close(context.Background())
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	session := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "nornic"})
	defer session.Close(context.Background())
	result, err := session.Run(ctx, query, nil)
	if err == nil {
		_, err = result.Consume(ctx)
	}
	if requests.Load() == before || err == nil || !strings.Contains(err.Error(), "synthetic rejection") {
		t.Error("Bolt remote FAILURE not reached")
	}
	if err != nil {
		solAbsent(t, "Bolt FAILURE", []byte(err.Error()))
	}
}
