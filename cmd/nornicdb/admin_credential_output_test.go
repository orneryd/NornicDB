package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// Unsupported standalone remote aliases remain unsupported on every ingress.
// Their native error must not publish the supplied password on either parser.
func TestAdminCredentialErrorWireResponse(t *testing.T) {
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
	parser := "nornic"
	if strings.EqualFold(strings.TrimSpace(os.Getenv("NORNICDB_PARSER")), "antlr") {
		parser = "antlr"
	}
	hp, bp := solPort(t), solPort(t)
	stop := investigationStart(t, binary, dir, hp, bp, false, parser)
	defer stop()
	base := "http://127.0.0.1:" + hp
	ready := false
	for attempt := 0; attempt < 3; attempt++ {
		status, _, err := investigationHTTP(base, "nornic", "RETURN 1", "")
		if err == nil && status == http.StatusOK {
			ready = true
			break
		}
		time.Sleep(30 * time.Second)
	}
	if !ready {
		t.Fatal("server readiness failed")
	}
	driver, err := neo4j.NewDriverWithContext("bolt://127.0.0.1:"+bp, neo4j.NoAuth())
	if err != nil {
		t.Fatal("Bolt setup failed")
	}
	defer driver.Close(context.Background())
	for _, scheme := range []string{"http", "bolt"} {
		query := "CREATE ALIAS rejected FOR DATABASE nornic AT '" + scheme + "://127.0.0.1:1' USER admin PASSWORD '" + investigationPass + "'"
		for _, ingress := range []string{"HTTP", "Bolt_autocommit", "Bolt_explicit"} {
			t.Run(scheme+"/"+ingress, func(t *testing.T) {
				if ingress == "HTTP" {
					status, data, err := investigationHTTP(base, "system", query, "")
					var body investigationResponse
					if err != nil || json.Unmarshal(data, &body) != nil || status != http.StatusOK || len(body.Errors) == 0 {
						t.Fatal("unsupported alias did not return an error; details withheld")
					}
					if bytes.Contains(data, []byte(investigationPass)) {
						t.Error("HTTP error disclosed a synthetic password; value withheld")
					}
					return
				}
				session := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "system"})
				defer session.Close(context.Background())
				var result neo4j.ResultWithContext
				var err error
				if ingress == "Bolt_explicit" {
					tx, beginErr := session.BeginTransaction(ctx)
					if beginErr != nil {
						t.Fatal("Bolt BEGIN failed")
					}
					defer tx.Rollback(context.Background())
					result, err = tx.Run(ctx, query, nil)
				} else {
					result, err = session.Run(ctx, query, nil)
				}
				if err == nil {
					_, err = result.Consume(ctx)
				}
				var wireError *neo4j.Neo4jError
				if err == nil || !errors.As(err, &wireError) {
					t.Fatal("unsupported alias did not return a Bolt server error; details withheld")
				}
				if strings.Contains(err.Error(), investigationPass) || strings.Contains(wireError.Msg, investigationPass) {
					t.Error("Bolt error disclosed a synthetic password; value withheld")
				}
			})
		}
	}
}
