package main

import (
	"bytes"
	"encoding/csv"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestScannerFindsProductionMessageSurfaces(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "pkg", "sample", "messages.go")
	requireWriteFile(t, path, `package sample

import (
	"errors"
	"fmt"
	"log"
	"net/http"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type response struct { Message string }
type customError struct{}
func (customError) Error() string { return "custom failure" }

func emit(w http.ResponseWriter, logger interface{ Warn(string, ...any) }) error {
	_ = errors.New("fixed failure")
	_ = fmt.Errorf("formatted failure: %d", 1)
	log.Printf("processed %d records", 2)
	logger.Warn("service degraded", "reason", "disk")
	fmt.Printf("ready on %s", "localhost")
	http.Error(w, "invalid request", http.StatusBadRequest)
	_ = status.Error(codes.Internal, "RPC failure")
	_ = status.New(codes.InvalidArgument, "RPC status failure")
	_ = response{Message: "operation complete"}
	register("sample", "Sample procedure description")
	logger.Log("warn", "custom log message", nil)
	_ = func() string { return "returned user message" }()
	return sendRunFailure("Neo.ClientError.Request.Invalid", "invalid RUN")
}

func register(string, string) {}
func sendRunFailure(string, string) error { return nil }

var schema = []byte(`+"`"+`{"description":"Schema help text"}`+"`"+`)
`)
	requireWriteFile(t, filepath.Join(root, "pkg", "sample", "native.m"), `
void run(void) {
    set_error(nil, "Native pipeline failed");
}
`)
	requireWriteFile(t, filepath.Join(root, "cmd", "nornicdb", "messages.go"), `package main
import "errors"
var included = errors.New("included main command message")
`)
	requireWriteFile(t, filepath.Join(root, "cmd", "nornicdb-admin", "messages.go"), `package main
import "errors"
var included = errors.New("included admin command message")
`)
	requireWriteFile(t, filepath.Join(root, "cmd", "recall-bench", "messages.go"), `package main
import "errors"
var excluded = errors.New("excluded command message")
`)
	requireWriteFile(t, filepath.Join(root, "apoc", "messages.go"), `package apoc
import "errors"
var excluded = errors.New("excluded APOC message")
`)
	requireWriteFile(t, filepath.Join(root, "plugins", "sample", "messages.go"), `package sample
import "errors"
var excluded = errors.New("excluded plugin message")
`)

	s := &scanner{root: root, fset: tokenFileSet(), seen: make(map[string]struct{})}
	if err := s.scan(); err != nil {
		t.Fatalf("scan: %v", err)
	}

	wantChannels := map[string]bool{
		"bolt": false, "cli": false, "embedded-json": false, "error": false,
		"error-method": false, "grpc": false, "http": false, "log": false,
		"metadata": false, "response-field": false, "return-value": false,
		"native-error": false,
	}
	includedCommands := map[string]bool{"cmd/nornicdb": false, "cmd/nornicdb-admin": false}
	for _, item := range s.occurrences {
		if strings.HasPrefix(item.File, "apoc/") || strings.HasPrefix(item.File, "plugins/") || strings.HasPrefix(item.File, "cmd/recall-bench/") {
			t.Errorf("excluded source was inventoried: %s", item.File)
		}
		if _, ok := includedCommands[item.Package]; ok {
			includedCommands[item.Package] = true
		}
		if _, ok := wantChannels[item.Channel]; ok {
			wantChannels[item.Channel] = true
		}
		if item.Text == "formatted failure: %d" && !item.Dynamic {
			t.Error("formatted error was not marked dynamic")
		}
	}
	for channel, found := range wantChannels {
		if !found {
			t.Errorf("channel %q was not inventoried", channel)
		}
	}
	for command, found := range includedCommands {
		if !found {
			t.Errorf("target command %q was not inventoried", command)
		}
	}
}

func TestNormalizeCandidatePreservesPlaceholderSchema(t *testing.T) {
	first := "Database '%s' not found."
	second := "database %q not found"
	if normalizeCandidate(first) != normalizeCandidate(second) {
		t.Fatalf("normalized values differ: %q != %q", normalizeCandidate(first), normalizeCandidate(second))
	}
	if got := placeholderSchema(first); got != "string" {
		t.Fatalf("placeholder schema = %q, want string", got)
	}
	if got := placeholderSchema("database {{.Name}} has %d items"); got != "named:Name,integer" {
		t.Fatalf("named placeholder schema = %q", got)
	}
}

func TestNormalizedCandidatesRequireDistinctVariants(t *testing.T) {
	s := &scanner{occurrences: []occurrence{
		{Audience: "client", Channel: "http", Package: "pkg/server", File: "a.go", Line: 1, Text: "Database '%s' not found.", Review: "localize"},
		{Audience: "client", Channel: "http", Package: "pkg/server", File: "b.go", Line: 2, Text: "database %q not found", Review: "localize"},
		{Audience: "client", Channel: "http", Package: "pkg/server", File: "c.go", Line: 3, Text: "same exact text", Review: "localize"},
		{Audience: "client", Channel: "http", Package: "pkg/server", File: "d.go", Line: 4, Text: "same exact text", Review: "localize"},
	}}
	var output bytes.Buffer
	if err := s.writeNormalizedCandidatesCSV(&output); err != nil {
		t.Fatal(err)
	}
	records, err := csv.NewReader(&output).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 {
		t.Fatalf("record count = %d, want header plus one candidate", len(records))
	}
	if records[1][5] != "database placeholder not found" {
		t.Fatalf("normalized text = %q", records[1][5])
	}
}

func TestNearCandidatesAreReviewOnlyAndSchemaScoped(t *testing.T) {
	s := &scanner{occurrences: []occurrence{
		{Audience: "client", Channel: "http", Package: "pkg/server", File: "a.go", Line: 1, Text: "database not found", Review: "localize"},
		{Audience: "client", Channel: "http", Package: "pkg/server", File: "b.go", Line: 2, Text: "database was not found", Review: "localize"},
		{Audience: "client", Channel: "http", Package: "pkg/server", File: "c.go", Line: 3, Text: "database not found", Review: "localize"},
		{Audience: "client", Channel: "grpc", Package: "pkg/server", File: "d.go", Line: 4, Text: "database is not found", Review: "localize"},
		{Audience: "operator", Channel: "http", Package: "pkg/server", File: "e.go", Line: 5, Text: "database is not found", Review: "localize"},
	}}
	var output bytes.Buffer
	if err := s.writeNearCandidatesCSV(&output); err != nil {
		t.Fatal(err)
	}
	records, err := csv.NewReader(&output).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 {
		t.Fatalf("record count = %d, want header plus one candidate", len(records))
	}
	if records[1][3] != "0.750" {
		t.Fatalf("similarity = %q", records[1][3])
	}
	if records[1][4] != "database not found" || records[1][5] != "database was not found" {
		t.Fatalf("unexpected near pair: %q / %q", records[1][4], records[1][5])
	}
}

func tokenFileSet() *token.FileSet {
	return token.NewFileSet()
}

func requireWriteFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}
