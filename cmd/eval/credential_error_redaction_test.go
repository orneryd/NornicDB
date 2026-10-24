package main

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/eval"
	"github.com/orneryd/nornicdb/pkg/observability"
)

type credentialEchoTransport struct{}

func (credentialEchoTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	return nil, fmt.Errorf("synthetic transport failure for %s", request.URL.String())
}

func TestR73EvaluationTransportErrorsNeverPublishQueryCredentials(t *testing.T) {
	credential := strings.Join([]string{"synthetic", "route", "credential"}, "-")
	searcher := &HTTPSearcher{
		url: "http://127.0.0.1:1/proxy?api_key=" + credential + "&region=public",
		client: &http.Client{
			Transport: credentialEchoTransport{},
		},
	}

	_, searchErr := searcher.Search(context.Background(), "probe")
	if searchErr == nil {
		t.Fatal("transport failure was not returned")
	}
	assertCredentialAbsent(t, searchErr.Error(), credential, "search error")

	harness := NewHTTPHarness(searcher)
	harness.AddTestCase(eval.TestCase{Name: "transport", Query: "probe", Expected: []string{"node"}})
	result, err := harness.Run(context.Background())
	if err != nil {
		t.Fatal("evaluation harness failed")
	}
	if len(result.Results) != 1 || result.Results[0].Error == "" {
		t.Fatal("evaluation result did not retain a safe error")
	}
	assertCredentialAbsent(t, result.Results[0].Error, credential, "stored result")

	var detailed bytes.Buffer
	eval.NewReporter(&detailed).PrintDetails(result)
	assertCredentialAbsent(t, detailed.String(), credential, "detailed output")

	var encoded bytes.Buffer
	if err := eval.NewReporter(&encoded).PrintJSON(result); err != nil {
		t.Fatal("JSON output failed")
	}
	assertCredentialAbsent(t, encoded.String(), credential, "JSON output")

	path := filepath.Join(t.TempDir(), "result.json")
	if err := eval.NewReporter(nil).SaveJSON(result, path); err != nil {
		t.Fatal("saved JSON failed")
	}
	saved, err := os.ReadFile(path)
	if err != nil {
		t.Fatal("saved JSON could not be read")
	}
	assertCredentialAbsent(t, string(saved), credential, "saved JSON")
}

func assertCredentialAbsent(t *testing.T, output, credential, surface string) {
	t.Helper()
	if strings.Contains(output, credential) {
		t.Fatalf("%s exposed the synthetic credential", surface)
	}
	marker := observability.RedactedPlaceholder()
	jsonMarker := strings.ReplaceAll(strings.ReplaceAll(marker, "<", `\u003c`), ">", `\u003e`)
	if !strings.Contains(output, marker) && !strings.Contains(output, jsonMarker) {
		t.Fatalf("%s did not retain the redaction marker", surface)
	}
}
