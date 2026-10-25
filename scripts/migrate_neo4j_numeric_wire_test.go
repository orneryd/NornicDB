package main

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
)

type numericWireTransport func(*http.Request) (*http.Response, error)

func (transport numericWireTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	return transport(request)
}

// Exercise the script's public fetch/insert functions without contacting its
// deployment endpoints. Source properties and destination parameter lexemes
// must agree; the test never logs configuration or request headers.
func TestNumericWireMigration(t *testing.T) {
	oldClient, oldStats := http.DefaultClient, allStats
	t.Cleanup(func() { http.DefaultClient = oldClient; allStats = oldStats })
	calls := 0
	http.DefaultClient = &http.Client{Transport: numericWireTransport(func(request *http.Request) (*http.Response, error) {
		calls++
		response := `{"results":[{"data":[{"row":[{"v":9007199254740993,"f":2.0,"nested":[3.0]},["Node"],1]}]}],"errors":[]}`
		if calls == 2 {
			decoder := json.NewDecoder(request.Body)
			decoder.UseNumber()
			var payload map[string]interface{}
			if err := decoder.Decode(&payload); err != nil {
				t.Fatal(err)
			}
			props := payload["statements"].([]interface{})[0].(map[string]interface{})["parameters"].(map[string]interface{})["props"].(map[string]interface{})
			if props["v"] != json.Number("9007199254740993") || props["f"] != json.Number("2.0") || props["nested"].([]interface{})[0] != json.Number("3.0") {
				t.Fatalf("numeric parameters changed: %#v", props)
			}
			response = `{"errors":[]}`
		}
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(response)), Header: make(http.Header)}, nil
	})}
	nodes, _, err := fetchNodesFromNeo4j()
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 1 || nodes[0].Properties["v"] != int64(9007199254740993) || nodes[0].Properties["f"] != float64(2) {
		t.Fatalf("numeric source properties changed: %#v", nodes)
	}
	if _, err := insertNodesIntoNornic(nodes); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatalf("expected two intercepted requests, got %d", calls)
	}
}
