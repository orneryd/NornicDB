//go:build closeout_l2_e53_7_neo4j

package server_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"
)

// E53.7 reference run on Neo4j 5.26.30 (lab: --ref neo-ref). The same request
// sequences as TestL2_e53_7_Repro / _Cause2, over Neo4j's HTTP API, recorded
// as L2NEO4J lines. It only records; it fails when the reference cannot be
// reached. The reference is given as NEO4J_URI (Bolt URI), NEO4J_USERNAME and
// NEO4J_PASSWORD (optional); the HTTP endpoint is http://<host of NEO4J_URI>:7474
// unless NEO4J_HTTP_URL is set.

type l2Neo4jClient struct {
	base       string
	user, pass string
	http       *http.Client
}

type l2Neo4jResponse struct {
	Commit string `json:"commit"`
	Errors []struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"errors"`
	Results []struct {
		Data []struct {
			Row []interface{} `json:"row"`
		} `json:"data"`
	} `json:"results"`
}

func l2Neo4jNewClient(t *testing.T) *l2Neo4jClient {
	t.Helper()
	base := strings.TrimRight(os.Getenv("NEO4J_HTTP_URL"), "/")
	if base == "" {
		uri := os.Getenv("NEO4J_URI")
		if uri == "" {
			t.Fatalf("NEO4J_URI is not set: this test needs the Neo4j reference (run with the lab's --ref neo-ref)")
		}
		parsed, err := url.Parse(uri)
		if err != nil || parsed.Hostname() == "" {
			t.Fatalf("NEO4J_URI %q has no host: %v", uri, err)
		}
		base = "http://" + net.JoinHostPort(parsed.Hostname(), "7474")
	}
	c := &l2Neo4jClient{base: base, user: os.Getenv("NEO4J_USERNAME"), pass: os.Getenv("NEO4J_PASSWORD"), http: &http.Client{Timeout: 30 * time.Second}}
	status, _, raw, err := c.do(http.MethodGet, "/", nil)
	if err != nil {
		t.Fatalf("Neo4j reference unreachable at %s: %v", base, err)
	}
	t.Logf("L2NEO4J E53.7 reference=%s discovery status=%d body=%s", base, status, l2Neo4jTrim(raw))
	return c
}

func l2Neo4jTrim(raw string) string {
	raw = strings.Join(strings.Fields(raw), " ")
	if len(raw) > 400 {
		return raw[:400] + "..."
	}
	return raw
}

func (c *l2Neo4jClient) do(method, path string, statements []string) (int, l2Neo4jResponse, string, error) {
	var body io.Reader
	if statements != nil {
		list := make([]map[string]interface{}, 0, len(statements))
		for _, s := range statements {
			list = append(list, map[string]interface{}{"statement": s})
		}
		encoded, _ := json.Marshal(map[string]interface{}{"statements": list})
		body = bytes.NewReader(encoded)
	}
	target := path
	if !strings.HasPrefix(path, "http://") && !strings.HasPrefix(path, "https://") {
		target = c.base + path
	}
	req, err := http.NewRequest(method, target, body)
	if err != nil {
		return 0, l2Neo4jResponse{}, "", err
	}
	req.Header.Set("Accept", "application/json;charset=UTF-8")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.user != "" {
		req.SetBasicAuth(c.user, c.pass)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return 0, l2Neo4jResponse{}, "", err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	var parsed l2Neo4jResponse
	_ = json.Unmarshal(data, &parsed)
	return resp.StatusCode, parsed, string(data), nil
}

func l2Neo4jCodes(r l2Neo4jResponse) string {
	codes := make([]string, 0, len(r.Errors))
	for _, e := range r.Errors {
		codes = append(codes, e.Code)
	}
	return "[" + strings.Join(codes, ",") + "]"
}

// open runs POST /db/neo4j/tx with CREATE (:TTLProbe) and returns the
// transaction URL (/db/neo4j/tx/{id}).
func (c *l2Neo4jClient) open(t *testing.T, label string) string {
	t.Helper()
	status, resp, raw, err := c.do(http.MethodPost, "/db/neo4j/tx", []string{"CREATE (:TTLProbe)"})
	if err != nil {
		t.Fatalf("open transaction: %v", err)
	}
	t.Logf("L2NEO4J E53.7 %s open status=%d errors=%s commit=%q", label, status, l2Neo4jCodes(resp), resp.Commit)
	if resp.Commit == "" {
		t.Fatalf("open transaction returned no commit URL: status %d body %s", status, l2Neo4jTrim(raw))
	}
	// Neo4j answers with an absolute URL on its advertised address; keep only
	// the path and send it to the reference address the test was given.
	commit := resp.Commit
	if parsed, err := url.Parse(resp.Commit); err == nil && parsed.Path != "" {
		commit = parsed.Path
	}
	return strings.TrimSuffix(commit, "/commit")
}

func (c *l2Neo4jClient) step(t *testing.T, label, step, txURL, statement string) {
	t.Helper()
	status, resp, raw, err := c.do(http.MethodPost, txURL, []string{statement})
	if err != nil {
		t.Logf("L2NEO4J E53.7 %s %s %q transport error %v", label, step, statement, err)
		return
	}
	message := ""
	if len(resp.Errors) > 0 {
		message = resp.Errors[0].Message
	}
	t.Logf("L2NEO4J E53.7 %s %s %q status=%d errors=%s message=%q body=%s", label, step, statement, status, l2Neo4jCodes(resp), message, l2Neo4jTrim(raw))
}

func (c *l2Neo4jClient) autocommit(t *testing.T, label, statement string) l2Neo4jResponse {
	t.Helper()
	status, resp, raw, err := c.do(http.MethodPost, "/db/neo4j/tx/commit", []string{statement})
	if err != nil {
		t.Fatalf("%s: %v", statement, err)
	}
	t.Logf("L2NEO4J E53.7 %s %q status=%d errors=%s body=%s", label, statement, status, l2Neo4jCodes(resp), l2Neo4jTrim(raw))
	return resp
}

func TestL2_e53_7_Neo4j(t *testing.T) {
	c := l2Neo4jNewClient(t)
	t.Cleanup(func() {
		_, _, _, _ = c.do(http.MethodPost, "/db/neo4j/tx/commit", []string{"MATCH (n:TTLProbe) DELETE n"})
	})

	// (i) A failing statement in an open transaction, then another statement.
	{
		txURL := c.open(t, "failing-statement")
		c.step(t, "failing-statement", "step2", txURL, "RETURN 1/0")
		c.step(t, "failing-statement", "step3-immediately", txURL, "RETURN 1")
		_, _, _, _ = c.do(http.MethodDelete, txURL, nil)
	}

	// (ii) The timeout case: db.transaction.timeout = 1s (dynamic in 5.x).
	original := ""
	listed := c.autocommit(t, "timeout-config", "CALL dbms.listConfig('db.transaction.timeout') YIELD name, value RETURN name, value")
	for _, result := range listed.Results {
		for _, row := range result.Data {
			if len(row.Row) == 2 && fmt.Sprint(row.Row[0]) == "db.transaction.timeout" {
				original = fmt.Sprint(row.Row[1])
			}
		}
	}
	set := c.autocommit(t, "timeout-config", "CALL dbms.setConfigValue('db.transaction.timeout','1s')")
	t.Logf("L2NEO4J E53.7 timeout-config original=%q set_errors=%s", original, l2Neo4jCodes(set))
	if original != "" {
		t.Cleanup(func() {
			status, resp, _, err := c.do(http.MethodPost, "/db/neo4j/tx/commit", []string{fmt.Sprintf("CALL dbms.setConfigValue('db.transaction.timeout','%s')", strings.ReplaceAll(original, "'", ""))})
			t.Logf("L2NEO4J E53.7 timeout-config restore to %q status=%d errors=%s err=%v", original, status, l2Neo4jCodes(resp), err)
		})
	}

	txURL := c.open(t, "timed-out")
	time.Sleep(2 * time.Second)
	c.step(t, "timed-out", "step2-after-2s", txURL, "RETURN 1")
	c.step(t, "timed-out", "step2b-immediately", txURL, "RETURN 1")
	time.Sleep(31 * time.Second)
	c.step(t, "timed-out", "step3-after-31s", txURL, "RETURN 1")
	_, _, _, _ = c.do(http.MethodDelete, txURL, nil)
}
