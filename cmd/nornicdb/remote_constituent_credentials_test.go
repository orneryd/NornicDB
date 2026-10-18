package main

// Exercise public protocols against separate real servers. Child processes use
// an allowlisted synthetic environment and are stopped by test cleanup.
import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

const investigationPass = "synthetic-userpass-investigation-48263917"
const investigationMarker = "real-remote-marker-67193824"
const investigationRead = "MATCH (n:AuthInvestigation) RETURN n.marker AS marker"

func investigationDiagnostic(message string) string {
	message = strings.ReplaceAll(message, investigationPass, "<synthetic-password>")
	return strings.ReplaceAll(message, base64.StdEncoding.EncodeToString([]byte("admin:"+investigationPass)), "<synthetic-basic-auth>")
}

func investigationPort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return fmt.Sprint(l.Addr().(*net.TCPAddr).Port)
}

func investigationStart(t *testing.T, binary, dir, hp, bp string, remote bool) {
	t.Helper()
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	logFile, err := os.Create(filepath.Join(dir, "process.log"))
	if err != nil {
		t.Fatal(err)
	}
	args := []string{"serve", "--address=127.0.0.1", "--http-port=" + hp, "--bolt-port=" + bp, "--data-dir=" + filepath.Join(dir, "data"), "--embedding-enabled=false", "--stdio-log-max-kb=0"}
	if remote {
		args = append(args, "--admin-password="+investigationPass)
	} else {
		args = append(args, "--no-auth")
	}
	cmd := exec.Command(binary, args...)
	cmd.Dir = dir
	cmd.Env = []string{"HOME=" + dir, "PATH=/usr/local/go/bin:/usr/local/bin:/usr/bin:/bin", "NORNICDB_LANGUAGE=en", "NORNICDB_BOLT_ENABLED=true", "NORNICDB_EMBEDDING_ENABLED=false", "NORNICDB_TELEMETRY_ENABLED=false", "NORNICDB_TELEMETRY_LISTEN=127.0.0.1:0", "NORNICDB_AUTH_JWT_SECRET=synthetic-investigation-encryption-key-78369214"}
	cmd.Stdout, cmd.Stderr = logFile, logFile
	if err := cmd.Start(); err != nil {
		logFile.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		_ = logFile.Close()
		data, _ := os.ReadFile(logFile.Name())
		for _, secret := range []string{investigationPass, base64.StdEncoding.EncodeToString([]byte("admin:" + investigationPass))} {
			if bytes.Contains(data, []byte(secret)) {
				t.Error("process logs disclosed synthetic remote credentials")
				for _, line := range strings.Split(string(data), "\n") {
					if strings.Contains(line, secret) {
						t.Log(investigationDiagnostic(line))
					}
				}
			}
		}
	})
}

type investigationResponse struct {
	Results []struct {
		Data []struct {
			Row []any `json:"row"`
		} `json:"data"`
	} `json:"results"`
	Errors []struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"errors"`
}

func investigationHTTP(base, db, query, password string) (int, []byte, error) {
	raw, _ := json.Marshal(map[string]any{"statements": []map[string]string{{"statement": query}}})
	req, err := http.NewRequest(http.MethodPost, base+"/db/"+db+"/tx/commit", bytes.NewReader(raw))
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if password != "" {
		req.SetBasicAuth("admin", password)
	}
	response, err := (&http.Client{Timeout: 8 * time.Second}).Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	return response.StatusCode, data, err
}

func investigationMustHTTP(t *testing.T, base, db, query, password string, marker bool) {
	t.Helper()
	status, data, err := investigationHTTP(base, db, query, password)
	var body investigationResponse
	parseErr := json.Unmarshal(data, &body)
	if err != nil || parseErr != nil || status != 200 || len(body.Errors) > 0 || (marker && !bytes.Contains(data, []byte(investigationMarker))) {
		t.Fatalf("HTTP control/setup failed: status=%d transportError=%t parseError=%t errors=%d marker=%t", status, err != nil, parseErr != nil, len(body.Errors), bytes.Contains(data, []byte(investigationMarker)))
	}
}

func investigationBolt(uri, db, query string, auth neo4j.AuthToken) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	driver, err := neo4j.NewDriverWithContext(uri, auth)
	if err != nil {
		return false, err
	}
	defer driver.Close(context.Background())
	session := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: db})
	defer session.Close(context.Background())
	result, err := session.Run(ctx, query, nil)
	if err != nil {
		return false, err
	}
	records, err := result.Collect(ctx)
	if err != nil {
		return false, err
	}
	for _, record := range records {
		for _, value := range record.Values {
			if value == investigationMarker {
				return true, nil
			}
		}
	}
	return false, nil
}

type investigationWire struct {
	mu                           sync.Mutex
	data                         []byte
	requests, correct, encrypted int
	events                       []string
}

func (w *investigationWire) add(p []byte) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.data) < 65536 {
		w.data = append(w.data, p...)
	}
}
func (w *investigationWire) reset() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.data = nil
	w.requests = 0
	w.correct = 0
	w.encrypted = 0
	w.events = nil
}
func (w *investigationWire) info() (int, bool, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.requests, w.correct > 0 || bytes.Contains(w.data, []byte(investigationPass)), w.encrypted > 0 || bytes.Contains(w.data, []byte("enc:v1:"))
}

func (w *investigationWire) trace() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]string(nil), w.events...)
}

type investigationStatusWriter struct {
	http.ResponseWriter
	status int
}

func (w *investigationStatusWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

// Transparent proxies forward to real authenticated NornicDB. They retain only
// synthetic wire data in memory and log booleans, never credential values.
func investigationHTTPProxy(t *testing.T, target string) (string, *investigationWire) {
	t.Helper()
	u, _ := url.Parse(target)
	proxy := httputil.NewSingleHostReverseProxy(u)
	wire := &investigationWire{}
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, pass, ok := r.BasicAuth()
		wire.mu.Lock()
		wire.requests++
		if ok && user == "admin" && pass == investigationPass {
			wire.correct++
		}
		if strings.HasPrefix(pass, "enc:v1:") {
			wire.encrypted++
		}
		wire.mu.Unlock()
		wrapped := &investigationStatusWriter{ResponseWriter: w, status: 200}
		proxy.ServeHTTP(wrapped, r)
		authClass := "none"
		if ok {
			authClass = "other"
		}
		if ok && user == "admin" && pass == investigationPass {
			authClass = "correct"
		}
		if strings.HasPrefix(pass, "enc:v1:") {
			authClass = "encrypted"
		}
		wire.mu.Lock()
		wire.events = append(wire.events, fmt.Sprintf("%s %s auth=%s remote_status=%d", r.Method, r.URL.Path, authClass, wrapped.status))
		wire.mu.Unlock()
	}))
	t.Cleanup(s.Close)
	return s.URL, wire
}

func investigationBoltProxy(t *testing.T, target string) (string, *investigationWire) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	wire := &investigationWire{}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			client, err := l.Accept()
			if err != nil {
				return
			}
			remote, err := net.DialTimeout("tcp", target, 3*time.Second)
			if err != nil {
				client.Close()
				continue
			}
			_ = client.SetDeadline(time.Now().Add(20 * time.Second))
			_ = remote.SetDeadline(time.Now().Add(20 * time.Second))
			wire.mu.Lock()
			wire.requests++
			wire.mu.Unlock()
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer client.Close()
				defer remote.Close()
				done := make(chan struct{})
				go func() { _, _ = io.Copy(client, remote); client.Close(); close(done) }()
				buf := make([]byte, 4096)
				for {
					n, e := client.Read(buf)
					if n > 0 {
						wire.add(buf[:n])
						if _, err := remote.Write(buf[:n]); err != nil {
							break
						}
					}
					if e != nil {
						break
					}
				}
				remote.Close()
				<-done
			}()
		}
	}()
	t.Cleanup(func() { _ = l.Close(); wg.Wait() })
	return "bolt://" + l.Addr().String(), wire
}

func TestRemoteConstituentCredentials(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	binary := filepath.Join(dir, "nornicdb")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	build := exec.CommandContext(ctx, "go", "build", "-tags=noui,nolocalllm", "-o", binary, "./cmd/nornicdb")
	build.Dir = root
	if data, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, data)
	}
	rh, rb, lh, lb := investigationPort(t), investigationPort(t), investigationPort(t), investigationPort(t)
	investigationStart(t, binary, filepath.Join(dir, "remote"), rh, rb, true)
	investigationStart(t, binary, filepath.Join(dir, "coordinator"), lh, lb, false)
	remote, local := "http://127.0.0.1:"+rh, "http://127.0.0.1:"+lh
	// At most one readiness attempt every 30 seconds.
	for _, base := range []string{remote, local} {
		ready := false
		for attempt := 0; attempt < 3; attempt++ {
			password := ""
			if base == remote {
				password = investigationPass
			}
			status, _, e := investigationHTTP(base, "nornic", "RETURN 1", password)
			if e == nil && status == 200 {
				ready = true
				break
			}
			time.Sleep(30 * time.Second)
		}
		if !ready {
			t.Fatal("real NornicDB not ready")
		}
	}
	investigationMustHTTP(t, remote, "nornic", "CREATE (n:AuthInvestigation {marker: '"+investigationMarker+"'})", investigationPass, false)
	investigationMustHTTP(t, remote, "nornic", investigationRead, investigationPass, true)
	status, _, err := investigationHTTP(remote, "nornic", investigationRead, "synthetic-wrong-password-82361749")
	if err != nil || status != 401 {
		t.Fatalf("remote HTTP wrong-password control: status=%d error=%t", status, err != nil)
	}
	found, err := investigationBolt("bolt://127.0.0.1:"+rb, "nornic", investigationRead, neo4j.BasicAuth("admin", investigationPass, ""))
	if err != nil || !found {
		t.Fatalf("remote Bolt correct-password control: error=%v marker=%t", err, found)
	}
	_, err = investigationBolt("bolt://127.0.0.1:"+rb, "nornic", investigationRead, neo4j.BasicAuth("admin", "synthetic-wrong-password-82361749", ""))
	if err == nil {
		t.Fatal("remote Bolt accepted wrong password")
	}
	t.Log("real remote controls: correct HTTP/Bolt credentials return marker; wrong HTTP/Bolt credentials rejected")
	httpURI, httpWire := investigationHTTPProxy(t, remote)
	boltURI, boltWire := investigationBoltProxy(t, "127.0.0.1:"+rb)
	for _, entry := range []struct{ alias, uri string }{{"h", httpURI}, {"b", boltURI}} {
		create := "CREATE COMPOSITE DATABASE explicit_" + entry.alias + " ALIAS r FOR DATABASE nornic AT '" + entry.uri + "' USER admin PASSWORD '" + investigationPass + "' TYPE remote ACCESS read_write"
		investigationMustHTTP(t, local, "system", create, "", false)
		u, _ := url.Parse(entry.uri)
		u.User = url.UserPassword("admin", investigationPass)
		create = "CREATE COMPOSITE DATABASE uri_" + entry.alias + " ALIAS r FOR DATABASE nornic AT '" + u.String() + "' TYPE remote ACCESS read_write"
		investigationMustHTTP(t, local, "system", create, "", false)
	}
	for _, target := range []struct {
		label string
		wire  *investigationWire
	}{{"h", httpWire}, {"b", boltWire}} {
		for _, mode := range []string{"explicit", "uri"} {
			graph := mode + "_" + target.label + ".r"
			for _, ingress := range []string{"HTTP", "GraphQL", "Bolt"} {
				for _, ending := range []string{"commit", "rollback"} {
					name := ingress
					if ending == "rollback" {
						name += "_implicit_rollback"
					}
					t.Run(mode+"/remote_"+target.label+"/"+name, func(t *testing.T) {
						target.wire.reset()
						query := "USE " + graph + " " + investigationRead
						if ending == "rollback" {
							query = "USE " + graph + " CALL nornic.synthetic_missing_credential_workflow()"
						}
						ok, failed, status, requestErr := investigationIngress(local, "bolt://127.0.0.1:"+lb, ingress, query)
						requests, correct, encrypted := target.wire.info()
						t.Logf("marker=%t http_status=%d transport_error=%t outgoing_requests=%d plaintext_password=%t encrypted_password=%t", ok, status, requestErr != nil, requests, correct, encrypted)
						for _, event := range target.wire.trace() {
							t.Log(event)
						}
						var neoErr *neo4j.Neo4jError
						if errors.As(requestErr, &neoErr) {
							t.Logf("Bolt ingress error code=%s", neoErr.Code)
						}
						if (ending == "commit" && (!ok || failed)) || (ending == "rollback" && !failed) || !correct || encrypted {
							t.Errorf("configured credential did not authenticate the full remote workflow")
						}
						investigationAssertHTTPWorkflow(t, target.wire, target.label, ending)
					})
				}
			}
			for _, ingress := range []string{"HTTP", "Bolt"} {
				for _, ending := range []string{"commit", "rollback"} {
					t.Run(mode+"/remote_"+target.label+"/explicit_"+ingress+"_"+ending, func(t *testing.T) {
						target.wire.reset()
						if ingress == "HTTP" {
							investigationHTTPTransaction(t, local, mode+"_"+target.label, graph, ending)
						} else {
							investigationBoltTransaction(t, "bolt://127.0.0.1:"+lb, mode+"_"+target.label, graph, ending)
						}
						requests, correct, encrypted := target.wire.info()
						t.Logf("explicit workflow: outgoing_requests=%d plaintext_password=%t encrypted_password=%t", requests, correct, encrypted)
						if !correct || encrypted {
							t.Error("explicit transaction credentials wrong")
						}
						investigationAssertHTTPWorkflow(t, target.wire, target.label, ending)
					})
				}
			}
		}
		// Database-addressed constituent queries use the manager's runtime
		// resolver rather than the USE/Fabric catalog. Check the real marker.
		t.Run("direct_database/remote_"+target.label, func(t *testing.T) {
			target.wire.reset()
			investigationMustHTTP(t, local, "explicit_"+target.label+".r", investigationRead, "", true)
			requests, correct, encrypted := target.wire.info()
			t.Logf("direct constituent: outgoing_requests=%d plaintext_password=%t encrypted_password=%t", requests, correct, encrypted)
			if !correct || encrypted {
				t.Error("direct constituent credentials wrong")
			}
		})
	}
}

func investigationIngress(base, boltURI, ingress, query string) (bool, bool, int, error) {
	if ingress == "Bolt" {
		marker, err := investigationBolt(boltURI, "nornic", query, neo4j.NoAuth())
		return marker, err != nil, 0, err
	}
	var status int
	var data []byte
	var err error
	if ingress == "HTTP" {
		status, data, err = investigationHTTP(base, "nornic", query, "")
	} else {
		raw, _ := json.Marshal(map[string]string{"query": fmt.Sprintf("query { cypher(input: {statement: %q}) {columns rows rowCount} }", query)})
		response, requestErr := (&http.Client{Timeout: 8 * time.Second}).Post(base+"/graphql", "application/json", bytes.NewReader(raw))
		err = requestErr
		if err == nil {
			data, err = io.ReadAll(response.Body)
			response.Body.Close()
			status = response.StatusCode
		}
	}
	var body struct {
		Errors []json.RawMessage `json:"errors"`
	}
	if err == nil {
		err = json.Unmarshal(data, &body)
	}
	return bytes.Contains(data, []byte(investigationMarker)), len(body.Errors) > 0, status, err
}

func investigationAssertHTTPWorkflow(t *testing.T, wire *investigationWire, transport, ending string) {
	t.Helper()
	if transport != "h" {
		return
	}
	var opened, queried, ended bool
	for _, event := range wire.trace() {
		t.Log(event)
		if !strings.Contains(event, "auth=correct") || strings.Contains(event, "remote_status=401") {
			t.Error("remote workflow request did not retain its credentials")
		}
		if strings.HasPrefix(event, "POST /db/nornic/tx ") {
			opened = true
		}
		if strings.HasPrefix(event, "POST /db/nornic/tx/") && !strings.Contains(event, "/commit ") {
			queried = true
		}
		if ending == "commit" && strings.Contains(event, "/commit ") {
			ended = true
		}
		if ending == "rollback" && strings.HasPrefix(event, "DELETE ") {
			ended = true
		}
	}
	if !opened || !queried || !ended {
		t.Error("remote workflow did not complete open, query and " + ending)
	}
}

func investigationHTTPTransaction(t *testing.T, base, db, graph, ending string) {
	t.Helper()
	request := func(method, target, statement string) []byte {
		t.Helper()
		var reader io.Reader
		if method != http.MethodDelete {
			statements := []map[string]string{}
			if statement != "" {
				statements = append(statements, map[string]string{"statement": statement})
			}
			raw, _ := json.Marshal(map[string]any{"statements": statements})
			reader = bytes.NewReader(raw)
		}
		req, err := http.NewRequest(method, target, reader)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		response, err := (&http.Client{Timeout: 8 * time.Second}).Do(req)
		if err != nil {
			t.Fatal("explicit HTTP request failed")
		}
		defer response.Body.Close()
		data, _ := io.ReadAll(response.Body)
		var decoded investigationResponse
		if json.Unmarshal(data, &decoded) != nil || response.StatusCode >= 400 || len(decoded.Errors) > 0 {
			t.Fatalf("explicit HTTP workflow failed: method=%s status=%d errors=%s", method, response.StatusCode, investigationDiagnostic(fmt.Sprint(decoded.Errors)))
		}
		return data
	}
	data := request(http.MethodPost, base+"/db/"+db+"/tx", "")
	var opened struct {
		Commit string `json:"commit"`
	}
	_ = json.Unmarshal(data, &opened)
	if opened.Commit == "" {
		t.Fatal("explicit HTTP transaction URL missing")
	}
	executeURL := strings.TrimSuffix(opened.Commit, "/commit")
	t.Cleanup(func() {
		req, _ := http.NewRequest(http.MethodDelete, executeURL, nil)
		response, err := (&http.Client{Timeout: 8 * time.Second}).Do(req)
		if err == nil {
			response.Body.Close()
		}
	})
	data = request(http.MethodPost, executeURL, "USE "+graph+" "+investigationRead)
	if !bytes.Contains(data, []byte(investigationMarker)) {
		t.Fatal("explicit HTTP query did not return real remote marker")
	}
	if ending == "commit" {
		request(http.MethodPost, opened.Commit, "")
	} else {
		request(http.MethodDelete, executeURL, "")
	}
}

func investigationBoltTransaction(t *testing.T, uri, db, graph, ending string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	driver, err := neo4j.NewDriverWithContext(uri, neo4j.NoAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer driver.Close(context.Background())
	session := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: db})
	defer session.Close(context.Background())
	tx, err := session.BeginTransaction(ctx)
	if err != nil {
		t.Fatal("Bolt BEGIN failed")
	}
	defer tx.Close(context.Background())
	result, err := tx.Run(ctx, "USE "+graph+" "+investigationRead, nil)
	if err != nil {
		t.Fatal("explicit Bolt query failed")
	}
	records, err := result.Collect(ctx)
	if err != nil || len(records) != 1 || len(records[0].Values) != 1 || records[0].Values[0] != investigationMarker {
		t.Fatal("explicit Bolt query did not return real remote marker")
	}
	if ending == "commit" {
		err = tx.Commit(ctx)
	} else {
		err = tx.Rollback(ctx)
	}
	if err != nil {
		t.Fatal("Bolt " + ending + " failed: " + investigationDiagnostic(err.Error()))
	}
}
