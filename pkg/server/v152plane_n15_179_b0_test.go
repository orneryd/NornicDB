package server

// NOR-442 reproduction: POST /nornicdb/embed/trigger?regenerate=true&database=<name>
// answers 202 but never clears the named database or re-embeds its nodes.
//
// These tests reproduce the reported sequence through the real HTTP router and
// the real manager-owned database storage wrapper, with a controlled embedder
// that records the text it receives:
//
//  1. write two Doc nodes through /db/<db>/tx/commit with pa as the configured
//     embedding property, and wait until the pa text reached the embedder;
//  2. stop the server and start it again on the same data directory with pb as
//     the configured embedding property;
//  3. POST the named-database regenerate trigger.
//
// The named database's old vectors must be cleared and re-embedded from pb, the
// embedder must receive pb: bravo-N, and another database must keep its vectors.
// The same file also pins the issue's controls: global regenerate, the trigger
// without regenerate, and restart alone. Every wait is bounded and observed
// through the controlled embedder's own notification channel.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"regexp"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/auth"
	nornicConfig "github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

const (
	nor442DocQuery = "UNWIND range(1, 2) AS i CREATE (:Doc {k: i, pa: 'alpha-' + toString(i), pb: 'bravo-' + toString(i)})"

	nor442OldProperty = "pa"
	nor442NewProperty = "pb"

	nor442OldTextOne = "pa: alpha-1"
	nor442OldTextTwo = "pa: alpha-2"
	nor442NewTextOne = "pb: bravo-1"
	nor442NewTextTwo = "pb: bravo-2"

	// Positive waits cover the issue's observed ~1 s first embed after a
	// trigger without the original 300 s observation.
	nor442PositiveWait = 8 * time.Second
	// Negative observations are deliberately bounded: they establish that no
	// new text arrived in a window after the controlled action, not that none
	// could arrive after 120 s.
	nor442NegativeObservation = 1 * time.Second

	nor442OtherDatabase = "nor442other"
	nor442OtherNodeID   = storage.NodeID("nor442-other-sentinel")
)

func nor442Dims() int { return 8 }

// nor442ErrorCapture keeps the server's error records so a failed clear keeps
// its own identity in the failing test output.
type nor442ErrorCapture struct {
	mu    sync.Mutex
	lines []string
}

func (c *nor442ErrorCapture) Enabled(context.Context, slog.Level) bool { return true }
func (c *nor442ErrorCapture) WithAttrs([]slog.Attr) slog.Handler       { return c }
func (c *nor442ErrorCapture) WithGroup(string) slog.Handler            { return c }

func (c *nor442ErrorCapture) Handle(_ context.Context, record slog.Record) error {
	if record.Level < slog.LevelError {
		return nil
	}
	var line strings.Builder
	line.WriteString(record.Message)
	record.Attrs(func(attr slog.Attr) bool {
		line.WriteString(" ")
		line.WriteString(attr.Key)
		line.WriteString("=")
		line.WriteString(attr.Value.String())
		return true
	})
	c.mu.Lock()
	c.lines = append(c.lines, line.String())
	c.mu.Unlock()
	return nil
}

func (c *nor442ErrorCapture) find(substring string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, line := range c.lines {
		if strings.Contains(line, substring) {
			return line
		}
	}
	return ""
}

// nor442RecordingEmbedder is the controlled embedder. It records the exact text
// it is asked to embed and returns a deterministic vector for that text, so a
// stored vector can be tied back to the property that produced it.
type nor442RecordingEmbedder struct {
	mu     sync.Mutex
	dims   int
	texts  []string
	notify chan struct{}
}

func newNOR442RecordingEmbedder(dims int) *nor442RecordingEmbedder {
	return &nor442RecordingEmbedder{dims: dims, notify: make(chan struct{}, 1)}
}

func (e *nor442RecordingEmbedder) record(text string) {
	e.mu.Lock()
	e.texts = append(e.texts, text)
	e.mu.Unlock()
	select {
	case e.notify <- struct{}{}:
	default:
	}
}

func (e *nor442RecordingEmbedder) Embed(_ context.Context, text string) ([]float32, error) {
	e.record(text)
	return nor442Vector(text, e.dims), nil
}

func (e *nor442RecordingEmbedder) EmbedBatch(_ context.Context, texts []string) ([][]float32, error) {
	out := make([][]float32, len(texts))
	for i, text := range texts {
		e.record(text)
		out[i] = nor442Vector(text, e.dims)
	}
	return out, nil
}

func (e *nor442RecordingEmbedder) ChunkText(text string, maxTokens, overlap int) ([]string, error) {
	return chunkTestText(text, maxTokens, overlap)
}

func (e *nor442RecordingEmbedder) Dimensions() int { return e.dims }
func (e *nor442RecordingEmbedder) Model() string   { return "nor442-recording-embedder" }
func (e *nor442RecordingEmbedder) Backend() string { return "cpu" }

func (e *nor442RecordingEmbedder) snapshot() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string(nil), e.texts...)
}

func (e *nor442RecordingEmbedder) hasPrefix(prefix string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, text := range e.texts {
		if strings.HasPrefix(text, prefix) {
			return true
		}
	}
	return false
}

// await waits, using the embedder's own notification channel, until every
// wanted text has been recorded. It never waits longer than timeout.
func (e *nor442RecordingEmbedder) await(ctx context.Context, timeout time.Duration, want ...string) error {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		missing := make([]string, 0, len(want))
		texts := e.snapshot()
		for _, wanted := range want {
			if !slices.Contains(texts, wanted) {
				missing = append(missing, wanted)
			}
		}
		if len(missing) == 0 {
			return nil
		}
		select {
		case <-e.notify:
		case <-timer.C:
			return fmt.Errorf("embedder did not receive %v within %s; received %v", missing, timeout, e.snapshot())
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// observeNoPrefix watches the embedder's notification channel for a bounded
// window and fails if any embedded text starts with prefix.
func (e *nor442RecordingEmbedder) observeNoPrefix(prefix string, window time.Duration) error {
	timer := time.NewTimer(window)
	defer timer.Stop()
	for {
		if e.hasPrefix(prefix) {
			return fmt.Errorf("embedder received text with prefix %q within %s: %v", prefix, window, e.snapshot())
		}
		select {
		case <-e.notify:
		case <-timer.C:
			if e.hasPrefix(prefix) {
				return fmt.Errorf("embedder received text with prefix %q within %s: %v", prefix, window, e.snapshot())
			}
			return nil
		}
	}
}

// nor442Vector is a deterministic, dimension-sized encoding of the exact text.
// Different embedding properties therefore produce different stored vectors.
func nor442Vector(text string, dims int) []float32 {
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(text))
	seed := hash.Sum64()
	out := make([]float32, dims)
	for i := range out {
		out[i] = float32((seed>>uint((i%8)*8))&0xff) + float32(i)
	}
	return out
}

func nor442OldVectors() map[string][]float32 {
	return map[string][]float32{
		"1": nor442Vector(nor442OldTextOne, nor442Dims()),
		"2": nor442Vector(nor442OldTextTwo, nor442Dims()),
	}
}

func nor442NewVectors() map[string][]float32 {
	return map[string][]float32{
		"1": nor442Vector(nor442NewTextOne, nor442Dims()),
		"2": nor442Vector(nor442NewTextTwo, nor442Dims()),
	}
}

func nor442OtherVector() []float32 {
	return []float32{921, 922, 923, 924, 925, 926, 927, 928}
}

// nor442Harness is one opened server/database lifetime against a fixed data
// directory. Closing it and opening another harness on the same directory is
// the test's restart.
type nor442Harness struct {
	db            *nornicdb.DB
	server        *Server
	authenticator *auth.Authenticator
	embedder      *nor442RecordingEmbedder
	errors        *nor442ErrorCapture
	token         string
	closed        bool
}

func openNOR442Harness(t *testing.T, dir string, asyncWrites bool, include string) *nor442Harness {
	t.Helper()
	t.Setenv("NORNICDB_REMOTE_CREDENTIALS_KEY", "nor442-test-remote-credential-key-32b!")

	config := nornicdb.DefaultConfig()
	config.Memory.DecayEnabled = false
	config.Memory.AutoLinksEnabled = false
	config.Memory.EmbeddingEnabled = true
	config.Memory.EmbeddingDimensions = nor442Dims()
	config.Database.AsyncWritesEnabled = asyncWrites
	config.Database.MVCCRetentionMaxVersions = 100
	config.EmbeddingWorker.PropertiesInclude = []string{include}
	config.EmbeddingWorker.IncludeLabels = false

	db, err := nornicdb.Open(dir, config)
	require.NoError(t, err)
	harness := &nor442Harness{db: db}
	t.Cleanup(func() { harness.close(t) })

	systemStorage := storage.NewNamespacedEngine(db.GetBaseStorageForManager(), "system")
	authenticator, err := auth.NewAuthenticator(auth.AuthConfig{
		SecurityEnabled: true,
		JWTSecret:       []byte("nor442-test-jwt-secret-32-bytes!!"),
	}, systemStorage)
	require.NoError(t, err)
	if _, err := authenticator.CreateUser(context.Background(), "admin", "password123", []auth.Role{auth.RoleAdmin}); err != nil && !errors.Is(err, auth.ErrUserExists) {
		require.NoError(t, err)
	}

	serverConfig := DefaultConfig()
	serverConfig.Port = 0
	// The per-database embedding gate is resolved from the process config.
	// Keep the gate enabled while the server itself does not start an external
	// provider: the controlled embedder is installed below.
	serverConfig.ProcessConfig = config
	// The controlled embedder is installed directly. Keep the server from
	// starting an external provider initializer.
	serverConfig.EmbeddingEnabled = false
	capture := &nor442ErrorCapture{}
	serverConfig.Logger = slog.New(capture)
	server, err := New(db, authenticator, serverConfig)
	require.NoError(t, err)
	harness.server = server
	harness.authenticator = authenticator
	harness.errors = capture
	harness.embedder = newNOR442RecordingEmbedder(nor442Dims())
	require.True(t, db.SetEmbedder(harness.embedder), "the database must accept the controlled embedder")
	harness.token = "Bearer " + getAuthToken(t, authenticator, "admin")
	return harness
}

func (h *nor442Harness) close(t *testing.T) {
	t.Helper()
	if h.closed {
		return
	}
	h.closed = true
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if h.server != nil {
		if err := h.server.Stop(ctx); err != nil {
			t.Errorf("NOR-442 harness: server stop: %v", err)
		}
	}
	if h.db != nil {
		if err := h.db.Close(); err != nil {
			t.Errorf("NOR-442 harness: database close: %v", err)
		}
	}
}

func (h *nor442Harness) defaultDatabase() string {
	return h.server.dbManager.DefaultDatabaseName()
}

func (h *nor442Harness) writeDocs(t *testing.T) {
	t.Helper()
	recorder := makeRequest(t, h.server, http.MethodPost, "/db/"+h.defaultDatabase()+"/tx/commit", map[string]any{
		"statements": []map[string]any{{"statement": nor442DocQuery}},
	}, h.token)
	require.Contains(t, []int{http.StatusOK, http.StatusAccepted}, recorder.Code, recorder.Body.String())
	var response struct {
		Errors []any `json:"errors"`
	}
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response), recorder.Body.String())
	require.Empty(t, response.Errors, recorder.Body.String())
}

func (h *nor442Harness) trigger(t *testing.T, query string) *httptest.ResponseRecorder {
	t.Helper()
	return makeRequest(t, h.server, http.MethodPost, "/nornicdb/embed/trigger?"+query, nil, h.token)
}

func (h *nor442Harness) embeddingStats(t *testing.T) map[string]any {
	t.Helper()
	response := makeRequest(t, h.server, http.MethodGet, "/nornicdb/embed/stats", nil, h.token)
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	var stats map[string]any
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &stats))
	require.Contains(t, stats, "total_embeddings")
	require.Contains(t, stats, "pending_nodes")
	t.Logf("actual public embedding stats: %s", response.Body.String())
	return stats
}

func (h *nor442Harness) docNodes(database string) ([]*storage.Node, error) {
	engine, err := h.server.dbManager.GetStorage(database)
	if err != nil {
		return nil, err
	}
	return engine.GetNodesByLabel(context.Background(), "Doc")
}

// docVectorsMatch reports whether both Doc nodes hold exactly the vectors for
// the supplied property values. The manager's scoped storage wrapper is used
// for the read, the same storage the trigger receives.
func (h *nor442Harness) docVectorsMatch(want map[string][]float32) bool {
	nodes, err := h.docNodes(h.defaultDatabase())
	if err != nil || len(nodes) != 2 {
		return false
	}
	for _, node := range nodes {
		key := fmt.Sprint(node.Properties["k"])
		expected, ok := want[key]
		if !ok || len(node.ChunkEmbeddings) != 1 || !slices.Equal(node.ChunkEmbeddings[0], expected) {
			return false
		}
	}
	return true
}

func (h *nor442Harness) describeDocVectors() string {
	nodes, err := h.docNodes(h.defaultDatabase())
	if err != nil {
		return "read error: " + err.Error()
	}
	parts := make([]string, 0, len(nodes))
	for _, node := range nodes {
		parts = append(parts, fmt.Sprintf("k=%v vectors=%v", node.Properties["k"], node.ChunkEmbeddings))
	}
	sort.Strings(parts)
	return strings.Join(parts, "; ")
}

func (h *nor442Harness) awaitPhaseOne(t *testing.T) {
	t.Helper()
	require.NoError(t, h.embedder.await(t.Context(), nor442PositiveWait, nor442OldTextOne, nor442OldTextTwo),
		"the initial pa embeddings must reach the controlled embedder")
	require.True(t, nor442Eventually(nor442PositiveWait, 20*time.Millisecond, func() bool {
		return h.docVectorsMatch(nor442OldVectors())
	}), "the initial pa vectors must be stored, got %s", h.describeDocVectors())
}

func (h *nor442Harness) seedOtherDatabase(t *testing.T) storage.Engine {
	t.Helper()
	require.NoError(t, h.server.dbManager.CreateDatabase(context.Background(), multidb.DatabaseCreateRequest{Name: nor442OtherDatabase}))
	engine, err := h.server.dbManager.GetStorage(nor442OtherDatabase)
	require.NoError(t, err)
	_, err = engine.CreateNode(context.Background(), &storage.Node{
		ID:              nor442OtherNodeID,
		Labels:          []string{"Doc"},
		Properties:      map[string]any{"content": "other-database"},
		ChunkEmbeddings: [][]float32{nor442OtherVector()},
	})
	require.NoError(t, err)
	return engine
}

func (h *nor442Harness) clearFailureLog() string {
	if h.errors == nil {
		return ""
	}
	return h.errors.find("failed to clear embeddings")
}

func (h *nor442Harness) requireOtherDatabaseUnchanged(t *testing.T, engine storage.Engine) {
	t.Helper()
	node, err := engine.GetNode(context.Background(), nor442OtherNodeID)
	require.NoError(t, err)
	require.Len(t, node.ChunkEmbeddings, 1)
	require.True(t, slices.Equal(node.ChunkEmbeddings[0], nor442OtherVector()),
		"the other database must keep its vectors, got %v", node.ChunkEmbeddings)
}

// nor442Eventually is a non-failing bounded wait so a failed target assertion
// does not hide the observed state that makes the failure identifiable.
func nor442Eventually(timeout, tick time.Duration, condition func() bool) bool {
	deadline := time.Now().Add(timeout)
	for {
		if condition() {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(tick)
	}
}

// phaseOne writes the two Doc nodes with pa configured and waits for those
// vectors. It closes the server lifetime so the caller can reopen the same
// directory with a different configured property.
func phaseOne(t *testing.T, asyncWrites bool) string {
	t.Helper()
	dir := t.TempDir()
	first := openNOR442Harness(t, dir, asyncWrites, nor442OldProperty)
	first.writeDocs(t)
	first.awaitPhaseOne(t)
	first.close(t)
	return dir
}

func runNOR442Scoped(t *testing.T, asyncWrites bool) {
	t.Helper()
	dir := phaseOne(t, asyncWrites)

	second := openNOR442Harness(t, dir, asyncWrites, nor442NewProperty)
	// Restart alone must not re-embed: existing vectors are from pa.
	if err := second.embedder.observeNoPrefix("pb: bravo-", nor442NegativeObservation); err != nil {
		t.Fatalf("restart alone already re-embedded before any trigger: %v", err)
	}
	require.True(t, second.docVectorsMatch(nor442OldVectors()),
		"restart alone must keep the old pa vectors, got %s", second.describeDocVectors())

	otherEngine := second.seedOtherDatabase(t)
	beforeStats := second.embeddingStats(t)

	recorder := second.trigger(t, "regenerate=true&database="+url.QueryEscape(second.defaultDatabase()))
	require.Equal(t, http.StatusAccepted, recorder.Code, recorder.Body.String())
	var response map[string]any
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response), recorder.Body.String())
	require.Equal(t, true, response["accepted"])
	require.Equal(t, second.defaultDatabase(), response["database"])

	if err := second.embedder.await(t.Context(), nor442PositiveWait, nor442NewTextOne, nor442NewTextTwo); err != nil {
		afterStats := second.embeddingStats(t)
		t.Logf("scoped regeneration stats before=%v after=%v", beforeStats, afterStats)
		if logged := second.clearFailureLog(); logged != "" {
			t.Logf("server clear-failure evidence: %s", logged)
		}
		t.Errorf("database-scoped regenerate did not reach the embedder with the new pb text: %v", err)
		if second.docVectorsMatch(nor442OldVectors()) {
			t.Errorf("named database still holds its old pa vectors after the scoped regenerate: %s", second.describeDocVectors())
		} else {
			t.Errorf("named database vectors after the scoped regenerate are neither the old pa vectors nor the new pb vectors: %s", second.describeDocVectors())
		}
	} else {
		require.True(t, nor442Eventually(nor442PositiveWait, 20*time.Millisecond, func() bool {
			return second.docVectorsMatch(nor442NewVectors())
		}), "named database vectors must be re-embedded from pb, got %s", second.describeDocVectors())
		if err := second.embedder.observeNoPrefix("pa: alpha-", 250*time.Millisecond); err != nil {
			t.Errorf("scoped regenerate must not send the old-property text: %v", err)
		}
	}

	second.requireOtherDatabaseUnchanged(t, otherEngine)
}

func runNOR442GlobalControl(t *testing.T) {
	t.Helper()
	dir := phaseOne(t, false)
	second := openNOR442Harness(t, dir, false, nor442NewProperty)

	recorder := second.trigger(t, "regenerate=true")
	require.Equal(t, http.StatusAccepted, recorder.Code, recorder.Body.String())
	var response map[string]any
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response), recorder.Body.String())
	require.Equal(t, true, response["accepted"])
	require.NotContains(t, response, "database")

	require.NoError(t, second.embedder.await(t.Context(), nor442PositiveWait, nor442NewTextOne, nor442NewTextTwo),
		"global regenerate must re-embed from the newly configured property")
	require.True(t, nor442Eventually(nor442PositiveWait, 20*time.Millisecond, func() bool {
		return second.docVectorsMatch(nor442NewVectors())
	}), "global regenerate must rebuild the vectors from pb, got %s", second.describeDocVectors())
}

func runNOR442PlainTrigger(t *testing.T) {
	t.Helper()
	dir := phaseOne(t, false)
	second := openNOR442Harness(t, dir, false, nor442NewProperty)

	recorder := second.trigger(t, "")
	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())

	if err := second.embedder.observeNoPrefix("pb: bravo-", nor442NegativeObservation); err != nil {
		t.Errorf("the trigger without regenerate must not re-embed existing nodes: %v", err)
	}
	require.True(t, second.docVectorsMatch(nor442OldVectors()),
		"the trigger without regenerate must keep the old pa vectors, got %s", second.describeDocVectors())
}

func runNOR442RestartOnly(t *testing.T) {
	t.Helper()
	dir := phaseOne(t, false)
	second := openNOR442Harness(t, dir, false, nor442NewProperty)

	if err := second.embedder.observeNoPrefix("pb: bravo-", nor442NegativeObservation); err != nil {
		t.Errorf("restart alone must not re-embed from the newly configured property: %v", err)
	}
	require.True(t, second.docVectorsMatch(nor442OldVectors()),
		"restart alone must keep the old pa vectors, got %s", second.describeDocVectors())
}

func TestNOR442DatabaseScopedEmbeddingRegeneration(t *testing.T) {
	forEachNOR442Parser(t, func(t *testing.T) {
		t.Run("sync writes", func(t *testing.T) {
			runNOR442Scoped(t, false)
		})
		t.Run("async writes", func(t *testing.T) {
			runNOR442Scoped(t, true)
		})
		t.Run("global regenerate control", func(t *testing.T) {
			runNOR442GlobalControl(t)
		})
		t.Run("plain trigger leaves vectors", func(t *testing.T) {
			runNOR442PlainTrigger(t)
		})
		t.Run("restart alone leaves vectors", func(t *testing.T) {
			runNOR442RestartOnly(t)
		})
	})
}

// forEachNOR442Parser runs the body once per process-wide Cypher parser
// selection, following the repository's existing parser-variant pattern. The
// query fixture path is exercised under both admitted parser modes.
func forEachNOR442Parser(t *testing.T, body func(*testing.T)) {
	t.Helper()
	const marker = "NOR442_TEST_PARSER_VARIANT"
	if expected := os.Getenv(marker); expected != "" {
		require.Equal(t, expected, nornicConfig.GetParserType())
		body(t)
		return
	}
	name := t.Name()
	for _, parser := range []string{nornicConfig.ParserTypeNornic, nornicConfig.ParserTypeANTLR} {
		t.Run(parser, func(t *testing.T) {
			output, err := runNOR442TestProcess(t, name, marker+"="+parser, nornicConfig.EnvParserType+"="+parser)
			require.NoError(t, err, string(output))
			t.Log(string(output))
		})
	}
}

func runNOR442TestProcess(t *testing.T, name string, environment ...string) ([]byte, error) {
	t.Helper()
	binary, err := os.Executable()
	require.NoError(t, err)
	command := exec.CommandContext(t.Context(), binary,
		"-test.run=^"+regexp.QuoteMeta(name)+"$", "-test.count=1", "-test.timeout=180s", "-test.v")
	command.Env = append(os.Environ(), environment...)
	return command.CombinedOutput()
}
