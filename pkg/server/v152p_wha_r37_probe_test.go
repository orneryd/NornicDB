package server

// Whole-head issues report, row 37 probe (untracked; never committed).
//
// Row 37: a commit that the journal holds but the store never applied, with
// change capture DIFF on, synchronous writes and strict durability. After the
// restart: is the data in the store, and does the change feed hold an event for
// it, read from a cursor taken before the commit? And can a clean shutdown leave
// such a commit behind?

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/auth"
	cypherconfig "github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

const whaEndpoint = "/db/nornic/tx/commit"

type whaNode struct {
	server   *Server
	auth     *auth.Authenticator
	db       *nornicdb.DB
	token    string
	stopped  bool
	stdout   string
	dataDir  string
	listened bool
}

// whaOpen opens dataDir with synchronous writes and strict durability, the way
// openPersistentDBConfigTestServer does otherwise, and returns the server with
// an admin token. With listen, the server is started on a real listener.
func whaOpen(t *testing.T, dataDir string, listen bool) *whaNode {
	t.Helper()
	dbConfig := nornicdb.DefaultConfig()
	dbConfig.Memory.DecayEnabled = false
	dbConfig.Memory.AutoLinksEnabled = false
	dbConfig.Database.AsyncWritesEnabled = false
	dbConfig.Database.StrictDurability = true
	dbConfig.Database.WALSyncMode = "immediate"

	// Capture what Open prints (the recovery at open reports there).
	stdout := os.Stdout
	reader, writer, err := os.Pipe()
	require.NoError(t, err)
	os.Stdout = writer
	captured := make(chan string)
	go func() {
		var buffer bytes.Buffer
		_, _ = io.Copy(&buffer, reader)
		captured <- buffer.String()
	}()
	database, openErr := nornicdb.Open(dataDir, dbConfig)
	os.Stdout = stdout
	_ = writer.Close()
	printed := <-captured
	_ = reader.Close()
	fmt.Print(printed)
	require.NoError(t, openErr)

	authStorage := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = authStorage.Close() })
	authenticator, err := auth.NewAuthenticator(auth.AuthConfig{
		SecurityEnabled: true,
		JWTSecret:       []byte("test-secret-key-for-testing-only-32b"),
	}, authStorage)
	require.NoError(t, err)
	_, err = authenticator.CreateUser(context.Background(), "admin", "password123", []auth.Role{auth.RoleAdmin})
	require.NoError(t, err)

	serverConfig := DefaultConfig()
	serverConfig.Address = "127.0.0.1"
	serverConfig.Port = 0
	serverConfig.EmbeddingEnabled = false
	serverConfig.ProcessConfig = dbConfig
	server, err := New(database, authenticator, serverConfig)
	require.NoError(t, err)
	node := &whaNode{server: server, auth: authenticator, db: database, stdout: printed, dataDir: dataDir}
	node.token = "Bearer " + getAuthToken(t, authenticator, "admin")
	if listen {
		require.NoError(t, server.Start())
		node.listened = true
	}
	t.Cleanup(func() { node.stop(t) })
	return node
}

// stop is the clean shutdown: the server drains and stops, then the database
// and its manager close.
func (n *whaNode) stop(t *testing.T) {
	if n.stopped {
		return
	}
	n.stopped = true
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	require.NoError(t, n.server.Stop(ctx))
	require.NoError(t, n.db.Close())
	if n.server.dbManager != nil {
		_ = n.server.dbManager.Close()
	}
}

func whaRows(t *testing.T, n *whaNode, statement string, params map[string]any) [][]any {
	t.Helper()
	response := makeRequest(t, n.server, http.MethodPost, whaEndpoint,
		map[string]any{"statements": []any{map[string]any{"statement": statement, "parameters": params}}}, n.token)
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	var body struct {
		Results []struct {
			Data []struct {
				Row []any `json:"row"`
			} `json:"data"`
		} `json:"results"`
		Errors []any `json:"errors"`
	}
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body), response.Body.String())
	require.Empty(t, body.Errors, statement)
	require.Len(t, body.Results, 1)
	rows := make([][]any, len(body.Results[0].Data))
	for index, data := range body.Results[0].Data {
		rows[index] = data.Row
	}
	return rows
}

func whaSetDiff(t *testing.T, n *whaNode) {
	t.Helper()
	require.Empty(t, txErrorCode(t, n.server, "system", strings.TrimPrefix(n.token, "Bearer "), "ALTER DATABASE nornic SET OPTION txLogEnrichment 'DIFF'"))
}

func whaCursor(t *testing.T, n *whaNode, procedure string) string {
	t.Helper()
	return whaRows(t, n, "CALL "+procedure+"() YIELD id RETURN id", nil)[0][0].(string)
}

// whaEventNames lists "elementId name" of the change events after from that
// touch a node labelled label, sorted.
func whaEventNames(t *testing.T, n *whaNode, from, label string) []string {
	t.Helper()
	rows := whaRows(t, n, "CALL db.cdc.query($from, $selectors) YIELD id, event RETURN event.elementId, event.state.after.properties.name",
		map[string]any{"from": from, "selectors": []any{map[string]any{"select": "n", "labels": []any{label}}}})
	return whaPairs(rows)
}

// whaNodeNames lists "elementId name" of the nodes labelled label, sorted.
func whaNodeNames(t *testing.T, n *whaNode, label string) []string {
	t.Helper()
	return whaPairs(whaRows(t, n, "MATCH (x:"+label+") RETURN elementId(x), x.name", nil))
}

func whaPairs(rows [][]any) []string {
	out := make([]string, 0, len(rows))
	for _, row := range rows {
		out = append(out, fmt.Sprintf("%v %v", row[0], row[1]))
	}
	sort.Strings(out)
	return out
}

func whaElementID(pair string) string { return strings.SplitN(pair, " ", 2)[0] }

func whaCopyDir(t *testing.T, from, to string) {
	t.Helper()
	require.NoError(t, filepath.WalkDir(from, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(from, path)
		if err != nil {
			return err
		}
		target := filepath.Join(to, rel)
		if entry.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(target, data, 0o644)
	}))
}

func whaJournal(t *testing.T, dir string) string {
	t.Helper()
	entries, err := whaReadJournal(dir)
	if err != nil {
		return "unreadable: " + err.Error()
	}
	counts := map[string]int{}
	for _, entry := range entries {
		counts[string(entry.Operation)]++
	}
	keys := make([]string, 0, len(counts))
	for key := range counts {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, fmt.Sprintf("%s=%d", key, counts[key]))
	}
	return fmt.Sprintf("%d records [%s]", len(entries), strings.Join(parts, " "))
}

func whaRecoveredLine(printed string) string {
	for _, line := range strings.Split(printed, "\n") {
		if strings.Contains(line, "Recovered") {
			return strings.TrimSpace(line)
		}
	}
	return "(no recovery line)"
}

func whaParsers(t *testing.T, run func(t *testing.T)) {
	for _, parser := range []string{cypherconfig.ParserTypeNornic, cypherconfig.ParserTypeANTLR} {
		t.Run(parser, func(t *testing.T) {
			previous := cypherconfig.GetParserType()
			cypherconfig.SetParserType(parser)
			t.Cleanup(func() { cypherconfig.SetParserType(previous) })
			run(t)
		})
	}
}

// A store whose journal holds a commit its store never applied: store A takes
// a baseline write and a cursor, closes cleanly and is copied to B; A then
// commits node "y" and closes cleanly; B receives A's journal. B's store lacks
// "y" while its journal holds y's commit, as after a stop between journaling
// and application. B is opened as the server opens a store.
func TestWhaR37JournaledCommitNotApplied(t *testing.T) {
	whaParsers(t, func(t *testing.T) {
		dirA, dirB := t.TempDir(), t.TempDir()
		a := whaOpen(t, dirA, false)
		whaSetDiff(t, a)
		whaRows(t, a, "CREATE (:R37 {name: 'base'})", nil)
		cursor := whaCursor(t, a, "db.cdc.current")
		earliest := whaCursor(t, a, "db.cdc.earliest")
		t.Logf("R37 session1: nodes=%v events-from-earliest=%v journal %s", whaNodeNames(t, a, "R37"), whaEventNames(t, a, earliest, "R37"), whaJournal(t, filepath.Join(dirA, "wal")))
		a.stop(t)
		whaCopyDir(t, dirA, dirB)

		a = whaOpen(t, dirA, false)
		whaRows(t, a, "CREATE (:R37 {name: 'y'})", nil)
		t.Logf("R37 session2 live: nodes=%v events-from-cursor=%v", whaNodeNames(t, a, "R37"), whaEventNames(t, a, cursor, "R37"))
		a.stop(t)
		t.Logf("R37 journal A after session2 %s; journal B before swap %s", whaJournal(t, filepath.Join(dirA, "wal")), whaJournal(t, filepath.Join(dirB, "wal")))

		require.NoError(t, os.RemoveAll(filepath.Join(dirB, "wal")))
		whaCopyDir(t, filepath.Join(dirA, "wal"), filepath.Join(dirB, "wal"))

		b := whaOpen(t, dirB, false)
		nodes := whaNodeNames(t, b, "R37")
		events := whaEventNames(t, b, cursor, "R37")
		t.Logf("R37 RESULT restart: recovery=%q nodes=%v events-from-cursor=%v events-from-earliest=%v", whaRecoveredLine(b.stdout), nodes, events, whaEventNames(t, b, earliest, "R37"))
		b.stop(t)

		b = whaOpen(t, dirB, false)
		t.Logf("R37 RESULT second restart: recovery=%q nodes=%v events-from-cursor=%v", whaRecoveredLine(b.stdout), whaNodeNames(t, b, "R37"), whaEventNames(t, b, cursor, "R37"))
		b.stop(t)
	})
}

// Clean shutdown under write load. Sixteen writers create nodes over HTTP
// through a real listener while the server is shut down cleanly (drain, stop,
// database close). On reopen: does the recovery at open apply anything, and
// does every node in the store have its change event? The "close during
// writes" variant sends the writes straight to the handler, so the server's
// drain does not wait for them and they race the database close.
func TestWhaR37CleanShutdownLeavesNoUnappliedCommit(t *testing.T) {
	for _, variant := range []string{"listener drain", "close during writes"} {
		t.Run(variant, func(t *testing.T) {
			for round := 0; round < 3; round++ {
				dir := t.TempDir()
				listen := variant == "listener drain"
				n := whaOpen(t, dir, listen)
				whaSetDiff(t, n)
				earliest := whaCursor(t, n, "db.cdc.earliest")
				var acknowledged, failed atomic.Int64
				var stop atomic.Bool
				var wg sync.WaitGroup
				client := &http.Client{Timeout: 10 * time.Second}
				address := n.server.Addr()
				for writer := 0; writer < 16; writer++ {
					wg.Add(1)
					go func(writer int) {
						defer wg.Done()
						for index := 0; !stop.Load(); index++ {
							payload, _ := json.Marshal(map[string]any{"statements": []any{map[string]any{
								"statement":  "CREATE (:R37L {name: $name})",
								"parameters": map[string]any{"name": fmt.Sprintf("w%02d-%05d", writer, index)},
							}}})
							ok := false
							if listen {
								request, _ := http.NewRequest(http.MethodPost, "http://"+address+whaEndpoint, bytes.NewReader(payload))
								request.Header.Set("Authorization", n.token)
								request.Header.Set("Content-Type", "application/json")
								response, err := client.Do(request)
								if err == nil {
									body, _ := io.ReadAll(response.Body)
									_ = response.Body.Close()
									ok = response.StatusCode == http.StatusOK && !bytes.Contains(body, []byte(`"code"`))
								}
							} else {
								func() {
									defer func() { _ = recover() }()
									var body map[string]any
									_ = json.Unmarshal(payload, &body)
									response := makeRequest(t, n.server, http.MethodPost, whaEndpoint, body, n.token)
									ok = response.Code == http.StatusOK && !bytes.Contains(response.Body.Bytes(), []byte(`"code"`))
								}()
							}
							if ok {
								acknowledged.Add(1)
							} else {
								failed.Add(1)
								if stop.Load() {
									return
								}
								time.Sleep(time.Millisecond)
							}
						}
					}(writer)
				}
				time.Sleep(1500 * time.Millisecond)
				shutdownStarted := acknowledged.Load()
				n.stop(t)
				stop.Store(true)
				wg.Wait()

				r := whaOpen(t, dir, false)
				nodes := whaNodeNames(t, r, "R37L")
				events := whaEventNames(t, r, earliest, "R37L")
				eventSet := map[string]bool{}
				for _, pair := range events {
					eventSet[whaElementID(pair)] = true
				}
				var withoutEvent []string
				for _, pair := range nodes {
					if !eventSet[whaElementID(pair)] {
						withoutEvent = append(withoutEvent, pair)
					}
				}
				t.Logf("R37 RESULT shutdown variant=%q round=%d: acknowledged=%d (at shutdown start %d) failed=%d recovery=%q nodes=%d events=%d nodes-without-event=%d %v",
					variant, round, acknowledged.Load(), shutdownStarted, failed.Load(), whaRecoveredLine(r.stdout), len(nodes), len(events), len(withoutEvent), whaFirst(withoutEvent, 10))
				r.stop(t)
			}
		})
	}
}

func whaReadJournal(dir string) ([]storage.WALEntry, error) {
	return storage.ReadWALEntriesFromDir(context.Background(), dir)
}

func whaFirst(list []string, limit int) []string {
	if len(list) > limit {
		return list[:limit]
	}
	return list
}
