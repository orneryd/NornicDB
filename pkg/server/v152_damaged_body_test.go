package server

// v1.5.2 verification: damaged node bodies in storage (scratch, test-only).
//
// Each test builds a real data directory through the public library API,
// closes it, overwrites chosen node bodies with damaged bytes through a raw
// Badger handle (the database is closed), reopens it with a server and reads
// over HTTP and Bolt. A test fails (t.Errorf with "DEFECTn") when the defect
// it reproduces is present; harness problems use t.Fatalf.
//
// Defect 1: whole-node read of a node with an unencodable value: HTTP 200
//           with an empty body; Bolt returns null instead of the node.
// Defect 2: point lookup of a node whose body cannot be read returns no row
//           and no error, while a label scan reports the damage.
// Defect 3: storage damage is classified as Neo.ClientError.Statement.SyntaxError,
//           and one damaged body stops the search index build for the database.
// Defect 4: server start fails when a body is damaged and the one-time
//           legacy-data check has not been recorded yet.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	neo4j "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/dbtype"
	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/bolt"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
)

const (
	dmgPassword = "password123"
	dmgDeadline = 60 * time.Second
	dmgSyntax   = "Neo.ClientError.Statement.SyntaxError"
)

// ---------------------------------------------------------------------------
// Outcome of one statement.

type dmgOutcome struct {
	Status      int
	Body        string
	Rows        [][]any
	Codes       []string
	Errors      []string
	Hang        bool
	Panic       bool
	Undecodable bool
}

func (o dmgOutcome) String() string {
	body := o.Body
	if len(body) > 300 {
		body = body[:300] + "..."
	}
	return fmt.Sprintf("status=%d bodyLen=%d undecodable=%v hang=%v panic=%v rows=%v errors=%v body=%q",
		o.Status, len(o.Body), o.Undecodable, o.Hang, o.Panic, o.Rows, o.Errors, body)
}

// ---------------------------------------------------------------------------
// HTTP and Bolt transports.

type dmgHTTP struct {
	t        *testing.T
	router   http.Handler
	token    string
	database string
}

func (h *dmgHTTP) run(statement string, params map[string]any) dmgOutcome {
	payload, err := json.Marshal(map[string]any{"statements": []map[string]any{{"statement": statement, "parameters": params}}})
	if err != nil {
		h.t.Fatalf("marshal: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/db/"+h.database+"/tx/commit", bytes.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+h.token)
	rec := httptest.NewRecorder()
	done := make(chan struct{})
	var panicked any
	go func() {
		defer close(done)
		defer func() { panicked = recover() }()
		h.router.ServeHTTP(rec, req)
	}()
	select {
	case <-done:
	case <-time.After(dmgDeadline):
		return dmgOutcome{Hang: true}
	}
	out := dmgOutcome{Status: rec.Code, Body: rec.Body.String()}
	if panicked != nil {
		out.Panic = true
		out.Errors = append(out.Errors, fmt.Sprintf("PANIC %v", panicked))
		return out
	}
	var resp TransactionResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		out.Undecodable = true
		return out
	}
	for _, e := range resp.Errors {
		out.Codes = append(out.Codes, e.Code)
		out.Errors = append(out.Errors, e.Code+": "+e.Message)
	}
	if len(resp.Results) > 0 {
		for _, row := range resp.Results[0].Data {
			out.Rows = append(out.Rows, row.Row)
		}
	}
	return out
}

// post sends one JSON request to path and returns status and body.
func (h *dmgHTTP) post(path string, body any) (int, string) {
	payload, err := json.Marshal(body)
	if err != nil {
		h.t.Fatalf("marshal: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+h.token)
	rec := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer func() { _ = recover() }()
		h.router.ServeHTTP(rec, req)
	}()
	select {
	case <-done:
	case <-time.After(dmgDeadline):
		return 0, "HANG"
	}
	return rec.Code, rec.Body.String()
}

type dmgBolt struct {
	driver   neo4j.DriverWithContext
	database string
}

func dmgStartBolt(t *testing.T, server *Server, authenticator *auth.Authenticator) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	config := bolt.DefaultConfig()
	adapter := bolt.NewAuthenticatorAdapter(authenticator)
	adapter.SetGetEffectivePermissions(server.GetEffectivePermissions)
	config.Authenticator = adapter
	config.RequireAuth = true
	config.DatabaseExecutorOptions = server.DatabaseScopedExecutorOptions
	boltServer := bolt.NewWithDatabaseManager(config, nil, server.GetDatabaseManager())
	boltServer.SetDatabaseAccessModeResolver(server.GetDatabaseAccessModeForRoles)
	boltServer.SetResolvedAccessResolver(server.GetResolvedAccessForRoles)
	boltServer.SetProcedureGrantResolver(server.ProcedureGrantChecker)
	finished := make(chan error, 1)
	go func() { finished <- boltServer.Serve(listener) }()
	t.Cleanup(func() {
		_ = boltServer.Close()
		select {
		case <-finished:
		case <-time.After(10 * time.Second):
		}
	})
	return "bolt://" + listener.Addr().String()
}

func dmgBoltCode(err error) string {
	var neoErr *neo4j.Neo4jError
	if errors.As(err, &neoErr) {
		return neoErr.Code
	}
	return ""
}

func (b *dmgBolt) run(statement string, params map[string]any) dmgOutcome {
	ctx, cancel := context.WithTimeout(context.Background(), dmgDeadline)
	defer cancel()
	session := b.driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: b.database})
	defer session.Close(context.Background())
	var out dmgOutcome
	fail := func(err error) dmgOutcome {
		out.Codes = append(out.Codes, dmgBoltCode(err))
		out.Errors = append(out.Errors, err.Error())
		out.Hang = ctx.Err() != nil
		return out
	}
	result, err := session.Run(ctx, statement, params)
	if err != nil {
		return fail(err)
	}
	records, err := result.Collect(ctx)
	if err != nil {
		return fail(err)
	}
	for _, record := range records {
		out.Rows = append(out.Rows, record.Values)
	}
	return out
}

// ---------------------------------------------------------------------------
// Store construction, raw damage and serving.

type dmgNode struct {
	id     string
	labels []string
	props  map[string]any
}

func dmgConfig() *nornicdb.Config {
	config := nornicdb.DefaultConfig()
	config.Memory.DecayEnabled = false
	config.Memory.AutoLinksEnabled = false
	config.Database.AsyncWritesEnabled = false
	return config
}

func dmgAuthenticator(t *testing.T, db *nornicdb.DB) *auth.Authenticator {
	t.Helper()
	authenticator, err := auth.NewAuthenticator(auth.AuthConfig{SecurityEnabled: true, JWTSecret: []byte("test-secret-key-for-testing-only-32b")},
		storage.NewNamespacedEngine(db.GetBaseStorageForManager(), "system"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := authenticator.CreateUser(context.Background(), "admin", dmgPassword, []auth.Role{auth.RoleAdmin}); err != nil {
		t.Logf("NOTE admin user: %v", err)
	}
	return authenticator
}

func dmgServerConfig() *Config {
	config := DefaultConfig()
	config.Port = 0
	config.EmbeddingEnabled = false
	return config
}

// dmgCreateStore writes nodes into a new data directory. With firstStart the
// server is started and stopped once on the valid data, as a normal first
// start does, which records the one-time legacy-data check.
func dmgCreateStore(t *testing.T, nodes []dmgNode, firstStart bool) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "v152-dmg-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	db, err := nornicdb.Open(dir, dmgConfig())
	if err != nil {
		t.Fatal(err)
	}
	store := db.GetStorage()
	for _, n := range nodes {
		if _, err := store.CreateNode(context.Background(), &storage.Node{ID: storage.NodeID(n.id), Labels: n.labels, Properties: n.props}); err != nil {
			t.Fatalf("create %s: %v", n.id, err)
		}
	}
	if firstStart {
		authenticator := dmgAuthenticator(t, db)
		first, err := New(db, authenticator, dmgServerConfig())
		if err != nil {
			t.Fatalf("first start on valid data: %v", err)
		}
		stopTestServer(t, first)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	return dir
}

func dmgNodeKeyID(key []byte) (string, bool) {
	if len(key) < 2 || key[0] != 0x01 {
		return "", false
	}
	rest := string(key[1:])
	if index := strings.LastIndex(rest, ":"); index >= 0 {
		rest = rest[index+1:]
	}
	return rest, true
}

// dmgDamage overwrites the stored body of each named node with damage(body)
// while the database is closed, and returns the original bodies.
func dmgDamage(t *testing.T, dir string, damage map[string]func([]byte) []byte) map[string][]byte {
	t.Helper()
	var manifests []string
	_ = filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err == nil && info != nil && !info.IsDir() && info.Name() == "MANIFEST" {
			manifests = append(manifests, filepath.Dir(path))
		}
		return nil
	})
	originals := map[string][]byte{}
	for _, candidate := range manifests {
		raw, err := badger.OpenManaged(badger.DefaultOptions(candidate).WithLogger(nil))
		if err != nil {
			t.Logf("NOTE raw open %s: %v", candidate, err)
			continue
		}
		keys := map[string][]byte{}
		readTxn := raw.NewTransactionAt(math.MaxUint64, false)
		iterator := readTxn.NewIterator(badger.DefaultIteratorOptions)
		for iterator.Seek([]byte{0x01}); iterator.ValidForPrefix([]byte{0x01}); iterator.Next() {
			item := iterator.Item()
			if id, ok := dmgNodeKeyID(item.Key()); ok && damage[id] != nil {
				if value, err := item.ValueCopy(nil); err == nil {
					keys[id] = item.KeyCopy(nil)
					originals[id] = value
				}
			}
		}
		iterator.Close()
		readTxn.Discard()
		if len(keys) == 0 {
			_ = raw.Close()
			continue
		}
		commitTs := raw.MaxVersion() + 1
		writeTxn := raw.NewTransactionAt(raw.MaxVersion(), true)
		ids := make([]string, 0, len(keys))
		for id := range keys {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		for _, id := range ids {
			body := damage[id](append([]byte(nil), originals[id]...))
			if err := writeTxn.Set(keys[id], body); err != nil {
				t.Fatalf("raw set %s: %v", id, err)
			}
		}
		if err := writeTxn.CommitAt(commitTs, nil); err != nil {
			t.Fatalf("raw commit: %v", err)
		}
		if err := raw.Close(); err != nil {
			t.Fatalf("raw close: %v", err)
		}
		break
	}
	if len(originals) != len(damage) {
		t.Fatalf("damaged %d of %d requested node bodies (manifests %v)", len(originals), len(damage), manifests)
	}
	return originals
}

// dmgCleanShutdownMarkerKey is the storage key a clean close writes
// (prefixMVCCMeta 0x10, prefixMVCCMetaCleanShutdown 0x08).
var dmgCleanShutdownMarkerKey = []byte{0x10, 0x08}

// dmgDropCleanShutdownMarker removes the clean-shutdown marker while the
// database is closed, as an unclean stop leaves it.
func dmgDropCleanShutdownMarker(t *testing.T, dir string) {
	t.Helper()
	var manifests []string
	_ = filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err == nil && info != nil && !info.IsDir() && info.Name() == "MANIFEST" {
			manifests = append(manifests, filepath.Dir(path))
		}
		return nil
	})
	dropped := 0
	for _, candidate := range manifests {
		raw, err := badger.OpenManaged(badger.DefaultOptions(candidate).WithLogger(nil))
		if err != nil {
			continue
		}
		readTxn := raw.NewTransactionAt(math.MaxUint64, false)
		_, getErr := readTxn.Get(dmgCleanShutdownMarkerKey)
		readTxn.Discard()
		if getErr == nil {
			writeTxn := raw.NewTransactionAt(raw.MaxVersion(), true)
			if err := writeTxn.Delete(dmgCleanShutdownMarkerKey); err != nil {
				t.Fatalf("drop marker: %v", err)
			}
			if err := writeTxn.CommitAt(raw.MaxVersion()+1, nil); err != nil {
				t.Fatalf("drop marker commit: %v", err)
			}
			dropped++
		}
		_ = raw.Close()
	}
	t.Logf("D4 clean-shutdown markers dropped: %d", dropped)
	if dropped == 0 {
		t.Fatalf("no clean-shutdown marker found (manifests %v)", manifests)
	}
}

type dmgEnv struct {
	db       *nornicdb.DB
	server   *Server
	http     *dmgHTTP
	bolt     *dmgBolt
	database string
}

// dmgServe reopens dir and starts a server on it. A failed server start is
// returned, not fatal (defect 4 reproduces exactly that).
func dmgServe(t *testing.T, dir string) (*dmgEnv, error) {
	t.Helper()
	db, err := nornicdb.Open(dir, dmgConfig())
	if err != nil {
		return nil, fmt.Errorf("open: %w", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	authenticator := dmgAuthenticator(t, db)
	srv, err := New(db, authenticator, dmgServerConfig())
	if err != nil {
		return &dmgEnv{db: db}, err
	}
	t.Cleanup(func() { stopTestServer(t, srv) })
	database := srv.dbManager.DefaultDatabaseName()
	env := &dmgEnv{db: db, server: srv, database: database}
	env.http = &dmgHTTP{t: t, router: srv.buildRouter(), token: getAuthToken(t, authenticator, "admin"), database: database}
	driver, err := neo4j.NewDriverWithContext(dmgStartBolt(t, srv, authenticator), neo4j.BasicAuth("admin", dmgPassword, ""))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = driver.Close(context.Background()) })
	env.bolt = &dmgBolt{driver: driver, database: database}
	return env, nil
}

// dmgExecutorView logs the Go value the executor produces for RETURN n and
// the type of every property, with whether standard JSON can encode it.
func dmgExecutorView(db *nornicdb.DB, id string) string {
	result, err := db.ExecuteCypher(context.Background(), "MATCH (n) WHERE id(n) = $id RETURN n AS n", map[string]any{"id": id})
	if err != nil {
		return "executor error: " + err.Error()
	}
	if len(result.Rows) == 0 || len(result.Rows[0]) == 0 {
		return "executor: no row"
	}
	cell := result.Rows[0][0]
	view := fmt.Sprintf("cell %T", cell)
	var props map[string]any
	switch node := cell.(type) {
	case *storage.Node:
		if node != nil {
			props = node.Properties
		}
	case map[string]any:
		props, _ = node["properties"].(map[string]any)
		if props == nil {
			props = node
		}
	}
	keys := make([]string, 0, len(props))
	for key := range props {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		value := props[key]
		_, encodeErr := json.Marshal(value)
		text := fmt.Sprintf("%v", value)
		if len(text) > 120 {
			text = text[:120] + "..."
		}
		kind := "<nil>"
		if value != nil {
			kind = reflect.TypeOf(value).String()
		}
		view += fmt.Sprintf(" | %s %s %s jsonErr=%v", key, kind, text, encodeErr)
	}
	return view
}

func dmgIsNil(value any) bool {
	if value == nil {
		return true
	}
	rv := reflect.ValueOf(value)
	switch rv.Kind() {
	case reflect.Ptr, reflect.Map, reflect.Slice, reflect.Interface:
		return rv.IsNil()
	}
	return false
}

// ---------------------------------------------------------------------------
// Defect 1, normal use: values a client can store with ordinary Cypher.

func TestV152DamagedBodyD1NormalUseValues(t *testing.T) {
	t.Setenv("NORNICDB_REMOTE_CREDENTIALS_KEY", "test-remote-credential-key-32b!")
	dir := dmgCreateStore(t, nil, true)
	env, err := dmgServe(t, dir)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	creates := []struct {
		id        string
		statement string
		params    map[string]any
		viaBolt   bool
	}{
		{"nan", "CREATE (n:N1 {id: 'nan', f: 0.0/0.0}) RETURN n.id AS id", nil, false},
		{"inf", "CREATE (n:N1 {id: 'inf', f: 1.0/0.0}) RETURN n.id AS id", nil, false},
		{"listnan", "CREATE (n:N1 {id: 'listnan', v: [1.0, 0.0/0.0]}) RETURN n.id AS id", nil, false},
		{"year12000", "CREATE (n:N1 {id: 'year12000', d: datetime({year: 12000, month: 1, day: 1})}) RETURN n.id AS id", nil, false},
		{"yearneg", "CREATE (n:N1 {id: 'yearneg', d: date({year: -50, month: 1, day: 1})}) RETURN n.id AS id", nil, false},
		{"boltnan", "CREATE (n:N1 {id: 'boltnan', f: $f}) RETURN n.id AS id", map[string]any{"f": math.NaN()}, true},
		{"boltlistnan", "CREATE (n:N1 {id: 'boltlistnan', v: $v}) RETURN n.id AS id", map[string]any{"v": []any{1.0, math.NaN()}}, true},
		{"control", "CREATE (n:N1 {id: 'control', f: 1.5}) RETURN n.id AS id", nil, false},
	}
	for _, c := range creates {
		var o dmgOutcome
		if c.viaBolt {
			o = env.bolt.run(c.statement, c.params)
		} else {
			o = env.http.run(c.statement, c.params)
		}
		t.Logf("D1N CREATE %s via bolt=%v: %s", c.id, c.viaBolt, o)
	}
	for _, c := range creates {
		params := map[string]any{"id": c.id}
		for _, statement := range []string{
			"MATCH (n:N1 {id: $id}) RETURN n AS n",
			"MATCH (n:N1 {id: $id}) RETURN properties(n) AS p",
			"MATCH (n:N1 {id: $id}) RETURN n.id AS id",
		} {
			h := env.http.run(statement, params)
			b := env.bolt.run(statement, params)
			t.Logf("D1N READ %s | %s | http %s | bolt %s", c.id, statement, h, b)
			if c.id == "control" {
				if h.Status != 200 || len(h.Rows) != 1 || len(b.Rows) != 1 {
					t.Fatalf("control read failed: http %s bolt %s", h, b)
				}
				continue
			}
			if h.Status == 200 && len(h.Body) == 0 {
				t.Errorf("DEFECT1 normal-use value %s: HTTP 200 with empty body for %q", c.id, statement)
			}
			if len(b.Errors) == 0 && len(b.Rows) == 1 && len(b.Rows[0]) == 1 && dmgIsNil(b.Rows[0][0]) && !strings.Contains(statement, "n.id") {
				t.Errorf("DEFECT1 normal-use value %s: Bolt returned null and no error for %q", c.id, statement)
			}
		}
		t.Logf("D1N EXEC %s: %s", c.id, dmgExecutorView(env.db, c.id))
	}
}

// ---------------------------------------------------------------------------
// Defect 1, damaged values: single-property bodies, every byte changed.

func TestV152DamagedBodyD1WholeNodeEncode(t *testing.T) {
	t.Setenv("NORNICDB_REMOTE_CREDENTIALS_KEY", "test-remote-credential-key-32b!")
	const perShape = 160
	shapes := map[string]func() map[string]any{
		"vec": func() map[string]any { return map[string]any{"vec": []float32{0.5, 1.5, 2.5}} },
		"t":   func() map[string]any { return map[string]any{"t": time.Date(2020, 1, 2, 3, 4, 5, 6, time.UTC)} },
		"f":   func() map[string]any { return map[string]any{"f": 3.25} },
	}
	shapeNames := []string{"f", "t", "vec"}
	var nodes []dmgNode
	for _, shape := range shapeNames {
		for i := 0; i < perShape; i++ {
			nodes = append(nodes, dmgNode{id: fmt.Sprintf("d1-%s-%03d", shape, i), labels: []string{"D1"}, props: shapes[shape]()})
		}
	}
	dir := dmgCreateStore(t, nodes, true)
	// One mutation per node: position p, kind ff or xor. The last node of each
	// shape stays intact as a control.
	type mutation struct {
		kind string
		pos  int
	}
	applied := map[string]mutation{}
	damage := map[string]func([]byte) []byte{}
	for _, shape := range shapeNames {
		for i := 0; i < perShape-1; i++ {
			m := mutation{kind: []string{"ff", "xor"}[i%2], pos: i / 2}
			id := fmt.Sprintf("d1-%s-%03d", shape, i)
			applied[id] = m
			damage[id] = func(body []byte) []byte {
				if m.pos >= len(body) {
					return body
				}
				if m.kind == "ff" {
					body[m.pos] = 0xFF
				} else {
					body[m.pos] ^= 0xFF
				}
				return body
			}
		}
	}
	originals := dmgDamage(t, dir, damage)
	for _, shape := range shapeNames {
		t.Logf("D1 shape %s: body %d bytes: % x", shape, len(originals[fmt.Sprintf("d1-%s-000", shape)]), originals[fmt.Sprintf("d1-%s-000", shape)])
	}
	env, err := dmgServe(t, dir)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	for _, shape := range shapeNames {
		control := fmt.Sprintf("d1-%s-%03d", shape, perShape-1)
		h := env.http.run("MATCH (n) WHERE id(n) = $id RETURN n AS n", map[string]any{"id": control})
		if h.Status != 200 || len(h.Rows) != 1 {
			t.Fatalf("control %s: %s", control, h)
		}
	}
	categories := map[string]int{}
	shown := map[string]int{}
	ids := make([]string, 0, len(applied))
	for id := range applied {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		if applied[id].pos >= len(originals[id]) {
			continue
		}
		params := map[string]any{"id": id}
		h := env.http.run("MATCH (n) WHERE id(n) = $id RETURN n AS n", params)
		b := env.bolt.run("MATCH (n) WHERE id(n) = $id RETURN n AS n", params)
		httpKind := "other"
		switch {
		case h.Hang || h.Panic || h.Status >= 500:
			httpKind = "crash"
		case h.Status == 200 && len(h.Body) == 0:
			httpKind = "empty200"
		case len(h.Errors) > 0:
			httpKind = "error"
		case len(h.Rows) == 0:
			httpKind = "norow"
		case len(h.Rows) == 1:
			httpKind = "row"
		}
		boltKind := "other"
		switch {
		case b.Hang:
			boltKind = "hang"
		case len(b.Errors) > 0:
			boltKind = "error"
		case len(b.Rows) == 0:
			boltKind = "norow"
		case len(b.Rows) == 1 && len(b.Rows[0]) == 1 && dmgIsNil(b.Rows[0][0]):
			boltKind = "null"
		case len(b.Rows) == 1:
			boltKind = "row"
		}
		category := "http-" + httpKind + "/bolt-" + boltKind
		categories[category]++
		if httpKind == "empty200" || boltKind == "null" {
			shape := strings.Split(id, "-")[1]
			key := category + "/" + shape
			if shown[key] < 4 {
				shown[key]++
				idRead := env.http.run("MATCH (n) WHERE id(n) = $id RETURN id(n) AS i", params)
				t.Logf("D1 CASE %s %v %s | http %s | bolt %s | id-only http %s | %s", id, applied[id], category, h, b, idRead, dmgExecutorView(env.db, id))
			}
		}
	}
	names := make([]string, 0, len(categories))
	for name := range categories {
		names = append(names, name)
	}
	sort.Strings(names)
	empty, null := 0, 0
	for _, name := range names {
		t.Logf("D1 CATEGORY %s: %d", name, categories[name])
		if strings.HasPrefix(name, "http-empty200") {
			empty += categories[name]
		}
		if strings.HasSuffix(name, "bolt-null") {
			null += categories[name]
		}
	}
	if empty > 0 {
		t.Errorf("DEFECT1 %d damaged nodes: RETURN n answered HTTP 200 with an empty body", empty)
	}
	if null > 0 {
		t.Errorf("DEFECT1 %d damaged nodes: RETURN n answered over Bolt with null and no error", null)
	}
}

// ---------------------------------------------------------------------------
// Defects 2 and 3: point lookups, error classification, search build.

func dmgD2Nodes() []dmgNode {
	return []dmgNode{
		{id: "d2-ok", labels: []string{"Fz"}, props: map[string]any{"k": int64(1), "s": "fine", "text": "zebrafind healthy one"}},
		{id: "d2-ok2", labels: []string{"Fz"}, props: map[string]any{"k": int64(5), "s": "fine2", "text": "zebrafind healthy two"}},
		{id: "d2-empty", labels: []string{"Fz"}, props: map[string]any{"k": int64(2), "s": "e"}},
		{id: "d2-format", labels: []string{"Fz"}, props: map[string]any{"k": int64(3), "s": "f"}},
		{id: "d2-trunc", labels: []string{"Fz"}, props: map[string]any{"k": int64(4), "s": "t"}},
	}
}

func dmgD2Damage() map[string]func([]byte) []byte {
	return map[string]func([]byte) []byte{
		"d2-empty":  func([]byte) []byte { return []byte{} },
		"d2-format": func(body []byte) []byte { body[0] = 0x00; return body },
		"d2-trunc":  func(body []byte) []byte { return body[:3] },
	}
}

func dmgWaitSearch(t *testing.T, env *dmgEnv) string {
	deadline := time.Now().Add(90 * time.Second)
	var status nornicdb.DatabaseSearchStatus
	for time.Now().Before(deadline) {
		status = env.db.GetDatabaseSearchStatus(env.database)
		if status.Ready {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Sprintf("%+v", status)
}

func dmgSearch(env *dmgEnv, query string) (string, []string) {
	type answer struct {
		results []*nornicdb.SearchResult
		err     error
	}
	done := make(chan answer, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	go func() {
		results, err := env.db.Search(ctx, query, nil, 10)
		done <- answer{results, err}
	}()
	select {
	case a := <-done:
		var found []string
		encoded, _ := json.Marshal(a.results)
		for _, id := range []string{"d2-ok", "d2-ok2"} {
			if strings.Contains(string(encoded), id) {
				found = append(found, id)
			}
		}
		text := string(encoded)
		if len(text) > 400 {
			text = text[:400] + "..."
		}
		return fmt.Sprintf("results=%d err=%v json=%s", len(a.results), a.err, text), found
	case <-time.After(40 * time.Second):
		return "HANG", nil
	}
}

func TestV152DamagedBodyD2D3PointReadsAndErrors(t *testing.T) {
	t.Setenv("NORNICDB_REMOTE_CREDENTIALS_KEY", "test-remote-credential-key-32b!")
	for _, variant := range []string{"control-undamaged", "damaged"} {
		t.Run(variant, func(t *testing.T) {
			dir := dmgCreateStore(t, dmgD2Nodes(), true)
			if variant == "damaged" {
				dmgDamage(t, dir, dmgD2Damage())
			}
			env, err := dmgServe(t, dir)
			if err != nil {
				t.Fatalf("start: %v", err)
			}
			searchStatus := dmgWaitSearch(t, env)
			searchResult, found := dmgSearch(env, "zebrafind")
			t.Logf("D3 SEARCH %s status=%s | %s | healthy nodes found=%v", variant, searchStatus, searchResult, found)
			httpCode, httpBody := env.http.post("/nornicdb/search", map[string]any{"query": "zebrafind", "limit": 10})
			failedPhase := strings.Contains(httpBody, "\"phase\":\"failed\"")
			if len(httpBody) > 700 {
				httpBody = httpBody[:700] + "..."
			}
			t.Logf("D3 HTTPSEARCH %s POST /nornicdb/search: status=%d body=%s", variant, httpCode, httpBody)
			if variant == "damaged" && httpCode == http.StatusServiceUnavailable && failedPhase {
				t.Errorf("DEFECT3 HTTP search answers 503 'being built' (retryable) while the build has failed")
			}
			if variant == "control-undamaged" && len(found) != 2 {
				t.Errorf("CONTROL search did not find both healthy nodes: %s", searchResult)
			}
			if variant == "damaged" && len(found) != 2 {
				t.Errorf("DEFECT3 search: healthy nodes not found after one damaged body: found=%v %s", found, searchResult)
			}
			for _, n := range dmgD2Nodes() {
				params := map[string]any{"id": n.id, "k": n.props["k"]}
				damaged := variant == "damaged" && dmgD2Damage()[n.id] != nil
				for _, statement := range []string{
					"MATCH (n) WHERE id(n) = $id RETURN id(n) AS i",
					"MATCH (n) WHERE id(n) = $id RETURN n.s AS s",
					"MATCH (n:Fz) WHERE id(n) = $id RETURN count(n) AS c",
					"MATCH (n:Fz {k: $k}) RETURN n.s AS s",
				} {
					h := env.http.run(statement, params)
					b := env.bolt.run(statement, params)
					t.Logf("D2 %s %s damaged=%v | %s | http %s | bolt %s", variant, n.id, damaged, statement, h, b)
					if !damaged {
						if len(h.Errors) > 0 || len(h.Rows) != 1 {
							t.Errorf("CONTROL %s %q: %s", n.id, statement, h)
						}
						continue
					}
					if strings.Contains(statement, "RETURN id(n)") || strings.Contains(statement, "RETURN n.s") {
						if len(h.Errors) == 0 && len(h.Rows) == 0 {
							t.Errorf("DEFECT2 http %s %q: no row and no error", n.id, statement)
						}
						if len(b.Errors) == 0 && len(b.Rows) == 0 {
							t.Errorf("DEFECT2 bolt %s %q: no row and no error", n.id, statement)
						}
					}
					if strings.Contains(statement, "count(n)") && len(h.Errors) == 0 && len(h.Rows) == 1 {
						if c, ok := h.Rows[0][0].(float64); ok && c == 0 {
							t.Errorf("DEFECT2 http %s %q: count 0 and no error", n.id, statement)
						}
					}
				}
			}
			for _, statement := range []string{
				"MATCH (n:Fz) RETURN n.s AS s",
				"MATCH (n:Fz) RETURN count(n) AS c",
				"MATCH (n) RETURN count(n) AS c",
			} {
				h := env.http.run(statement, nil)
				b := env.bolt.run(statement, nil)
				t.Logf("D3 SCAN %s | %s | http %s | bolt %s", variant, statement, h, b)
				if variant == "damaged" {
					for _, code := range append(append([]string(nil), h.Codes...), b.Codes...) {
						if code == dmgSyntax {
							t.Errorf("DEFECT3 storage damage reported as %s for %q: http %v bolt %v", code, statement, h.Errors, b.Errors)
						}
					}
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Defect 4: server start with a damaged body before the legacy-data check.

func TestV152DamagedBodyD4StartupMigrationScan(t *testing.T) {
	t.Setenv("NORNICDB_REMOTE_CREDENTIALS_KEY", "test-remote-credential-key-32b!")
	nodes := []dmgNode{
		{id: "d4-ok", labels: []string{"Fz"}, props: map[string]any{"k": int64(1), "s": "fine"}},
		{id: "d4-bad", labels: []string{"Fz"}, props: map[string]any{"k": int64(2), "s": "bad"}},
	}
	damages := map[string]func([]byte) []byte{
		"empty":  func([]byte) []byte { return []byte{} },
		"format": func(body []byte) []byte { body[0] = 0x00; return body },
		"trunc":  func(body []byte) []byte { return body[:3] },
	}
	for _, variant := range []string{"control-undamaged", "empty", "format", "trunc", "empty-marker-written", "format-first-start-done",
		"control-undamaged-first-start-done-unclean", "format-first-start-done-unclean"} {
		t.Run(variant, func(t *testing.T) {
			firstStart := strings.Contains(variant, "first-start-done")
			dir := dmgCreateStore(t, nodes, firstStart)
			kind := strings.Split(variant, "-")[0]
			if damage := damages[kind]; damage != nil {
				dmgDamage(t, dir, map[string]func([]byte) []byte{"d4-bad": damage})
			}
			if strings.HasSuffix(variant, "-unclean") {
				// The state an unclean stop (crash, kill) leaves: no clean-shutdown
				// marker. Nothing else is changed.
				dmgDropCleanShutdownMarker(t, dir)
			}
			if strings.HasSuffix(variant, "marker-written") {
				// Test-side bypass: record the one-time legacy-data check as done
				// (the record a completed first start writes), nothing else.
				db, err := nornicdb.Open(dir, dmgConfig())
				if err != nil {
					t.Fatal(err)
				}
				system := storage.NewNamespacedEngine(db.GetBaseStorageForManager(), "system")
				if _, err := system.CreateNode(context.Background(), &storage.Node{ID: "migration:legacy_data", Labels: []string{"_System", "_Migration"},
					Properties: map[string]any{"status": "complete", "migrated_to": "nornic"}}); err != nil {
					t.Fatalf("write marker: %v", err)
				}
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
			}
			env, err := dmgServe(t, dir)
			t.Logf("D4 START %s: err=%v", variant, err)
			if err != nil {
				if strings.HasPrefix(variant, "control") {
					t.Fatalf("CONTROL start failed: %v", err)
				}
				t.Errorf("DEFECT4 %s: server does not start: %v", variant, err)
				return
			}
			h := env.http.run("MATCH (n) WHERE id(n) = $id RETURN n.s AS s", map[string]any{"id": "d4-ok"})
			t.Logf("D4 READ %s d4-ok: %s", variant, h)
			if len(h.Errors) > 0 || len(h.Rows) != 1 {
				t.Errorf("after start %s: healthy node not readable: %s", variant, h)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Defect 1, normal use: temporal values outside years 0..9999 written by an
// ordinary client (Bolt parameters, Cypher functions) or the library API.

func TestV152DamagedBodyD1OutOfRangeTemporalWrites(t *testing.T) {
	t.Setenv("NORNICDB_REMOTE_CREDENTIALS_KEY", "test-remote-credential-key-32b!")
	dir := dmgCreateStore(t, nil, true)
	env, err := dmgServe(t, dir)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	y12000 := time.Date(12000, 1, 1, 0, 0, 0, 0, time.UTC)
	yNeg := time.Date(-50, 1, 1, 0, 0, 0, 0, time.UTC)
	writes := []struct {
		id        string
		statement string
		params    map[string]any
		via       string
	}{
		{"boltdt12000", "CREATE (n:N1 {id: 'boltdt12000', d: $d}) RETURN n.id AS id", map[string]any{"d": y12000}, "bolt"},
		{"boltldt12000", "CREATE (n:N1 {id: 'boltldt12000', d: $d}) RETURN n.id AS id", map[string]any{"d": dbtype.LocalDateTime(y12000)}, "bolt"},
		{"boltdate12000", "CREATE (n:N1 {id: 'boltdate12000', d: $d}) RETURN n.id AS id", map[string]any{"d": dbtype.Date(y12000)}, "bolt"},
		{"boltdtneg", "CREATE (n:N1 {id: 'boltdtneg', d: $d}) RETURN n.id AS id", map[string]any{"d": yNeg}, "bolt"},
		{"cyldt12000", "CREATE (n:N1 {id: 'cyldt12000', d: localdatetime({year: 12000, month: 1, day: 1})}) RETURN n.id AS id", nil, "http"},
		{"cydate12000", "CREATE (n:N1 {id: 'cydate12000', d: date({year: 12000, month: 1, day: 1})}) RETURN n.id AS id", nil, "http"},
		{"cydtstr", "CREATE (n:N1 {id: 'cydtstr', d: datetime('+12000-01-01T00:00Z')}) RETURN n.id AS id", nil, "http"},
		{"libdt12000", "", nil, "library"},
		{"control", "CREATE (n:N1 {id: 'control', d: datetime('2020-01-01T00:00Z')}) RETURN n.id AS id", nil, "http"},
	}
	for _, w := range writes {
		switch w.via {
		case "bolt":
			t.Logf("D1T CREATE %s via bolt: %s", w.id, env.bolt.run(w.statement, w.params))
		case "http":
			t.Logf("D1T CREATE %s via http: %s", w.id, env.http.run(w.statement, w.params))
		case "library":
			_, err := env.db.GetStorage().CreateNode(context.Background(), &storage.Node{ID: storage.NodeID(w.id), Labels: []string{"N1"},
				Properties: map[string]any{"id": w.id, "d": y12000}})
			t.Logf("D1T CREATE %s via library API: err=%v", w.id, err)
		}
	}
	for _, w := range writes {
		params := map[string]any{"id": w.id}
		for _, statement := range []string{
			"MATCH (n:N1 {id: $id}) RETURN n AS n",
			"MATCH (n:N1 {id: $id}) RETURN n.d AS d",
		} {
			h := env.http.run(statement, params)
			b := env.bolt.run(statement, params)
			t.Logf("D1T READ %s | %s | http %s | bolt %s", w.id, statement, h, b)
			if w.id == "control" {
				if h.Status != 200 || len(h.Rows) != 1 {
					t.Fatalf("control read failed: http %s", h)
				}
				continue
			}
			if h.Status == 200 && len(h.Body) == 0 {
				t.Errorf("DEFECT1 out-of-range temporal %s (written via %s): HTTP 200 with empty body for %q", w.id, w.via, statement)
			}
		}
		t.Logf("D1T EXEC %s: %s", w.id, dmgExecutorView(env.db, w.id))
	}
}
