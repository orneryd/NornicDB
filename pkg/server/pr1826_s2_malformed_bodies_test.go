package server

// PR #1826 review, lane S2: malformed stored node bodies through the server
// path (HTTP and Bolt). Test-only; identical on the PR head and its base.
//
// Phase 1 writes nodes of several shapes and closes the database. Phase 2
// opens the Badger directory raw (the database is closed) and overwrites each
// node body with one mutation of its own valid bytes (truncation, byte
// changes, appended garbage, inserted oversized length). Phase 3 reopens the
// database with a server and reads every node projected and whole. Nothing
// may panic or hang; projected and whole reads are compared.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
)

type s2Mutation struct {
	kind string
	pos  int
}

func (m s2Mutation) apply(body []byte) []byte {
	out := append([]byte(nil), body...)
	pos := m.pos
	if pos >= len(out) {
		pos = len(out) - 1
	}
	if pos < 0 {
		pos = 0
	}
	switch m.kind {
	case "truncate":
		if m.pos < len(out) {
			return out[:m.pos]
		}
		return out
	case "xor":
		if len(out) > 0 {
			out[pos] ^= 0xFF
		}
	case "zero":
		if len(out) > 0 {
			out[pos] = 0x00
		}
	case "ff":
		if len(out) > 0 {
			out[pos] = 0xFF
		}
	case "7f":
		if len(out) > 0 {
			out[pos] = 0x7F
		}
	case "inc":
		if len(out) > 0 {
			out[pos]++
		}
	case "append":
		out = append(out, bytes.Repeat([]byte{0xFF}, 9)...)
	case "varint":
		huge := append(bytes.Repeat([]byte{0xFF}, 9), 0x01)
		out = append(out[:pos], append(huge, out[pos:]...)...)
	}
	return out
}

func s2Mutations(length int) []s2Mutation {
	seen := map[s2Mutation]bool{}
	var list []s2Mutation
	add := func(m s2Mutation) {
		if m.pos < 0 || (m.kind != "append" && m.pos >= length) || seen[m] {
			return
		}
		seen[m] = true
		list = append(list, m)
	}
	for _, n := range []int{0, 1, 2, 3, 4, 5, 6, 8, 12, 16, length / 4, length / 2, 3 * length / 4, length - 1, length - 2, length - 3, length - 5} {
		add(s2Mutation{kind: "truncate", pos: n})
	}
	head := length
	if head > 48 {
		head = 48
	}
	for pos := 0; pos < head; pos++ {
		for _, kind := range []string{"xor", "zero", "ff", "7f", "inc"} {
			add(s2Mutation{kind: kind, pos: pos})
		}
	}
	if length > 48 {
		for k := 1; k < 25; k++ {
			pos := 48 + (length-48)*k/25
			for _, kind := range []string{"xor", "zero", "ff"} {
				add(s2Mutation{kind: kind, pos: pos})
			}
		}
	}
	add(s2Mutation{kind: "append", pos: 0})
	for _, pos := range []int{0, 1, 2, length / 2} {
		add(s2Mutation{kind: "varint", pos: pos})
	}
	return list
}

func s2Shapes() map[string]func(i int) map[string]any {
	return map[string]func(i int) map[string]any{
		"a": func(i int) map[string]any {
			return map[string]any{"k": int64(i), "s": "short-a", "long": strings.Repeat("x", 5000), "f": 3.25, "b": true, "e": ""}
		},
		"b": func(i int) map[string]any {
			return map[string]any{"k": int64(i), "s": "bee", "bytes": []byte{0, 1, 2, 255}, "list": []any{"a", int64(1), 2.5}, "m": map[string]any{"x": "y"}}
		},
		"c": func(i int) map[string]any {
			return map[string]any{"k": int64(i), "s": "sea", "t": time.Date(2020, 1, 2, 3, 4, 5, 6, time.UTC), "vec": []float32{0.5, 1.5, 2.5}, "nested": []any{[]any{"x"}}}
		},
		"d": func(i int) map[string]any {
			props := map[string]any{"k": int64(i), "s": "dee"}
			for p := 0; p < 100; p++ {
				props[fmt.Sprintf("p%03d", p)] = fmt.Sprintf("v%03d", p)
			}
			return props
		},
	}
}

const s2NodesPerShape = 360

func s2MalformedConfig() *nornicdb.Config {
	config := nornicdb.DefaultConfig()
	config.Memory.DecayEnabled = false
	config.Memory.AutoLinksEnabled = false
	config.Database.AsyncWritesEnabled = false
	return config
}

func s2FindBadgerEngine(engine storage.Engine) *storage.BadgerEngine {
	for depth := 0; engine != nil && depth < 16; depth++ {
		if badgerEngine, ok := engine.(*storage.BadgerEngine); ok {
			return badgerEngine
		}
		unwrapper, ok := engine.(storage.EngineUnwrapper)
		if !ok {
			return nil
		}
		engine = unwrapper.GetInnerEngine()
	}
	return nil
}

// s2NodeKeySuffix matches a stored node key (prefix byte 0x01, then the
// namespaced ID) to its unprefixed test ID.
func s2NodeKeyID(key []byte) (string, bool) {
	if len(key) < 2 || key[0] != 0x01 {
		return "", false
	}
	rest := string(key[1:])
	if index := strings.LastIndex(rest, ":"); index >= 0 {
		rest = rest[index+1:]
	}
	return rest, strings.HasPrefix(rest, "fz-")
}

func TestPR1826S2MalformedBodiesThroughServer(t *testing.T) {
	dir, err := os.MkdirTemp("", "pr1826-s2-malformed-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	t.Setenv("NORNICDB_REMOTE_CREDENTIALS_KEY", "test-remote-credential-key-32b!")

	// Phase 1: create nodes.
	db, err := nornicdb.Open(dir, s2MalformedConfig())
	if err != nil {
		t.Fatal(err)
	}
	store := db.GetStorage()
	shapes := s2Shapes()
	shapeNames := make([]string, 0, len(shapes))
	for name := range shapes {
		shapeNames = append(shapeNames, name)
	}
	sort.Strings(shapeNames)
	for _, shape := range shapeNames {
		for i := 0; i < s2NodesPerShape; i++ {
			id := storage.NodeID(fmt.Sprintf("fz-%s-%d", shape, i))
			if _, err := store.CreateNode(t.Context(), &storage.Node{ID: id, Labels: []string{"Fz"}, Properties: shapes[shape](i)}); err != nil {
				t.Fatalf("create %s: %v", id, err)
			}
		}
	}
	// Start the server once on the valid data so the database manager's
	// one-time legacy-data detection completes and is recorded; otherwise the
	// reopen below stops at that whole-store scan instead of serving reads.
	{
		firstAuth, err := auth.NewAuthenticator(auth.AuthConfig{SecurityEnabled: true, JWTSecret: []byte("test-secret-key-for-testing-only-32b")},
			storage.NewNamespacedEngine(db.GetBaseStorageForManager(), "system"))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := firstAuth.CreateUser(context.Background(), "admin", s2Password, []auth.Role{auth.RoleAdmin}); err != nil {
			t.Fatal(err)
		}
		firstConfig := DefaultConfig()
		firstConfig.Port = 0
		firstConfig.EmbeddingEnabled = false
		first, err := New(db, firstAuth, firstConfig)
		if err != nil {
			t.Fatalf("first server: %v", err)
		}
		stopTestServer(t, first)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Phase 2: raw mutation while the database is closed.
	var manifests []string
	_ = filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err == nil && info != nil && !info.IsDir() && info.Name() == "MANIFEST" {
			manifests = append(manifests, filepath.Dir(path))
		}
		return nil
	})
	mutated := map[string][]byte{}
	applied := map[string]s2Mutation{}
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
			if id, ok := s2NodeKeyID(item.Key()); ok {
				value, err := item.ValueCopy(nil)
				if err == nil {
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
		for _, shape := range shapeNames {
			reference := originals[fmt.Sprintf("fz-%s-0", shape)]
			recipes := s2Mutations(len(reference))
			t.Logf("shape %s: body %d bytes, %d mutations", shape, len(reference), len(recipes))
			for i, recipe := range recipes {
				if i >= s2NodesPerShape {
					break
				}
				id := fmt.Sprintf("fz-%s-%d", shape, i)
				key, ok := keys[id]
				if !ok {
					continue
				}
				body := recipe.apply(originals[id])
				if err := writeTxn.Set(key, body); err != nil {
					writeTxn.Discard()
					writeTxn = raw.NewTransactionAt(raw.MaxVersion(), true)
					t.Logf("NOTE set %s: %v", id, err)
					continue
				}
				mutated[id] = body
				applied[id] = recipe
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
	if len(mutated) == 0 {
		t.Fatalf("no node body was mutated (manifests %v)", manifests)
	}

	// Phase 3: reopen with a server.
	type opened struct {
		db  *nornicdb.DB
		err error
	}
	openDone := make(chan opened, 1)
	go func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				openDone <- opened{err: fmt.Errorf("PANIC during open: %v", recovered)}
			}
		}()
		reopened, err := nornicdb.Open(dir, s2MalformedConfig())
		openDone <- opened{db: reopened, err: err}
	}()
	var reopened *nornicdb.DB
	select {
	case result := <-openDone:
		if result.err != nil {
			t.Fatalf("CRASH-OR-REFUSAL reopening a database with malformed node bodies: %v", result.err)
		}
		reopened = result.db
	case <-time.After(5 * time.Minute):
		t.Fatalf("HANG reopening a database with malformed node bodies")
	}
	t.Cleanup(func() { _ = reopened.Close() })
	persisted := 0
	if engine := s2FindBadgerEngine(reopened.GetBaseStorageForManager()); engine != nil && engine.DB() != nil {
		txn := engine.DB().NewTransactionAt(math.MaxUint64, false)
		iterator := txn.NewIterator(badger.DefaultIteratorOptions)
		for iterator.Seek([]byte{0x01}); iterator.ValidForPrefix([]byte{0x01}); iterator.Next() {
			if id, ok := s2NodeKeyID(iterator.Item().Key()); ok {
				if value, err := iterator.Item().ValueCopy(nil); err == nil && bytes.Equal(value, mutated[id]) {
					persisted++
				}
			}
		}
		iterator.Close()
		txn.Discard()
	}
	t.Logf("malformed bodies written %d, still stored after reopen %d", len(mutated), persisted)

	authenticator, err := auth.NewAuthenticator(auth.AuthConfig{SecurityEnabled: true, JWTSecret: []byte("test-secret-key-for-testing-only-32b")},
		storage.NewNamespacedEngine(reopened.GetBaseStorageForManager(), "system"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := authenticator.CreateUser(context.Background(), "admin", s2Password, []auth.Role{auth.RoleAdmin}); err != nil {
		t.Logf("NOTE admin user on reopen: %v", err)
	}
	config := DefaultConfig()
	config.Port = 0
	config.EmbeddingEnabled = false
	server, err := New(reopened, authenticator, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { stopTestServer(t, server) })
	h := &s2HTTP{t: t, router: server.buildRouter()}
	token := getAuthToken(t, authenticator, "admin")
	database := server.dbManager.DefaultDatabaseName()
	boltDriver := s2NewBolt(t, s2StartBolt(t, server, authenticator), "admin")

	categories := map[string]int{}
	examples := map[string][]string{}
	note := func(category, example string) {
		categories[category]++
		if len(examples[category]) < 12 {
			examples[category] = append(examples[category], example)
		}
	}
	crashOrHang := func(where, id string, o s2Outcome) bool {
		if o.Hang {
			t.Errorf("HANG [%s] node %s (%v): %v", where, id, applied[id], o.Errors)
			return true
		}
		if o.Crash {
			t.Errorf("CRASH [%s] node %s (%v): %.600s", where, id, applied[id], o.Raw)
			return true
		}
		return false
	}
	ids := make([]string, 0, len(mutated))
	for id := range mutated {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		params := map[string]any{"id": id}
		full := h.autocommit(database, token, "MATCH (n) WHERE id(n) = $id RETURN properties(n) AS p", params)
		projected := h.autocommit(database, token, "MATCH (n) WHERE id(n) = $id RETURN n.s AS s", params)
		wider := h.autocommit(database, token, "MATCH (n) WHERE id(n) = $id RETURN n.k AS k, n.long AS l, n.list AS li, n.t AS t, n.p050 AS p", params)
		whole := h.autocommit(database, token, "MATCH (n) WHERE id(n) = $id RETURN n AS n", params)
		boltProjected := boltDriver.autocommit(database, "MATCH (n) WHERE id(n) = $id RETURN n.s AS s", params)
		boltWhole := boltDriver.autocommit(database, "MATCH (n) WHERE id(n) = $id RETURN n AS n", params)
		bad := false
		for where, o := range map[string]s2Outcome{"http-full": full, "http-projected": projected, "http-wider": wider, "http-whole": whole, "bolt-projected": boltProjected, "bolt-whole": boltWhole} {
			if crashOrHang(where, id, o) {
				bad = true
			}
		}
		if bad {
			continue
		}
		diagnose := func(category string) {
			if categories[category] > 4 {
				return
			}
			for _, statement := range []string{
				"MATCH (n) WHERE id(n) = $id RETURN properties(n) AS p",
				"MATCH (n) WHERE id(n) = $id RETURN n AS n",
				"MATCH (n) WHERE id(n) = $id RETURN id(n) AS i",
				"MATCH (n) WHERE id(n) = $id RETURN n.t AS t, n.vec AS v, n.nested AS x",
				"MATCH (n:Fz) WHERE id(n) = $id RETURN count(n) AS c",
			} {
				code, body, _, _ := h.do("POST", "/db/"+database+"/tx/commit", token, map[string]any{
					"statements": []map[string]any{{"statement": statement, "parameters": params}},
				})
				b := boltDriver.autocommit(database, statement, params)
				t.Logf("DIAG %s %s %v | %s | http %d body(%d)=%.300q | bolt rows=%s errors=%.300v", category, id, applied[id], statement, code, len(body), body, b.rowsJSON(), b.Errors)
			}
		}
		fullOK := len(full.Errors) == 0 && len(full.Rows) == 1
		projOK := len(projected.Errors) == 0 && len(projected.Rows) == 1
		example := fmt.Sprintf("%s %v full=%v/%.200s projected=%v/%.200s", id, applied[id], full.Errors, full.rowsJSON(), projected.Errors, projected.rowsJSON())
		switch {
		case fullOK && projOK:
			fullProps, _ := full.Rows[0][0].(map[string]any)
			fullS, present := fullProps["s"]
			if !present {
				fullS = nil
			}
			fullJSON, _ := json.Marshal(fullS)
			projJSON, _ := json.Marshal(projected.Rows[0][0])
			if !bytes.Equal(fullJSON, projJSON) {
				note("both-read-disagree", example)
				t.Errorf("WRONG node %s (%v): projected n.s %s but whole read gives %s", id, applied[id], projJSON, fullJSON)
			} else {
				note("both-read-agree", example)
			}
		case fullOK && !projOK:
			diagnose("whole-read-ok-projected-fails")
			note("whole-read-ok-projected-fails", example)
		case !fullOK && projOK:
			diagnose("projected-ok-whole-fails")
			note("projected-ok-whole-fails", example)
		default:
			diagnose("both-fail")
			note("both-fail", example)
		}
		if len(boltProjected.Errors) == 0 && len(projected.Errors) == 0 && boltProjected.rowsJSON() != projected.rowsJSON() {
			note("bolt-http-projected-disagree", example+" bolt="+boltProjected.rowsJSON())
		}
	}
	for _, statement := range []string{
		"MATCH (n:Fz) RETURN n.s AS s",
		"MATCH (n:Fz) RETURN count(n.long) AS c",
		"MATCH (n:Fz) WHERE n.s = 'bee' RETURN count(*) AS c",
		"MATCH (n:Fz) RETURN n.k AS k ORDER BY k LIMIT 5",
		"MATCH (n:Fz) RETURN n AS n",
	} {
		o := h.autocommit(database, token, statement, nil)
		crashOrHang("http-scan "+statement, "*", o)
		b := boltDriver.autocommit(database, statement, nil)
		crashOrHang("bolt-scan "+statement, "*", b)
		t.Logf("scan %q: http rows=%d errors=%.300v | bolt rows=%d errors=%.300v", statement, len(o.Rows), o.Errors, len(b.Rows), b.Errors)
	}
	names := make([]string, 0, len(categories))
	for name := range categories {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		t.Logf("CATEGORY %s: %d", name, categories[name])
		for _, example := range examples[name] {
			t.Logf("  e.g. %s", example)
		}
	}
}
