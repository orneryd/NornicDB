package server

// Batch-3b probe (research only, untracked; same text on both builds): the
// row-72 server condition of TestZZ3BSlowDelete60k, with Badger's
// conflict-check state sampled every second during each delete:
// len(oracle.committedTxns) and oracle.discardTs of every *storage.BadgerEngine
// reachable from the server (found by a reflective walk; unexported fields
// read without the oracle's lock: an observation only).

import (
	"context"
	"fmt"
	"net"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/bolt"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func zz3bSField(v reflect.Value, name string) reflect.Value {
	f := v.FieldByName(name)
	return reflect.NewAt(f.Type(), unsafe.Pointer(f.UnsafeAddr())).Elem()
}

func zz3bSOracle(engine *storage.BadgerEngine) (int, uint64) {
	managed := zz3bSField(reflect.ValueOf(engine).Elem(), "db")
	if managed.IsNil() {
		return -1, 0
	}
	bdb := zz3bSField(managed.Elem(), "DB")
	orc := zz3bSField(bdb.Elem(), "orc")
	return zz3bSField(orc.Elem(), "committedTxns").Len(), zz3bSField(orc.Elem(), "discardTs").Uint()
}

// zz3bFindEngines walks pointers, interfaces, structs, slices and maps from
// root and returns every distinct *storage.BadgerEngine it reaches.
func zz3bFindEngines(root any) []*storage.BadgerEngine {
	target := reflect.TypeOf((*storage.BadgerEngine)(nil))
	seen := map[uintptr]bool{}
	var found []*storage.BadgerEngine
	var walk func(v reflect.Value, depth int)
	walk = func(v reflect.Value, depth int) {
		if depth > 14 || !v.IsValid() {
			return
		}
		switch v.Kind() {
		case reflect.Pointer:
			if v.IsNil() {
				return
			}
			p := v.Pointer()
			if seen[p] {
				return
			}
			seen[p] = true
			if v.Type() == target {
				found = append(found, (*storage.BadgerEngine)(unsafe.Pointer(p)))
				return
			}
			walk(v.Elem(), depth+1)
		case reflect.Interface:
			if !v.IsNil() {
				walk(v.Elem(), depth+1)
			}
		case reflect.Struct:
			if !v.CanAddr() {
				c := reflect.New(v.Type()).Elem()
				c.Set(v)
				v = c
			}
			for i := 0; i < v.NumField(); i++ {
				f := v.Field(i)
				walk(reflect.NewAt(f.Type(), unsafe.Pointer(f.UnsafeAddr())).Elem(), depth+1)
			}
		case reflect.Slice, reflect.Array:
			for i := 0; i < v.Len() && i < 64; i++ {
				walk(v.Index(i), depth+1)
			}
		case reflect.Map:
			n := 0
			for it := v.MapRange(); it.Next() && n < 64; n++ {
				walk(it.Value(), depth+1)
			}
		}
	}
	walk(reflect.ValueOf(root), 0)
	return found
}

func TestZZ3BServerDeleteOracle(t *testing.T) {
	const nodes, runs = 60000, 10
	const q = "MATCH (n:ZZDel) DELETE n RETURN count(*) AS deleted"
	t.Setenv("NORNICDB_HTTP_TX_TIMEOUT", "10m")
	// The row-72 server, built as zz3bStart builds it, keeping srv so that the
	// Badger engines can be found from it.
	srv, authenticator := setupTestServer(t)
	peer := httptest.NewServer(srv.buildRouter())
	t.Cleanup(peer.Close)
	token := getAuthToken(t, authenticator, "admin")
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	bs := bolt.NewWithDatabaseManager(bolt.DefaultConfig(), nil, srv.GetDatabaseManager())
	done := make(chan error, 1)
	go func() { done <- bs.Serve(listener) }()
	t.Cleanup(func() { _ = bs.Close(); <-done })
	driver, err := neo4j.NewDriverWithContext("bolt://"+listener.Addr().String(), neo4j.NoAuth())
	require.NoError(t, err)
	t.Cleanup(func() { _ = driver.Close(context.Background()) })
	env := &zz3bEnv{t: t, peer: peer, token: token, driver: driver}
	engines := zz3bFindEngines(srv)
	t.Logf("ZZ3B SORACLE engines reachable: %d", len(engines))
	state := func() string {
		var parts []string
		for i, e := range engines {
			c, d := zz3bSOracle(e)
			parts = append(parts, fmt.Sprintf("e%d=%d/%d", i, c, d))
		}
		return strings.Join(parts, ",")
	}
	for run := 1; run <= runs; run++ {
		before := state()
		out := env.run("Bolt-auto", "UNWIND range(1, $n) AS i CREATE (:ZZDel {i: i})", map[string]any{"n": nodes})
		require.Empty(t, out.Err)
		afterSeed := state()
		stop := make(chan struct{})
		samples := make(chan string, 1)
		go func() {
			var s []string
			tick := time.NewTicker(time.Second)
			defer tick.Stop()
			start := time.Now()
			for {
				select {
				case <-stop:
					samples <- strings.Join(s, " ")
					return
				case <-tick.C:
					s = append(s, fmt.Sprintf("%ds:%s", int(time.Since(start).Round(time.Second)/time.Second), state()))
				}
			}
		}()
		cpu0 := zz3bCPU()
		start := time.Now()
		status, resp, err := env.post(context.Background(), "/db/nornic/tx/commit", q, nil)
		wall := time.Since(start)
		cpu := zz3bCPU() - cpu0
		close(stop)
		left := env.run("Bolt-auto", "MATCH (n:ZZDel) RETURN count(n) AS c", nil).String()
		t.Logf("ZZ3B SORACLE run=%d delete wall=%v CPU=%v (%.2f cores) status=%d response=%s left=%s; committedTxns/discardTs per engine: before seed %s, before delete %s, after delete %s; during delete %s",
			run, wall.Round(time.Millisecond), cpu.Round(time.Millisecond), float64(cpu)/float64(wall), status, zz3bHTTPOutcome(status, resp, err), left,
			before, afterSeed, state(), <-samples)
	}
}
