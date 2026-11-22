package bolt

// Closeout lane L3.
//   P-N152-8 (NOR-182): the server side of the __proto__ map-key item: a map
//   with key __proto__ must reach a Bolt client intact (literal and
//   parameter). The JavaScript loss itself is checked in testing/closeoutl3.
//   P-N151-33 (NOR-135): behavioural facts behind the stale documentation:
//   the server accepts Bolt 5.0 and encodes time values as Bolt structures.

import (
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestCloseoutL3_N152_8_ServerReturnsProtoKey(t *testing.T) {
	engine := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	store := storage.NewNamespacedEngine(engine, "proto_keys")
	_, port := startBoltIntegrationServer(t, store, boltIntegrationExplicitTx)
	ctx := context.Background()
	driver, err := neo4j.NewDriverWithContext(fmt.Sprintf("bolt://127.0.0.1:%d", port), neo4j.NoAuth())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, driver.Close(ctx)) })

	run := func(t *testing.T, query string, params map[string]any) map[string]any {
		t.Helper()
		records, err := runListInputBoltStatement(ctx, driver, "proto_keys", false, query, params)
		require.NoError(t, err, query)
		require.Len(t, records, 1, query)
		value, ok := records[0].Values[0].(map[string]any)
		require.True(t, ok, "value %#v", records[0].Values[0])
		t.Logf("CLOSEOUT_L3_PROTO query=%q value=%#v", query, value)
		return value
	}
	check := func(t *testing.T, value map[string]any) {
		t.Helper()
		require.Equal(t, int64(13), value["ordinary"], "control key")
		got, present := value["__proto__"]
		require.True(t, present, "key __proto__ missing from %#v", value)
		require.Equal(t, int64(7), got)
		nested, ok := value["nested"].(map[string]any)
		require.True(t, ok, "nested %#v", value["nested"])
		require.Equal(t, int64(8), nested["__proto__"])
	}
	t.Run("literal", func(t *testing.T) {
		check(t, run(t, "RETURN {__proto__: 7, ordinary: 13, nested: {__proto__: 8}} AS value", nil))
	})
	t.Run("backticked_literal", func(t *testing.T) {
		check(t, run(t, "RETURN {`__proto__`: 7, ordinary: 13, nested: {`__proto__`: 8}} AS value", nil))
	})
	t.Run("parameter", func(t *testing.T) {
		param := map[string]any{"__proto__": int64(7), "ordinary": int64(13), "nested": map[string]any{"__proto__": int64(8)}}
		check(t, run(t, "RETURN $m AS value", map[string]any{"m": param}))
	})
}

// P-N152-11 (NOR-186 umbrella), finding F23 (NOR-190): with LogQueries on, a
// credential known from the statement (PASSWORD, or URI userinfo in AT) must
// not appear in the Bolt query log when repeated under an ordinary parameter
// key; the numeric parameter stays visible. Exact statement and parameters
// from the item.
func TestCloseoutL3_N152_11_F23BoltParameterDiagnostics(t *testing.T) {
	const credential = "synthetic-bolt-param-credential-781326"
	for _, entry := range []struct{ name, query string }{
		{"password", "CREATE COMPOSITE DATABASE bolt_display ALIAS r FOR DATABASE remote AT 'http://example.invalid' USER u PASSWORD '" + credential + "'"},
		{"uri_userinfo", "CREATE COMPOSITE DATABASE bolt_display ALIAS r FOR DATABASE remote AT 'http://u:" + credential + "@example.invalid' USER u PASSWORD 'other-synthetic-pw-55'"},
		{"control_no_credential", "RETURN $visible AS v"},
	} {
		t.Run(entry.name, func(t *testing.T) {
			params := map[string]any{"echo": credential, "visible": int64(17)}
			cfg := DefaultConfig()
			cfg.LogQueries, cfg.RequireAuth = true, false
			server, output := newCapturingServer(t, cfg)
			serverConn, clientConn := net.Pipe()
			done := make(chan struct{})
			go func() { _, _ = io.Copy(io.Discard, clientConn); close(done) }()
			t.Cleanup(func() { _ = serverConn.Close(); _ = clientConn.Close(); <-done })
			executor := &mockExecutor{executeFunc: func(ctx context.Context, got string, values map[string]any) (*QueryResult, error) {
				return &QueryResult{Columns: []string{"v"}, Rows: [][]any{{"ok"}}}, nil
			}}
			session := newTestSession(serverConn, executor)
			session.server = server
			data := append(encodePackStreamString(entry.query), encodePackStreamMap(params)...)
			if err := session.handleRun(data); err != nil {
				t.Fatalf("RUN failed: %v", err)
			}
			log := output.String()
			disclosed := strings.Contains(log, credential)
			t.Logf("CLOSEOUT_L3_F23 case=%s disclosed=%v visible17=%v log_bytes=%d", entry.name, disclosed, strings.Contains(log, "visible"), len(log))
			if !strings.Contains(log, "visible") {
				t.Errorf("ordinary numeric parameter not visible in the query log")
			}
			if entry.name == "control_no_credential" {
				// No credential in the statement: the echo value is ordinary.
				if !disclosed {
					t.Errorf("control: ordinary parameter value missing from the log")
				}
				return
			}
			if disclosed {
				t.Errorf("Bolt query log disclosed the statement's credential through parameter echo")
			}
		})
	}
}

func TestCloseoutL3_N151_33_ServerBehaviour(t *testing.T) {
	t.Run("accepts_bolt_5_0", func(t *testing.T) {
		// A driver offering exactly 5.0 (wire [0x00, back=0, minor=0, major=5]).
		version, reply, ok := selectBoltVersion([]uint32{0x00000005, 0, 0, 0})
		t.Logf("CLOSEOUT_L3_BOLT offer=5.0 selected=%#04x reply=%#08x ok=%v", version, reply, ok)
		require.True(t, ok)
		require.Equal(t, uint32(BoltV5_0), version)
	})
	t.Run("control_accepts_bolt_4_4", func(t *testing.T) {
		version, _, ok := selectBoltVersion([]uint32{0x00000404, 0, 0, 0})
		require.True(t, ok)
		require.Equal(t, uint32(BoltV4_4), version)
	})
	t.Run("time_is_a_bolt_structure", func(t *testing.T) {
		value := time.Date(2026, 11, 22, 5, 0, 0, 0, time.UTC)
		encoded := encodePackStreamList([]any{value})
		t.Logf("CLOSEOUT_L3_BOLT time encoding=% x", encoded)
		require.GreaterOrEqual(t, len(encoded), 3)
		require.Equal(t, byte(0x91), encoded[0], "one-element list header")
		// A tiny struct marker 0xB0..0xBF, not an integer marker.
		require.Equal(t, byte(0xB0), encoded[1]&0xF0, "time.Time must be a Bolt structure, got marker %#x", encoded[1])
		t.Logf("CLOSEOUT_L3_BOLT time signature=%#x", encoded[2])
	})
}
