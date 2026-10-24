package bolt

import (
	"bytes"
	"fmt"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"io"
	"net"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	legacydriver "github.com/neo4j/neo4j-go-driver/v4/neo4j"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func dateTimeReferenceAddress(t *testing.T) string {
	t.Helper()
	uri := os.Getenv("NORNICDB_NEO4J_REFERENCE_URI")
	if uri == "" {
		t.Skip("set NORNICDB_NEO4J_REFERENCE_URI to run the Neo4j 5.26 oracle")
	}
	parsed, err := url.Parse(uri)
	require.NoError(t, err)
	return parsed.Host
}

func TestLegacyDateTimeWire(t *testing.T) {
	store := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	_, port := startBoltIntegrationServerWithExplicitTx(t, store)
	testDateTimeWire(t, fmt.Sprintf("127.0.0.1:%d", port), false)
}

func TestLegacyDateTimeWireNeo4j526(t *testing.T) {
	testDateTimeWire(t, dateTimeReferenceAddress(t), true)
}

func dateTimeWireConnect(t *testing.T, address string, version byte, patch, reference bool) net.Conn {
	t.Helper()
	conn, err := net.DialTimeout("tcp", address, 5*time.Second)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	require.NoError(t, conn.SetDeadline(time.Now().Add(time.Minute)))
	handshake := make([]byte, 20)
	copy(handshake, []byte{0x60, 0x60, 0xB0, 0x17})
	handshake[7] = version
	if version == 4 {
		handshake[6] = 4
	}
	_, err = conn.Write(handshake)
	require.NoError(t, err)
	selected := make([]byte, 4)
	_, err = io.ReadFull(conn, selected)
	require.NoError(t, err)
	require.Equal(t, handshake[4:8], selected, "must negotiate the exact requested protocol")
	hello := map[string]any{"user_agent": "nornicdb-datetime-regression", "scheme": "none"}
	if patch {
		hello["patch_bolt"] = []any{"utc"}
	}
	require.NoError(t, SendMessage(conn, buildHelloMessageWithPatchBolt(hello)))
	metadata, err := AssertSuccess(t, conn)
	require.NoError(t, err)
	if reference {
		require.True(t, strings.HasPrefix(fmt.Sprint(metadata["server"]), "Neo4j/5.26."))
		t.Logf("reference=%s protocol=%x utc_requested=%t", metadata["server"], selected, patch)
	}
	return conn
}

func testDateTimeWire(t *testing.T, address string, reference bool) {
	t.Helper()
	for _, protocol := range []struct {
		name    string
		version byte
		patch   bool
	}{{"4.4 legacy", 4, false}, {"4.4 utc", 4, true}, {"5.0", 5, false}} {
		t.Run(protocol.name, func(t *testing.T) {
			conn := dateTimeWireConnect(t, address, protocol.version, protocol.patch, reference)
			defer conn.Close()
			utc := protocol.patch || protocol.version == 5
			for i, value := range legacyDateTimeCases(t) {
				literal := value.Time.Format(time.RFC3339Nano)
				if value.ZoneID != "" {
					literal += "[" + value.ZoneID + "]"
				}
				for _, shape := range []string{"scalar", "list", "map"} {
					t.Run(fmt.Sprintf("%d/%s/output", i, shape), func(t *testing.T) {
						expression := "datetime('" + literal + "')"
						want := dateTimeWire(value, utc)
						if shape == "list" {
							expression = "[" + expression + "]"
							want = append([]byte{0x91}, want...)
						}
						if shape == "map" {
							expression = "{v: " + expression + "}"
							want = append([]byte{0xA1, 0x81, 'v'}, want...)
						}
						got := dateTimeWireQuery(t, conn, "RETURN "+expression, []byte{0xA0})
						require.Equal(t, append([]byte{0x91}, want...), got, "raw RECORD, independently constructed seconds")
					})
					t.Run(fmt.Sprintf("%d/%s/input", i, shape), func(t *testing.T) {
						wire := dateTimeWire(value, utc)
						access := "$v"
						if shape == "list" {
							access += "[0]"
							wire = append([]byte{0x91}, wire...)
						}
						if shape == "map" {
							access += ".t"
							wire = append([]byte{0xA1, 0x81, 't'}, wire...)
						}
						params := append([]byte{0xA1, 0x81, 'v'}, wire...)
						// Full text preserves the offset/zone and negative fractional
						// seconds; epochSeconds is a separate Cypher property contract.
						got := dateTimeWireQuery(t, conn, "RETURN toString("+access+"), "+access+".nanosecond", params)
						want := encodePackStreamValue([]any{literal, int64(value.Time.Nanosecond())})
						require.Equal(t, want, got, "parameter must represent the original instant")
					})
				}
			}
		})
	}
}

func dateTimeWireQuery(t *testing.T, conn net.Conn, query string, params []byte) []byte {
	t.Helper()
	message := encodePackStreamStringInto([]byte{0xB3, MsgRun}, query)
	message = append(message, params...)
	require.NoError(t, SendMessage(conn, append(message, 0xA0)))
	require.NoError(t, ReadSuccess(t, conn))
	require.NoError(t, SendPull(t, conn, map[string]any{"n": int64(-1)}))
	signature, record, err := ReadMessage(conn)
	require.NoError(t, err)
	require.Equal(t, byte(MsgRecord), signature)
	require.NoError(t, ReadSuccess(t, conn))
	return record
}

// v4.4.0 is deliberately pinned: it implements Bolt 4.4 before the utc patch
// was added. No driver code or HELLO message is modified for these tests.
func TestLegacyDateTimeRealDriver(t *testing.T) {
	store := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	_, port := startBoltIntegrationServerWithExplicitTx(t, store)
	testLegacyDateTimeDriver(t, fmt.Sprintf("bolt://127.0.0.1:%d", port), false)
}

func TestLegacyDateTimeRealDriverNeo4j526(t *testing.T) {
	testLegacyDateTimeDriver(t, "bolt://"+dateTimeReferenceAddress(t), true)
}

type dateTimeDriverLog struct {
	mu                     sync.Mutex
	hello, patched, bolt44 bool
}

func (l *dateTimeDriverLog) LogClientMessage(_ string, format string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if strings.HasPrefix(format, "HELLO") {
		l.hello = true
		l.patched = strings.Contains(fmt.Sprintf(format, args...), "patch_bolt")
	}
}

func (l *dateTimeDriverLog) LogServerMessage(_ string, format string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if strings.HasPrefix(format, "<HANDSHAKE>") {
		l.bolt44 = bytes.Equal(args[0].([]byte), []byte{0, 0, 4, 4})
	}
}

func testLegacyDateTimeDriver(t *testing.T, uri string, reference bool) {
	t.Helper()
	log := &dateTimeDriverLog{}
	driver, err := legacydriver.NewDriver(uri, legacydriver.NoAuth())
	require.NoError(t, err)
	defer driver.Close()
	session := driver.NewSession(legacydriver.SessionConfig{BoltLogger: log})
	defer session.Close()
	for i, value := range legacyDateTimeCases(t) {
		for _, explicit := range []bool{false, true} {
			t.Run(fmt.Sprintf("%d/tx=%t", i, explicit), func(t *testing.T) {
				run := func(query string, params map[string]interface{}) (legacydriver.Result, error) {
					return session.Run(query, params)
				}
				if explicit {
					tx, err := session.BeginTransaction()
					require.NoError(t, err)
					defer tx.Close()
					run = tx.Run
				}
				literal := value.Time.Format(time.RFC3339Nano)
				if value.ZoneID != "" {
					literal += "[" + value.ZoneID + "]"
				}
				t.Run("output", func(t *testing.T) {
					result, err := run("RETURN datetime('"+literal+"'), [datetime('"+literal+"')], {v: datetime('"+literal+"')}", nil)
					require.NoError(t, err)
					record, err := result.Single()
					require.NoError(t, err)
					for _, got := range []any{record.Values[0], record.Values[1].([]interface{})[0], record.Values[2].(map[string]interface{})["v"]} {
						actual, ok := got.(time.Time)
						require.True(t, ok, "got %T", got)
						require.Equal(t, value.Time.Unix(), actual.Unix())
						require.Equal(t, value.Time.Nanosecond(), actual.Nanosecond())
						require.Equal(t, value.Time.Format(time.RFC3339Nano), actual.Format(time.RFC3339Nano))
						if value.ZoneID != "" {
							require.Equal(t, value.ZoneID, actual.Location().String())
						}
					}
				})
				t.Run("input", func(t *testing.T) {
					result, err := run("RETURN toString($v), $v.nanosecond, toString($vs[0]), toString($m.v)",
						map[string]interface{}{"v": value.Time, "vs": []interface{}{value.Time}, "m": map[string]interface{}{"v": value.Time}})
					require.NoError(t, err)
					record, err := result.Single()
					require.NoError(t, err)
					require.Equal(t, []interface{}{literal, int64(value.Time.Nanosecond()), literal, literal}, record.Values)
				})
			})
		}
	}
	log.mu.Lock()
	defer log.mu.Unlock()
	require.True(t, log.hello, "must observe the real driver's HELLO")
	require.False(t, log.patched, "driver must not request patch_bolt")
	require.True(t, log.bolt44, "driver must negotiate Bolt 4.4")
	t.Logf("real driver v4.4.0: Bolt 4.4, HELLO without patch_bolt; reference=%t", reference)
}
