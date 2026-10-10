package bolt

import (
	"encoding/json"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// connectionLimitRecords decodes the connection-limit warnings logged so far.
func connectionLimitRecords(t *testing.T, output string) []map[string]any {
	t.Helper()
	var records []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(output), "\n") {
		if line == "" {
			continue
		}
		var record map[string]any
		require.NoError(t, json.Unmarshal([]byte(line), &record), line)
		if record["event_id"] == "bolt.connection.limit_reached" {
			records = append(records, record)
		}
	}
	return records
}

// A connection past MaxConnections is closed and logged, with the limit.
func TestHandleConnectionLogsConnectionLimit(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxConnections = 1
	srv, output := newCapturingServer(t, cfg)
	srv.activeConnections.Store(1)

	client, server := net.Pipe()
	defer client.Close()
	srv.handleConnection(server)

	_, err := client.Read(make([]byte, 1))
	require.Error(t, err, "the rejected connection is closed")
	require.Equal(t, int64(1), srv.activeConnections.Load(), "the rejected connection isn't counted")

	records := connectionLimitRecords(t, output.String())
	require.Len(t, records, 1, output.String())
	require.Equal(t, "WARN", records[0]["level"])
	require.Equal(t, float64(1), records[0]["max_connections"])
	require.Equal(t, float64(1), records[0]["rejected"])
	require.Equal(t, "rejecting connections: Bolt connection limit reached", records[0]["msg"])
}

// After the first rejection, one warning per interval reports how many
// connections were turned away since the previous one.
func TestLogConnectionLimitReportsOncePerInterval(t *testing.T) {
	srv, output := newCapturingServer(t, DefaultConfig())
	conn, peer := net.Pipe()
	defer conn.Close()
	defer peer.Close()
	start := time.Unix(1_700_000_000, 0)

	srv.logConnectionLimit(conn, 100, start)
	srv.logConnectionLimit(conn, 100, start.Add(time.Second))
	srv.logConnectionLimit(conn, 100, start.Add(connectionLimitLogInterval-time.Nanosecond))
	require.Len(t, connectionLimitRecords(t, output.String()), 1)

	srv.logConnectionLimit(conn, 100, start.Add(connectionLimitLogInterval))
	records := connectionLimitRecords(t, output.String())
	require.Len(t, records, 2)
	require.Equal(t, float64(1), records[0]["rejected"])
	require.Equal(t, float64(3), records[1]["rejected"], "the two held back and this one")
	require.Equal(t, float64(100), records[1]["max_connections"])
}

// A connection without a remote address is still reported.
func TestLogConnectionLimitWithoutRemoteAddress(t *testing.T) {
	srv, output := newCapturingServer(t, DefaultConfig())
	srv.logConnectionLimit(addresslessConn{}, 2, time.Unix(1, 0))
	records := connectionLimitRecords(t, output.String())
	require.Len(t, records, 1)
	require.Equal(t, "", records[0]["remote"])
}

type addresslessConn struct{ net.Conn }

func (addresslessConn) RemoteAddr() net.Addr { return nil }
