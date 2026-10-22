package bolt

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/memory"
	"github.com/stretchr/testify/require"
)

func TestBoltHTTPMetadataAdmission(t *testing.T) {
	for _, upgraded := range []bool{false, true} {
		t.Run(fmt.Sprint(upgraded), func(t *testing.T) {
			server, port := startWSBoltServer(t, nil)
			policy := memory.NewPolicy(128<<10, 0, time.Minute, func() (memory.Sample, error) { return memory.Sample{Used: 1}, nil }, time.Now)
			require.NoError(t, policy.Refresh())
			server.memoryPolicy = policy
			conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
			require.NoError(t, err)
			defer conn.Close()
			require.NoError(t, conn.SetDeadline(time.Now().Add(3*time.Second)))
			headers := ""
			if upgraded {
				headers = "Connection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"
			}
			_, err = io.WriteString(conn, "GET /?x="+strings.Repeat("%78", 32<<10)+" HTTP/1.1\r\nHost: localhost\r\n"+headers+"\r\n")
			require.NoError(t, err)
			response, err := http.ReadResponse(bufio.NewReader(conn), nil)
			require.NoError(t, err)
			defer response.Body.Close()
			require.Equal(t, http.StatusServiceUnavailable, response.StatusCode)
			if response.StatusCode == http.StatusServiceUnavailable {
				data, err := io.ReadAll(response.Body)
				require.NoError(t, err)
				require.Contains(t, string(data), "MemoryPoolOutOfMemoryError")
			}
			require.Eventually(t, func() bool { return policy.Reserved() == 0 }, time.Second, time.Millisecond)
		})
	}
}
