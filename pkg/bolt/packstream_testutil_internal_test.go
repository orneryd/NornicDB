package bolt

import (
	"net"
	"testing"
)

func TestPackstreamTestutil_SendMessage_ReadMessage_MultiChunk(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	// Build a >64KiB message so it must be chunked.
	// Put a PackStream struct marker/signature up front so ReadMessage can parse the type.
	payload := make([]byte, 70000)
	payload[0] = 0xB1
	payload[1] = MsgSuccess

	done := make(chan error, 1)
	go func() {
		done <- SendMessage(client, payload)
	}()

	msgType, msgData, err := ReadMessage(server)
	if err != nil {
		t.Fatalf("ReadMessage failed: %v", err)
	}
	if msgType != MsgSuccess {
		t.Fatalf("msgType mismatch: got=0x%02X want=0x%02X", msgType, MsgSuccess)
	}
	if len(msgData) != len(payload)-2 {
		t.Fatalf("msgData length mismatch: got=%d want=%d", len(msgData), len(payload)-2)
	}

	if err := <-done; err != nil {
		t.Fatalf("SendMessage failed: %v", err)
	}
}
