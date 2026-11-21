package bolt

// Verifier of hunt lead L-H10-004 (test-side only): what a Bolt client sees
// for a statement with one extra ')' before '{', in auto-commit and inside an
// explicit transaction, and whether the server then still serves a new
// connection (a write and a read).

import (
	"encoding/json"
	"net"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func verifyH10BoltObserve(conn net.Conn) map[string]any {
	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	defer conn.SetReadDeadline(time.Time{})
	msgType, data, err := ReadMessage(conn)
	if err != nil {
		return map[string]any{"read_error": err.Error()}
	}
	out := map[string]any{"msg_type": msgType}
	if msgType == MsgFailure {
		meta, _, derr := decodePackStreamMap(data, 0)
		if derr == nil {
			out["code"], out["message"] = meta["code"], meta["message"]
		}
	}
	return out
}

func verifyH10BoltEmit(t *testing.T, fields map[string]any) {
	t.Helper()
	data, _ := json.Marshal(fields)
	t.Logf("VERIFY_H10_PANIC_BOLT %s", data)
}

func TestVerifyH10ExtraParenBolt(t *testing.T) {
	for _, mode := range []struct {
		name string
		mode boltIntegrationMode
	}{{"autocommit", boltIntegrationAutocommit}, {"explicit-tx", boltIntegrationExplicitTx}} {
		mode := mode
		t.Run("L-H10-004/"+mode.name, func(t *testing.T) {
			store := storage.NewMemoryEngine()
			t.Cleanup(func() { _ = store.Close() })
			_, port := startBoltIntegrationServer(t, store, mode.mode)

			for _, stmt := range []string{") {}", "MATCH (n) CALL (n)) { RETURN 1 AS y } RETURN y"} {
				conn := openBoltTestConn(t, port)
				if mode.mode == boltIntegrationExplicitTx {
					requireNoError(t, SendBegin(t, conn, nil))
					requireNoError(t, ReadSuccess(t, conn))
				}
				requireNoError(t, SendRun(t, conn, stmt, nil, nil))
				seen := verifyH10BoltObserve(conn)
				// After a FAILURE the client RESETs; a dropped connection fails here.
				resetErr := SendReset(t, conn)
				after := verifyH10BoltObserve(conn)
				fields := map[string]any{"lead": "L-H10-004", "route": "bolt", "mode": mode.name, "role": "fail", "statement": stmt, "run_reply": seen, "reset_reply": after}
				if resetErr != nil {
					fields["reset_send_error"] = resetErr.Error()
				}
				verifyH10BoltEmit(t, fields)
				if seen["code"] != "Neo.ClientError.Statement.SyntaxError" {
					t.Errorf("DISAGREE %s %q: expected FAILURE Neo.ClientError.Statement.SyntaxError, got %v", mode.name, stmt, seen)
				}
			}

			// Control: the same shape without '{' is a syntax error.
			conn := openBoltTestConn(t, port)
			requireNoError(t, SendRun(t, conn, "MATCH (n)) RETURN n", nil, nil))
			ctl := verifyH10BoltObserve(conn)
			verifyH10BoltEmit(t, map[string]any{"lead": "L-H10-004", "route": "bolt", "mode": mode.name, "role": "control", "statement": "MATCH (n)) RETURN n", "run_reply": ctl})
			if ctl["code"] != "Neo.ClientError.Statement.SyntaxError" {
				t.Errorf("control: expected a syntax error, got %v", ctl)
			}

			// Control: a new connection still writes and reads.
			conn2 := openBoltTestConn(t, port)
			runBoltQueryWithParams(t, conn2, "CREATE (:VerifyH10 {k: 1})", nil)
			rows := runBoltQueryWithParams(t, conn2, "MATCH (n:VerifyH10) RETURN count(n) AS c", nil)
			verifyH10BoltEmit(t, map[string]any{"lead": "L-H10-004", "route": "bolt", "mode": mode.name, "role": "control", "step": "new-connection-write-read", "rows": rows})
		})
	}
}
