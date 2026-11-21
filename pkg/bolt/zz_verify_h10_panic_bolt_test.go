package bolt

// Verifier of hunt lead L-H10-004 (test-side only): what a Bolt client sees
// for a statement with one extra ')' before '{', in auto-commit and inside an
// explicit transaction, and whether the server then still serves a new
// connection (a write and a read).

import (
	"encoding/json"
	"fmt"
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
			base := storage.NewMemoryEngine()
			t.Cleanup(func() { _ = base.Close() })
			store := storage.NewNamespacedEngine(base, "test")
			_, port := startBoltIntegrationServer(t, store, mode.mode)

			// Baseline (before any bad statement): a new connection writes and reads.
			verifyH10BoltWriteRead(t, port, mode.name, "baseline-write-read", 1)

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

			// After the bad statements: a new connection still writes and reads.
			verifyH10BoltWriteRead(t, port, mode.name, "after-write-read", 2)
		})
	}
}

// verifyH10BoltWriteRead opens a new connection, creates one node and counts
// them; it reports the replies instead of failing on the first FAILURE.
func verifyH10BoltWriteRead(t *testing.T, port int, mode, step string, want int64) {
	t.Helper()
	conn := openBoltTestConn(t, port)
	fields := map[string]any{"lead": "L-H10-004", "route": "bolt", "mode": mode, "role": "control", "step": step}
	requireNoError(t, SendRun(t, conn, "CREATE (:VerifyH10 {k: 1})", nil, nil))
	fields["create_run_reply"] = verifyH10BoltObserve(conn)
	requireNoError(t, SendPull(t, conn, nil))
	fields["create_pull_reply"] = verifyH10BoltObserve(conn)
	requireNoError(t, SendRun(t, conn, "MATCH (n:VerifyH10) RETURN count(n) AS c", nil, nil))
	fields["count_run_reply"] = verifyH10BoltObserve(conn)
	requireNoError(t, SendPull(t, conn, nil))
	msgType, data, err := ReadMessage(conn)
	if err == nil && msgType == MsgRecord {
		rec, _, derr := decodePackStreamList(data, 0)
		if derr == nil {
			fields["count"] = rec
			if len(rec) != 1 || fmt.Sprint(rec[0]) != fmt.Sprint(want) {
				t.Errorf("control %s %s: count %v, want %d", mode, step, rec, want)
			}
		}
	} else {
		fields["count_read"] = fmt.Sprintf("type 0x%02X err %v", msgType, err)
		t.Errorf("control %s %s: no count record: %v", mode, step, fields)
	}
	verifyH10BoltEmit(t, fields)
}
