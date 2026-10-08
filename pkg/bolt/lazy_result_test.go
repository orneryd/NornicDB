package bolt

import (
	"fmt"
	"net"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// pullBatch sends PULL {n} and reads its records and summary: the SUCCESS
// metadata, or the FAILURE code.
func pullBatch(t *testing.T, conn net.Conn, n int64) (records [][]any, success map[string]any, failureCode string) {
	t.Helper()
	requireNoError(t, SendPull(t, conn, map[string]any{"n": n}))
	return readBatch(t, conn)
}

func readBatch(t *testing.T, conn net.Conn) (records [][]any, success map[string]any, failureCode string) {
	t.Helper()
	for {
		msgType, msgData, err := ReadMessage(conn)
		requireNoError(t, err)
		switch msgType {
		case MsgRecord:
			fields, _, err := decodePackStreamList(msgData, 0)
			requireNoError(t, err)
			records = append(records, fields)
		case MsgSuccess:
			metadata, _, err := decodePackStreamMap(msgData, 0)
			requireNoError(t, err)
			return records, metadata, ""
		case MsgFailure:
			metadata, _, err := decodePackStreamMap(msgData, 0)
			requireNoError(t, err)
			code, _ := metadata["code"].(string)
			return records, nil, code
		case MsgIgnored:
			return records, nil, "IGNORED"
		default:
			t.Fatalf("unexpected Bolt message type 0x%02X", msgType)
		}
	}
}

func sendDiscard(t *testing.T, conn net.Conn, n int64) {
	t.Helper()
	message := append([]byte{0xB1, MsgDiscard}, encodePackStreamMap(map[string]any{"n": n})...)
	requireNoError(t, SendMessage(conn, message))
}

func lazyResultTestConn(t *testing.T, items int) net.Conn {
	t.Helper()
	store := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "test")
	_, port := startBoltIntegrationServer(t, store)
	conn := openBoltTestConn(t, port)
	if items > 0 {
		runBoltQueryAndCollectRecords(t, conn, fmt.Sprintf("UNWIND range(0, %d) AS i CREATE (:Item {k: i})", items-1))
	}
	return conn
}

// An auto-commit read larger than lazyResultThreshold is produced as PULL
// asks for it: every row arrives once, across the batches the client asked
// for, and the last PULL ends the result (#939).
func TestBoltLazyResultDeliversEveryRowInBatches(t *testing.T) {
	conn := lazyResultTestConn(t, 3000)
	requireNoError(t, SendRun(t, conn, "MATCH (n:Item) RETURN n.k AS k", nil, nil))
	run, err := AssertSuccess(t, conn)
	requireNoError(t, err)
	if fields, _ := run["fields"].([]any); len(fields) != 1 || fields[0] != "k" {
		t.Fatalf("RUN fields = %v", run["fields"])
	}
	seen := make(map[int64]bool)
	collect := func(records [][]any) {
		for _, record := range records {
			k := record[0].(int64)
			if seen[k] {
				t.Fatalf("row %d delivered twice", k)
			}
			seen[k] = true
		}
	}
	for _, n := range []int64{10, 500, 1200} {
		records, success, failure := pullBatch(t, conn, n)
		if failure != "" || len(records) != int(n) || success["has_more"] != true {
			t.Fatalf("PULL %d: %d records, %v, failure %q", n, len(records), success, failure)
		}
		collect(records)
	}
	records, success, failure := pullBatch(t, conn, -1)
	if failure != "" || success["has_more"] != nil || success["type"] != "r" {
		t.Fatalf("last PULL: %v, failure %q", success, failure)
	}
	collect(records)
	if len(seen) != 3000 {
		t.Fatalf("got %d distinct rows, want 3000", len(seen))
	}
	// The session takes the next statement.
	if rows := runBoltQueryAndCollectRecords(t, conn, "RETURN 1 AS one"); len(rows) != 1 {
		t.Fatalf("next statement rows = %v", rows)
	}
}

// An error the statement meets after it started streaming fails the PULL
// that reaches it, after the rows before it, with the error's code (#939).
// A statement that fails within lazyResultThreshold rows fails at RUN, as
// before.
func TestBoltLazyResultErrorFailsThePullThatReachesIt(t *testing.T) {
	conn := lazyResultTestConn(t, 0)
	requireNoError(t, SendRun(t, conn, "UNWIND range(1, 5000) AS i RETURN 10 / (i - 2000) AS x", nil, nil))
	requireNoError(t, ReadSuccess(t, conn))
	records, success, failure := pullBatch(t, conn, 1000)
	if failure != "" || len(records) != 1000 || success["has_more"] != true {
		t.Fatalf("first PULL: %d records, %v, failure %q", len(records), success, failure)
	}
	records, _, failure = pullBatch(t, conn, 1000)
	if failure != "Neo.ClientError.Statement.ArithmeticError" || len(records) != 999 {
		t.Fatalf("second PULL: %d records, failure %q", len(records), failure)
	}
	// Failed until RESET, as after any FAILURE.
	_, _, failure = pullBatch(t, conn, 10)
	if failure != "IGNORED" {
		t.Fatalf("PULL after FAILURE = %q, want IGNORED", failure)
	}
	requireNoError(t, SendReset(t, conn))
	requireNoError(t, ReadSuccess(t, conn))

	// Failing on the row right after the first lazyResultThreshold rows is
	// past them: RUN succeeds and the PULL asking past them fails.
	requireNoError(t, SendRun(t, conn, "UNWIND range(1, 5000) AS i RETURN 10 / (i - 1001) AS x", nil, nil))
	requireNoError(t, ReadSuccess(t, conn))
	records, success, failure = pullBatch(t, conn, 1000)
	if failure != "" || len(records) != 1000 || success["has_more"] != true {
		t.Fatalf("boundary first PULL: %d records, %v, failure %q", len(records), success, failure)
	}
	records, _, failure = pullBatch(t, conn, 1000)
	if failure != "Neo.ClientError.Statement.ArithmeticError" || len(records) != 0 {
		t.Fatalf("boundary second PULL: %d records, failure %q", len(records), failure)
	}
	requireNoError(t, SendReset(t, conn))
	requireNoError(t, ReadSuccess(t, conn))

	code, _ := runBoltQueryExpectFailure(t, conn, "UNWIND range(1, 500) AS i RETURN 10 / (i - 200) AS x")
	if code != "Neo.ClientError.Statement.ArithmeticError" {
		t.Fatalf("small result RUN failure = %q", code)
	}
}

// DISCARD, RESET and a new RUN end a lazy result; the session goes on.
func TestBoltLazyResultEndsOnDiscardResetAndRun(t *testing.T) {
	conn := lazyResultTestConn(t, 3000)
	const query = "MATCH (n:Item) RETURN n.k AS k"

	requireNoError(t, SendRun(t, conn, query, nil, nil))
	requireNoError(t, ReadSuccess(t, conn))
	sendDiscard(t, conn, -1)
	if _, success, failure := readBatch(t, conn); failure != "" || success["has_more"] != nil {
		t.Fatalf("DISCARD all: %v, failure %q", success, failure)
	}

	// A partial DISCARD skips rows; the PULL after it gets the rest.
	requireNoError(t, SendRun(t, conn, query, nil, nil))
	requireNoError(t, ReadSuccess(t, conn))
	sendDiscard(t, conn, 2500)
	if _, success, failure := readBatch(t, conn); failure != "" || success["has_more"] != true {
		t.Fatalf("DISCARD 2500: %v, failure %q", success, failure)
	}
	if records, success, failure := pullBatch(t, conn, -1); failure != "" || len(records) != 500 || success["has_more"] != nil {
		t.Fatalf("PULL after DISCARD: %d records, %v, failure %q", len(records), success, failure)
	}

	requireNoError(t, SendRun(t, conn, query, nil, nil))
	requireNoError(t, ReadSuccess(t, conn))
	if records, _, failure := pullBatch(t, conn, 10); failure != "" || len(records) != 10 {
		t.Fatalf("PULL before RESET: %d records, failure %q", len(records), failure)
	}
	requireNoError(t, SendReset(t, conn))
	requireNoError(t, ReadSuccess(t, conn))

	requireNoError(t, SendRun(t, conn, query, nil, nil))
	requireNoError(t, ReadSuccess(t, conn))
	rows := runBoltQueryAndCollectRecords(t, conn, "MATCH (n:Item) WHERE n.k < 3 RETURN n.k AS k ORDER BY k")
	if len(rows) != 3 {
		t.Fatalf("RUN after an unread lazy result: rows = %v", rows)
	}
}

// Statements the stream doesn't take return their whole result at RUN:
// ORDER BY, a write, and an explicit transaction.
func TestBoltLazyResultLeavesOtherStatementsWhole(t *testing.T) {
	conn := lazyResultTestConn(t, 3000)
	for _, query := range []string{
		"MATCH (n:Item) RETURN n.k AS k ORDER BY k",
		"MATCH (n:Item) SET n.seen = true RETURN n.k AS k",
	} {
		rows := runBoltQueryAndCollectRecords(t, conn, query)
		if len(rows) != 3000 {
			t.Fatalf("%s: %d rows", query, len(rows))
		}
	}
}
