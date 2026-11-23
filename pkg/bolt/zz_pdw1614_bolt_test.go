package bolt

// Scratch investigation (pd-w161-4), not for merge. Observation 3: the
// result summary's t_first (RUN SUCCESS) and t_last (PULL SUCCESS), and when
// an auto-commit result's records are produced relative to RUN and PULL.
// Every deciding value is logged with the prefix PDW1614BOLT.

import (
	"fmt"
	"net"
	"testing"
	"time"
)

const pdw1614BoltMatch = "MATCH (n:Item) WHERE n.k < $n RETURN n.k AS k, reduce(s = 0, x IN range(1, $w) | s + x) AS v"
const pdw1614BoltUnwind = "UNWIND range(1, $n) AS i RETURN i AS k, reduce(s = 0, x IN range(1, $w) | s + x) AS v"

type pdw1614BoltTiming struct {
	runSuccess   map[string]any
	pullSuccess  map[string]any
	runLatency   time.Duration // RUN sent -> RUN SUCCESS read
	firstRecord  time.Duration // PULL sent -> first RECORD read
	pullTotal    time.Duration // PULL sent -> PULL SUCCESS read
	records      int
	failure      string
	runToFirst   time.Duration // RUN sent -> first RECORD (pipelined)
}

func pdw1614BoltRead(t *testing.T, conn net.Conn, start time.Time, timing *pdw1614BoltTiming) {
	t.Helper()
	for {
		msgType, msgData, err := ReadMessage(conn)
		requireNoError(t, err)
		switch msgType {
		case MsgRecord:
			if timing.records == 0 {
				timing.firstRecord = time.Since(start)
			}
			timing.records++
		case MsgSuccess:
			metadata, _, err := decodePackStreamMap(msgData, 0)
			requireNoError(t, err)
			timing.pullSuccess = metadata
			timing.pullTotal = time.Since(start)
			return
		case MsgFailure:
			metadata, _, _ := decodePackStreamMap(msgData, 0)
			timing.failure = fmt.Sprint(metadata)
			return
		default:
			t.Fatalf("unexpected Bolt message type 0x%02X", msgType)
		}
	}
}

// pdw1614BoltRunPull sends RUN, waits for its response, then sends PULL
// {n: -1} and reads every record and the summary.
func pdw1614BoltRunPull(t *testing.T, conn net.Conn, query string, params map[string]any) pdw1614BoltTiming {
	t.Helper()
	var timing pdw1614BoltTiming
	start := time.Now()
	requireNoError(t, SendRun(t, conn, query, params, nil))
	msgType, msgData, err := ReadMessage(conn)
	requireNoError(t, err)
	timing.runLatency = time.Since(start)
	if msgType != MsgSuccess {
		metadata, _, _ := decodePackStreamMap(msgData, 0)
		timing.failure = fmt.Sprint(metadata)
		return timing
	}
	timing.runSuccess, _, err = decodePackStreamMap(msgData, 0)
	requireNoError(t, err)
	pullStart := time.Now()
	requireNoError(t, SendPull(t, conn, map[string]any{"n": int64(-1)}))
	pdw1614BoltRead(t, conn, pullStart, &timing)
	return timing
}

// pdw1614BoltPipelined sends RUN and PULL {n: -1} together, as drivers do,
// and times the first record from the RUN.
func pdw1614BoltPipelined(t *testing.T, conn net.Conn, query string, params map[string]any) pdw1614BoltTiming {
	t.Helper()
	var timing pdw1614BoltTiming
	start := time.Now()
	requireNoError(t, SendRun(t, conn, query, params, nil))
	requireNoError(t, SendPull(t, conn, map[string]any{"n": int64(-1)}))
	msgType, msgData, err := ReadMessage(conn)
	requireNoError(t, err)
	timing.runLatency = time.Since(start)
	if msgType != MsgSuccess {
		metadata, _, _ := decodePackStreamMap(msgData, 0)
		timing.failure = fmt.Sprint(metadata)
		return timing
	}
	timing.runSuccess, _, _ = decodePackStreamMap(msgData, 0)
	pdw1614BoltRead(t, conn, start, &timing)
	timing.runToFirst = timing.firstRecord
	return timing
}

func pdw1614BoltSummary(m map[string]any) string {
	if m == nil {
		return "<none>"
	}
	out := ""
	for _, key := range []string{"t_first", "t_last", "type", "has_more", "qid", "db"} {
		if value, ok := m[key]; ok {
			out += fmt.Sprintf("%s=%v(%T) ", key, value, value)
		}
	}
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	return out + fmt.Sprintf("keys=%v", keys)
}

func TestPDW1614BoltSummaryTimingAndStreaming(t *testing.T) {
	store := nor213BoltStore(t, false)
	_, port := startBoltIntegrationServer(t, store, boltIntegrationExplicitTx)
	setup := openBoltTestConn(t, port)
	runBoltQueryWithParams(t, setup, "UNWIND range(0, 4999) AS i CREATE (:Item {k: i})", nil)

	work := int64(400)
	for _, statement := range []struct{ name, text string }{{"match", pdw1614BoltMatch}, {"unwind", pdw1614BoltUnwind}} {
		for _, n := range []int64{10, 1000, 1001, 5000} {
			params := map[string]any{"n": n, "w": work}
			conn := openBoltTestConn(t, port)
			// Warm the statement once so plan caching does not decide the timing.
			pdw1614BoltRunPull(t, conn, statement.text, params)
			for round := 0; round < 2; round++ {
				auto := pdw1614BoltRunPull(t, conn, statement.text, params)
				t.Logf("PDW1614BOLT %s n=%d round=%d mode=auto-commit RUN->SUCCESS=%v PULL->first-record=%v PULL->SUCCESS=%v records=%d failure=%q RUN SUCCESS{%s} PULL SUCCESS{%s}",
					statement.name, n, round, auto.runLatency, auto.firstRecord, auto.pullTotal, auto.records, auto.failure,
					pdw1614BoltSummary(auto.runSuccess), pdw1614BoltSummary(auto.pullSuccess))
				piped := pdw1614BoltPipelined(t, conn, statement.text, params)
				t.Logf("PDW1614BOLT %s n=%d round=%d mode=auto-commit-pipelined RUN->RUN-SUCCESS=%v RUN->first-record=%v RUN->PULL-SUCCESS=%v records=%d failure=%q RUN SUCCESS{%s} PULL SUCCESS{%s}",
					statement.name, n, round, piped.runLatency, piped.runToFirst, piped.pullTotal, piped.records, piped.failure,
					pdw1614BoltSummary(piped.runSuccess), pdw1614BoltSummary(piped.pullSuccess))
				nor213BoltBegin(t, conn)
				tx := pdw1614BoltRunPull(t, conn, statement.text, params)
				commit := nor213BoltCommitResult(t, conn)
				t.Logf("PDW1614BOLT %s n=%d round=%d mode=explicit-tx RUN->SUCCESS=%v PULL->first-record=%v PULL->SUCCESS=%v records=%d failure=%q commit=%v RUN SUCCESS{%s} PULL SUCCESS{%s}",
					statement.name, n, round, tx.runLatency, tx.firstRecord, tx.pullTotal, tx.records, tx.failure, commit,
					pdw1614BoltSummary(tx.runSuccess), pdw1614BoltSummary(tx.pullSuccess))
			}
		}
	}
}
