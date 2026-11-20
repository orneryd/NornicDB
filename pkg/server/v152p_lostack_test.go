package server

// Lost-acknowledgement probe (untracked; never committed). The setup of
// TestWhaR37CleanShutdownLeavesNoUnappliedCommit ("listener drain"), with every
// request recorded by name: status, body, body read error, send and answer
// times relative to the start of the clean shutdown. After the stop the data
// directory is copied; the journal of that copy is searched for every name;
// the store is reopened and every name is looked up in the store and in the
// change feed.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type lostackRecord struct {
	name     string
	status   int
	body     string
	readErr  string
	doErr    string
	sent     time.Time
	answered time.Time
}

// loose is the original probe's rule: 200 and no `"code"` in the body.
func (r lostackRecord) loose() bool {
	return r.doErr == "" && r.status == http.StatusOK && !strings.Contains(r.body, `"code"`)
}

// strict: 200, the whole body read, valid JSON with an empty errors list and
// one result.
func (r lostackRecord) strict() bool {
	if !r.loose() || r.readErr != "" {
		return false
	}
	var body struct {
		Results []json.RawMessage `json:"results"`
		Errors  []json.RawMessage `json:"errors"`
	}
	if json.Unmarshal([]byte(r.body), &body) != nil {
		return false
	}
	return len(body.Errors) == 0 && len(body.Results) == 1
}

func lostackRounds() int {
	if v := os.Getenv("LOSTACK_ROUNDS"); v != "" {
		var n int
		fmt.Sscan(v, &n)
		if n > 0 {
			return n
		}
	}
	return 20
}

func TestLostAckCleanShutdownListenerDrain(t *testing.T) {
	rounds := lostackRounds()
	totals := map[string]int{}
	for round := 0; round < rounds; round++ {
		dir := filepath.Join(t.TempDir(), "data")
		n := whaOpen(t, dir, true)
		whaSetDiff(t, n)
		earliest := whaCursor(t, n, "db.cdc.earliest")
		var mu sync.Mutex
		var records []lostackRecord
		var stop atomic.Bool
		var wg sync.WaitGroup
		client := &http.Client{Timeout: 10 * time.Second}
		address := n.server.Addr()
		for writer := 0; writer < 16; writer++ {
			wg.Add(1)
			go func(writer int) {
				defer wg.Done()
				for index := 0; !stop.Load(); index++ {
					name := fmt.Sprintf("r%02d-w%02d-%05d", round, writer, index)
					payload, _ := json.Marshal(map[string]any{"statements": []any{map[string]any{
						"statement":  "CREATE (:R37L {name: $name})",
						"parameters": map[string]any{"name": name},
					}}})
					rec := lostackRecord{name: name, sent: time.Now()}
					request, _ := http.NewRequest(http.MethodPost, "http://"+address+whaEndpoint, bytes.NewReader(payload))
					request.Header.Set("Authorization", n.token)
					request.Header.Set("Content-Type", "application/json")
					response, err := client.Do(request)
					if err != nil {
						rec.doErr = err.Error()
					} else {
						rec.status = response.StatusCode
						body, readErr := io.ReadAll(response.Body)
						_ = response.Body.Close()
						rec.body = string(body)
						if readErr != nil {
							rec.readErr = readErr.Error()
						}
					}
					rec.answered = time.Now()
					mu.Lock()
					records = append(records, rec)
					mu.Unlock()
					if !rec.loose() {
						if stop.Load() {
							return
						}
						time.Sleep(time.Millisecond)
					}
				}
			}(writer)
		}
		time.Sleep(1500 * time.Millisecond)
		shutdownStart := time.Now()
		n.stop(t)
		shutdownEnd := time.Now()
		stop.Store(true)
		wg.Wait()

		// The journal as the clean stop left it, before any reopen.
		snapshotDir := filepath.Join(t.TempDir(), "copy")
		whaCopyDir(t, dir, snapshotDir)
		journal, journalErr := whaReadJournal(filepath.Join(snapshotDir, "wal"))
		inJournal := func(name string) string {
			if journalErr != nil {
				return "journal unreadable: " + journalErr.Error()
			}
			var ops []string
			for _, entry := range journal {
				if bytes.Contains(entry.Data, []byte(name)) {
					ops = append(ops, fmt.Sprintf("%s#%d", entry.Operation, entry.Sequence))
				}
			}
			if len(ops) == 0 {
				return "absent"
			}
			return strings.Join(ops, ",")
		}

		r := whaOpen(t, dir, false)
		stored := map[string]int{}
		for _, row := range whaRows(t, r, "MATCH (x:R37L) RETURN x.name", nil) {
			stored[fmt.Sprint(row[0])]++
		}
		events := map[string]int{}
		for _, pair := range whaEventNames(t, r, earliest, "R37L") {
			parts := strings.SplitN(pair, " ", 2)
			if len(parts) == 2 {
				events[parts[1]]++
			}
		}
		recovery := whaRecoveredLine(r.stdout)
		r.stop(t)

		var loose, strict, looseNotStrict, missing, storedNotAcked, duplicates int
		var details []string
		for _, rec := range records {
			if rec.loose() {
				loose++
			}
			if rec.strict() {
				strict++
			}
			if rec.loose() && !rec.strict() {
				looseNotStrict++
				details = append(details, fmt.Sprintf("LOOSE-NOT-STRICT %s status=%d readErr=%q body=%q", rec.name, rec.status, rec.readErr, lostackCut(rec.body)))
			}
			if rec.loose() && stored[rec.name] == 0 {
				missing++
				details = append(details, fmt.Sprintf("ACKED-MISSING %s strict=%v status=%d readErr=%q body=%q sent=%+dms answered=%+dms (shutdown %dms) journal=%s events=%d",
					rec.name, rec.strict(), rec.status, rec.readErr, lostackCut(rec.body),
					rec.sent.Sub(shutdownStart).Milliseconds(), rec.answered.Sub(shutdownStart).Milliseconds(), shutdownEnd.Sub(shutdownStart).Milliseconds(),
					inJournal(rec.name), events[rec.name]))
			}
			if !rec.loose() && stored[rec.name] > 0 {
				storedNotAcked++
			}
		}
		for name, count := range stored {
			if count > 1 {
				duplicates++
				details = append(details, fmt.Sprintf("DUPLICATE %s x%d", name, count))
			}
		}
		storedTotal := 0
		for _, c := range stored {
			storedTotal += c
		}
		sort.Strings(details)
		totals["rounds"]++
		totals["missing"] += missing
		totals["looseNotStrict"] += looseNotStrict
		if missing > 0 {
			totals["roundsWithMissing"]++
		}
		t.Logf("LOSTACK round=%d requests=%d loose-acked=%d strict-acked=%d loose-not-strict=%d stored=%d stored-names=%d events=%d acked-missing=%d stored-not-acked=%d duplicates=%d journal=%d records recovery=%q shutdown=%dms",
			round, len(records), loose, strict, looseNotStrict, storedTotal, len(stored), len(events), missing, storedNotAcked, duplicates, len(journal), recovery, shutdownEnd.Sub(shutdownStart).Milliseconds())
		for _, d := range details {
			t.Logf("LOSTACK round=%d %s", round, d)
		}
	}
	t.Logf("LOSTACK TOTAL %v", totals)
}

func lostackCut(s string) string {
	if len(s) > 300 {
		return s[:300] + "…"
	}
	return s
}
