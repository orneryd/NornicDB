package server

// Decoded journal read for the HTTP clean-shutdown run (untracked; never
// committed). Same writes and stop as TestLostAckCleanShutdownListenerDrain.
// Right after the clean stop the data directory is copied, and the copy's
// journal is replayed by the product's own journal reader into an empty
// in-memory store (no snapshot), which decodes every record. The names found
// there are compared with the answers: every write answered 200 with a full
// success body is the positive control (it was committed, so a complete
// journal must yield it); every write answered 200 with an empty body is
// looked up the same way. The replay result (applied, skipped, failed and
// any errors) is logged, and the snapshot files present are listed.

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

func TestLostAckDecodedJournal(t *testing.T) {
	rounds := lostackRounds()
	totals := map[string]int{}
	for round := 0; round < rounds; round++ {
		dir := filepath.Join(t.TempDir(), "data")
		n := whaOpen(t, dir, true)
		whaSetDiff(t, n)
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
		n.stop(t)
		stop.Store(true)
		wg.Wait()

		copyDir := filepath.Join(t.TempDir(), "copy")
		whaCopyDir(t, dir, copyDir)
		var snapshots []string
		if entries, err := os.ReadDir(filepath.Join(copyDir, "snapshots")); err == nil {
			for _, e := range entries {
				snapshots = append(snapshots, e.Name())
			}
		}
		raw, rawErr := whaReadJournal(filepath.Join(copyDir, "wal"))
		ops := map[string]int{}
		for _, e := range raw {
			ops[string(e.Operation)]++
		}
		replayed, replay, replayErr := lostackReplayJournal(filepath.Join(copyDir, "wal"))

		r := whaOpen(t, dir, false)
		stored := map[string]int{}
		for _, row := range whaRows(t, r, "MATCH (x:R37L) RETURN x.name", nil) {
			stored[fmt.Sprint(row[0])]++
		}
		r.stop(t)

		var strictN, strictInJournal, strictStored, emptyN, emptyInJournal, emptyStored, inJournalNotStored int
		var emptyNames []string
		for _, rec := range records {
			switch {
			case rec.strict():
				strictN++
				if replayed[rec.name] > 0 {
					strictInJournal++
				}
				if stored[rec.name] > 0 {
					strictStored++
				}
			case rec.loose(): // 200 without a full success body
				emptyN++
				if replayed[rec.name] > 0 {
					emptyInJournal++
				}
				if stored[rec.name] > 0 {
					emptyStored++
				}
				emptyNames = append(emptyNames, fmt.Sprintf("%s(body=%q journal=%d stored=%d)", rec.name, lostackCut(rec.body), replayed[rec.name], stored[rec.name]))
			}
		}
		for name := range replayed {
			if stored[name] == 0 {
				inJournalNotStored++
			}
		}
		totals["strict"] += strictN
		totals["strictInJournal"] += strictInJournal
		totals["strictStored"] += strictStored
		totals["empty200"] += emptyN
		totals["empty200InJournal"] += emptyInJournal
		totals["empty200Stored"] += emptyStored
		totals["journalNamesNotStored"] += inJournalNotStored
		keys := make([]string, 0, len(ops))
		for k := range ops {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		var opParts []string
		for _, k := range keys {
			opParts = append(opParts, fmt.Sprintf("%s=%d", k, ops[k]))
		}
		sort.Strings(emptyNames)
		t.Logf("DECODED round=%d requests=%d strict=%d strict-in-journal=%d strict-stored=%d empty200=%d empty200-in-journal=%d empty200-stored=%d journal-names=%d journal-names-not-stored=%d journal-records=%d rawErr=%v ops=[%s] replay=%s replayErr=%v snapshots=%v",
			round, len(records), strictN, strictInJournal, strictStored, emptyN, emptyInJournal, emptyStored, len(replayed), inJournalNotStored, len(raw), rawErr, strings.Join(opParts, " "), replay, replayErr, snapshots)
		if len(emptyNames) > 0 {
			t.Logf("DECODED round=%d empty-200 writes: %s", round, strings.Join(emptyNames, " "))
		}
	}
	t.Logf("DECODED TOTAL %v", totals)
}
