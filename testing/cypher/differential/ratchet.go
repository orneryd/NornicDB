package differential

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
)

// UpdateRatchetEnv, set to 1, rewrites a route's entries in the ratchet file
// from UpdateRuns runs instead of checking one run against them.
const UpdateRatchetEnv = "NORNICDB_DIFFERENTIAL_UPDATE_RATCHET"

// UpdateRuns is how many times an update runs a route: a statement that
// matches Neo4j in some runs and not in others is listed as unstable.
const UpdateRuns = 3

// Entry is one known difference: a statement (or a statement's graph state)
// that differs from Neo4j through one route, and the issue that tracks it.
// An Unstable entry sometimes matches and sometimes doesn't (rows tied under
// an ORDER BY, say); checks skip it.
type Entry struct {
	ID       string `json:"id"`
	Route    string `json:"route"`
	Issue    int    `json:"issue"`
	Unstable bool   `json:"unstable,omitempty"`
}

// Ratchet is the known differences, by route and id.
type Ratchet map[string]map[string]Entry

// LoadRatchet reads the ratchet file: one JSON Entry per line. A missing file
// is an empty ratchet.
func LoadRatchet(path string) (Ratchet, error) {
	ratchet := Ratchet{}
	file, err := os.Open(path)
	if os.IsNotExist(err) {
		return ratchet, nil
	}
	if err != nil {
		return nil, err
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for line := 1; scanner.Scan(); line++ {
		text := strings.TrimSpace(scanner.Text())
		if text == "" {
			continue
		}
		var entry Entry
		if err := json.Unmarshal([]byte(text), &entry); err != nil || entry.ID == "" || entry.Route == "" {
			return nil, fmt.Errorf("%s:%d: not a ratchet entry: %s", path, line, text)
		}
		if ratchet[entry.Route] == nil {
			ratchet[entry.Route] = map[string]Entry{}
		}
		ratchet[entry.Route][entry.ID] = entry
	}
	return ratchet, scanner.Err()
}

// Report is a run checked against the ratchet.
type Report struct {
	Route      string
	Compared   int
	Known      int
	Unstable   int
	Regressed  []Result // differ from Neo4j and aren't listed
	NowMatches []Entry  // listed, and match Neo4j now
	Stale      []Entry  // listed, and no longer compared
}

// Check compares results, every statement of route, with its listed
// differences.
func (ratchet Ratchet) Check(route string, results []Result) Report {
	report := Report{Route: route, Compared: len(results)}
	listed := ratchet[route]
	compared := make(map[string]bool, len(results))
	for _, result := range results {
		compared[result.ID] = true
		entry, known := listed[result.ID]
		switch {
		case known && entry.Unstable:
			report.Unstable++
		case !result.Match && known:
			report.Known++
		case !result.Match:
			report.Regressed = append(report.Regressed, result)
		case known:
			report.NowMatches = append(report.NowMatches, listed[result.ID])
		}
	}
	for id, entry := range listed {
		if !compared[id] {
			report.Stale = append(report.Stale, entry)
		}
	}
	sort.Slice(report.Stale, func(left, right int) bool { return report.Stale[left].ID < report.Stale[right].ID })
	return report
}

// Update replaces route's entries with the differences of runs (repeated
// runs of every statement of the route) and writes the file, sorted, keeping
// the other routes' entries. A statement that differs in every run is listed;
// one that differs in some runs only is listed as unstable.
func (ratchet Ratchet) Update(path, route string, runs [][]Result) error {
	ratchet[route] = map[string]Entry{}
	matched := map[string]int{}
	issues := map[string]int{}
	for _, results := range runs {
		for _, result := range results {
			issues[result.ID] = result.Issue
			if result.Match {
				matched[result.ID]++
			}
		}
	}
	for id := range issues {
		switch matches := matched[id]; {
		case matches == len(runs):
		case matches == 0:
			ratchet[route][id] = Entry{ID: id, Route: route, Issue: issues[id]}
		default:
			ratchet[route][id] = Entry{ID: id, Route: route, Issue: issues[id], Unstable: true}
		}
	}
	var entries []Entry
	for _, byID := range ratchet {
		for _, entry := range byID {
			entries = append(entries, entry)
		}
	}
	sort.Slice(entries, func(left, right int) bool {
		if entries[left].Route != entries[right].Route {
			return entries[left].Route < entries[right].Route
		}
		if entries[left].Issue != entries[right].Issue {
			return entries[left].Issue < entries[right].Issue
		}
		return entries[left].ID < entries[right].ID
	})
	var builder strings.Builder
	for _, entry := range entries {
		// An Entry is strings, an int and a bool: it always encodes.
		line, _ := json.Marshal(entry)
		builder.Write(line)
		builder.WriteByte('\n')
	}
	return os.WriteFile(path, []byte(builder.String()), 0o644)
}

// maxReported bounds the differences a failing run prints in full.
const maxReported = 40

// Assert runs a route (run returns its results and the resets it retried)
// and checks the results against the ratchet file at path, or, when
// UpdateRatchetEnv is 1, runs it UpdateRuns times and rewrites the route's
// entries. A check fails t for every statement that differs from Neo4j and
// isn't listed, and lists the entries that match now so they can be removed.
func Assert(t testing.TB, path, route string, run func() ([]Result, int, error)) {
	t.Helper()
	ratchet, err := LoadRatchet(path)
	if err != nil {
		t.Fatalf("load differential ratchet: %v", err)
	}
	if os.Getenv(UpdateRatchetEnv) == "1" {
		runs := make([][]Result, UpdateRuns)
		for index := range runs {
			if runs[index], _, err = run(); err != nil {
				t.Fatalf("%s run %d: %v", route, index+1, err)
			}
		}
		if err := ratchet.Update(path, route, runs); err != nil {
			t.Fatalf("update differential ratchet: %v", err)
		}
		t.Logf("DIFFERENTIAL_RATCHET route=%s updated %s from %d runs", route, path, UpdateRuns)
		return
	}
	results, resetRetries, err := run()
	if err != nil {
		t.Fatalf("%s: %v", route, err)
	}
	report := ratchet.Check(route, results)
	t.Logf("DIFFERENTIAL_RATCHET route=%s compared=%d known=%d unstable=%d regressed=%d now_match=%d stale=%d reset_retries=%d",
		route, report.Compared, report.Known, report.Unstable, len(report.Regressed), len(report.NowMatches), len(report.Stale), resetRetries)
	for _, removable := range []struct {
		entries []Entry
		reason  string
	}{{report.NowMatches, "matches Neo4j now"}, {report.Stale, "no longer compared"}} {
		for index, entry := range removable.entries {
			if index == maxReported {
				t.Logf("... and %d more (%s)", len(removable.entries)-maxReported, removable.reason)
				break
			}
			t.Logf("remove from the ratchet (%s): %s", removable.reason, mustJSON(entry))
		}
	}
	for index, result := range report.Regressed {
		if index == maxReported {
			t.Errorf("... and %d more statements that differ from Neo4j", len(report.Regressed)-maxReported)
			break
		}
		t.Errorf("differs from Neo4j (%s, %s #%d): %s\n  query:    %s\n  Neo4j:    %s\n  NornicDB: %s",
			route, result.Kind, result.Issue, result.ID, result.Query, mustJSON(result.Neo4j), mustJSON(result.NornicDB))
	}
}

func mustJSON(value any) string {
	encoded, err := json.Marshal(value)
	if err != nil {
		return fmt.Sprintf("%v", value)
	}
	if len(encoded) > 600 {
		return string(encoded[:600]) + "…"
	}
	return string(encoded)
}
