package tck

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"

	"github.com/cucumber/godog"
)

var generatedUUIDPattern = regexp.MustCompile(`(?i)\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b`)

const (
	ratchetRouteNormal       = "normal"
	ratchetStatusPass        = "pass"
	ratchetStatusExpectedGap = "expected-gap"
	ratchetStatusHarness     = "harness-error"
)

// RatchetBaseline records every scenario outcome in the pinned TCK corpus.
// Entries are intentionally scenario-specific: aggregate pass counts cannot
// detect one passing behavior regressing while a different behavior improves.
type RatchetBaseline struct {
	UpstreamRevision string         `json:"upstream_revision"`
	CorpusSHA256     string         `json:"corpus_sha256"`
	RouteMode        string         `json:"route_mode"`
	VettedFeatures   []string       `json:"vetted_features"`
	Entries          []RatchetEntry `json:"entries"`
}

// RatchetEntry identifies one expanded scenario in one transaction mode.
type RatchetEntry struct {
	FeaturePath      string          `json:"feature_path"`
	ScenarioName     string          `json:"scenario_name"`
	ExampleRow       string          `json:"example_row"`
	TransactionMode  TransactionMode `json:"transaction_mode"`
	RouteMode        string          `json:"route_mode"`
	Status           string          `json:"status"`
	FailureSignature string          `json:"failure_signature,omitempty"`
	GapID            string          `json:"gap_id,omitempty"`
	Reason           string          `json:"reason,omitempty"`
	Workstream       string          `json:"workstream,omitempty"`
	failureDetail    string
}

// RatchetReport summarizes the independently classified corpus outcomes.
type RatchetReport struct {
	SupportedPass int
	ExpectedGap   int
	SetupBlocked  int
	HarnessError  int
	Total         int
}

func (r RatchetReport) String() string {
	return fmt.Sprintf(
		"supported-pass=%d expected-gap=%d setup-blocked=%d harness-error=%d total=%d",
		r.SupportedPass, r.ExpectedGap, r.SetupBlocked, r.HarnessError, r.Total,
	)
}

type ratchetRecorder struct {
	mu          sync.Mutex
	mode        TransactionMode
	occurrences map[string]int
	entries     []RatchetEntry
}

func newRatchetRecorder(mode TransactionMode) *ratchetRecorder {
	return &ratchetRecorder{mode: mode, occurrences: make(map[string]int)}
}

func (r *ratchetRecorder) record(scenario *godog.Scenario, scenarioErr error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	featurePath := normalizedFeaturePath(scenario.Uri)
	exampleRow := scenarioExampleRow(scenario)
	occurrenceKey := strings.Join([]string{featurePath, scenario.Name, exampleRow}, "\x00")
	r.occurrences[occurrenceKey]++
	if occurrence := r.occurrences[occurrenceKey]; occurrence > 1 {
		exampleRow = fmt.Sprintf("%s-%d", exampleRow, occurrence)
	}

	entry := RatchetEntry{
		FeaturePath:     featurePath,
		ScenarioName:    scenario.Name,
		ExampleRow:      exampleRow,
		TransactionMode: r.mode,
		RouteMode:       ratchetRouteNormal,
		Status:          ratchetStatusPass,
	}
	if scenarioErr != nil {
		entry.Status = ratchetStatusExpectedGap
		entry.FailureSignature = failureSignature(scenarioErr)
		entry.GapID = localGapID(entry)
		entry.Reason = "Engine behavior does not yet match the pinned openCypher TCK expectation."
		entry.Workstream = featureWorkstream(featurePath)
		entry.failureDetail = strings.TrimSpace(scenarioErr.Error())
		if isHarnessFailure(scenarioErr) {
			entry.Status = ratchetStatusHarness
			entry.Reason = "The TCK harness could not execute or classify the scenario."
		}
	}
	r.entries = append(r.entries, entry)
}

func (r *ratchetRecorder) snapshot() []RatchetEntry {
	r.mu.Lock()
	defer r.mu.Unlock()
	entries := append([]RatchetEntry(nil), r.entries...)
	sortRatchetEntries(entries)
	return entries
}

func normalizedFeaturePath(path string) string {
	path = filepath.ToSlash(path)
	if index := strings.Index(path, "/features/"); index >= 0 {
		return path[index+len("/features/"):]
	}
	return strings.TrimPrefix(path, "features/")
}

func scenarioExampleRow(scenario *godog.Scenario) string {
	if len(scenario.AstNodeIds) <= 1 {
		return "single"
	}
	hash := sha256.New()
	for _, step := range scenario.Steps {
		_, _ = hash.Write([]byte(step.Type))
		_, _ = hash.Write([]byte{0})
		_, _ = hash.Write([]byte(step.Text))
		_, _ = hash.Write([]byte{0})
		if step.Argument == nil {
			continue
		}
		if step.Argument.DocString != nil {
			_, _ = hash.Write([]byte(step.Argument.DocString.MediaType))
			_, _ = hash.Write([]byte{0})
			_, _ = hash.Write([]byte(step.Argument.DocString.Content))
		}
		if step.Argument.DataTable != nil {
			for _, row := range step.Argument.DataTable.Rows {
				for _, cell := range row.Cells {
					_, _ = hash.Write([]byte(cell.Value))
					_, _ = hash.Write([]byte{0})
				}
			}
		}
		_, _ = hash.Write([]byte{0xff})
	}
	return hex.EncodeToString(hash.Sum(nil))[:16]
}

func failureSignature(err error) string {
	detail := generatedUUIDPattern.ReplaceAllString(strings.TrimSpace(err.Error()), "<generated-uuid>")
	sum := sha256.Sum256([]byte(detail))
	return "sha256:" + hex.EncodeToString(sum[:])
}

func localGapID(entry RatchetEntry) string {
	sum := sha256.Sum256([]byte(ratchetEntryKey(entry)))
	return "tck-gap-" + hex.EncodeToString(sum[:])[:12]
}

func featureWorkstream(path string) string {
	directory := filepath.ToSlash(filepath.Dir(path))
	if directory == "." {
		return "corpus"
	}
	return directory
}

func isHarnessFailure(err error) bool {
	return errors.Is(err, godog.ErrUndefined) ||
		errors.Is(err, godog.ErrAmbiguous) ||
		errors.Is(err, godog.ErrPending)
}

func ratchetEntryKey(entry RatchetEntry) string {
	return strings.Join([]string{
		entry.FeaturePath,
		entry.ScenarioName,
		entry.ExampleRow,
		string(entry.TransactionMode),
		entry.RouteMode,
	}, "\x00")
}

func sortRatchetEntries(entries []RatchetEntry) {
	sort.Slice(entries, func(i, j int) bool {
		return ratchetEntryKey(entries[i]) < ratchetEntryKey(entries[j])
	})
}

func newRatchetBaseline(corpusSHA string, entries []RatchetEntry) RatchetBaseline {
	entries = append([]RatchetEntry(nil), entries...)
	sortRatchetEntries(entries)
	return RatchetBaseline{
		UpstreamRevision: UpstreamRevision,
		CorpusSHA256:     corpusSHA,
		RouteMode:        ratchetRouteNormal,
		VettedFeatures:   fullyPassingFeatures(entries),
		Entries:          entries,
	}
}

func fullyPassingFeatures(entries []RatchetEntry) []string {
	allPass := make(map[string]bool)
	modes := make(map[string]map[TransactionMode]bool)
	for _, entry := range entries {
		if _, ok := allPass[entry.FeaturePath]; !ok {
			allPass[entry.FeaturePath] = true
			modes[entry.FeaturePath] = make(map[TransactionMode]bool)
		}
		allPass[entry.FeaturePath] = allPass[entry.FeaturePath] && entry.Status == ratchetStatusPass
		modes[entry.FeaturePath][entry.TransactionMode] = true
	}
	features := make([]string, 0, len(allPass))
	for path, passes := range allPass {
		if passes && modes[path][AutocommitMode] && modes[path][ExplicitTransactionMode] {
			features = append(features, path)
		}
	}
	sort.Strings(features)
	return features
}

func loadRatchetBaseline(path string) (RatchetBaseline, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return RatchetBaseline{}, fmt.Errorf("read TCK ratchet: %w", err)
	}
	var baseline RatchetBaseline
	if err := json.Unmarshal(content, &baseline); err != nil {
		return RatchetBaseline{}, fmt.Errorf("decode TCK ratchet: %w", err)
	}
	return baseline, nil
}

func writeRatchetBaseline(path string, baseline RatchetBaseline) error {
	content, err := json.MarshalIndent(baseline, "", "  ")
	if err != nil {
		return fmt.Errorf("encode TCK ratchet: %w", err)
	}
	content = append(content, '\n')
	if err := os.WriteFile(path, content, 0o644); err != nil {
		return fmt.Errorf("write TCK ratchet: %w", err)
	}
	return nil
}

func compareRatchet(baseline RatchetBaseline, current RatchetBaseline) (RatchetReport, error) {
	var problems []string
	if baseline.UpstreamRevision != current.UpstreamRevision {
		problems = append(problems, fmt.Sprintf("upstream revision changed: baseline %s, current %s", baseline.UpstreamRevision, current.UpstreamRevision))
	}
	if baseline.CorpusSHA256 != current.CorpusSHA256 {
		problems = append(problems, fmt.Sprintf("corpus digest changed: baseline %s, current %s", baseline.CorpusSHA256, current.CorpusSHA256))
	}

	baselineByKey := make(map[string]RatchetEntry, len(baseline.Entries))
	for _, entry := range baseline.Entries {
		key := ratchetEntryKey(entry)
		if _, exists := baselineByKey[key]; exists {
			problems = append(problems, "duplicate baseline entry: "+displayRatchetEntry(entry))
		}
		baselineByKey[key] = entry
		if entry.Status == ratchetStatusExpectedGap && (entry.FailureSignature == "" || entry.GapID == "" || entry.Reason == "" || entry.Workstream == "") {
			problems = append(problems, "unclassified expected gap: "+displayRatchetEntry(entry))
		}
		if entry.Status == ratchetStatusHarness {
			problems = append(problems, "baseline contains harness error: "+displayRatchetEntry(entry))
		}
	}

	currentByKey := make(map[string]RatchetEntry, len(current.Entries))
	for _, entry := range current.Entries {
		key := ratchetEntryKey(entry)
		if _, exists := currentByKey[key]; exists {
			problems = append(problems, "duplicate current entry: "+displayRatchetEntry(entry))
		}
		currentByKey[key] = entry
	}

	for key, expected := range baselineByKey {
		actual, exists := currentByKey[key]
		if !exists {
			problems = append(problems, "missing scenario: "+displayRatchetEntry(expected))
			continue
		}
		switch {
		case expected.Status == ratchetStatusPass && actual.Status != ratchetStatusPass:
			problems = append(problems, fmt.Sprintf("passing scenario regressed to %s: %s: %s", actual.Status, displayRatchetEntry(actual), conciseFailureDetail(actual.failureDetail)))
		case expected.Status == ratchetStatusExpectedGap && actual.Status == ratchetStatusPass:
			problems = append(problems, "stale expected gap now passes; update the ratchet: "+displayRatchetEntry(actual))
		case expected.Status == ratchetStatusExpectedGap && actual.Status == ratchetStatusExpectedGap && expected.FailureSignature != actual.FailureSignature:
			problems = append(problems, "expected-gap failure signature changed: "+displayRatchetEntry(actual))
		case actual.Status == ratchetStatusHarness:
			problems = append(problems, "harness error: "+displayRatchetEntry(actual))
		}
	}
	for key, actual := range currentByKey {
		if _, exists := baselineByKey[key]; !exists {
			problems = append(problems, "scenario is absent from the ratchet: "+displayRatchetEntry(actual))
		}
	}

	report := summarizeRatchet(current.Entries)
	sort.Strings(problems)
	if len(problems) != 0 {
		return report, fmt.Errorf("TCK ratchet failed:\n- %s", strings.Join(problems, "\n- "))
	}
	return report, nil
}

func validateRatchetUpdate(baseline RatchetBaseline, current RatchetBaseline) error {
	if baseline.UpstreamRevision != current.UpstreamRevision || baseline.CorpusSHA256 != current.CorpusSHA256 {
		return errors.New("the pinned TCK provenance changed; update provenance separately before updating the ratchet")
	}
	baselineByKey := make(map[string]RatchetEntry, len(baseline.Entries))
	for _, entry := range baseline.Entries {
		baselineByKey[ratchetEntryKey(entry)] = entry
	}
	currentByKey := make(map[string]RatchetEntry, len(current.Entries))
	for _, entry := range current.Entries {
		if entry.Status == ratchetStatusHarness {
			return fmt.Errorf("refusing ratchet update with harness error: %s", displayRatchetEntry(entry))
		}
		currentByKey[ratchetEntryKey(entry)] = entry
	}
	if len(baselineByKey) != len(currentByKey) {
		return fmt.Errorf("refusing ratchet update after scenario inventory changed: baseline %d, current %d", len(baselineByKey), len(currentByKey))
	}
	for key, expected := range baselineByKey {
		actual, exists := currentByKey[key]
		if !exists {
			return fmt.Errorf("refusing ratchet update with missing scenario: %s", displayRatchetEntry(expected))
		}
		if expected.Status == ratchetStatusPass && actual.Status != ratchetStatusPass {
			return fmt.Errorf("refusing ratchet update after passing scenario regressed: %s: %s", displayRatchetEntry(actual), conciseFailureDetail(actual.failureDetail))
		}
	}
	return nil
}

func conciseFailureDetail(detail string) string {
	detail = strings.Join(strings.Fields(detail), " ")
	const limit = 500
	if len(detail) > limit {
		return detail[:limit] + "..."
	}
	return detail
}

func summarizeRatchet(entries []RatchetEntry) RatchetReport {
	report := RatchetReport{Total: len(entries)}
	for _, entry := range entries {
		switch entry.Status {
		case ratchetStatusPass:
			report.SupportedPass++
		case ratchetStatusExpectedGap:
			report.ExpectedGap++
		case ratchetStatusHarness:
			report.HarnessError++
		}
	}
	return report
}

func displayRatchetEntry(entry RatchetEntry) string {
	return fmt.Sprintf("%s :: %s :: %s :: %s :: %s", entry.FeaturePath, entry.ScenarioName, entry.ExampleRow, entry.TransactionMode, entry.RouteMode)
}
