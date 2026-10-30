package tck

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// issueLedgerEntry mirrors testing/cypher/tck/testdata/issues.json.
type issueLedgerEntry struct {
	Number             int      `json:"number"`
	URL                string   `json:"url"`
	Title              string   `json:"title"`
	FixStep            any      `json:"fix_step"`
	CandidateTCKFamily []string `json:"candidate_tck_family"`
	TckMapping         struct {
		Status    string   `json:"status"`
		Scenarios []string `json:"scenarios"`
		Note      string   `json:"note"`
	} `json:"tck_mapping"`
	LocalReproductions []string `json:"local_reproductions"`
	Status             string   `json:"status"`
}

type issueLedger struct {
	Repository string             `json:"repository"`
	MatrixDoc  string             `json:"matrix_doc"`
	Issues     []issueLedgerEntry `json:"issues"`
}

func loadIssueLedger(t *testing.T) issueLedger {
	t.Helper()
	content, err := os.ReadFile(filepath.Join("testdata", "issues.json"))
	require.NoError(t, err)
	var ledger issueLedger
	require.NoError(t, json.Unmarshal(content, &ledger))
	return ledger
}

// repoRoot returns the repository root relative to this package
// (testing/cypher/tck -> repository root).
func repoRoot() string {
	return filepath.Join("..", "..", "..")
}

// TestIssuesLedger_ScopedMatrixCompleteness verifies the ledger contains exactly
// the 35 scoped issues from docs/plans/cypher-convergence-issues.md
// (#447-#482 minus #472/#473, plus #487) with unique numbers and URLs.
func TestIssuesLedger_ScopedMatrixCompleteness(t *testing.T) {
	ledger := loadIssueLedger(t)
	require.Len(t, ledger.Issues, 35)

	expected := make(map[int]bool)
	for number := 447; number <= 482; number++ {
		if number == 472 || number == 473 {
			continue
		}
		expected[number] = true
	}
	expected[487] = true

	seen := make(map[int]bool)
	for _, issue := range ledger.Issues {
		require.Falsef(t, seen[issue.Number], "duplicate issue number %d", issue.Number)
		seen[issue.Number] = true
		require.Truef(t, expected[issue.Number], "issue %d is outside the scoped matrix", issue.Number)
		require.Truef(t, strings.HasPrefix(issue.URL, "https://github.com/orneryd/NornicDB/issues/"), "issue %d has no canonical URL", issue.Number)
		require.Contains(t, []string{"exact", "family", "local-only", "program"}, issue.TckMapping.Status, "issue %d has invalid tck mapping status", issue.Number)
	}
	require.Len(t, seen, 35, "expected exactly the 35 scoped issues")
}

// TestIssuesLedger_ReproductionsExist verifies every local reproduction ID names a
// real test function in pkg/cypher or pkg/storage test files, and every verified
// issue has at least one reproduction.
func TestIssuesLedger_ReproductionsExist(t *testing.T) {
	ledger := loadIssueLedger(t)
	root := repoRoot()

	reproductions := make(map[string]bool)
	for _, dir := range []string{
		filepath.Join(root, "pkg", "cypher"),
		filepath.Join(root, "pkg", "storage"),
	} {
		entries, err := os.ReadDir(dir)
		require.NoError(t, err)
		for _, entry := range entries {
			if entry.IsDir() || !strings.HasSuffix(entry.Name(), "_test.go") {
				continue
			}
			content, err := os.ReadFile(filepath.Join(dir, entry.Name()))
			require.NoError(t, err)
			text := string(content)
			for _, id := range ledgerReproductionIDs(ledger) {
				if strings.Contains(text, "func "+id+"(") {
					reproductions[id] = true
				}
			}
		}
	}

	for _, issue := range ledger.Issues {
		if issue.Status == "verified" {
			require.NotEmptyf(t, issue.LocalReproductions, "verified issue %d has no local reproductions", issue.Number)
		}
		for _, id := range issue.LocalReproductions {
			require.Truef(t, reproductions[id], "reproduction %s (issue %d) not found in pkg/cypher or pkg/storage tests", id, issue.Number)
		}
	}
}

// TestIssuesLedger_ExactTckReferencesExist verifies every scenario cited in the
// ledger exists in the vendored corpus with its scenario title, and that exact
// mappings cite at least one scenario while local-only mappings cite none.
func TestIssuesLedger_ExactTckReferencesExist(t *testing.T) {
	ledger := loadIssueLedger(t)
	root := filepath.Join("testdata", "opencypher", "features")

	for _, issue := range ledger.Issues {
		switch issue.TckMapping.Status {
		case "exact":
			require.NotEmptyf(t, issue.TckMapping.Scenarios, "issue %d claims exact mapping but cites no scenarios", issue.Number)
		case "local-only", "program":
			require.Emptyf(t, issue.TckMapping.Scenarios, "issue %d cites scenarios without a scenario mapping", issue.Number)
		}
		for _, reference := range issue.TckMapping.Scenarios {
			parts := strings.SplitN(reference, " :: ", 2)
			require.Lenf(t, parts, 2, "issue %d scenario reference %q must be '<feature> :: <title>'", issue.Number, reference)
			content, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(parts[0])))
			require.NoErrorf(t, err, "issue %d references missing feature %s", issue.Number, parts[0])
			require.Containsf(t, string(content), parts[1], "issue %d scenario title %q not found in %s", issue.Number, parts[1], parts[0])
		}
	}
}

func ledgerReproductionIDs(ledger issueLedger) []string {
	set := make(map[string]bool)
	for _, issue := range ledger.Issues {
		for _, id := range issue.LocalReproductions {
			set[id] = true
		}
	}
	ids := make([]string, 0, len(set))
	for id := range set {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func TestIssuesLedger_EndpointPropertyAccessHasPassingTckCoverage(t *testing.T) {
	const (
		issueNumber  = 465
		featurePath  = "clauses/merge/Merge5.feature"
		scenarioName = "[11] Use outgoing direction when unspecified"
	)

	ledger := loadIssueLedger(t)
	var endpointIssue *issueLedgerEntry
	for i := range ledger.Issues {
		if ledger.Issues[i].Number == issueNumber {
			endpointIssue = &ledger.Issues[i]
			break
		}
	}
	require.NotNil(t, endpointIssue)
	require.Equal(t, "exact", endpointIssue.TckMapping.Status)
	require.Equal(t, []string{featurePath + " :: " + scenarioName}, endpointIssue.TckMapping.Scenarios)

	feature, err := os.ReadFile(filepath.Join("testdata", "opencypher", "features", filepath.FromSlash(featurePath)))
	require.NoError(t, err)
	require.Contains(t, string(feature), "RETURN startNode(r).id AS s, endNode(r).id AS e")

	content, err := os.ReadFile(filepath.Join("testdata", "ratchet.json"))
	require.NoError(t, err)
	var ratchet RatchetBaseline
	require.NoError(t, json.Unmarshal(content, &ratchet))

	modes := make(map[string]bool, 2)
	for _, entry := range ratchet.Entries {
		if entry.FeaturePath != featurePath || entry.ScenarioName != scenarioName || entry.ExampleRow != "single" || entry.RouteMode != "normal" {
			continue
		}
		require.Equalf(t, "pass", entry.Status, "#%d %s in %s", issueNumber, scenarioName, entry.TransactionMode)
		modes[string(entry.TransactionMode)] = true
	}
	require.Equal(t, map[string]bool{"autocommit": true, "explicit-transaction": true}, modes)
}
