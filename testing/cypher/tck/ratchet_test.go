package tck

import (
	"errors"
	"strings"
	"testing"

	"github.com/cucumber/godog"
	messages "github.com/cucumber/messages/go/v21"
)

func TestScenarioExampleRowUsesExpandedBehaviorInsteadOfParserIDs(t *testing.T) {
	left := &godog.Scenario{
		AstNodeIds: []string{"unstable-scenario-id", "unstable-row-id"},
		Steps:      []*messages.PickleStep{{Text: "executing RETURN 1"}},
	}
	right := &godog.Scenario{
		AstNodeIds: []string{"different-scenario-id", "different-row-id"},
		Steps:      []*messages.PickleStep{{Text: "executing RETURN 1"}},
	}
	different := &godog.Scenario{
		AstNodeIds: []string{"unstable-scenario-id", "another-row-id"},
		Steps:      []*messages.PickleStep{{Text: "executing RETURN 2"}},
	}

	if scenarioExampleRow(left) != scenarioExampleRow(right) {
		t.Fatal("example identity changed with parser-generated IDs")
	}
	if scenarioExampleRow(left) == scenarioExampleRow(different) {
		t.Fatal("different expanded example behavior received the same identity")
	}
}

func TestCompareRatchetRejectsPassingScenarioRegression(t *testing.T) {
	passing := ratchetTestEntry(ratchetStatusPass)
	baseline := newRatchetBaseline("corpus", []RatchetEntry{passing})
	regressed := passing
	regressed.Status = ratchetStatusExpectedGap
	regressed.FailureSignature = failureSignature(errors.New("wrong result"))
	regressed.GapID = "local-gap"
	regressed.Reason = "result differs"
	regressed.Workstream = "clauses/return"
	current := newRatchetBaseline("corpus", []RatchetEntry{regressed})

	_, err := compareRatchet(baseline, current)
	if err == nil || !strings.Contains(err.Error(), "passing scenario regressed") {
		t.Fatalf("compare error = %v, want passing scenario regression", err)
	}
}

func TestFailureSignatureIgnoresGeneratedUUIDsButRetainsBehavior(t *testing.T) {
	left := errors.New(`rows differ: got {"_edgeId":"e83bd11e-e46d-43c5-a38d-0d867bd77179","num":1}`)
	right := errors.New(`rows differ: got {"_edgeId":"4e1cf4b6-ed63-4abe-84a2-bc9528058e94","num":1}`)
	different := errors.New(`rows differ: got {"_edgeId":"4e1cf4b6-ed63-4abe-84a2-bc9528058e94","num":2}`)

	if failureSignature(left) != failureSignature(right) {
		t.Fatal("generated UUID changed the behavioral failure signature")
	}
	if failureSignature(left) == failureSignature(different) {
		t.Fatal("behavioral result change retained the same failure signature")
	}
}

func TestCompareRatchetRejectsMissingAndUntrackedScenarios(t *testing.T) {
	baselineEntry := ratchetTestEntry(ratchetStatusPass)
	currentEntry := baselineEntry
	currentEntry.ScenarioName = "new scenario"
	baseline := newRatchetBaseline("corpus", []RatchetEntry{baselineEntry})
	current := newRatchetBaseline("corpus", []RatchetEntry{currentEntry})

	_, err := compareRatchet(baseline, current)
	if err == nil || !strings.Contains(err.Error(), "missing scenario") || !strings.Contains(err.Error(), "absent from the ratchet") {
		t.Fatalf("compare error = %v, want missing and untracked scenario failures", err)
	}
}

func TestCompareRatchetRequiresNewPassesAndChangedFailuresToBeReviewed(t *testing.T) {
	gap := ratchetTestEntry(ratchetStatusExpectedGap)
	gap.FailureSignature = failureSignature(errors.New("old failure"))
	gap.GapID = "local-gap"
	gap.Reason = "known result mismatch"
	gap.Workstream = "clauses/return"

	t.Run("new pass", func(t *testing.T) {
		passing := gap
		passing.Status = ratchetStatusPass
		passing.FailureSignature = ""
		passing.GapID = ""
		passing.Reason = ""
		passing.Workstream = ""
		_, err := compareRatchet(newRatchetBaseline("corpus", []RatchetEntry{gap}), newRatchetBaseline("corpus", []RatchetEntry{passing}))
		if err == nil || !strings.Contains(err.Error(), "stale expected gap now passes") {
			t.Fatalf("compare error = %v, want stale expected gap", err)
		}
	})

	t.Run("changed failure", func(t *testing.T) {
		changed := gap
		changed.FailureSignature = failureSignature(errors.New("new failure"))
		_, err := compareRatchet(newRatchetBaseline("corpus", []RatchetEntry{gap}), newRatchetBaseline("corpus", []RatchetEntry{changed}))
		if err == nil || !strings.Contains(err.Error(), "failure signature changed") {
			t.Fatalf("compare error = %v, want changed failure signature", err)
		}
	})
}

func TestFullyPassingFeaturesRequiresBothTransactionModes(t *testing.T) {
	auto := ratchetTestEntry(ratchetStatusPass)
	explicit := auto
	explicit.TransactionMode = ExplicitTransactionMode
	other := auto
	other.FeaturePath = "clauses/set/Set1.feature"

	features := fullyPassingFeatures([]RatchetEntry{auto, explicit, other})
	if len(features) != 1 || features[0] != auto.FeaturePath {
		t.Fatalf("fully passing features = %v, want [%s]", features, auto.FeaturePath)
	}
}

func TestValidateRatchetUpdateNeverAcceptsARegression(t *testing.T) {
	passing := ratchetTestEntry(ratchetStatusPass)
	baseline := newRatchetBaseline("corpus", []RatchetEntry{passing})
	regressed := passing
	regressed.Status = ratchetStatusExpectedGap
	regressed.FailureSignature = failureSignature(errors.New("regression"))
	current := newRatchetBaseline("corpus", []RatchetEntry{regressed})

	err := validateRatchetUpdate(baseline, current)
	if err == nil || !strings.Contains(err.Error(), "passing scenario regressed") {
		t.Fatalf("update validation error = %v, want passing scenario regression", err)
	}
}

func ratchetTestEntry(status string) RatchetEntry {
	return RatchetEntry{
		FeaturePath:     "clauses/return/Return1.feature",
		ScenarioName:    "return a literal",
		ExampleRow:      "single",
		TransactionMode: AutocommitMode,
		RouteMode:       ratchetRouteNormal,
		Status:          status,
	}
}
