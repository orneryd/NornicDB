package capturecensus

// Author: storage-format-owners lane (Claude), #1693, 30 October 2026.

import (
	"fmt"
	"sort"
	"strings"
)

// Acceptance is one reason the census did not fail a finding, recorded by the
// one recorder (acceptanceLog.accept). Every path that turns a would-be
// violation into a non-violation goes through it, so the census can list and
// count all of them; Count is set for aggregated proof and convention rules.
type Acceptance struct {
	Kind, Key, Reason, Approver string
	Count                       int
}

// Acceptance kinds. Contract A (no nil or substituted context reaches a
// function that reads one) forbids the first two outright.
const (
	// KindNilDisposition is a reviewed claim that a flagged nil cannot occur.
	KindNilDisposition = "nil-disposition"
	// KindRecordedOwner is a substitution waived because another change owns
	// it. No policy produces it any more; it stays forbidden.
	KindRecordedOwner = "recorded-owner"
	// KindCrossUnitOwner is a substitution still present in this tree that a
	// named merge unit fixes on its own lifetime owner; listed and counted,
	// never silent (approver, unit and fixing change are recorded).
	KindCrossUnitOwner = "cross-unit-owner"
	// KindAPIBoundary is a fresh context created in an exported function that
	// takes no context, which no request holder in the module reaches.
	KindAPIBoundary = "api-boundary"
	// KindNoRequestChain is a fresh context created where no caller chain in
	// the module reaches a function holding a request.
	KindNoRequestChain = "no-request-chain"
	// KindUnfulfilled is a substitution whose request holder reaches it only
	// across the public API (a caller in another package, or dynamic dispatch
	// into an exported method). Contract A is NOT met there: the census lists
	// and counts it against the tracked deferral, and it is never a pass.
	KindUnfulfilled = "unfulfilled-contract-a"
	// KindOwnedSource is a named context owned by work outliving any request.
	KindOwnedSource = "owned-source"
	// KindOutsideBoundary is a nil context passed to code that reaches no
	// context reader.
	KindOutsideBoundary = "outside-boundary"
	// KindReviewedHeld is a fresh context created where the function holds
	// contexts the census does not exclude (held.go) and a reviewed record,
	// pinned to those exact held values and to the source of every function
	// the review relied on, says why none is a request. Listed and counted,
	// never a pass; any change to what it is pinned to fails it as stale.
	KindReviewedHeld = "reviewed-held-value"
	// KindReviewedHolder is a caller on a walk whose held contexts are exactly
	// those of its own reviewed record, still current: listed and counted per
	// walk, walked past, never a pass.
	KindReviewedHolder = "reviewed-holder"
	// KindNilDetached is a nil context on a merged path whose convention is
	// that nil means detached, at a pinned use, reaching readers reviewed to
	// tolerate it. Listed and counted by root, never cleared.
	KindNilDetached = "nil-detached"
	// KindTrackedDefect is a real defect filed for Linear, listed until fixed.
	KindTrackedDefect = "tracked-defect"
	// KindDistinctDecision is a reviewed storage read responsibility.
	KindDistinctDecision = "distinct-decision"
	// KindProof is a rule of the nil analysis that proved a value non-nil.
	KindProof = "proof"
	// KindConvention is a model convention the nil analysis trusted.
	KindConvention = "model-convention"
)

// ForbiddenAcceptanceKinds are the kinds Contract A does not allow at all.
var ForbiddenAcceptanceKinds = []string{KindNilDisposition, KindRecordedOwner}

// acceptanceLog is the census's one acceptance recorder.
type acceptanceLog struct {
	list  []Acceptance
	rules map[string]int // proof and convention rules, counted
}

// accept records one acceptance. When site is not nil, its Reason states the
// kind and reason; no other code sets a site's reason.
func (a *acceptanceLog) accept(kind, key, reason, approver string, site *ContextSite) {
	a.list = append(a.list, Acceptance{Kind: kind, Key: key, Reason: reason, Approver: approver, Count: 1})
	if site != nil {
		site.Reason = kind + ": " + reason
	}
}

// discharge counts one use of a nil-analysis rule ("proof: ..." or
// "convention: ...") that explained a non-nil verdict.
func (a *acceptanceLog) discharge(rule string) {
	if a.rules == nil {
		a.rules = map[string]int{}
	}
	a.rules[rule]++
}

// all returns every acceptance, with the counted rules folded in, sorted.
func (a *acceptanceLog) all() []Acceptance {
	out := append([]Acceptance(nil), a.list...)
	for rule, n := range a.rules {
		kind := KindProof
		if strings.HasPrefix(rule, "convention:") {
			kind = KindConvention
		}
		out = append(out, Acceptance{Kind: kind, Key: rule, Reason: "explained a non-nil verdict", Approver: "nil analysis", Count: n})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Kind != out[j].Kind {
			return out[i].Kind < out[j].Kind
		}
		return out[i].Key < out[j].Key
	})
	return out
}

// keys returns the keys of one kind, sorted.
func (a *acceptanceLog) keys(kind string) []string {
	var out []string
	for _, entry := range a.list {
		if entry.Kind == kind {
			out = append(out, entry.Key)
		}
	}
	sort.Strings(out)
	return out
}

// AcceptanceCounts sums the acceptances by kind.
func AcceptanceCounts(list []Acceptance) map[string]int {
	counts := map[string]int{}
	for _, entry := range list {
		counts[entry.Kind] += entry.Count
	}
	return counts
}

// String renders one acceptance for the census log.
func (a Acceptance) String() string {
	return fmt.Sprintf("%s x%d %s [%s; approver %s]", a.Kind, a.Count, a.Key, a.Reason, a.Approver)
}
