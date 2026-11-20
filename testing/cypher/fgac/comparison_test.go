package fgac

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

// TestFGACComparison runs the documented fixture against a pinned Neo4j
// Enterprise reference and a NornicDB candidate. It is skipped unless
// NORNICDB_FGAC_EE_EVALUATION=1 and both endpoints are configured.
func TestFGACComparison(t *testing.T) {
	config, err := ComparisonConfigFromEnv()
	if errors.Is(err, ErrNotOptedIn) {
		t.Skip(err)
	}
	if err != nil {
		t.Fatalf("comparison configuration: %v", err)
	}

	fixture, err := LoadFixture(config.FixturePath)
	if err != nil {
		t.Fatalf("fixture: %v", err)
	}
	pins, err := LoadReferencePins(config.ReferencesPath)
	if err != nil {
		t.Fatalf("reference pins: %v", err)
	}
	if _, err := pins.Resolve(config.ReferenceVersion); err != nil {
		t.Fatalf("reference pins: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	reference, err := newEndpointRunner(ctx, config.Reference, fixture, config.SyntheticPassword)
	if err != nil {
		t.Fatalf("reference: %v", err)
	}
	candidate, err := newEndpointRunner(ctx, config.Candidate, fixture, config.SyntheticPassword)
	if err != nil {
		reference.close(context.Background())
		t.Fatalf("candidate: %v", err)
	}

	closeContext, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(closeCancel)
	t.Cleanup(func() { reference.close(closeContext) })
	t.Cleanup(func() { candidate.close(closeContext) })

	referenceInfo, err := reference.components(ctx)
	if err != nil {
		t.Fatalf("reference identity: %v", err)
	}
	referencePin, err := VerifyReferenceIdentity(referenceInfo, pins, config.ReferenceVersion)
	if err != nil {
		t.Fatalf("reference identity: %v", err)
	}
	t.Logf("reference: %s (%s)", referenceInfo, referencePin.Ref())

	fixture, err = fixture.ForReference(referencePin.Version)
	if err != nil {
		t.Fatalf("reference fixture: %v", err)
	}
	candidate.fixture, reference.fixture = fixture, fixture
	// Registered after the driver cleanups so it runs before they close the
	// drivers, including when a later setup or case fails.
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		if err := candidate.teardown(cleanupCtx); err != nil {
			t.Logf("candidate teardown: %v", err)
		}
		if err := reference.teardown(cleanupCtx); err != nil {
			t.Logf("reference teardown: %v", err)
		}
	})

	// Candidate identity is recorded, not asserted: the comparison itself is
	// the compatibility check.
	if candidateInfo, err := candidate.components(ctx); err != nil {
		t.Logf("candidate identity unavailable: %v", err)
	} else {
		t.Logf("candidate: %s (%s database %s)", candidateInfo, config.Candidate.URI, config.Candidate.Database)
	}

	if err := reference.prepare(ctx); err != nil {
		t.Fatalf("reference setup: %v", err)
	}
	if err := candidate.prepare(ctx); err != nil {
		t.Fatalf("candidate setup: %v", err)
	}

	for _, testCase := range fixture.Cases {
		testCase := testCase
		t.Run(testCase.ID, func(t *testing.T) {
			caseCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
			defer cancel()

			referenceOutcome, err := reference.executeCase(caseCtx, testCase)
			if err != nil {
				t.Fatalf("reference execute: %v", err)
			}
			if err := CheckExpectation(testCase.Expect, referenceOutcome); err != nil {
				t.Fatalf("reference does not satisfy the documented expectation: %v", err)
			}

			candidateOutcome, err := candidate.executeCase(caseCtx, testCase)
			if err != nil {
				t.Errorf("candidate execute: %v", err)
				return
			}
			t.Logf("reference %s; candidate %s", describeOutcome(referenceOutcome), describeOutcome(candidateOutcome))
			if err := CheckExpectation(testCase.Expect, candidateOutcome); err != nil {
				t.Errorf("candidate does not satisfy the documented expectation: %v", err)
			}
			if err := CompareOutcomes(referenceOutcome, candidateOutcome); err != nil {
				t.Errorf("candidate differs from reference: %v", err)
			}
		})
	}
}

func describeOutcome(outcome Outcome) string {
	if outcome.Denied() {
		return "denied " + outcome.Code
	}
	return fmt.Sprintf("rows %v", outcome.Rows)
}
