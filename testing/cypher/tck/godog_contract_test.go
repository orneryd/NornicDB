package tck

import (
	"bytes"
	"errors"
	"testing"

	"github.com/cucumber/godog"
)

func runHarnessContract(feature string, initialize func(*godog.ScenarioContext)) (int, string) {
	var output bytes.Buffer
	suite := godog.TestSuite{
		Name:                "cypher-tck-harness-contract",
		ScenarioInitializer: initialize,
		Options: &godog.Options{
			Format:      "progress",
			NoColors:    true,
			Strict:      true,
			Concurrency: 1,
			Output:      &output,
			FeatureContents: []godog.Feature{{
				Name:     "harness-contract.feature",
				Contents: []byte(feature),
			}},
		},
	}
	return suite.Run(), output.String()
}

func TestGodogHarnessAcceptsBoundSteps(t *testing.T) {
	feature := `Feature: harness contract
  Scenario: bound steps execute
    Given an empty graph
    When executing query:
      """
      RETURN 1
      """
    Then the result should be empty
    And no side effects
`
	status, output := runHarnessContract(feature, func(ctx *godog.ScenarioContext) {
		ctx.Step(`^an empty graph$`, func() error { return nil })
		ctx.Step(`^executing query:$`, func(_ *godog.DocString) error { return nil })
		ctx.Step(`^the result should be empty$`, func() error { return nil })
		ctx.Step(`^no side effects$`, func() error { return nil })
	})
	if status != 0 {
		t.Fatalf("bound harness failed with status %d:\n%s", status, output)
	}
}

func TestGodogHarnessRejectsUnknownSteps(t *testing.T) {
	feature := `Feature: harness contract
  Scenario: unknown steps are failures
    Given an unbound conformance step
`
	status, output := runHarnessContract(feature, func(_ *godog.ScenarioContext) {})
	if status == 0 {
		t.Fatalf("strict harness accepted an unknown step:\n%s", output)
	}
}

func TestGodogHarnessPropagatesBindingErrors(t *testing.T) {
	feature := `Feature: harness contract
  Scenario: binding errors are failures
    Given an empty graph
`
	status, output := runHarnessContract(feature, func(ctx *godog.ScenarioContext) {
		ctx.Step(`^an empty graph$`, func() error { return errors.New("negative control") })
	})
	if status == 0 {
		t.Fatalf("strict harness accepted a binding error:\n%s", output)
	}
}
