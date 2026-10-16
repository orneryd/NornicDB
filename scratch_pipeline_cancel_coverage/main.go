package main

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
)

func main() {
	head, err := exec.Command("git", "rev-parse", "HEAD").Output()
	if err != nil {
		panic(err)
	}
	os.Setenv("PIPELINE_SOURCE_HEAD", strings.TrimSpace(string(head))+"+captured-overlay")
	fmt.Printf("COMPONENT_COVERAGE parent=%s overlay=immutable-job-payload parser=%s base=fb8e38ece466f909cf588eeff2e04ad7a2bb6d8e componentbase=3b83c86e7932a24ed6ed3059d25481d454722514\n", head, os.Getenv("NORNICDB_PARSER"))
	profile := "/tmp/pipeline-cancel-combined.cov"
	args := []string{"test", "./pkg/cypher", "-run", os.Args[1], "-count=1", "-v", "-coverprofile=" + profile, "-covermode=count", "-timeout=15m"}
	command := exec.Command("go", args...)
	command.Stdout, command.Stderr = os.Stdout, os.Stderr
	testErr := command.Run()
	failed := testErr != nil
	for _, diff := range []string{"scratch_pipeline_cancel_coverage/changed.diff", "scratch_pipeline_cancel_coverage/component.diff"} {
		if err := reportCoverage(profile, diff, "component"); err != nil {
			fmt.Printf("AST_COVERAGE_FAILURE diff=%s: %v\n", diff, err)
			failed = true
		}
	}
	if failed {
		fmt.Printf("FOCUSED_FAILURE tests=%v\n", testErr)
		os.Exit(1)
	}
}
