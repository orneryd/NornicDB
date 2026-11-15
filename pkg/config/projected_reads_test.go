package config

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

func TestProjectedReadsEnvironment(t *testing.T) {
	if expected, child := os.LookupEnv("NORNIC_TEST_EXPECT_PROJECTED_READS"); child {
		if IsProjectedReadsEnabled() != (expected == "true") {
			t.Fatal("unexpected startup flag")
		}
		return
	}
	for _, value := range []string{"", "false", "0", "true", "1"} {
		t.Run(value, func(t *testing.T) {
			command := exec.Command(os.Args[0], "-test.run=^TestProjectedReadsEnvironment$")
			for _, entry := range os.Environ() {
				if !strings.HasPrefix(entry, EnvProjectedReads+"=") && !strings.HasPrefix(entry, "NORNIC_TEST_EXPECT_PROJECTED_READS=") {
					command.Env = append(command.Env, entry)
				}
			}
			expected := "false"
			if value == "1" || value == "true" {
				expected = "true"
			}
			command.Env = append(command.Env, EnvProjectedReads+"="+value, "NORNIC_TEST_EXPECT_PROJECTED_READS="+expected)
			if output, err := command.CombinedOutput(); err != nil {
				t.Fatalf("%v: %s", err, output)
			}
		})
	}
}

func TestProjectedReadsStatusAndReset(t *testing.T) {
	previous := IsProjectedReadsEnabled()
	t.Cleanup(func() { SetProjectedReadsEnabled(previous) })
	SetProjectedReadsEnabled(true)
	if !GetFeatureStatus().ProjectedReadsEnabled {
		t.Fatal("missing feature status")
	}
	ResetFeatureFlags()
	if IsProjectedReadsEnabled() {
		t.Fatal("reset must restore default-off behavior")
	}
}
