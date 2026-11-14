package config

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

func TestExperimentalPropertyStorageEnvironment(t *testing.T) {
	if expected, child := os.LookupEnv("NORNIC_TEST_EXPECT_PROPERTY_STORAGE"); child {
		if IsExperimentalPropertyStorageEnabled() != (expected == "true") {
			t.Fatal("unexpected startup flag")
		}
		return
	}
	for _, value := range []string{"", "true", "1", "false", "0"} {
		t.Run(value, func(t *testing.T) {
			command := exec.Command(os.Args[0], "-test.run=^TestExperimentalPropertyStorageEnvironment$")
			for _, entry := range os.Environ() {
				if !strings.HasPrefix(entry, EnvExperimentalPropertyStorage+"=") && !strings.HasPrefix(entry, "NORNIC_TEST_EXPECT_PROPERTY_STORAGE=") {
					command.Env = append(command.Env, entry)
				}
			}
			expected := "false"
			if value == "true" || value == "1" {
				expected = "true"
			}
			command.Env = append(command.Env, EnvExperimentalPropertyStorage+"="+value, "NORNIC_TEST_EXPECT_PROPERTY_STORAGE="+expected)
			if output, err := command.CombinedOutput(); err != nil {
				t.Fatalf("%v: %s", err, output)
			}
		})
	}
}
