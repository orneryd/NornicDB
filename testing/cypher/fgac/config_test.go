package fgac

import (
	"errors"
	"strings"
	"testing"
)

var comparisonEnvironment = []string{
	"NORNICDB_FGAC_EE_EVALUATION",
	"NORNICDB_FGAC_FIXTURE",
	"NORNICDB_FGAC_REFERENCES",
	"NORNICDB_FGAC_REFERENCE_VERSION",
	"NORNICDB_FGAC_SYNTHETIC_PASSWORD",
	"NORNICDB_NEO4J_REFERENCE_URI",
	"NORNICDB_NEO4J_REFERENCE_USER",
	"NORNICDB_NEO4J_REFERENCE_PASSWORD",
	"NORNICDB_NEO4J_REFERENCE_DATABASE",
	"NORNICDB_FGAC_CANDIDATE_URI",
	"NORNICDB_FGAC_CANDIDATE_USER",
	"NORNICDB_FGAC_CANDIDATE_PASSWORD",
	"NORNICDB_FGAC_CANDIDATE_DATABASE",
}

func clearComparisonEnvironment(t *testing.T) {
	t.Helper()
	for _, name := range comparisonEnvironment {
		t.Setenv(name, "")
	}
}

func completeComparisonEnvironment(t *testing.T) {
	t.Helper()
	t.Setenv("NORNICDB_FGAC_EE_EVALUATION", "1")
	t.Setenv("NORNICDB_NEO4J_REFERENCE_URI", "bolt://127.0.0.1:7687")
	t.Setenv("NORNICDB_NEO4J_REFERENCE_PASSWORD", "reference-password")
	t.Setenv("NORNICDB_FGAC_CANDIDATE_URI", "bolt://127.0.0.1:17687")
	t.Setenv("NORNICDB_FGAC_CANDIDATE_PASSWORD", "candidate-password")
}

func TestComparisonConfigRequiresExplicitEvaluationOptIn(t *testing.T) {
	clearComparisonEnvironment(t)
	completeComparisonEnvironment(t)
	t.Setenv("NORNICDB_FGAC_EE_EVALUATION", "")
	if _, err := ComparisonConfigFromEnv(); !errors.Is(err, ErrNotOptedIn) {
		t.Fatalf("ComparisonConfigFromEnv error = %v, want ErrNotOptedIn", err)
	}
}

func TestComparisonConfigRequiresEndpoints(t *testing.T) {
	clearComparisonEnvironment(t)
	t.Setenv("NORNICDB_FGAC_EE_EVALUATION", "1")
	_, err := ComparisonConfigFromEnv()
	if err == nil {
		t.Fatal("ComparisonConfigFromEnv succeeded without endpoints")
	}
	for _, want := range []string{"reference Bolt URI is empty", "candidate Bolt URI is empty"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error = %v, want substring %q", err, want)
		}
	}
}

func TestComparisonConfigDefaultsAndOverrides(t *testing.T) {
	clearComparisonEnvironment(t)
	completeComparisonEnvironment(t)
	t.Setenv("NORNICDB_NEO4J_REFERENCE_DATABASE", "refdb")
	t.Setenv("NORNICDB_FGAC_CANDIDATE_DATABASE", "candb")
	t.Setenv("NORNICDB_FGAC_REFERENCE_VERSION", "5.26.30")
	config, err := ComparisonConfigFromEnv()
	if err != nil {
		t.Fatalf("ComparisonConfigFromEnv: %v", err)
	}
	if config.Reference.User != "neo4j" || config.Candidate.User != "admin" {
		t.Errorf("default users = %q/%q", config.Reference.User, config.Candidate.User)
	}
	if config.Reference.Database != "refdb" || config.Candidate.Database != "candb" {
		t.Errorf("databases = %q/%q", config.Reference.Database, config.Candidate.Database)
	}
	if config.ReferenceVersion != "5.26.30" {
		t.Errorf("reference version = %q", config.ReferenceVersion)
	}
	if config.SyntheticPassword != "fgac-synthetic-password" {
		t.Errorf("synthetic password = %q", config.SyntheticPassword)
	}
	if config.FixturePath != "fixtures/authorization_flows.json" || config.ReferencesPath != "references.json" {
		t.Errorf("paths = %q/%q", config.FixturePath, config.ReferencesPath)
	}
}

func TestComparisonConfigRejectsUnsafeSyntheticPassword(t *testing.T) {
	clearComparisonEnvironment(t)
	completeComparisonEnvironment(t)
	t.Setenv("NORNICDB_FGAC_SYNTHETIC_PASSWORD", "short'quote")
	_, err := ComparisonConfigFromEnv()
	if err == nil || !strings.Contains(err.Error(), "synthetic password") {
		t.Fatalf("ComparisonConfigFromEnv error = %v, want synthetic password", err)
	}
}

func TestComparisonConfigValidateRejectsAmbiguousEndpoints(t *testing.T) {
	config := ComparisonConfig{
		FixturePath:       "fixtures/authorization_flows.json",
		ReferencesPath:    "references.json",
		SyntheticPassword: "fgac-synthetic-password",
		Reference: EndpointConfig{
			Name:     "reference",
			URI:      "bolt://127.0.0.1:7687",
			User:     "neo4j",
			Password: "password",
			Database: "graph",
		},
		Candidate: EndpointConfig{
			Name:     "candidate",
			URI:      "bolt://127.0.0.1:7687",
			User:     "admin",
			Password: "password",
			Database: "graph",
		},
	}
	if err := config.Validate(); err == nil || !strings.Contains(err.Error(), "distinct") {
		t.Fatalf("Validate error = %v, want distinct endpoints", err)
	}

	config.Candidate.URI = "bolt://127.0.0.1:17687"
	config.Candidate.Database = DatabaseSystem
	if err := config.Validate(); err == nil || !strings.Contains(err.Error(), "cannot be \"system\"") {
		t.Fatalf("Validate error = %v, want system database rejection", err)
	}
}

func TestComparisonConfigAcceptsTLSBoltEndpoint(t *testing.T) {
	clearComparisonEnvironment(t)
	completeComparisonEnvironment(t)
	t.Setenv("NORNICDB_FGAC_CANDIDATE_URI", "bolt+s://127.0.0.1:17687")
	if _, err := ComparisonConfigFromEnv(); err != nil {
		t.Fatal(err)
	}
}
