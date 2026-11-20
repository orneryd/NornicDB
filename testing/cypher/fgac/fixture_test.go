package fgac

import (
	"os"
	"strings"
	"testing"
)

const (
	fixturePath    = "fixtures/authorization_flows.json"
	referencesPath = "references.json"
)

func TestAuthorizationFixtureLoadsAndCoversDocumentedFlows(t *testing.T) {
	fixture, err := LoadFixture(fixturePath)
	if err != nil {
		t.Fatalf("load fixture: %v", err)
	}
	if fixture.Name != "nor12-fgac-authorization-flows" {
		t.Errorf("fixture name = %q", fixture.Name)
	}
	if !strings.Contains(fixture.Purpose, "no exploit") {
		t.Errorf("fixture purpose must state that it contains no vulnerability probe: %q", fixture.Purpose)
	}
	coverage := make(map[string]bool)
	for _, testCase := range fixture.Cases {
		for _, covered := range testCase.Covers {
			coverage[covered] = true
		}
	}
	for _, required := range RequiredCoverage {
		if !coverage[required] {
			t.Errorf("fixture does not cover %q", required)
		}
	}
	if len(fixture.Cases) != 16 {
		t.Errorf("fixture has %d cases, want 16", len(fixture.Cases))
	}
	for _, testCase := range fixture.Cases {
		if _, exists := fixture.Principals[testCase.Principal]; !exists {
			t.Errorf("case %q references unknown principal %q", testCase.ID, testCase.Principal)
		}
	}
}

func TestAuthorizationFixtureExtendsOnlyThePinnedCurrentRelease(t *testing.T) {
	base, err := LoadFixture("fixtures/authorization_flows.json")
	if err != nil {
		t.Fatal(err)
	}
	lts, err := base.ForReference("5.26.30")
	if err != nil || len(lts.Cases) != 16 {
		t.Fatalf("LTS fixture selected the wrong cases: %v", err)
	}
	current, err := base.ForReference("2026.10.2")
	if err != nil {
		t.Fatal(err)
	}
	if len(current.Cases) != 18 || current.Principals["ruled"].User != "fgac_ruled" {
		t.Fatal("current fixture did not include the authorization-rule cases")
	}
	if _, exists := base.Principals["ruled"]; exists {
		t.Fatal("reference selection changed the common fixture")
	}
}

func TestLoadFixtureRejectsUnknownFields(t *testing.T) {
	path := writeFixtureFile(t, `{"name":"x","purpose":"y","principals":{},"setup":{},"teardown":{},"cases":[],"extra":true}`)
	if _, err := LoadFixture(path); err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("LoadFixture error = %v, want unknown field", err)
	}
}

func TestLoadFixtureRejectsTrailingJSON(t *testing.T) {
	path := writeFixtureFile(t, `{"name":"x","purpose":"y","principals":{},"setup":{},"teardown":{},"cases":[]}{}`)
	if _, err := LoadFixture(path); err == nil || !strings.Contains(err.Error(), "trailing JSON") {
		t.Fatalf("LoadFixture error = %v, want trailing JSON", err)
	}
}

func TestFixtureValidation(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Fixture)
		want   string
	}{
		{
			name: "duplicate case id",
			mutate: func(fixture *Fixture) {
				fixture.Cases = append(fixture.Cases, fixture.Cases[0])
			},
			want: "duplicate id",
		},
		{
			name: "unknown principal",
			mutate: func(fixture *Fixture) {
				fixture.Cases[0].Principal = "ghost"
			},
			want: "unknown principal",
		},
		{
			name: "allow without rows",
			mutate: func(fixture *Fixture) {
				fixture.Cases[0].Expect.Rows = nil
			},
			want: "allow expectation must name rows",
		},
		{
			name: "deny with rows",
			mutate: func(fixture *Fixture) {
				fixture.Cases[0].Expect = Expectation{Kind: "deny", Code: "Neo.ClientError.Security.Forbidden", Rows: [][]any{{}}}
			},
			want: "deny expectation must not name rows",
		},
		{
			name: "unsafe graph teardown",
			mutate: func(fixture *Fixture) {
				fixture.Teardown.Graph = []string{"MATCH (n) DETACH DELETE n"}
			},
			want: "scope its delete",
		},
		{
			name: "teardown without detach",
			mutate: func(fixture *Fixture) {
				fixture.Teardown.Graph = []string{"MATCH (n) WHERE n:FGACDoc OR n:FGACOther DELETE n"}
			},
			want: "DETACH DELETE",
		},
		{
			name: "teardown that is not a drop",
			mutate: func(fixture *Fixture) {
				fixture.Teardown.Administration = []string{"CREATE USER fgac_extra IF NOT EXISTS SET PASSWORD 'x' CHANGE NOT REQUIRED"}
			},
			want: "must be a DROP",
		},
		{
			name: "non synthetic user",
			mutate: func(fixture *Fixture) {
				fixture.Principals["user"] = Principal{User: "someone"}
			},
			want: "must match",
		},
		{
			name: "unsupported placeholder",
			mutate: func(fixture *Fixture) {
				fixture.Cases[0].Statements = []string{"RETURN '${secret}' AS value"}
			},
			want: "unsupported placeholder",
		},
		{
			name: "missing coverage",
			mutate: func(fixture *Fixture) {
				fixture.Cases[0].Covers = []string{"role-grant"}
			},
			want: "do not cover",
		},
		{
			name: "invalid database",
			mutate: func(fixture *Fixture) {
				fixture.Cases[0].Database = "elsewhere"
			},
			want: "must be",
		},
		{
			name: "unknown coverage name",
			mutate: func(fixture *Fixture) {
				fixture.Cases[0].Covers = append(fixture.Cases[0].Covers, "not-a-flow")
			},
			want: "unknown coverage",
		},
		{
			name: "unsafe role drop",
			mutate: func(fixture *Fixture) {
				fixture.Teardown.Administration = []string{"DROP ROLE admin IF EXISTS"}
			},
			want: "non-synthetic name",
		},
		{
			name: "deny code outside neo4j",
			mutate: func(fixture *Fixture) {
				fixture.Cases[0].Expect = Expectation{Kind: "deny", Code: "Forbidden"}
			},
			want: "not a Neo4j client error",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := validFixtureForTest()
			test.mutate(&fixture)
			err := fixture.Validate()
			if err == nil {
				t.Fatal("Validate succeeded, want an error")
			}
			if !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Validate error = %v, want substring %q", err, test.want)
			}
		})
	}
}

func TestValidFixturePassesValidation(t *testing.T) {
	fixture := validFixtureForTest()
	if err := fixture.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
}

func TestSubstitute(t *testing.T) {
	got, err := Substitute("GRANT ACCESS ON DATABASE ${database} TO fgac_reader", "neo4j", "unused")
	if err != nil {
		t.Fatalf("Substitute: %v", err)
	}
	if got != "GRANT ACCESS ON DATABASE neo4j TO fgac_reader" {
		t.Fatalf("Substitute = %q", got)
	}
	if _, err := Substitute("RETURN '${unknown}'", "neo4j", "unused"); err == nil {
		t.Fatal("Substitute accepted an unknown placeholder")
	}
}

func validFixtureForTest() Fixture {
	covers := append([]string(nil), RequiredCoverage...)
	return Fixture{
		Name:    "fixture",
		Purpose: "normative",
		Principals: map[string]Principal{
			"admin": {Admin: true},
			"user":  {User: "fgac_user"},
		},
		Setup: Phase{
			Graph:          []string{"CREATE (:FGACDoc {id: 'a'})"},
			Administration: []string{"CREATE USER fgac_user IF NOT EXISTS SET PASSWORD '${password}' CHANGE NOT REQUIRED"},
		},
		Teardown: Phase{
			Graph:          []string{"MATCH (n) WHERE n:FGACDoc OR n:FGACOther DETACH DELETE n"},
			Administration: []string{"DROP USER fgac_user IF EXISTS"},
		},
		Cases: []Case{
			{
				ID:          "case-one",
				Covers:      covers,
				Description: "one documented case",
				Database:    DatabaseTarget,
				Principal:   "user",
				Statements:  []string{"RETURN 1 AS ok"},
				Expect:      Expectation{Kind: "allow", Rows: [][]any{{int64(1)}}},
			},
		},
	}
}

func writeFixtureFile(t *testing.T, content string) string {
	t.Helper()
	path := t.TempDir() + "/fixture.json"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	return path
}
