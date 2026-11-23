package fgac

import (
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"os"
	"regexp"
	"slices"
	"strings"

	"github.com/orneryd/nornicdb/pkg/convert"
)

const (
	// DatabaseSystem selects the Neo4j system database for administration.
	DatabaseSystem = "system"
	// DatabaseTarget selects the endpoint's target graph database.
	DatabaseTarget = "target"

	databasePlaceholder = "${database}"
	passwordPlaceholder = "${password}"
)

// Session shapes a case may request. The zero value of SessionSpec keeps the
// original coverage: one explicit managed transaction on the endpoint's
// configured target database over the endpoint's direct Bolt URI.
const (
	// SessionTransactionExplicit runs a case inside one explicit transaction.
	SessionTransactionExplicit = "explicit"
	// SessionTransactionAutocommit runs each statement as an auto-commit
	// statement on one reused session.
	SessionTransactionAutocommit = "autocommit"
	// SessionDatabaseExplicit names the case's database on the session.
	SessionDatabaseExplicit = "explicit"
	// SessionDatabaseOmitted leaves database selection to the endpoint.
	SessionDatabaseOmitted = "omitted"
	// SessionRoutingDirect connects with the endpoint's direct Bolt URI.
	SessionRoutingDirect = "direct"
	// SessionRoutingRouted connects with the endpoint's routing URI.
	SessionRoutingRouted = "routed"
)

// RequiredCoverage lists the documented flows every fixture must exercise.
var RequiredCoverage = []string{
	"property-rule",
	"create-then-set",
	"create-then-set-side-effect",
	"role-grant",
	"access-grant",
	"access-deny",
	"traverse-property-read",
	"cross-role-deny",
	"introspection",
	"scoped-write-deny",
	"scoped-write-deny-side-effect",
	"transaction-rollback",
	"transaction-rollback-side-effect",
}

var (
	syntheticUserName = regexp.MustCompile(`^fgac_[a-z0-9_]+$`)
	caseIDPattern     = regexp.MustCompile(`^[a-z0-9]+(?:-[a-z0-9]+)*$`)
)

// Fixture is the synthetic graph, administration plan and documented cases
// shared by both endpoints.
type Fixture struct {
	Name       string                      `json:"name"`
	Purpose    string                      `json:"purpose"`
	Principals map[string]Principal        `json:"principals"`
	Setup      Phase                       `json:"setup"`
	Teardown   Phase                       `json:"teardown"`
	Cases      []Case                      `json:"cases"`
	References map[string]FixtureExtension `json:"referenceExtensions,omitempty"`
}

// FixtureExtension adds version-specific cases to the common compatibility contract.
type FixtureExtension struct {
	Principals map[string]Principal `json:"principals"`
	Setup      Phase                `json:"setup"`
	Teardown   Phase                `json:"teardown"`
	Cases      []Case               `json:"cases"`
}

// ForReference returns a validated fixture with the pinned version's cases.
func (fixture *Fixture) ForReference(version string) (*Fixture, error) {
	extension, ok := fixture.References[version]
	if !ok {
		return fixture, nil
	}
	merged := *fixture
	merged.References = nil
	merged.Principals = maps.Clone(fixture.Principals)
	for name, value := range extension.Principals {
		if _, exists := merged.Principals[name]; exists {
			return nil, fmt.Errorf("reference %s repeats principal %q", version, name)
		}
		merged.Principals[name] = value
	}
	merged.Setup = Phase{
		Graph:          slices.Concat(fixture.Setup.Graph, extension.Setup.Graph),
		Administration: slices.Concat(fixture.Setup.Administration, extension.Setup.Administration),
	}
	merged.Teardown = Phase{
		Graph:          slices.Concat(extension.Teardown.Graph, fixture.Teardown.Graph),
		Administration: slices.Concat(extension.Teardown.Administration, fixture.Teardown.Administration),
	}
	merged.Cases = slices.Concat(fixture.Cases, extension.Cases)
	if err := merged.Validate(); err != nil {
		return nil, err
	}
	return &merged, nil
}

// Principal is a fixture principal. Admin principals use the endpoint's
// operator-supplied credentials; every other principal uses the synthetic
// password supplied to the harness.
type Principal struct {
	Admin bool   `json:"admin,omitempty"`
	User  string `json:"user,omitempty"`
}

// Phase groups statements by the database they execute against.
type Phase struct {
	Graph          []string `json:"graph"`
	Administration []string `json:"administration"`
}

// Case is one documented authorization flow.
type Case struct {
	ID          string      `json:"id"`
	Covers      []string    `json:"covers"`
	Description string      `json:"description"`
	Database    string      `json:"database"`
	Principal   string      `json:"principal"`
	Impersonate string      `json:"impersonate,omitempty"`
	Session     SessionSpec `json:"session,omitempty"`
	Statements  []string    `json:"statements"`
	Expect      Expectation `json:"expect"`
}

// SessionSpec selects how the official Go driver sends a case to the endpoint.
// The zero value is one explicit managed transaction that names the case's
// database and connects over the direct Bolt URI.
type SessionSpec struct {
	Transaction string `json:"transaction,omitempty"`
	Database    string `json:"database,omitempty"`
	Routing     string `json:"routing,omitempty"`
}

// Autocommit reports whether the case runs each statement as an auto-commit
// statement on one session instead of inside an explicit transaction.
func (s SessionSpec) Autocommit() bool {
	return s.Transaction == SessionTransactionAutocommit
}

// OmitDatabase reports whether the session leaves database selection to the
// endpoint instead of naming the case's database.
func (s SessionSpec) OmitDatabase() bool {
	return s.Database == SessionDatabaseOmitted
}

// Routed reports whether the case connects through the endpoint's routing URI.
func (s SessionSpec) Routed() bool {
	return s.Routing == SessionRoutingRouted
}

// Validate reports an unsupported session shape.
func (s SessionSpec) Validate() error {
	var problems []error
	switch s.Transaction {
	case "", SessionTransactionExplicit, SessionTransactionAutocommit:
	default:
		problems = append(problems, fmt.Errorf("session transaction %q must be %q or %q",
			s.Transaction, SessionTransactionExplicit, SessionTransactionAutocommit))
	}
	switch s.Database {
	case "", SessionDatabaseExplicit, SessionDatabaseOmitted:
	default:
		problems = append(problems, fmt.Errorf("session database %q must be %q or %q",
			s.Database, SessionDatabaseExplicit, SessionDatabaseOmitted))
	}
	switch s.Routing {
	case "", SessionRoutingDirect, SessionRoutingRouted:
	default:
		problems = append(problems, fmt.Errorf("session routing %q must be %q or %q",
			s.Routing, SessionRoutingDirect, SessionRoutingRouted))
	}
	return errors.Join(problems...)
}

// Expectation is the documented outcome a reference and candidate must both
// produce. Allow cases name their exact normalized rows. Deny cases name an
// error code; a trailing dot means "any code with this prefix".
type Expectation struct {
	Kind string  `json:"kind"`
	Code string  `json:"code,omitempty"`
	Rows [][]any `json:"rows,omitempty"`
}

// LoadFixture reads, decodes, normalizes and validates a fixture file.
func LoadFixture(path string) (*Fixture, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open FGAC fixture %s: %w", path, err)
	}
	defer file.Close()

	decoder := convert.CypherInputJSON.NewDecoder(file)
	decoder.DisallowUnknownFields()
	var fixture Fixture
	if err := decoder.Decode(&fixture); err != nil {
		return nil, fmt.Errorf("decode FGAC fixture %s: %w", path, err)
	}
	if err := ensureEOF(decoder); err != nil {
		return nil, fmt.Errorf("decode FGAC fixture %s: %w", path, err)
	}
	fixture.normalize()
	if err := fixture.Validate(); err != nil {
		return nil, fmt.Errorf("validate FGAC fixture %s: %w", path, err)
	}
	return &fixture, nil
}

func ensureEOF(decoder interface{ Decode(any) error }) error {
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return errors.New("unexpected trailing JSON value")
		}
		return err
	}
	return nil
}

func (f *Fixture) normalize() {
	normalizeFixtureCases(f.Cases)
	for _, section := range f.References {
		normalizeFixtureCases(section.Cases)
	}
}

func normalizeFixtureCases(cases []Case) {
	for caseIndex := range cases {
		rows := cases[caseIndex].Expect.Rows
		for rowIndex := range rows {
			for columnIndex := range rows[rowIndex] {
				rows[rowIndex][columnIndex] = normalizeFixtureValue(rows[rowIndex][columnIndex])
			}
		}
	}
}

func normalizeFixtureValue(value any) any {
	switch typed := value.(type) {
	case float64:
		if typed == math.Trunc(typed) && !math.IsInf(typed, 0) && !math.IsNaN(typed) {
			return int64(typed)
		}
		return typed
	case []any:
		normalized := make([]any, len(typed))
		for index, item := range typed {
			normalized[index] = normalizeFixtureValue(item)
		}
		return normalized
	case map[string]any:
		normalized := make(map[string]any, len(typed))
		for key, item := range typed {
			normalized[key] = normalizeFixtureValue(item)
		}
		return normalized
	default:
		return value
	}
}

// Validate reports every defect in the fixture rather than stopping at the
// first one.
func (f *Fixture) Validate() error {
	var problems []error
	if strings.TrimSpace(f.Name) == "" {
		problems = append(problems, errors.New("name is empty"))
	}
	if strings.TrimSpace(f.Purpose) == "" {
		problems = append(problems, errors.New("purpose is empty"))
	}
	if len(f.Principals) == 0 {
		problems = append(problems, errors.New("principals are empty"))
	}
	admin, hasAdmin := f.Principals["admin"]
	if !hasAdmin || !admin.Admin {
		problems = append(problems, errors.New("principal \"admin\" must be an admin principal"))
	}
	userNames := make(map[string]string, len(f.Principals))
	for key, principal := range f.Principals {
		if key == "" {
			problems = append(problems, errors.New("principal key is empty"))
			continue
		}
		if principal.Admin {
			if principal.User != "" {
				problems = append(problems, fmt.Errorf("admin principal %q must not name a user", key))
			}
			continue
		}
		if !syntheticUserName.MatchString(principal.User) {
			problems = append(problems, fmt.Errorf("principal %q user %q must match %s", key, principal.User, syntheticUserName))
		}
		if previous, exists := userNames[principal.User]; exists {
			problems = append(problems, fmt.Errorf("principals %q and %q share user %q", previous, key, principal.User))
		}
		userNames[principal.User] = key
	}

	problems = append(problems, validatePhase("setup", f.Setup)...)
	problems = append(problems, validatePhase("teardown", f.Teardown)...)
	problems = append(problems, validateTeardownSafety(f.Teardown)...)
	problems = append(problems, validateSetupNames(f.Setup)...)

	if len(f.Cases) == 0 {
		problems = append(problems, errors.New("cases are empty"))
	}
	seenIDs := make(map[string]bool, len(f.Cases))
	coverage := make(map[string]bool)
	for caseIndex, testCase := range f.Cases {
		location := fmt.Sprintf("case %d (%q)", caseIndex, testCase.ID)
		if !caseIDPattern.MatchString(testCase.ID) {
			problems = append(problems, fmt.Errorf("%s: id must be lower kebab-case", location))
		}
		if seenIDs[testCase.ID] {
			problems = append(problems, fmt.Errorf("%s: duplicate id", location))
		}
		seenIDs[testCase.ID] = true
		if strings.TrimSpace(testCase.Description) == "" {
			problems = append(problems, fmt.Errorf("%s: description is empty", location))
		}
		if len(testCase.Covers) == 0 {
			problems = append(problems, fmt.Errorf("%s: covers is empty", location))
		}
		for _, covered := range testCase.Covers {
			if !knownCoverage(covered) {
				problems = append(problems, fmt.Errorf("%s: unknown coverage %q", location, covered))
			}
			coverage[covered] = true
		}
		if testCase.Database != DatabaseSystem && testCase.Database != DatabaseTarget {
			problems = append(problems, fmt.Errorf("%s: database %q must be %q or %q", location, testCase.Database, DatabaseSystem, DatabaseTarget))
		}
		if _, exists := f.Principals[testCase.Principal]; !exists {
			problems = append(problems, fmt.Errorf("%s: unknown principal %q", location, testCase.Principal))
		}
		if testCase.Impersonate != "" {
			principal, exists := f.Principals[testCase.Impersonate]
			if !exists || principal.Admin {
				problems = append(problems, fmt.Errorf("%s: impersonation requires a synthetic user principal", location))
			}
		}
		if err := testCase.Session.Validate(); err != nil {
			problems = append(problems, fmt.Errorf("%s: %w", location, err))
		}
		if testCase.Session.OmitDatabase() && testCase.Database != DatabaseTarget {
			problems = append(problems, fmt.Errorf("%s: an omitted database selection requires the %q database", location, DatabaseTarget))
		}
		if len(testCase.Statements) == 0 {
			problems = append(problems, fmt.Errorf("%s: statements are empty", location))
		}
		for statementIndex, statement := range testCase.Statements {
			if err := validateStatement(statement); err != nil {
				problems = append(problems, fmt.Errorf("%s: statement %d: %w", location, statementIndex, err))
			}
		}
		if err := testCase.Expect.Validate(); err != nil {
			problems = append(problems, fmt.Errorf("%s: %w", location, err))
		}
	}
	for _, required := range RequiredCoverage {
		if !coverage[required] {
			problems = append(problems, fmt.Errorf("cases do not cover %q", required))
		}
	}
	return errors.Join(problems...)
}

func knownCoverage(value string) bool {
	for _, required := range RequiredCoverage {
		if value == required {
			return true
		}
	}
	return false
}

func validatePhase(name string, phase Phase) []error {
	var problems []error
	for _, group := range []struct {
		name       string
		statements []string
	}{
		{"graph", phase.Graph},
		{"administration", phase.Administration},
	} {
		for index, statement := range group.statements {
			if err := validateStatement(statement); err != nil {
				problems = append(problems, fmt.Errorf("%s.%s statement %d: %w", name, group.name, index, err))
			}
		}
	}
	return problems
}

func validateStatement(statement string) error {
	if strings.TrimSpace(statement) == "" {
		return errors.New("statement is empty")
	}
	rest := statement
	for {
		start := strings.Index(rest, "${")
		if start < 0 {
			return nil
		}
		end := strings.Index(rest[start:], "}")
		if end < 0 {
			return errors.New("unterminated placeholder")
		}
		placeholder := rest[start : start+end+1]
		if placeholder != databasePlaceholder && placeholder != passwordPlaceholder {
			return fmt.Errorf("unsupported placeholder %q", placeholder)
		}
		rest = rest[start+end+1:]
	}
}

// Validate checks one case expectation.
func (expectation Expectation) Validate() error {
	switch expectation.Kind {
	case "allow":
		if expectation.Code != "" {
			return errors.New("allow expectation must not name a code")
		}
		if expectation.Rows == nil {
			return errors.New("allow expectation must name rows; use [] for no rows")
		}
	case "deny":
		if expectation.Rows != nil {
			return errors.New("deny expectation must not name rows")
		}
		if !strings.HasPrefix(expectation.Code, "Neo.ClientError.") {
			return fmt.Errorf("deny code %q is not a Neo4j client error", expectation.Code)
		}
		if strings.HasSuffix(expectation.Code, ".") && len(expectation.Code) == len("Neo.ClientError.") {
			return fmt.Errorf("deny code prefix %q is too broad", expectation.Code)
		}
	default:
		return fmt.Errorf("expectation kind %q must be \"allow\" or \"deny\"", expectation.Kind)
	}
	return nil
}

func validateTeardownSafety(phase Phase) []error {
	var problems []error
	for index, statement := range phase.Graph {
		if !strings.Contains(statement, "DETACH DELETE") {
			problems = append(problems, fmt.Errorf("teardown.graph statement %d must delete with DETACH DELETE", index))
		}
		if !strings.Contains(statement, "WHERE") || !strings.Contains(statement, "FGACDoc") || !strings.Contains(statement, "FGACOther") {
			problems = append(problems, fmt.Errorf("teardown.graph statement %d must scope its delete to the synthetic FGACDoc/FGACOther labels", index))
		}
	}
	for index, statement := range phase.Administration {
		verb, name, tail, ok := fixtureAdministrationTarget(statement)
		if !ok || !strings.EqualFold(verb, "DROP") ||
			(len(tail) != 0 && (len(tail) != 2 || !strings.EqualFold(tail[0], "IF") || !strings.EqualFold(tail[1], "EXISTS"))) {
			problems = append(problems, fmt.Errorf("teardown.administration statement %d must be a DROP of a synthetic user, role, or auth rule", index))
			continue
		}
		if !syntheticUserName.MatchString(name) {
			problems = append(problems, fmt.Errorf("teardown.administration statement %d drops non-synthetic name %q", index, name))
		}
	}
	return problems
}

func validateSetupNames(phase Phase) []error {
	var problems []error
	for index, statement := range phase.Administration {
		verb, name, _, ok := fixtureAdministrationTarget(statement)
		if !strings.EqualFold(verb, "CREATE") {
			continue
		}
		if !ok {
			problems = append(problems, fmt.Errorf("setup.administration statement %d may only create synthetic users, roles or auth rules", index))
			continue
		}
		if !syntheticUserName.MatchString(name) {
			problems = append(problems, fmt.Errorf("setup.administration statement %d creates non-synthetic name %q", index, name))
		}
	}
	return problems
}

func fixtureAdministrationTarget(statement string) (verb, name string, tail []string, ok bool) {
	fields := strings.Fields(statement)
	if len(fields) >= 2 && strings.EqualFold(fields[0], "CYPHER") && (fields[1] == "5" || fields[1] == "25") {
		fields = fields[2:]
	}
	if len(fields) == 0 {
		return "", "", nil, false
	}
	verb = fields[0]
	if len(fields) >= 3 && (strings.EqualFold(fields[1], "USER") || strings.EqualFold(fields[1], "ROLE")) {
		return verb, fields[2], fields[3:], true
	}
	if len(fields) >= 4 && strings.EqualFold(fields[1], "AUTH") && strings.EqualFold(fields[2], "RULE") {
		return verb, fields[3], fields[4:], true
	}
	return verb, "", nil, false
}

// Substitute fills the only supported placeholders in a fixture statement.
func Substitute(statement, database, password string) (string, error) {
	substituted := strings.ReplaceAll(statement, databasePlaceholder, database)
	substituted = strings.ReplaceAll(substituted, passwordPlaceholder, password)
	if strings.Contains(substituted, "${") {
		return "", fmt.Errorf("statement still contains a placeholder: %s", statement)
	}
	return substituted, nil
}
