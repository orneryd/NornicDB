package fgac

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// ComponentInfo is the subset of CALL dbms.components() used to prove which
// reference answered.
type ComponentInfo struct {
	Name     string
	Versions []string
	Edition  string
}

func (c ComponentInfo) String() string {
	return fmt.Sprintf("%s versions=%s edition=%s", c.Name, strings.Join(c.Versions, ","), c.Edition)
}

// endpointRunner owns the drivers and synthetic setup for one endpoint.
type endpointRunner struct {
	config            EndpointConfig
	fixture           *Fixture
	syntheticPassword string
	admin             neo4j.DriverWithContext
	userDrivers       map[string]neo4j.DriverWithContext
}

func newEndpointRunner(ctx context.Context, config EndpointConfig, fixture *Fixture, syntheticPassword string) (*endpointRunner, error) {
	admin, err := openAdminDriver(ctx, config)
	if err != nil {
		return nil, err
	}
	return &endpointRunner{
		config:            config,
		fixture:           fixture,
		syntheticPassword: syntheticPassword,
		admin:             admin,
		userDrivers:       make(map[string]neo4j.DriverWithContext),
	}, nil
}

func openAdminDriver(ctx context.Context, config EndpointConfig) (neo4j.DriverWithContext, error) {
	driver, err := neo4j.NewDriverWithContext(config.URI, neo4j.BasicAuth(config.User, config.Password, ""))
	if err != nil {
		return nil, fmt.Errorf("%s: create Bolt driver for %s: %w", config.Name, config.URI, err)
	}
	deadline := time.Now().Add(2 * time.Minute)
	for {
		verifyContext, cancel := context.WithTimeout(ctx, 10*time.Second)
		err = driver.VerifyConnectivity(verifyContext)
		cancel()
		if err == nil {
			return driver, nil
		}
		if ctx.Err() != nil || time.Now().After(deadline) {
			_ = driver.Close(context.Background())
			return nil, fmt.Errorf("%s: connect to %s: %w", config.Name, config.URI, err)
		}
		select {
		case <-ctx.Done():
			_ = driver.Close(context.Background())
			return nil, fmt.Errorf("%s: connect to %s: %w", config.Name, config.URI, ctx.Err())
		case <-time.After(2 * time.Second):
		}
	}
}

func openUserDriver(ctx context.Context, config EndpointConfig, user, password string) (neo4j.DriverWithContext, error) {
	driver, err := neo4j.NewDriverWithContext(config.URI, neo4j.BasicAuth(user, password, ""))
	if err != nil {
		return nil, fmt.Errorf("%s: create Bolt driver for synthetic user %q: %w", config.Name, user, err)
	}
	verifyContext, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := driver.VerifyConnectivity(verifyContext); err != nil {
		_ = driver.Close(context.Background())
		return nil, fmt.Errorf("%s: connect synthetic user %q: %w", config.Name, user, err)
	}
	return driver, nil
}

func (r *endpointRunner) close(ctx context.Context) {
	for _, driver := range r.userDrivers {
		_ = driver.Close(ctx)
	}
	if r.admin != nil {
		_ = r.admin.Close(ctx)
	}
}

func (r *endpointRunner) driver(ctx context.Context, principalKey string) (neo4j.DriverWithContext, error) {
	principal, exists := r.fixture.Principals[principalKey]
	if !exists {
		return nil, fmt.Errorf("unknown fixture principal %q", principalKey)
	}
	if principal.Admin {
		return r.admin, nil
	}
	if driver, exists := r.userDrivers[principalKey]; exists {
		return driver, nil
	}
	driver, err := openUserDriver(ctx, r.config, principal.User, r.syntheticPassword)
	if err != nil {
		return nil, err
	}
	r.userDrivers[principalKey] = driver
	return driver, nil
}

func (r *endpointRunner) databaseFor(key string) string {
	if key == DatabaseSystem {
		return DatabaseSystem
	}
	return r.config.Database
}

// teardown removes only the fixture's synthetic users, roles, labels and
// relationships.
func (r *endpointRunner) teardown(ctx context.Context) error {
	var problems []error
	for index, statement := range r.fixture.Teardown.Graph {
		if err := r.runAdminStatement(ctx, r.config.Database, statement); err != nil {
			problems = append(problems, fmt.Errorf("teardown graph statement %d: %w", index, err))
		}
	}
	for index, statement := range r.fixture.Teardown.Administration {
		if err := r.runAdminStatement(ctx, DatabaseSystem, statement); err != nil {
			problems = append(problems, fmt.Errorf("teardown administration statement %d: %w", index, err))
		}
	}
	return errors.Join(problems...)
}

// prepare resets synthetic state and applies the fixture. A reset failure is
// returned instead of applying setup onto state the harness cannot account for.
func (r *endpointRunner) prepare(ctx context.Context) error {
	if err := r.teardown(ctx); err != nil {
		return fmt.Errorf("%s: reset synthetic state: %w", r.config.Name, err)
	}

	var setupProblems []error
	for index, statement := range r.fixture.Setup.Graph {
		if err := r.runAdminStatement(ctx, r.config.Database, statement); err != nil {
			setupProblems = append(setupProblems, fmt.Errorf("setup graph statement %d: %w", index, err))
		}
	}
	for index, statement := range r.fixture.Setup.Administration {
		if err := r.runAdminStatement(ctx, DatabaseSystem, statement); err != nil {
			setupProblems = append(setupProblems, fmt.Errorf("setup administration statement %d: %w", index, err))
		}
	}
	if err := errors.Join(setupProblems...); err != nil {
		return fmt.Errorf("%s: apply synthetic setup: %w", r.config.Name, err)
	}
	return nil
}

func (r *endpointRunner) runAdminStatement(ctx context.Context, database, statement string) error {
	substituted, err := Substitute(statement, r.config.Database, r.syntheticPassword)
	if err != nil {
		return err
	}
	session := r.admin.NewSession(ctx, neo4j.SessionConfig{DatabaseName: database, AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)
	outcome, err := executeAutocommit(ctx, session, substituted)
	if err != nil {
		return err
	}
	if outcome.Denied() {
		return fmt.Errorf("%s: %s", outcome.Code, outcome.Message)
	}
	return nil
}

// executeCase runs one fixture case through an explicit managed transaction.
// Multi-statement cases therefore roll back completely when a later statement
// is denied.
func (r *endpointRunner) executeCase(ctx context.Context, testCase Case) (Outcome, error) {
	driver, err := r.driver(ctx, testCase.Principal)
	if err != nil {
		return Outcome{}, err
	}
	statements := make([]string, 0, len(testCase.Statements))
	for _, statement := range testCase.Statements {
		substituted, err := Substitute(statement, r.config.Database, r.syntheticPassword)
		if err != nil {
			return Outcome{}, err
		}
		statements = append(statements, substituted)
	}
	session := driver.NewSession(ctx, neo4j.SessionConfig{
		DatabaseName:     r.databaseFor(testCase.Database),
		AccessMode:       neo4j.AccessModeWrite,
		ImpersonatedUser: r.fixture.Principals[testCase.Impersonate].User,
	})
	defer session.Close(ctx)

	var outcome Outcome
	_, err = session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		result, runErr := runStatements(ctx, tx, statements)
		outcome = result
		return nil, runErr
	})
	if err != nil {
		denied, ok, convertErr := outcomeFromError(err)
		if convertErr != nil {
			return Outcome{}, convertErr
		}
		if !ok {
			return Outcome{}, err
		}
		return denied, nil
	}
	return normalizeOutcome(outcome)
}

func runStatements(ctx context.Context, tx neo4j.ManagedTransaction, statements []string) (Outcome, error) {
	var outcome Outcome
	for _, statement := range statements {
		result, err := tx.Run(ctx, statement, nil)
		if err != nil {
			return Outcome{}, err
		}
		columns, err := result.Keys()
		if err != nil {
			return Outcome{}, err
		}
		rows := make([][]any, 0)
		for result.Next(ctx) {
			rows = append(rows, result.Record().Values)
		}
		if err := result.Err(); err != nil {
			return Outcome{}, err
		}
		outcome = Outcome{Columns: columns, Rows: rows}
	}
	return outcome, nil
}

func executeAutocommit(ctx context.Context, session neo4j.SessionWithContext, statement string) (Outcome, error) {
	result, err := session.Run(ctx, statement, nil)
	if err != nil {
		denied, ok, convertErr := outcomeFromError(err)
		if convertErr != nil {
			return Outcome{}, convertErr
		}
		if !ok {
			return Outcome{}, err
		}
		return denied, nil
	}
	columns, err := result.Keys()
	if err != nil {
		return Outcome{}, err
	}
	rows := make([][]any, 0)
	for result.Next(ctx) {
		rows = append(rows, result.Record().Values)
	}
	if err := result.Err(); err != nil {
		return Outcome{}, err
	}
	return normalizeOutcome(Outcome{Columns: columns, Rows: rows})
}

func outcomeFromError(err error) (Outcome, bool, error) {
	var neoErr *neo4j.Neo4jError
	if errors.As(err, &neoErr) {
		return Outcome{Code: neoErr.Code, Message: neoErr.Msg}, true, nil
	}
	return Outcome{}, false, err
}

func normalizeOutcome(outcome Outcome) (Outcome, error) {
	normalized := Outcome{
		Columns: append([]string(nil), outcome.Columns...),
		Code:    outcome.Code,
		Message: outcome.Message,
		Rows:    make([][]any, len(outcome.Rows)),
	}
	for rowIndex, row := range outcome.Rows {
		normalized.Rows[rowIndex] = make([]any, len(row))
		for columnIndex, value := range row {
			converted, err := normalizeBoltValue(value)
			if err != nil {
				return Outcome{}, fmt.Errorf("row %d column %d: %w", rowIndex, columnIndex, err)
			}
			normalized.Rows[rowIndex][columnIndex] = converted
		}
	}
	return normalized, nil
}

func normalizeBoltValue(value any) (any, error) {
	switch typed := value.(type) {
	case nil, bool, string, int64, float64:
		return typed, nil
	case int:
		return int64(typed), nil
	case []any:
		normalized := make([]any, len(typed))
		for index, item := range typed {
			converted, err := normalizeBoltValue(item)
			if err != nil {
				return nil, err
			}
			normalized[index] = converted
		}
		return normalized, nil
	case map[string]any:
		normalized := make(map[string]any, len(typed))
		for key, item := range typed {
			converted, err := normalizeBoltValue(item)
			if err != nil {
				return nil, err
			}
			normalized[key] = converted
		}
		return normalized, nil
	default:
		return nil, fmt.Errorf("unsupported Bolt value type %T", value)
	}
}

// components reads the endpoint identity through the admin login.
func (r *endpointRunner) components(ctx context.Context) (ComponentInfo, error) {
	session := r.admin.NewSession(ctx, neo4j.SessionConfig{DatabaseName: DatabaseSystem, AccessMode: neo4j.AccessModeRead})
	defer session.Close(ctx)
	result, err := session.Run(ctx, "CALL dbms.components() YIELD name, versions, edition RETURN name, versions, edition", nil)
	if err != nil {
		return ComponentInfo{}, fmt.Errorf("%s: read dbms.components: %w", r.config.Name, err)
	}
	defer result.Consume(ctx)
	if !result.Next(ctx) {
		return ComponentInfo{}, fmt.Errorf("%s: dbms.components returned no rows: %w", r.config.Name, result.Err())
	}
	record := result.Record()
	name, _ := record.Get("name")
	versionsValue, _ := record.Get("versions")
	edition, _ := record.Get("edition")
	info := ComponentInfo{}
	if text, ok := name.(string); ok {
		info.Name = text
	}
	if text, ok := edition.(string); ok {
		info.Edition = text
	}
	switch typed := versionsValue.(type) {
	case []any:
		for _, version := range typed {
			if text, ok := version.(string); ok {
				info.Versions = append(info.Versions, text)
			}
		}
	case []string:
		info.Versions = append(info.Versions, typed...)
	}
	if info.Name == "" || info.Edition == "" || len(info.Versions) == 0 {
		return ComponentInfo{}, fmt.Errorf("%s: dbms.components returned an incomplete identity %+v", r.config.Name, info)
	}
	return info, nil
}

// VerifyReferenceIdentity proves that the reference endpoint is a pinned
// Enterprise build. It never accepts Community as a substitute.
func VerifyReferenceIdentity(info ComponentInfo, pins *ReferencePins, requestedVersion string) (ReferencePin, error) {
	if !strings.EqualFold(info.Edition, "enterprise") {
		return ReferencePin{}, fmt.Errorf("reference edition is %q, not enterprise", info.Edition)
	}
	candidates, err := pins.Resolve(requestedVersion)
	if err != nil {
		return ReferencePin{}, err
	}
	for _, pin := range candidates {
		for _, version := range info.Versions {
			if version == pin.Version {
				return pin, nil
			}
		}
	}
	pinned := make([]string, 0, len(candidates))
	for _, pin := range candidates {
		pinned = append(pinned, pin.Version)
	}
	return ReferencePin{}, fmt.Errorf("reference versions %v do not match pinned %s", info.Versions, strings.Join(pinned, ", "))
}
