package fgac

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"regexp"
	"slices"
	"strings"
)

// ErrNotOptedIn reports that the caller did not explicitly opt in to the
// Neo4j Enterprise evaluation comparison.
var ErrNotOptedIn = errors.New("set NORNICDB_FGAC_EE_EVALUATION=1 to run the Neo4j Enterprise FGAC comparison")

var syntheticPasswordPattern = regexp.MustCompile(`^[A-Za-z0-9._-]{12,128}$`)

var (
	directBoltSchemes = []string{"bolt", "bolt+s", "neo4j", "neo4j+s"}
	routedBoltSchemes = []string{"neo4j", "neo4j+s"}
)

// EndpointConfig is one Bolt endpoint and its operator-supplied admin login.
// RoutedURI is optional and is only used by cases that ask for a routed
// session; the operator supplies it because only the endpoint's own
// configuration shows whether it advertises a routing table.
type EndpointConfig struct {
	Name      string
	URI       string
	RoutedURI string
	User      string
	Password  string
	Database  string
}

// ComparisonConfig is the opt-in comparison configuration.
type ComparisonConfig struct {
	FixturePath       string
	ReferencesPath    string
	ReferenceVersion  string
	SyntheticPassword string
	Reference         EndpointConfig
	Candidate         EndpointConfig
}

// ComparisonConfigFromEnv reads the opt-in comparison contract. It returns
// ErrNotOptedIn when the explicit evaluation opt-in is absent.
func ComparisonConfigFromEnv() (ComparisonConfig, error) {
	if os.Getenv("NORNICDB_FGAC_EE_EVALUATION") != "1" {
		return ComparisonConfig{}, ErrNotOptedIn
	}
	config := ComparisonConfig{
		FixturePath:       envOr("NORNICDB_FGAC_FIXTURE", "fixtures/authorization_flows.json"),
		ReferencesPath:    envOr("NORNICDB_FGAC_REFERENCES", "references.json"),
		ReferenceVersion:  strings.TrimSpace(os.Getenv("NORNICDB_FGAC_REFERENCE_VERSION")),
		SyntheticPassword: envOr("NORNICDB_FGAC_SYNTHETIC_PASSWORD", "fgac-synthetic-password"),
		Reference: EndpointConfig{
			Name:      "reference",
			URI:       strings.TrimSpace(os.Getenv("NORNICDB_NEO4J_REFERENCE_URI")),
			RoutedURI: strings.TrimSpace(os.Getenv("NORNICDB_NEO4J_REFERENCE_ROUTED_URI")),
			User:      envOr("NORNICDB_NEO4J_REFERENCE_USER", "neo4j"),
			Password:  os.Getenv("NORNICDB_NEO4J_REFERENCE_PASSWORD"),
			Database:  envOr("NORNICDB_NEO4J_REFERENCE_DATABASE", "neo4j"),
		},
		Candidate: EndpointConfig{
			Name:      "candidate",
			URI:       strings.TrimSpace(os.Getenv("NORNICDB_FGAC_CANDIDATE_URI")),
			RoutedURI: strings.TrimSpace(os.Getenv("NORNICDB_FGAC_CANDIDATE_ROUTED_URI")),
			User:      envOr("NORNICDB_FGAC_CANDIDATE_USER", "admin"),
			Password:  os.Getenv("NORNICDB_FGAC_CANDIDATE_PASSWORD"),
			Database:  envOr("NORNICDB_FGAC_CANDIDATE_DATABASE", "nornic"),
		},
	}
	if err := config.Validate(); err != nil {
		return ComparisonConfig{}, err
	}
	return config, nil
}

func envOr(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

// Validate checks the complete comparison contract.
func (c ComparisonConfig) Validate() error {
	var problems []error
	if strings.TrimSpace(c.FixturePath) == "" {
		problems = append(problems, errors.New("fixture path is empty"))
	}
	if strings.TrimSpace(c.ReferencesPath) == "" {
		problems = append(problems, errors.New("references path is empty"))
	}
	if !syntheticPasswordPattern.MatchString(c.SyntheticPassword) {
		problems = append(problems, fmt.Errorf("synthetic password must match %s", syntheticPasswordPattern))
	}
	for _, endpoint := range []EndpointConfig{c.Reference, c.Candidate} {
		if strings.TrimSpace(endpoint.URI) == "" {
			problems = append(problems, fmt.Errorf("%s Bolt URI is empty", endpoint.Name))
		} else if err := validateBoltURI(endpoint.Name, "Bolt URI", endpoint.URI, directBoltSchemes); err != nil {
			problems = append(problems, err)
		}
		if endpoint.RoutedURI != "" {
			if err := validateBoltURI(endpoint.Name, "routed Bolt URI", endpoint.RoutedURI, routedBoltSchemes); err != nil {
				problems = append(problems, err)
			}
		}
		if strings.TrimSpace(endpoint.User) == "" {
			problems = append(problems, fmt.Errorf("%s admin user is empty", endpoint.Name))
		}
		if endpoint.Password == "" {
			problems = append(problems, fmt.Errorf("%s admin password is empty", endpoint.Name))
		}
		if strings.TrimSpace(endpoint.Database) == "" {
			problems = append(problems, fmt.Errorf("%s target database is empty", endpoint.Name))
		}
		if strings.EqualFold(endpoint.Database, DatabaseSystem) {
			problems = append(problems, fmt.Errorf("%s target database cannot be %q", endpoint.Name, DatabaseSystem))
		}
	}
	if (c.Reference.RoutedURI == "") != (c.Candidate.RoutedURI == "") {
		problems = append(problems, errors.New("routed cases require a routed Bolt URI for both endpoints"))
	}
	if c.Reference.Database == c.Candidate.Database && c.Reference.URI == c.Candidate.URI {
		problems = append(problems, errors.New("reference and candidate must be distinct endpoints"))
	}
	return errors.Join(problems...)
}

func validateBoltURI(endpointName, kind, value string, schemes []string) error {
	uri, err := url.Parse(value)
	if err != nil || uri.Hostname() == "" || uri.User != nil || !slices.Contains(schemes, uri.Scheme) {
		return fmt.Errorf("%s %s must use %s with a host and separate credentials",
			endpointName, kind, strings.Join(schemes, ", "))
	}
	return nil
}
