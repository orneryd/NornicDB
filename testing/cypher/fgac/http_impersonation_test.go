package fgac

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"
)

type queryAPICaseResult struct {
	status int
	body   struct {
		Data *struct {
			Fields []string
			Values [][]any
		}
		Errors               []struct{ Code string }
		Transaction          *struct{ ID string }
		Bookmarks            []string
		QueryType            string
		ResultAvailableAfter *int64
		ResultConsumedAfter  *int64
	}
}

type queryAPIContract struct {
	Status             int
	Fields             []string
	Values             [][]any
	Codes              []string
	HasData            bool
	HasSession         bool
	HasBookmarks       bool
	QueryType          string
	HasAvailableTiming bool
	HasConsumedTiming  bool
}

func (result queryAPICaseResult) contract() queryAPIContract {
	outcome := queryAPIContract{
		Status: result.status, HasData: result.body.Data != nil,
		HasSession:   result.body.Transaction != nil && result.body.Transaction.ID != "",
		HasBookmarks: len(result.body.Bookmarks) != 0, QueryType: result.body.QueryType,
		HasAvailableTiming: result.body.ResultAvailableAfter != nil, HasConsumedTiming: result.body.ResultConsumedAfter != nil,
	}
	if result.body.Data != nil {
		outcome.Fields = result.body.Data.Fields
		outcome.Values = result.body.Data.Values
	}
	for _, failure := range result.body.Errors {
		outcome.Codes = append(outcome.Codes, failure.Code)
	}
	return outcome
}

func compareHTTPImpersonation(t *testing.T, ctx context.Context, reference, candidate *endpointRunner) {
	t.Helper()
	referenceURL := strings.TrimSpace(os.Getenv("NORNICDB_NEO4J_REFERENCE_HTTP_URI"))
	candidateURL := strings.TrimSpace(os.Getenv("NORNICDB_FGAC_CANDIDATE_HTTP_URI"))
	if referenceURL == "" && candidateURL == "" {
		t.Skip("set both HTTP endpoint URIs to compare Query API impersonation")
	}
	for _, endpoint := range []string{referenceURL, candidateURL} {
		parsed, err := url.Parse(endpoint)
		if err != nil || parsed.User != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") ||
			(parsed.Path != "" && parsed.Path != "/") || parsed.RawQuery != "" || parsed.Fragment != "" {
			t.Fatal("HTTP endpoint must be an HTTP(S) origin with separate credentials")
		}
	}
	if strings.TrimRight(referenceURL, "/") == strings.TrimRight(candidateURL, "/") {
		t.Fatal("reference and candidate must be distinct HTTP origins")
	}

	client := &http.Client{
		Timeout:       15 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	referenceOutcomes := runHTTPImpersonationCases(t, ctx, client, referenceURL, reference)
	candidateOutcomes := runHTTPImpersonationCases(t, ctx, client, candidateURL, candidate)
	for _, oracle := range referenceOutcomes {
		oracle := oracle
		t.Run(oracle.name, func(t *testing.T) {
			actual, ok := candidateOutcomes.get(oracle.name)
			if !ok {
				t.Fatal("candidate did not complete the case")
			}
			if !reflect.DeepEqual(oracle.outcome, actual) {
				t.Errorf("reference %+v; candidate %+v", oracle.outcome, actual)
			}
		})
	}
}

type namedQueryAPIOutcome struct {
	name    string
	outcome queryAPIContract
}

type queryAPIOutcomes []namedQueryAPIOutcome

func (outcomes queryAPIOutcomes) get(name string) (queryAPIContract, bool) {
	for _, item := range outcomes {
		if item.name == name {
			return item.outcome, true
		}
	}
	return queryAPIContract{}, false
}

func runHTTPImpersonationCases(t *testing.T, ctx context.Context, client *http.Client, origin string, endpoint *endpointRunner) queryAPIOutcomes {
	t.Helper()
	path := "/db/" + url.PathEscape(endpoint.config.Database) + "/query/v2"
	const system = "/db/system/query/v2"
	const target = "fgac_reader"
	const projection = "MATCH (n:FGACDoc) RETURN n.id AS id, n.published AS published ORDER BY id"
	var outcomes queryAPIOutcomes
	run := func(name, method, route, user, password string, body map[string]string) queryAPICaseResult {
		t.Helper()
		result, err := callQueryAPI(ctx, client, origin, route, method, user, password, body)
		if err != nil {
			t.Fatalf("%s %s: %v", endpoint.config.Name, name, err)
		}
		outcomes = append(outcomes, namedQueryAPIOutcome{name: name, outcome: result.contract()})
		return result
	}
	admin := func(name, method, route string, body map[string]string) queryAPICaseResult {
		return run(name, method, route, endpoint.config.User, endpoint.config.Password, body)
	}
	impersonate := func(statement, user string) map[string]string {
		return map[string]string{"statement": statement, "impersonatedUser": user}
	}
	admin("current-user", http.MethodPost, system, impersonate("SHOW CURRENT USER YIELD user RETURN user", target))
	admin("ordinary-before", http.MethodPost, path, map[string]string{"statement": projection})
	admin("property-projection", http.MethodPost, path, impersonate(projection, target))
	admin("denied-target-rows", http.MethodPost, path, impersonate(projection, "fgac_denied"))
	admin("ordinary-after", http.MethodPost, path, map[string]string{"statement": projection})
	delegatedRead := func(name string, allowed bool) {
		t.Helper()
		outcome := run(name, http.MethodPost, path, "fgac_access", endpoint.syntheticPassword, impersonate(projection, target)).contract()
		if allowed {
			if outcome.Status != http.StatusAccepted || !reflect.DeepEqual(outcome.Values, [][]any{{"doc-a", nil}, {"doc-b", nil}}) {
				t.Errorf("%s %s: wanted the target's projected rows, got %+v", endpoint.config.Name, name, outcome)
			}
		} else if !reflect.DeepEqual(outcome.Codes, []string{"Neo.ClientError.Security.Forbidden"}) ||
			outcome.HasData || outcome.HasBookmarks || outcome.HasSession {
			t.Errorf("%s %s: expected a denial with no results or transaction, got %+v", endpoint.config.Name, name, outcome)
		}
	}
	delegatedRead("actor-without-grant", false)
	for _, step := range []struct {
		name, statement string
		allowed         bool
	}{
		{"named-grant", "GRANT IMPERSONATE (fgac_reader) ON DBMS TO fgac_access", true},
		{"named-revoke", "REVOKE GRANT IMPERSONATE (fgac_reader) ON DBMS FROM fgac_access", false},
		{"wildcard-grant", "GRANT IMPERSONATE (*) ON DBMS TO fgac_access", true},
		{"deny-over-grant", "DENY IMPERSONATE (fgac_reader) ON DBMS TO fgac_access", false},
		{"revoke-deny", "REVOKE DENY IMPERSONATE (fgac_reader) ON DBMS FROM fgac_access", true},
	} {
		if err := endpoint.runAdminStatement(ctx, DatabaseSystem, step.statement); err != nil {
			t.Fatalf("%s %s: %v", endpoint.config.Name, step.name, err)
		}
		delegatedRead(step.name, step.allowed)
	}
	if err := endpoint.runAdminStatement(ctx, DatabaseSystem, "REVOKE GRANT IMPERSONATE (*) ON DBMS FROM fgac_access"); err != nil {
		t.Fatalf("%s release fixture impersonation grant: %v", endpoint.config.Name, err)
	}
	for _, account := range []struct{ name, statement string }{
		{"suspended-target", "ALTER USER fgac_reader SET STATUS SUSPENDED"},
		{"active-target", "ALTER USER fgac_reader SET STATUS ACTIVE"},
		{"target-with-expired-password", "ALTER USER fgac_reader SET PASSWORD CHANGE REQUIRED"},
		{"target-with-valid-password", "ALTER USER fgac_reader SET PASSWORD CHANGE NOT REQUIRED"},
	} {
		if err := endpoint.runAdminStatement(ctx, DatabaseSystem, account.statement); err != nil {
			t.Fatalf("%s %s: %v", endpoint.config.Name, account.name, err)
		}
		admin(account.name, http.MethodPost, path, impersonate(projection, target))
	}
	admin("absent-target", http.MethodPost, path, impersonate(projection, "fgac_missing_impersonation_target"))
	first := admin("begin", http.MethodPost, path+"/tx", map[string]string{"impersonatedUser": target})
	if first.body.Transaction != nil && first.body.Transaction.ID != "" {
		tx := path + "/tx/" + url.PathEscape(first.body.Transaction.ID)
		admin("continue", http.MethodPost, tx, map[string]string{"statement": projection})
		admin("commit", http.MethodPost, tx+"/commit", map[string]string{})
	}
	second := admin("begin-with-statement", http.MethodPost, path+"/tx", impersonate(projection, target))
	if second.body.Transaction != nil && second.body.Transaction.ID != "" {
		admin("rollback", http.MethodDelete, path+"/tx/"+url.PathEscape(second.body.Transaction.ID), nil)
	}
	return outcomes
}

func callQueryAPI(ctx context.Context, client *http.Client, origin, path, method, user, password string, body map[string]string) (queryAPICaseResult, error) {
	var result queryAPICaseResult
	payload, err := json.Marshal(body)
	if err != nil {
		return result, err
	}
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(origin, "/")+path, bytes.NewReader(payload))
	if err != nil {
		return result, err
	}
	req.SetBasicAuth(user, password)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
	response, err := client.Do(req)
	if err != nil {
		return result, err
	}
	defer response.Body.Close()
	result.status = response.StatusCode
	if response.StatusCode == http.StatusOK && method == http.MethodDelete {
		return result, nil
	}
	if err := json.NewDecoder(response.Body).Decode(&result.body); err != nil {
		return result, fmt.Errorf("decode Query API response (HTTP %d): %w", result.status, err)
	}
	return result, nil
}
