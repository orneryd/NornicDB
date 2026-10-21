package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/internal/testutil/memorypressure"
	"github.com/orneryd/nornicdb/pkg/cypher"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/memory"
	"github.com/stretchr/testify/require"
)

// Transaction memory limits over HTTP (#1343). Neo4j 5.26 (neo-ref)
// answers a statement over its limit with 200, empty data and
// {"code": "Neo.TransientError.General.MemoryPoolOutOfMemoryError",
// "message": "The allocation of an extra … threshold reached"} in errors;
// an open transaction is rolled back, and later requests to it get 404
// Neo.ClientError.Transaction.TransactionNotFound.

const httpHeavyCollect = "UNWIND range(1, 200000) AS x RETURN size(collect(x)) AS n"

func TestTransactionMemoryYAMLOverrideActiveBeforeFirstProtocolExecutor(t *testing.T) {
	cypher.ConfigureTransactionMemory(0, cypher.TransactionMemoryLimits{})
	t.Cleanup(func() {
		cypher.SetDatabaseTransactionMemoryLimits("nornic", cypher.TransactionMemoryLimits{})
		cypher.ConfigureTransactionMemory(0, cypher.TransactionMemoryLimits{})
	})
	server, _ := setupTestServerWithConfig(t, func(config *Config) {
		config.PerDBYAMLOverrides = map[string]map[string]string{
			"nornic": {"db.memory.transaction.max": "1m"},
		}
	})
	engine, err := server.dbManager.GetStorage("nornic")
	require.NoError(t, err)
	// This represents Bolt or another embedded client arriving before HTTP
	// has built a cached executor for the database.
	executor := cypher.NewStorageExecutorWithQueryCachePolicy(engine, 0, 0)
	_, err = executor.Execute(context.Background(), httpHeavyCollect, nil)
	require.Error(t, err)
	code, _ := nornicerrors.Neo4jStatus(err)
	require.Equal(t, "Neo.ClientError.General.TransactionOutOfMemoryError", code)
}

type memoryHTTPClient struct {
	t      *testing.T
	base   string
	token  string
	client *http.Client
}

func (c memoryHTTPClient) post(path string, statements ...string) (int, TransactionResponse, http.Header) {
	c.t.Helper()
	status, decoded, header, err := c.do(path, statements...)
	require.NoError(c.t, err)
	return status, decoded, header
}

// do is post for goroutines other than the test's: it returns errors
// instead of failing the test.
func (c memoryHTTPClient) do(path string, statements ...string) (int, TransactionResponse, http.Header, error) {
	body := map[string]any{"statements": []map[string]any{}}
	for _, statement := range statements {
		body["statements"] = append(body["statements"].([]map[string]any), map[string]any{"statement": statement})
	}
	encoded, err := json.Marshal(body)
	if err != nil {
		return 0, TransactionResponse{}, nil, err
	}
	request, err := http.NewRequest(http.MethodPost, c.base+path, strings.NewReader(string(encoded)))
	if err != nil {
		return 0, TransactionResponse{}, nil, err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", c.token)
	response, err := c.client.Do(request)
	if err != nil {
		return 0, TransactionResponse{}, nil, err
	}
	defer response.Body.Close()
	var decoded TransactionResponse
	if err := json.NewDecoder(response.Body).Decode(&decoded); err != nil {
		return response.StatusCode, decoded, response.Header, err
	}
	return response.StatusCode, decoded, response.Header, nil
}

// measureHTTPStatementPeak is the most the pools held while one statement
// ran alone (a limit must be configured, so it is tracked).
func measureHTTPStatementPeak(t *testing.T, client memoryHTTPClient, statement string) int64 {
	t.Helper()
	var peak int64
	done := make(chan error, 1)
	go func() {
		_, response, _, err := client.do("/db/nornic/tx/commit", statement)
		if err == nil && len(response.Errors) > 0 {
			err = fmt.Errorf("%s: %s", response.Errors[0].Code, response.Errors[0].Message)
		}
		done <- err
	}()
	for {
		select {
		case err := <-done:
			require.NoError(t, err)
			if peak == 0 {
				// Too fast to sample: at least the collected list.
				peak = 4 << 20
			}
			return peak
		default:
			if used, _ := cypher.TransactionMemoryPoolUsage("nornic"); used > peak {
				peak = used
			}
		}
	}
}

func requireHTTPMemoryError(t *testing.T, status int, response TransactionResponse, code, setting string) {
	t.Helper()
	require.Equal(t, http.StatusOK, status)
	require.Len(t, response.Errors, 1, "%+v", response)
	require.Equal(t, code, response.Errors[0].Code)
	message := response.Errors[0].Message
	require.True(t, strings.HasPrefix(message, "The allocation of an extra "), message)
	require.True(t, strings.HasSuffix(message, ". "+setting+" threshold reached"), message)
	for _, result := range response.Results {
		require.Empty(t, result.Data)
	}
}

func startMemoryHTTPServer(t *testing.T) (*Server, memoryHTTPClient, string) {
	t.Helper()
	server, authenticator := setupTestServer(t)
	t.Cleanup(func() {
		cypher.SetDatabaseTransactionMemoryLimits("nornic", cypher.TransactionMemoryLimits{})
		cypher.ConfigureTransactionMemory(0, cypher.TransactionMemoryLimits{})
	})
	local := httptest.NewServer(server.buildRouter())
	t.Cleanup(local.Close)
	token := "Bearer " + getAuthToken(t, authenticator, "admin")
	return server, memoryHTTPClient{t: t, base: local.URL, token: token, client: &http.Client{Timeout: 60 * time.Second}}, token
}

func TestHTTPApplicationMemoryAdmissionAndRecovery(t *testing.T) {
	server, client, _ := startMemoryHTTPServer(t)
	var used atomic.Int64
	var sampleFails atomic.Bool
	used.Store(950 << 10)
	var clock atomic.Int64
	clock.Store(time.Now().UnixNano())
	policy := memory.NewPolicy(1<<20, 128<<10, time.Second, func() (memory.Sample, error) {
		if sampleFails.Load() {
			return memory.Sample{}, errors.New("cgroup reading failed")
		}
		return memory.Sample{Used: used.Load(), Ceiling: 1 << 20}, nil
	}, func() time.Time { return time.Unix(0, clock.Load()) })
	require.NoError(t, policy.Refresh())
	server.memoryPolicy = policy

	status, body, _ := client.post("/db/nornic/tx/commit", "RETURN 1")
	require.Equal(t, http.StatusServiceUnavailable, status)
	require.Len(t, body.Errors, 1)
	require.Equal(t, "Neo.TransientError.General.MemoryPoolOutOfMemoryError", body.Errors[0].Code)
	require.Equal(t, int64(0), policy.Reserved())

	// A failed refresh must close admission at once, even though the last
	// successful sample remains within its freshness deadline.
	sampleFails.Store(true)
	require.ErrorIs(t, policy.Refresh(), memory.ErrStaleSample)
	status, body, _ = client.post("/db/nornic/tx/commit", "RETURN 1")
	require.Equal(t, http.StatusServiceUnavailable, status)
	require.Equal(t, "Neo.TransientError.General.MemoryPoolOutOfMemoryError", body.Errors[0].Code)
	require.Contains(t, body.Errors[0].Message, "readings are unavailable")
	require.Zero(t, policy.Reserved())
	sampleFails.Store(false)

	clock.Add(int64(2 * time.Second))
	status, body, _ = client.post("/db/nornic/tx/commit", "RETURN 1")
	require.Equal(t, http.StatusServiceUnavailable, status)
	require.Contains(t, body.Errors[0].Message, "readings are unavailable")

	used.Store(1)
	require.NoError(t, policy.Refresh())
	status, body, _ = client.post("/db/nornic/tx/commit", "RETURN 1")
	require.Equal(t, http.StatusOK, status)
	require.Empty(t, body.Errors)
	require.Equal(t, int64(0), policy.Reserved())
}

func TestHTTPApplicationMemoryReclaimsCompletedWork(t *testing.T) {
	server, client, _ := startMemoryHTTPServer(t)
	status, body, _ := client.post("/db/nornic/tx/commit", "RETURN 1 AS ok")
	require.Equal(t, http.StatusOK, status)
	require.Empty(t, body.Errors)
	policy := memorypressure.New(t)
	require.NoError(t, policy.Refresh())
	work, err := policy.Reserve(64 << 10)
	require.NoError(t, err)
	memorypressure.Garbage()
	work.Release()
	require.NoError(t, policy.Refresh())
	server.memoryPolicy = policy
	status, body, _ = client.post("/db/nornic/tx/commit", "RETURN 1 AS ok")
	require.Equal(t, http.StatusServiceUnavailable, status)
	require.Equal(t, "Neo.TransientError.General.MemoryPoolOutOfMemoryError", body.Errors[0].Code)

	ctx, cancel := context.WithCancel(context.Background())
	ticks := time.NewTicker(25 * time.Millisecond)
	done := make(chan struct{})
	go func() { defer close(done); policy.Run(ctx, ticks.C) }()
	t.Cleanup(func() { cancel(); ticks.Stop(); <-done })
	require.Eventually(t, func() bool {
		status, body, _, err := client.do("/db/nornic/tx/commit", "RETURN 1 AS ok")
		return err == nil && status == http.StatusOK && len(body.Errors) == 0 && len(body.Results) == 1 && len(body.Results[0].Data) == 1
	}, 3*time.Second, 25*time.Millisecond, "small HTTP queries must recover without manual GC")
	require.Zero(t, policy.Reserved())
}

func TestHTTPTransactionMemoryLimit(t *testing.T) {
	server, client, token := startMemoryHTTPServer(t)

	// db.memory.transaction.max is dynamic: it applies without a restart.
	response := makeRequest(t, server, http.MethodPut, "/admin/databases/nornic/config",
		map[string]any{"overrides": map[string]string{"db.memory.transaction.max": "1m"}}, token)
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	var config map[string]any
	require.NoError(t, json.NewDecoder(response.Body).Decode(&config))
	require.Equal(t, false, config["pendingRestart"], "%v", config)
	require.Equal(t, "1048576", config["configured"].(map[string]any)["db.memory.transaction.max"], "%v", config)
	require.Equal(t, "1048576", config["effective"].(map[string]any)["db.memory.transaction.max"], "%v", config)

	status, body, _ := client.post("/db/nornic/tx/commit", httpHeavyCollect)
	requireHTTPMemoryError(t, status, body, "Neo.ClientError.General.TransactionOutOfMemoryError", "db.memory.transaction.max")
	status, body, _ = client.post("/db/nornic/tx/commit", "RETURN 1 AS x")
	require.Equal(t, http.StatusOK, status)
	require.Empty(t, body.Errors)

	// The database's config reads the new value back.
	response = makeRequest(t, server, http.MethodGet, "/admin/databases/nornic/config", nil, token)
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	config = map[string]any{}
	require.NoError(t, json.NewDecoder(response.Body).Decode(&config))
	require.Equal(t, "1048576", config["effective"].(map[string]any)["db.memory.transaction.max"], "%v", config)

	// SHOW SETTINGS lists the three settings.
	status, body, _ = client.post("/db/nornic/tx/commit", "SHOW SETTINGS YIELD name, value WHERE name CONTAINS 'memory.transaction' RETURN name, value ORDER BY name")
	require.Equal(t, http.StatusOK, status)
	require.Empty(t, body.Errors)
	require.Len(t, body.Results, 1)
	settings := map[string]any{}
	for _, row := range body.Results[0].Data {
		settings[row.Row[0].(string)] = row.Row[1]
	}
	require.Equal(t, "1048576", settings["db.memory.transaction.max"], "%+v", body)
	require.Contains(t, settings, "db.memory.transaction.total.max")
	require.Contains(t, settings, "dbms.memory.transaction.total.max")

	// An open transaction over the limit is rolled back.
	status, body, header := client.post("/db/nornic/tx", "CREATE (:HTTPMemTx)")
	require.Equal(t, http.StatusCreated, status, "%+v", body)
	location := header.Get("Location")
	require.NotEmpty(t, location)
	txPath := location[strings.Index(location, "/db/"):]
	status, body, _ = client.post(txPath, httpHeavyCollect)
	requireHTTPMemoryError(t, status, body, "Neo.ClientError.General.TransactionOutOfMemoryError", "db.memory.transaction.max")
	status, body, _ = client.post(txPath, "RETURN 1")
	require.Equal(t, http.StatusNotFound, status, "%+v", body)
	require.Equal(t, "Neo.ClientError.Transaction.TransactionNotFound", body.Errors[0].Code)
	status, body, _ = client.post("/db/nornic/tx/commit", "MATCH (n:HTTPMemTx) RETURN count(n) AS c")
	require.Equal(t, http.StatusOK, status)
	require.Equal(t, float64(0), body.Results[0].Data[0].Row[0])

	global, database := cypher.TransactionMemoryPoolUsage("nornic")
	require.Zero(t, global)
	require.Zero(t, database)
}

// TestHTTPTransactionMemoryPoolConcurrentAggregations runs six heavy
// aggregations at once under a pool that holds about two, with small
// statements alongside: the server stays up, each heavy statement either
// completes or fails with MemoryPoolOutOfMemoryError, every small one
// succeeds, and the pool is empty afterwards.
func TestHTTPTransactionMemoryPoolConcurrentAggregations(t *testing.T) {
	_, client, _ := startMemoryHTTPServer(t)
	status, _, _ := client.post("/db/nornic/tx/commit", "RETURN 1")
	require.Equal(t, http.StatusOK, status)
	// Size the pool from one statement's peak, so it holds about two.
	cypher.ConfigureTransactionMemory(1<<40, cypher.TransactionMemoryLimits{})
	peak := measureHTTPStatementPeak(t, client, httpHeavyCollect)
	cypher.ConfigureTransactionMemory(peak*5/2, cypher.TransactionMemoryLimits{})

	const heavy = 6
	succeeded, failed := 0, 0
	for round := 0; round < 20 && (succeeded == 0 || failed == 0); round++ {
		var wg sync.WaitGroup
		start := make(chan struct{})
		type outcome struct {
			status   int
			response TransactionResponse
			err      error
		}
		heavyOutcomes := make([]outcome, heavy)
		smallOutcomes := make([]outcome, heavy)
		for worker := 0; worker < heavy; worker++ {
			wg.Add(2)
			go func(worker int) {
				defer wg.Done()
				<-start
				status, response, _, err := client.do("/db/nornic/tx/commit", httpHeavyCollect+fmt.Sprintf(", %d AS tag", round*heavy+worker))
				heavyOutcomes[worker] = outcome{status, response, err}
			}(worker)
			go func(worker int) {
				defer wg.Done()
				<-start
				status, response, _, err := client.do("/db/nornic/tx/commit", "UNWIND range(1, 100) AS x RETURN collect(x) AS xs")
				smallOutcomes[worker] = outcome{status, response, err}
			}(worker)
		}
		close(start)
		wg.Wait()
		for worker := 0; worker < heavy; worker++ {
			small := smallOutcomes[worker]
			require.NoError(t, small.err)
			require.Equal(t, http.StatusOK, small.status)
			require.Empty(t, small.response.Errors, "a small statement failed")
			result := heavyOutcomes[worker]
			require.NoError(t, result.err)
			if len(result.response.Errors) == 0 {
				succeeded++
				require.Len(t, result.response.Results, 1, "%+v", result.response)
				require.Len(t, result.response.Results[0].Data, 1, "%+v", result.response)
				require.Equal(t, float64(200000), result.response.Results[0].Data[0].Row[0])
				continue
			}
			failed++
			requireHTTPMemoryError(t, result.status, result.response, "Neo.TransientError.General.MemoryPoolOutOfMemoryError", "dbms.memory.transaction.total.max")
		}
		global, _ := cypher.TransactionMemoryPoolUsage("nornic")
		require.Zero(t, global, "the pool holds no memory once the statements ended")
	}
	require.Positive(t, succeeded, "no heavy statement completed under the pool")
	require.Positive(t, failed, "the pool never refused a heavy statement")
	status, body, _ := client.post("/db/nornic/tx/commit", "RETURN 1 AS up")
	require.Equal(t, http.StatusOK, status)
	require.Empty(t, body.Errors)
}
