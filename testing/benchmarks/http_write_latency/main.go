package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// HTTP write performance benchmark harness for NornicDB
//
// This tool benchmarks HTTP write latency by sending concurrent POST requests
// to the /db/{database}/tx/commit endpoint and measuring response times.
//
// Usage:
//
//	# Start NornicDB server first (in another terminal)
//	./nornicdb --http-port 7474 --data-dir ./data/test
//
//	# Run benchmark (local)
//	go run testing/benchmarks/http_write_latency/main.go \
//		-url http://localhost:7474 \
//		-database testdb \
//		-requests 1000 \
//		-concurrency 10 \
//		-auth admin:admin
//
//	# Run benchmark (sit2, Neo4j HTTP over base path /nornic-db; all writes go to testdb via URL path)
//	go run testing/benchmarks/http_write_latency/main.go \
//		-url https://remote-url.com/remote-path \
//		-database testdb \
//		-requests 1000 \
//		-concurrency 10 \
//		-auth admin:password
//
//	# With pprof profiling (requires NORNICDB_PPROF_ENABLED=true on server)
//	go run testing/benchmarks/http_write_latency/main.go \
//		-url http://localhost:7474 \
//		-pprof-url http://127.0.0.1:9091 \
//		-database testdb \
//		-requests 1000 \
//		-concurrency 10 \
//		-auth admin:admin \
//		-pprof-enabled \
//		-pprof-duration 30s
func main() {
	var (
		url           = flag.String("url", "http://localhost:7474", "NornicDB HTTP server URL")
		database      = flag.String("database", "testdb", "Database name (used in URL path /db/{database}/tx/commit)")
		requests      = flag.Int("requests", 1000, "Total number of requests")
		concurrency   = flag.Int("concurrency", runtime.GOMAXPROCS(0), "Number of concurrent goroutines")
		auth          = flag.String("auth", "admin:password", "Basic auth credentials (username:password)")
		pprofEnabled  = flag.Bool("pprof-enabled", false, "Collect a CPU profile from the pprof listener")
		pprofURL      = flag.String("pprof-url", "http://127.0.0.1:9091", "pprof listener URL")
		pprofDuration = flag.Duration("pprof-duration", 30*time.Second, "Duration for pprof CPU profile")
		warmup        = flag.Int("warmup", 10, "Number of warmup requests")
		verbose       = flag.Bool("verbose", false, "Print detailed per-request stats")
	)
	flag.Parse()

	fmt.Printf("HTTP Write Performance Benchmark\n")
	fmt.Printf("================================\n")
	fmt.Printf("URL:           %s\n", *url)
	fmt.Printf("Database:      %s\n", *database)
	fmt.Printf("Requests:      %d\n", *requests)
	fmt.Printf("Concurrency:   %d\n", *concurrency)
	fmt.Printf("Warmup:        %d\n", *warmup)
	fmt.Printf("Pprof enabled: %v\n", *pprofEnabled)
	if *pprofEnabled {
		fmt.Printf("Pprof URL:     %s\n", *pprofURL)
	}
	fmt.Printf("\n")

	// Create HTTP client with connection pooling
	client := &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        *concurrency * 2,
			MaxIdleConnsPerHost: *concurrency * 2,
			IdleConnTimeout:     60 * time.Second,
			DisableKeepAlives:   false,
		},
	}

	// Prepare auth header - use JWT token if auth provided (more efficient than Basic Auth per request)
	authHeader := ""
	if *auth != "" {
		username, password, ok := parseAuth(*auth)
		if ok {
			// Get JWT token once and reuse it (avoids per-request auth overhead and lockout issues)
			token, err := getJWTToken(*url, username, password)
			if err != nil {
				fmt.Printf("Warning: Failed to get JWT token, falling back to Basic Auth: %v\n", err)
				authHeader = basicAuth(username, password)
			} else {
				authHeader = "Bearer " + token
				fmt.Printf("Using JWT token for authentication (more efficient)\n")
			}
		}
	}

	// Warmup phase
	fmt.Printf("Warming up...\n")
	for i := 0; i < *warmup; i++ {
		req := createWriteRequest(*url, *database, authHeader, i)
		resp, err := client.Do(req)
		if err != nil {
			fmt.Printf("Warmup request %d failed: %v\n", i, err)
			continue
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
	fmt.Printf("Warmup complete.\n\n")

	// Start pprof profiling if enabled
	var pprofCmd *exec.Cmd
	var pprofDone chan struct{}
	if *pprofEnabled {
		fmt.Printf("Starting pprof CPU profile (duration: %v)...\n", *pprofDuration)
		pprofDone = make(chan struct{})
		profileURL := fmt.Sprintf("%s/debug/pprof/profile", strings.TrimRight(*pprofURL, "/"))
		pprofCmd = exec.Command("go", "tool", "pprof",
			"-proto",
			"-seconds", fmt.Sprintf("%.0f", pprofDuration.Seconds()),
			profileURL,
		)
		pprofCmd.Stdout = os.Stdout
		pprofCmd.Stderr = os.Stderr
		if err := pprofCmd.Start(); err != nil {
			fmt.Printf("Warning: Failed to start pprof: %v\n", err)
		} else {
			go func() {
				pprofCmd.Wait()
				close(pprofDone)
			}()
		}
	}

	// Benchmark phase
	fmt.Printf("Starting benchmark...\n")
	startTime := time.Now()

	var (
		latencies    = make([]time.Duration, 0, *requests)
		latenciesMu  sync.Mutex
		successCount int64
		errorCount   int64
		totalBytes   int64
		wg           sync.WaitGroup
	)

	// Create request queue
	requestQueue := make(chan int, *requests)
	for i := 0; i < *requests; i++ {
		requestQueue <- i
	}
	close(requestQueue)

	// Start worker goroutines
	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for reqID := range requestQueue {
				reqStart := time.Now()
				req := createWriteRequest(*url, *database, authHeader, reqID)

				resp, err := client.Do(req)
				if err != nil {
					atomic.AddInt64(&errorCount, 1)
					if *verbose {
						fmt.Printf("[Worker %d] Request %d failed: %v\n", workerID, reqID, err)
					}
					continue
				}

				// Read response body
				bodyBytes, readErr := io.ReadAll(resp.Body)
				resp.Body.Close()

				latency := time.Since(reqStart)

				if readErr != nil {
					atomic.AddInt64(&errorCount, 1)
					if *verbose {
						fmt.Printf("[Worker %d] Request %d read error: %v\n", workerID, reqID, readErr)
					}
					continue
				}

				// Accept both 200 (OK) and 202 (Accepted) as success
				if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
					atomic.AddInt64(&errorCount, 1)
					if *verbose {
						fmt.Printf("[Worker %d] Request %d status %d: %s\n", workerID, reqID, resp.StatusCode, string(bodyBytes))
					}
					continue
				}

				// Success
				atomic.AddInt64(&successCount, 1)
				atomic.AddInt64(&totalBytes, int64(len(bodyBytes)))

				latenciesMu.Lock()
				latencies = append(latencies, latency)
				latenciesMu.Unlock()

				if *verbose {
					fmt.Printf("[Worker %d] Request %d: %v\n", workerID, reqID, latency)
				}
			}
		}(i)
	}

	// Wait for all requests to complete
	wg.Wait()
	endTime := time.Now()

	// Wait for pprof to finish
	if pprofCmd != nil {
		fmt.Printf("\nWaiting for pprof to finish...\n")
		select {
		case <-pprofDone:
			fmt.Printf("Pprof profile complete.\n")
		case <-time.After(*pprofDuration + 5*time.Second):
			fmt.Printf("Pprof profile timeout.\n")
			if pprofCmd.Process != nil {
				pprofCmd.Process.Kill()
			}
		}
	}

	// Calculate statistics
	totalDuration := endTime.Sub(startTime)
	success := int(successCount)
	errors := int(errorCount)

	if success == 0 {
		fmt.Printf("\n❌ No successful requests!\n")
		os.Exit(1)
	}

	// Sort latencies for percentile calculation
	latenciesMu.Lock()
	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})
	latenciesMu.Unlock()

	// Calculate percentiles
	p50 := latencies[len(latencies)*50/100]
	p95 := latencies[len(latencies)*95/100]
	p99 := latencies[len(latencies)*99/100]
	p999 := latencies[len(latencies)*999/1000]
	min := latencies[0]
	max := latencies[len(latencies)-1]

	// Calculate average
	var totalLatency time.Duration
	for _, lat := range latencies {
		totalLatency += lat
	}
	avg := totalLatency / time.Duration(len(latencies))

	// Calculate throughput
	throughput := float64(success) / totalDuration.Seconds()
	avgLatencyMs := avg.Seconds() * 1000

	// Print results
	fmt.Printf("\n")
	fmt.Printf("Results\n")
	fmt.Printf("=======\n")
	fmt.Printf("Total duration:     %v\n", totalDuration)
	fmt.Printf("Successful:         %d\n", success)
	fmt.Printf("Errors:            %d\n", errors)
	fmt.Printf("Success rate:       %.2f%%\n", float64(success)/float64(success+errors)*100)
	fmt.Printf("Throughput:         %.2f req/s\n", throughput)
	fmt.Printf("Total bytes:        %d\n", totalBytes)
	fmt.Printf("Avg bytes/req:      %d\n", totalBytes/int64(success))
	fmt.Printf("\n")
	fmt.Printf("Latency Statistics\n")
	fmt.Printf("------------------\n")
	fmt.Printf("Min:                %v (%.2f ms)\n", min, min.Seconds()*1000)
	fmt.Printf("P50 (median):       %v (%.2f ms)\n", p50, p50.Seconds()*1000)
	fmt.Printf("P95:                %v (%.2f ms)\n", p95, p95.Seconds()*1000)
	fmt.Printf("P99:                %v (%.2f ms)\n", p99, p99.Seconds()*1000)
	fmt.Printf("P99.9:              %v (%.2f ms)\n", p999, p999.Seconds()*1000)
	fmt.Printf("Max:                %v (%.2f ms)\n", max, max.Seconds()*1000)
	fmt.Printf("Average:            %v (%.2f ms)\n", avg, avgLatencyMs)
	fmt.Printf("\n")

	// Save detailed results to file
	resultsFile := filepath.Join(os.TempDir(), fmt.Sprintf("http_write_bench_%d.json", time.Now().Unix()))
	results := map[string]interface{}{
		"url":         *url,
		"database":    *database,
		"requests":    *requests,
		"concurrency": *concurrency,
		"duration":    totalDuration.String(),
		"success":     success,
		"errors":      errors,
		"throughput":  throughput,
		"latencies": map[string]float64{
			"min_ms":  min.Seconds() * 1000,
			"p50_ms":  p50.Seconds() * 1000,
			"p95_ms":  p95.Seconds() * 1000,
			"p99_ms":  p99.Seconds() * 1000,
			"p999_ms": p999.Seconds() * 1000,
			"max_ms":  max.Seconds() * 1000,
			"avg_ms":  avgLatencyMs,
		},
	}
	resultsJSON, _ := json.MarshalIndent(results, "", "  ")
	os.WriteFile(resultsFile, resultsJSON, 0644)
	fmt.Printf("Detailed results saved to: %s\n", resultsFile)
}

func createWriteRequest(url, database, authHeader string, reqID int) *http.Request {
	// Create a simple write query: CREATE (n:TestNode {id: $id})
	query := map[string]interface{}{
		"statements": []map[string]interface{}{
			{
				"statement": "CREATE (n:TestNode {id: $id, timestamp: $timestamp}) RETURN n.id as id",
				"parameters": map[string]interface{}{
					"id":        reqID,
					"timestamp": time.Now().UnixNano(),
				},
			},
		},
	}

	body, _ := json.Marshal(query)
	req, _ := http.NewRequest("POST", fmt.Sprintf("%s/db/%s/tx/commit", url, database), bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if authHeader != "" {
		req.Header.Set("Authorization", authHeader)
	}
	return req
}

func parseAuth(auth string) (username, password string, ok bool) {
	idx := -1
	for i, c := range auth {
		if c == ':' {
			idx = i
			break
		}
	}
	if idx < 0 {
		return "", "", false
	}
	return auth[:idx], auth[idx+1:], true
}

func basicAuth(username, password string) string {
	auth := username + ":" + password
	return "Basic " + base64.StdEncoding.EncodeToString([]byte(auth))
}

// getJWTToken obtains a JWT token from the /auth/token endpoint
func getJWTToken(baseURL, username, password string) (string, error) {
	reqBody := map[string]string{
		"username": username,
		"password": password,
	}
	body, err := json.Marshal(reqBody)
	if err != nil {
		return "", err
	}

	resp, err := http.Post(baseURL+"/auth/token", "application/json", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("token request failed: %d %s", resp.StatusCode, string(bodyBytes))
	}

	var tokenResp struct {
		AccessToken string `json:"access_token"`
		TokenType   string `json:"token_type"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&tokenResp); err != nil {
		return "", err
	}

	return tokenResp.AccessToken, nil
}
