// migrate_neo4j_to_nornic.go
// Migrates memories from Neo4j to NornicDB with timing comparison
//
// Usage: go run scripts/migrate_neo4j_to_nornic.go
//
// Prerequisites:
//   - Neo4j running on localhost:7688 (bolt)
//   - NornicDB running on localhost:7687 (bolt) or 7474 (http)

package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

// Config
var (
	// Neo4j (source) - REMOTE SERVER (READ-ONLY!)
	neo4jBoltURL = "bolt://192.168.1.167:7687"
	neo4jHTTPURL = "http://192.168.1.167:7474"
	neo4jUser    = "neo4j"
	neo4jPass    = "password"

	// NornicDB (target) - LOCAL
	nornicHTTPURL = "http://localhost:7474"
	nornicUser    = "admin"
	nornicPass    = "admin"
)

// Timing stats
type TimingStats struct {
	Operation   string
	Neo4jTime   time.Duration
	NornicTime  time.Duration
	RecordCount int
}

var allStats []TimingStats

func main() {
	fmt.Println("╔════════════════════════════════════════════════════════════════╗")
	fmt.Println("║       Neo4j → NornicDB Migration with Timing Comparison        ║")
	fmt.Println("╚════════════════════════════════════════════════════════════════╝")
	fmt.Println()

	// Step 1: Clear NornicDB
	fmt.Println("📋 Step 1: Clearing NornicDB...")
	if err := clearNornicDB(); err != nil {
		fmt.Printf("   ⚠️  Warning: %v\n", err)
	} else {
		fmt.Println("   ✓ NornicDB cleared")
	}
	fmt.Println()

	// Step 2: Fetch all nodes from Neo4j
	fmt.Println("📥 Step 2: Fetching nodes from Neo4j...")
	nodes, neo4jReadTime, err := fetchNodesFromNeo4j()
	if err != nil {
		fmt.Printf("   ❌ Error: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("   ✓ Fetched %d nodes in %v\n", len(nodes), neo4jReadTime)
	fmt.Println()

	// Step 3: Insert nodes into NornicDB
	fmt.Println("📤 Step 3: Inserting nodes into NornicDB...")
	nornicWriteTime, err := insertNodesIntoNornic(nodes)
	if err != nil {
		fmt.Printf("   ❌ Error: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("   ✓ Inserted %d nodes in %v\n", len(nodes), nornicWriteTime)
	fmt.Println()

	// Step 4: Verify counts
	fmt.Println("🔍 Step 4: Verifying migration...")
	verifyMigration(len(nodes))
	fmt.Println()

	// Step 5: Run comparison queries
	fmt.Println("⏱️  Step 5: Running comparison queries...")
	runComparisonQueries()
	fmt.Println()

	// Print summary
	printSummary()
}

func basicAuth(user, pass string) string {
	return "Basic " + base64.StdEncoding.EncodeToString([]byte(user+":"+pass))
}

func clearNornicDB() error {
	// Delete all nodes
	query := map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": "MATCH (n) DETACH DELETE n"},
		},
	}

	body, _ := json.Marshal(query)
	req, _ := http.NewRequest("POST", nornicHTTPURL+"/db/neo4j/tx/commit", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", basicAuth(nornicUser, nornicPass))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

type Neo4jNode struct {
	ID         string                 `json:"id"`
	Labels     []string               `json:"labels"`
	Properties map[string]interface{} `json:"properties"`
}

func fetchNodesFromNeo4j() ([]Neo4jNode, time.Duration, error) {
	start := time.Now()

	query := map[string]interface{}{
		"statements": []map[string]interface{}{
			{
				// Fetch all nodes with content (memories, concepts, etc) - excludes FileChunk/File bulk data
				"statement":          "MATCH (n:Node) WHERE NOT n:FileChunk AND NOT n:File AND NOT n:NodeChunk RETURN n, labels(n) as labels, id(n) as id LIMIT 10000",
				"resultDataContents": []string{"row"},
			},
		},
	}

	body, _ := json.Marshal(query)
	req, _ := http.NewRequest("POST", neo4jHTTPURL+"/db/neo4j/tx/commit", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", basicAuth(neo4jUser, neo4jPass))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("neo4j connection failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return nil, 0, fmt.Errorf("neo4j status %d: %s", resp.StatusCode, string(body))
	}

	var result struct {
		Results []struct {
			Data []struct {
				Row []interface{} `json:"row"`
			} `json:"data"`
		} `json:"results"`
		Errors []struct {
			Message string `json:"message"`
		} `json:"errors"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, 0, err
	}

	if len(result.Errors) > 0 {
		return nil, 0, fmt.Errorf("neo4j error: %s", result.Errors[0].Message)
	}

	elapsed := time.Since(start)

	var nodes []Neo4jNode
	if len(result.Results) > 0 {
		for _, row := range result.Results[0].Data {
			if len(row.Row) >= 3 {
				props, _ := row.Row[0].(map[string]interface{})
				labels, _ := row.Row[1].([]interface{})
				id := fmt.Sprintf("%v", row.Row[2])

				labelStrs := make([]string, len(labels))
				for i, l := range labels {
					labelStrs[i] = fmt.Sprintf("%v", l)
				}

				nodes = append(nodes, Neo4jNode{
					ID:         id,
					Labels:     labelStrs,
					Properties: props,
				})
			}
		}
	}

	allStats = append(allStats, TimingStats{
		Operation:   "Read all nodes",
		Neo4jTime:   elapsed,
		RecordCount: len(nodes),
	})

	return nodes, elapsed, nil
}

func insertNodesIntoNornic(nodes []Neo4jNode) (time.Duration, error) {
	start := time.Now()

	// Build batch insert statements
	var statements []map[string]interface{}
	for i, node := range nodes {
		// Build label string
		labelStr := ""
		for j, l := range node.Labels {
			if j > 0 {
				labelStr += ":"
			}
			labelStr += l
		}
		if labelStr == "" {
			labelStr = "Node"
		}

		// Copy properties and ensure unique ID to avoid conflicts
		props := make(map[string]interface{})
		for k, v := range node.Properties {
			props[k] = v
		}

		// Generate a unique migration ID if node doesn't have one or to avoid conflicts
		originalID := ""
		if id, ok := props["id"].(string); ok {
			originalID = id
		}
		// Create a new unique ID for migration, store original as neo4j_id
		props["id"] = fmt.Sprintf("migrated-%d-%s", i, time.Now().Format("20060102150405"))
		if originalID != "" {
			props["neo4j_id"] = originalID
		}

		// Build CREATE statement with unique ID
		stmt := fmt.Sprintf("CREATE (n:%s $props) RETURN id(n)", labelStr)
		statements = append(statements, map[string]interface{}{
			"statement": stmt,
			"parameters": map[string]interface{}{
				"props": props,
			},
		})
	}

	// Send in batches of 10 (smaller batch to avoid issues)
	batchSize := 10
	successCount := 0
	for i := 0; i < len(statements); i += batchSize {
		end := i + batchSize
		if end > len(statements) {
			end = len(statements)
		}
		batch := statements[i:end]

		query := map[string]interface{}{
			"statements": batch,
		}

		body, _ := json.Marshal(query)
		req, _ := http.NewRequest("POST", nornicHTTPURL+"/db/neo4j/tx/commit", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", basicAuth(nornicUser, nornicPass))

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			fmt.Printf("   ⚠️  Batch %d-%d failed (network): %v\n", i+1, end, err)
			continue
		}

		respBody, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		if resp.StatusCode != 200 {
			fmt.Printf("   ⚠️  Batch %d-%d failed (status %d): %s\n", i+1, end, resp.StatusCode, string(respBody)[:min(200, len(respBody))])
			continue
		}

		// Check for Cypher errors in response
		var result struct {
			Errors []struct {
				Message string `json:"message"`
			} `json:"errors"`
		}
		json.Unmarshal(respBody, &result)
		if len(result.Errors) > 0 {
			fmt.Printf("   ⚠️  Batch %d-%d had errors: %s\n", i+1, end, result.Errors[0].Message)
			continue
		}

		successCount += len(batch)

		fmt.Printf("   ... inserted batch %d-%d\n", i+1, end)
	}

	elapsed := time.Since(start)
	allStats = append(allStats, TimingStats{
		Operation:   "Write all nodes",
		NornicTime:  elapsed,
		RecordCount: len(nodes),
	})

	return elapsed, nil
}

func verifyMigration(expectedCount int) {
	// Count nodes in NornicDB
	query := map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": "MATCH (n) RETURN count(n) as count"},
		},
	}

	body, _ := json.Marshal(query)
	req, _ := http.NewRequest("POST", nornicHTTPURL+"/db/neo4j/tx/commit", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", basicAuth(nornicUser, nornicPass))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Printf("   ⚠️  Verification failed: %v\n", err)
		return
	}
	defer resp.Body.Close()

	var result struct {
		Results []struct {
			Data []struct {
				Row []interface{} `json:"row"`
			} `json:"data"`
		} `json:"results"`
	}
	json.NewDecoder(resp.Body).Decode(&result)

	if len(result.Results) > 0 && len(result.Results[0].Data) > 0 {
		count := result.Results[0].Data[0].Row[0]
		fmt.Printf("   ✓ NornicDB node count: %v (expected: %d)\n", count, expectedCount)
	}
}

func runComparisonQueries() {
	queries := []struct {
		name  string
		query string
	}{
		{"Count all nodes", "MATCH (n) RETURN count(n)"},
		{"Get Memory nodes", "MATCH (n:Memory) RETURN n LIMIT 10"},
		{"Get nodes with content", "MATCH (n) WHERE n.content IS NOT NULL RETURN n LIMIT 10"},
		{"Count by label", "MATCH (n) RETURN labels(n)[0] as label, count(n) as count"},
	}

	for _, q := range queries {
		fmt.Printf("\n   Query: %s\n", q.name)

		// Run on Neo4j
		neo4jTime := runQuery(neo4jHTTPURL, neo4jUser, neo4jPass, q.query)
		fmt.Printf("      Neo4j:   %v\n", neo4jTime)

		// Run on NornicDB
		nornicTime := runQuery(nornicHTTPURL, nornicUser, nornicPass, q.query)
		fmt.Printf("      NornicDB: %v\n", nornicTime)

		allStats = append(allStats, TimingStats{
			Operation:  q.name,
			Neo4jTime:  neo4jTime,
			NornicTime: nornicTime,
		})
	}
}

func runQuery(baseURL, user, pass, query string) time.Duration {
	start := time.Now()

	q := map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": query},
		},
	}

	body, _ := json.Marshal(q)
	req, _ := http.NewRequest("POST", baseURL+"/db/neo4j/tx/commit", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", basicAuth(user, pass))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return -1
	}
	defer resp.Body.Close()
	io.ReadAll(resp.Body) // Drain body

	return time.Since(start)
}

func printSummary() {
	fmt.Println("╔════════════════════════════════════════════════════════════════╗")
	fmt.Println("║                        TIMING SUMMARY                          ║")
	fmt.Println("╠════════════════════════════════════════════════════════════════╣")
	fmt.Printf("║ %-25s │ %10s │ %10s │ %6s ║\n", "Operation", "Neo4j", "NornicDB", "Winner")
	fmt.Println("╠════════════════════════════════════════════════════════════════╣")

	for _, stat := range allStats {
		neo4jStr := "-"
		nornicStr := "-"
		winner := "-"

		if stat.Neo4jTime > 0 {
			neo4jStr = stat.Neo4jTime.Round(time.Microsecond).String()
		}
		if stat.NornicTime > 0 {
			nornicStr = stat.NornicTime.Round(time.Microsecond).String()
		}

		if stat.Neo4jTime > 0 && stat.NornicTime > 0 {
			if stat.NornicTime < stat.Neo4jTime {
				winner = "Nornic"
			} else if stat.Neo4jTime < stat.NornicTime {
				winner = "Neo4j"
			} else {
				winner = "Tie"
			}
		}

		fmt.Printf("║ %-25s │ %10s │ %10s │ %6s ║\n",
			truncate(stat.Operation, 25), neo4jStr, nornicStr, winner)
	}

	fmt.Println("╚════════════════════════════════════════════════════════════════╝")
}

func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen-3] + "..."
}
