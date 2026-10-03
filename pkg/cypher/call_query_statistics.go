package cypher

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type queryStatisticsCollector struct {
	registry   *queryStatisticsRegistry
	active     atomic.Bool
	mu         sync.Mutex
	generation uint64
	deadline   time.Time
	queries    map[string]*queryStatisticsRecord
	order      []string
}

type queryStatisticsRegistry struct {
	mu         sync.Mutex
	collectors map[string]*queryStatisticsCollector
}

func (registry *queryStatisticsRegistry) forDatabase(name string) *queryStatisticsCollector {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.collectors == nil {
		registry.collectors = make(map[string]*queryStatisticsCollector)
	}
	if collector := registry.collectors[name]; collector != nil {
		return collector
	}
	// Query collection is on from the start, as in Neo4j 5.26: a fresh
	// database reports "collecting" and records every query until
	// db.stats.stop('QUERIES').
	collector := &queryStatisticsCollector{registry: registry, generation: 1}
	collector.active.Store(true)
	registry.collectors[name] = collector
	return collector
}

type queryStatisticsRecord struct {
	invocations                    []queryStatisticsInvocation
	count, total, minimum, maximum int64
}

// queryStatisticsInvocation is one recorded execution; db.stats.retrieve
// renders it as an invocation map.
type queryStatisticsInvocation struct {
	elapsedUs, startMillis int64
}

// ShareQueryStatisticsFrom shares a database's collector with a fresh executor.
// Configure this before executing queries; transaction state remains independent.
// For example, a protocol transaction executor can share its cached database
// executor's collector while keeping a separate storage transaction.
//
//	transactionExecutor := NewStorageExecutor(databaseStorage)
//	transactionExecutor.ShareQueryStatisticsFrom(databaseExecutor)
func (e *StorageExecutor) ShareQueryStatisticsFrom(source *StorageExecutor) {
	if source == nil {
		return
	}
	e.queryStatistics = source.queryStatistics
	if source.queryStatistics != nil && source.queryStatistics.registry != nil {
		e.queryStatistics = source.queryStatistics.registry.forDatabase(e.currentDatabaseName())
	}
}

func (collector *queryStatisticsCollector) start(query string, now time.Time) uint64 {
	if collector == nil || !collector.active.Load() || len(query) > 65536 || containsFold(query, "db.stats.") {
		return 0
	}
	collector.mu.Lock()
	defer collector.mu.Unlock()
	if !collector.deadline.IsZero() && !now.Before(collector.deadline) {
		collector.active.Store(false)
	}
	if !collector.active.Load() {
		return 0
	}
	return collector.generation
}

func (collector *queryStatisticsCollector) record(generation uint64, query string, started time.Time, duration time.Duration, success bool) {
	if collector == nil || generation == 0 || !success {
		return
	}
	collector.mu.Lock()
	defer collector.mu.Unlock()
	if generation != collector.generation {
		return
	}
	if collector.queries == nil {
		collector.queries = make(map[string]*queryStatisticsRecord)
	}
	if _, exists := collector.queries[query]; !exists {
		if len(collector.order) == 1000 {
			delete(collector.queries, collector.order[0])
			collector.order = collector.order[1:]
		}
		collector.order = append(collector.order, query)
		collector.queries[query] = &queryStatisticsRecord{}
	}
	record := collector.queries[query]
	elapsed := duration.Microseconds()
	record.count++
	record.total += elapsed
	if record.count == 1 || elapsed < record.minimum {
		record.minimum = elapsed
	}
	if elapsed > record.maximum {
		record.maximum = elapsed
	}
	invocations := record.invocations
	if len(invocations) == 100 {
		invocations = invocations[1:]
	}
	record.invocations = append(invocations, queryStatisticsInvocation{elapsedUs: elapsed, startMillis: started.UnixMilli()})
}

func (e *StorageExecutor) callQueryStatistics(ctx context.Context, action string, arguments []interface{}) (*ExecuteResult, error) {
	collector := e.queryStatistics
	if collector == nil {
		return nil, newSemanticError("Neo.ClientError.Procedure.ProcedureCallFailed", "StatisticsUnavailable", "query statistics collector is unavailable")
	}
	section := "QUERIES"
	if len(arguments) > 0 {
		var valid bool
		section, valid = arguments[0].(string)
		if !valid {
			return nil, newSemanticError("Neo.ClientError.Statement.TypeError", "InvalidArgumentType", "statistics section must be a STRING")
		}
		section = strings.ToUpper(section)
	}
	if section == "ALL" && action != "retrieve" {
		section = "QUERIES"
	}
	configuration := map[string]interface{}{}
	if len(arguments) > 1 {
		var valid bool
		configuration, valid = toStringAnyMap(arguments[1])
		if expression, raw := arguments[1].(string); raw {
			configuration, valid = toStringAnyMap(e.evaluateExpressionWithContext(ctx, expression, nil, nil))
		}
		if !valid {
			return nil, newSemanticError("Neo.ClientError.Statement.TypeError", "InvalidArgumentType", "statistics configuration must be a MAP")
		}
	}
	if section != "QUERIES" {
		if action == "retrieve" && (section == "GRAPH COUNTS" || section == "TOKENS" || section == "META" || section == "ALL") {
			return e.retrieveGraphStatistics(ctx, section)
		}
		message := fmt.Sprintf("Unknown section '%s', known sections are [GRAPH COUNTS, TOKENS, QUERIES]", section)
		if section == "GRAPH COUNTS" || section == "TOKENS" {
			message = fmt.Sprintf("Section '%s' does not have to be explicitly collected, it can always be directly retrieved.", section)
		}
		return nil, newSemanticError("Neo.ClientError.General.InvalidArguments", "InvalidArgument", message)
	}
	collector.mu.Lock()
	defer collector.mu.Unlock()
	if !collector.deadline.IsZero() && !time.Now().Before(collector.deadline) {
		collector.active.Store(false)
	}
	result := &ExecuteResult{Columns: []string{"section", "success", "message"}, Rows: [][]interface{}{}}
	switch action {
	case "collect":
		durationSeconds := int64(-1)
		if value, exists := configuration["durationSeconds"]; exists {
			if !isIntegerProcedureValue(value) || toInt64(value) < -1 || toInt64(value) > int64((1<<63-1)/time.Second) {
				return nil, newSemanticError("Neo.ClientError.General.InvalidArguments", "InvalidArgument", "durationSeconds must be an INTEGER >= -1")
			}
			durationSeconds = toInt64(value)
		}
		if collector.active.Load() {
			result.Rows = [][]interface{}{{section, true, "Collection is already ongoing."}}
			return result, nil
		}
		collector.generation++
		collector.deadline = time.Time{}
		if durationSeconds > 0 {
			collector.deadline = time.Now().Add(time.Duration(durationSeconds) * time.Second)
		}
		collector.active.Store(true)
		result.Rows = [][]interface{}{{section, true, "Collection started."}}
	case "stop":
		collector.active.Store(false)
		result.Rows = [][]interface{}{{section, true, "Collection stopped."}}
	case "clear":
		if collector.active.Load() {
			result.Rows = [][]interface{}{{section, false, "Collected data cannot be cleared while collecting."}}
			return result, nil
		}
		collector.generation++
		collector.queries = nil
		collector.order = nil
		result.Rows = [][]interface{}{{section, true, "Data cleared."}}
	case "status":
		status := "idle"
		if collector.active.Load() {
			status = "collecting"
		}
		result.Columns = []string{"section", "status", "data"}
		result.Rows = [][]interface{}{{section, status, map[string]interface{}{}}}
	case "retrieve":
		maximum := int64(100)
		if value, exists := configuration["maxInvocations"]; exists {
			if !isIntegerProcedureValue(value) || toInt64(value) < 0 {
				return nil, newSemanticError("Neo.ClientError.General.InvalidArguments", "InvalidArgument", "maxInvocations must be a nonnegative INTEGER")
			}
			maximum = toInt64(value)
		}
		result.Columns = []string{"section", "data"}
		for _, query := range collector.order {
			record := collector.queries[query]
			invocations := record.invocations
			copies := make([]interface{}, 0)
			for _, invocation := range invocations {
				if int64(len(copies)) < maximum {
					copies = append(copies, map[string]interface{}{
						"elapsedCompileTimeInUs":   nil,
						"elapsedExecutionTimeInUs": invocation.elapsedUs,
						"startTimestampMillis":     invocation.startMillis,
					})
				}
			}
			result.Rows = append(result.Rows, []interface{}{section, map[string]interface{}{
				"query":              query,
				"queryExecutionPlan": nil,
				"estimatedRows":      nil,
				"invocations":        copies,
				"invocationSummary": map[string]interface{}{
					"invocationCount":   record.count,
					"compileTimeInUs":   nil,
					"executionTimeInUs": map[string]interface{}{"min": record.minimum, "max": record.maximum, "avg": record.total / record.count},
				},
			}})
		}
	}
	return result, nil
}

func (e *StorageExecutor) retrieveGraphStatistics(ctx context.Context, section string) (*ExecuteResult, error) {
	store := e.getStorage(ctx)
	nodes, err := store.AllNodes()
	if err != nil {
		return nil, err
	}
	edges, err := store.AllEdges()
	if err != nil {
		return nil, err
	}
	labels, relationships, properties := map[string]bool{}, map[string]bool{}, map[string]bool{}
	labelCounts, typeCounts := map[string]int64{}, map[string]int64{}
	for _, node := range nodes {
		for _, label := range node.Labels {
			labels[label] = true
			labelCounts[label]++
		}
		for key := range node.Properties {
			properties[key] = true
		}
	}
	for _, edge := range edges {
		relationships[edge.Type] = true
		typeCounts[edge.Type]++
		for key := range edge.Properties {
			properties[key] = true
		}
	}
	orderedLabels := store.GetSchema().OrderTokens(sortedStringSet(labels), false)
	orderedRelationships := store.GetSchema().OrderTokens(sortedStringSet(relationships), true)
	tokens := map[string]interface{}{"labels": orderedLabels, "relationshipTypes": orderedRelationships, "propertyKeys": sortedStringSet(properties)}
	result := &ExecuteResult{Columns: []string{"section", "data"}, Rows: [][]interface{}{}}
	if section == "GRAPH COUNTS" || section == "ALL" {
		nodeRows := []interface{}{map[string]interface{}{"count": int64(len(nodes))}}
		for _, label := range orderedLabels {
			nodeRows = append(nodeRows, map[string]interface{}{"label": label, "count": labelCounts[label]})
		}
		edgeRows := []interface{}{map[string]interface{}{"count": int64(len(edges))}}
		for _, relationship := range orderedRelationships {
			edgeRows = append(edgeRows, map[string]interface{}{"relationshipType": relationship, "count": typeCounts[relationship]})
		}
		scoped := e.cloneWithStorage(store)
		constraints, err := scoped.executeShowConstraints(ctx, "SHOW CONSTRAINTS YIELD *")
		if err != nil {
			return nil, err
		}
		constraintRows := make([]interface{}, 0, len(constraints.Rows))
		for _, row := range constraints.Rows {
			values := make(map[string]interface{}, len(constraints.Columns))
			for index, column := range constraints.Columns {
				values[column] = row[index]
			}
			constraintRows = append(constraintRows, values)
		}
		result.Rows = append(result.Rows, []interface{}{"GRAPH COUNTS", map[string]interface{}{"nodes": nodeRows, "relationships": edgeRows, "indexes": store.GetSchema().GetIndexes(), "constraints": constraintRows, "nodeCount": int64(len(nodes)), "relationshipCount": int64(len(edges))}})
	}
	if section == "TOKENS" || section == "ALL" {
		result.Rows = append(result.Rows, []interface{}{"TOKENS", tokens})
	}
	if section == "META" || section == "ALL" {
		result.Rows = append(result.Rows, []interface{}{"META", map[string]interface{}{
			"labelCount": int64(len(labels)), "relationshipTypeCount": int64(len(relationships)), "propertyKeyCount": int64(len(properties)),
			"retrieveTime": time.Now().UTC(), "graphToken": nil,
			"system": map[string]interface{}{"runtime": runtime.Version(), "osName": runtime.GOOS, "osArch": runtime.GOARCH, "availableProcessors": int64(runtime.NumCPU())},
		}})
	}
	if section == "ALL" {
		queries, err := e.callQueryStatistics(ctx, "retrieve", []interface{}{"QUERIES"})
		if err != nil {
			return nil, err
		}
		result.Rows = append(result.Rows, queries.Rows...)
	}
	return result, nil
}
