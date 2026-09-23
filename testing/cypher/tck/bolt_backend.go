package tck

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/dbtype"
	"github.com/orneryd/nornicdb/pkg/cypher"
)

// TransactionMode selects how the statement under test is sent through Bolt.
type TransactionMode string

const (
	// AutocommitMode executes each statement with Session.Run.
	AutocommitMode TransactionMode = "autocommit"
	// ExplicitTransactionMode executes each statement in a new explicit driver
	// transaction, consuming all records before commit and rolling back errors.
	ExplicitTransactionMode TransactionMode = "explicit-transaction"
)

// BoltBackendConfig configures the production-driver TCK backend.
type BoltBackendConfig struct {
	Driver          neo4j.DriverWithContext
	DatabaseName    string
	Mode            TransactionMode
	NamedGraphsRoot string
}

// BoltBackend executes TCK operations through the official Neo4j Go driver.
type BoltBackend struct {
	driver          neo4j.DriverWithContext
	databaseName    string
	mode            TransactionMode
	namedGraphsRoot string
}

// NewBoltBackend constructs a backend over an already-connected driver.
func NewBoltBackend(config BoltBackendConfig) (*BoltBackend, error) {
	if config.Driver == nil {
		return nil, fmt.Errorf("Bolt driver is nil")
	}
	if config.Mode != AutocommitMode && config.Mode != ExplicitTransactionMode {
		return nil, fmt.Errorf("unsupported transaction mode %q", config.Mode)
	}
	return &BoltBackend{
		driver:          config.Driver,
		databaseName:    config.DatabaseName,
		mode:            config.Mode,
		namedGraphsRoot: config.NamedGraphsRoot,
	}, nil
}

// Reset removes all graph entities through Bolt.
func (b *BoltBackend) Reset(ctx context.Context) error {
	cypher.ClearUserProcedures()
	if _, err := b.executeAutocommit(ctx, "MATCH (n) DETACH DELETE n", nil); err != nil {
		return err
	}
	if err := b.dropSchemaObjects(ctx, "SHOW CONSTRAINTS", "name", "DROP CONSTRAINT"); err != nil {
		return err
	}
	if err := b.dropSchemaObjects(ctx, "SHOW INDEXES", "name", "DROP INDEX"); err != nil {
		return err
	}
	_, err := b.executeAutocommit(ctx, "CALL db.clearQueryCaches()", nil)
	return err
}

func (b *BoltBackend) dropSchemaObjects(ctx context.Context, showQuery, nameColumn, dropPrefix string) error {
	result, err := b.executeAutocommit(ctx, showQuery, nil)
	if err != nil {
		return err
	}
	nameIndex := -1
	for index, column := range result.Columns {
		if column == nameColumn {
			nameIndex = index
			break
		}
	}
	if nameIndex < 0 {
		return fmt.Errorf("%s did not return column %q", showQuery, nameColumn)
	}
	for _, row := range result.Rows {
		if nameIndex >= len(row) {
			return fmt.Errorf("%s returned a row without column %q", showQuery, nameColumn)
		}
		name, ok := row[nameIndex].(string)
		if !ok || strings.TrimSpace(name) == "" {
			return fmt.Errorf("%s returned invalid schema object name %v", showQuery, row[nameIndex])
		}
		escapedName := strings.ReplaceAll(name, "`", "``")
		if _, err := b.executeAutocommit(ctx, dropPrefix+" `"+escapedName+"` IF EXISTS", nil); err != nil {
			return fmt.Errorf("drop schema object %q: %w", name, err)
		}
	}
	return nil
}

// LoadNamedGraph executes the pinned graph fixture with committed setup writes.
func (b *BoltBackend) LoadNamedGraph(ctx context.Context, name string) error {
	if b.namedGraphsRoot == "" {
		return fmt.Errorf("named graph root is not configured")
	}
	path := filepath.Join(b.namedGraphsRoot, name, name+".cypher")
	content, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read named graph %q: %w", name, err)
	}
	query := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(string(content)), ";"))
	if query == "" {
		return fmt.Errorf("named graph %q is empty", name)
	}
	_, err = b.executeAutocommit(ctx, query, nil)
	if err != nil {
		return fmt.Errorf("load named graph %q: %w", name, err)
	}
	return nil
}

// Execute runs and fully consumes a statement in the configured mode.
func (b *BoltBackend) Execute(ctx context.Context, query string, params map[string]any) (QueryResult, error) {
	if b.mode == ExplicitTransactionMode {
		return b.executeExplicit(ctx, query, params)
	}
	return b.executeAutocommit(ctx, query, params)
}

// Snapshot observes graph entities through fresh autocommit reads.
func (b *BoltBackend) Snapshot(ctx context.Context) (GraphSnapshot, error) {
	nodesResult, err := b.executeAutocommit(ctx, "MATCH (n) RETURN n", nil)
	if err != nil {
		return GraphSnapshot{}, fmt.Errorf("observe nodes: %w", err)
	}
	relationshipsResult, err := b.executeAutocommit(ctx, "MATCH ()-[r]->() RETURN r", nil)
	if err != nil {
		return GraphSnapshot{}, fmt.Errorf("observe relationships: %w", err)
	}
	return graphSnapshotFromResults(nodesResult, relationshipsResult)
}

func graphSnapshotFromResults(nodesResult, relationshipsResult QueryResult) (GraphSnapshot, error) {
	snapshot := GraphSnapshot{
		Nodes:         make([]NodeValue, 0, len(nodesResult.Rows)),
		Relationships: make([]RelationshipValue, 0, len(relationshipsResult.Rows)),
	}
	for rowIndex, row := range nodesResult.Rows {
		if len(row) != 1 {
			return GraphSnapshot{}, fmt.Errorf("node observer row %d has width %d", rowIndex, len(row))
		}
		node, ok := row[0].(NodeValue)
		if !ok {
			return GraphSnapshot{}, fmt.Errorf("node observer row %d returned %T", rowIndex, row[0])
		}
		snapshot.Nodes = append(snapshot.Nodes, node)
	}
	for rowIndex, row := range relationshipsResult.Rows {
		if len(row) != 1 {
			return GraphSnapshot{}, fmt.Errorf("relationship observer row %d has width %d", rowIndex, len(row))
		}
		relationship, ok := row[0].(RelationshipValue)
		if !ok {
			return GraphSnapshot{}, fmt.Errorf("relationship observer row %d returned %T", rowIndex, row[0])
		}
		snapshot.Relationships = append(snapshot.Relationships, relationship)
	}
	return snapshot, nil
}

func (b *BoltBackend) executeAutocommit(ctx context.Context, query string, params map[string]any) (result QueryResult, err error) {
	session := b.driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: b.databaseName})
	defer func() { err = errors.Join(err, session.Close(ctx)) }()
	cursor, err := session.Run(ctx, query, params)
	if err != nil {
		return QueryResult{}, classifyBoltError(err)
	}
	return consumeResult(ctx, cursor)
}

func (b *BoltBackend) executeExplicit(ctx context.Context, query string, params map[string]any) (result QueryResult, err error) {
	session := b.driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: b.databaseName})
	defer func() { err = errors.Join(err, session.Close(ctx)) }()
	tx, err := session.BeginTransaction(ctx)
	if err != nil {
		return QueryResult{}, classifyBoltError(err)
	}
	committed := false
	defer func() {
		if !committed {
			err = errors.Join(err, tx.Rollback(ctx))
		}
	}()
	cursor, err := tx.Run(ctx, query, params)
	if err != nil {
		return QueryResult{}, classifyBoltError(err)
	}
	result, err = consumeResult(ctx, cursor)
	if err != nil {
		return QueryResult{}, err
	}
	inTransaction, err := snapshotWithRunner(ctx, tx.Run)
	if err != nil {
		return QueryResult{}, fmt.Errorf("observe explicit transaction before commit: %w", err)
	}
	if err = tx.Commit(ctx); err != nil {
		return QueryResult{}, classifyBoltError(err)
	}
	committed = true
	fromFreshSession, err := b.Snapshot(ctx)
	if err != nil {
		return QueryResult{}, fmt.Errorf("observe explicit transaction after commit: %w", err)
	}
	if err := compareGraphSnapshots(inTransaction, fromFreshSession); err != nil {
		return QueryResult{}, fmt.Errorf("explicit transaction commit visibility: %w", err)
	}
	return result, nil
}

type boltQueryRunner func(context.Context, string, map[string]any) (neo4j.ResultWithContext, error)

func snapshotWithRunner(ctx context.Context, run boltQueryRunner) (GraphSnapshot, error) {
	nodeCursor, err := run(ctx, "MATCH (n) RETURN n", nil)
	if err != nil {
		return GraphSnapshot{}, classifyBoltError(err)
	}
	nodesResult, err := consumeResult(ctx, nodeCursor)
	if err != nil {
		return GraphSnapshot{}, err
	}
	relationshipCursor, err := run(ctx, "MATCH ()-[r]->() RETURN r", nil)
	if err != nil {
		return GraphSnapshot{}, classifyBoltError(err)
	}
	relationshipsResult, err := consumeResult(ctx, relationshipCursor)
	if err != nil {
		return GraphSnapshot{}, err
	}
	return graphSnapshotFromResults(nodesResult, relationshipsResult)
}

func consumeResult(ctx context.Context, cursor neo4j.ResultWithContext) (QueryResult, error) {
	columns, err := cursor.Keys()
	if err != nil {
		return QueryResult{}, classifyBoltError(err)
	}
	result := QueryResult{Columns: append([]string(nil), columns...)}
	var record *neo4j.Record
	for cursor.NextRecord(ctx, &record) {
		row := make([]any, len(record.Values))
		for i, value := range record.Values {
			converted, convertErr := convertBoltValue(value)
			if convertErr != nil {
				return QueryResult{}, fmt.Errorf("convert column %q: %w", columns[i], convertErr)
			}
			row[i] = converted
		}
		result.Rows = append(result.Rows, row)
	}
	if err := cursor.Err(); err != nil {
		return QueryResult{}, classifyBoltError(err)
	}
	if _, err := cursor.Consume(ctx); err != nil {
		return QueryResult{}, classifyBoltError(err)
	}
	return result, nil
}

func convertBoltValue(value any) (any, error) {
	switch v := value.(type) {
	case neo4j.Node:
		return nodeFromBolt(v), nil
	case neo4j.Relationship:
		return relationshipFromBolt(v), nil
	case neo4j.Path:
		return pathFromBolt(v), nil
	case []any:
		result := make([]any, len(v))
		for i := range v {
			converted, err := convertBoltValue(v[i])
			if err != nil {
				return nil, err
			}
			result[i] = converted
		}
		return result, nil
	case map[string]any:
		result := make(map[string]any, len(v))
		for key, item := range v {
			converted, err := convertBoltValue(item)
			if err != nil {
				return nil, err
			}
			result[key] = converted
		}
		return result, nil
	case dbtype.Date:
		return v.Time().Format("2006-01-02"), nil
	case dbtype.LocalTime:
		return formatBoltTemporalClock(v.Time(), false), nil
	case dbtype.Time:
		return formatBoltTemporalClock(v.Time(), true), nil
	case dbtype.LocalDateTime:
		return formatBoltTemporalDateTime(v.Time(), false), nil
	case time.Time:
		return formatBoltTemporalDateTime(v, true), nil
	case dbtype.Duration:
		return formatBoltDuration(v), nil
	case nil, bool, string, int, int32, int64, float32, float64:
		return v, nil
	default:
		return nil, fmt.Errorf("unsupported Bolt value %T", value)
	}
}

func formatBoltTemporalClock(value time.Time, zoned bool) string {
	format := "15:04"
	if value.Second() != 0 || value.Nanosecond() != 0 {
		format = "15:04:05"
		if value.Nanosecond() != 0 {
			format += ".999999999"
		}
	}
	result := value.Format(format)
	if zoned {
		_, offset := value.Zone()
		result += formatBoltTemporalOffset(offset)
	}
	return result
}

func formatBoltTemporalDateTime(value time.Time, zoned bool) string {
	result := value.Format("2006-01-02T") + formatBoltTemporalClock(value, zoned)
	if zoned {
		zoneID := value.Location().String()
		if zoneID != "" && zoneID != "UTC" && zoneID != "Local" && !strings.HasPrefix(zoneID, "Offset") {
			result += "[" + zoneID + "]"
		}
	}
	return result
}

func formatBoltTemporalOffset(offset int) string {
	if offset == 0 {
		return "Z"
	}
	sign := '+'
	if offset < 0 {
		sign = '-'
		offset = -offset
	}
	hours := offset / 3600
	minutes := offset % 3600 / 60
	seconds := offset % 60
	if seconds == 0 {
		return fmt.Sprintf("%c%02d:%02d", sign, hours, minutes)
	}
	return fmt.Sprintf("%c%02d:%02d:%02d", sign, hours, minutes, seconds)
}

func formatBoltDuration(value dbtype.Duration) string {
	return cypher.FormatCypherDuration(value.Months, value.Days, value.Seconds, int64(value.Nanos))
}

func nodeFromBolt(node neo4j.Node) NodeValue {
	return NodeValue{
		Identity:   boltIdentity(node.ElementId, node.Id),
		Labels:     append([]string(nil), node.Labels...),
		Properties: cloneMap(node.Props),
	}
}

func relationshipFromBolt(relationship neo4j.Relationship) RelationshipValue {
	return RelationshipValue{
		Identity:      boltIdentity(relationship.ElementId, relationship.Id),
		Type:          relationship.Type,
		StartIdentity: boltIdentity(relationship.StartElementId, relationship.StartId),
		EndIdentity:   boltIdentity(relationship.EndElementId, relationship.EndId),
		Properties:    cloneMap(relationship.Props),
	}
}

func pathFromBolt(path neo4j.Path) PathValue {
	result := PathValue{Segments: make([]PathSegment, 0, len(path.Relationships))}
	if len(path.Nodes) == 0 {
		return result
	}

	nodesByIdentity := make(map[string]NodeValue, len(path.Nodes))
	for _, node := range path.Nodes {
		converted := nodeFromBolt(node)
		nodesByIdentity[converted.Identity] = converted
	}
	current := nodeFromBolt(path.Nodes[0])
	result.Nodes = append(result.Nodes, current)
	for _, relationship := range path.Relationships {
		converted := relationshipFromBolt(relationship)
		forward := false
		var nextIdentity string
		switch {
		case converted.StartIdentity == current.Identity:
			forward = true
			nextIdentity = converted.EndIdentity
		case converted.EndIdentity == current.Identity:
			nextIdentity = converted.StartIdentity
		default:
			return result
		}
		next, ok := nodesByIdentity[nextIdentity]
		if !ok {
			break
		}
		result.Segments = append(result.Segments, PathSegment{
			Relationship: converted,
			Forward:      forward,
		})
		result.Nodes = append(result.Nodes, next)
		current = next
	}
	return result
}

func boltIdentity(elementID string, legacyID int64) string {
	if elementID != "" {
		return elementID
	}
	return strconv.FormatInt(legacyID, 10)
}

func classifyBoltError(err error) error {
	var databaseErr *neo4j.Neo4jError
	if !errors.As(err, &databaseErr) {
		return err
	}
	parts := strings.Split(databaseErr.Code, ".")
	errorType := "Error"
	if len(parts) > 0 && parts[len(parts)-1] != "" {
		errorType = parts[len(parts)-1]
	}
	phase := "runtime"
	// The Neo4j status namespace identifies the subsystem, not the TCK phase.
	// Statement.TypeError and Statement.ArgumentError are runtime failures;
	// compile-time failures carry an explicitly compile-time error type.
	switch errorType {
	case "SyntaxError", "ParameterMissing", "ProcedureError":
		phase = "compile time"
	}
	detail := "*"
	if databaseErr.GqlStatus != "" {
		detail = databaseErr.GqlStatus
	}
	return &QueryError{Type: errorType, Phase: phase, Detail: detail, Cause: err}
}
