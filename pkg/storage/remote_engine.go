package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/dbtype"
)

// Compile-time interface assertion.
var _ Engine = (*RemoteEngine)(nil)

// RemoteEngineConfig configures a remote storage engine.
//
// The URI field determines the transport protocol:
//   - bolt://, bolt+s://, bolt+ssc://, neo4j://, neo4j+s://, neo4j+ssc:// → Bolt transport (preferred)
//   - http://, https:// → HTTP tx API transport (fallback)
type RemoteEngineConfig struct {
	// URI is the remote NornicDB endpoint (bolt:// or http:// scheme).
	URI string

	// Database is the target database name on the remote instance.
	Database string

	// AuthToken is the caller's authorization header value (e.g. "Bearer <token>"),
	// used for OIDC credential forwarding.
	AuthToken string

	// User and Password are used for explicit basic auth (user_password auth mode).
	User     string
	Password string

	// HTTPClient is an optional custom HTTP client, used only when transport is HTTP.
	HTTPClient *http.Client
}

// remoteTransport abstracts Bolt vs HTTP execution of Cypher statements.
type remoteTransport interface {
	query(ctx context.Context, statement string, params map[string]interface{}) ([][]interface{}, error)
	queryWithColumns(ctx context.Context, statement string, params map[string]interface{}) ([]string, [][]interface{}, error)
	queryBatch(ctx context.Context, statements []remoteStatement) error
	beginCypherTx(ctx context.Context) (RemoteCypherTx, error)
	close() error
}

// RemoteCypherTx represents an explicit remote Cypher transaction handle.
type RemoteCypherTx interface {
	QueryCypher(ctx context.Context, statement string, params map[string]interface{}) ([]string, [][]interface{}, error)
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}

// RemoteEngine implements Engine by forwarding operations to a remote NornicDB instance
// via Bolt protocol (preferred) or HTTP tx API (fallback), auto-detected from the URI scheme.
type RemoteEngine struct {
	transport remoteTransport
	schema    *SchemaManager
}

type remoteStatement struct {
	Statement  string                 `json:"statement"`
	Parameters map[string]interface{} `json:"parameters,omitempty"`
}

// NewRemoteEngine creates a remote engine. Transport is auto-detected from the URI scheme.
func NewRemoteEngine(cfg RemoteEngineConfig) (*RemoteEngine, error) {
	uri := strings.TrimSpace(cfg.URI)
	database := strings.TrimSpace(cfg.Database)
	if uri == "" {
		return nil, fmt.Errorf("remote engine URI cannot be empty")
	}
	if database == "" {
		return nil, fmt.Errorf("remote engine database cannot be empty")
	}

	var transport remoteTransport
	var err error

	lower := strings.ToLower(uri)
	switch {
	case strings.HasPrefix(lower, "bolt://"),
		strings.HasPrefix(lower, "bolt+s://"),
		strings.HasPrefix(lower, "bolt+ssc://"),
		strings.HasPrefix(lower, "neo4j://"),
		strings.HasPrefix(lower, "neo4j+s://"),
		strings.HasPrefix(lower, "neo4j+ssc://"):
		transport, err = newBoltTransport(uri, database, cfg)
	case strings.HasPrefix(lower, "http://"),
		strings.HasPrefix(lower, "https://"):
		transport, err = newHTTPTransport(uri, database, cfg)
	default:
		return nil, fmt.Errorf("unsupported remote engine URI scheme: %s (expected bolt://, neo4j://, http://, or https://)", uri)
	}
	if err != nil {
		return nil, err
	}

	return &RemoteEngine{
		transport: transport,
		schema:    NewSchemaManager(),
	}, nil
}

// ---------------------------------------------------------------------------
// Bolt transport
// ---------------------------------------------------------------------------

type boltTransport struct {
	driver   neo4j.DriverWithContext
	database string
}

type boltCypherTx struct {
	session neo4j.SessionWithContext
	tx      neo4j.ExplicitTransaction
}

func newBoltTransport(uri, database string, cfg RemoteEngineConfig) (*boltTransport, error) {
	auth := buildNeo4jAuth(cfg)
	driver, err := neo4j.NewDriverWithContext(uri, auth)
	if err != nil {
		return nil, fmt.Errorf("failed to create Bolt driver: %w", err)
	}
	return &boltTransport{driver: driver, database: database}, nil
}

func buildNeo4jAuth(cfg RemoteEngineConfig) neo4j.AuthToken {
	user := strings.TrimSpace(cfg.User)
	pass := strings.TrimSpace(cfg.Password)
	token := strings.TrimSpace(cfg.AuthToken)

	if user != "" || pass != "" {
		return neo4j.BasicAuth(user, pass, "")
	}
	if token != "" {
		// Strip "Bearer " prefix if present (case-insensitive per RFC 7235) —
		// neo4j.BearerAuth expects the raw token value without the scheme.
		raw := token
		if len(token) > 7 && strings.EqualFold(token[:7], "Bearer ") {
			raw = token[7:]
		}
		return neo4j.BearerAuth(raw)
	}
	return neo4j.NoAuth()
}

func (b *boltTransport) query(ctx context.Context, statement string, params map[string]interface{}) ([][]interface{}, error) {
	session := b.driver.NewSession(ctx, neo4j.SessionConfig{
		DatabaseName: b.database,
		AccessMode:   accessModeForStatement(statement),
	})
	defer func() { _ = session.Close(ctx) }()

	var rows [][]interface{}
	_, err := executeManagedStatement(ctx, session, statement, params, func(_ []string, record *neo4j.Record) {
		row := make([]interface{}, len(record.Values))
		for i, v := range record.Values {
			row[i] = normalizeBoltValue(v)
		}
		rows = append(rows, row)
	})
	if err != nil {
		return nil, err
	}
	return rows, nil
}

func (b *boltTransport) queryWithColumns(ctx context.Context, statement string, params map[string]interface{}) ([]string, [][]interface{}, error) {
	session := b.driver.NewSession(ctx, neo4j.SessionConfig{
		DatabaseName: b.database,
		AccessMode:   accessModeForStatement(statement),
	})
	defer func() { _ = session.Close(ctx) }()

	rows := make([][]interface{}, 0)
	columns, err := executeManagedStatement(ctx, session, statement, params, func(_ []string, record *neo4j.Record) {
		row := make([]interface{}, len(record.Values))
		for i, v := range record.Values {
			row[i] = normalizeBoltValue(v)
		}
		rows = append(rows, row)
	})
	if err != nil {
		return nil, nil, err
	}
	return columns, rows, nil
}

func executeManagedStatement(
	ctx context.Context,
	session neo4j.SessionWithContext,
	statement string,
	params map[string]interface{},
	onRecord func(columns []string, record *neo4j.Record),
) ([]string, error) {
	runInTx := func(tx neo4j.ManagedTransaction) (interface{}, error) {
		result, err := tx.Run(ctx, statement, params)
		if err != nil {
			return nil, err
		}
		columns, err := result.Keys()
		if err != nil {
			return nil, fmt.Errorf("failed to get result keys: %w", err)
		}
		for result.Next(ctx) {
			onRecord(columns, result.Record())
		}
		if err := result.Err(); err != nil {
			return nil, err
		}
		return columns, nil
	}
	mode := accessModeForStatement(statement)
	if mode == neo4j.AccessModeWrite {
		v, err := session.ExecuteWrite(ctx, runInTx)
		if err != nil {
			return nil, err
		}
		cols, _ := v.([]string)
		return cols, nil
	}
	v, err := session.ExecuteRead(ctx, runInTx)
	if err != nil {
		return nil, err
	}
	cols, _ := v.([]string)
	return cols, nil
}

func (b *boltTransport) queryBatch(ctx context.Context, statements []remoteStatement) error {
	session := b.driver.NewSession(ctx, neo4j.SessionConfig{
		DatabaseName: b.database,
		AccessMode:   neo4j.AccessModeWrite,
	})
	defer func() { _ = session.Close(ctx) }()

	_, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (interface{}, error) {
		for _, stmt := range statements {
			result, err := tx.Run(ctx, stmt.Statement, stmt.Parameters)
			if err != nil {
				return nil, err
			}
			// Consume to ensure statement completes.
			if _, err := result.Consume(ctx); err != nil {
				return nil, err
			}
		}
		return nil, nil
	})
	return err
}

func (b *boltTransport) beginCypherTx(ctx context.Context) (RemoteCypherTx, error) {
	session := b.driver.NewSession(ctx, neo4j.SessionConfig{
		DatabaseName: b.database,
		AccessMode:   neo4j.AccessModeWrite,
	})
	tx, err := session.BeginTransaction(ctx)
	if err != nil {
		_ = session.Close(ctx)
		return nil, err
	}
	return &boltCypherTx{session: session, tx: tx}, nil
}

func (t *boltCypherTx) QueryCypher(ctx context.Context, statement string, params map[string]interface{}) ([]string, [][]interface{}, error) {
	result, err := t.tx.Run(ctx, statement, params)
	if err != nil {
		return nil, nil, err
	}
	columns, err := result.Keys()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get result keys: %w", err)
	}
	rows := make([][]interface{}, 0)
	for result.Next(ctx) {
		record := result.Record()
		row := make([]interface{}, len(record.Values))
		for i, v := range record.Values {
			row[i] = normalizeBoltValue(v)
		}
		rows = append(rows, row)
	}
	if err := result.Err(); err != nil {
		return nil, nil, err
	}
	return columns, rows, nil
}

func (t *boltCypherTx) Commit(ctx context.Context) error {
	defer func() { _ = t.session.Close(ctx) }()
	return t.tx.Commit(ctx)
}

func (t *boltCypherTx) Rollback(ctx context.Context) error {
	defer func() { _ = t.session.Close(ctx) }()
	return t.tx.Rollback(ctx)
}

func (b *boltTransport) close() error {
	return b.driver.Close(context.Background())
}

func accessModeForStatement(statement string) neo4j.AccessMode {
	upper := strings.ToUpper(statement)
	writeKeywords := []string{"CREATE", "MERGE", "DELETE", "DETACH DELETE", "SET ", "REMOVE ", "DROP ", "ALTER ", "RENAME ", "GRANT ", "REVOKE "}
	for _, kw := range writeKeywords {
		if strings.Contains(upper, kw) {
			return neo4j.AccessModeWrite
		}
	}
	return neo4j.AccessModeRead
}

// normalizeBoltValue converts neo4j dbtype values into the map shapes that
// valueToNode/valueToEdge expect, so all downstream code works for both transports.
func normalizeBoltValue(v interface{}) interface{} {
	switch n := v.(type) {
	case dbtype.Node:
		m := map[string]interface{}{
			"elementId":  n.ElementId,
			"labels":     toInterfaceSlice(n.Labels),
			"properties": n.Props,
		}
		if id, ok := n.Props["id"]; ok {
			m["id"] = id
		}
		return m
	case dbtype.Relationship:
		m := map[string]interface{}{
			"elementId":          n.ElementId,
			"type":               n.Type,
			"startNodeElementId": n.StartElementId,
			"endNodeElementId":   n.EndElementId,
			"properties":         n.Props,
		}
		if id, ok := n.Props["id"]; ok {
			m["id"] = id
		}
		return m
	default:
		return v
	}
}

func toInterfaceSlice(ss []string) []interface{} {
	out := make([]interface{}, len(ss))
	for i, s := range ss {
		out[i] = s
	}
	return out
}

// ---------------------------------------------------------------------------
// HTTP tx API transport
// ---------------------------------------------------------------------------

type httpTransport struct {
	baseURL   string
	database  string
	authToken string
	user      string
	password  string
	client    *http.Client
}

type httpCypherTx struct {
	transport   *httpTransport
	executeURL  string
	commitURL   string
	rollbackURL string
	closed      bool
}

type remoteTxRequest struct {
	Statements []remoteStatement `json:"statements"`
}

type remoteTxResponse struct {
	Results []struct {
		Columns []string `json:"columns"`
		Data    []struct {
			Row []interface{} `json:"row"`
		} `json:"data"`
	} `json:"results"`
	Errors []struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"errors"`
}

func newHTTPTransport(uri, database string, cfg RemoteEngineConfig) (*httpTransport, error) {
	client := cfg.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	return &httpTransport{
		baseURL:   strings.TrimRight(uri, "/"),
		database:  database,
		authToken: strings.TrimSpace(cfg.AuthToken),
		user:      strings.TrimSpace(cfg.User),
		password:  strings.TrimSpace(cfg.Password),
		client:    client,
	}, nil
}

func (h *httpTransport) commitURL() string {
	if strings.HasSuffix(h.baseURL, "/tx/commit") {
		return h.baseURL
	}
	if strings.Contains(h.baseURL, "/db/") {
		return h.baseURL + "/tx/commit"
	}
	return fmt.Sprintf("%s/db/%s/tx/commit", h.baseURL, h.database)
}

func (h *httpTransport) txOpenURL() string {
	if strings.HasSuffix(h.baseURL, "/tx/commit") {
		return strings.TrimSuffix(h.baseURL, "/commit")
	}
	if strings.Contains(h.baseURL, "/db/") {
		return h.baseURL + "/tx"
	}
	return fmt.Sprintf("%s/db/%s/tx", h.baseURL, h.database)
}

func (h *httpTransport) doRequest(ctx context.Context, body remoteTxRequest) (*remoteTxResponse, error) {
	return h.doRequestToURL(ctx, http.MethodPost, h.commitURL(), body)
}

func (h *httpTransport) doRequestToURL(ctx context.Context, method string, targetURL string, body interface{}) (*remoteTxResponse, error) {
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, targetURL, reader)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if h.user != "" || h.password != "" {
		req.SetBasicAuth(h.user, h.password)
	} else if h.authToken != "" {
		req.Header.Set("Authorization", h.authToken)
	}
	resp, err := h.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var txResp remoteTxResponse
	if err := json.Unmarshal(respBody, &txResp); err != nil {
		return nil, fmt.Errorf("remote tx decode failed (status=%d): %w", resp.StatusCode, err)
	}
	if len(txResp.Errors) > 0 {
		first := txResp.Errors[0]
		return nil, fmt.Errorf("%s: %s", first.Code, first.Message)
	}
	return &txResp, nil
}

func (h *httpTransport) query(ctx context.Context, statement string, params map[string]interface{}) ([][]interface{}, error) {
	txResp, err := h.doRequest(ctx, remoteTxRequest{
		Statements: []remoteStatement{{Statement: statement, Parameters: params}},
	})
	if err != nil {
		return nil, err
	}
	if len(txResp.Results) == 0 {
		return [][]interface{}{}, nil
	}
	rows := make([][]interface{}, 0, len(txResp.Results[0].Data))
	for _, data := range txResp.Results[0].Data {
		rows = append(rows, data.Row)
	}
	return rows, nil
}

func (h *httpTransport) queryWithColumns(ctx context.Context, statement string, params map[string]interface{}) ([]string, [][]interface{}, error) {
	txResp, err := h.doRequest(ctx, remoteTxRequest{
		Statements: []remoteStatement{{Statement: statement, Parameters: params}},
	})
	if err != nil {
		return nil, nil, err
	}
	if len(txResp.Results) == 0 {
		return []string{}, [][]interface{}{}, nil
	}
	result := txResp.Results[0]
	rows := make([][]interface{}, 0, len(result.Data))
	for _, data := range result.Data {
		rows = append(rows, data.Row)
	}
	return result.Columns, rows, nil
}

func (h *httpTransport) queryBatch(ctx context.Context, statements []remoteStatement) error {
	_, err := h.doRequest(ctx, remoteTxRequest{Statements: statements})
	return err
}

func (h *httpTransport) beginCypherTx(ctx context.Context) (RemoteCypherTx, error) {
	openBody := remoteTxRequest{Statements: []remoteStatement{}}
	raw, err := json.Marshal(openBody)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, h.txOpenURL(), bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if h.user != "" || h.password != "" {
		req.SetBasicAuth(h.user, h.password)
	} else if h.authToken != "" {
		req.Header.Set("Authorization", h.authToken)
	}
	resp, err := h.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var openResp struct {
		Commit string `json:"commit"`
		Errors []struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"errors"`
	}
	if err := json.Unmarshal(body, &openResp); err != nil {
		return nil, fmt.Errorf("remote tx open decode failed (status=%d): %w", resp.StatusCode, err)
	}
	if len(openResp.Errors) > 0 {
		return nil, fmt.Errorf("%s: %s", openResp.Errors[0].Code, openResp.Errors[0].Message)
	}
	if strings.TrimSpace(openResp.Commit) == "" {
		return nil, fmt.Errorf("remote tx open returned empty commit URL")
	}
	executeURL := strings.TrimSuffix(openResp.Commit, "/commit")
	return &httpCypherTx{
		transport:   h,
		executeURL:  executeURL,
		commitURL:   openResp.Commit,
		rollbackURL: executeURL,
	}, nil
}

func (h *httpTransport) close() error { return nil }

func (t *httpCypherTx) QueryCypher(ctx context.Context, statement string, params map[string]interface{}) ([]string, [][]interface{}, error) {
	if t.closed {
		return nil, nil, fmt.Errorf("remote transaction is closed")
	}
	txResp, err := t.transport.doRequestToURL(ctx, http.MethodPost, t.executeURL, remoteTxRequest{
		Statements: []remoteStatement{{Statement: statement, Parameters: params}},
	})
	if err != nil {
		return nil, nil, err
	}
	if len(txResp.Results) == 0 {
		return []string{}, [][]interface{}{}, nil
	}
	result := txResp.Results[0]
	rows := make([][]interface{}, 0, len(result.Data))
	for _, data := range result.Data {
		rows = append(rows, data.Row)
	}
	return result.Columns, rows, nil
}

func (t *httpCypherTx) Commit(ctx context.Context) error {
	if t.closed {
		return nil
	}
	t.closed = true
	_, err := t.transport.doRequestToURL(ctx, http.MethodPost, t.commitURL, remoteTxRequest{Statements: []remoteStatement{}})
	return err
}

func (t *httpCypherTx) Rollback(ctx context.Context) error {
	if t.closed {
		return nil
	}
	t.closed = true
	_, err := t.transport.doRequestToURL(ctx, http.MethodDelete, t.rollbackURL, nil)
	return err
}

// ---------------------------------------------------------------------------
// Helpers shared across transports
// ---------------------------------------------------------------------------

func quoteIdent(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

func labelsExpr(labels []string) string {
	if len(labels) == 0 {
		return ""
	}
	parts := make([]string, 0, len(labels))
	for _, l := range labels {
		if strings.TrimSpace(l) == "" {
			continue
		}
		parts = append(parts, ":"+quoteIdent(l))
	}
	return strings.Join(parts, "")
}

func asMap(v interface{}) map[string]interface{} {
	if m, ok := v.(map[string]interface{}); ok {
		return m
	}
	return nil
}

func valueToNode(v interface{}) (*Node, error) {
	m := asMap(v)
	if m == nil {
		return nil, fmt.Errorf("expected node map, got %T", v)
	}
	props := asMap(m["properties"])
	if props == nil {
		props = map[string]interface{}{}
	}
	id := ""
	if s, ok := m["id"].(string); ok && s != "" {
		id = s
	} else if s, ok := props["id"].(string); ok && s != "" {
		id = s
	} else if s, ok := m["elementId"].(string); ok && s != "" {
		id = s
	}
	labels := []string{}
	if rawLabels, ok := m["labels"].([]interface{}); ok {
		for _, lv := range rawLabels {
			if s, ok := lv.(string); ok {
				labels = append(labels, s)
			}
		}
	}
	return &Node{
		ID:         NodeID(id),
		Labels:     labels,
		Properties: props,
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}, nil
}

func valueToEdge(v interface{}) (*Edge, error) {
	m := asMap(v)
	if m == nil {
		return nil, fmt.Errorf("expected relationship map, got %T", v)
	}
	props := asMap(m["properties"])
	if props == nil {
		props = map[string]interface{}{}
	}
	id := ""
	if s, ok := m["id"].(string); ok && s != "" {
		id = s
	} else if s, ok := props["id"].(string); ok && s != "" {
		id = s
	} else if s, ok := m["elementId"].(string); ok && s != "" {
		id = s
	}
	startNode := ""
	if s, ok := m["startNodeElementId"].(string); ok {
		startNode = s
	}
	endNode := ""
	if s, ok := m["endNodeElementId"].(string); ok {
		endNode = s
	}
	typ, _ := m["type"].(string)
	return &Edge{
		ID:         EdgeID(id),
		StartNode:  NodeID(startNode),
		EndNode:    NodeID(endNode),
		Type:       typ,
		Properties: props,
		CreatedAt:  time.Now(),
	}, nil
}

// remoteToInt64 converts a numeric value to int64.
// Named to avoid collision with pkg/cypher/type_conversion.go:toInt64.
func remoteToInt64(v interface{}) int64 {
	switch n := v.(type) {
	case int64:
		return n
	case int:
		return int64(n)
	case float64:
		return int64(n)
	default:
		return 0
	}
}

// defaultCtx creates a context with a 30-second timeout as a fallback
// for Engine methods that don't accept a caller-provided context.
func defaultCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 30*time.Second)
}

// ---------------------------------------------------------------------------
// Engine interface implementation
// ---------------------------------------------------------------------------

func (r *RemoteEngine) CreateNode(node *Node) (NodeID, error) {
	props := map[string]interface{}{}
	for k, v := range node.Properties {
		props[k] = v
	}
	if node.ID != "" {
		if _, ok := props["id"]; !ok {
			props["id"] = string(node.ID)
		}
	}
	stmt := fmt.Sprintf("CREATE (n%s) SET n += $props RETURN n", labelsExpr(node.Labels))
	ctx, cancel := defaultCtx()
	defer cancel()
	rows, err := r.transport.query(ctx, stmt, map[string]interface{}{"props": props})
	if err != nil {
		return "", err
	}
	if len(rows) == 0 || len(rows[0]) == 0 {
		return "", fmt.Errorf("remote create node returned no rows")
	}
	created, err := valueToNode(rows[0][0])
	if err != nil {
		return "", err
	}
	return created.ID, nil
}

func (r *RemoteEngine) GetNode(id NodeID) (*Node, error) {
	ctx, cancel := defaultCtx()
	defer cancel()
	rows, err := r.transport.query(ctx, "MATCH (n) WHERE n.id = $id RETURN n LIMIT 1", map[string]interface{}{"id": string(id)})
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, ErrNotFound
	}
	return valueToNode(rows[0][0])
}

func (r *RemoteEngine) UpdateNode(node *Node) error {
	ctx, cancel := defaultCtx()
	defer cancel()
	rows, err := r.transport.query(ctx, "MATCH (n) WHERE n.id = $id SET n += $props RETURN count(n)", map[string]interface{}{
		"id":    string(node.ID),
		"props": node.Properties,
	})
	if err != nil {
		return err
	}
	if len(rows) == 0 || len(rows[0]) == 0 || remoteToInt64(rows[0][0]) == 0 {
		return ErrNotFound
	}
	return nil
}

func (r *RemoteEngine) DeleteNode(id NodeID) error {
	ctx, cancel := defaultCtx()
	defer cancel()
	_, err := r.transport.query(ctx, "MATCH (n) WHERE n.id = $id DETACH DELETE n", map[string]interface{}{"id": string(id)})
	return err
}

func (r *RemoteEngine) CreateEdge(edge *Edge) error {
	if edge == nil {
		return ErrInvalidData
	}
	props := map[string]interface{}{}
	for k, v := range edge.Properties {
		props[k] = v
	}
	params := map[string]interface{}{
		"start": string(edge.StartNode),
		"end":   string(edge.EndNode),
		"props": props,
	}
	stmt := fmt.Sprintf(
		"MATCH (a),(b) WHERE a.id = $start AND b.id = $end CREATE (a)-[r:%s]->(b) SET r += $props RETURN r",
		quoteIdent(edge.Type),
	)
	if edge.ID != "" {
		props["id"] = string(edge.ID)
		params["id"] = string(edge.ID)
		stmt = fmt.Sprintf(
			"MATCH (a),(b) WHERE a.id = $start AND b.id = $end MERGE (a)-[r:%s {id: $id}]->(b) SET r += $props RETURN r",
			quoteIdent(edge.Type),
		)
	}
	ctx, cancel := defaultCtx()
	defer cancel()
	rows, err := r.transport.query(ctx, stmt, params)
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		return ErrNotFound
	}
	return nil
}

func (r *RemoteEngine) GetEdge(id EdgeID) (*Edge, error) {
	ctx, cancel := defaultCtx()
	defer cancel()
	rows, err := r.transport.query(ctx, "MATCH ()-[r]->() WHERE r.id = $id RETURN r LIMIT 1", map[string]interface{}{"id": string(id)})
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, ErrNotFound
	}
	return valueToEdge(rows[0][0])
}

func (r *RemoteEngine) UpdateEdge(edge *Edge) error {
	ctx, cancel := defaultCtx()
	defer cancel()
	rows, err := r.transport.query(ctx, "MATCH ()-[r]->() WHERE r.id = $id SET r += $props RETURN count(r)", map[string]interface{}{
		"id":    string(edge.ID),
		"props": edge.Properties,
	})
	if err != nil {
		return err
	}
	if len(rows) == 0 || len(rows[0]) == 0 || remoteToInt64(rows[0][0]) == 0 {
		return ErrNotFound
	}
	return nil
}

func (r *RemoteEngine) DeleteEdge(id EdgeID) error {
	ctx, cancel := defaultCtx()
	defer cancel()
	_, err := r.transport.query(ctx, "MATCH ()-[r]->() WHERE r.id = $id DELETE r", map[string]interface{}{"id": string(id)})
	return err
}

func (r *RemoteEngine) GetNodesByLabel(label string) ([]*Node, error) {
	stmt := "MATCH (n) RETURN n"
	if strings.TrimSpace(label) != "" {
		stmt = fmt.Sprintf("MATCH (n:%s) RETURN n", quoteIdent(label))
	}
	ctx, cancel := defaultCtx()
	defer cancel()
	rows, err := r.transport.query(ctx, stmt, nil)
	if err != nil {
		return nil, err
	}
	out := make([]*Node, 0, len(rows))
	for _, row := range rows {
		if len(row) == 0 {
			continue
		}
		n, err := valueToNode(row[0])
		if err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, nil
}

func (r *RemoteEngine) GetFirstNodeByLabel(label string) (*Node, error) {
	stmt := "MATCH (n) RETURN n LIMIT 1"
	if strings.TrimSpace(label) != "" {
		stmt = fmt.Sprintf("MATCH (n:%s) RETURN n LIMIT 1", quoteIdent(label))
	}
	ctx, cancel := defaultCtx()
	defer cancel()
	rows, err := r.transport.query(ctx, stmt, nil)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, ErrNotFound
	}
	return valueToNode(rows[0][0])
}

func (r *RemoteEngine) GetOutgoingEdges(nodeID NodeID) ([]*Edge, error) {
	ctx, cancel := defaultCtx()
	defer cancel()
	rows, err := r.transport.query(ctx, "MATCH (n)-[r]->() WHERE n.id = $id RETURN r", map[string]interface{}{"id": string(nodeID)})
	if err != nil {
		return nil, err
	}
	out := make([]*Edge, 0, len(rows))
	for _, row := range rows {
		if len(row) == 0 {
			continue
		}
		edge, err := valueToEdge(row[0])
		if err != nil {
			return nil, err
		}
		out = append(out, edge)
	}
	return out, nil
}

func (r *RemoteEngine) GetIncomingEdges(nodeID NodeID) ([]*Edge, error) {
	ctx, cancel := defaultCtx()
	defer cancel()
	rows, err := r.transport.query(ctx, "MATCH ()-[r]->(n) WHERE n.id = $id RETURN r", map[string]interface{}{"id": string(nodeID)})
	if err != nil {
		return nil, err
	}
	out := make([]*Edge, 0, len(rows))
	for _, row := range rows {
		if len(row) == 0 {
			continue
		}
		edge, err := valueToEdge(row[0])
		if err != nil {
			return nil, err
		}
		out = append(out, edge)
	}
	return out, nil
}

func (r *RemoteEngine) GetEdgesBetween(startID, endID NodeID) ([]*Edge, error) {
	ctx, cancel := defaultCtx()
	defer cancel()
	rows, err := r.transport.query(ctx, "MATCH (a)-[r]->(b) WHERE a.id = $start AND b.id = $end RETURN r", map[string]interface{}{
		"start": string(startID),
		"end":   string(endID),
	})
	if err != nil {
		return nil, err
	}
	out := make([]*Edge, 0, len(rows))
	for _, row := range rows {
		if len(row) == 0 {
			continue
		}
		edge, err := valueToEdge(row[0])
		if err != nil {
			return nil, err
		}
		out = append(out, edge)
	}
	return out, nil
}

func (r *RemoteEngine) GetEdgeBetween(startID, endID NodeID, edgeType string) *Edge {
	ctx, cancel := defaultCtx()
	defer cancel()
	rows, err := r.transport.query(ctx,
		fmt.Sprintf("MATCH (a)-[r:%s]->(b) WHERE a.id = $start AND b.id = $end RETURN r LIMIT 1", quoteIdent(edgeType)),
		map[string]interface{}{"start": string(startID), "end": string(endID)},
	)
	if err != nil || len(rows) == 0 || len(rows[0]) == 0 {
		return nil
	}
	edge, err := valueToEdge(rows[0][0])
	if err != nil {
		return nil
	}
	return edge
}

func (r *RemoteEngine) GetEdgesByType(edgeType string) ([]*Edge, error) {
	ctx, cancel := defaultCtx()
	defer cancel()
	rows, err := r.transport.query(ctx, fmt.Sprintf("MATCH ()-[r:%s]->() RETURN r", quoteIdent(edgeType)), nil)
	if err != nil {
		return nil, err
	}
	out := make([]*Edge, 0, len(rows))
	for _, row := range rows {
		if len(row) == 0 {
			continue
		}
		edge, err := valueToEdge(row[0])
		if err != nil {
			return nil, err
		}
		out = append(out, edge)
	}
	return out, nil
}

func (r *RemoteEngine) AllNodes() ([]*Node, error) { return r.GetNodesByLabel("") }

func (r *RemoteEngine) AllEdges() ([]*Edge, error) {
	ctx, cancel := defaultCtx()
	defer cancel()
	rows, err := r.transport.query(ctx, "MATCH ()-[r]->() RETURN r", nil)
	if err != nil {
		return nil, err
	}
	out := make([]*Edge, 0, len(rows))
	for _, row := range rows {
		if len(row) == 0 {
			continue
		}
		edge, err := valueToEdge(row[0])
		if err != nil {
			return nil, err
		}
		out = append(out, edge)
	}
	return out, nil
}

func (r *RemoteEngine) GetAllNodes() []*Node {
	nodes, err := r.AllNodes()
	if err != nil {
		return []*Node{}
	}
	return nodes
}

func (r *RemoteEngine) GetInDegree(nodeID NodeID) int {
	ctx, cancel := defaultCtx()
	defer cancel()
	rows, err := r.transport.query(ctx, "MATCH ()-[r]->(n) WHERE n.id = $id RETURN count(r)", map[string]interface{}{"id": string(nodeID)})
	if err != nil || len(rows) == 0 || len(rows[0]) == 0 {
		return 0
	}
	return int(remoteToInt64(rows[0][0]))
}

func (r *RemoteEngine) GetOutDegree(nodeID NodeID) int {
	ctx, cancel := defaultCtx()
	defer cancel()
	rows, err := r.transport.query(ctx, "MATCH (n)-[r]->() WHERE n.id = $id RETURN count(r)", map[string]interface{}{"id": string(nodeID)})
	if err != nil || len(rows) == 0 || len(rows[0]) == 0 {
		return 0
	}
	return int(remoteToInt64(rows[0][0]))
}

func (r *RemoteEngine) GetSchema() *SchemaManager { return r.schema }

// Bulk operations use batched queries to avoid N+1 round-trips.
const remoteBulkChunkSize = 100

func (r *RemoteEngine) BulkCreateNodes(nodes []*Node) error {
	ctx, cancel := defaultCtx()
	defer cancel()
	for i := 0; i < len(nodes); i += remoteBulkChunkSize {
		end := i + remoteBulkChunkSize
		if end > len(nodes) {
			end = len(nodes)
		}
		stmts := make([]remoteStatement, 0, end-i)
		for _, node := range nodes[i:end] {
			props := map[string]interface{}{}
			for k, v := range node.Properties {
				props[k] = v
			}
			if node.ID != "" {
				if _, ok := props["id"]; !ok {
					props["id"] = string(node.ID)
				}
			}
			stmts = append(stmts, remoteStatement{
				Statement:  fmt.Sprintf("CREATE (n%s) SET n += $props", labelsExpr(node.Labels)),
				Parameters: map[string]interface{}{"props": props},
			})
		}
		if err := r.transport.queryBatch(ctx, stmts); err != nil {
			return err
		}
	}
	return nil
}

func (r *RemoteEngine) BulkCreateEdges(edges []*Edge) error {
	ctx, cancel := defaultCtx()
	defer cancel()
	for i := 0; i < len(edges); i += remoteBulkChunkSize {
		end := i + remoteBulkChunkSize
		if end > len(edges) {
			end = len(edges)
		}
		stmts := make([]remoteStatement, 0, end-i)
		for _, edge := range edges[i:end] {
			props := map[string]interface{}{}
			for k, v := range edge.Properties {
				props[k] = v
			}
			if edge.ID != "" {
				props["id"] = string(edge.ID)
			}
			stmts = append(stmts, remoteStatement{
				Statement: fmt.Sprintf(
					"MATCH (a),(b) WHERE a.id = $start AND b.id = $end CREATE (a)-[r:%s]->(b) SET r += $props",
					quoteIdent(edge.Type),
				),
				Parameters: map[string]interface{}{
					"start": string(edge.StartNode),
					"end":   string(edge.EndNode),
					"props": props,
				},
			})
		}
		if err := r.transport.queryBatch(ctx, stmts); err != nil {
			return err
		}
	}
	return nil
}

func (r *RemoteEngine) BulkDeleteNodes(ids []NodeID) error {
	ctx, cancel := defaultCtx()
	defer cancel()
	for i := 0; i < len(ids); i += remoteBulkChunkSize {
		end := i + remoteBulkChunkSize
		if end > len(ids) {
			end = len(ids)
		}
		stmts := make([]remoteStatement, 0, end-i)
		for _, id := range ids[i:end] {
			stmts = append(stmts, remoteStatement{
				Statement:  "MATCH (n) WHERE n.id = $id DETACH DELETE n",
				Parameters: map[string]interface{}{"id": string(id)},
			})
		}
		if err := r.transport.queryBatch(ctx, stmts); err != nil {
			return err
		}
	}
	return nil
}

func (r *RemoteEngine) BulkDeleteEdges(ids []EdgeID) error {
	ctx, cancel := defaultCtx()
	defer cancel()
	for i := 0; i < len(ids); i += remoteBulkChunkSize {
		end := i + remoteBulkChunkSize
		if end > len(ids) {
			end = len(ids)
		}
		stmts := make([]remoteStatement, 0, end-i)
		for _, id := range ids[i:end] {
			stmts = append(stmts, remoteStatement{
				Statement:  "MATCH ()-[r]->() WHERE r.id = $id DELETE r",
				Parameters: map[string]interface{}{"id": string(id)},
			})
		}
		if err := r.transport.queryBatch(ctx, stmts); err != nil {
			return err
		}
	}
	return nil
}

func (r *RemoteEngine) BatchGetNodes(ids []NodeID) (map[NodeID]*Node, error) {
	ctx, cancel := defaultCtx()
	defer cancel()
	out := make(map[NodeID]*Node, len(ids))
	for _, id := range ids {
		rows, err := r.transport.query(ctx, "MATCH (n) WHERE n.id = $id RETURN n LIMIT 1", map[string]interface{}{"id": string(id)})
		if err != nil {
			if err == ErrNotFound {
				continue
			}
			return nil, err
		}
		if len(rows) == 0 {
			continue
		}
		node, err := valueToNode(rows[0][0])
		if err != nil {
			return nil, err
		}
		out[id] = node
	}
	return out, nil
}

func (r *RemoteEngine) Close() error { return r.transport.close() }

func (r *RemoteEngine) NodeCount() (int64, error) {
	ctx, cancel := defaultCtx()
	defer cancel()
	rows, err := r.transport.query(ctx, "MATCH (n) RETURN count(n)", nil)
	if err != nil || len(rows) == 0 || len(rows[0]) == 0 {
		return 0, err
	}
	return remoteToInt64(rows[0][0]), nil
}

func (r *RemoteEngine) EdgeCount() (int64, error) {
	ctx, cancel := defaultCtx()
	defer cancel()
	rows, err := r.transport.query(ctx, "MATCH ()-[r]->() RETURN count(r)", nil)
	if err != nil || len(rows) == 0 || len(rows[0]) == 0 {
		return 0, err
	}
	return remoteToInt64(rows[0][0]), nil
}

func (r *RemoteEngine) DeleteByPrefix(prefix string) (nodesDeleted int64, edgesDeleted int64, err error) {
	return 0, 0, fmt.Errorf("DeleteByPrefix is not supported for remote engines (prefix=%s)", prefix)
}

// QueryCypher executes an arbitrary Cypher query against the remote instance
// and returns column names and raw rows. This is used by the fabric layer to
// dispatch fragment queries to remote constituents without going through the
// node/edge Engine abstraction.
//
// The Bolt transport returns columns from the result record keys.
// The HTTP transport returns columns from the tx API response.
func (r *RemoteEngine) QueryCypher(ctx context.Context, statement string, params map[string]interface{}) ([]string, [][]interface{}, error) {
	if ctx == nil {
		var cancel context.CancelFunc
		ctx, cancel = defaultCtx()
		defer cancel()
	}
	return r.transport.queryWithColumns(ctx, statement, params)
}

// BeginCypherTx opens an explicit remote transaction handle.
func (r *RemoteEngine) BeginCypherTx(ctx context.Context) (RemoteCypherTx, error) {
	if ctx == nil {
		var cancel context.CancelFunc
		ctx, cancel = defaultCtx()
		defer cancel()
	}
	return r.transport.beginCypherTx(ctx)
}
