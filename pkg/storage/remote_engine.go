package storage

import (
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/orneryd/nornicdb/internal/jsontext"

	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/dbtype"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/observability"
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

// RemoteAccessMode is the caller's required routing intent for one implicit
// remote query. The transport never infers intent from statement text.
type RemoteAccessMode uint8

const (
	// RemoteAccessRead selects a read session and managed read transaction.
	RemoteAccessRead RemoteAccessMode = iota + 1
	// RemoteAccessWrite selects a write session and managed write transaction.
	RemoteAccessWrite
)

// remoteTransport abstracts Bolt vs HTTP execution of Cypher statements.
type remoteTransport interface {
	query(ctx context.Context, statement string, params map[string]interface{}, mode RemoteAccessMode) ([]string, [][]interface{}, error)
	queryBatch(ctx context.Context, statements []remoteStatement) error
	close() error
}

// RemoteCypherTx represents an explicit remote Cypher transaction handle.
type RemoteCypherTx interface {
	QueryCypher(ctx context.Context, statement string, params map[string]interface{}) ([]string, [][]interface{}, error)
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}

// RemoteCypherMode reports a completed query's resolved write participation.
// known=false requires conservative admission for a dynamic remote call.
type RemoteCypherMode interface{ ResolvedWriteMode() (write, known bool) }

// RemoteEngine implements Engine by forwarding operations to a remote NornicDB instance
// via Bolt protocol (preferred) or HTTP tx API (fallback), auto-detected from the URI scheme.
type RemoteEngine struct {
	transport remoteTransport
	schema    *SchemaManager
}

type remoteStatement struct {
	Statement          string                 `json:"statement"`
	IncludeStats       bool                   `json:"includeStats,omitempty"`
	Parameters         map[string]interface{} `json:"parameters,omitempty"`
	ResultDataContents []string               `json:"resultDataContents,omitempty"`
}

// NewRemoteEngine creates a remote engine. Transport is auto-detected from the URI scheme.
func NewRemoteEngine(cfg RemoteEngineConfig) (engine *RemoteEngine, err error) {
	defer func() {
		if err != nil {
			err = observability.RedactClientError(err, cfg.URI, remoteDisplayCredentials(cfg)...)
		}
	}()
	uri := strings.TrimSpace(cfg.URI)
	database := strings.TrimSpace(cfg.Database)
	if uri == "" {
		return nil, localizedError(localization.StorageRemoteURIRequired(), nil)
	}
	if database == "" {
		return nil, localizedError(localization.StorageRemoteDatabaseRequired(), nil)
	}

	var transport remoteTransport

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
		return nil, localizedError(localization.StorageRemoteURISchemeUnsupported(uri), nil)
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
	credentials []string
	baseURL     string
	driver      neo4j.DriverWithContext
	database    string
}

// remoteCypherTx owns the public transaction lifecycle for either wire transport.
type remoteCypherTx struct {
	resolvedMode atomic.Int32
	credentials  []string
	baseURL      string
	session      neo4j.SessionWithContext
	tx           neo4j.ExplicitTransaction
	transport    *httpTransport
	executeURL   string
	commitURL    string
	rollbackURL  string
	closed       bool
}

func newBoltTransport(uri, database string, cfg RemoteEngineConfig) (*boltTransport, error) {
	auth := buildNeo4jAuth(cfg)
	driver, err := neo4j.NewDriverWithContext(uri, auth)
	if err != nil {
		return nil, localizedError(localization.StorageRemoteBoltDriverCreateFailed(err), err)
	}
	return &boltTransport{driver: driver, database: database, baseURL: uri, credentials: remoteDisplayCredentials(cfg)}, nil
}

func buildNeo4jAuth(cfg RemoteEngineConfig) neo4j.AuthToken {
	user := cfg.User
	pass := cfg.Password
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

func (b *boltTransport) query(ctx context.Context, statement string, params map[string]interface{}, mode RemoteAccessMode) (_ []string, _ [][]interface{}, err error) {
	defer func() {
		if err != nil {
			err = observability.RedactClientError(err, b.baseURL, b.credentials...)
		}
	}()
	accessMode := neo4j.AccessModeWrite
	if mode == RemoteAccessRead {
		accessMode = neo4j.AccessModeRead
	}
	session := b.driver.NewSession(ctx, neo4j.SessionConfig{
		DatabaseName: b.database,
		AccessMode:   accessMode,
	})
	defer func() { _ = session.Close(ctx) }()

	return executeManagedStatement(ctx, session, accessMode, statement, params)
}

// decodeRemoteBoltResult is the result-reading owner for both managed and
// explicit transactions. Consumption remains with the transaction mechanism:
// the driver completes managed work, while explicit queries read its summary.
func decodeRemoteBoltResult(ctx context.Context, result neo4j.ResultWithContext) ([]string, [][]interface{}, error) {
	columns, err := result.Keys()
	if err != nil {
		return nil, nil, localizedError(localization.StorageRemoteResultKeysFailed(err), err)
	}
	rows := make([][]interface{}, 0)
	for result.Next(ctx) {
		record := result.Record()
		row := make([]interface{}, len(record.Values))
		for i, value := range record.Values {
			row[i] = normalizeBoltValue(value)
		}
		rows = append(rows, row)
	}
	if err := result.Err(); err != nil {
		return nil, nil, err
	}
	return columns, rows, nil
}

func executeManagedStatement(ctx context.Context, session neo4j.SessionWithContext, mode neo4j.AccessMode, statement string, params map[string]interface{}) ([]string, [][]interface{}, error) {
	var columns []string
	rows := make([][]interface{}, 0)
	runInTx := func(tx neo4j.ManagedTransaction) (interface{}, error) {
		result, err := tx.Run(ctx, statement, remoteBoltParameters(params))
		if err != nil {
			return nil, err
		}
		cols, batch, err := decodeRemoteBoltResult(ctx, result)
		if err != nil {
			return nil, err
		}
		columns = cols
		rows = append(rows, batch...)
		return nil, nil
	}
	var err error
	if mode == neo4j.AccessModeWrite {
		_, err = session.ExecuteWrite(ctx, runInTx)
	} else {
		_, err = session.ExecuteRead(ctx, runInTx)
	}
	if err != nil {
		return nil, nil, err
	}
	return columns, rows, nil
}

func (b *boltTransport) queryBatch(ctx context.Context, statements []remoteStatement) (err error) {
	defer func() {
		if err != nil {
			err = observability.RedactClientError(err, b.baseURL, b.credentials...)
		}
	}()
	session := b.driver.NewSession(ctx, neo4j.SessionConfig{
		DatabaseName: b.database,
		AccessMode:   neo4j.AccessModeWrite,
	})
	defer func() { _ = session.Close(ctx) }()

	_, err = session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (interface{}, error) {
		for _, stmt := range statements {
			result, err := tx.Run(ctx, stmt.Statement, remoteBoltParameters(stmt.Parameters))
			if err != nil {
				return nil, err
			}
			// Consume to ensure statement completes.
			if _, err = result.Consume(ctx); err != nil {
				return nil, err
			}
		}
		return nil, nil
	})
	return err
}

func (b *boltTransport) close() (err error) {
	defer func() {
		if err != nil {
			err = observability.RedactClientError(err, b.baseURL, b.credentials...)
		}
	}()
	return b.driver.Close(context.Background())
}

// normalizeBoltValue converts neo4j dbtype values into the map shapes that
// valueToNode/valueToEdge expect, and temporal and point values into
// RemoteTypedValue, so all downstream code works for both transports.
func normalizeBoltValue(v interface{}) interface{} {
	value, _ := normalizeBoltGraphValue(v)
	return value
}

// normalizeBoltGraphValue copies a container only when an actual graph child
// needs normalization. Ordinary remote row maps/lists retain their zero-allocation
// baseline; a recursive graph conversion never mutates the driver's input value.
func normalizeBoltGraphValue(v interface{}) (interface{}, bool) {
	if typed, ok := remoteTypedFromBolt(v); ok {
		return typed, true
	}
	switch n := v.(type) {
	case dbtype.Node:
		m := RemoteEntityValue{
			remoteIdentityKey: remoteNumericIdentity{id: n.Id, start: -1, end: -1},
			"elementId":       n.ElementId,
			"labels":          toInterfaceSlice(n.Labels),
			"properties":      n.Props,
		}
		if id, ok := n.Props["id"]; ok {
			m["id"] = id
		}
		return m, true
	case dbtype.Relationship:
		m := RemoteEntityValue{
			remoteIdentityKey:    remoteNumericIdentity{id: n.Id, start: n.StartId, end: n.EndId},
			"elementId":          n.ElementId,
			"type":               n.Type,
			"startNodeElementId": n.StartElementId,
			"endNodeElementId":   n.EndElementId,
			"properties":         n.Props,
		}
		if id, ok := n.Props["id"]; ok {
			m["id"] = id
		}
		return m, true
	case dbtype.Path:
		path := make(RemotePathValue, 0, len(n.Nodes)+len(n.Relationships))
		for i, node := range n.Nodes {
			path = append(path, normalizeBoltValue(node).(RemoteEntityValue))
			if i < len(n.Relationships) {
				path = append(path, normalizeBoltValue(n.Relationships[i]).(RemoteEntityValue))
			}
		}
		return path, true
	case []interface{}:
		var result []interface{}
		for index, value := range n {
			normalized, changed := normalizeBoltGraphValue(value)
			if !changed {
				continue
			}
			if result == nil {
				result = append([]interface{}(nil), n...)
			}
			result[index] = normalized
		}
		if result == nil {
			return v, false
		}
		return result, true
	case map[string]interface{}:
		var result map[string]interface{}
		for key, value := range n {
			normalized, changed := normalizeBoltGraphValue(value)
			if !changed {
				continue
			}
			if result == nil {
				result = make(map[string]interface{}, len(n))
				for originalKey, originalValue := range n {
					result[originalKey] = originalValue
				}
			}
			result[key] = normalized
		}
		if result == nil {
			return v, false
		}
		return result, true
	default:
		return v, false
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

type remoteTxRequest struct {
	Statements          []remoteStatement      `json:"statements"`
	TransactionMetadata map[string]interface{} `json:"tx_metadata,omitempty"`
}

type remoteTxResponse struct {
	Results []struct {
		Columns   []string          `json:"columns"`
		Stats     *remoteQueryStats `json:"stats"`
		QueryType string            `json:"query_type"`
		Data      []struct {
			RawRow []jsontext.RawMessage `json:"row"`
			Row    []interface{}         `json:"-"`
			Meta   jsontext.NumberList   `json:"meta"`
			REST   []jsontext.RawMessage `json:"rest"`
			Graph  struct {
				Nodes         []jsontext.NumberMap `json:"nodes"`
				Relationships []jsontext.NumberMap `json:"relationships"`
			} `json:"graph"`
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
		client = &http.Client{Timeout: RemoteRequestTimeout}
	}
	return &httpTransport{
		baseURL:   uri,
		database:  database,
		authToken: strings.TrimSpace(cfg.AuthToken),
		user:      cfg.User,
		password:  cfg.Password,
		client:    client,
	}, nil
}

func (h *httpTransport) commitURL() (string, error) {
	base, err := url.Parse(h.baseURL)
	if err != nil {
		return "", fmt.Errorf("invalid remote HTTP base URL")
	}
	path := base.EscapedPath()
	route := "/db/" + url.PathEscape(h.database) + "/tx/commit"
	if strings.HasSuffix(path, "/tx/commit") {
		route = ""
	} else if strings.Contains(path, "/db/") {
		route = "/tx/commit"
	}
	return observability.ComposeRequestURL(h.baseURL, route, observability.AppendRequestRoute)
}

func (h *httpTransport) txOpenURL() (string, error) {
	base, err := url.Parse(h.baseURL)
	if err != nil {
		return "", fmt.Errorf("invalid remote HTTP base URL")
	}
	path := base.EscapedPath()
	if strings.HasSuffix(path, "/tx/commit") {
		return observability.ComposeRequestURL(h.baseURL, strings.TrimSuffix(path, "/commit"), observability.MergeRequestFollowup)
	}
	route := "/db/" + url.PathEscape(h.database) + "/tx"
	if strings.Contains(path, "/db/") {
		route = "/tx"
	}
	return observability.ComposeRequestURL(h.baseURL, route, observability.AppendRequestRoute)
}

func (h *httpTransport) doRequestToURL(ctx context.Context, method string, targetURL string, body interface{}) (_ *remoteTxResponse, err error) {
	defer func() {
		if err != nil {
			err = observability.RedactClientError(err, h.baseURL, h.displayCredentials()...)
		}
	}()
	if targetURL == "" {
		targetURL, err = h.commitURL()
		if err != nil {
			return nil, err
		}
	}
	var reader io.Reader
	if body != nil {
		raw, err := jsontext.Marshal(body)
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
	if err := unmarshalRemoteHTTPJSON(respBody, &txResp); err != nil {
		return nil, localizedError(localization.StorageRemoteTransactionDecodeFailed(resp.StatusCode, err), err)
	}
	if len(txResp.Errors) > 0 {
		first := txResp.Errors[0]
		return nil, localizedError(localization.StorageRemoteError(first.Code, first.Message), nil)
	}
	for resultIndex := range txResp.Results {
		for rowIndex := range txResp.Results[resultIndex].Data {
			data := &txResp.Results[resultIndex].Data[rowIndex]
			if data.REST != nil && len(data.REST) != len(data.RawRow) {
				return nil, localizedError(localization.StorageRemoteRESTRowLengthMismatch(), nil)
			}
			for _, entry := range data.Meta {
				remoteHTTPNumbers(entry)
			}
			for _, entities := range [][]jsontext.NumberMap{data.Graph.Nodes, data.Graph.Relationships} {
				for _, entity := range entities {
					remoteHTTPNumbers(map[string]interface{}(entity))
				}
			}
			entities := make(map[string]interface{})
			for _, node := range data.Graph.Nodes {
				entities[fmt.Sprint(node["elementId"])] = map[string]interface{}{
					remoteIdentityKey: remoteNumericIdentity{id: remoteHTTPNumericID(node["id"]), start: -1, end: -1},
					"elementId":       node["elementId"], "labels": node["labels"], "properties": node["properties"],
				}
			}
			for _, edge := range data.Graph.Relationships {
				entities[fmt.Sprint(edge["elementId"])] = map[string]interface{}{
					remoteIdentityKey: remoteNumericIdentity{id: remoteHTTPNumericID(edge["id"]), start: remoteHTTPEndpointID(edge, "startNode", entities), end: remoteHTTPEndpointID(edge, "endNode", entities)},
					"elementId":       edge["elementId"], "type": edge["type"], "properties": edge["properties"],
					"startNodeElementId": edge["startNodeElementId"], "endNodeElementId": edge["endNodeElementId"],
				}
			}
			metadataIndex := 0
			data.Row = make([]interface{}, len(data.RawRow))
			for column, raw := range data.RawRow {
				var rest []byte
				if len(data.REST) == len(data.RawRow) {
					rest = data.REST[column]
				}
				value, err := decodeRemoteHTTPValue(raw, rest, data.Meta, &metadataIndex, entities, remoteHTTPAuto)
				if err != nil {
					return nil, err
				}
				data.Row[column] = value
			}
		}
	}
	return &txResp, nil
}

func (h *httpTransport) query(ctx context.Context, statement string, params map[string]interface{}, mode RemoteAccessMode) ([]string, [][]interface{}, error) {
	txResp, err := h.doRequestToURL(ctx, http.MethodPost, "", remoteTxRequest{
		Statements: []remoteStatement{{Statement: statement, Parameters: params, IncludeStats: true, ResultDataContents: []string{"row", "graph", "rest"}}},
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
	_, err := h.doRequestToURL(ctx, http.MethodPost, "", remoteTxRequest{Statements: statements})
	return err
}

// openTransaction exchanges HTTP's explicit-transaction handshake and returns its URLs.
func (h *httpTransport) openTransaction(ctx context.Context) (_ string, _ string, err error) {
	openBody := remoteTxRequest{Statements: []remoteStatement{}, TransactionMetadata: coordinatorTransactionMetadata(ctx)}
	raw, err := jsontext.Marshal(openBody)
	if err != nil {
		return "", "", err
	}
	target, err := h.txOpenURL()
	if err != nil {
		return "", "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(raw))
	if err != nil {
		return "", "", err
	}
	req.Header.Set("Content-Type", "application/json")
	if h.user != "" || h.password != "" {
		req.SetBasicAuth(h.user, h.password)
	} else if h.authToken != "" {
		req.Header.Set("Authorization", h.authToken)
	}
	resp, err := h.client.Do(req)
	if err != nil {
		return "", "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", "", err
	}
	var openResp struct {
		Commit string `json:"commit"`
		Errors []struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"errors"`
	}
	if err := jsontext.Unmarshal(body, &openResp); err != nil {
		return "", "", localizedError(localization.StorageRemoteTransactionOpenDecodeFailed(resp.StatusCode, err), err)
	}
	if len(openResp.Errors) > 0 {
		return "", "", localizedError(localization.StorageRemoteError(openResp.Errors[0].Code, openResp.Errors[0].Message), nil)
	}
	if strings.TrimSpace(openResp.Commit) == "" {
		return "", "", localizedError(localization.StorageRemoteTransactionCommitURLMissing(), nil)
	}
	commitURL, err := observability.ComposeRequestURL(h.baseURL, openResp.Commit, observability.MergeRequestFollowup)
	if err != nil {
		return "", "", err
	}
	commit, err := url.Parse(commitURL)
	if err != nil {
		return "", "", fmt.Errorf("invalid composed remote transaction URL")
	}
	commit.Path = strings.TrimSuffix(commit.Path, "/commit")
	commit.RawPath = strings.TrimSuffix(commit.RawPath, "/commit")
	executeURL := commit.String()
	return executeURL, commitURL, nil
}

func (h *httpTransport) close() error { return nil }

// QueryCypher executes through the selected wire mechanism while owning the
// common result-mode and credential-safe error boundary.
func (t *remoteCypherTx) QueryCypher(ctx context.Context, statement string, params map[string]interface{}) (_ []string, _ [][]interface{}, err error) {
	t.resolvedMode.Store(0)
	defer func() {
		if err != nil {
			err = observability.RedactClientError(err, t.baseURL, t.credentials...)
		}
	}()
	if t.transport != nil && t.closed {
		return nil, nil, localizedError(localization.StorageRemoteTransactionClosed(), nil)
	}
	if t.transport == nil {
		result, err := t.tx.Run(ctx, statement, remoteBoltParameters(params))
		if err != nil {
			return nil, nil, err
		}
		columns, rows, err := decodeRemoteBoltResult(ctx, result)
		if err != nil {
			return nil, nil, err
		}
		summary, err := result.Consume(ctx)
		if err != nil {
			return nil, nil, err
		}
		t.resolvedMode.Store(remoteBoltQueryMode(summary))
		return columns, rows, nil
	}
	txResp, err := t.transport.doRequestToURL(ctx, http.MethodPost, t.executeURL, remoteTxRequest{
		Statements: []remoteStatement{{Statement: statement, Parameters: params, IncludeStats: true, ResultDataContents: []string{"row", "graph", "rest"}}},
	})
	if err != nil {
		return nil, nil, err
	}
	if len(txResp.Results) == 0 {
		return []string{}, [][]interface{}{}, nil
	}
	result := txResp.Results[0]
	t.resolvedMode.Store(remoteHTTPQueryMode(result.Stats, result.QueryType))
	rows := make([][]interface{}, 0, len(result.Data))
	for _, data := range result.Data {
		rows = append(rows, data.Row)
	}
	return result.Columns, rows, nil
}

// Commit completes the selected transport and releases its connection owner.
func (t *remoteCypherTx) Commit(ctx context.Context) error { return t.finish(ctx, true) }

func (t *remoteCypherTx) Rollback(ctx context.Context) error { return t.finish(ctx, false) }

func (t *remoteCypherTx) finish(ctx context.Context, commit bool) (err error) {
	if t.transport != nil {
		if t.closed {
			return nil
		}
		t.closed = true
	}
	defer func() {
		if err != nil {
			err = observability.RedactClientError(err, t.baseURL, t.credentials...)
		}
	}()
	if t.transport != nil {
		method, target, body := http.MethodDelete, t.rollbackURL, interface{}(nil)
		if commit {
			method, target, body = http.MethodPost, t.commitURL, remoteTxRequest{Statements: []remoteStatement{}}
		}
		_, err = t.transport.doRequestToURL(ctx, method, target, body)
		return err
	}
	defer func() { _ = t.session.Close(ctx) }()
	if commit {
		return t.tx.Commit(ctx)
	}
	return t.tx.Rollback(ctx)
}

// OutlivesConnection reports whether the server addresses this transaction by URL.
func (t *remoteCypherTx) OutlivesConnection() bool { return t.transport != nil }

// ---------------------------------------------------------------------------
// Helpers shared across transports
// ---------------------------------------------------------------------------

func labelsExpr(labels []string) string {
	if len(labels) == 0 {
		return ""
	}
	parts := make([]string, 0, len(labels))
	for _, l := range labels {
		if l == "" {
			continue
		}
		parts = append(parts, ":"+cyphertext.PrintIdentifier(l, nil, cyphertext.RawSource))
	}
	return strings.Join(parts, "")
}

func asMap(v interface{}) map[string]interface{} {
	if remote, ok := v.(RemoteEntityValue); ok {
		return map[string]interface{}(remote)
	}
	if m, ok := v.(map[string]interface{}); ok {
		return m
	}
	return nil
}

func valueToNode(v interface{}) (*Node, error) {
	m := asMap(v)
	if m == nil {
		return nil, localizedError(localization.StorageRemoteNodeMapExpected(fmt.Sprintf("%T", v)), nil)
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
		return nil, localizedError(localization.StorageRemoteRelationshipMapExpected(fmt.Sprintf("%T", v)), nil)
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

// RemoteRequestTimeout bounds a remote request that has no deadline of its
// own: the HTTP transport's client timeout and the fallback context of
// Engine methods that don't accept one.
const RemoteRequestTimeout = 30 * time.Second

// defaultCtx creates a context with a RemoteRequestTimeout timeout as a
// fallback for Engine methods that don't accept a caller-provided context.
func defaultCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), RemoteRequestTimeout)
}

// ---------------------------------------------------------------------------
// Engine interface implementation
// ---------------------------------------------------------------------------

// queryRemoteEntities owns the query boundary for entity reads. The required
// admission function retains the caller's exact cardinality and result type;
// single reads never allocate a list or inspect later rows.
func queryRemoteEntities[T, R any](r *RemoteEngine, ctx context.Context, statement string, params map[string]interface{}, decode func(interface{}) (*T, error), admit func([][]interface{}, func(interface{}) (*T, error)) (R, error)) (R, error) {
	_, rows, err := r.QueryCypher(ctx, statement, params, RemoteAccessRead)
	if err != nil {
		var zero R
		return zero, err
	}
	return admit(rows, decode)
}

func firstRemoteEntity[T any](rows [][]interface{}, decode func(interface{}) (*T, error)) (*T, error) {
	if len(rows) == 0 || len(rows[0]) == 0 {
		return nil, ErrNotFound
	}
	return decode(rows[0][0])
}

func allRemoteEntities[T any](rows [][]interface{}, decode func(interface{}) (*T, error)) ([]*T, error) {
	out := make([]*T, 0, len(rows))
	for _, row := range rows {
		if len(row) == 0 {
			continue
		}
		entity, err := decode(row[0])
		if err != nil {
			return nil, err
		}
		out = append(out, entity)
	}
	return out, nil
}

// remoteCount admits the shared scalar result for counts, degrees and updates.
func (r *RemoteEngine) remoteCount(ctx context.Context, statement string, params map[string]interface{}, mode RemoteAccessMode) (int64, error) {
	_, rows, err := r.QueryCypher(ctx, statement, params, mode)
	if err != nil || len(rows) == 0 || len(rows[0]) == 0 {
		return 0, err
	}
	return remoteToInt64(rows[0][0]), nil
}

// mutateRemoteEntity owns parameter binding and match admission for mutations.
func (r *RemoteEngine) mutateRemoteEntity(statement, id string, props map[string]interface{}, requireMatch bool) error {
	params := map[string]interface{}{"id": id}
	if requireMatch {
		params["props"] = props
		matched, err := r.remoteCount(nil, statement, params, RemoteAccessWrite)
		if err != nil {
			return err
		}
		if matched == 0 {
			return ErrNotFound
		}
		return nil
	}
	_, _, err := r.QueryCypher(nil, statement, params, RemoteAccessWrite)
	return err
}

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
	_, rows, err := r.transport.query(ctx, stmt, map[string]interface{}{"props": props}, RemoteAccessWrite)
	if err != nil {
		return "", err
	}
	if len(rows) == 0 || len(rows[0]) == 0 {
		return "", localizedError(localization.StorageRemoteCreateNodeNoRows(), nil)
	}
	created, err := valueToNode(rows[0][0])
	if err != nil {
		return "", err
	}
	return created.ID, nil
}

func (r *RemoteEngine) GetNode(id NodeID) (*Node, error) {
	return queryRemoteEntities(r, nil, "MATCH (n) WHERE n.id = $id RETURN n LIMIT 1", map[string]interface{}{"id": string(id)}, valueToNode, firstRemoteEntity[Node])
}

func (r *RemoteEngine) UpdateNode(node *Node) error {
	return r.mutateRemoteEntity("MATCH (n) WHERE n.id = $id SET n += $props RETURN count(n)", string(node.ID), node.Properties, true)
}

func (r *RemoteEngine) DeleteNode(id NodeID) error {
	return r.mutateRemoteEntity("MATCH (n) WHERE n.id = $id DETACH DELETE n", string(id), nil, false)
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
		cyphertext.PrintIdentifier(edge.Type, nil, cyphertext.RawSource),
	)
	if edge.ID != "" {
		props["id"] = string(edge.ID)
		params["id"] = string(edge.ID)
		stmt = fmt.Sprintf(
			"MATCH (a),(b) WHERE a.id = $start AND b.id = $end MERGE (a)-[r:%s {id: $id}]->(b) SET r += $props RETURN r",
			cyphertext.PrintIdentifier(edge.Type, nil, cyphertext.RawSource),
		)
	}
	ctx, cancel := defaultCtx()
	defer cancel()
	_, rows, err := r.transport.query(ctx, stmt, params, RemoteAccessWrite)
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		return ErrNotFound
	}
	return nil
}

func (r *RemoteEngine) GetEdge(id EdgeID) (*Edge, error) {
	return queryRemoteEntities(r, nil, "MATCH ()-[r]->() WHERE r.id = $id RETURN r LIMIT 1", map[string]interface{}{"id": string(id)}, valueToEdge, firstRemoteEntity[Edge])
}

func (r *RemoteEngine) UpdateEdge(edge *Edge) error {
	return r.mutateRemoteEntity("MATCH ()-[r]->() WHERE r.id = $id SET r += $props RETURN count(r)", string(edge.ID), edge.Properties, true)
}

func (r *RemoteEngine) DeleteEdge(id EdgeID) error {
	return r.mutateRemoteEntity("MATCH ()-[r]->() WHERE r.id = $id DELETE r", string(id), nil, false)
}

func (r *RemoteEngine) GetNodesByLabel(label string) ([]*Node, error) {
	ctx, cancel := defaultCtx()
	defer cancel()
	return r.getNodesByLabel(ctx, label)
}

func (r *RemoteEngine) getNodesByLabel(ctx context.Context, label string) ([]*Node, error) {
	return r.getNodesByLabelWithReveal(ctx, label, false)
}

func (r *RemoteEngine) getNodesByLabelWithReveal(ctx context.Context, label string, revealSuppressed bool) ([]*Node, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	returnExpr := "n"
	if revealSuppressed {
		returnExpr = "reveal(n)"
	}
	stmt := fmt.Sprintf("MATCH (n) RETURN %s", returnExpr)
	if label != "" {
		stmt = fmt.Sprintf("MATCH (n:%s) RETURN %s", cyphertext.PrintIdentifier(label, nil, cyphertext.RawSource), returnExpr)
	}
	return queryRemoteEntities(r, ctx, stmt, nil, valueToNode, allRemoteEntities[Node])
}

func (r *RemoteEngine) GetFirstNodeByLabel(label string) (*Node, error) {
	stmt := "MATCH (n) RETURN n LIMIT 1"
	if label != "" {
		stmt = fmt.Sprintf("MATCH (n:%s) RETURN n LIMIT 1", cyphertext.PrintIdentifier(label, nil, cyphertext.RawSource))
	}
	return queryRemoteEntities(r, nil, stmt, nil, valueToNode, firstRemoteEntity[Node])
}

func (r *RemoteEngine) GetOutgoingEdges(nodeID NodeID) ([]*Edge, error) {
	return queryRemoteEntities(r, nil, "MATCH (n)-[r]->() WHERE n.id = $id RETURN r", map[string]interface{}{"id": string(nodeID)}, valueToEdge, allRemoteEntities[Edge])
}

func (r *RemoteEngine) GetIncomingEdges(nodeID NodeID) ([]*Edge, error) {
	return queryRemoteEntities(r, nil, "MATCH ()-[r]->(n) WHERE n.id = $id RETURN r", map[string]interface{}{"id": string(nodeID)}, valueToEdge, allRemoteEntities[Edge])
}

// readEdgesBetween owns endpoint binding and the two Engine lookup contracts.
// A type selects the typed first-edge query; nil selects every connecting edge.
func (r *RemoteEngine) readEdgesBetween(startID, endID NodeID, edgeType *string) ([]*Edge, error) {
	relationship, limit := "r", ""
	if edgeType != nil {
		relationship += ":" + cyphertext.PrintIdentifier(*edgeType, nil, cyphertext.RawSource)
		limit = " LIMIT 1"
	}
	statement := "MATCH (a)-[" + relationship + "]->(b) WHERE a.id = $start AND b.id = $end RETURN r" + limit
	return queryRemoteEntities(r, nil, statement, map[string]interface{}{"start": string(startID), "end": string(endID)}, valueToEdge, allRemoteEntities[Edge])
}

func (r *RemoteEngine) GetEdgesBetween(startID, endID NodeID) ([]*Edge, error) {
	return r.readEdgesBetween(startID, endID, nil)
}

func (r *RemoteEngine) GetEdgeBetween(startID, endID NodeID, edgeType string) *Edge {
	edges, _ := r.readEdgesBetween(startID, endID, &edgeType)
	if len(edges) == 0 {
		return nil
	}
	return edges[0]
}

func (r *RemoteEngine) GetEdgesByType(edgeType string) ([]*Edge, error) {
	return queryRemoteEntities(r, nil, fmt.Sprintf("MATCH ()-[r:%s]->() RETURN r", cyphertext.PrintIdentifier(edgeType, nil, cyphertext.RawSource)), nil, valueToEdge, allRemoteEntities[Edge])
}

func (r *RemoteEngine) AllNodes() ([]*Node, error) { return r.GetNodesByLabel("") }

// StreamNodesWithOptions satisfies the Engine streaming contract for the
// remote view: nodes are materialized through the remote read path and the
// per-option transforms (prefix scope, projection, embedding stripping) are
// applied locally.
func (r *RemoteEngine) StreamNodesWithOptions(ctx context.Context, opts StreamNodesOptions, fn func(node *Node) error) error {
	if fn == nil {
		return ErrInvalidData
	}
	nodes, err := r.getNodesByLabelWithReveal(ctx, "", !opts.ApplyDecayFilter)
	if err != nil {
		return err
	}
	for _, node := range nodes {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if node == nil {
			continue
		}
		if opts.Prefix != "" && !strings.HasPrefix(string(node.ID), opts.Prefix) {
			continue
		}
		out := node
		switch {
		case opts.Projection != nil:
			out = projectCachedNodeForRead(node, opts.Projection, true)
		case opts.StripEmbeddings || (!opts.WithEmbeddings && !opts.ApplyDecayFilter):
			out = copyNodeWithoutEmbeddings(node)
		}
		if err := fn(out); err != nil {
			if err == ErrIterationStopped {
				return nil
			}
			return err
		}
	}
	return nil
}

func (r *RemoteEngine) AllEdges() ([]*Edge, error) {
	return queryRemoteEntities(r, nil, "MATCH ()-[r]->() RETURN r", nil, valueToEdge, allRemoteEntities[Edge])
}

func (r *RemoteEngine) GetAllNodes() []*Node {
	nodes, err := r.AllNodes()
	if err != nil {
		return []*Node{}
	}
	return nodes
}

func (r *RemoteEngine) GetInDegree(nodeID NodeID) int {
	count, _ := r.remoteCount(nil, "MATCH ()-[r]->(n) WHERE n.id = $id RETURN count(r)", map[string]interface{}{"id": string(nodeID)}, RemoteAccessRead)
	return int(count)
}

func (r *RemoteEngine) GetOutDegree(nodeID NodeID) int {
	count, _ := r.remoteCount(nil, "MATCH (n)-[r]->() WHERE n.id = $id RETURN count(r)", map[string]interface{}{"id": string(nodeID)}, RemoteAccessRead)
	return int(count)
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
					cyphertext.PrintIdentifier(edge.Type, nil, cyphertext.RawSource),
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
		node, err := queryRemoteEntities(r, ctx, "MATCH (n) WHERE n.id = $id RETURN n LIMIT 1", map[string]interface{}{"id": string(id)}, valueToNode, firstRemoteEntity[Node])
		if err == ErrNotFound {
			continue
		}
		if err != nil {
			return nil, err
		}
		out[id] = node
	}
	return out, nil
}

func (r *RemoteEngine) Close() error { return r.transport.close() }

func (r *RemoteEngine) NodeCount() (int64, error) {
	return r.remoteCount(nil, "MATCH (n) RETURN count(n)", nil, RemoteAccessRead)
}

func (r *RemoteEngine) EdgeCount() (int64, error) {
	return r.remoteCount(nil, "MATCH ()-[r]->() RETURN count(r)", nil, RemoteAccessRead)
}

// EdgeCountByType answers the typed count from the remote instance's own
// per-type counter fast path (issue #638): the remote server serves this
// shape O(1) from its counters, so no client-side edge materialization exists
// on this path.
func (r *RemoteEngine) EdgeCountByType(edgeType string) (int64, error) {
	return r.remotePositionalCount("", edgeType, false)
}

// EdgeCountByStartLabel answers the positional (label, type) count from the
// remote instance's own counters: the remote server serves
// MATCH (s:L)-[r:T]->() O(1).
func (r *RemoteEngine) EdgeCountByStartLabel(label, edgeType string) (int64, error) {
	return r.remotePositionalCount(label, edgeType, false)
}

// EdgeCountByEndLabel answers the positional (label, type) count from the
// remote instance's own counters: the remote server serves
// MATCH ()-[r:T]->(e:L) O(1).
func (r *RemoteEngine) EdgeCountByEndLabel(label, edgeType string) (int64, error) {
	return r.remotePositionalCount(label, edgeType, true)
}

func (r *RemoteEngine) remotePositionalCount(label, edgeType string, end bool) (int64, error) {
	quotedType := cyphertext.PrintIdentifier(edgeType, nil, cyphertext.RawSource)
	statement := "MATCH ()-[r:" + quotedType + "]->() RETURN count(r)"
	if label != "" {
		quotedLabel := cyphertext.PrintIdentifier(label, nil, cyphertext.RawSource)
		if end {
			statement = "MATCH ()-[r:" + quotedType + "]->(e:" + quotedLabel + ") RETURN count(r)"
		} else {
			statement = "MATCH (s:" + quotedLabel + ")-[r:" + quotedType + "]->() RETURN count(r)"
		}
	}
	return r.remoteCount(nil, statement, nil, RemoteAccessRead)
}

func (r *RemoteEngine) DeleteByPrefix(prefix string) (nodesDeleted int64, edgesDeleted int64, err error) {
	return 0, 0, localizedError(localization.StorageRemoteDeletePrefixUnsupported(prefix), nil)
}

// QueryCypher executes an arbitrary Cypher query against the remote instance
// and returns column names and raw rows. This is used by the fabric layer to
// dispatch fragment queries to remote constituents without going through the
// node/edge Engine abstraction.
//
// mode is required: RemoteAccessRead selects a managed read transaction;
// RemoteAccessWrite selects a managed write transaction. Storage never parses
// source to infer mode. For example:
//
//	columns, rows, err := remote.QueryCypher(ctx, "RETURN 1", nil, RemoteAccessRead)
//
// The Bolt transport returns columns from the result record keys.
// The HTTP transport returns columns from the tx API response.
func (r *RemoteEngine) QueryCypher(ctx context.Context, statement string, params map[string]interface{}, mode RemoteAccessMode) ([]string, [][]interface{}, error) {
	if ctx == nil {
		var cancel context.CancelFunc
		ctx, cancel = defaultCtx()
		defer cancel()
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	if mode != RemoteAccessRead && mode != RemoteAccessWrite {
		return nil, nil, fmt.Errorf("invalid remote access mode: %d", mode)
	}
	return r.transport.query(ctx, statement, params, mode)
}

// BeginCypherTx opens an explicit remote transaction handle.
func (r *RemoteEngine) BeginCypherTx(ctx context.Context) (_ RemoteCypherTx, err error) {
	if ctx == nil {
		var cancel context.CancelFunc
		ctx, cancel = defaultCtx()
		defer cancel()
	}
	t := &remoteCypherTx{}
	switch wire := r.transport.(type) {
	case *boltTransport:
		t.baseURL, t.credentials = wire.baseURL, wire.credentials
	case *httpTransport:
		t.baseURL, t.credentials, t.transport = wire.baseURL, wire.displayCredentials(), wire
	default:
		return nil, fmt.Errorf("unsupported remote transaction transport")
	}
	defer func() {
		if err != nil {
			err = observability.RedactClientError(err, t.baseURL, t.credentials...)
		}
	}()
	if t.transport != nil {
		t.executeURL, t.commitURL, err = t.transport.openTransaction(ctx)
		if err != nil {
			return nil, err
		}
		t.rollbackURL = t.executeURL
	} else {
		wire := r.transport.(*boltTransport)
		t.session = wire.driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: wire.database, AccessMode: neo4j.AccessModeWrite})
		t.tx, err = t.session.BeginTransaction(ctx, neo4j.WithTxMetadata(coordinatorTransactionMetadata(ctx)))
		if err != nil {
			_ = t.session.Close(ctx)
			return nil, err
		}
	}
	return t, nil
}
