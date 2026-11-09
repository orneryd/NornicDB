// Package load provides APOC data loading functions.
//
// This package implements the apoc.load.* scalar loaders the bundled APOC
// plugin exports (jsonStream, csvStream, xmlSimple, graphQL, rest, binary,
// stream) and the Go entry points the same names share. Data loading from
// external sources (JSON, CSV, XML, JDBC, ...) lives here.
//
// Every loader that performs real network or file I/O is context-first and
// takes the caller's statement context as its first argument. That context
// must carry the typed APOC I/O capability (pkg/apocio) the query executor
// derives from its configured policy; a Go caller binds one explicitly with
// apocio.WithCapability. A context without a capability performs no I/O and
// receives apocio.ErrCapabilityMissing: there is no unrestricted fallback.
// The loaders below that return fixed data are placeholders; they perform no
// I/O and take no context.
package load

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"net/http"

	"github.com/orneryd/nornicdb/pkg/apocio"
	valueconvert "github.com/orneryd/nornicdb/pkg/convert"
)

// openSource opens an APOC source through the caller's bound I/O capability.
// It is the single place this package turns a context into I/O admission.
func openSource(ctx context.Context, source string, request apocio.Request) (io.ReadCloser, error) {
	capability, err := apocio.CapabilityFrom(ctx)
	if err != nil {
		return nil, err
	}
	return capability.Open(ctx, source, request)
}

// Json loads JSON data from a URL or import file.
//
// Example:
//
//	apoc.load.json('https://example.com/data.json') => parsed data
//	apoc.load.json('/path/to/file.json') => parsed data
func Json(ctx context.Context, urlOrFile string) (interface{}, error) {
	data, err := decodeJSON(ctx, urlOrFile, apocio.Request{}, decodeInputDocument)
	if err != nil {
		return nil, fmt.Errorf("failed to load JSON: %w", err)
	}
	return data, nil
}

// JSONItemShape selects how a decoded JSON document becomes loader items.
type JSONItemShape int

const (
	// DocumentItems publishes a JSON array's items, and any other document
	// (object, scalar or null) as one item.
	DocumentItems JSONItemShape = iota
	// RequireArray publishes a JSON array's items and refuses any other
	// document.
	RequireArray
)

// documentDecoder decodes a document under one number domain. The domains are
// distinct types, so each route names its own here.
type documentDecoder func(io.Reader, *interface{}) error

func decodeInputDocument(reader io.Reader, data *interface{}) error {
	return valueconvert.CypherInputJSON.NewDecoder(reader).Decode(data)
}

func decodePropertyDocument(reader io.Reader, data *interface{}) error {
	return valueconvert.PropertyJSON.NewDecoder(reader).Decode(data)
}

// PropertyDocument loads a JSON document as stored properties, keeping their
// exact numbers. Graph imports read a document this way.
func PropertyDocument(ctx context.Context, urlOrFile string) (interface{}, error) {
	return decodeJSON(ctx, urlOrFile, apocio.Request{}, decodePropertyDocument)
}

// JSONItems binds one item shape once and returns the context-first loader the
// bundled plugin and the native procedures share.
//
// The plugin registers the returned handler under the Cypher names whose row
// shape it declares, and the native JSON procedures consume the DocumentItems
// handler before assembling their YIELD rows. Acquisition, decoding, the
// original error and the caller's cancellation all come from Json. A shape
// other than DocumentItems or RequireArray is rejected before any source is
// opened.
//
// Example:
//
//	func loadDocument(ctx context.Context, policy apocio.Policy, source string) ([]interface{}, error) {
//		ctx = apocio.WithCapability(ctx, policy.Capability())
//		document := JSONItems(DocumentItems)
//		return document(ctx, source)
//	}
//
//	func loadRequiredArray(ctx context.Context, policy apocio.Policy, source string) ([]interface{}, error) {
//		ctx = apocio.WithCapability(ctx, policy.Capability())
//		array := JSONItems(RequireArray)
//		return array(ctx, source)
//	}
func JSONItems(shape JSONItemShape) func(context.Context, string) ([]interface{}, error) {
	return func(ctx context.Context, source string) ([]interface{}, error) {
		if shape != DocumentItems && shape != RequireArray {
			return nil, fmt.Errorf("invalid JSON item shape %d", shape)
		}

		data, err := Json(ctx, source)
		if err != nil {
			return nil, err
		}

		if arr, ok := data.([]interface{}); ok {
			return arr, nil
		}
		if shape == RequireArray {
			return nil, fmt.Errorf("data is not an array")
		}

		return []interface{}{data}, nil
	}
}

// JsonParams loads JSON with a request method and headers.
//
// Example:
//
//	apoc.load.jsonParams('https://api.com/data', {headers: {...}}, 'POST') => data
func JsonParams(ctx context.Context, url string, headers map[string]string, method string) (interface{}, error) {
	return decodeJSON(ctx, url, apocio.Request{Method: method, Headers: headers}, decodeInputDocument)
}

// decodeJSON decodes the JSON document at source through the shared I/O owner.
func decodeJSON(ctx context.Context, source string, request apocio.Request, decode documentDecoder) (interface{}, error) {
	body, err := openSource(ctx, source, request)
	if err != nil {
		return nil, err
	}
	defer body.Close()

	var data interface{}
	if err := decode(body, &data); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return data, nil
}

// Csv loads CSV data from a URL or import file.
//
// Example:
//
//	apoc.load.csv('https://example.com/data.csv', {delimiter: ',', header: true}) => rows
//	apoc.load.csv('/path/to/file.csv', {delimiter: ',', header: true}) => rows
func Csv(ctx context.Context, urlOrFile string, config map[string]interface{}) ([]map[string]interface{}, error) {
	// Parse configuration
	delimiter := ','
	if d, ok := config["delimiter"].(string); ok && len(d) > 0 {
		delimiter = rune(d[0])
	}
	if d, ok := config["sep"].(string); ok && len(d) > 0 {
		delimiter = rune(d[0])
	}

	hasHeader := true
	if h, ok := config["header"].(bool); ok {
		hasHeader = h
	}

	source, err := openSource(ctx, urlOrFile, apocio.Request{})
	if err != nil {
		if apocio.IsHTTPSource(urlOrFile) {
			return nil, fmt.Errorf("failed to fetch CSV: %w", err)
		}
		return nil, fmt.Errorf("failed to open CSV: %w", err)
	}
	defer source.Close()

	reader := csv.NewReader(source)
	reader.Comma = delimiter
	reader.LazyQuotes = true

	// Read all records
	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("failed to parse CSV: %w", err)
	}

	if len(records) == 0 {
		return []map[string]interface{}{}, nil
	}

	// Build result
	rows := make([]map[string]interface{}, 0)
	var headers []string

	startRow := 0
	if hasHeader && len(records) > 0 {
		headers = records[0]
		startRow = 1
	}

	for i := startRow; i < len(records); i++ {
		record := records[i]

		// Build map if we have headers
		row := make(map[string]interface{})
		if hasHeader && headers != nil {
			for j, header := range headers {
				if j < len(record) {
					row[header] = record[j]
				}
			}
		} else {
			// No headers - use column indices
			for j, value := range record {
				row[fmt.Sprintf("col%d", j)] = value
			}
		}

		rows = append(rows, row)
	}

	return rows, nil
}

// Xml loads XML data from a URL or import file.
//
// Example:
//
//	apoc.load.xml('https://example.com/data.xml') => parsed XML
func Xml(ctx context.Context, url string) (map[string]interface{}, error) {
	// Placeholder - would parse XML
	body, err := openSource(ctx, url, apocio.Request{})
	if err != nil {
		return nil, err
	}
	defer body.Close()

	// Simple XML parsing placeholder
	return map[string]interface{}{
		"root": "xml data",
	}, nil
}

// Html loads HTML data from a URL.
//
// Example:
//
//	apoc.load.html('https://example.com', {}) => parsed HTML
func Html(ctx context.Context, url string, config map[string]interface{}) (map[string]interface{}, error) {
	body, err := openSource(ctx, url, apocio.Request{})
	if err != nil {
		return nil, err
	}
	defer body.Close()

	// Placeholder - would parse HTML
	return map[string]interface{}{
		"title": "Page Title",
		"body":  "Page Content",
	}, nil
}

// Jdbc loads data from a JDBC database connection.
//
// Example:
//
//	apoc.load.jdbc('jdbc:mysql://localhost/db', 'SELECT * FROM users') => rows
func Jdbc(jdbcUrl string, query string) ([]map[string]interface{}, error) {
	// Placeholder - would connect to database and execute query
	return []map[string]interface{}{}, nil
}

// JdbcUpdate executes an update query via JDBC.
//
// Example:
//
//	apoc.load.jdbcUpdate('jdbc:mysql://localhost/db', 'UPDATE users SET active=1') => count
func JdbcUpdate(jdbcUrl string, query string) (int, error) {
	// Placeholder - would execute update query
	return 0, nil
}

// Driver loads data using a custom driver.
//
// Example:
//
//	apoc.load.driver('mongodb', 'mongodb://localhost/db', 'users.find()') => data
func Driver(driverName string, url string, query string) (interface{}, error) {
	// Placeholder - would use custom driver
	return nil, fmt.Errorf("driver not implemented: %s", driverName)
}

// Directory loads all files from a directory.
//
// Example:
//
//	apoc.load.directory('/path/to/dir', '*.json') => file list
func Directory(path string, pattern string) ([]string, error) {
	// Placeholder - would list directory files
	return []string{}, nil
}

// DirectoryTree loads directory structure as a tree.
//
// Example:
//
//	apoc.load.directoryTree('/path/to/dir') => tree structure
func DirectoryTree(path string) (map[string]interface{}, error) {
	// Placeholder - would build directory tree
	return map[string]interface{}{
		"path":     path,
		"children": []interface{}{},
	}, nil
}

// Ldap loads data from an LDAP server.
//
// Example:
//
//	apoc.load.ldap('ldap://server', 'dc=example,dc=com', '(uid=*)') => entries
func Ldap(url string, baseDN string, filter string) ([]map[string]interface{}, error) {
	// Placeholder - would query LDAP
	return []map[string]interface{}{}, nil
}

// JsonSchema validates JSON against a schema.
//
// Example:
//
//	apoc.load.jsonSchema(data, schema) => {valid: true}
func JsonSchema(data interface{}, schema map[string]interface{}) (map[string]interface{}, error) {
	// Placeholder - would validate against JSON schema
	return map[string]interface{}{
		"valid":  true,
		"errors": []string{},
	}, nil
}

// Arrow loads Apache Arrow data.
//
// Example:
//
//	apoc.load.arrow('https://example.com/data.arrow') => data
func Arrow(url string) ([]map[string]interface{}, error) {
	// Placeholder - would load Arrow format
	return []map[string]interface{}{}, nil
}

// Parquet loads Apache Parquet data.
//
// Example:
//
//	apoc.load.parquet('https://example.com/data.parquet') => data
func Parquet(url string) ([]map[string]interface{}, error) {
	// Placeholder - would load Parquet format
	return []map[string]interface{}{}, nil
}

// Avro loads Apache Avro data.
//
// Example:
//
//	apoc.load.avro('https://example.com/data.avro') => data
func Avro(url string) ([]map[string]interface{}, error) {
	// Placeholder - would load Avro format
	return []map[string]interface{}{}, nil
}

// S3 loads data from Amazon S3.
//
// Example:
//
//	apoc.load.s3('s3://bucket/key', {credentials: {...}}) => data
func S3(url string, config map[string]interface{}) ([]byte, error) {
	// Placeholder - would load from S3
	return []byte{}, nil
}

// Gcs loads data from Google Cloud Storage.
//
// Example:
//
//	apoc.load.gcs('gs://bucket/key', {credentials: {...}}) => data
func Gcs(url string, config map[string]interface{}) ([]byte, error) {
	// Placeholder - would load from GCS
	return []byte{}, nil
}

// Azure loads data from Azure Blob Storage.
//
// Example:
//
//	apoc.load.azure('https://account.blob.core.windows.net/container/blob', {}) => data
func Azure(url string, config map[string]interface{}) ([]byte, error) {
	// Placeholder - would load from Azure
	return []byte{}, nil
}

// Kafka loads messages from Apache Kafka.
//
// Example:
//
//	apoc.load.kafka('localhost:9092', 'topic', {}) => messages
func Kafka(brokers string, topic string, config map[string]interface{}) ([]map[string]interface{}, error) {
	// Placeholder - would consume from Kafka
	return []map[string]interface{}{}, nil
}

// Redis loads data from Redis.
//
// Example:
//
//	apoc.load.redis('localhost:6379', 'GET', 'key') => value
func Redis(addr string, command string, args ...string) (interface{}, error) {
	// Placeholder - would execute Redis command
	return nil, nil
}

// Elasticsearch loads data from Elasticsearch.
//
// Example:
//
//	apoc.load.elasticsearch('http://localhost:9200', 'index', 'query') => hits
func Elasticsearch(url string, index string, query string) ([]map[string]interface{}, error) {
	// Placeholder - would query Elasticsearch
	return []map[string]interface{}{}, nil
}

// GraphQL loads data from a GraphQL endpoint.
//
// Example:
//
//	apoc.load.graphql('https://api.com/graphql', 'query { users { id name } }', {}) => data
func GraphQL(ctx context.Context, url string, query string, variables map[string]interface{}) (interface{}, error) {
	payload := map[string]interface{}{
		"query":     query,
		"variables": variables,
	}

	jsonData, err := valueconvert.CypherInputJSON.Marshal(payload)
	if err != nil {
		return nil, err
	}

	body, err := openSource(ctx, url, apocio.Request{
		Method:  http.MethodPost,
		Headers: map[string]string{"Content-Type": "application/json"},
		Body:    jsonData,
	})
	if err != nil {
		return nil, err
	}
	defer body.Close()

	var result map[string]interface{}
	if err := valueconvert.PropertyJSON.NewDecoder(body).Decode(&result); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return result["data"], nil
}

// Rest loads data from a REST API.
//
// Example:
//
//	apoc.load.rest('https://api.com/users', {method: 'GET'}) => data
func Rest(ctx context.Context, url string, config map[string]interface{}) (interface{}, error) {
	method := "GET"
	if m, ok := config["method"].(string); ok {
		method = m
	}

	request := apocio.Request{Method: method}
	if headers, ok := config["headers"].(map[string]string); ok {
		request.Headers = headers
	}

	return decodeJSON(ctx, url, request, decodePropertyDocument)
}

// Binary loads a URL or import file to completion and returns its bytes. It is
// the canonical byte loader: both apoc.load.binary and apoc.load.stream
// register it, and it owns admission, the read and the close inside the
// caller's statement lifetime. Cypher cannot transport a reader, so the stream
// name returns bytes rather than a handle; Stream stays the Go reader API.
//
// config keeps apoc.load.stream's existing optional argument and has no other
// effect. At most one map is accepted; more are rejected before any I/O.
//
// Example:
//
//	apoc.load.binary('https://example.com/file.bin') => bytes
//	apoc.load.stream('https://example.com/data', {format: 'json'}) => bytes
func Binary(ctx context.Context, url string, config ...map[string]interface{}) ([]byte, error) {
	if len(config) > 1 {
		return nil, fmt.Errorf("Binary accepts at most one config map, got %d", len(config))
	}
	body, err := openSource(ctx, url, apocio.Request{})
	if err != nil {
		return nil, err
	}
	defer body.Close()

	return io.ReadAll(body)
}

// Stream loads data as a stream and returns a reader the caller closes. The
// reader reads on ctx: it is usable while the caller's context is live and
// fails with that context's error afterwards. Cypher cannot transport this
// handle, so the plugin's apoc.load.stream registration uses Binary.
//
// Example:
//
//	apoc.load.stream('https://example.com/data', {format: 'json'}) => stream
func Stream(ctx context.Context, url string, config map[string]interface{}) (io.ReadCloser, error) {
	return openSource(ctx, url, apocio.Request{})
}
