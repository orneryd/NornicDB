# OpenAPI/Swagger Specification

NornicDB provides a complete OpenAPI 3.0 specification for all REST API endpoints, enabling interactive API documentation and easy integration with API testing tools.

## 📋 Specification File

- **[openapi.yaml](openapi.yaml)** - Complete OpenAPI 3.0 specification

## Quick Start

### Using Swagger UI

1. **Online (Swagger Editor)**
   - Visit [Swagger Editor](https://editor.swagger.io/)
   - Click "File" → "Import file"
   - Select `docs/api-reference/openapi.yaml`
   - Test endpoints directly in the browser

2. **Local Swagger UI**
   ```bash
   # Install Swagger UI
   docker run -p 8080:8080 -e SWAGGER_JSON=/openapi.yaml \
     -v $(pwd)/docs/api-reference/openapi.yaml:/openapi.yaml \
     swaggerapi/swagger-ui
   ```
   Then visit `http://localhost:8080`

### Using Postman

1. Open Postman
2. Click "Import" → "File"
3. Select `docs/api-reference/openapi.yaml`
4. All endpoints will be imported with example requests

### Using Insomnia

1. Open Insomnia
2. Click "Create" → "Import/Export" → "Import Data" → "From File"
3. Select `docs/api-reference/openapi.yaml`
4. All endpoints will be available for testing

## 📚 What's Included

The OpenAPI specification includes:

- **All REST Endpoints** - Complete coverage of all HTTP API endpoints
- **Request/Response Schemas** - Detailed schemas for all requests and responses
- **Authentication Methods** - Documentation of Basic Auth and Bearer Token authentication
- **Error Responses** - Standard error response formats
- **Examples** - Example requests and responses for each endpoint

## 🔐 Authentication

The OpenAPI spec documents two authentication methods:

1. **Bearer Token (JWT)**
   ```yaml
   security:
     - bearerAuth: []
   ```

2. **Basic Auth (Neo4j Compatible)**
   ```yaml
   security:
     - basicAuth: []
   ```

To authenticate in Swagger UI:
- Click the "Authorize" button
- Enter your credentials
- All requests will include the authentication header

## 📖 Endpoint Categories

### Health & Status
- `GET /health` - Health check (public)
- `GET /status` - Server status (authenticated)
- `GET /metrics` - Prometheus metrics (authenticated)

### Authentication
- `POST /auth/token` - Get JWT token
- `POST /auth/logout` - Logout
- `GET /auth/me` - Current user info
- `POST /auth/api-token` - Generate API token (admin)
- `GET /auth/oauth/redirect` - OAuth redirect
- `GET /auth/oauth/callback` - OAuth callback
- User management endpoints (admin only)

### Neo4j Compatible
- `POST /db/{database}/tx/commit` - Execute Cypher query

### Search & Embeddings
- `POST /nornicdb/search` - Hybrid search. 200 responses include `bm25_enabled` and `vector_enabled` so clients can see which path produced the result set. 503 with `request_status: search_disabled_for_database` (`retryable: false`) when both indexes are disabled for the target database; 503 with `request_status: search_not_ready` (`retryable: true`) when an eager build is mid-flight. Lazy-warmed databases block the request synchronously inside `Service.EnsureWarm` and return 200 — no transient lazy 503 is emitted.
- `POST /nornicdb/similar` - Vector similarity search
- `GET /nornicdb/decay` - Memory decay statistics
- Embedding management endpoints

### Graph (Nornic Extensions)
- `POST /nornicdb/graph/{database}/neighborhood` - Build a neighborhood subgraph from `node_ids`
- `POST /nornicdb/graph/{database}/expand` - Expand an existing graph selection (same request shape as neighborhood)
- `POST /nornicdb/graph/{database}/path` - Resolve path between `source_node_id` and `target_node_id`
- `POST /nornicdb/graph/{database}/temporal` - Reconstruct snapshot graph at `as_of`
- `POST /nornicdb/graph/{database}/diff` - Diff target graph `as_of` against `compare_to` or `current`

### Admin & System
- `GET /admin/stats` - System statistics
- `GET /admin/config` - Server configuration
- `POST /admin/backup` - Create backup
- GPU control endpoints

### GDPR Compliance
- `GET /gdpr/export` - GDPR data export
- `POST /gdpr/delete` - GDPR erasure request

### GraphQL & AI
- `POST /graphql` - GraphQL endpoint
- `GET /graphql/playground` - GraphQL Playground
- MCP and Heimdall endpoints

## 🔧 Code Generation

You can generate client libraries from the OpenAPI spec using tools like:

### OpenAPI Generator

```bash
# Install OpenAPI Generator
npm install @openapitools/openapi-generator-cli -g

# Generate Python client
openapi-generator-cli generate \
  -i docs/api-reference/openapi.yaml \
  -g python \
  -o ./generated/python-client

# Generate JavaScript client
openapi-generator-cli generate \
  -i docs/api-reference/openapi.yaml \
  -g javascript \
  -o ./generated/js-client

# Generate Go client
openapi-generator-cli generate \
  -i docs/api-reference/openapi.yaml \
  -g go \
  -o ./generated/go-client
```

### Swagger Codegen

```bash
# Generate Java client
swagger-codegen generate \
  -i docs/api-reference/openapi.yaml \
  -l java \
  -o ./generated/java-client
```

## 📝 Example Usage

### Graph Endpoint Quick Notes

- `node_ids` are normalized (trimmed and de-duplicated); whitespace-only values are rejected with `400`.
- `path` returns `404` when no path exists, and `400` when traversal limits are exhausted before target discovery.
- `temporal` and `diff` require `as_of`.
- In `diff` responses:
  - `meta.as_of` = target graph timestamp
  - `meta.compare_to` = baseline timestamp or `"current"`
- `existing_node_ids` and `existing_edge_ids` in `GraphRequest` are currently advisory-only and may be ignored by the server.

### Testing with curl

```bash
# 1. Get authentication token
TOKEN=$(curl -X POST http://localhost:7474/auth/token \
  -H "Content-Type: application/json" \
  -d '{"username": "admin", "password": "password123"}' \
  | jq -r '.access_token')

# 2. Use token for authenticated requests
curl -X POST http://localhost:7474/nornicdb/search \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "query": "machine learning",
    "limit": 10
  }'
```

### Testing with Python

```python
import requests

# Authenticate
response = requests.post(
    "http://localhost:7474/auth/token",
    json={"username": "admin", "password": "password123"}
)
token = response.json()["access_token"]

# Search
response = requests.post(
    "http://localhost:7474/nornicdb/search",
    headers={"Authorization": f"Bearer {token}"},
    json={"query": "machine learning", "limit": 10}
)
results = response.json()
```

## 🔄 Keeping the Spec Updated

The OpenAPI specification is maintained manually and should be updated whenever:

- New endpoints are added
- Request/response schemas change
- Authentication methods change
- Error response formats change

To update the spec:

1. Edit `docs/api-reference/openapi.yaml`
2. Validate the YAML syntax
3. Test endpoints in Swagger UI
4. Update this README if needed

## 📚 Related Documentation

- **[API Reference](README.md)** - Complete API documentation
- **[User Guides](../user-guides/README.md)** - Usage examples
- **[Getting Started](../getting-started/README.md)** - Installation and setup

## 🐛 Reporting Issues

If you find any issues with the OpenAPI specification:

1. Check that the endpoint behavior matches the spec
2. Verify request/response schemas are correct
3. Report issues on GitHub with:
   - Endpoint path
   - Expected vs actual behavior
   - Example request/response

---

**Ready to test?** → **[OpenAPI Spec](openapi.yaml)**  
**Need help?** → **[User Guides](../user-guides/README.md)**
