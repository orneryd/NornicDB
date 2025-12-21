# Swagger UI Test Server

A standalone Swagger UI server for testing and validating the NornicDB OpenAPI specification.

## Overview

This server provides an interactive Swagger UI interface that allows you to:

- **Browse all API endpoints** - See all available NornicDB REST API endpoints
- **Test endpoints interactively** - Make real API calls directly from the browser
- **Validate the OpenAPI spec** - Ensure the specification is correct and complete
- **View request/response schemas** - Understand the data structures for each endpoint

## Quick Start

### Build and Run

```bash
# Build the server
make build-swagger-ui

# Run the server
./bin/swagger-ui
```

Or run directly:

```bash
go run cmd/swagger-ui/main.go
```

### Using the Server

1. **Start the Swagger UI server:**
   ```bash
   ./bin/swagger-ui
   ```

2. **Open your browser:**
   ```
   http://localhost:8080/swagger
   ```

3. **Configure the NornicDB server URL:**
   - In Swagger UI, look for the server dropdown in the top-right
   - Set it to your NornicDB instance (default: `http://localhost:7474`)

4. **Authenticate:**
   - Click the "Authorize" button
   - Enter your NornicDB credentials (username/password)
   - Or paste a JWT token

5. **Test endpoints:**
   - Expand any endpoint
   - Click "Try it out"
   - Fill in the parameters
   - Click "Execute"
   - View the response

## Custom Port

```bash
./bin/swagger-ui -port 9000
```

## Endpoints

- **`/swagger`** - Swagger UI interface
- **`/openapi.yaml`** - OpenAPI specification file

## Integration with NornicDB

### 1. Start NornicDB

```bash
./bin/nornicdb serve
```

NornicDB will be available at `http://localhost:7474`

### 2. Start Swagger UI

```bash
./bin/swagger-ui
```

Swagger UI will be available at `http://localhost:8080`

### 3. Configure Swagger UI

1. Open `http://localhost:8080/swagger`
2. In the server dropdown (top-right), set:
   ```
   http://localhost:7474
   ```
3. Click "Authorize" and enter your credentials
4. Start testing endpoints!

## Example: Testing the Search Endpoint

1. Navigate to the `/nornicdb/search` endpoint
2. Click "Try it out"
3. Enter request body:
   ```json
   {
     "query": "machine learning",
     "limit": 10
   }
   ```
4. Click "Execute"
5. View the response with search results

## Example: Testing Authentication

1. Navigate to `/auth/token`
2. Click "Try it out"
3. Enter credentials:
   ```json
   {
     "username": "admin",
     "password": "password123"
   }
   ```
4. Click "Execute"
5. Copy the `access_token` from the response
6. Click "Authorize" and paste the token
7. Now all authenticated endpoints will work!

## Features

- ✅ **Interactive API Testing** - Test all endpoints directly from the browser
- ✅ **Request/Response Validation** - See exactly what data structures are expected
- ✅ **Authentication Support** - Test with Basic Auth or Bearer tokens
- ✅ **Real-time Validation** - OpenAPI spec validation in real-time
- ✅ **Export/Import** - Download the OpenAPI spec for use in other tools

## Troubleshooting

### CORS Issues

If you encounter CORS errors when testing endpoints:

1. Ensure NornicDB has CORS enabled:
   ```bash
   NORNICDB_SERVER_ENABLE_CORS=true ./bin/nornicdb serve
   ```

2. Or configure CORS origins in NornicDB config

### Authentication Issues

- Make sure you've clicked "Authorize" and entered valid credentials
- For OAuth endpoints, ensure OAuth is configured in NornicDB
- Check that the server URL in Swagger UI matches your NornicDB instance

### OpenAPI Spec Not Found

If the server can't find the OpenAPI spec:

1. Ensure `docs/api-reference/openapi.yaml` exists
2. Rebuild the server: `make build-swagger-ui`
3. Check that you're running from the project root

## Development

### Updating the OpenAPI Spec

1. Edit `docs/api-reference/openapi.yaml`
2. Restart the Swagger UI server
3. Refresh the browser to see changes

### Adding New Endpoints

When adding new endpoints to NornicDB:

1. Update `docs/api-reference/openapi.yaml`
2. Restart Swagger UI server
3. Test the new endpoint in Swagger UI

## Related Documentation

- **[OpenAPI Specification](../docs/api-reference/openapi.yaml)** - Complete API specification
- **[OpenAPI Guide](../docs/api-reference/OPENAPI.md)** - How to use the OpenAPI spec
- **[API Reference](../docs/api-reference/README.md)** - Complete API documentation

---

**Ready to test?** → `make build-swagger-ui && ./bin/swagger-ui`

