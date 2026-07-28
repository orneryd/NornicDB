# Local OAuth 2.0 Provider

A minimal OAuth 2.0 provider for local testing and development of NornicDB's OAuth integration.

## Features

- **OAuth 2.0 Authorization Code Flow** (RFC 6749)
- **Authorization endpoint** with user consent form
- **Token exchange endpoint** for access tokens
- **Userinfo endpoint** for user profile data
- **OAuth 2.0 Discovery** endpoint for automatic configuration
- **In-memory storage** (no database required)
- **Test users** pre-configured for different roles

## Building

```bash
# Build the binary
go build -o oauth-provider ./cmd/oauth-provider

# Or run directly
go run ./cmd/oauth-provider
```

## Usage

### Basic Usage

```bash
# Start with defaults (port 8888)
./oauth-provider

# Custom port
./oauth-provider -port 9999

# Custom client credentials
./oauth-provider -client-id my-client -client-secret my-secret
```

### Command Line Options

```
-port int
    Port to listen on (default 8888)

-client-id string
    OAuth client ID (default "nornicdb-local-test")

-client-secret string
    OAuth client secret (default "local-test-secret-123")

-issuer string
    OAuth issuer URL (default "http://localhost:8888")
```

## Endpoints

### Authorization Endpoint
```
GET /oauth2/v1/authorize?response_type=code&client_id=...&redirect_uri=...&state=...
```
Shows a consent form where users can select a test user to authenticate as.

### Token Exchange
```
POST /oauth2/v1/token
Content-Type: application/x-www-form-urlencoded

grant_type=authorization_code&code=...&redirect_uri=...&client_id=...&client_secret=...
```
Exchanges an authorization code for an access token.

### Userinfo Endpoint
```
GET /oauth2/v1/userinfo
Authorization: Bearer <access_token>
```
Returns user profile information for the authenticated user.

### Discovery Endpoint
```
GET /.well-known/oauth-authorization-server
```
Returns OAuth 2.0 metadata for automatic configuration.

### Health Check
```
GET /health
```
Returns server status and user count.

## Test Users

The provider includes three pre-configured test users:

| Username | Password | Roles | Email |
|----------|----------|-------|-------|
| admin | admin123 | admin, developer | admin@localhost |
| developer | dev123 | developer | developer@localhost |
| viewer | view123 | viewer | viewer@localhost |

## Integration with NornicDB

### Configuration

Add these environment variables to your NornicDB configuration:

```bash
NORNICDB_AUTH_PROVIDER=oauth
NORNICDB_OAUTH_CLIENT_ID=nornicdb-local-test
NORNICDB_OAUTH_CLIENT_SECRET=local-test-secret-123
NORNICDB_OAUTH_ISSUER=http://localhost:8888
NORNICDB_OAUTH_CALLBACK_URL=http://localhost:7474/auth/oauth/callback
```

### Testing OAuth Flow

1. **Start the OAuth provider:**
   ```bash
   go run ./cmd/oauth-provider
   ```

    The provider registers `http://localhost:7474/auth/oauth/callback` by default.
    Use `-redirect-uri` to register a different exact local callback URL.

2. **Start NornicDB** with OAuth configuration

3. **Navigate to login page** in NornicDB UI

4. **Click "Login with OAuth"** - redirects to provider

5. **Select a test user** from the consent form

6. **Authorize** - redirects back to NornicDB with code

7. **NornicDB exchanges code** for access token

8. **User is authenticated** in NornicDB

## OAuth 2.0 Flow

```
┌─────────┐         ┌──────────────┐         ┌─────────────┐
│ Browser │         │ OAuth        │         │ NornicDB    │
│         │         │ Provider     │         │             │
└────┬────┘         └──────┬──────┘         └──────┬──────┘
     │                      │                       │
     │ 1. Redirect to       │                       │
     │    /oauth2/v1/       │                       │
     │    authorize         │                       │
     ├──────────────────────>│                       │
     │                      │                       │
     │ 2. Show consent form │                       │
     │<──────────────────────│                       │
     │                      │                       │
     │ 3. User selects      │                       │
     │    and authorizes    │                       │
     ├──────────────────────>│                       │
     │                      │                       │
     │ 4. Redirect with     │                       │
     │    authorization code│                       │
     │<──────────────────────│                       │
     │                      │                       │
     │ 5. Redirect to       │                       │
     │    callback with code│                       │
     ├──────────────────────────────────────────────>│
     │                      │                       │
     │                      │ 6. Exchange code      │
     │                      │    for token          │
     │                      │<──────────────────────│
     │                      │                       │
     │                      │ 7. Return access token│
     │                      │──────────────────────>│
     │                      │                       │
     │ 8. User authenticated│                       │
     │<──────────────────────────────────────────────│
```

## Security Notes

⚠️ **This is for testing only!**

- Uses in-memory storage (data lost on restart)
- No password validation (any user can be selected)
- No HTTPS (use only on localhost)
- Simple token generation (not production-grade)
- No rate limiting or security hardening

**Do not use in production!**

## Implementation Details

- **Authorization codes**: Valid for 10 minutes, single-use
- **Access tokens**: Valid for 1 hour
- **Token storage**: In-memory maps (cleaned up automatically)
- **User selection**: HTML form with dropdown
- **Token format**: 64-character hex string (32 random bytes)

## Troubleshooting

### Port already in use

```bash
# Use a different port
./oauth-provider -port 9999
```

### Invalid client credentials

Ensure the client ID and secret match between:
- OAuth provider (`-client-id`, `-client-secret`)
- NornicDB configuration (`NORNICDB_OAUTH_CLIENT_ID`, `NORNICDB_OAUTH_CLIENT_SECRET`)

### Redirect URI mismatch

Ensure the redirect URI in the authorization request matches exactly what's configured in NornicDB.

### Token expired

Access tokens expire after 1 hour. Re-authenticate to get a new token.

