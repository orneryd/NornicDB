package testing

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestContainerNoAuthDefaultsEmitWarning(t *testing.T) {
	root := filepath.Clean("..")
	dockerfiles, err := filepath.Glob(filepath.Join(root, "docker", "Dockerfile*"))
	require.NoError(t, err)
	require.NotEmpty(t, dockerfiles)
	authDefaultImages := 0
	for _, path := range dockerfiles {
		content, readErr := os.ReadFile(path)
		require.NoError(t, readErr)
		if !strings.Contains(string(content), "NORNICDB_NO_AUTH=") {
			continue
		}
		authDefaultImages++
		require.Contains(t, string(content), "NORNICDB_NO_AUTH=true", path)
	}
	require.Positive(t, authDefaultImages)
	entrypoint, err := os.ReadFile(filepath.Join(root, "docker", "entrypoint.sh"))
	require.NoError(t, err)
	require.Contains(t, string(entrypoint), `"event_id":"security.insecure_no_auth.enabled"`)

	composeFiles, err := filepath.Glob(filepath.Join(root, "docker-compose*.yml"))
	require.NoError(t, err)
	nested, err := filepath.Glob(filepath.Join(root, "docker", "docker-compose*.yml"))
	require.NoError(t, err)
	composeFiles = append(composeFiles, nested...)
	require.NotEmpty(t, composeFiles)

	for _, path := range composeFiles {
		content, readErr := os.ReadFile(path)
		require.NoError(t, readErr)
		var document yaml.Node
		require.NoError(t, yaml.Unmarshal(content, &document), path)
		require.True(t, hasNoAuthDefault(t, path, &document), "%s must declare a no-auth compatibility default", path)
	}
}

func TestRuntimeImagesPreserveIngressConfiguration(t *testing.T) {
	root := filepath.Clean("..")
	dockerfiles, err := filepath.Glob(filepath.Join(root, "docker", "Dockerfile*"))
	require.NoError(t, err)

	runtimeImages := 0
	for _, path := range dockerfiles {
		content, readErr := os.ReadFile(path)
		require.NoError(t, readErr)
		text := string(content)
		if !strings.Contains(text, "ENTRYPOINT") {
			continue
		}
		runtimeImages++
		require.Contains(t, text, "NORNICDB_ADDRESS=0.0.0.0", path)
		require.Contains(t, text, "COPY docker/healthcheck.sh", path)
		require.Contains(t, text, "CMD /app/healthcheck.sh", path)
		require.Contains(t, text, "EXPOSE 7473", path)
		require.Contains(t, text, "6334", path)
	}
	require.Positive(t, runtimeImages)
}

func TestContainerEntrypointPreservesIngressEnvironment(t *testing.T) {
	tempDir := t.TempDir()
	capturePath := filepath.Join(tempDir, "capture.sh")
	capture := `#!/bin/sh
printf 'ARG=%s\n' "$@"
printf 'HTTP_ADDRESS=%s\n' "$NORNICDB_HTTP_ADDRESS"
printf 'HTTPS_PORT=%s\n' "$NORNICDB_HTTPS_PORT"
printf 'BOLT_ENABLED=%s\n' "$NORNICDB_BOLT_ENABLED"
printf 'GRPC_ADDRESS=%s\n' "$NORNICDB_QDRANT_GRPC_LISTEN_ADDR"
printf 'TRACE_GRAPHQL=%s\n' "$NORNICDB_TRACE_GRAPHQL"
`
	require.NoError(t, os.WriteFile(capturePath, []byte(capture), 0o755))

	entrypoint := filepath.Join("..", "docker", "entrypoint.sh")
	command := exec.Command("sh", entrypoint)
	command.Env = append(os.Environ(),
		"NORNICDB_BIN="+capturePath,
		"NORNICDB_NO_AUTH=false",
		"NORNICDB_EMBEDDING_PROVIDER=openai",
		"NORNICDB_HTTP_PORT=17474",
		"NORNICDB_HTTP_ADDRESS=127.0.0.2",
		"NORNICDB_HTTPS_PORT=17473",
		"NORNICDB_BOLT_PORT=17687",
		"NORNICDB_BOLT_ENABLED=false",
		"NORNICDB_QDRANT_GRPC_LISTEN_ADDR=127.0.0.3:16334",
		"NORNICDB_TRACE_GRAPHQL=1",
	)
	output, err := command.CombinedOutput()
	require.NoError(t, err, string(output))
	text := string(output)
	require.Contains(t, text, "ARG=serve")
	require.NotContains(t, text, "ARG=--http-port=")
	require.NotContains(t, text, "ARG=--bolt-port=")
	require.NotContains(t, text, "ARG=--address=")
	require.Contains(t, text, "HTTP_ADDRESS=127.0.0.2")
	require.Contains(t, text, "HTTPS_PORT=17473")
	require.Contains(t, text, "BOLT_ENABLED=false")
	require.Contains(t, text, "GRPC_ADDRESS=127.0.0.3:16334")
	require.Contains(t, text, "TRACE_GRAPHQL=1")
}

func TestContainerHealthcheckUsesEffectiveHTTPTransport(t *testing.T) {
	tests := []struct {
		name string
		env  []string
		want string
	}{
		{
			name: "HTTP address port and base path",
			env: []string{
				"NORNICDB_HTTP_ADDRESS=127.0.0.2",
				"NORNICDB_HTTP_PORT=17474",
				"NORNICDB_BASE_PATH=/graph/",
			},
			want: "--spider\n-q\nhttp://127.0.0.2:17474/graph/health\n",
		},
		{
			name: "native HTTPS port",
			env: []string{
				"NORNICDB_ADDRESS=0.0.0.0",
				"NORNICDB_HTTPS_ENABLED=true",
				"NORNICDB_HTTPS_PORT=17473",
			},
			want: "--spider\n-q\n--no-check-certificate\nhttps://127.0.0.1:17473/health\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tempDir := t.TempDir()
			wgetPath := filepath.Join(tempDir, "wget")
			require.NoError(t, os.WriteFile(wgetPath, []byte("#!/bin/sh\nprintf '%s\\n' \"$@\"\n"), 0o755))

			command := exec.Command("sh", filepath.Join("..", "docker", "healthcheck.sh"))
			command.Env = append([]string{"PATH=" + tempDir}, test.env...)
			output, err := command.CombinedOutput()
			require.NoError(t, err, string(output))
			require.Equal(t, test.want, string(output))
		})
	}
}

func TestComposeFilesForwardProxyEnvironment(t *testing.T) {
	required := []string{
		"NORNICDB_ADDRESS",
		"NORNICDB_BASE_PATH",
		"NORNICDB_AUTH",
		"NORNICDB_AUTH_JWT_SECRET",
		"NORNICDB_AUTH_PROVIDER",
		"NORNICDB_AUTH_TOKEN_EXPIRY",
		"NORNICDB_MIN_PASSWORD_LENGTH",
		"NORNICDB_NO_AUTH",
		"NORNICDB_OAUTH_ISSUER",
		"NORNICDB_OAUTH_CLIENT_ID",
		"NORNICDB_OAUTH_CLIENT_SECRET",
		"NORNICDB_OAUTH_CALLBACK_URL",
		"NORNICDB_CORS_ENABLED",
		"NORNICDB_CORS_ORIGINS",
		"NORNICDB_HTTP_ENABLED",
		"NORNICDB_HTTP_PORT",
		"NORNICDB_HTTP_ADDRESS",
		"NORNICDB_HTTP_TRUSTED_PROXIES",
		"NORNICDB_HTTPS_ENABLED",
		"NORNICDB_HTTPS_PORT",
		"NORNICDB_HTTP_TLS_CERT",
		"NORNICDB_HTTP_TLS_KEY",
		"NORNICDB_BOLT_ENABLED",
		"NORNICDB_BOLT_PORT",
		"NORNICDB_BOLT_ADDRESS",
		"NORNICDB_BOLT_SERVER_ANNOUNCEMENT",
		"NORNICDB_BOLT_TLS_ENABLED",
		"NORNICDB_BOLT_TLS_REQUIRE",
		"NORNICDB_BOLT_TLS_CERT",
		"NORNICDB_BOLT_TLS_KEY",
		"NORNICDB_BOLT_TLS_CLIENT_CA",
		"NORNICDB_BOLT_TLS_CLIENT_AUTH_MODE",
		"NORNICDB_BOLT_SNIFF_TIMEOUT",
		"NORNICDB_BOLT_AUTH_TIMEOUT",
		"NORNICDB_BOLT_STATEMENT_TIMEOUT",
		"NORNICDB_BOLT_WEBSOCKET_ENABLED",
		"NORNICDB_BOLT_WEBSOCKET_ALLOWED_ORIGINS",
		"NORNICDB_BOLT_WEBSOCKET_MAX_MESSAGE_SIZE",
		"NORNICDB_BOLT_WEBSOCKET_WRITE_BUFFER_SIZE",
		"NORNICDB_BOLT_WEBSOCKET_PING_INTERVAL",
		"NORNICDB_BOLT_WEBSOCKET_PONG_TIMEOUT",
		"NORNICDB_TLS_DIR",
		"NORNICDB_QDRANT_GRPC_ENABLED",
		"NORNICDB_QDRANT_GRPC_LISTEN_ADDR",
		"NORNICDB_QDRANT_GRPC_MAX_VECTOR_DIM",
		"NORNICDB_QDRANT_GRPC_MAX_BATCH_POINTS",
		"NORNICDB_QDRANT_GRPC_MAX_TOP_K",
		"NORNICDB_QDRANT_GRPC_TLS_ENABLED",
		"NORNICDB_QDRANT_GRPC_TLS_CERT",
		"NORNICDB_QDRANT_GRPC_TLS_KEY",
		"NORNICDB_QDRANT_GRPC_TLS_CLIENT_CA",
		"NORNICDB_QDRANT_GRPC_TLS_CLIENT_AUTH_MODE",
		"NORNICDB_TRACE_GRAPHQL",
	}

	for _, path := range composeFiles(t) {
		t.Run(filepath.Base(path), func(t *testing.T) {
			content, err := os.ReadFile(path)
			require.NoError(t, err)
			text := string(content)
			for _, name := range required {
				require.Contains(t, text, name+"=${"+name, "%s must forward %s", path, name)
			}
			if strings.Contains(text, "healthcheck:") {
				require.Contains(t, text, "/app/healthcheck.sh", "%s must use the image health check", path)
				require.NotContains(t, text, "localhost:7474", "%s must not hard-code plaintext health transport", path)
			}
		})
	}
}

func composeFiles(t *testing.T) []string {
	t.Helper()
	root := filepath.Clean("..")
	composeFiles, err := filepath.Glob(filepath.Join(root, "docker-compose*.yml"))
	require.NoError(t, err)
	nested, err := filepath.Glob(filepath.Join(root, "docker", "docker-compose*.yml"))
	require.NoError(t, err)
	composeFiles = append(composeFiles, nested...)
	require.NotEmpty(t, composeFiles)
	return composeFiles
}

func hasNoAuthDefault(t *testing.T, path string, node *yaml.Node) bool {
	t.Helper()
	found := false
	if node.Kind == yaml.MappingNode {
		for index := 0; index+1 < len(node.Content); index += 2 {
			key, value := node.Content[index], node.Content[index+1]
			if key.Value == "environment" && hasNoAuthEnvironmentDefault(t, path, value) {
				found = true
			}
		}
	}
	for _, child := range node.Content {
		if hasNoAuthDefault(t, path, child) {
			found = true
		}
	}
	return found
}

func hasNoAuthEnvironmentDefault(t *testing.T, path string, node *yaml.Node) bool {
	t.Helper()
	found := false
	if node.Kind == yaml.MappingNode {
		for index := 0; index+1 < len(node.Content); index += 2 {
			key, value := node.Content[index], node.Content[index+1]
			if strings.TrimSpace(key.Value) == "NORNICDB_NO_AUTH" {
				require.Contains(t, strings.ToLower(strings.TrimSpace(value.Value)), "true", path)
				found = true
			}
		}
		return found
	}
	for _, child := range node.Content {
		value := strings.TrimSpace(child.Value)
		if strings.Contains(value, "NORNICDB_NO_AUTH") {
			require.Contains(t, strings.ToLower(value), "true", path)
			found = true
		}
	}
	return found
}
