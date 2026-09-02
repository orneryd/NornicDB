package testing

import (
	"os"
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
