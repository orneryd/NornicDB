package testing

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestContainerAuthDefaultsAndExposure(t *testing.T) {
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
		assertSecureComposeNode(t, path, &document)
	}
}

func assertSecureComposeNode(t *testing.T, path string, node *yaml.Node) {
	t.Helper()
	if node.Kind == yaml.MappingNode {
		for index := 0; index+1 < len(node.Content); index += 2 {
			key, value := node.Content[index], node.Content[index+1]
			switch key.Value {
			case "ports":
				for _, port := range value.Content {
					require.Equal(t, "127.0.0.1:7474:7474", strings.TrimSpace(port.Value), path)
				}
			case "environment":
				assertSecureEnvironmentNode(t, path, value)
			}
		}
	}
	for _, child := range node.Content {
		assertSecureComposeNode(t, path, child)
	}
}

func assertSecureEnvironmentNode(t *testing.T, path string, node *yaml.Node) {
	t.Helper()
	for index, child := range node.Content {
		value := strings.TrimSpace(child.Value)
		if strings.Contains(value, "NORNICDB_NO_AUTH") {
			require.NotContains(t, strings.ToLower(value), "true", path)
			if node.Kind == yaml.MappingNode && index+1 < len(node.Content) {
				require.Equal(t, "false", strings.ToLower(strings.TrimSpace(node.Content[index+1].Value)), path)
			}
		}
	}
}
