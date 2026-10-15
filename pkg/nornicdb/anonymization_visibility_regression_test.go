package nornicdb

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
)

func TestAnonymizationPreservesEntitySuppression(t *testing.T) {
	node := &storage.Node{ID: "nornic:private", Properties: map[string]any{"owner_id": "alice", "email": "private@example.com"}, VisibilitySuppressed: true, NamedEmbeddings: map[string][]float32{"title": {1, 2}}, EmbedMeta: map[string]any{"text": "private"}}
	anonymous := (&DB{}).anonymizedNodeCopy(node, "anonymous")
	assert.True(t, anonymous.VisibilitySuppressed, "anonymizing properties must not reveal an entity hidden by policy")
	assert.Equal(t, "anonymous", anonymous.Properties["owner_id"])
	assert.NotContains(t, anonymous.Properties, "email")
	assert.Nil(t, anonymous.NamedEmbeddings, "retain established anonymization behavior for named vectors")
	assert.Nil(t, anonymous.EmbedMeta, "retain established anonymization behavior for embedding metadata")
	assert.Equal(t, "alice", node.Properties["owner_id"])
	assert.Contains(t, node.Properties, "email")
}
