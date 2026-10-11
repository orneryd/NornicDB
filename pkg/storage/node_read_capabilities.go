package storage

var (
	_ NodeWithoutEmbeddingsReader          = (*WALEngine)(nil)
	_ BatchNodeWithoutEmbeddingsReader     = (*WALEngine)(nil)
	_ BatchNodeWithoutEmbeddingsCapability = (*WALEngine)(nil)
	_ NodeWithoutEmbeddingsReader          = (*NamespacedEngine)(nil)
	_ BatchNodeWithoutEmbeddingsReader     = (*NamespacedEngine)(nil)
	_ BatchNodeWithoutEmbeddingsCapability = (*NamespacedEngine)(nil)
)

// batchNodeWithoutEmbeddingsSupported reports whether engine can perform a
// real batched embedding-free read. Wrappers use this shared check so merely
// implementing the forwarding method does not advertise a broken capability.
func batchNodeWithoutEmbeddingsSupported(engine Engine) bool {
	if _, ok := engine.(BatchNodeWithoutEmbeddingsReader); !ok {
		return false
	}
	if capability, ok := engine.(BatchNodeWithoutEmbeddingsCapability); ok {
		return capability.BatchGetNodesWithoutEmbeddingsSupported()
	}
	return true
}
