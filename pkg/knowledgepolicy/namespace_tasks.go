package knowledgepolicy

// NamespaceTaskStore binds a background batch to its storage owner's logical
// lifetime. The lease covers the task's follow-up effects as well as its write.
type NamespaceTaskStore interface {
	CaptureNamespaceTask() any
	RunNamespaceTask(any, string, func() error) error
}
