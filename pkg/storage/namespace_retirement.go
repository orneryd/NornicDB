package storage

import "errors"

type namespaceRetirementHook struct {
	logical  func(string)
	complete func(string) error
}

// Catalog owners subscribe at the physical namespace owner. Thus replay,
// forwarded commands and native namespace deletion have the same completion
// boundary as the originating manager's DROP call.
func RegisterNamespaceRetirement(engine Engine, complete func(string) error, logical ...func(string)) func() {
	if owner, ok := engine.(interface{ namespaceLifetimeOwner() *BadgerEngine }); ok {
		b := owner.namespaceLifetimeOwner()
		id := b.namespaceRetirementHookID.Add(1)
		hook := namespaceRetirementHook{complete: complete}
		if len(logical) != 0 {
			hook.logical = logical[0]
		}
		b.namespaceRetirementHooks.Store(id, hook)
		return func() { b.namespaceRetirementHooks.Delete(id) }
	}
	if wrapper, ok := engine.(EngineUnwrapper); ok {
		return RegisterNamespaceRetirement(wrapper.GetInnerEngine(), complete, logical...)
	}
	return func() {}
}

func (b *BadgerEngine) completeNamespaceRetirement(namespace string) error {
	var err error
	b.namespaceRetirementHooks.Range(func(_, hook any) bool {
		err = errors.Join(err, hook.(namespaceRetirementHook).complete(namespace))
		return true
	})
	return err
}

func (b *BadgerEngine) publishLogicalNamespaceRetirement(namespace string) {
	b.namespaceRetirementHooks.Range(func(_, value any) bool {
		if logical := value.(namespaceRetirementHook).logical; logical != nil {
			logical(namespace)
		}
		return true
	})
}
