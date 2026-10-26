package storage

import (
	"reflect"
	"sync/atomic"
)

// ReadViewBinding captures only immutable topology. Each layer's availability
// and the final view's scope remain live operation decisions.
type ReadViewBinding struct {
	owner  any
	issuer reflect.Type
	layers []any
}
type ReadViewBindingProvider interface{ ReadCapabilityBinding() *ReadViewBinding }

// ReadBindingSlot holds an issued topology. Replacing a source requires an
// explicit RebindReadView at issuance; row lookups never revalidate descendants.
type ReadBindingSlot struct {
	value atomic.Pointer[ReadViewBinding]
}

func (s *ReadBindingSlot) Store(binding *ReadViewBinding) { s.value.Store(binding) }
func (s *ReadBindingSlot) Binding(owner Engine, _ Engine) *ReadViewBinding {
	capabilityProviderVisit()
	if binding := s.value.Load(); binding != nil {
		return binding
	}
	binding := BindReadView(owner)
	s.value.CompareAndSwap(nil, binding)
	return s.value.Load()
}

type ReadViewBindingPublisher interface{ PublishReadCapabilityBinding(*ReadViewBinding) }

// BindReadView binds an operation's actual sources using the common cursor.
// It bypasses cached descendant descriptors once, at operation issuance.
func BindReadView(engine Engine) *ReadViewBinding {
	binding := &ReadViewBinding{issuer: reflect.TypeOf(engine)}
	owner, ok := engineCapability[any](engine, capabilityBindReadView, func(layer any) bool { binding.layers = append(binding.layers, layer); return true })
	if ok {
		binding.owner = owner
	}
	return binding
}

// RebindReadView publishes an explicit source replacement to the issued stack.
// The replacement writer calls it before starting readers; retained operations
// keep the adapter they captured. No per-row reflection or dependency walk.
func RebindReadView(engine Engine) *ReadViewBinding {
	binding := BindReadView(engine)
	for i, layer := range binding.layers {
		if publisher, ok := OwnedCapability[ReadViewBindingPublisher](layer); ok {
			publisher.PublishReadCapabilityBinding(&ReadViewBinding{owner: binding.owner, issuer: reflect.TypeOf(layer), layers: binding.layers[i:]})
		}
	}
	return binding
}
func (t *TracedEngine) PublishReadCapabilityBinding(binding *ReadViewBinding) {
	t.readBinding.Store(binding)
}
func (w *WALEngine) PublishReadCapabilityBinding(binding *ReadViewBinding) {
	w.readBinding.Store(binding)
}

func (b *ReadViewBinding) admits(request reflect.Type) bool {
	for _, layer := range b.layers {
		if availability, ok := OwnedCapability[ReadCapabilityAvailability](layer); ok && !availability.ReadCapabilityAvailable(request) {
			return false
		}
		if batch, ok := OwnedCapability[BatchNodeWithoutEmbeddingsCapability](layer); ok && ReadCapabilityIncludes[BatchNodeWithoutEmbeddingsReader](request) && !batch.BatchGetNodesWithoutEmbeddingsSupported() {
			return false
		}
	}
	return true
}

func (t *TracedEngine) ReadCapabilityBinding() *ReadViewBinding {
	return t.readBinding.Binding(t, t.Engine)
}
func (w *WALEngine) ReadCapabilityBinding() *ReadViewBinding {
	return w.readBinding.Binding(w, w.engine)
}
