package cypher

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/storage"
)

type executorQueryLimitProvider interface {
	GetQueryLimitChecker() interface {
		CheckQueryRate() error
		CheckQueryLimits(context.Context) (context.Context, context.CancelFunc, error)
		GetQueryLimits() interface{}
	}
}

// An executor's engine chain is immutable. Bind its read view once, using the
// same resolver as MATCH, so statement metadata never walks decorators. Values
// supplied by the view (limits, graph revisions and names) remain live.
func executorReadView(engine storage.Engine) storage.Engine {
	view, _ := storage.ViewMetadataForEngine[storage.Engine](engine)
	return view
}

// View identities and revision providers belong to the immutable bound view;
// provider values remain live. Optional data operations use ReadWithCapability
// or VisitWithCapability so execution and refusal handling have one owner.
func viewMetadataForExecutor[T any](e *StorageExecutor) (T, bool) {
	if e.storageReadViewKnown {
		return storage.OwnedCapability[T](e.storageReadView)
	}
	return storage.ViewMetadataForEngine[T](e.storage)
}

func (e *StorageExecutor) queryLimitProvider() executorQueryLimitProvider {
	if e.storageQueryLimitsKnown {
		return e.storageQueryLimits
	}
	provider, _ := storage.MetadataCapabilityForEngine[executorQueryLimitProvider](e.storage)
	return provider
}

func (e *StorageExecutor) compositeRoot() bool {
	provider, ok := viewMetadataForExecutor[interface{ IsComposite() bool }](e)
	return ok && provider.IsComposite()
}
