package multidb

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// managedDatabaseRuntime attaches the existing DatabaseInfo accounting owner,
// admission gate and limit policy to one physical engine/namespace lifetime.
// Other manager metadata handles delegate here; they keep no maintained mirror.
type managedDatabaseRuntime struct {
	accounting     *DatabaseInfo
	namespace      string
	observer       storedEntityObserverInitializer
	registry       storage.ManagedEntityRuntimeRegistry
	boundary       storage.StoredEntityReadBoundary
	boundaryOnce   sync.Once
	policy         *databaseLimitPolicy
	policyUpdateMu sync.Mutex
	writeMu        writeAdmission
	transport      atomic.Bool
	retired        atomic.Bool
}

func (info *DatabaseInfo) accountingOwner() *DatabaseInfo {
	if runtime := info.managed.Load(); runtime != nil {
		return runtime.accounting
	}
	return info
}
func (info *DatabaseInfo) limitsSnapshot() *Limits {
	if runtime := info.managed.Load(); runtime != nil {
		return runtime.policy.snapshot()
	}
	return cloneLimits(info.Limits)
}

// bindManagedRuntime is only a short registry attachment. It never scans,
// invokes admission, or waits for a native lifecycle lease.
func bindManagedRuntime(info *DatabaseInfo, limits *Limits, namespace string, owner managedEntityOwner) (*managedDatabaseRuntime, error) {
	if current := info.managed.Load(); current != nil {
		if owner.route.RequiresSubmission {
			current.transport.Store(true)
		}
		return current, nil
	}
	var runtime *managedDatabaseRuntime
	if owner.registry != nil {
		if existing := owner.registry.GetOrAttachManagedEntityRuntime(namespace, nil); existing != nil {
			var ok bool
			runtime, ok = existing.(*managedDatabaseRuntime)
			if !ok || runtime == nil {
				return nil, fmt.Errorf("invalid managed runtime attachment: %w", storage.ErrInvalidData)
			}
		}
	}
	if runtime == nil {
		policy := &databaseLimitPolicy{}
		policy.updateLimits(limits)
		candidate := &managedDatabaseRuntime{accounting: info, namespace: namespace, observer: owner.observer, registry: owner.registry, boundary: owner.boundary, policy: policy}
		runtime = candidate
		if owner.registry != nil {
			var ok bool
			runtime, ok = owner.registry.GetOrAttachManagedEntityRuntime(namespace, candidate).(*managedDatabaseRuntime)
			if !ok || runtime == nil {
				return nil, fmt.Errorf("invalid managed runtime attachment: %w", storage.ErrInvalidData)
			}
		}
	}
	if owner.route.RequiresSubmission {
		runtime.transport.Store(true)
	}
	info.managed.CompareAndSwap(nil, runtime)
	return info.managed.Load(), nil
}

func (runtime *managedDatabaseRuntime) requiresManaged(limits *Limits) bool {
	info := runtime.accounting
	info.sizeMu.RLock()
	tracked := info.usageInitialized || info.sizeInitialized
	info.sizeMu.RUnlock()
	return runtime.transport.Load() || tracked || (limits != nil && (limits.Storage.MaxNodes > 0 || limits.Storage.MaxEdges > 0 || limits.Storage.MaxBytes > 0 || limits.Rate.MaxWritesPerSecond > 0))
}

// initializeManagedUsage is pure initialization, not leader/quota admission.
// Native peer application and helpers may call it before their first lifecycle
// lease even when a peer's local quota would reject that accepted change.
func (runtime *managedDatabaseRuntime) initializeManagedUsage(ctx context.Context, boundary storage.StoredEntityReadBoundary) error {
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	limits := runtime.policy.snapshot()
	if !runtime.requiresManaged(limits) {
		return nil
	}
	trackBytes := limits != nil && limits.Storage.MaxBytes > 0
	return runtime.initializeStorageUsage(trackBytes, boundary)
}

// ResetManagedEntityUsage is called by the successful physical graph replacement
// owner while its lifecycle lease is held, before listeners or new admission.
// Pending accepted allowances retain their own terminal owner.
func (runtime *managedDatabaseRuntime) ResetManagedEntityUsage(transition storage.ManagedEntityUsageTransition) {
	info := runtime.accounting
	info.sizeMu.Lock()
	defer info.sizeMu.Unlock()
	if transition == storage.ManagedEntityUsageRestored {
		// Preserve last-known reporting values and sticky byte tracking. The
		// existing initializer will recapture actual restored contents before
		// the next admission, retaining policy and pending accepted allowances.
		info.usageInitialized = false
		return
	}
	info.usageNodes, info.usageEdges = 0, 0
	info.nodeSize, info.edgeSize, info.totalSize = 0, 0, 0
}

func (runtime *managedDatabaseRuntime) retireDatabase() {
	runtime.retired.Store(true)
	runtime.ResetManagedEntityUsage(storage.ManagedEntityUsageEmpty)
	info := runtime.accounting
	info.sizeMu.Lock()
	info.usageInitialized, info.sizeInitialized = false, false
	info.sizeMu.Unlock()
	runtime.policy.updateLimits(nil)
	if runtime.registry != nil {
		runtime.registry.RetireManagedEntityRuntime(runtime.namespace)
	}
}

func (m *DatabaseManager) configSystemDatabase() string {
	if m.config != nil && m.config.SystemDatabase != "" {
		return m.config.SystemDatabase
	}
	return "system"
}

func (runtime *managedDatabaseRuntime) activateDatabase(limits *Limits) {
	runtime.policy.updateLimits(limits)
	runtime.retired.Store(false)
}
