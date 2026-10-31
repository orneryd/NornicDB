package storage

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
)

// Runtime origins can be bound before Fabric/namespace resolution. Only schema
// retirement advances this clock. The pending-marker owner recovers work from
// a prior process as startup admission, rather than reusing that process's epoch.
var vectorRetirementClock atomic.Uint64

// CaptureVectorWorkOrigin admits new work: it is the only reader of the
// retirement clock. Call it only where work enters the process (public write
// entry, transaction write admission, startup or recovery, a background job's
// start, AdmitVectorWork). Admitted work transports the value unchanged.
func CaptureVectorWorkOrigin() uint64 { return vectorRetirementClock.Load() }

type vectorWorkOriginKey struct{}

// WithVectorWorkOrigin carries an origin through routed execution/callbacks.
func WithVectorWorkOrigin(ctx context.Context, origin uint64) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if bound, ok := ctx.Value(vectorWorkOriginKey{}).(uint64); ok && bound == origin {
		return ctx
	}
	return context.WithValue(ctx, vectorWorkOriginKey{}, origin)
}

// VectorWorkOrigin reads the origin transported by ctx. It never admits work:
// a missing origin is reported to the caller, not replaced by a new capture.
func VectorWorkOrigin(ctx context.Context) (uint64, bool) {
	if ctx == nil {
		return 0, false
	}
	origin, ok := ctx.Value(vectorWorkOriginKey{}).(uint64)
	return origin, ok
}

// AdmitVectorWork is the admission boundary for a request, callback or job
// entering the process: an origin already transported by ctx is kept (nested
// entry), otherwise the work is admitted now.
func AdmitVectorWork(ctx context.Context) (context.Context, uint64) {
	if origin, ok := VectorWorkOrigin(ctx); ok {
		return ctx, origin
	}
	origin := CaptureVectorWorkOrigin()
	return WithVectorWorkOrigin(ctx, origin), origin
}

// ErrVectorWorkNotAdmitted reports deferred vector work that reached an
// enqueue without a transported origin.
var ErrVectorWorkNotAdmitted = errors.New("vector work reached an enqueue without an admitted origin")

type vectorDescriptor struct{ entity, label, property string }

func vectorDescriptorOf(index *VectorIndex) vectorDescriptor {
	entity := strings.ToUpper(string(index.EntityType))
	if entity == "" {
		entity = "NODE"
	}
	return vectorDescriptor{entity, index.Label, index.Property}
}

// VectorIndexGeneration reports this schema's last published retirement.
func (sm *SchemaManager) VectorIndexGeneration() uint64 {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.vectorIndexGeneration
}

// VectorEntryGenerationCurrent is the sole descriptor-retirement decision.
// A shared property entry remains eligible through a surviving descriptor in
// the same entity domain and label/type. Unrelated implicit entries remain valid.
func (sm *SchemaManager) VectorEntryGenerationCurrent(entity string, labels []string, property string, origin uint64) bool {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	matches := func(label string) bool {
		if label == "" {
			return true
		}
		for _, candidate := range labels {
			if candidate == label {
				return true
			}
		}
		return false
	}
	retired := false
	if sm.vectorPropertyRetired[vectorDescriptor{entity, "", property}] > origin {
		retired = true
	}
	for _, label := range labels {
		if sm.vectorPropertyRetired[vectorDescriptor{entity, label, property}] > origin {
			retired = true
			break
		}
	}
	if !retired {
		return true
	}
	for _, index := range sm.vectorIndexes {
		descriptor := vectorDescriptorOf(index)
		if descriptor.entity == entity && descriptor.property == property && matches(descriptor.label) && sm.vectorPropertyRetired[descriptor] <= origin {
			return true
		}
	}
	return false
}

// One schema publication gives all its retired descriptors one identity.
func (sm *SchemaManager) retireVectorDescriptorsLocked(descriptors ...vectorDescriptor) {
	var epoch uint64
	for _, descriptor := range descriptors {
		if descriptor.property == "" {
			continue
		}
		if epoch == 0 {
			epoch = vectorRetirementClock.Add(1)
			sm.vectorIndexGeneration = epoch
			if sm.vectorPropertyRetired == nil {
				sm.vectorPropertyRetired = make(map[vectorDescriptor]uint64)
			}
		}
		sm.vectorPropertyRetired[descriptor] = epoch
	}
}

// Retirement follows descriptor ownership, not changes to an alias record.
// Explicit staged DROP intents also retire an identical recreated descriptor.
func (sm *SchemaManager) retireChangedVectorIndexesLocked(previous, next map[string]*VectorIndex, dropped map[vectorDescriptor]uint64) {
	retired := make(map[vectorDescriptor]struct{}, len(dropped))
	for _, before := range previous {
		if before == nil {
			continue
		}
		descriptor := vectorDescriptorOf(before)
		if !hasVectorDescriptor(next, descriptor) {
			retired[descriptor] = struct{}{}
		}
	}
	for descriptor := range dropped {
		retired[descriptor] = struct{}{}
	}
	descriptors := make([]vectorDescriptor, 0, len(retired))
	for descriptor := range retired {
		descriptors = append(descriptors, descriptor)
	}
	sm.retireVectorDescriptorsLocked(descriptors...)
}

func hasVectorDescriptor(indexes map[string]*VectorIndex, descriptor vectorDescriptor) bool {
	for _, index := range indexes {
		if vectorDescriptorOf(index) == descriptor {
			return true
		}
	}
	return false
}
