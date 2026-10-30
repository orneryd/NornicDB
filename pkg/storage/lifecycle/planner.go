package lifecycle

import (
	"context"
	"encoding/binary"
	"hash/fnv"
	"strings"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// LifecycleStorageEngine is the storage contract required by the lifecycle manager.
type LifecycleStorageEngine interface {
	IterateMVCCHeads(ctx context.Context, yield func(logicalKey []byte, head storage.MVCCHead) error) error
	IterateMVCCVersions(ctx context.Context, logicalKey []byte, yield func(version storage.MVCCVersion, tombstoned bool, sizeBytes int64) error) error
	DeleteMVCCVersion(ctx context.Context, logicalKey []byte, version storage.MVCCVersion) error
	WriteMVCCHead(ctx context.Context, logicalKey []byte, head storage.MVCCHead) error
	ReadMVCCHead(ctx context.Context, logicalKey []byte) (storage.MVCCHead, error)
	DataDirFreeSpace() (int64, error)
}

// PrunePlan describes an immutable lifecycle prune run.
type PrunePlan struct {
	CreatedAt   time.Time
	KeysScanned int
	Entries     []PrunePlanEntry
}

// PrunePlanEntry describes one logical key's prune work.
type PrunePlanEntry struct {
	CreatedAt        time.Time
	LogicalKey       []byte
	HeadVersion      storage.MVCCVersion
	FloorVersion     storage.MVCCVersion
	NewFloorVersion  storage.MVCCVersion
	VersionsToDelete []storage.MVCCVersion
	DebtBytes        int64
	TombstoneDepth   int
}

// PrunePlanner builds immutable plans from MVCC heads and version chains.
type PrunePlanner struct {
	config     LifecycleConfig
	cycleCount int
}

type versionInfo struct {
	version    storage.MVCCVersion
	tombstoned bool
	sizeBytes  int64
}

// NewPrunePlanner creates a planner.
func NewPrunePlanner(config LifecycleConfig) *PrunePlanner {
	return &PrunePlanner{config: config}
}

// Plan scans MVCC heads and computes per-key prune work.
//
// safeFloorForNamespace resolves the prune-safe floor version for a given
// namespace. With per-database MVCC counters the global oldest-reader
// version is no longer comparable across namespaces; the planner must
// resolve a namespace-specific floor for each head it visits. A nil
// callback is treated as "no active readers anywhere" — every namespace
// resolves to maxVersion(), which lets the TTL/MaxVersions bounds drive
// pruning alone.
func (p *PrunePlanner) Plan(ctx context.Context, engine LifecycleStorageEngine, safeFloorForNamespace func(namespace string) storage.MVCCVersion) (*PrunePlan, error) {
	p.cycleCount++
	entries := make([]PrunePlanEntry, 0)
	keysScanned := 0
	ttlBound := TTLBoundVersion(p.config.TTL)
	if safeFloorForNamespace == nil {
		safeFloorForNamespace = func(string) storage.MVCCVersion { return maxVersion() }
	}
	err := engine.IterateMVCCHeads(ctx, func(logicalKey []byte, head storage.MVCCHead) error {
		if !p.shouldScanKey(logicalKey) {
			return nil
		}
		keysScanned++
		versions := make([]versionInfo, 0, 8)
		var debtBytes int64
		var tombstoneDepth int
		if err := engine.IterateMVCCVersions(ctx, logicalKey, func(version storage.MVCCVersion, tombstoned bool, sizeBytes int64) error {
			versions = append(versions, versionInfo{version: version, tombstoned: tombstoned, sizeBytes: sizeBytes})
			debtBytes += sizeBytes
			if tombstoned {
				tombstoneDepth++
			}
			return nil
		}); err != nil {
			return err
		}
		if len(versions) == 0 {
			return nil
		}
		maxVersionsBound := p.maxVersionsBoundVersion(versions)
		nsFloor := safeFloorForNamespace(plannerNamespaceFromLogicalKey(logicalKey))
		newFloor := ComputeSafeFloor(nsFloor, ttlBound, maxVersionsBound, head.FloorVersion)
		toDelete := make([]storage.MVCCVersion, 0)
		for _, version := range versions {
			if version.version.Compare(newFloor) < 0 && version.version.Compare(head.Version) != 0 {
				toDelete = append(toDelete, version.version)
			}
		}
		if len(toDelete) == 0 && head.FloorVersion.Compare(newFloor) == 0 {
			return nil
		}
		entries = append(entries, PrunePlanEntry{
			CreatedAt:        time.Now(),
			LogicalKey:       append([]byte(nil), logicalKey...),
			HeadVersion:      head.Version,
			FloorVersion:     head.FloorVersion,
			NewFloorVersion:  newFloor,
			VersionsToDelete: toDelete,
			DebtBytes:        debtBytes,
			TombstoneDepth:   tombstoneDepth,
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &PrunePlan{CreatedAt: time.Now(), KeysScanned: keysScanned, Entries: entries}, nil
}

// plannerNamespaceFromLogicalKey extracts the namespace from an MVCC head
// logical key. Logical keys are the namespace-prefixed entity ID bytes
// produced by the storage layer; the namespace is the segment up to the
// first ':' separator.
func plannerNamespaceFromLogicalKey(logicalKey []byte) string {
	if len(logicalKey) <= 1 {
		return ""
	}
	parts := strings.SplitN(string(logicalKey[1:]), ":", 2)
	if len(parts) == 0 {
		return ""
	}
	return parts[0]
}

func (p *PrunePlanner) shouldScanKey(logicalKey []byte) bool {
	if p.config.FullScanEveryNCycles > 0 && p.cycleCount%p.config.FullScanEveryNCycles == 0 {
		return true
	}
	if p.config.DebtSampleFraction <= 0 || p.config.DebtSampleFraction >= 1 {
		return true
	}
	hasher := fnv.New32a()
	_, _ = hasher.Write(logicalKey)
	value := float64(binary.BigEndian.Uint32(hasher.Sum(nil))) / float64(^uint32(0))
	return value <= p.config.DebtSampleFraction
}

func (p *PrunePlanner) maxVersionsBoundVersion(versions []versionInfo) storage.MVCCVersion {
	keepHistorical := p.config.MaxVersionsPerKey
	if keepHistorical < 0 {
		keepHistorical = 0
	}
	if p.config.MaxChainHardCap > 0 {
		maxHistoricalByHardCap := p.config.MaxChainHardCap - 1
		if maxHistoricalByHardCap < 0 {
			maxHistoricalByHardCap = 0
		}
		if keepHistorical == 0 || keepHistorical > maxHistoricalByHardCap {
			keepHistorical = maxHistoricalByHardCap
		}
	}
	idx := len(versions) - keepHistorical - 1
	if idx < 0 {
		idx = 0
	}
	return versions[idx].version
}
