package search

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/orneryd/nornicdb/pkg/convert"
	"github.com/orneryd/nornicdb/pkg/security"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/pkg/util"
)

// A rejected derived checkpoint can be rebuilt. Operational errors retain
// their original cause and must not turn explicit disk storage into RAM.
var errVectorCheckpointRejected = errors.New("search: vector checkpoint rejected")

func rejectVectorCheckpoint(err error) error {
	return fmt.Errorf("%w: %w", errVectorCheckpointRejected, err)
}

func isRejectedCheckpoint(err error) bool {
	var pathErr *os.PathError
	if errors.As(err, &pathErr) && !os.IsNotExist(pathErr) {
		return false // An operational error takes precedence over joined rejection.
	}
	return errors.Is(err, errVectorCheckpointRejected)
}

// vectorCheckpointCommit is the recoverable publication decision. All paths
// are derived from the configured base, never taken from disk metadata. The
// new metadata and, for a replacement, the new payload are durable BEFORE this
// record is published. Recovery completes that one decision before opening a
// payload. No shutdown/defer is needed to obtain a coherent pair after a crash.
type vectorCheckpointCommit struct {
	Version        int
	MetadataDigest string
	ReplacePayload bool
}

// vectorCheckpointHook is a package-test crash seam at the actual protocol
// boundaries. It does not provide an alternate writer or recovery path.
var vectorCheckpointHook func(string, string)

func vectorCheckpointBoundary(base, phase string) {
	if vectorCheckpointHook != nil {
		vectorCheckpointHook(base, phase)
	}
}

func vectorMetadataDigest(meta VectorFileStoreMeta) (string, error) {
	meta.MetadataDigest = ""
	// The wire form omits empty containers, so an omitted optional field and a
	// present empty one must hash the same. Keys and ordered values stay
	// significant; this only removes nil/empty ambiguity from the digest.
	if len(meta.IDToOrdinal) == 0 {
		meta.IDToOrdinal = nil
	}
	if len(meta.NodeLabels) == 0 {
		meta.NodeLabels = nil
	}
	if len(meta.NodeTypes) == 0 {
		meta.NodeTypes = nil
	}
	if len(meta.NodeNamedVectors) == 0 {
		meta.NodeNamedVectors = nil
	}
	if len(meta.NodePropertyVectors) == 0 {
		meta.NodePropertyVectors = nil
	}
	if len(meta.NodeChunkVectors) == 0 {
		meta.NodeChunkVectors = nil
	}
	// The typed-state JSON owner sorts map keys, including nested association
	// maps. Label/chunk slices deliberately keep their semantic order.
	data, err := convert.PropertyJSON.MarshalState(meta)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

func vectorPayloadDigest(file *os.File, size int64) (string, error) {
	if size < vecHeaderSize {
		return "", rejectVectorCheckpoint(fmt.Errorf("invalid vector payload length %d", size))
	}
	digest, n, err := checkpointStreamDigest(io.NewSectionReader(file, 0, size))
	if err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return "", rejectVectorCheckpoint(err)
		}
		return "", err
	}
	if n != size {
		return "", rejectVectorCheckpoint(io.ErrUnexpectedEOF)
	}
	return digest, nil
}

func decodeVectorCheckpoint(path string) (VectorFileStoreMeta, error) {
	var meta VectorFileStoreMeta
	f, err := security.OpenRootedFile(path, os.O_RDONLY, 0)
	if err != nil {
		return meta, err
	}
	err = util.DecodeMsgpackFile(f.File, &meta)
	closeErr := f.Close()
	if err != nil {
		var pathErr *os.PathError
		if errors.As(err, &pathErr) {
			return meta, err
		}
		return meta, rejectVectorCheckpoint(err)
	}
	if closeErr != nil {
		return meta, closeErr
	}
	if meta.Version != vecFileVersion || meta.Dimensions <= 0 || meta.DataSlots < 0 {
		return meta, rejectVectorCheckpoint(fmt.Errorf("invalid vector metadata version/dimensions/slots: %d/%d/%d", meta.Version, meta.Dimensions, meta.DataSlots))
	}
	digest, err := vectorMetadataDigest(meta)
	if err != nil {
		return meta, rejectVectorCheckpoint(err)
	}
	if meta.MetadataDigest == "" || meta.MetadataDigest != digest || meta.PayloadDigest == "" {
		return meta, rejectVectorCheckpoint(errors.New("vector metadata integrity mismatch"))
	}
	seen := make(map[int64]struct{}, len(meta.IDToOrdinal))
	for id, ordinal := range meta.IDToOrdinal {
		if ordinal < 0 || ordinal >= meta.DataSlots {
			return meta, rejectVectorCheckpoint(fmt.Errorf("ordinal %d for %q outside %d slots", ordinal, id, meta.DataSlots))
		}
		if _, duplicate := seen[ordinal]; duplicate {
			return meta, rejectVectorCheckpoint(fmt.Errorf("duplicate vector ordinal %d", ordinal))
		}
		seen[ordinal] = struct{}{}
	}
	return meta, nil
}

// publishVectorCheckpoint is the sole tuple publisher, used by Save,
// compaction and population relocation. Appends preserve every old committed
// slot, so a metadata-only publication need not copy that immutable prefix.
// Replacements stage the entire payload and use the same completion record.
func publishVectorCheckpoint(base string, meta *VectorFileStoreMeta, replacement bool) error {
	digest, err := vectorMetadataDigest(*meta)
	if err != nil {
		return err
	}
	meta.MetadataDigest = digest
	if err := writeMsgpackSnapshot(base+".meta.next", meta); err != nil {
		return err
	}
	if err := storage.SyncDir(filepath.Dir(base)); err != nil {
		return err
	}
	vectorCheckpointBoundary(base, "prepared")
	decision := vectorCheckpointCommit{Version: 1, MetadataDigest: digest, ReplacePayload: replacement}
	if err := writeMsgpackSnapshot(base+".checkpoint", &decision); err != nil {
		return err
	}
	if err := storage.SyncDir(filepath.Dir(base)); err != nil {
		return err
	}
	vectorCheckpointBoundary(base, "decided")
	return recoverVectorCheckpoint(base)
}

func recoverVectorCheckpoint(base string) error {
	f, err := security.OpenRootedFile(base+".checkpoint", os.O_RDONLY, 0)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	var decision vectorCheckpointCommit
	err = util.DecodeMsgpackFile(f.File, &decision)
	closeErr := f.Close()
	if err != nil {
		var pathErr *os.PathError
		if errors.As(err, &pathErr) {
			return err
		}
		return rejectVectorCheckpoint(err)
	}
	if closeErr != nil {
		return closeErr
	}
	if decision.Version != 1 || decision.MetadataDigest == "" {
		return rejectVectorCheckpoint(errors.New("invalid vector publication decision"))
	}
	metaPath := base + ".meta.next"
	meta, err := decodeVectorCheckpoint(metaPath)
	if os.IsNotExist(err) {
		metaPath = base + ".meta"
		meta, err = decodeVectorCheckpoint(metaPath)
	}
	if err != nil {
		return err
	}
	if meta.MetadataDigest != decision.MetadataDigest {
		return rejectVectorCheckpoint(errors.New("vector publication metadata does not match decision"))
	}
	if decision.ReplacePayload {
		if _, err := security.RootedStat(base + ".vec.next"); err == nil {
			if err := security.RenameRootedFile(base+".vec.next", base+".vec"); err != nil {
				return err
			}
		} else if !os.IsNotExist(err) {
			return err
		}
	}
	vectorCheckpointBoundary(base, "payload")
	if err := storage.SyncDir(filepath.Dir(base)); err != nil {
		return err
	}
	// Load is still the sole installer. Recovery only verifies and completes
	// the already durable pair decision; it never installs an ordinal map.
	payload, err := security.OpenRootedFile(base+".vec", os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	size := int64(vecHeaderSize) + meta.DataSlots*int64(meta.Dimensions)*4
	digest, readErr := vectorPayloadDigest(payload.File, size)
	err = errors.Join(readErr, payload.Close())
	if err != nil {
		return err
	}
	if digest != meta.PayloadDigest {
		return rejectVectorCheckpoint(errors.New("vector publication payload does not match metadata"))
	}
	if metaPath != base+".meta" {
		if err := security.RenameRootedFile(metaPath, base+".meta"); err != nil {
			return err
		}
	}
	vectorCheckpointBoundary(base, "metadata")
	if err := storage.SyncDir(filepath.Dir(base)); err != nil {
		return err
	}
	if err := security.RemoveRootedPath(base + ".checkpoint"); err != nil {
		return err
	}
	vectorCheckpointBoundary(base, "committed")
	return storage.SyncDir(filepath.Dir(base))
}

// replaceCheckpointLocked publishes an already synced .vec.next and then
// switches the active handle and tuple together. appendMu and mu are held.
// Independent read leases keep their old inode/mapping; no old bytes are
// truncated or rewritten. A failed publication is not usable for further
// writes and its prepared files remain available for constructor recovery.
func (v *VectorFileStore) replaceCheckpointLocked(base string, meta VectorFileStoreMeta) error {
	staged, err := security.OpenRootedFile(base+".vec.next", os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	meta.PayloadDigest, err = vectorPayloadDigest(staged.File, vecHeaderSize+meta.DataSlots*int64(meta.Dimensions)*4)
	err = errors.Join(err, staged.Close())
	if err != nil {
		return err
	}
	if err := v.releaseQueryReadMapLocked(); err != nil {
		return err
	}
	// Portable file replacement cannot retain the active append handle.
	// Owned iteration handles/mappings keep their original file independently.
	if err := v.file.Close(); err != nil {
		return err
	}
	v.file = nil
	if err := publishVectorCheckpoint(base, &meta, true); err != nil {
		v.publicationErr = err
		return err
	}
	file, err := security.OpenRootedFile(base+".vec", os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		v.publicationErr = err
		return err
	}
	v.file = file
	v.vecPath, v.metaPath = base+".vec", base+".meta"
	v.installCheckpointLocked(meta)
	return nil
}

// relocateCheckpoint is used for both private population preparation and
// canonical population publication. It copies exact committed bytes, without
// decoding/normalizing vectors or moving metadata independently.
func (v *VectorFileStore) relocateCheckpoint(base string) error {
	v.appendMu.Lock()
	defer v.appendMu.Unlock()
	v.mu.Lock()
	defer v.mu.Unlock()
	if err := v.checkpointHealthLocked(); err != nil {
		return err
	}
	meta := v.checkpointMetadataLocked()
	if err := v.stageCheckpointPrefixLocked(base, meta); err != nil {
		return err
	}
	return v.replaceCheckpointLocked(base, meta)
}

func (v *VectorFileStore) stageCheckpointPrefixLocked(base string, meta VectorFileStoreMeta) error {
	if err := security.EnsureRootedParent(base+".vec.next", 0o755); err != nil {
		return err
	}
	file, err := security.CreateRootedFile(base+".vec.next", 0o644)
	if err != nil {
		return err
	}
	size := int64(vecHeaderSize) + meta.DataSlots*int64(meta.Dimensions)*4
	_, copyErr := io.CopyN(file, io.NewSectionReader(v.file.File, 0, size), size)
	if copyErr == nil {
		copyErr = file.Sync()
	}
	err = errors.Join(copyErr, file.Close())
	if err != nil {
		_ = security.RemoveRootedPath(base + ".vec.next")
	}
	return err
}

// installCheckpointLocked is the one in-memory tuple installation, after Load
// admission or a successful owned publication. The caller holds mu.
func (v *VectorFileStore) installCheckpointLocked(meta VectorFileStoreMeta) {
	v.idToOrdinal = meta.IDToOrdinal
	if v.idToOrdinal == nil {
		v.idToOrdinal = make(map[string]int64)
	}
	v.nextOrdinal, v.buildIndexedCount = meta.DataSlots, meta.BuildIndexedCount
	v.nodeLabels, v.nodeTypes = meta.NodeLabels, meta.NodeTypes
	v.nodeNamedVectors, v.nodePropertyVectors, v.nodeChunkVectors = meta.NodeNamedVectors, meta.NodePropertyVectors, meta.NodeChunkVectors
	v.nodeMetadataPersisted, v.nodeTypesPersisted = meta.NodeMetadataPersisted, meta.NodeTypesPersisted
	v.generation = meta.Generation
	v.checkpointLoaded = true
	v.checkpointDigest = meta.MetadataDigest
	v.obsoleteCount = meta.DataSlots - int64(len(v.idToOrdinal))
	v.publicationErr = nil
}

func (v *VectorFileStore) checkpointHealth() error {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.checkpointHealthLocked()
}

// checkpointHealthLocked preserves a failed publication's cause even when
// replacement has retired the append handle. The caller holds mu.
func (v *VectorFileStore) checkpointHealthLocked() error {
	if v.publicationErr != nil {
		return v.publicationErr
	}
	if v.closed || v.file == nil {
		return errVecFileClosed
	}
	return nil
}

// removeVectorCheckpoint retires every file in the tuple protocol. Callers
// must first retire the live store through its lifecycle owner.
func removeVectorCheckpoint(base string) error {
	var result error
	for _, suffix := range []string{".checkpoint", ".vec", ".meta", ".vec.next", ".meta.next", ".checkpoint.tmp", ".meta.next.tmp"} {
		if err := security.RemoveRootedPath(base + suffix); err != nil && !os.IsNotExist(err) {
			result = errors.Join(result, err)
		}
	}
	return result
}
