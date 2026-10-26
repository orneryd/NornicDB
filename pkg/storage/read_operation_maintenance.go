package storage

import "context"

// MVCCHeadScanner reads maintenance heads on its owning view.
type MVCCHeadScanner interface {
	IterateMVCCHeads(context.Context, func([]byte, MVCCHead) error) error
}

// MVCCVersionScanner reads a logical key's maintenance history on its view.
type MVCCVersionScanner interface {
	IterateMVCCVersions(context.Context, []byte, func(MVCCVersion, bool, int64) error) error
}

// VisitMVCCHeads retains the maintenance provider and borrowed key through the
// visitor. A namespace cannot supply this engine-wide scan and must refuse it.
func VisitMVCCHeads(engine any, ctx context.Context, visit func([]byte, MVCCHead) error) error {
	type row struct {
		key  []byte
		head MVCCHead
	}
	return VisitWithCapability(engine, func(reader MVCCHeadScanner, deliver func(row) error) error {
		return reader.IterateMVCCHeads(ctx, func(key []byte, head MVCCHead) error { return deliver(row{key, head}) })
	}, nil, func(value row) error { return visit(value.key, value.head) })
}

// VisitMVCCVersions owns history-scan delivery without a current-data fallback.
func VisitMVCCVersions(engine any, ctx context.Context, key []byte, visit func(MVCCVersion, bool, int64) error) error {
	type row struct {
		version    MVCCVersion
		tombstoned bool
		size       int64
	}
	return VisitWithCapability(engine, func(reader MVCCVersionScanner, deliver func(row) error) error {
		return reader.IterateMVCCVersions(ctx, key, func(version MVCCVersion, tombstoned bool, size int64) error {
			return deliver(row{version, tombstoned, size})
		})
	}, nil, func(value row) error { return visit(value.version, value.tombstoned, value.size) })
}
