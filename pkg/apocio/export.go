package apocio

import (
	"context"
	"fmt"

	"github.com/orneryd/nornicdb/pkg/security"
)

// ExportDirectoryPolicy selects the established destination-directory behavior.
type ExportDirectoryPolicy uint8

const (
	// ExistingDirectory writes only where the parent directory already exists.
	ExistingDirectory ExportDirectoryPolicy = iota
	// CreateDirectories creates the destination's parent directories.
	CreateDirectories
)

// ExportDestination retains a path admitted by the caller's export capability.
// Its zero value names no file. The resolved path is private to avoid resolving
// an already rooted destination a second time.
type ExportDestination struct{ path string }

// AdmitExport validates the caller and resolves a named destination through the
// existing export capability. An empty destination performs no I/O and needs no
// capability. A canceled caller is rejected before either route.
//
// Example:
//
//	destination, err := apocio.AdmitExport(ctx, "graph.json")
//	if err != nil { return err }
//	return apocio.WriteExport(ctx, destination, content, apocio.CreateDirectories)
func AdmitExport(ctx context.Context, path string) (ExportDestination, error) {
	if err := ctx.Err(); err != nil {
		return ExportDestination{}, err
	}
	if path == "" {
		return ExportDestination{}, nil
	}
	capability, err := CapabilityFrom(ctx)
	if err != nil {
		return ExportDestination{}, err
	}
	resolved, err := capability.ExportPath(path)
	if err != nil {
		return ExportDestination{}, err
	}
	return ExportDestination{path: resolved}, nil
}

// WriteExport publishes bytes or text to the admitted destination using the shared rooted
// filesystem owner. It observes cancellation before filesystem work and after
// completion. An OS write is not retractable or promised to be atomic.
func WriteExport[T ~[]byte | ~string](ctx context.Context, d ExportDestination, content T, directories ExportDirectoryPolicy) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if d.path == "" {
		return nil
	}
	switch directories {
	case ExistingDirectory:
	case CreateDirectories:
		if err := security.EnsureRootedParent(d.path, 0o755); err != nil {
			return fmt.Errorf("failed to create directory: %w", err)
		}
	default:
		return fmt.Errorf("unsupported export directory policy %d", directories)
	}
	if err := security.WriteRootedFile(d.path, []byte(content), 0o644); err != nil {
		return fmt.Errorf("failed to write file: %w", err)
	}
	return ctx.Err()
}
