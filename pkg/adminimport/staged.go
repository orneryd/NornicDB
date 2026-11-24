package adminimport

import (
	"context"
	"errors"
	"os"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// StageFull builds a private store under the server data directory and
// returns its ID only once the graph is durable and the store is closed.
// Catalog publication is a separate server operation.
func StageFull(ctx context.Context, root string, opts Options) (id string, report Report, result error) {
	return StageFullWithKey(ctx, root, opts, nil)
}

// StageFullWithKey builds a staged store using its target installation's key.
func StageFullWithKey(ctx context.Context, root string, opts Options, key []byte) (id string, report Report, result error) {
	if err := ctx.Err(); err != nil {
		return "", Report{}, err
	}
	id, directory, err := storage.CreatePreparedDatabaseDirectory(root, opts.DatabaseName)
	if err != nil {
		return "", Report{}, err
	}
	defer func() {
		if result != nil {
			result = errors.Join(result, os.RemoveAll(directory))
		}
	}()
	engine, err := storage.OpenBadgerForOfflineImport(directory, opts.DatabaseName, key)
	if err != nil {
		return "", Report{}, err
	}
	opts.DataDir = directory
	// Search artifacts are owned by the serving database after publication.
	opts.BuildIndexes = false
	report, err = ImportFull(ctx, engine, opts)
	var nodes, edges int64
	if err == nil {
		graph := storage.NewNamespacedEngine(engine, opts.DatabaseName)
		var nodeErr, edgeErr error
		nodes, nodeErr = graph.NodeCount(ctx)
		edges, edgeErr = graph.EdgeCount(ctx)
		err = errors.Join(nodeErr, edgeErr)
	}
	if err == nil {
		err = engine.MarkCleanShutdown(ctx)
	}
	if err == nil {
		err = engine.Sync()
	}
	if err := errors.Join(err, engine.Close()); err != nil {
		return "", report, err
	}
	if err := ctx.Err(); err != nil {
		return "", report, err
	}
	if err := storage.SealPreparedDatabase(directory, storage.PreparedDatabase{
		Database: opts.DatabaseName, Nodes: nodes, Relationships: edges,
	}); err != nil {
		return "", report, err
	}
	return id, report, nil
}
