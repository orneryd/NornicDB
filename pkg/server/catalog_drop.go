package server

import "context"

// catalogDropPurpose identifies which manager entry point a catalog drop uses.
// It also selects whether the regular-database per-database follow-up runs.
type catalogDropPurpose int

const (
	catalogDropRegular catalogDropPurpose = iota
	catalogDropComposite
)

// dropCatalogDatabase performs the shared catalog drop sequence: the manager
// call selected by purpose, the error short-circuit, the regular-only
// per-database follow-ups, the shared executor-wide invalidation and a nil
// return on success. Any error from the manager is returned unchanged and
// skips every follow-up step.
func (a *databaseManagerAdapter) dropCatalogDatabase(ctx context.Context, name string, purpose catalogDropPurpose) error {
	var err error
	if purpose == catalogDropComposite {
		err = a.Manager.DropCompositeDatabase(ctx, name)
	} else {
		err = a.Manager.DropDatabase(ctx, name)
	}
	if err != nil {
		return err
	}

	if purpose == catalogDropRegular {
		if a.db != nil {
			a.db.ResetInferenceService(name)
		}
		if a.server != nil {
			a.server.invalidateExecutor(name)
		}
	}

	if a.server != nil {
		a.server.invalidateAllExecutors()
	}
	return nil
}
