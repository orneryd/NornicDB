package bolt

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// Research only. rfNewManager: 2dbc67e96 signature (context first).
func rfNewManager(base storage.Engine) (*multidb.DatabaseManager, error) {
	return multidb.NewDatabaseManager(context.Background(), base, nil)
}

func rfRecover(e storage.Engine, walDir, snap string) (storage.ReplayResult, error) {
	r, _, err := storage.RecoverIntoEngine(context.Background(), e, walDir, snap)
	return r, err
}
