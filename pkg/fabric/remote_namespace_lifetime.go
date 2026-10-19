package fabric

import (
	"context"
	"fmt"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// CaptureLogicalBegin captures the remote catalog lifetime without opening a
// participant. A lazy participant compares it inside its physical transaction:
// recreation before BEGIN changes the ID; DROP after BEGIN is rejected by the
// remote transaction's storage lifetime. The protocol remains ordinary Cypher.
func (r *RemoteFragmentExecutor) CaptureLogicalBegin(ctx context.Context, locations []*LocationRemote, authToken string) error {
	for _, location := range locations {
		engine, err := r.getOrCreateEngine(location, authToken)
		if err != nil {
			return err
		}
		r.mu.RLock()
		_, captured := r.logicalDatabases[engine]
		r.mu.RUnlock()
		if captured {
			continue
		}
		columns, rows, err := engine.QueryCypher(ctx, "CALL db.info()", nil)
		if err != nil {
			return fmt.Errorf("remote logical BEGIN on %q: %w", location.DBName, err)
		}
		id, err := remoteNamespaceIdentity(columns, rows)
		if err != nil {
			return err
		}
		r.mu.Lock()
		if r.logicalDatabases == nil {
			r.logicalDatabases = make(map[*storage.RemoteEngine]string)
		}
		r.logicalDatabases[engine] = id
		r.mu.Unlock()
	}
	return nil
}

func remoteNamespaceIdentity(columns []string, rows [][]interface{}) (string, error) {
	for index, column := range columns {
		if column == "id" && len(rows) == 1 && index < len(rows[0]) {
			// Embedded legacy servers without a catalog cannot administratively
			// DROP/recreate a database and return a null ID. Their physical BEGIN
			// retains its existing transaction contract.
			if rows[0][index] == nil {
				return "", nil
			}
			if id, ok := rows[0][index].(string); ok && id != "" {
				return id, nil
			}
		}
	}
	return "", fmt.Errorf("remote logical BEGIN requires db.info database identity")
}

// Called with the remote participant registry locked by its admission owner.
func (r *RemoteFragmentExecutor) validateLogicalParticipant(ctx context.Context, engine *storage.RemoteEngine, tx storage.RemoteCypherTx) error {
	expected := r.logicalDatabases[engine]
	if expected == "" {
		return nil
	}
	columns, rows, err := tx.QueryCypher(ctx, "CALL db.info()", nil)
	if err != nil {
		return err
	}
	actual, err := remoteNamespaceIdentity(columns, rows)
	if err != nil {
		return err
	}
	if actual != expected {
		return fmt.Errorf("%w: remote database was recreated after logical BEGIN", storage.ErrConflict)
	}
	return nil
}
