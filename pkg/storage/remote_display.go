package storage

import (
	"context"
	"strings"

	"github.com/orneryd/nornicdb/pkg/observability"
)

// Keep credential context at the transport boundary, before callers format
// driver errors. This covers Bolt and every HTTP transaction operation alike.
type remoteDisplayTransport struct {
	remoteTransport
	credentials []string
}

func remoteDisplayCredentials(cfg RemoteEngineConfig) []string {
	return []string{cfg.Password, cfg.AuthToken, strings.TrimSpace(cfg.User) + ":" + strings.TrimSpace(cfg.Password), cfg.URI}
}

func (t *remoteDisplayTransport) query(ctx context.Context, statement string, params map[string]interface{}) ([][]interface{}, error) {
	rows, err := t.remoteTransport.query(ctx, statement, params)
	return rows, observability.RedactError(err, t.credentials...)
}

func (t *remoteDisplayTransport) queryWithColumns(ctx context.Context, statement string, params map[string]interface{}) ([]string, [][]interface{}, error) {
	columns, rows, err := t.remoteTransport.queryWithColumns(ctx, statement, params)
	return columns, rows, observability.RedactError(err, t.credentials...)
}

func (t *remoteDisplayTransport) queryBatch(ctx context.Context, statements []remoteStatement) error {
	return observability.RedactError(t.remoteTransport.queryBatch(ctx, statements), t.credentials...)
}

func (t *remoteDisplayTransport) beginCypherTx(ctx context.Context) (RemoteCypherTx, error) {
	tx, err := t.remoteTransport.beginCypherTx(ctx)
	if err != nil {
		return nil, observability.RedactError(err, t.credentials...)
	}
	return &remoteDisplayTx{RemoteCypherTx: tx, credentials: t.credentials}, nil
}

func (t *remoteDisplayTransport) close() error {
	return observability.RedactError(t.remoteTransport.close(), t.credentials...)
}

type remoteDisplayTx struct {
	RemoteCypherTx
	credentials []string
}

func (t *remoteDisplayTx) QueryCypher(ctx context.Context, statement string, params map[string]interface{}) ([]string, [][]interface{}, error) {
	columns, rows, err := t.RemoteCypherTx.QueryCypher(ctx, statement, params)
	return columns, rows, observability.RedactError(err, t.credentials...)
}

func (t *remoteDisplayTx) Commit(ctx context.Context) error {
	return observability.RedactError(t.RemoteCypherTx.Commit(ctx), t.credentials...)
}

func (t *remoteDisplayTx) Rollback(ctx context.Context) error {
	return observability.RedactError(t.RemoteCypherTx.Rollback(ctx), t.credentials...)
}

func (t *remoteDisplayTx) OutlivesConnection() bool {
	if independent, ok := t.RemoteCypherTx.(interface{ OutlivesConnection() bool }); ok {
		return independent.OutlivesConnection()
	}
	return false
}
