package graphql

import (
	"context"
	"net/http"

	gql "github.com/99designs/gqlgen/graphql"
	"github.com/99designs/gqlgen/graphql/handler/transport"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

// searchOriginAdmission extends gqlgen's operation envelope before its first
// dispatch. A websocket handshake can precede many independently admitted
// operations, so its HTTP origin must not become their permanent identity.
type searchOriginAdmission struct{}

const searchOriginExtension = "NornicSearchOrigin"

func (searchOriginAdmission) ExtensionName() string               { return searchOriginExtension }
func (searchOriginAdmission) Validate(gql.ExecutableSchema) error { return nil }

func (searchOriginAdmission) MutateOperationContext(ctx context.Context, operation *gql.OperationContext) *gqlerror.Error {
	_, origin := storage.AdmitVectorWork(ctx)
	if (transport.Websocket{}).Supports(&http.Request{Header: operation.Headers}) {
		// A handshake can precede many operations: admit each one itself.
		origin = storage.CaptureVectorWorkOrigin()
	}
	operation.Stats.SetExtension(searchOriginExtension, origin)
	return nil
}

func (searchOriginAdmission) InterceptOperation(ctx context.Context, next gql.OperationHandler) gql.ResponseHandler {
	origin := gql.GetOperationContext(ctx).Stats.GetExtension(searchOriginExtension).(uint64)
	return next(storage.WithVectorWorkOrigin(ctx, origin))
}
