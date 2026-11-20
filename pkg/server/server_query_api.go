package server

import (
	"context"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/orneryd/nornicdb/internal/httpresponse"
	"github.com/orneryd/nornicdb/internal/jsontext"
	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/convert"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/txsession"
)

type queryAPIResultContextKey struct{}

type queryAPIRequest struct {
	Statement           string             `json:"statement"`
	Parameters          jsontext.NumberMap `json:"parameters,omitempty"`
	ImpersonatedUser    *string            `json:"impersonatedUser,omitempty"`
	AccessMode          string             `json:"accessMode,omitempty"`
	IncludeCounters     bool               `json:"includeCounters,omitempty"`
	TransactionMetadata jsontext.NumberMap `json:"txMetadata,omitempty"`
}

type queryAPIResult struct {
	Fields []string `json:"fields"`
	Values [][]any  `json:"values"`
}

type queryAPITransactionInfo struct {
	ID      string `json:"id"`
	Expires string `json:"expires"`
}

type queryAPIResponse struct {
	Transaction   *queryAPITransactionInfo `json:"transaction,omitempty"`
	Data          *queryAPIResult          `json:"data,omitempty"`
	Errors        []QueryError             `json:"errors,omitempty"`
	Bookmarks     []string                 `json:"bookmarks,omitempty"`
	Notifications []ServerNotification     `json:"notifications,omitempty"`
	Counters      *QueryStats              `json:"counters,omitempty"`
	QueryType     string                   `json:"queryType,omitempty"`
}

func (s *Server) handleQueryAPI(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.writeNeo4jPostRequired(w, r, "Neo.ClientError.Request.Invalid")
		return
	}
	var request queryAPIRequest
	decoder := convert.CypherInputJSON.NewDecoder(http.MaxBytesReader(w, r.Body, s.config.MaxRequestSize))
	if err := decoder.Decode(&request); err != nil || decoder.Decode(new(any)) != io.EOF {
		if s.writeNeo4jUnsupportedNumber(w, err) {
			return
		}
		s.writeNeo4jInvalidRequestBody(w, r, "Neo.ClientError.Request.InvalidFormat")
		return
	}
	ctx := r.Context()
	switch strings.ToLower(request.AccessMode) {
	case "read":
		ctx = cypher.WithReadOnlyStatements(ctx)
	case "", "write":
	default:
		s.writeNeo4jInvalidRequestBody(w, r, "Neo.ClientError.Request.InvalidFormat")
		return
	}
	claims := getClaims(r)
	authToken := r.Header.Get("Authorization")
	if request.ImpersonatedUser != nil {
		var err error
		ctx, claims, err = s.queryAPIImpersonatedContext(ctx, claims, *request.ImpersonatedUser)
		if err != nil {
			if actor := getClaims(r); actor != nil {
				s.logAudit(r, actor.Sub, "impersonation", false, "target="+*request.ImpersonatedUser)
			}
			httpresponse.WritePropertyJSON(w, http.StatusAccepted, queryAPIResponse{
				Errors: []QueryError{{Code: "Neo.ClientError.Security.Forbidden", Message: err.Error()}},
			})
			return
		}
		s.logAudit(r, getClaims(r).Sub, "impersonation", true, "target="+claims.Username+" target_id="+claims.Sub)
		// The authenticated actor's credential must not enter an execution
		// adapter as though it were the selected account's credential.
		authToken = ""
	}
	ctx = context.WithValue(ctx, queryAPIResultContextKey{}, true)
	ctx = cypher.WithTransactionMetadata(ctx, request.TransactionMetadata)
	ctx = s.withRequestIdentity(ctx, r, claims)
	transaction := TransactionResponse{}
	s.runRequestStatements(ctx, authToken, claims, r.PathValue("database"), []StatementRequest{{
		Statement: request.Statement, Parameters: request.Parameters, IncludeStats: request.IncludeCounters,
	}}, s.autoCommitStatementRunner(authToken), func(message localization.Message) string {
		return s.localizedText(w, r, message)
	}, &transaction)
	s.writeQueryAPIResponse(w, transaction, nil)
}

func (s *Server) writeQueryAPIResponse(w http.ResponseWriter, transaction TransactionResponse, explicit *queryAPITransactionInfo) {
	response := queryAPIResponse{Errors: transaction.Errors, Notifications: transaction.Notifications, Transaction: explicit}
	if len(transaction.Errors) == 0 {
		if explicit == nil {
			response.Bookmarks = []string{s.generateBookmark()}
		}
		if len(transaction.Results) != 0 {
			result := transaction.Results[0]
			response.Data = &queryAPIResult{Fields: result.Columns, Values: make([][]any, len(result.Data))}
			for i, row := range result.Data {
				response.Data.Values[i] = row.Row
			}
			response.Counters, response.QueryType = result.Stats, result.QueryType
		}
	}
	httpresponse.WritePropertyJSON(w, http.StatusAccepted, response)
}

func (s *Server) queryAPIImpersonatedContext(ctx context.Context, claims *auth.JWTClaims, username string) (context.Context, *auth.JWTClaims, error) {
	actor := auth.FineGrainedRequestFromContext(ctx)
	if actor == nil || actor.Runtime == nil || claims == nil || username == "" {
		return ctx, nil, auth.ErrFineGrainedForbidden
	}
	request, target, err := actor.Runtime.Impersonate(ctx, actor, username)
	if err != nil {
		return ctx, nil, auth.ErrFineGrainedForbidden
	}
	roles := make([]string, len(target.Roles))
	for i, role := range target.Roles {
		roles[i] = string(role)
	}
	selected := *claims
	selected.Username, selected.Sub, selected.Roles = target.Username, target.ID, roles
	ctx = auth.WithRequestIdentity(ctx, auth.RequestIdentity{ID: auth.PrincipalID(&selected), Username: selected.Username})
	ctx = context.WithValue(ctx, contextKeyClaims, &selected)
	return auth.WithFineGrainedRequest(ctx, request), &selected, nil
}

func (s *Server) handleQueryAPITransaction(w http.ResponseWriter, r *http.Request) {
	txID := r.PathValue("transaction")
	opening := txID == ""
	committing := strings.HasSuffix(r.Pattern, "/commit")
	rollingBack := !committing && !opening && r.Method == http.MethodDelete
	if r.Method != http.MethodPost && !rollingBack {
		s.writeNeo4jPostRequired(w, r, "Neo.ClientError.Request.Invalid")
		return
	}
	var request queryAPIRequest
	if !rollingBack {
		decoder := convert.CypherInputJSON.NewDecoder(http.MaxBytesReader(w, r.Body, s.config.MaxRequestSize))
		if err := decoder.Decode(&request); err != io.EOF && (err != nil || decoder.Decode(new(any)) != io.EOF) {
			if s.writeNeo4jUnsupportedNumber(w, err) {
				return
			}
			s.writeNeo4jInvalidRequestBody(w, r, "Neo.ClientError.Request.InvalidFormat")
			return
		}
	}
	ctx := r.Context()
	database := r.PathValue("database")
	actor := getClaims(r)
	owner := "query-v2:" + transactionOwnerKey(r, actor)
	var lease *txsession.Lease
	var binding txsession.ClientPolicyBinding
	claims := actor
	authToken := r.Header.Get("Authorization")
	localize := func(message localization.Message) string { return s.localizedText(w, r, message) }
	forbidden := QueryError{Code: "Neo.ClientError.Security.Forbidden", Message: auth.ErrFineGrainedForbidden.Error()}

	if opening {
		switch strings.ToLower(request.AccessMode) {
		case "read":
			binding.ReadOnly = true
		case "", "write":
		default:
			s.writeNeo4jInvalidRequestBody(w, r, "Neo.ClientError.Request.InvalidFormat")
			return
		}
		if request.ImpersonatedUser != nil {
			var err error
			ctx, claims, err = s.queryAPIImpersonatedContext(ctx, actor, *request.ImpersonatedUser)
			if err != nil {
				s.writeQueryAPIResponse(w, TransactionResponse{Errors: []QueryError{forbidden}}, nil)
				return
			}
			binding.Username, binding.Principal = claims.Username, claims.Sub
			authToken = ""
		}
		if !s.principalAccessForRequest(ctx, claims).Allows(database, "access") {
			s.writeQueryAPIResponse(w, TransactionResponse{Errors: []QueryError{forbidden}}, nil)
			return
		}
		if principal := auth.FineGrainedRequestFromContext(ctx); principal != nil {
			binding.Fingerprint = principal.Fingerprint
		}
		ctx = cypher.WithTransactionMetadata(ctx, request.TransactionMetadata)
		ctx = s.withRequestIdentity(ctx, r, claims)
		var err error
		lease, err = s.txSessions.OpenRequestForOwner(ctx, database, owner)
		if err != nil {
			s.writeQueryAPIResponse(w, TransactionResponse{Errors: []QueryError{s.transactionResponseError(ctx, err)}}, nil)
			return
		}
		lease.Session.ClientPolicy = binding
		if binding.Username != "" {
			s.logAudit(r, actor.Sub, "impersonation", true, "target="+binding.Username+" target_id="+binding.Principal)
		}
	} else {
		var exists bool
		lease, exists = s.txSessions.AcquireForOwner(txID, owner)
		if !exists || lease == nil {
			s.writeQueryAPIResponse(w, TransactionResponse{Errors: []QueryError{{
				Code: "Neo.ClientError.Request.Invalid", Message: "Transaction was not found",
			}}}, nil)
			return
		}
		if lease.Session.Database != database {
			lease.Release()
			s.writeQueryAPIResponse(w, TransactionResponse{Errors: []QueryError{{
				Code: "Neo.ClientError.Request.Invalid", Message: "Transaction was not found",
			}}}, nil)
			return
		}
	}
	defer lease.Release()

	response := TransactionResponse{}
	if rollingBack {
		s.rollbackRequestTransaction(ctx, lease, &response)
		if len(response.Errors) == 0 {
			w.WriteHeader(http.StatusOK)
		} else {
			s.writeQueryAPIResponse(w, response, nil)
		}
		return
	}
	binding = lease.Session.ClientPolicy
	if !opening {
		if request.ImpersonatedUser != nil && *request.ImpersonatedUser != binding.Username {
			s.writeQueryAPIResponse(w, TransactionResponse{Errors: []QueryError{forbidden}}, nil)
			return
		}
		if binding.Username != "" {
			var err error
			ctx, claims, err = s.queryAPIImpersonatedContext(ctx, actor, binding.Username)
			if err != nil || claims.Sub != binding.Principal {
				response.Errors = []QueryError{forbidden}
				s.rollbackRequestTransaction(ctx, lease, &response)
				s.writeQueryAPIResponse(w, response, nil)
				return
			}
			authToken = ""
		}
		current := auth.FineGrainedRequestFromContext(ctx)
		if binding.Fingerprint != "" && (current == nil || current.Fingerprint != binding.Fingerprint) ||
			!s.principalAccessForRequest(ctx, claims).Allows(database, "access") {
			response.Errors = []QueryError{forbidden}
			s.rollbackRequestTransaction(ctx, lease, &response)
			s.writeQueryAPIResponse(w, response, nil)
			return
		}
		ctx = s.withRequestIdentity(ctx, r, claims)
	}
	if binding.ReadOnly {
		ctx = cypher.WithReadOnlyStatements(ctx)
	}
	ctx = context.WithValue(ctx, queryAPIResultContextKey{}, true)
	if strings.TrimSpace(request.Statement) != "" && s.runRequestStatements(ctx, authToken, claims, database, []StatementRequest{{
		Statement: request.Statement, Parameters: request.Parameters, IncludeStats: request.IncludeCounters,
	}}, s.sessionStatementRunner(lease), localize, &response) {
		s.rollbackRequestTransaction(ctx, lease, &response)
		s.writeQueryAPIResponse(w, response, nil)
		return
	}
	if committing {
		s.commitRequestTransaction(ctx, lease, &response)
		s.writeQueryAPIResponse(w, response, nil)
		return
	}
	s.writeQueryAPIResponse(w, response, &queryAPITransactionInfo{
		ID: lease.Session.ID, Expires: lease.Session.ExpiresAt().UTC().Format(time.RFC3339),
	})
}
