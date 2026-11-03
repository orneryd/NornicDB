package heimdall

// Regression coverage for the PostExecute hook context of the legacy streaming
// action route (handleStreamingResponse, POST /v1/chat/completions and
// POST /api/bifrost/chat/completions with stream=true on a provider without
// native tool calls). That route's PostExecuteContext literal must carry the
// request the handler is serving, as the four other PostExecuteContext literals
// in handler.go do.
//
// Without the request context, CallPostExecuteHooks acquires work through
// context.Background() and the plugin callback receives a context that carries
// no request principal and no authentication-disabled mark. A plugin that
// persists state from PostExecute, as plugins/heimdall does for its session
// memory, then reaches the server's request-authorizing router
// (pkg/server server_heimdall.go authorizeDatabase) with neither, and every
// one of its database calls is refused.
//
// The tests below drive the public handler routes and observe, inside the real
// PostExecute callback, the same request decisions the router reads from the
// context (auth.RequestEntitlementResolverFromContext,
// auth.RequestDatabaseScopeFromContext, auth.RequestAuthenticationDisabledFromContext).
//
// The PreExecuteContext literal of the same route (Phase 4) omitted Context
// outright while the two agentic-loop PreExecuteContext literals in handler.go
// carry it, so a registered plugin's PreExecute callback was handed no request
// either. The PreExecute tests hold that callback to the same rules through the
// same fixtures: the request the handler serves must reach it, and a denied or
// unattributed request must gain no authority from that propagation.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	// postExecuteProbeDatabase is the database the probe principal may select.
	postExecuteProbeDatabase = "nornic"
	// postExecuteProbePluginName registers its action as
	// heimdall_postexecute_probe_memory_write.
	postExecuteProbePluginName = "postexecute_probe"
	// postExecuteProbeAction is the action envelope the streamed SLM response
	// asks for; it is the plugin's own action.
	postExecuteProbeAction = "heimdall_" + postExecuteProbePluginName + "_memory_write"
	// postExecuteProbeMerge is the write plugins/heimdall performs in
	// PostExecute (captureSessionMemory -> persistSessionMemoryToGraph).
	postExecuteProbeMerge = `MERGE (m:HeimdallSessionMemory {plugin: $plugin, scope: $scope})
SET m.summary = $summary, m.updated_at = $updated_at
RETURN m.summary AS summary`
	// postExecuteProbeOpenAIRoute is the OpenAI-compatible alias of the Bifrost
	// chat route; both are registered on the same public handler.
	postExecuteProbeOpenAIRoute = "/v1/chat/completions"
	// postExecuteProbeBifrostRoute is the native Bifrost chat route.
	postExecuteProbeBifrostRoute = "/api/bifrost/chat/completions"
)

// errHookProbeNoContext records a callback that was given no context.
var errHookProbeNoContext = errors.New("hook callback context is nil")

// postExecuteProbeRouter records the queries a plugin makes from a lifecycle
// callback. It stands in for the server's router, which this package cannot
// import (pkg/server imports pkg/heimdall); the authorization decision the
// router enforces is read from the same request context values inside the
// plugin.
type postExecuteProbeRouter struct {
	mockDBRouter
	mu      sync.Mutex
	queries []string
}

func (r *postExecuteProbeRouter) Query(ctx context.Context, database, query string, params map[string]interface{}) ([]map[string]interface{}, error) {
	r.mu.Lock()
	r.queries = append(r.queries, query)
	r.mu.Unlock()
	return []map[string]interface{}{{"summary": "ok"}}, nil
}

func (r *postExecuteProbeRouter) recordedQueries() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.queries...)
}

// hookRequestObservation is the request a lifecycle hook callback was handed and
// what the plugin then did with it. The PreExecute and PostExecute callbacks
// record it through the same helper so neither hook can be corrected by
// loosening the other.
type hookRequestObservation struct {
	requestID              string
	action                 string
	callbackErr            error
	callbackHasDeadline    bool
	resolverPresent        bool
	scopePresent           bool
	resolvedDatabase       string
	allowsAccess           bool
	allowsWrite            bool
	authorized             bool
	authenticationDisabled bool
	persisted              bool
	persistErr             string
}

// observeHookRequest reads, from the context a lifecycle hook callback was
// handed, the request decisions the server's request-authorizing router takes,
// and derives the write decision a plugin makes from them.
func observeHookRequest(ctx context.Context) (hookRequestObservation, bool) {
	if ctx == nil {
		// A callback given no context carries no request at all; the router
		// refuses such a call, so the plugin does no authorized work.
		return hookRequestObservation{callbackErr: errHookProbeNoContext}, false
	}

	observation := hookRequestObservation{callbackErr: ctx.Err()}
	_, observation.callbackHasDeadline = ctx.Deadline()
	observation.resolverPresent = auth.RequestEntitlementResolverFromContext(ctx) != nil
	observation.scopePresent = auth.RequestDatabaseScopeFromContext(ctx) != nil
	observation.authenticationDisabled = auth.RequestAuthenticationDisabledFromContext(ctx)

	allows := auth.RequestEntitlementResolverFromContext(ctx)
	scope := auth.RequestDatabaseScopeFromContext(ctx)
	allowed := false
	if allows != nil && scope != nil {
		if dbName, selectable := scope.Resolve(postExecuteProbeDatabase); selectable {
			observation.resolvedDatabase = dbName
			observation.allowsAccess = allows(dbName, auth.Permission("access"))
			observation.allowsWrite = allows(dbName, auth.PermWrite)
			allowed = observation.allowsAccess && observation.allowsWrite
		}
	} else if allows == nil {
		// A request the server serves with authentication disabled carries no
		// principal; the router runs its selection as given.
		allowed = observation.authenticationDisabled
	}
	observation.authorized = allowed
	return observation, allowed
}

// streamPostExecuteContextPlugin persists coding-session memory in PostExecute,
// as plugins/heimdall does, and records the request each of its lifecycle
// callbacks was given.
type streamPostExecuteContextPlugin struct {
	programmed    chan hookRequestObservation
	preProgrammed chan hookRequestObservation

	mu       sync.Mutex
	database DatabaseRouter
	status   SubsystemStatus
	events   []SubsystemEvent
}

func newStreamPostExecuteContextPlugin(router *postExecuteProbeRouter) *streamPostExecuteContextPlugin {
	return &streamPostExecuteContextPlugin{
		programmed:    make(chan hookRequestObservation, 1),
		preProgrammed: make(chan hookRequestObservation, 1),
		database:      router,
		status:        StatusUninitialized,
	}
}

func (p *streamPostExecuteContextPlugin) Name() string    { return postExecuteProbePluginName }
func (p *streamPostExecuteContextPlugin) Version() string { return "1.0.0" }
func (p *streamPostExecuteContextPlugin) Type() string    { return PluginTypeHeimdall }
func (p *streamPostExecuteContextPlugin) Description() string {
	return "records the request its lifecycle callbacks are given"
}

func (p *streamPostExecuteContextPlugin) Initialize(ctx SubsystemContext) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.database = ctx.Database
	p.status = StatusReady
	p.events = append(p.events, SubsystemEvent{Time: time.Now(), Type: "info", Message: "initialized"})
	return nil
}

func (p *streamPostExecuteContextPlugin) Start() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.status = StatusRunning
	return nil
}

func (p *streamPostExecuteContextPlugin) Stop() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.status = StatusStopped
	return nil
}

func (p *streamPostExecuteContextPlugin) Shutdown() error { return nil }

func (p *streamPostExecuteContextPlugin) Status() SubsystemStatus {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.status
}

func (p *streamPostExecuteContextPlugin) Health() SubsystemHealth {
	return SubsystemHealth{Status: p.Status(), Healthy: true, LastCheck: time.Now()}
}

func (p *streamPostExecuteContextPlugin) Metrics() map[string]interface{} {
	return map[string]interface{}{}
}

func (p *streamPostExecuteContextPlugin) Config() map[string]interface{} {
	return map[string]interface{}{"summary_scope": "coding_session"}
}

func (p *streamPostExecuteContextPlugin) Configure(settings map[string]interface{}) error { return nil }

func (p *streamPostExecuteContextPlugin) ConfigSchema() map[string]interface{} {
	return map[string]interface{}{"type": "object"}
}

func (p *streamPostExecuteContextPlugin) Actions() map[string]ActionFunc {
	return map[string]ActionFunc{
		"memory_write": {
			Description: "Write the coding-session memory record",
			Category:    "coding",
			Handler: func(ctx ActionContext) (*ActionResult, error) {
				return &ActionResult{Success: true, Message: "action executed"}, nil
			},
		},
	}
}

func (p *streamPostExecuteContextPlugin) Summary() string { return "post-execute context probe" }

func (p *streamPostExecuteContextPlugin) RecentEvents(limit int) []SubsystemEvent {
	p.mu.Lock()
	defer p.mu.Unlock()
	if limit > len(p.events) {
		limit = len(p.events)
	}
	return append([]SubsystemEvent(nil), p.events[:limit]...)
}

// PreExecute is the sibling lifecycle callback under test: the streaming action
// route calls it with the parsed action before executing that action. It
// records the request decisions its callback was given and lets the action
// continue, so the same request reaches the route's PostExecute callback.
func (p *streamPostExecuteContextPlugin) PreExecute(ctx *PreExecuteContext, done func(PreExecuteResult)) {
	observation, _ := observeHookRequest(ctx.Context)
	observation.requestID = ctx.RequestID
	observation.action = ctx.Action
	p.preProgrammed <- observation
	done(PreExecuteResult{Continue: true})
}

// PostExecute is the lifecycle callback under test: it records the request
// decisions the server's request-authorizing router reads, and writes the
// session memory only when those decisions allow that write, as the router does.
func (p *streamPostExecuteContextPlugin) PostExecute(ctx *PostExecuteContext) {
	observation, allowed := observeHookRequest(ctx.Context)
	observation.requestID = ctx.RequestID
	observation.action = ctx.Action

	p.mu.Lock()
	database := p.database
	p.mu.Unlock()

	if allowed && database != nil {
		_, err := database.Query(ctx.Context, postExecuteProbeDatabase, postExecuteProbeMerge, map[string]interface{}{
			"plugin":     p.Name(),
			"scope":      "coding_session",
			"summary":    "recent coding session facts",
			"updated_at": time.Now().UTC().Format(time.RFC3339Nano),
		})
		if err != nil {
			observation.persistErr = err.Error()
		} else {
			observation.persisted = true
		}
	}

	p.programmed <- observation
}

// streamedActionProbe is the plugin and router a public streaming action route
// drove, so a test can read what each lifecycle callback was handed.
type streamedActionProbe struct {
	plugin *streamPostExecuteContextPlugin
	router *postExecuteProbeRouter
}

// runStreamedActionProbe drives a public chat route with stream=true through the
// real handler, so the SLM response is an action envelope and the legacy
// handleStreamingResponse route takes that action through its lifecycle hooks.
func runStreamedActionProbe(t *testing.T, path string, requestContext context.Context) *streamedActionProbe {
	t.Helper()

	ResetSubsystemManagerForTests()
	t.Cleanup(ResetSubsystemManagerForTests)

	router := &postExecuteProbeRouter{}
	plugin := newStreamPostExecuteContextPlugin(router)

	subsystems := GetSubsystemManager()
	subsystems.SetHostContext(SubsystemHost{
		Config:   DefaultConfig(),
		Database: router,
		Bifrost:  &NoOpBifrost{},
	})
	require.NoError(t, subsystems.RegisterPlugin(plugin, "", true))

	generator := NewMockGenerator("/test/model.gguf")
	generator.streamFunc = func(ctx context.Context, prompt string, params GenerateParams, callback func(token string) error) error {
		return callback(`{"action": "` + postExecuteProbeAction + `", "params": {}}`)
	}
	manager := newTestManager(generator)
	require.False(t, manager.SupportsTools(), "the legacy SSE route serves providers without native tool calls")

	handler := NewHandler(manager, manager.config, router, &mockMetricsReader{})
	require.NotNil(t, handler)

	body, err := json.Marshal(ChatRequest{
		Model:    "gpt-4o",
		Stream:   true,
		Messages: []ChatMessage{{Role: "user", Content: "remember the probe"}},
	})
	require.NoError(t, err)

	request := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request = request.WithContext(requestContext)

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	require.Equal(t, http.StatusOK, recorder.Code)

	return &streamedActionProbe{plugin: plugin, router: router}
}

// awaitHookObservation joins the callback under test without another route adapter.
func awaitHookObservation(t *testing.T, observations <-chan hookRequestObservation, phase string) hookRequestObservation {
	t.Helper()
	select {
	case observation := <-observations:
		return observation
	case <-time.After(10 * time.Second):
		t.Fatalf("%s hook did not run for the streamed action", phase)
		return hookRequestObservation{}
	}
}

// postExecuteProbeScope is the database scope the server derives for a
// principal (withBifrostRBAC): the databases and aliases it may select.
func postExecuteProbeScope() *auth.RequestDatabaseScope {
	return auth.NewRequestDatabaseScope(postExecuteProbeDatabase, map[string]string{
		postExecuteProbeDatabase: postExecuteProbeDatabase,
	})
}

// postExecuteProbePrincipal is the request principal the server derives for an
// editor of postExecuteProbeDatabase.
func postExecuteProbePrincipal() context.Context {
	return auth.WithRequestPrincipal(
		auth.WithRequestPrincipalRoles(context.Background(), []string{"editor"}),
		func(database string, permission auth.Permission) bool { return database == postExecuteProbeDatabase },
		postExecuteProbeScope(),
	)
}

// postExecuteProbeReadOnlyPrincipal is a request principal that may access but
// not write postExecuteProbeDatabase.
func postExecuteProbeReadOnlyPrincipal() context.Context {
	return auth.WithRequestPrincipal(
		context.Background(),
		func(database string, permission auth.Permission) bool {
			if database != postExecuteProbeDatabase {
				return false
			}
			return permission == auth.Permission("access") || permission == auth.PermRead
		},
		postExecuteProbeScope(),
	)
}

// assertHookCarriesRequestPrincipal asserts a lifecycle hook callback was handed
// the request the handler serves: the principal and scope the database router
// authorizes through, with the request's cancellation and deadline dropped so
// the callback is not tied to the request's lifetime.
func assertHookCarriesRequestPrincipal(t *testing.T, hook string, observation hookRequestObservation) {
	t.Helper()

	assert.Equal(t, postExecuteProbeAction, observation.action)
	assert.NotEmpty(t, observation.requestID)
	assert.True(t, observation.resolverPresent,
		"%s callback must carry the request principal the database router authorizes through", hook)
	assert.True(t, observation.scopePresent, "%s callback must carry the request's database scope", hook)
	assert.Equal(t, postExecuteProbeDatabase, observation.resolvedDatabase)
	assert.True(t, observation.allowsAccess)
	assert.True(t, observation.allowsWrite)
	assert.True(t, observation.authorized)
	assert.False(t, observation.authenticationDisabled)
	assert.NoError(t, observation.callbackErr, "%s callback runs before its request is cancelled", hook)
	assert.False(t, observation.callbackHasDeadline,
		"%s callback retains request values and drops request cancellation and deadline", hook)
}

// assertHookCarriesAuthenticationDisabledMark asserts a lifecycle hook callback
// was handed the server's authentication-disabled mark, which is the only thing
// that authorizes its work when the deployment serves requests without a
// principal.
func assertHookCarriesAuthenticationDisabledMark(t *testing.T, hook string, observation hookRequestObservation) {
	t.Helper()

	assert.True(t, observation.authenticationDisabled,
		"%s callback must carry the server's authentication-disabled mark", hook)
	assert.False(t, observation.resolverPresent, "%s callback runs without a principal", hook)
	assert.True(t, observation.authorized, "%s callback is authorized by the authentication-disabled mark", hook)
	assert.NoError(t, observation.callbackErr, "%s callback runs before its request is cancelled", hook)
}

// The streaming action route's PostExecute callback must receive the request
// the handler serves: the principal the router authorizes through, so the
// coding plugin's session memory is persisted instead of refused. Both public
// chat routes reach the same construction.
func TestStreamingPostExecuteHookCarriesRequestPrincipal(t *testing.T) {
	principal := postExecuteProbePrincipal()

	for _, path := range []string{postExecuteProbeOpenAIRoute, postExecuteProbeBifrostRoute} {
		t.Run(path, func(t *testing.T) {
			probe := runStreamedActionProbe(t, path, principal)
			observation := awaitHookObservation(t, probe.plugin.programmed, "PostExecute")
			router := probe.router

			assertHookCarriesRequestPrincipal(t, "PostExecute", observation)
			assert.Empty(t, observation.persistErr)
			assert.True(t, observation.persisted, "the coding plugin's session memory write must reach the database")

			queries := router.recordedQueries()
			require.Len(t, queries, 1)
			assert.Contains(t, queries[0], "HeimdallSessionMemory")
		})
	}
}

// A deployment that serves Bifrost with authentication disabled marks the
// request; that mark must reach the callback too, because it is the only thing
// that authorizes the call when there is no principal.
func TestStreamingPostExecuteHookCarriesAuthenticationDisabledMark(t *testing.T) {
	probe := runStreamedActionProbe(t, postExecuteProbeOpenAIRoute, auth.WithRequestAuthenticationDisabled(context.Background()))
	observation := awaitHookObservation(t, probe.plugin.programmed, "PostExecute")
	router := probe.router

	assertHookCarriesAuthenticationDisabledMark(t, "PostExecute", observation)
	assert.True(t, observation.persisted, "the session memory write must run on an authentication-disabled deployment")
	assert.Len(t, router.recordedQueries(), 1)
}

// Carrying the request must not loosen it: a principal without write on the
// database writes nothing, and a request with no principal and no
// authentication-disabled mark writes nothing either.
func TestStreamingPostExecuteHookKeepsDeniedRequestsOut(t *testing.T) {
	t.Run("principal without write", func(t *testing.T) {
		probe := runStreamedActionProbe(t, postExecuteProbeOpenAIRoute, postExecuteProbeReadOnlyPrincipal())
		observation := awaitHookObservation(t, probe.plugin.programmed, "PostExecute")
		router := probe.router

		assert.True(t, observation.resolverPresent)
		assert.True(t, observation.allowsAccess)
		assert.False(t, observation.allowsWrite)
		assert.False(t, observation.persisted, "a principal without write must not persist session memory")
		assert.Empty(t, router.recordedQueries())
	})

	t.Run("no principal and no authentication-disabled mark", func(t *testing.T) {
		probe := runStreamedActionProbe(t, postExecuteProbeOpenAIRoute, context.Background())
		observation := awaitHookObservation(t, probe.plugin.programmed, "PostExecute")
		router := probe.router

		assert.False(t, observation.resolverPresent)
		assert.False(t, observation.authenticationDisabled)
		assert.False(t, observation.persisted, "an unattributed request must not persist session memory")
		assert.Empty(t, router.recordedQueries())
	})
}

// The streaming action route's sibling PreExecute callback must receive the same
// request, which is the request its hook view authorizes any request-scoped work
// through. Both public chat routes reach the same construction.
func TestStreamingPreExecuteHookCarriesRequestPrincipal(t *testing.T) {
	principal, cancel := context.WithTimeout(postExecuteProbePrincipal(), 30*time.Second)
	defer cancel()

	for _, path := range []string{postExecuteProbeOpenAIRoute, postExecuteProbeBifrostRoute} {
		t.Run(path, func(t *testing.T) {
			probe := runStreamedActionProbe(t, path, principal)
			observation := awaitHookObservation(t, probe.plugin.preProgrammed, "PreExecute")

			assertHookCarriesRequestPrincipal(t, "PreExecute", observation)
		})
	}
}

// A deployment that serves Bifrost with authentication disabled marks the
// request; the PreExecute callback must be handed that mark as well.
func TestStreamingPreExecuteHookCarriesAuthenticationDisabledMark(t *testing.T) {
	probe := runStreamedActionProbe(t, postExecuteProbeOpenAIRoute, auth.WithRequestAuthenticationDisabled(context.Background()))
	observation := awaitHookObservation(t, probe.plugin.preProgrammed, "PreExecute")

	assertHookCarriesAuthenticationDisabledMark(t, "PreExecute", observation)
}

// Propagating the request must not authorize anyone: a principal without write
// and a request with no principal and no authentication-disabled mark stay
// refused inside the PreExecute callback.
func TestStreamingPreExecuteHookKeepsDeniedRequestsOut(t *testing.T) {
	t.Run("principal without write", func(t *testing.T) {
		probe := runStreamedActionProbe(t, postExecuteProbeOpenAIRoute, postExecuteProbeReadOnlyPrincipal())
		observation := awaitHookObservation(t, probe.plugin.preProgrammed, "PreExecute")

		assert.True(t, observation.resolverPresent)
		assert.True(t, observation.allowsAccess)
		assert.False(t, observation.allowsWrite)
		assert.False(t, observation.authorized,
			"a principal without write must not authorize request-scoped work in PreExecute")
	})

	t.Run("no principal and no authentication-disabled mark", func(t *testing.T) {
		probe := runStreamedActionProbe(t, postExecuteProbeOpenAIRoute, context.Background())
		observation := awaitHookObservation(t, probe.plugin.preProgrammed, "PreExecute")

		assert.False(t, observation.resolverPresent)
		assert.False(t, observation.scopePresent)
		assert.False(t, observation.authenticationDisabled)
		assert.Empty(t, observation.resolvedDatabase)
		assert.False(t, observation.authorized,
			"an unattributed request must not authorize request-scoped work in PreExecute")
	})
}
