# Database Event Triggers and Automatic Remediation

Heimdall plugins can react to **database events** (node/relationship changes, query execution, failures) and use the **Heimdall invoker** to run the model on accumulated events and take **remediation actions** inside the database (e.g. run Cypher to fix or annotate data). This guide describes how to set that up.

## Overview

1. **Events** – Plugins implement `OnDatabaseEvent`. They receive a stream of events (e.g. `query.failed`, `node.created`) and can maintain **accumulated state** (counters, recent events, aggregates).
2. **Trigger** – When your logic decides “enough data to act” (e.g. 5 failed queries in 5 minutes), you call the **Heimdall invoker** to run the model:
   - **InvokeActionAsync** – Run a registered action (e.g. `heimdall.anomaly.detect`) with params.
   - **SendPromptAsync** – Send a natural-language prompt (e.g. “Multiple query failures detected. Suggest remediation.”).
3. **Inference** – The model (and any actions it triggers) can analyze the situation. Results can be sent to Bifrost (notifications) or consumed by your plugin if you use the synchronous APIs.
4. **Remediation** – Your plugin (or an action) can run **Cypher** or **MCP-style operations** via the same database context to apply fixes: create nodes, set properties, create relationships, or delete/archive data.

So the flow is: **database events → plugin accumulates → trigger model → model (or action) suggests/decides → plugin or action runs Cypher to remediate**.

## Database events

Plugins that implement `OnDatabaseEvent(event *heimdall.DatabaseEvent)` receive events from the database layer. Event types include (depending on implementation):

- **Node / relationship**: `node.created`, `node.updated`, `node.deleted`, `node.read`, `relationship.created`, `relationship.updated`, `relationship.deleted`
- **Query**: `query.executed`, `query.failed`
- **Transaction**: `transaction.commit`, `transaction.rollback`
- **System**: `database.started`, `database.shutdown`, `backup.started`, `backup.completed`, etc.

Events are delivered through per-plugin bounded queues. Under load, events may be dropped to preserve system health.

## Accumulating state and triggering the model

Keep **in-memory state** in your plugin (counters, time windows, recent event IDs). When a condition is met, call the invoker.

### Example: trigger on repeated query failures

```go
type SecurityPlugin struct {
    ctx           heimdall.SubsystemContext
    failedQueries int64
    lastReset     time.Time
    window        time.Duration
}

func (p *SecurityPlugin) OnDatabaseEvent(event *heimdall.DatabaseEvent) {
    if event.Type != heimdall.EventQueryFailed {
        return
    }
    if time.Since(p.lastReset) > p.window {
        p.failedQueries = 0
        p.lastReset = time.Now()
    }
    p.failedQueries++

    if p.failedQueries >= 5 && p.ctx.Heimdall != nil {
        // Trigger model with context
        p.ctx.Heimdall.InvokeActionAsync(context.Background(), "heimdall.anomaly.detect", map[string]interface{}{
            "trigger": "autonomous",
            "reason":  "query_failures",
            "count":   p.failedQueries,
        })
        // Or natural language
        p.ctx.Heimdall.SendPromptAsync(context.Background(),
            "Multiple query failures detected in the last window. Analyze and suggest remediation.")
        p.failedQueries = 0
    }
}
```

- **InvokeActionAsync** – Fire-and-forget; results can be sent to Bifrost or handled inside the action.
- **SendPromptAsync** – Same idea; the model gets a text prompt and can respond (e.g. via Bifrost) or trigger further actions.
- Both run with the values of the context you pass, but not its cancellation, so work started from a request keeps that request's user. Database calls an action makes (`ctx.Database.Query`, `Discover`, `Stats`) are checked against the user the context carries: on a server with authentication enabled, a call made with `context.Background()`, as in the event hook above, carries no user, and those database calls are refused.

Use **InvokeAction** / **SendPrompt** (synchronous) if you need the result in the plugin to decide remediation steps.

## Taking actions in the database (remediation)

Remediation = running Cypher (or MCP tools) to change or annotate data. Two main patterns:

### 1. Remediation inside a plugin action

Your plugin can register an action (e.g. `heimdall.myplugin.remediate`) that:

- Receives params (e.g. `event_summary`, `suggested_fix`).
- Uses `ActionContext.Database` to run Cypher against the database (query or mutate).
- Returns a result and optionally sends a Bifrost notification.

```go
func (p *MyPlugin) Actions() map[string]heimdall.ActionFunc {
    return map[string]heimdall.ActionFunc{
        "remediate": p.actionRemediate,
    }
}

func (p *MyPlugin) actionRemediate(ctx heimdall.ActionContext) (*heimdall.ActionResult, error) {
    summary, _ := ctx.Params["event_summary"].(string)
    // Run Cypher to create an audit node or apply a fix
    result, err := ctx.Database.Query(ctx.Context, "", `
        CREATE (a:AuditEvent {summary: $summary, at: datetime()})
        RETURN elementId(a) AS id
    `, map[string]interface{}{"summary": summary})
    if err != nil {
        return nil, err
    }
    return &heimdall.ActionResult{
        Success: true,
        Message: "Remediation audit node created",
    }, nil
}
```

The **model** can be triggered by the event path above; then either the model calls this action (if exposed as a tool), or your plugin calls it directly after `SendPrompt`/`InvokeAction` returns a recommendation.

### 2. Event handler runs Cypher directly

Your plugin can run Cypher from inside `OnDatabaseEvent` (or from a goroutine started there) using the same `SubsystemContext` database router:

```go
func (p *MyPlugin) OnDatabaseEvent(event *heimdall.DatabaseEvent) {
    if event.Type != heimdall.EventNodeDeleted {
        return
    }
    // Optional: accumulate and only act every N events
    p.deletedCount++
    if p.deletedCount < 10 {
        return
    }
    p.deletedCount = 0

    // Run remediation Cypher (e.g. create a summary node or alert)
    _, err := p.ctx.Database.Query(context.Background(), "", `
        CREATE (s:DeletionSummary {count: 10, at: datetime()})
        RETURN s
    `, nil)
    if err != nil {
        log.Printf("Remediation query failed: %v", err)
    }
}
```

Use the database router from `SubsystemContext` so you target the correct logical database and respect multi-db configuration.

## End-to-end pattern: events → inference → remediation

1. **Plugin** – Implements `OnDatabaseEvent`, accumulates events (e.g. failed queries, deletion rate).
2. **Trigger** – When threshold or window condition is met, call `InvokeActionAsync(ctx, "heimdall.myplugin.analyze", params)` or `SendPromptAsync(ctx, "Summarize these events and suggest fixes")`.
3. **Action / model** – The action or the model’s response can:
   - Produce a recommendation (e.g. “Create an index on X” or “Annotate node Y”).
   - Call another action that runs Cypher to apply the fix (create node, set property, create relationship).
4. **Remediation** – Either:
   - The **action** runs Cypher via `ctx.Database.Query` to create audit nodes, set flags, or mutate graph data; or
   - The **plugin** (after synchronous `InvokeAction`/`SendPrompt`) parses the result and runs Cypher itself.

For **automatic** remediation without a human in the loop, keep logic in plugin actions or in the plugin’s event path and use Cypher for all database writes; use the model for **analysis and suggestion** and treat the action as the executor of approved remediation steps (e.g. “if model suggests ‘create index’, action calls a procedure or creates a marker node”).

## Best practices

- **Throttle** – Don’t trigger the model on every single event; use windows and thresholds (e.g. “every 5 failures in 5 minutes”).
- **Scoping** – Use the database name from context when running Cypher so multi-database setups only touch the intended database.
- **Safety** – Prefer creating audit/summary nodes or “suggested remediation” nodes rather than blindly deleting or overwriting data; add a human or approval step for destructive actions if needed.
- **Async vs sync** – Use `InvokeActionAsync` / `SendPromptAsync` so the event path doesn’t block; use sync APIs when you need the result to run remediation in the same goroutine.

## Related documentation

- [Heimdall Plugins](heimdall-plugins.md) – Implementing plugins, actions, and `OnDatabaseEvent`.
- [Heimdall agentic loop](heimdall-agentic-loop.md) – How the loop works and how tools/actions are executed.
- [Heimdall AI Assistant](heimdall-ai-assistant.md) – Configuration and Bifrost.
