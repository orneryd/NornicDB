# Plugin host metadata API migration

v1.4.4 separates persistent plugin host metadata from each registration's
initialization context. Rebuild Go embedding applications and plugin binaries.
HTTP, Bolt, storage keys, payloads and storage versions are unchanged.

| Previous call or literal | Current contract |
| --- | --- |
| `manager.SetContext(SubsystemContext{...})` | `manager.SetContext(SubsystemHost{...})` |
| `manager.SetHostContext(SubsystemContext{...})` | `manager.SetHostContext(SubsystemHost{...})` |
| `nornicdb.LoadPluginsFromDir(dir, hostContext)` | `nornicdb.LoadPluginsFromDir(ctx, dir, hostMetadata)` |
| `heimdall.LoadHeimdallPluginsFromDir(dir, subsystemContext)` | `heimdall.LoadHeimdallPluginsFromDir(dir, hostMetadata)` |
| `SubsystemContext{Config: config, Context: ctx}` | `SubsystemContext{Context: ctx, SubsystemHost: SubsystemHost{Config: config}}` |

`hostMetadata` is a `*heimdall.SubsystemHost` for the NornicDB loader, and a
`heimdall.SubsystemHost` value for the Heimdall loader. The NornicDB loader's
explicit `ctx` controls registration admission. The standalone Heimdall loader
retains its existing startup-owned registration behavior.

```go
host := heimdall.SubsystemHost{Config: config, Lifetime: owner}
manager.SetContext(host)
err := nornicdb.LoadPluginsFromDir(ctx, pluginDir, &host)
```

Plugins still implement `Initialize(heimdall.SubsystemContext) error`. Its host
fields remain available through field promotion (`received.Config`, for
example), but keyed composite literals must name the embedded `SubsystemHost`.
Its `Context` lasts only through that registration's initialization: caller
cancellation, host stop, and registration completion cancel it. Later actions
use their callback context. Host setters retain metadata without retaining a
request context; admitted initialization still receives caller values and
cancellation. Ordinary `SetContext` updates metadata without reopening closed
admission; `SetHostContext` retains the existing new-host completion barrier.
