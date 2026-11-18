# Code graph workspace

The Graphify demo uses three working areas: source and scope, the 3D project map,
and a docked symbol inspector. Import retains the existing Graphify `graph.json`
node/link format, including `edges` as an alternative to `links`, incremental
updates, and explicit consent before syncing an existing database.

## Intent and interaction map

| Component or interaction | Developer intent | Placement and behavior | NN/g principle |
| --- | --- | --- | --- |
| Shared page header and navigation | Locate the feature and check connection | Same header and status popover as other pages | Consistency; visibility of system status |
| Source database and Load tree | Choose the project to explore | Left rail; initial loading is explicit | User control; match the user's task |
| Import artifact | Bring another project into the demo | Inline workspace panel, labelled file input and database target, progress and errors | Progressive disclosure; error prevention |
| Existing-database sync consent | Intentionally update an existing graph | Required checkbox describes creation, updates and stale-record deletion | Error prevention; informed control |
| Starting point and detection reason | Find where execution begins | Visible suggestions with source paths; manual selection overrides inference | Recognition rather than recall; explain system behavior |
| Depth and test visibility | Adjust how much code to inspect | Source rail; values reload the current neighborhood | User control; visible state |
| Community grouping and color | Understand packages and clusters | Adjacent view controls; textual legend accompanies colors | Recognition; consistency; information hierarchy |
| Advanced inclusion/exclusion lists | Narrow a difficult graph | Collapsed disclosure with labelled chips and property inputs | Progressive disclosure; reduce initial complexity |
| Filter apply/reset | Reapply or undo restrictions | Within the filter disclosure; existing automatic updates retained | User control and freedom |
| Symbol search | Locate a known function or file | Above the map; results replace guidance, preserve the query and show failures | Recognition; visibility; recovery guidance |
| Browse symbols | Select without precision pointing or WebGL interaction | Keyboard-accessible bounded list below the map; selected row is indicated | Accessibility; alternative interaction paths |
| 3D rotate, pan and zoom | Understand relationships spatially | Unobstructed map; discoverable Graph controls help | Help and documentation; user control |
| Return to starting point | Recover orientation | Graph controls; frames the current root without changing filters | User control and freedom |
| Symbol selection | Inspect an entity without losing context | Docked right inspector; source file, kind and degree remain visible | Context preservation; recognition |
| Source code and connection disclosures | Examine implementation or follow a call | Separately collapsible inspector sections | Progressive disclosure |
| Connection links | Traverse callers and callees | Direction and relationship labels next to destination symbols | Match the mental model; recognition |
| Start graph from this symbol | Explore a different call neighborhood | Inspector action attached to the selected symbol | Contextual actions; user control |
| Find similar symbols | Discover related implementations | Secondary inspector action; reports results and loading | Task hierarchy; visibility of system status |
| Float, drag, resize and dock inspector | Arrange a large source inspection | Optional floating mode; dock is the default | Flexibility and efficiency |
| Disconnected components | Inspect graph fragments after exclusions | Separate component drawer with selected state and Show all | Visibility; user control |
| Status, progress and error messages | Know what happened and recover | Persistent status area; actual error text remains visible | Visibility; error recognition and recovery |

The placement choices apply [NN/g's complex-application heuristics](https://www.nngroup.com/articles/usability-heuristics-complex-applications/),
[progressive disclosure](https://www.nngroup.com/articles/progressive-disclosure/), and
[recognition rather than recall](https://www.nngroup.com/articles/recognition-and-recall/).
These are design decisions informed by that guidance, not claims of NN/g certification.

## Entry detection

1. Respect explicitly marked `Main` symbols, excluding test and dependency paths.
2. Look for callable symbols using either `symbol_kind` or `node_kind` metadata.
   Rank executable names (`main`, `Main`, `__main__`, `wmain`, `WinMain`), executable
   files, application bootstraps and request handlers across language conventions.
3. If no recognizable entry is found, sample connectivity for up to 128 source callables through the existing neighborhood API (depth 1, 2,000-node limit). This is not a global degree leaderboard.
4. For graphs without callable metadata, retain the deterministic first-node fallback.

Explicit and recognizable entries avoid the whole-graph connectivity query. Named
candidates are fetched independently of degree, so a small executable entry is not
displaced by a popular utility function. Suggestions remain heuristics: libraries,
frameworks and monorepos can have multiple valid starts. Search and the inspector
allow the developer to choose any symbol as the root.

## URL state

The existing `UrlStateProvider` remains the single URL writer. The `/graphify`
route identifies the demo; query parameters identify its reproducible view.

| State | Query keys |
| --- | --- |
| Source and neighborhood | `database`, `loaded`, `root`, `depth`, `tests` |
| Symbol and search | `node`, `q` |
| Graph presentation | `color`, `group` |
| Disclosures | `controls`, `advanced`, `help`, `symbols`, `sourceBody`, `connections`, `floating`, `inspectorBounds`, `upload` |
| Components | `components`, `component` |
| Inclusion/exclusion | Existing `graphify.include*` and `graphify.exclude*` keys |

Files, credentials, fetched results, progress and errors are not serialized into
the URL. Reloading an import view requires selecting the local file again.
Browser Back/Forward restores URL-owned selections and controls. Floating panel
coordinates are URL-owned; pointer updates replace the current history entry rather
than adding an entry for every pixel moved.

## Verification

- `node --test tests/graphify-entry.test.mjs tests/graphify-layout.test.mjs`
- `npx playwright test browser-tests/graphify-workspace.spec.mjs`
- `npx playwright test browser-tests/assistant-launcher.spec.mjs browser-tests/assistant-workspace.spec.mjs browser-tests/assistant-context.spec.mjs browser-tests/assistant-chat.spec.mjs`
- `npm run build`

The browser fixture covers inference without a `Main` marker, keyboard selection,
query and disclosure restoration, inline artifact import with existing-database
consent, mobile overflow and accessibility. Live query verification checks the
actual Cypher syntax separately from the browser fixture.

Current verification: 15 entry/layout unit tests and 11 focused browser tests
passed, including accessibility checks and the production UI build. After the
last database-selection change, all four Graphify browser tests passed again.
Explicit and parameterized named-entry queries succeeded against the live graph.
Subsequent database requests, including health/status, timed out. The final
metadata-only fallback and bounded neighborhood sample therefore still need live
verification. The mandatory duplicate check completed extraction but failed on
the same database connection timeout; it has not passed. No full suite or CI
result is claimed.
