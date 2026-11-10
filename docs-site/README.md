# Documentation portal

VitePress builds the Markdown in `../docs`. The portal includes an English locale, dark mode, documentation and code search, source previews, and a D3 caller/callee explorer. GitHub sign-in and inference are deferred.

## Build and test

Requirements: Node 24 and the Go version in the repository's `go.mod`.

```sh
npm ci
npm run build
npm test
npm run test:browser
npm run dev
```

Open `http://127.0.0.1:4173/NornicDB/`. Browser tests use installed Chrome. CI can use Playwright Chromium instead. The production output is `.vitepress/dist`.

The corpus remains in `docs/`; `.generated` is disposable build output. `corpus-manifest.json` records every source file and its hash. `source-provenance.json` records additional release documentation retained when the PR moved to main.

Before their source changes are integrated, release-only pages carry a branch-reference notice and link source files at their original release commit. They remain published and browser-searchable, but are excluded from the current semantic index and current-source cross-links.

`releaseContentIntegrated` marks the retained release documentation as current once its code lands. Integrated pages participate in semantic search and source cross-links; retired archives are not published. Current source links use the checked-out head, while tagged bundles stay pinned to their tag. The tracked corpus manifest contains file hashes rather than a self-referencing build commit.

The development contribution page is rendered and indexed from root `CONTRIBUTING.md`; its small source page is a pointer, not a second guide. Publication rules exclude retired development archives in current and tagged builds.

## Public search

The header search supports documentation and code scopes. Full-text indexes run in a Web Worker; the code index is loaded only when requested. Semantic results can replace the local results when the API responds. A busy, failed, or timed-out request leaves local results visible and displays a dismissible notification. Unknown paths open an editable search page.

Header search expands the existing input in place and anchors a non-modal results panel beneath it. Initial guidance gives way to results. Keyboard navigation, dismissal, and viewport changes retain the document's scroll position. Semantic results show browser request latency and the API's measured processing time separately. Version choices are sorted numerically, newest first, below Current.

Set `VITE_DOCS_API_ORIGIN` to the public API origin before building. For local testing, `.env` can contain:

```text
VITE_DOCS_API_ORIGIN=http://127.0.0.1:4174
```

For GitHub Pages, set repository variable `DOCS_API_ORIGIN`. Its value is a public HTTPS origin, including the port when needed. The build consumes that variable; no credentials belong in the static site.

The API is maintained separately in the private [documentation service repository](https://github.com/orneryd/nornicdb-docs-service). Its README covers Docker, nginx port 4269, the read-only API account, the separate ingestion account, rate limits, ETags, and optional IP maintenance. The site remains usable when that API is unavailable or a browser does not trust its self-signed certificate.

## Source graph

The build extracts declarations and package-resolved Go calls without executing application code. Source previews link to the exact source commit. Documentation references provide navigation in both directions. Previews open in an accessible dialog and retain the document's position.

To add Graphify-extracted call relationships:

```sh
python -m pip install -r requirements-graph.txt
```

Set `GRAPHIFY_OUTPUT` to an output directory outside the source tree, then run:

```sh
python tools/extract-graphify.py
python tools/import-graphify.py
npm run build
```

Graphify output is reduced to source-matched declarations. A stale package tree is rejected. The 3D D3 force-directed explorer groups packages and call branches recursively. It shows at most 60 nodes and 180 edges across one to three hops, with a keyboard-accessible node list, caller/callee controls, and a navigation trail. Static call relationships are not captured runtime stacks; dynamic dispatch may be incomplete.

## Release versions

```sh
npm run build
npm run build:version -- v1.4.3
```

The version command archives documentation and Go source from the exact tag, writes a separate bundle under `.vitepress/dist/versions/<tag>`, and updates `versions.json`. Source links use the tagged commit. It restores the current generated corpus afterward. Historical bundles reject API calls in the client and use browser search only. They create no historical semantic indexes.

Publishing builds the three most recent stable tags. Additional tagged bundles can be generated explicitly. Older content is retained as recorded at its tag, including historical behavior; the portal identifies the selected version.

## Verification

The interface follows NN/g's [progressive disclosure](https://www.nngroup.com/articles/progressive-disclosure/) and [recognition rather than recall](https://www.nngroup.com/articles/recognition-and-recall/) guidance: task-based navigation comes first; code appears on request; visible caller lists and a navigation trail provide context. Dialogs retain page position and return keyboard focus. Search reports its active mode and preserves results during outages. Graph controls expose a text alternative, bounded depth, and a freeze option; reduced-motion preferences are respected.

The checks cover file preservation, inventory coverage, graph edge integrity, link/rendering errors, default dark mode, search fallback, source navigation, keyboard use, mobile overflow, and tagged source provenance. Screenshots in `review-screenshots/` document the reviewed build.

The browser API client exposes only search and related-public-reference operations. OAuth, session tokens, model access, database credentials, and arbitrary queries are not part of this portal iteration.
# Analytics

The public site uses PostHog project `724362` (US Cloud). The write-only project token is configured in `.vitepress/theme/analytics.mjs`; no personal API key or database credential is bundled. Collection is limited to `orneryd.github.io`, so localhost previews and CI do not pollute production analytics.

Open the [NornicDB OSS dashboard](https://us.posthog.com/project/724362/dashboard/2329195) for daily pageviews, top documentation paths, version usage, semantic/fallback search result events, average result counts, search-result opens, code exploration, and outbound destination categories. These are usage counts, not verified installs, GitHub stars, revenue, or managed-service inquiries. Search events currently cover semantic responses with results and fallback notices; they are not a count of every keystroke or every local-only search.

The footer provides a persistent opt-out and links to the [analytics privacy notice](../docs/privacy.md). Do Not Track and Global Privacy Control disable collection. The SDK uses memory persistence, no person profiles, and no recording, autocapture, automatic exceptions, or remote feature configuration. Its event property allowlist removes raw URLs, query parameters, referrers, and form values. Review the allowlist and privacy notice together before collecting additional data.

Run `npx playwright test browser-tests/analytics.spec.mjs` after building. The test serves the local build under the public hostname and intercepts PostHog requests, checking real SDK payloads without sending test events to the project. The simulated browser disables automation identification only inside that test; production bot filtering remains enabled.
