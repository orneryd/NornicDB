# Inventory correction evidence

Baseline and merge base: `a5b7980e89171764091a6d47d0b8a8ee3ca19c88`.
The full release verifier for #1936 failed in the observability group on the
same fingerprint in `/tmp/n152-58-cache-release-verify.log` (including retries).
This assignment entered release in #1922 (`f0f5a2f4f`).

## Source audit

The unchanged checker's canonical `format.Node`/SHA256 identifies fingerprint
`0740835c022bc29774a617a68d1a166cfd42d93a67a38a549afd10582e316828`
as `txResp.statusCode = resp.StatusCode`, not an error-return or URL assignment.
Source positions at this base: private int field at remote_engine.go:428,
assignment at :539, sole 2xx-range consumer at :758. The field is unexported
and is not response serialization or a diagnostic/network sink.

The runtime diff from #1922 leaves request URL composition and auth setup
unchanged. NewRemoteEngine retains scheme admission, commitURL/txOpenURL and
openTransaction retain ComposeRequestURL, and doRequestToURL retains its named
error-result redaction defer. Compose.Outcomes errors are recursively processed
by the existing RedactClientError owner. This review does not claim broader
origin-policy guarantees or authorize runtime policy changes.

Add one exact-key classification to the existing non-egress inventory. Existing
entries and all checker logic are unchanged. The structural checker continues
to reject missing and stale classifications. No new Plane issue or payload
experiment; this is an integration follow-up to #1922/#1936.

## Checks

All Go checks use `GOMAXPROCS=2 GOMEMLIMIT=1500MiB`, `-p=1`, and tags
`noui,nolocalllm`.

- Clean detached baseline `/tmp/n152-68-base-controls`, exact same SHA:
  `go test -p=1 -tags=noui,nolocalllm ./pkg/observability -run '^TestCredentialURLSinkStructure$' -count=1`
  **RED 1.526s**, same single unreviewed fingerprint. Checkout remained clean
  and was released to its owning agent when complete.
- Same command with the one classification: **GREEN 1.572s**.
- `go test -p=1 -tags=noui,nolocalllm ./pkg/observability -count=1 -timeout=5m`:
  **PASS 16.749s**, entire observability package.
- `go test -race -p=1 -tags=noui,nolocalllm ./pkg/observability -run 'TestCredentialURL|TestConfiguredHTTP|TestCredentialedHTTP|TestComposeRequestURL|TestCredentialStructural' -count=1 -timeout=3m`:
  **PASS 14.102s**, focused inventory/URL/redaction controls.
- `go vet -p=1 -tags=noui,nolocalllm ./pkg/observability`: **PASS**.
- `make verify-list BASE=origin/release/v1.5.2 FETCH=0`: **PASS**; runtime owners
  selected, docs path-skipped.
- `GOMAXPROCS=2 GOMEMLIMIT=1500MiB GOFLAGS=-p=1 make verify BASE=origin/release/v1.5.2 ONLY=localization FETCH=0`: **PASS 9s**; all other owners explicitly skipped.
- Staged duplicate tool: exit0, **no-op** because no source additions are staged; this is not an indexed discovery pass.

Full combined release verification remains separately pending. This metadata
correction does not establish a fresh application build, broad race/coverage,
allocation, conformance, differential, platform or CI pass.
