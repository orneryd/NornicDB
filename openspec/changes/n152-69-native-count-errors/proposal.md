# N152-69 native count errors

Scope: preserve native count failures through HTTP admin stats, native database info, status fallback, scoped metrics and error-returning Heimdall router. Reuse existing localized HTTP failure envelopes; no new schema or API. Composite partial-stat behavior and separate composite schema admission work remain distinct.

Prior unfinished proposal was read from /private/tmp/n152-86-history.8nOnDGob/native-count and copied as evidence only; its source/branch remain untouched. Four test files are reused from that proposal, which extracted and ported original ticket acceptance fixtures to current manager/auth seams. Original inline blocks remain verbatim in evidence; no exact byte-identical runtime fixture claim. All production fixes require fresh RED and independent owner review.

DeepSeek source audit cc60cccd3dea400dac2feef596269f65 completed in116.561s. Its source-only findings corroborated the five native error owners and existing composite partial-stat distinction. It ran no tests; graph retrieval was unavailable. Parent independently traced the obsolete prefix interface and all bundled Heimdall consumers and owns acceptance.

## Parent causal decision after RED

Fresh original acceptance selectors fail all10 node/edge fault cells (2.396s), while healthy/recovery controls pass. The old status/metrics prefix fast path asserts obsolete methods NodeCountByPrefix(string), but every built-in supported owner uses storage.PrefixStatsEngine with context.Context. Thus supported DB storage always used the physical manager path. Remove this obsolete duplicate path rather than changing it to bypass canonical manager admission; NamespacedEngine.NodeCount/EdgeCount already route through the proper prefix owner. One context-aware physical population and a shared complete-pair helper now serve the public consumers. No new statistics cache or API is introduced. Existing storage-acquisition skip behavior is not changed or represented as repaired; composite admission remains separately tracked.
