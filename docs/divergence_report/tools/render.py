"""Render graphify/report/data.json (+ cypher graph) into DIVERGENCE_REPORT.md. Pure formatting, no LLM."""
import json, collections, os
B = os.path.dirname(os.path.abspath(__file__))
D = json.load(open(f'{B}/report/data.json', encoding='utf-8'))
L = []
w = L.append

w('# NornicDB — divergent code paths that are candidates for convergence')
w('')
w('Source: upstream main at 994b3a68, non-test Go files of 8 components. Built with graphify (code-only AST graphs, one per component, no LLM) plus a deterministic analysis of the graphs and sources (`graphify/analyze.py`). Nothing here was judged by a model; every item is a structural signal and needs a human look before acting. Not exhaustive.')
w('')
w('## 0. Graphs')
w('')
w('| component | graph nodes | call edges | graph |')
w('| --- | --- | --- | --- |')
for c, s in D['graph_stats'].items():
    w(f"| pkg/{c} | {s['nodes']} | {s['call_edges']} | `graphify/out/{c}/graphify-out/graph.json` |")

# ---- 1 routers
g = json.load(open(f'{B}/out/cypher/graphify-out/graph.json', encoding='utf-8'))
nodes = {n['id']: n for n in g['nodes']}
calls = collections.defaultdict(set)
for e in g.get('edges', g.get('links', [])):
    if e.get('relation') == 'calls':
        calls[e['source']].add(e['target'])
def find(label, file):
    return [i for i, n in nodes.items() if n.get('label') == label and n.get('source_file', '').endswith(file)]
w('')
w('## 1. The two Cypher routers (auto-commit vs explicit transaction)')
w('')
ra, rb = find('.executeWithoutTransaction()', 'executor_query_routing.go'), find('.executeQueryAgainstStorage()', 'transaction.go')
if ra and rb:
    A, Bc = calls[ra[0]], calls[rb[0]]
    lab = lambda s: sorted(nodes[x]['label'] for x in s if x in nodes)
    w(f"`executeWithoutTransaction` (executor_query_routing.go) dispatches to {len(A)} functions, `executeQueryAgainstStorage` (transaction.go) to {len(Bc)}; {len(A & Bc)} are shared.")
    w('')
    w(f"- reached **only from the auto-commit router** ({len(A - Bc)}): " + ', '.join(f'`{x}`' for x in lab(A - Bc)))
    w(f"- reached **only from the transaction router** ({len(Bc - A)}): " + ', '.join(f'`{x}`' for x in lab(Bc - A)))
    w('')
    w('Every handler in the first list is a query shape that behaves differently inside `BEGIN … COMMIT` (the pattern behind #397→#410, #399→#459, #457). Convergence target: one router, with the transaction supplying only the storage view.')
else:
    w(f'(router nodes not found by label: {len(ra)} / {len(rb)})')

# ---- 2 wrappers
w('')
w('## 2. Capabilities that are forwarded by some wrappers and not by others')
w('')
w('Method present on the inner type and on at least one wrapper of the production chain, but missing on another wrapper of the same chain. A caller that type-asserts for the capability gets it or silently falls back depending on which wrapper it happens to hold (the pattern behind #420, #424, #473).')
for fam in D['forwarding_gaps']:
    chain = [x for x in fam['wrappers'] if x not in ('MemoryEngine',)]
    rows = [r for r in fam['gaps'] if any(p in chain for p in r['present_in']) and any(m in chain for m in r['missing_in'])]
    w('')
    w(f"### {fam['family']}: inner `{fam['inner']}` ({fam['inner_exported_methods']} exported methods), wrappers {', '.join('`'+x+'`' for x in fam['wrappers'])}")
    w('')
    if not rows:
        w('No partially forwarded methods.')
        continue
    w('| method | defined at | forwarded by | **missing in** |')
    w('| --- | --- | --- | --- |')
    for r in rows[:60]:
        w(f"| `{r['method']}` | {r['inner_loc']} | {', '.join(r['present_in'])} | **{', '.join(r['missing_in'])}** |")
    none = [r['method'] for r in fam['gaps'] if not any(p in chain for p in r['present_in'])]
    if none:
        w('')
        w(f"Forwarded by no wrapper at all ({len(none)}; only reachable by type-asserting down to `{fam['inner']}`), data-access ones only: " + ', '.join(f'`{m}`' for m in none[:40]))

# ---- 3 variant families
w('')
w('## 3. Variant families: one operation, several hand-written variants')
w('')
w('Functions on the same receiver whose names differ only by a variant suffix (`WithContext`, `Locked`, `VisibleAt`, `InTxn`, `WithoutEmbeddings`, `Fast`, `Full`, `Batch`, …). Each variant is a place where a fix can land in one copy and not the others.')
w('')
w('| component | receiver | operation | variants | total lines | members |')
w('| --- | --- | --- | --- | --- | --- |')
for f in D['variant_families'][:25]:
    w(f"| {f['comp']} | `{f['recv'] or '(func)'}` | `{f['stem']}` | {f['variants']} | {f['total_lines']} | " + ', '.join(f"`{m['name']}` ({m['loc']}, {m['lines']}L)" for m in f['members'][:8]) + (' …' if len(f['members']) > 8 else '') + ' |')

# ---- 4 parallel implementations
w('')
w('## 4. Parallel implementations: function pairs that call largely the same helpers')
w('')
w('From the graphify call graph: pairs with ≥6 callees each and callee-set Jaccard ≥0.45. High overlap means two functions orchestrate the same steps; the columns show what each one does that the other does not.')
w('')
w('| component | A | B | shared callees | Jaccard | only in A | only in B |')
w('| --- | --- | --- | --- | --- | --- | --- |')
for p in D['parallel_dispatchers'][:40]:
    w(f"| {p['comp']} | `{p['a']}` | `{p['b']}` | {p['shared_callees']} | {p['jaccard']} | {', '.join(p['only_a'][:6])} | {', '.join(p['only_b'][:6])} |")

# ---- 5 clones
w('')
w('## 5. Near-duplicate function bodies')
w('')
cl = D['clones']
w(f"Token-shingle comparison with identifiers and literals normalised (functions ≥90 tokens, similarity ≥0.70 or containment ≥0.85): {len(cl)} pairs, about {sum(c['dup_lines'] for c in cl)} duplicated lines, {sum(c['cross_component'] for c in cl)} pairs across components.")
w('')
w('### Groups of 3 or more copies')
for gi, grp in enumerate(D['clone_groups'], 1):
    w('')
    w(f'{gi}. ' + ' · '.join(f'`{x}`' for x in grp[0][:10]) + (' …' if len(grp[0]) > 10 else ''))
w('')
w('### Largest pairs')
w('')
w('| A | B | similarity | containment | lines |')
w('| --- | --- | --- | --- | --- |')
for c in cl[:45]:
    w(f"| `{c['a']}` | `{c['b']}` | {c['similarity']} | {c['containment']} | {c['dup_lines']} |")

open(f'{B}/report/DIVERGENCE_REPORT.md', 'w', encoding='utf-8').write('\n'.join(L) + '\n')
print('written', len(L), 'lines')
