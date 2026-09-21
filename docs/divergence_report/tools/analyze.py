"""Deterministic divergence analysis over per-component graphify graphs + Go sources. No LLM, no network.
Runs inside the graphify image (tree-sitter-go available):  python /work/analyze.py
Inputs : /work/out/<component>/graphify-out/graph.json , /work/src/<component>/**/*.go
Outputs: /work/report/data.json (everything), printed short summary
"""
import json, os, re, collections, itertools, hashlib
import tree_sitter_go as tsgo
from tree_sitter import Language, Parser

ROOT = '/work'
COMPS = ['cypher', 'storage', 'search', 'nornicdb', 'server', 'bolt', 'multidb', 'embed']
GO = Language(tsgo.language())
parser = Parser(GO)

# ---------------------------------------------------------------- source scan (functions, receivers, bodies)
ID_KINDS = {'identifier', 'field_identifier', 'type_identifier', 'package_identifier'}
LIT_KINDS = {'interpreted_string_literal', 'raw_string_literal', 'int_literal', 'float_literal', 'rune_literal'}


def leaves(node, out):
    if node.child_count == 0:
        k = node.type
        if k == 'comment':
            return
        out.append('ID' if k in ID_KINDS else 'LIT' if k in LIT_KINDS else (node.text.decode('utf-8', 'ignore') if len(node.text) < 12 else k))
    else:
        for c in node.children:
            leaves(c, out)


def scan(comp):
    funcs = []
    base = f'{ROOT}/src/{comp}'
    for r, _, fs in os.walk(base):
        for f in fs:
            if not f.endswith('.go'):
                continue
            p = os.path.join(r, f)
            src = open(p, 'rb').read()
            tree = parser.parse(src)
            for n in tree.root_node.children:
                if n.type not in ('function_declaration', 'method_declaration'):
                    continue
                name = n.child_by_field_name('name').text.decode()
                recv = ''
                if n.type == 'method_declaration':
                    rt = n.child_by_field_name('receiver').text.decode()
                    m = re.search(r'\*?\s*([A-Za-z_][A-Za-z0-9_]*)\s*(\[.*\])?\s*\)$', rt)
                    recv = m.group(1) if m else rt
                body = n.child_by_field_name('body')
                toks = []
                if body is not None:
                    leaves(body, toks)
                funcs.append(dict(comp=comp, file=os.path.relpath(p, base), line=n.start_point[0] + 1, lines=n.end_point[0] - n.start_point[0] + 1,
                                  name=name, recv=recv, toks=toks))
    return funcs


# ---------------------------------------------------------------- A. capability forwarding gaps between wrapper types
def forwarding_gaps(funcs, families):
    by_type = collections.defaultdict(dict)
    for f in funcs:
        if f['recv'] and f['name'][0].isupper():
            by_type[f['recv']][f['name']] = f
    out = []
    for fam_name, (inner, wrappers) in families.items():
        if inner not in by_type:
            continue
        present = [w for w in wrappers if w in by_type]
        rows = []
        for m, f in sorted(by_type[inner].items()):
            missing = [w for w in present if m not in by_type[w]]
            if missing and len(missing) < len(present) or (missing and len(present) and m.startswith(('Batch', 'Stream', 'Get', 'Backup', 'Embed', 'Count', 'Find', 'Iterate'))):
                rows.append(dict(method=m, inner_loc=f"{f['file']}:{f['line']}", missing_in=missing, present_in=[w for w in present if w not in missing]))
        out.append(dict(family=fam_name, inner=inner, wrappers=present, inner_exported_methods=len(by_type[inner]), gaps=rows))
    return out


# ---------------------------------------------------------------- B. name-variant families
AFFIX = ['WithContextFull', 'WithPathContext', 'WithContext', 'WithOptions', 'WithExhaustion', 'WithLexicalEntries', 'FromEntries', 'WithoutEmbeddings', 'WithEmbeddings',
         'VisibleAt', 'InTxn', 'InTransaction', 'WithView', 'Locked', 'Unlocked', 'Internal', 'Impl', 'Fast', 'Slow', 'Full', 'Simple', 'Legacy', 'Indexed', 'Streaming', 'Stream',
         'Batch', 'Bulk', 'Single', 'Pooled', 'Heap', 'Into', 'Projected', 'Cached', 'Uncached', 'Atomic', 'Background', 'Async', 'Sync', 'Safe', 'Raw', 'V2', 'V1', 'Ctx', 'WithFallback',
         'ByPrefix', 'ForNode', 'ForEdge', 'Pipeline', 'Compound', 'Optimized', 'Direct', 'Multi', 'Chained', 'Generic', 'Native', 'WithLimit', 'WithStats', 'WithFilter', 'WithTimeout']
AFFIX.sort(key=len, reverse=True)


def stem(name):
    s = name
    changed = True
    while changed:
        changed = False
        for a in AFFIX:
            if s.endswith(a) and len(s) > len(a) + 3:
                s = s[:-len(a)]
                changed = True
    return s[0].lower() + s[1:]


def variant_families(funcs):
    fam = collections.defaultdict(list)
    for f in funcs:
        fam[(f['comp'], f['recv'], stem(f['name']))].append(f)
    out = []
    for (comp, recv, st), fs in fam.items():
        names = sorted({f['name'] for f in fs})
        if len(names) >= 3:
            out.append(dict(comp=comp, recv=recv, stem=st, variants=len(names), total_lines=sum(f['lines'] for f in fs),
                            members=[dict(name=f['name'], loc=f"{f['file']}:{f['line']}", lines=f['lines']) for f in sorted(fs, key=lambda x: x['name'])]))
    return sorted(out, key=lambda x: (-x['variants'], -x['total_lines']))


# ---------------------------------------------------------------- C. parallel dispatchers from the graphify call graph
def load_graph(comp):
    g = json.load(open(f'{ROOT}/out/{comp}/graphify-out/graph.json', encoding='utf-8'))
    nodes = {n['id']: n for n in g['nodes']}
    calls = collections.defaultdict(set)
    for e in g.get('edges', g.get('links', [])):
        if e.get('relation') == 'calls' and e['source'] in nodes and e['target'] in nodes:
            calls[e['source']].add(e['target'])
    return g, nodes, calls


def parallel_dispatchers(comp, nodes, calls, min_callees=6, min_jaccard=0.45):
    big = {k: v for k, v in calls.items() if len(v) >= min_callees}
    inv = collections.defaultdict(set)
    for k, v in big.items():
        for t in v:
            inv[t].add(k)
    cand = collections.Counter()
    for t, srcs in inv.items():
        if len(srcs) > 60:
            continue
        for a, b in itertools.combinations(sorted(srcs), 2):
            cand[(a, b)] += 1
    out = []
    for (a, b), inter in cand.items():
        j = inter / len(big[a] | big[b])
        if j >= min_jaccard:
            lab = lambda i: f"{nodes[i].get('label')} ({nodes[i].get('source_file')}:{nodes[i].get('source_location')})"
            only_a = sorted(nodes[x].get('label') for x in big[a] - big[b])
            only_b = sorted(nodes[x].get('label') for x in big[b] - big[a])
            out.append(dict(comp=comp, a=lab(a), b=lab(b), shared_callees=inter, jaccard=round(j, 2), only_a=only_a[:25], only_b=only_b[:25]))
    return sorted(out, key=lambda x: (-x['shared_callees'] * x['jaccard']))


# ---------------------------------------------------------------- D. near-duplicate function bodies (token shingles, identifiers and literals normalised)
def clones(funcs, k=12, min_tokens=90, min_sim=0.70):
    sh = {}
    for i, f in enumerate(funcs):
        t = f['toks']
        if len(t) < min_tokens:
            continue
        s = {hashlib.blake2b(' '.join(t[j:j + k]).encode(), digest_size=6).digest() for j in range(len(t) - k + 1)}
        sh[i] = s
    inv = collections.defaultdict(list)
    for i, s in sh.items():
        for h in s:
            inv[h].append(i)
    pair = collections.Counter()
    for h, ids in inv.items():
        if len(ids) > 40:
            continue
        for a, b in itertools.combinations(ids, 2):
            pair[(a, b)] += 1
    out = []
    for (a, b), inter in pair.items():
        sim = inter / len(sh[a] | sh[b])
        cont = inter / min(len(sh[a]), len(sh[b]))
        if sim >= min_sim or (cont >= 0.85 and min(len(sh[a]), len(sh[b])) > 150):
            fa, fb = funcs[a], funcs[b]
            d = lambda f: f"{f['comp']}/{f['file']}:{f['line']} {('(' + f['recv'] + ').') if f['recv'] else ''}{f['name']} [{f['lines']} lines]"
            out.append(dict(a=d(fa), b=d(fb), similarity=round(sim, 2), containment=round(cont, 2), dup_lines=min(fa['lines'], fb['lines']),
                            same_name=fa['name'] == fb['name'], cross_component=fa['comp'] != fb['comp'], ia=a, ib=b))
    return sorted(out, key=lambda x: -x['dup_lines'] * x['similarity'])


def clone_groups(cl):
    parent = {}

    def find(x):
        while parent.setdefault(x, x) != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x
    for c in cl:
        parent[find(c['ia'])] = find(c['ib'])
    groups = collections.defaultdict(set)
    for c in cl:
        groups[find(c['ia'])].update([c['a'], c['b']])
    return sorted(([sorted(v)] for v in groups.values() if len(v) >= 3), key=lambda g: -len(g[0]))


# ---------------------------------------------------------------- run
allf = []
for c in COMPS:
    allf += scan(c)
print('functions scanned:', len(allf), '| lines in functions:', sum(f['lines'] for f in allf))

FAMILIES = {
    'storage engine stack': ('BadgerEngine', ['WALEngine', 'AsyncEngine', 'NamespacedEngine', 'MemoryEngine']),
    'storage transaction': ('BadgerEngine', ['BadgerTransaction']),
    'multidb engine wrappers': ('NamespacedEngine', ['StorageSizeTrackingEngine', 'sizeTrackingEngine', 'CompositeEngine', 'LimitEnforcingEngine', 'limitEnforcedEngine']),
    'embedder decorators': ('VoyageEmbedder', ['CachedEmbedder', 'TracedEmbedder', 'OpenAIEmbedder', 'OllamaEmbedder', 'LocalGGUFEmbedder']),
}
gaps = forwarding_gaps(allf, FAMILIES)
fams = variant_families(allf)
disp = []
gstats = {}
for c in COMPS:
    g, nodes, calls = load_graph(c)
    gstats[c] = dict(nodes=len(nodes), call_edges=sum(len(v) for v in calls.values()))
    disp += parallel_dispatchers(c, nodes, calls)
cl = clones(allf)
groups = clone_groups(cl)
for c in cl:
    c.pop('ia'); c.pop('ib')
types = sorted({f['recv'] for f in allf if f['recv'].endswith(('Engine', 'Embedder'))})

os.makedirs(f'{ROOT}/report', exist_ok=True)
json.dump(dict(graph_stats=gstats, engine_and_embedder_types=types, forwarding_gaps=gaps, variant_families=fams, parallel_dispatchers=disp, clones=cl, clone_groups=groups),
          open(f'{ROOT}/report/data.json', 'w', encoding='utf-8'), indent=1)
print('types ending in Engine/Embedder:', types)
for g in gaps:
    print(f"gaps[{g['family']}]: inner {g['inner']} has {g['inner_exported_methods']} exported methods; wrappers {g['wrappers']}; methods not forwarded everywhere: {len(g['gaps'])}")
print('variant families (>=3 variants):', len(fams), '| top:', [(f['comp'], f['recv'], f['stem'], f['variants']) for f in fams[:8]])
print('parallel dispatcher pairs:', len(disp), '| top:', [(d['a'][:50], d['b'][:50], d['shared_callees'], d['jaccard']) for d in disp[:5]])
print('clone pairs:', len(cl), '| duplicated lines (sum of smaller side):', sum(c['dup_lines'] for c in cl), '| cross-component:', sum(c['cross_component'] for c in cl), '| groups>=3:', len(groups))
