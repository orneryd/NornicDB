# Call-graph inventory of relationship-pattern readers (pkg/cypher, develop 16923aef).
import json, re, collections, sys
G = json.load(open('/graphs/nornic-wt-rel-inv/graphify-out/graph.json'))
nodes = {n['id']: n for n in G['nodes']}
def src(nid): return nodes.get(nid, {}).get('source_file', '')
def is_test(nid): return src(nid).endswith('_test.go')
def name(nid):
    n = nodes.get(nid, {}); return '%s:%s %s' % (src(nid).replace('pkg/cypher/', ''), n.get('source_location', '').lstrip('L'), n.get('label', nid))
calls = collections.defaultdict(set); callers = collections.defaultdict(set)
for e in G['links']:
    if e.get('relation') in ('calls', 'indirect_call', 'references'):
        s, t = e['source'], e['target']
        if s in nodes and t in nodes and src(s).endswith('.go') and src(t).endswith('.go'):
            calls[s].add(t); callers[t].add(s)
aug = json.load(open('/graphs/nornic-wt-rel-inv/calls-augmented.json'))
calls_g = {k: set(v) for k, v in calls.items()}
for s, ts in aug.items():
    for t in ts:
        if t in nodes:
            calls[s].add(t); callers[t].add(s)
# location index for pkg/cypher functions
byloc = {}
for nid, n in nodes.items():
    if src(nid).startswith('pkg/cypher/') and n.get('source_location', '').startswith('L'):
        k = (src(nid)[len('pkg/cypher/'):], n['source_location'][1:])
        if k not in byloc or n.get('label', '').endswith('()'): byloc[k] = nid
# entry points: every exported StorageExecutor execute entry + other packages' calls into pkg/cypher
entry_labels = re.compile(r'^\.?(Execute|ExecuteWithDB|ExecuteImplicit|ExecuteInTransaction|ExecuteWithContext)\(\)$')
entries = [nid for nid, n in nodes.items() if src(nid).startswith('pkg/cypher/') and not is_test(nid) and entry_labels.match(n.get('label', ''))]
outside = set()
for t in list(callers):
    if src(t).startswith('pkg/cypher/') and not is_test(t):
        for s in callers[t]:
            if not src(s).startswith('pkg/cypher/') and not is_test(s) and src(s).startswith(('pkg/', 'cmd/')):
                outside.add(t)
def bfs(starts, prod_only=True):
    prev = {s: None for s in starts}; q = collections.deque(starts)
    while q:
        u = q.popleft()
        for v in calls[u]:
            if v in prev or (prod_only and is_test(v)): continue
            prev[v] = u; q.append(v)
    return prev
reach_exec = bfs(entries)
reach_any = bfs(entries + sorted(outside))
_saved = calls
calls = collections.defaultdict(set, calls_g)
reach_g = bfs(entries + sorted(outside))
calls = _saved
def chain(prev, v):
    out = []
    while v is not None: out.append(nodes[v].get('label', v)); v = prev[v]
    return ' <- '.join(out)
# candidates from the grep/AST extractors
cands = []
for line in open('/graphs/nornic-wt-rel-inv/union.txt'):
    m = re.match(r'(\S+?\.go):(\d+) ', line)
    if not m: continue
    nid = byloc.get((m.group(1), m.group(2)))
    cands.append((line.strip()[:110], nid))
mode = sys.argv[1] if len(sys.argv) > 1 else 'cands'
if mode == 'cands':
    print('entries:', [name(e) for e in entries]); print('outside-called pkg/cypher functions:', len(outside))
    for text, nid in cands:
        if nid is None: print('NOGRAPH |', text); continue
        prodc = sorted(nodes[c].get('label') for c in callers[nid] if not is_test(c))
        testc = sum(1 for c in callers[nid] if is_test(c))
        r = ('EXEC' if nid in reach_exec else ('OTHER-ENTRY' if nid in reach_any else 'UNREACHED')) + ('/g' if nid in reach_g else '/aug')
        print('%s | %s | prod callers %d test callers %d | %s' % (r, name(nid), len(prodc), testc, chain(reach_exec if nid in reach_exec else reach_any, nid)[:400] if nid in reach_any else ','.join(prodc)[:200]))
elif mode == 'prims':
    # readers grep missed: callers of the bracket / declaration primitives not in the candidate list
    known = {nid for _, nid in cands if nid}
    prim_names = sys.argv[2].split(',')
    prims = [nid for nid, n in nodes.items() if src(nid).startswith('pkg/cypher/') and not is_test(nid) and n.get('label', '').rstrip('()').lstrip('.') in prim_names]
    print('primitives:', [name(p) for p in prims])
    for p in prims:
        for c in sorted(callers[p]):
            if is_test(c) or not src(c).startswith('pkg/cypher/'): continue
            flag = 'known' if c in known else 'NEW'
            print('%s | %s -> %s | %s' % (flag, name(c), nodes[p]['label'], 'EXEC' if c in reach_exec else ('OTHER' if c in reach_any else 'UNREACHED')))
elif mode == 'chain':
    for label in sys.argv[2].split(','):
        for nid, n in nodes.items():
            if src(nid).startswith('pkg/cypher/') and not is_test(nid) and n.get('label', '').rstrip('()').lstrip('.') == label:
                print(name(nid), '|', chain(reach_any, nid) if nid in reach_any else 'UNREACHED', '| callers:', sorted(nodes[c]['label'] for c in callers[nid] if not is_test(c)))
