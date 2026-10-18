# Adds name-resolved call/reference edges for pkg/cypher (graphify's own edges miss
# many receiver method calls). For each graphify function node, its body is the text
# from its start line to the next function node's start line in the same file; every
# identifier in it that names a pkg/cypher function (called or passed as a value) is
# an edge. Writes calls-augmented.json: {node_id: [callee ids]}.
import json, re, collections
G = json.load(open('/graphs/nornic-wt-rel-inv/graphify-out/graph.json'))
root = '/repos/nornic-wt-rel-inv/'
funcs = collections.defaultdict(list); byname = collections.defaultdict(list)
for n in G['nodes']:
    f = n.get('source_file', ''); loc = n.get('source_location', '')
    lab = n.get('label', '')
    if f.startswith('pkg/cypher/') and f.endswith('.go') and lab.endswith('()') and loc.startswith('L'):
        funcs[f].append((int(loc[1:]), n['id']))
        byname[lab[:-2].lstrip('.')].append(n['id'])
edges = {}
ident = re.compile(r'[A-Za-z_]\w*')
for f, lst in funcs.items():
    lines = open(root + f, encoding='utf-8', errors='replace').read().split('\n')
    lst.sort()
    for i, (start, nid) in enumerate(lst):
        end = lst[i + 1][0] - 1 if i + 1 < len(lst) else len(lines)
        body = '\n'.join(lines[start:end])  # skip the signature line itself
        body = re.sub(r'//[^\n]*', '', body)
        body = re.sub(r'"(?:[^"\\n]|\.)*"|`[^`]*`', '""', body)
        out = set()
        for w in set(ident.findall(body)):
            for t in byname.get(w, ()):
                if t != nid: out.add(t)
        edges[nid] = sorted(out)
json.dump(edges, open('/graphs/nornic-wt-rel-inv/calls-augmented.json', 'w'))
print('functions', sum(len(v) for v in funcs.values()), 'edges', sum(len(v) for v in edges.values()))
