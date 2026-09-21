"""Summarise golang.org/x/tools/cmd/deadcode output into deletion candidates. No LLM."""
import re, collections, os, json
B = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.join(B, '..', 'src-main86')


def load(p):
    out = []
    for l in open(p, encoding='utf-8'):
        m = re.match(r'(.+?):(\d+):\d+: unreachable func: (.+)', l.strip())
        if m:
            out.append((m.group(1).replace(chr(92), '/'), int(m.group(2)), m.group(3)))
    return out


T = load(f'{B}/deadcode/dead-test-noui.txt')      # dead even when tests are roots
N = load(f'{B}/deadcode/dead-notest-noui.txt')    # dead for the binaries
tset = {(f, n) for f, _, n in T}
only_tests = [x for x in N if (x[0], x[2]) not in tset]
print('unreachable from the binaries:', len(N), '| of those also unreachable from every test:', len(T), '| kept alive only by tests:', len(only_tests))

# GPU / tagged files that were excluded from analysis may use "dead" functions: collect identifiers used there
tagged_src = ''
for r, _, fs in os.walk(ROOT):
    if '.git' in r:
        continue
    for f in fs:
        if f.endswith('.go'):
            p = os.path.join(r, f)
            head = open(p, encoding='utf-8', errors='ignore').read(600)
            if re.search(r'//go:build .*(cuda|metal|vulkan|localllm|darwin|windows|arm64|ui)\b', head) and 'noui' not in head.split('\n')[0]:
                tagged_src += open(p, encoding='utf-8', errors='ignore').read()
tagged_ids = set(re.findall(r'[A-Za-z_][A-Za-z0-9_]*', tagged_src))


def short(n):
    return n.split('.')[-1]


safe = [x for x in T if short(x[2]) not in tagged_ids]
print('after removing names that appear in build-tagged files (cuda/metal/vulkan/localllm/os-specific):', len(safe))
unexp = [x for x in safe if not short(x[2])[0].isupper()]
print('  unexported (cannot be public API):', len(unexp), '| exported:', len(safe) - len(unexp))

bypkg = collections.Counter(os.path.dirname(f) for f, _, _ in safe)
print('by package:', bypkg.most_common(16))

# function sizes
def func_lines(path, line):
    try:
        src = open(os.path.join(ROOT, path), encoding='utf-8', errors='ignore').read().split('\n')
    except OSError:
        return 0
    i = line - 1
    depth = 0
    started = False
    for j in range(i, min(len(src), i + 3000)):
        depth += src[j].count('{') - src[j].count('}')
        if '{' in src[j]:
            started = True
        if started and depth <= 0:
            return j - i + 1
    return 0


rows = [dict(file=f, line=l, name=n, lines=func_lines(f, l), exported=short(n)[0].isupper()) for f, l, n in safe]
print('total lines in truly dead functions:', sum(r['lines'] for r in rows), '| unexported only:', sum(r['lines'] for r in rows if not r['exported']))

# fully dead files
byfile = collections.defaultdict(list)
for r in rows:
    byfile[r['file']].append(r)
full = []
for f, rs in byfile.items():
    p = os.path.join(ROOT, f)
    src = open(p, encoding='utf-8', errors='ignore').read()
    total = len(re.findall(r'(?m)^func ', src))
    if total and len(rs) >= total:
        full.append(dict(file=f, funcs=total, lines=src.count('\n'), has_test=os.path.exists(p[:-3] + '_test.go')))
full.sort(key=lambda x: -x['lines'])
print('files in which EVERY function is dead:', len(full), '| lines:', sum(x['lines'] for x in full))
for x in full[:30]:
    print('  ', x)

# fully dead packages
pkgfiles = collections.defaultdict(lambda: [0, 0])
for r2, _, fs in os.walk(ROOT):
    if '.git' in r2:
        continue
    for f in fs:
        if f.endswith('.go') and not f.endswith('_test.go'):
            rel = os.path.relpath(os.path.join(r2, f), ROOT).replace(chr(92), '/')
            pkgfiles[os.path.dirname(rel)][0] += 1
fullset = {x['file'] for x in full}
for f in fullset:
    pkgfiles[os.path.dirname(f)][1] += 1
deadpk = [(k, v) for k, v in pkgfiles.items() if v[0] and v[0] == v[1]]
print('packages in which every non-test file is fully dead:', deadpk)

big = sorted([r for r in rows if not r['exported']], key=lambda r: -r['lines'])[:40]
json.dump(dict(rows=rows, full_files=full, dead_packages=deadpk, only_tests=[dict(file=f, line=l, name=n) for f, l, n in only_tests]), open(f'{B}/deadcode/summary.json', 'w', encoding='utf-8'), indent=1)
print('largest unexported dead functions:')
for r in big[:25]:
    print(f"   {r['lines']:4d}L {r['file']}:{r['line']} {r['name']}")
