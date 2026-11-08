"""Reduce Graphify output to unambiguous, source-matched Go call edges."""
import json
import os
from pathlib import Path
import re
import subprocess
import ijson


def reduce_graph(graph, declarations):
    locations, files, mapping = {}, {}, {}
    for node in declarations:
        locations.setdefault((node['file'], node['line']), []).append(node)
        files.setdefault(node['file'], []).append(node)
    with Path(graph).open('rb') as stream:
        for node in ijson.items(stream, 'nodes.item'):
            label = node.get('label', '')
            if not label.endswith('()'):
                continue
            line = re.match(r'L(\d+)', node.get('source_location', ''))
            candidates = locations.get((node.get('source_file'), int(line.group(1))), []) if line else []
            candidates = [candidate for candidate in candidates if candidate['name'].split('.')[-1] == label[:-2].lstrip('.')]
            if len(candidates) == 1:
                target = candidates[0]['id']
                if node['id'] in mapping and mapping[node['id']] != target:
                    mapping[node['id']] = None
                else:
                    mapping[node['id']] = target
    edges = set()
    with Path(graph).open('rb') as stream:
        for edge in ijson.items(stream, 'edges.item'):
            if edge.get('relation') != 'calls' or edge.get('_origin') != 'ast':
                continue
            line = re.match(r'L(\d+)', edge.get('source_location', ''))
            callers = [node for node in files.get(edge.get('source_file'), []) if line and node['line'] <= int(line.group(1)) <= node['end']]
            source = callers[0]['id'] if len(callers) == 1 else mapping.get(edge.get('source'))
            target = mapping.get(edge.get('target'))
            if source and target:
                edges.add((source, target))
    return edges


def main():
    site = Path(__file__).resolve().parents[1]
    root = site.parent
    export = Path(os.environ['GRAPHIFY_OUTPUT'])
    revision = (export / 'revision.txt').read_text().strip()
    if subprocess.run(['git', '-C', str(root), 'diff', '--quiet', revision, 'HEAD', '--', 'pkg']).returncode:
        raise RuntimeError('Graphify source differs from the current package tree; extract again')
    ast = json.loads((site / '.source-graph.json').read_text(encoding='utf-8-sig'))
    edges = reduce_graph(export / 'graphify-out' / 'graph.json', ast['nodes'])
    result = {'revision': revision, 'packageTree': subprocess.check_output(['git', '-C', str(root), 'rev-parse', revision + ':pkg'], text=True).strip(), 'links': [{'source': a, 'target': b, 'kind': 'graphify-call'} for a, b in sorted(edges)]}
    (site / '.graphify-calls.json').write_text(json.dumps(result))
    print('Imported', len(edges), 'Graphify call edges matched to source declarations.')


if __name__ == '__main__':
    main()
