"""Extract the current committed source as data for the documentation explorer."""
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import tarfile

root = Path(__file__).resolve().parents[2]

destination = Path(os.environ['GRAPHIFY_OUTPUT']).resolve()
destination.mkdir(parents=True, exist_ok=True)
with tempfile.TemporaryDirectory(prefix='nornic-docs-graph-') as temp:
    archive = Path(temp) / 'source.tar'
    subprocess.run(['git', '-C', str(root), 'archive', '--format=tar', '--output='+str(archive), 'HEAD'], check=True)
    source = Path(temp) / 'source'
    source.mkdir()
    with tarfile.open(archive) as contents:
        contents.extractall(source, filter='data')
    executable = os.environ.get('CODE_INTEL_GRAPHIFY') or str(Path(sys.executable).parent / 'graphify')
    subprocess.run([executable, 'extract', '.', '--code-only', '--no-cluster', '--out', str(destination)], cwd=source, check=True)
    graph = destination / 'graphify-out' / 'graph.json'
    if not graph.is_file():
        raise RuntimeError('Graphify did not produce its graph')
    revision = subprocess.check_output(['git', '-C', str(root), 'rev-parse', 'HEAD'], text=True).strip()
    (destination / 'revision.txt').write_text(revision)
    print(graph)
