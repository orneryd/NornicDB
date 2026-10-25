"""Lab-only supported-runtime checks; run from the repository root.

The portable interpreters are unpacked into a disposable lab directory. Driver
stubs allow imports and captured ordinary emission without network sessions.
"""
import ast
import contextlib
import io
import json
import os
import pathlib
import runpy
import subprocess
import sys
import tarfile
import tempfile
import types
import urllib.request

ROOT = pathlib.Path(__file__).resolve().parents[3]
# Every maintained Python source in this layer, plus the Qdrant peer whose
# migration README shares the Python 3.10 minimum. Tests/probes are imported too.
FILES = [
    'internal/testutil/cyphernames/completion.py',
    'internal/testutil/cyphernames/test_completion.py',
    'internal/testutil/cyphernames/coverage.py',
    'internal/testutil/cyphernames/graphify_probe.py',
    'internal/testutil/cyphernames/inventory.py',
    'internal/testutil/cyphernames/peer_recording.py',
    'scripts/graphify_local.py',
    'scripts/migration/cypher_names.py',
    'scripts/migration/neo4j/migrate.py',
    'scripts/migration/neo4j/migration_names_probe.py',
    'scripts/migration/qdrant/migrate.py',
    'scripts/test_graphify_local.py',
    'internal/testutil/cyphernames/runtime_matrix.py',
    'internal/testutil/cyphernames/run_ui_identity.py',
]


def check_python():
    sys.path[:0] = [str(ROOT), str(ROOT / 'scripts'), str(ROOT / 'internal/testutil/cyphernames')]
    for file in FILES:
        ast.parse((ROOT / file).read_text(encoding='utf-8'), filename=file)
    neo4j = types.ModuleType('neo4j')
    neo4j.GraphDatabase = neo4j.Driver = neo4j.Session = object
    sys.modules['neo4j'] = neo4j
    qdrant = types.ModuleType('qdrant_client')
    qdrant.QdrantClient = object
    http = types.ModuleType('qdrant_client.http')
    http.models = types.ModuleType('qdrant_client.http.models')
    sys.modules.update({'qdrant_client': qdrant, 'qdrant_client.http': http})
    parser = types.ModuleType('ijson')
    parser.items = lambda stream, prefix, **kwargs: iter(json.load(stream).get(prefix.removesuffix('.item'), []))
    sys.modules['ijson'] = parser
    for file in FILES:
        path = ROOT / file
        ast.parse(path.read_text(encoding='utf-8'), filename=file)
        sys.stdin = io.StringIO('["Person"]')
        with contextlib.redirect_stdout(io.StringIO()):
            runpy.run_path(str(path), run_name='runtime_import_check')
        print('PYTHON_PARSE_IMPORT', sys.version.split()[0], file, flush=True)
    migration = runpy.run_path(str(ROOT / 'scripts/migration/neo4j/migrate.py'))
    emitted = migration['constraint_to_cypher'](dict(name='person_name', type='UNIQUENESS',
        entityType='NODE', labelsOrTypes=['Person'], properties=['name']))
    expected = 'CREATE CONSTRAINT `person_name` IF NOT EXISTS FOR (n:`Person`) REQUIRE n.`name` IS UNIQUE'
    assert emitted == expected, emitted
    print('PYTHON_ORDINARY_EMISSION', sys.version.split()[0], emitted, flush=True)


def unpack(url, destination):
    archive = destination / 'runtime.tar.gz'
    print('RUNTIME_FETCH', url, flush=True)
    urllib.request.urlretrieve(url, archive)
    with tarfile.open(archive) as tar:
        tar.extractall(destination)
    archive.unlink()


def matrix():
    with tempfile.TemporaryDirectory(prefix='name-runtime-') as folder:
        destination = pathlib.Path(folder)
        unpack('https://github.com/indygreg/python-build-standalone/releases/download/20241016/cpython-3.10.15+20241016-x86_64-unknown-linux-gnu-install_only.tar.gz', destination)
        python310 = destination / 'python/bin/python3.10'
        subprocess.run([str(python310), __file__, '--check-python'], check=True)
        if '--python310-only' in sys.argv:
            return
        assert sys.version_info[:2] == (3, 12), sys.version
        subprocess.run([sys.executable, __file__, '--check-python'], check=True)
        unpack('https://nodejs.org/dist/v20.19.0/node-v20.19.0-linux-x64.tar.gz', destination)
        node = destination / 'node-v20.19.0-linux-x64/bin/node'
        print('NODE_RUNTIME', subprocess.check_output([str(node), '--version'], text=True).strip(), flush=True)
        for file in ('ui/src/utils/cypherNames.mjs', 'scripts/migration/neo4j/migrate.mjs',
                     'scripts/migration/qdrant/migrate.mjs',
                     'internal/testutil/cyphernames/peer_recording.mjs',
                     'internal/testutil/cyphernames/runtime_subjects.mjs',
                     'scripts/migration/neo4j/migration_names_probe.mjs'):
            subprocess.run([str(node), '--check', file], check=True)
            print('NODE_PARSE', file, flush=True)
        output = subprocess.check_output([str(node), 'scripts/migration/neo4j/migration_names_probe.mjs'], input='["Person"]', text=True)
        statements = json.loads(output)['Person']
        expected = 'CREATE CONSTRAINT `Person` IF NOT EXISTS FOR (n:`Person`) REQUIRE n.`Person` IS UNIQUE'
        assert any(row['Route'] == 'UNIQUENESS' and row['Text'] == expected for row in statements), statements
        ordinary = json.loads(subprocess.check_output([str(node), 'internal/testutil/cyphernames/runtime_subjects.mjs'], text=True))
        assert any(row['Route'] == 'browser' for row in ordinary)
        print('NODE_ORDINARY_EMISSION', expected, flush=True)


if __name__ == '__main__':
    os.chdir(ROOT)
    if '--policy-controls' in sys.argv:
        subprocess.run([sys.executable, '-m', 'unittest', 'discover', '-s',
                        'internal/testutil/cyphernames', '-p', 'test_completion.py', '-v'], check=True)
    elif '--check-python' in sys.argv:
        check_python()
    else:
        matrix()
