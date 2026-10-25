"""Run the real UI modules in the lab on the supported Node 20 floor.

The compiler is a test dependency, not a production UI dependency. Network and
grid adapters stand in for I/O; module bodies, hooks and registered callbacks
are transpiled intact. Nothing is evaluated on the workstation.
"""
import base64
import hashlib
import json
import os
import pathlib
import subprocess
import sys
import tarfile
import tempfile
import urllib.request

ROOT = pathlib.Path(__file__).resolve().parents[3]


def fetch(url, path, integrity=None):
    print('UI_TEST_FETCH', url, file=sys.stderr, flush=True)
    urllib.request.urlretrieve(url, path)
    if integrity:
        actual = base64.b64encode(hashlib.sha512(path.read_bytes()).digest()).decode()
        if actual != integrity:
            raise RuntimeError('test compiler integrity mismatch')


def main():
    with tempfile.TemporaryDirectory(prefix='ui-name-identity-') as folder:
        dest = pathlib.Path(folder)
        archive = dest / 'runtime.tar.gz'
        fetch('https://nodejs.org/dist/v20.19.0/node-v20.19.0-linux-x64.tar.gz', archive)
        with tarfile.open(archive) as tar:
            tar.extractall(dest)
        archive.unlink()
        node = dest / 'node-v20.19.0-linux-x64/bin/node'
        print('UI_TEST_RUNTIME', subprocess.check_output([str(node), '--version'], text=True).strip(), file=sys.stderr, flush=True)
        if '--build-ui' in sys.argv:
            environment = dict(os.environ)
            environment['PATH'] = str(node.parent) + os.pathsep + environment.get('PATH', '')
            npm = node.parent / 'npm'
            subprocess.run([str(npm), 'ci', '--no-audit', '--no-fund'], cwd=ROOT / 'ui', env=environment, check=True)
            subprocess.run([str(npm), 'run', 'build'], cwd=ROOT / 'ui', env=environment, check=True)
            return
        fetch('https://registry.npmjs.org/typescript/-/typescript-5.7.3.tgz', archive,
              '84MVSjMEHP+FQRPy3pX9sTVV/INIex71s9TL2Gm5FG/WG1SqXeKyZ0k7/blY/4FdOzI12CBy1vGc4og/eus0fw==')
        with tarfile.open(archive) as tar:
            tar.extractall(dest)
        # Exercise the real grid's public accessors, using the production lock.
        packages = json.loads((ROOT / 'ui/package-lock.json').read_text())['packages']
        grid_package = packages['node_modules/@ornery/ui-grid-core']
        grid_root = dest / 'grid'
        grid_root.mkdir()
        fetch(grid_package['resolved'], archive, grid_package['integrity'].removeprefix('sha512-'))
        with tarfile.open(archive) as tar:
            tar.extractall(grid_root)
        print('UI_TEST_GRID', grid_package['version'], grid_package['integrity'], file=sys.stderr)
        driver_root = ''
        if len(sys.argv) > 1 and json.loads(pathlib.Path(sys.argv[1]).read_text()).get('boltUri'):
            # Use the production lock's exact dependency closure, including
            # package integrity; no npm resolution or workstation runtime.
            packages = json.loads((ROOT / 'ui/package-lock.json').read_text())['packages']
            driver_root = str(dest / 'driver')
            pending, seen = ['node_modules/neo4j-driver'], set()
            while pending:
                key = pending.pop()
                if key in seen:
                    continue
                seen.add(key)
                package = packages[key]
                stage = dest / ('driver-stage-' + str(len(seen)))
                stage.mkdir()
                fetch(package['resolved'], archive, package['integrity'].removeprefix('sha512-'))
                with tarfile.open(archive) as tar:
                    tar.extractall(stage)
                target = pathlib.Path(driver_root) / key
                target.parent.mkdir(parents=True, exist_ok=True)
                (stage / 'package').rename(target)
                for dependency in package.get('dependencies', {}):
                    parent = key
                    while True:
                        candidate = parent + '/node_modules/' + dependency if parent else 'node_modules/' + dependency
                        if candidate in packages:
                            pending.append(candidate)
                            break
                        parent = parent.rsplit('/node_modules/', 1)[0] if '/node_modules/' in parent else ''
                        if not parent and 'node_modules/' + dependency not in packages:
                            raise RuntimeError('missing locked driver dependency ' + dependency)
            print('UI_TEST_DRIVER', packages['node_modules/neo4j-driver']['version'], 'locked packages', len(seen), file=sys.stderr)
        command = [str(node), str(ROOT / 'internal/testutil/cyphernames/ui_identity_harness.mjs'),
                   str(dest / 'package/lib/typescript.js'), *sys.argv[1:], driver_root, str(grid_root / 'package')]
        subprocess.run(command, check=True)


if __name__ == '__main__':
    main()
