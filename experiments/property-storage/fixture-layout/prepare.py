"""Copy and normalize paired lab stores; never mutate the input directories."""
import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile

p = argparse.ArgumentParser()
p.add_argument('--stock-normalizer', required=True)
p.add_argument('--new-normalizer', required=True)
p.add_argument('--stock-combined', required=True)
p.add_argument('--new-combined', required=True)
p.add_argument('--stock-northwind', required=True)
p.add_argument('--new-northwind', required=True)
p.add_argument('--copy-root', required=True)
p.add_argument('--receipt-dir', required=True)
args = p.parse_args()
out = Path(args.receipt_dir)
out.mkdir(parents=True, exist_ok=False)
root = Path(tempfile.mkdtemp(prefix='settled-comparison-', dir=args.copy_root))
prepared = {}
for name, source, experimental in (
    ('stock-combined', args.stock_combined, False),
    ('new-combined', args.new_combined, True),
    ('stock-northwind', args.stock_northwind, False),
    ('new-northwind', args.new_northwind, True),
):
    source = Path(source).resolve()
    target = root / name
    if target.is_relative_to(source):
        raise ValueError('copy-root must be outside the source stores')
    shutil.copytree(source, target)
    env = os.environ.copy()
    env.update(NORNICDB_EXPERIMENTAL_PROPERTY_STORAGE_ENABLED=str(experimental).lower(), GOGC='400', GOMEMLIMIT='8GiB')
    binary = args.new_normalizer if experimental else args.stock_normalizer
    print('settling', name, flush=True)
    with open(out / (name + '.log'), 'wb') as log:
        subprocess.run([binary, '-mode', 'settle', '-data', str(target), '-out', str(out / (name + '.json'))], env=env, check=True, stdout=log, stderr=subprocess.STDOUT)
    report = json.loads((out / (name + '.json')).read_text())
    prepared[name] = {'source': str(source), 'prepared': str(target), 'normalizer': binary, 'experimental': experimental, 'after_levels': report['after'], 'closed_file_bytes': report['closed_file_bytes']}
for kind in ('combined', 'northwind'):
    if set(prepared['stock-' + kind]['after_levels']) != set(prepared['new-' + kind]['after_levels']):
        raise ValueError('paired sources did not reach matching table levels: ' + kind)
(out / 'metadata.json').write_text(json.dumps({'prepared': prepared, 'original_sources_unchanged': True}, indent=2) + '\n')
print('receipt:', out / 'metadata.json', flush=True)
