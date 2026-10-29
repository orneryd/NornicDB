"""Completion obligations come from every discovered row, never a review list.

Reach remains the recorded source-span join. Semantic contracts separately pin
the concrete assertion program and its helpers, require an actual passing test
outcome, and require that program in the row's recorded caller provenance.
Neither a test symbol nor a source disposition grants reach. Contracts and
reviewed exclusions are exact-site entries: new rows inherit neither.
"""
import hashlib
import json
import pathlib

ROOT = pathlib.Path(__file__).resolve().parents[3]
SITE_FIELDS = ('Runtime', 'File', 'Function', 'Callee', 'Source', 'Line', 'End')


def site_id(row):
    site = {key: row[key] for key in SITE_FIELDS}
    return hashlib.sha256(json.dumps(site, sort_keys=True, ensure_ascii=False).encode()).hexdigest()


def program_errors(program, root):
    errors = []
    sources = {}
    if not program.get('observation') or not program.get('assertions') or not program.get('sources'):
        return ['semantic program lacks a concrete observation, assertions or source fingerprints']
    for file, expected in program['sources'].items():
        path = root / file
        if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest() != expected:
            errors.append('semantic assertion/helper source changed: ' + file)
        if path.is_file():
            sources[file] = path.read_text(encoding='utf-8')
    for assertion in program['assertions']:
        file, source = assertion.get('file'), assertion.get('source')
        if not file or file not in program['sources'] or not source:
            errors.append('semantic assertion is not pinned to its source')
        elif source not in sources.get(file, ''):
            errors.append('semantic assertion removed: ' + file)
    return errors


def obligations(rows, records, passed_subjects, catalog, root=ROOT):
    """Return one obligation for every census/delegation row and all failures."""
    result, errors = [], []
    passed_subjects = set(passed_subjects)
    programs = catalog.get('programs', {})
    checked_programs = {key: program_errors(program, root) for key, program in programs.items()}
    for row in rows:
        identity = site_id(row)
        obligation = dict(site_id=identity, site={key: row[key] for key in SITE_FIELDS},
                          requires=['recorded_reach', 'semantic_observation'],
                          recorded_reach=row['exercised'], semantic_observations=[])
        exclusion = catalog.get('exclusions', {}).get(identity)
        if exclusion:
            if (exclusion.get('site') != obligation['site'] or not exclusion.get('reason')
                    or not exclusion.get('review', {}).get('reference')
                    or not exclusion.get('review', {}).get('head')):
                errors.append(identity + ': exclusion lacks its exact site and reviewed narrow reason')
                obligation['status'] = 'incomplete'
            else:
                obligation['status'] = 'reviewed_exclusion'
                obligation['exclusion'] = exclusion
            result.append(obligation)
            continue
        # A function/file/test membership cannot silently admit another call.
        contract = catalog.get('rows', {}).get(identity)
        if not contract:
            errors.append(identity + ': undispositioned executable row: ' + row['File'] + ':' + str(row['Line']))
        elif contract.get('site') != obligation['site']:
            errors.append(identity + ': semantic contract site changed')
        elif (not contract.get('source_sha256') or not (root / row['File']).is_file()
              or hashlib.sha256((root / row['File']).read_bytes()).hexdigest() != contract['source_sha256']):
            errors.append(identity + ': construction source changed; semantic contract needs reconciliation')
        else:
            observed = [r for r in records if r['File'] == row['File'] and row['Line'] <= r['Line'] <= row['End']]
            for key in contract.get('programs', []):
                program = programs.get(key)
                if not program or checked_programs[key]:
                    continue
                if program.get('outcome') not in passed_subjects:
                    continue
                if not any(r.get('Subject') == program.get('subject') for r in observed):
                    continue
                obligation['semantic_observations'].append(dict(program=key, observation=program['observation'],
                    outcome=program['outcome'], assertions=program['assertions']))
            if not row['exercised']:
                errors.append(identity + ': executable row lacks recorded reach')
            if not obligation['semantic_observations']:
                errors.append(identity + ': executable row lacks its source-pinned, passing semantic observation')
        obligation['status'] = 'complete' if row['exercised'] and obligation['semantic_observations'] else 'incomplete'
        result.append(obligation)
    if not rows:
        errors.append('empty discovered obligation population')
    return result, errors
