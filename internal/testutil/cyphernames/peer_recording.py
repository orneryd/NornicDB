"""Test-only Python caller capture, installed by production-body probes."""
import json
import os
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[3]
seen = set()


def record(role, input_kind):
    frame = sys._getframe(2)  # Skip quote_name and this harness.
    while frame:
        try:
            file = pathlib.Path(frame.f_code.co_filename).resolve().relative_to(ROOT).as_posix()
        except ValueError:
            file = ""
        if file.startswith("scripts/") and not any(s in file for s in ("_probe", "test_", "cypher_names")):
            row = json.dumps(dict(Runtime="python", File=file, Function=frame.f_code.co_name,
                                  Line=frame.f_lineno, Role=role, Input=input_kind,
                                  Subject=pathlib.Path(sys.modules['__main__'].__file__).resolve().relative_to(ROOT).as_posix()), sort_keys=True)
            if row not in seen:
                seen.add(row)
                with open(os.environ["NORNIC_NAME_RECORDING"], "a") as out:
                    out.write(row + "\n")
        frame = frame.f_back


def install():
    if os.environ.get("NORNIC_NAME_RECORDING"):
        sys.path.insert(0, str(ROOT))
        sys.path.insert(0, str(ROOT / "scripts/migration"))
        import cypher_names
        # The importer is also imported as a package by its test driver.
        from scripts.migration import cypher_names as packaged
        for module in (cypher_names, packaged):
            original = module.quote_name
            if getattr(original, '_recording_wrapper', False):
                continue

            def quote_name(name, role='variable', _original=original):
                record(role, 'decoded')
                return _original(name, role)

            quote_name._recording_wrapper = True
            module.quote_name = quote_name
