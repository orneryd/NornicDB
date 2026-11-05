"""Add the operator-provided NornicDB certificate to the runner's CA bundle."""
import os
from pathlib import Path
import ssl

pem = os.environ.get("NORNICDB_CA_PEM", "").strip()
if not pem:
    raise SystemExit("NORNICDB_CA_PEM is required for the self-signed database certificate")
ssl.PEM_cert_to_DER_cert(pem)
bundle = Path(os.environ["RUNNER_TEMP"]) / "nornicdb-ca-bundle.pem"
system = Path("/etc/ssl/certs/ca-certificates.crt").read_bytes()
bundle.write_bytes(system + b"\n" + pem.encode("ascii") + b"\n")
os.chmod(bundle, 0o600)
ssl.create_default_context(cafile=str(bundle))
with open(os.environ["GITHUB_ENV"], "a", encoding="utf-8") as output:
    output.write(f"SSL_CERT_FILE={bundle}\n")
