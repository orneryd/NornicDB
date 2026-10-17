#!/usr/bin/env python3
"""Install a Qdrant benchmark binary from a verified release archive.

Cached archives are verified on every invocation. Executables are always
extracted from that verified archive into the invocation's private directory.
"""
import argparse
import hashlib
import os
import shutil
import tarfile
import tempfile
import urllib.request
from pathlib import Path

VERSION = "v1.19.2"
# GitHub release asset digests for https://github.com/qdrant/qdrant/releases/tag/v1.19.2
DIGESTS = {
    "qdrant-aarch64-apple-darwin.tar.gz": "d24dfd25f4584684b337dd30ee016472cd25f53fc2501f1cd058ab3d7ee0b019",
    "qdrant-x86_64-apple-darwin.tar.gz": "e095b682ad7460baff43827eaa13647b5ec97c48edcb676646ae2f4108395044",
    "qdrant-x86_64-unknown-linux-musl.tar.gz": "50b253243309ed0ae50a19f678f0d0adcc4f0b567b3bd2c5c0c7e02fa8404bb6",
    "qdrant-aarch64-unknown-linux-musl.tar.gz": "6970b93b56fa1203f0cea47d2f330fdb654988fd56617aa77e8478cffd3c0ccb",
}


def verify(path, digest):
    h = hashlib.sha256()
    with open(path, "rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            h.update(block)
    if h.hexdigest() != digest:
        raise ValueError(f"Qdrant archive checksum mismatch: {path}")


def install(cache, destination, version, asset, digest=None):
    digest = digest or (DIGESTS.get(asset) if version == VERSION else None)
    if not digest:
        raise ValueError("this version/asset requires an explicit QDRANT_SHA256")
    cache.mkdir(parents=True, exist_ok=True)
    archive = cache / f"{version}-{asset}"
    if not archive.exists():
        fd, temporary = tempfile.mkstemp(dir=cache, suffix=".download")
        try:
            with os.fdopen(fd, "wb") as target, urllib.request.urlopen(
                    f"https://github.com/qdrant/qdrant/releases/download/{version}/{asset}") as source:
                shutil.copyfileobj(source, target)
            verify(temporary, digest)
            os.replace(temporary, archive)
        finally:
            Path(temporary).unlink(missing_ok=True)
    verify(archive, digest)
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tarfile.open(archive, "r:gz") as package:
        binaries = [member for member in package.getmembers()
                    if member.isfile() and Path(member.name).name == "qdrant"]
        if len(binaries) != 1:
            raise ValueError("release archive must contain exactly one qdrant executable")
        with package.extractfile(binaries[0]) as source, open(destination, "wb") as target:
            shutil.copyfileobj(source, target)
    destination.chmod(0o755)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--cache", type=Path, required=True)
    ap.add_argument("--destination", type=Path, required=True)
    ap.add_argument("--version", default=VERSION)
    ap.add_argument("--asset", required=True)
    ap.add_argument("--sha256")
    args = ap.parse_args()
    install(args.cache, args.destination, args.version, args.asset, args.sha256)


if __name__ == "__main__":
    main()
